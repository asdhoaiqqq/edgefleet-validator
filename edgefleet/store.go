package edgefleet

// Persistent heartbeat store.
//
// Durability model
//
//	heartbeat.log is an append-only frame journal. Each accepted batch is
//	written as one HB frame per new record followed by a single BATCH_COMMIT
//	frame in a single write + fsync. Records become visible only when COMMIT is
//	seen during replay, so a crash leaves an uncommitted tail which is
//	truncated on the next open. Every frame carries a CRC32; any structural
//	damage inside the committed prefix is hard corruption: Open refuses to read
//	and to continue writing rather than silently running against an empty
//	database.
//
// Concurrency model
//
//	All processes serialise through an flock on heartbeat.lock. Receive and
//	query both take the exclusive lock for their duration, which gives:
//	  - concurrent identical commits insert exactly once (the loser sees it);
//	  - concurrent conflicting commits: exactly one succeeds;
//	  - queries never observe a half-written batch.
//
// The journal is rewritten (compacted) once it exceeds compactionThreshold;
// the rewrite is a temp-file + fsync + atomic rename under the same lock.

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	logFileName  = "heartbeat.log"
	lockFileName = "heartbeat.lock"
	// compactionThreshold bounds replay growth; CLI traffic stays far below it.
	compactionThreshold = 4 << 20

	frameMagic  = "EFHB"
	frameHB     = 1
	frameCommit = 2

	frameHeaderSize = 4 + 1 + 4 // magic + type + length
	frameCRCSuffix  = 4
	maxFramePayload = 16 << 20
)

// CorruptionError means persisted data is damaged. The store stays closed.
type CorruptionError struct {
	Path   string
	Offset int64
	Reason string
}

func (e *CorruptionError) Error() string {
	return fmt.Sprintf("心跳数据已损坏，拒绝读取与写入: %s (偏移 %d): %s", e.Path, e.Offset, e.Reason)
}

// InvalidRecordError identifies a structurally or semantically invalid input.
type InvalidRecordError struct {
	Index  int
	NodeID string
	Reason string
}

func (e *InvalidRecordError) Error() string {
	if e.NodeID != "" {
		return fmt.Sprintf("第 %d 条记录 (node=%s) 非法: %s", e.Index+1, e.NodeID, e.Reason)
	}
	return fmt.Sprintf("第 %d 条记录非法: %s", e.Index+1, e.Reason)
}

// ConflictError reports same node+seq with different field values.
type ConflictError struct {
	Index    int
	NodeID   string
	Seq      int64
	Existing Heartbeat
	Incoming Heartbeat
}

func (e *ConflictError) Error() string {
	return fmt.Sprintf("第 %d 条记录与 node=%s seq=%d 的已保存内容冲突，禁止覆盖",
		e.Index+1, e.NodeID, e.Seq)
}

type recordKey struct {
	node string
	seq  int64
}

type pendingRecord struct {
	hb        Heartbeat
	origIndex int
}

// Store is a process handle on the persistent heartbeat directory.
type Store struct {
	mu  sync.Mutex
	dir string

	lockFile *os.File
	log      *os.File

	nextBatch uint64
	// records is the deduplicated materialised view: one entry per node+seq.
	records map[recordKey]Heartbeat
	// nodes indexes records per node by seq for O(1) current-telemetry lookup.
	nodes map[string]map[int64]Heartbeat

	// failed latches after a write/fsync failure; durability of later calls
	// cannot be guaranteed inside this process, so further writes are refused.
	failed bool
}

// Open opens (or creates) the store in dir and takes the cross-process lock.
// It replays the journal, truncates any crash tail, and refuses corruption.
func Open(dir string) (*Store, error) {
	if strings.TrimSpace(dir) == "" {
		return nil, fmt.Errorf("数据目录不能为空")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("创建数据目录失败: %w", err)
	}
	lockPath := filepath.Join(dir, lockFileName)
	lf, err := os.OpenFile(lockPath, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("打开锁文件失败: %w", err)
	}
	if err := syscall.Flock(int(lf.Fd()), syscall.LOCK_EX); err != nil {
		_ = lf.Close()
		return nil, fmt.Errorf("获取数据目录锁失败: %w", err)
	}
	s := &Store{
		dir:      dir,
		lockFile: lf,
		records:  map[recordKey]Heartbeat{},
		nodes:    map[string]map[int64]Heartbeat{},
	}
	if err := s.openAndReplay(); err != nil {
		_ = syscall.Flock(int(lf.Fd()), syscall.LOCK_UN)
		_ = lf.Close()
		return nil, err
	}
	return s, nil
}

// DataDir reports the on-disk location of the store.
func (s *Store) DataDir() string { return s.dir }

// Close releases the lock; persisted data is unaffected.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var firstErr error
	if s.log != nil {
		if err := s.log.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		s.log = nil
	}
	if s.lockFile != nil {
		_ = syscall.Flock(int(s.lockFile.Fd()), syscall.LOCK_UN)
		if err := s.lockFile.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		s.lockFile = nil
	}
	return firstErr
}

func (s *Store) logPath() string { return filepath.Join(s.dir, logFileName) }

func (s *Store) openAndReplay() error {
	f, err := os.OpenFile(s.logPath(), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return fmt.Errorf("打开心跳日志失败: %w", err)
	}
	s.log = f

	_, tail, err := s.replay(f)
	if err != nil {
		_ = f.Close()
		s.log = nil
		return err
	}
	if tail {
		if err := f.Sync(); err != nil {
			_ = f.Close()
			s.log = nil
			return fmt.Errorf("截断未提交尾巴后同步失败: %w", err)
		}
		if err := syncDir(s.dir); err != nil {
			_ = f.Close()
			s.log = nil
			return err
		}
	}
	return nil
}

// replay applies all committed frames. It returns the byte offset immediately
// after the last COMMIT and whether an uncommitted crash tail was truncated.
//
// Rule for structural anomalies:
//   - a torn read (the file ends mid-frame) is an interrupted append: the
//     uncommitted suffix is truncated, even at offset 0 (a torn first write);
//   - a complete frame with bad magic/length/CRC, or an unparseable payload,
//     can never be produced by our writer and is hard corruption regardless
//     of its position — truncating it would silently destroy data.
func (s *Store) replay(f *os.File) (committedEnd int64, tail bool, err error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return 0, false, err
	}

	var (
		off      int64
		pending  = map[uint64][]Heartbeat{}
		maxBatch uint64
	)

	dropTail := func() (int64, bool, error) {
		if err := f.Truncate(committedEnd); err != nil {
			return 0, false, err
		}
		return committedEnd, true, nil
	}
	corrupt := func(reason string) error {
		return &CorruptionError{Path: s.logPath(), Offset: off, Reason: reason}
	}
	// torn handles a short read at the frame starting at off: always a crash
	// tail (only our process ever appends, in single writes).
	torn := func() (int64, bool, error) { return dropTail() }
	isShort := func(readErr error) bool {
		return readErr == io.EOF || readErr == io.ErrUnexpectedEOF
	}

	for {
		frameOff := off
		magic := make([]byte, 4)
		n, readErr := io.ReadFull(f, magic)
		if readErr == io.EOF && n == 0 {
			break // clean frame boundary
		}
		if readErr != nil {
			if isShort(readErr) {
				return torn()
			}
			return 0, false, readErr
		}
		if string(magic) != frameMagic {
			return 0, false, corrupt("帧魔数不匹配")
		}

		header := make([]byte, 5)
		if _, readErr := io.ReadFull(f, header); readErr != nil {
			if isShort(readErr) {
				return torn()
			}
			return 0, false, readErr
		}
		typ := header[0]
		length := int(binary.BigEndian.Uint32(header[1:5]))
		if length < 8 || length > maxFramePayload {
			return 0, false, corrupt(fmt.Sprintf("帧长度异常: %d", length))
		}

		payload := make([]byte, length)
		if _, readErr := io.ReadFull(f, payload); readErr != nil {
			if isShort(readErr) {
				return torn()
			}
			return 0, false, readErr
		}
		crcBuf := make([]byte, 4)
		if _, readErr := io.ReadFull(f, crcBuf); readErr != nil {
			if isShort(readErr) {
				return torn()
			}
			return 0, false, readErr
		}

		protected := make([]byte, 0, 5+length)
		protected = append(protected, header...)
		protected = append(protected, payload...)
		if binary.BigEndian.Uint32(crcBuf) != crc32.ChecksumIEEE(protected) {
			return 0, false, corrupt("CRC 校验失败")
		}

		batchID := binary.BigEndian.Uint64(payload[:8])
		body := payload[8:]
		switch typ {
		case frameHB:
			hb, perr := UnmarshalHeartbeat(body)
			if perr != nil {
				return 0, false, corrupt("已保存心跳无法解析: " + perr.Error())
			}
			if verr := hb.Validate(hb.ReceivedAt); verr != nil {
				return 0, false, corrupt("已保存心跳不合法: " + verr.Error())
			}
			pending[batchID] = append(pending[batchID], hb)
		case frameCommit:
			var cc commitBody
			if jerr := json.Unmarshal(body, &cc); jerr != nil {
				return 0, false, corrupt("提交帧无法解析: " + jerr.Error())
			}
			list := pending[batchID]
			if len(list) != cc.Count {
				return 0, false, corrupt(fmt.Sprintf(
					"提交帧与待提交记录数量不匹配 (commit=%d, pending=%d)", cc.Count, len(list)))
			}
			recvAt, perr := time.Parse(time.RFC3339Nano, cc.ReceivedAt)
			if perr != nil {
				return 0, false, corrupt("提交帧接收时间无效: " + perr.Error())
			}
			if aerr := s.applyCommitted(list, recvAt); aerr != nil {
				return 0, false, corrupt(aerr.Error())
			}
			delete(pending, batchID)
			if batchID > maxBatch {
				maxBatch = batchID
			}
		default:
			return 0, false, corrupt(fmt.Sprintf("未知帧类型 %d", typ))
		}

		off = frameOff + int64(frameHeaderSize+length+frameCRCSuffix)
		if typ == frameCommit {
			committedEnd = off
		}
	}
	s.nextBatch = maxBatch + 1
	return committedEnd, tail, nil
}

type commitBody struct {
	Count      int    `json:"count"`
	ReceivedAt string `json:"received_at"`
}

// applyCommitted merges a replayed batch into the in-memory view.
func (s *Store) applyCommitted(list []Heartbeat, receivedAt time.Time) error {
	seen := map[recordKey]bool{}
	for _, hb := range list {
		if hb.ReceivedAt.IsZero() {
			hb.ReceivedAt = receivedAt
		}
		key := recordKey{hb.NodeID, hb.Seq}
		if seen[key] {
			return fmt.Errorf("已提交批次内 node=%s seq=%d 重复", key.node, key.seq)
		}
		seen[key] = true
		if existing, ok := s.records[key]; ok {
			if !heartbeatEqual(existing, hb) {
				return fmt.Errorf("已提交批次与历史记录冲突 node=%s seq=%d", key.node, key.seq)
			}
			continue
		}
		s.put(hb)
	}
	return nil
}

func (s *Store) put(hb Heartbeat) {
	s.records[recordKey{hb.NodeID, hb.Seq}] = hb
	bySeq := s.nodes[hb.NodeID]
	if bySeq == nil {
		bySeq = map[int64]Heartbeat{}
		s.nodes[hb.NodeID] = bySeq
	}
	bySeq[hb.Seq] = hb
}

// Receive validates and atomically persists one batch. It returns the counts
// of newly inserted and duplicate records. Duplicate identity is node+seq with
// identical content; node+seq with differing content conflicts and rejects the
// whole batch. An invalid or conflicting record makes the entire batch a
// no-op: nothing is written and prior data stays queryable.
func (s *Store) Receive(batch []Heartbeat, receivedAt time.Time) (inserted, duplicated int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed {
		return 0, 0, fmt.Errorf("存储此前保存失败，为避免破坏数据已拒绝继续写入，请检查后重新打开")
	}
	if len(batch) == 0 {
		return 0, 0, fmt.Errorf("批次为空：至少需要一条心跳")
	}
	if receivedAt.IsZero() {
		return 0, 0, fmt.Errorf("接收时间不能为空")
	}

	// Phase 1: field-level validation, in caller order.
	for i := range batch {
		if verr := batch[i].Validate(receivedAt); verr != nil {
			return 0, 0, &InvalidRecordError{Index: i, NodeID: batch[i].NodeID, Reason: verr.Error()}
		}
	}

	// Phase 2: fold the batch itself. Exact repeats are duplicates; same key
	// with different content is a conflict.
	unique := make([]pendingRecord, 0, len(batch))
	byKey := map[recordKey]pendingRecord{}
	for i := range batch {
		hb := batch[i]
		key := recordKey{hb.NodeID, hb.Seq}
		if first, seen := byKey[key]; seen {
			if !heartbeatEqual(first.hb, hb) {
				return 0, 0, &ConflictError{Index: i, NodeID: key.node, Seq: key.seq,
					Existing: first.hb, Incoming: hb}
			}
			duplicated++
			continue
		}
		pr := pendingRecord{hb: hb, origIndex: i}
		byKey[key] = pr
		unique = append(unique, pr)
	}

	// Phase 3: compare unique records with committed data.
	toInsert := make([]Heartbeat, 0, len(unique))
	for _, pr := range unique {
		key := recordKey{pr.hb.NodeID, pr.hb.Seq}
		existing, ok := s.records[key]
		if !ok {
			toInsert = append(toInsert, pr.hb)
			continue
		}
		if !heartbeatEqual(existing, pr.hb) {
			return 0, 0, &ConflictError{Index: pr.origIndex, NodeID: key.node, Seq: key.seq,
				Existing: existing, Incoming: pr.hb}
		}
		duplicated++
	}

	// Phase 4: durable, atomic append. Nothing is visible until COMMIT lands
	// and fsync succeeds.
	if len(toInsert) > 0 {
		batchID := s.nextBatch
		if err := s.appendBatch(batchID, toInsert, receivedAt); err != nil {
			s.failed = true
			return 0, 0, err
		}
		s.nextBatch++
		for _, hb := range toInsert {
			hb.ReceivedAt = receivedAt
			s.put(hb)
		}
		inserted = len(toInsert)
	}
	return inserted, duplicated, nil
}

func (s *Store) appendBatch(batchID uint64, list []Heartbeat, receivedAt time.Time) error {
	buf := make([]byte, 0, len(list)*128)
	for _, hb := range list {
		hb.ReceivedAt = receivedAt
		data, err := json.Marshal(hb.toJSON())
		if err != nil {
			return fmt.Errorf("编码心跳失败: %w", err)
		}
		payload := append(u64be(batchID), data...)
		buf = appendFrame(buf, frameHB, payload)
	}
	body, err := json.Marshal(commitBody{
		Count:      len(list),
		ReceivedAt: formatTime(receivedAt),
	})
	if err != nil {
		return fmt.Errorf("编码提交帧失败: %w", err)
	}
	buf = appendFrame(buf, frameCommit, append(u64be(batchID), body...))

	n, err := s.log.Write(buf)
	if err != nil {
		return fmt.Errorf("写入心跳日志失败: %w", err)
	}
	if n != len(buf) {
		return fmt.Errorf("心跳日志写入不完整: %d/%d 字节", n, len(buf))
	}
	if err := s.log.Sync(); err != nil {
		return fmt.Errorf("同步心跳日志失败: %w", err)
	}

	fi, err := s.log.Stat()
	if err == nil && fi.Size() > compactionThreshold {
		if cerr := s.compact(); cerr != nil {
			// Committed bytes are safe, but the file layout is now suspect;
			// refuse further writes in this process rather than risk damage.
			s.failed = true
			return cerr
		}
	}
	return nil
}

// compact rewrites the deduplicated view into a fresh journal as one
// single-record committed batch per entry, then atomically replaces the log.
func (s *Store) compact() error {
	tmpPath := s.logPath() + ".compact"
	tmp, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("创建压缩日志失败: %w", err)
	}
	abort := func(cause error) error {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return cause
	}

	keys := make([]recordKey, 0, len(s.records))
	for k := range s.records {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].node != keys[j].node {
			return keys[i].node < keys[j].node
		}
		return keys[i].seq < keys[j].seq
	})

	var (
		batchID uint64 = 1
		buf     []byte
	)
	for _, k := range keys {
		hb := s.records[k]
		data, err := json.Marshal(hb.toJSON())
		if err != nil {
			return abort(fmt.Errorf("压缩时编码心跳失败: %w", err))
		}
		buf = appendFrame(buf, frameHB, append(u64be(batchID), data...))
		body, err := json.Marshal(commitBody{Count: 1, ReceivedAt: formatTime(hb.ReceivedAt)})
		if err != nil {
			return abort(fmt.Errorf("压缩时编码提交帧失败: %w", err))
		}
		buf = appendFrame(buf, frameCommit, append(u64be(batchID), body...))
		batchID++
	}
	if _, err := tmp.Write(buf); err != nil {
		return abort(fmt.Errorf("写入压缩日志失败: %w", err))
	}
	if err := tmp.Sync(); err != nil {
		return abort(fmt.Errorf("同步压缩日志失败: %w", err))
	}
	if err := tmp.Close(); err != nil {
		return abort(fmt.Errorf("关闭压缩日志失败: %w", err))
	}
	if err := os.Rename(tmpPath, s.logPath()); err != nil {
		return abort(fmt.Errorf("替换心跳日志失败: %w", err))
	}
	if err := syncDir(s.dir); err != nil {
		return err
	}
	newLog, err := os.OpenFile(s.logPath(), os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("压缩后重开心跳日志失败: %w", err)
	}
	_ = s.log.Close()
	s.log = newLog
	s.nextBatch = batchID
	return nil
}

// Latest returns the current telemetry for a node: the maximum seq regardless
// of arrival time. It returns nil when the node has no heartbeat.
func (s *Store) Latest(node string) *Heartbeat {
	s.mu.Lock()
	defer s.mu.Unlock()
	bySeq := s.nodes[node]
	if len(bySeq) == 0 {
		return nil
	}
	var maxSeq int64
	first := true
	for seq := range bySeq {
		if first || seq > maxSeq {
			maxSeq, first = seq, false
		}
	}
	hb := bySeq[maxSeq]
	return &hb
}

// Health evaluates a node from its current telemetry, reusing Evaluate rules.
func (s *Store) Health(node string, queryAt time.Time, expectedVersion string, toleratedMisses int64) (HealthStatus, error) {
	if queryAt.IsZero() {
		return HealthStatus{}, fmt.Errorf("查询时间不能为空")
	}
	st, err := QueryHealth(s.Latest(node), queryAt, expectedVersion, toleratedMisses)
	if st.NodeID == "" {
		st.NodeID = node // echo the queried name, including the 无遥测 case
	}
	return st, err
}

// History returns every stored heartbeat for a node ordered by ascending seq,
// with no duplicates.
func (s *Store) History(node string) []Heartbeat {
	s.mu.Lock()
	defer s.mu.Unlock()
	bySeq := s.nodes[node]
	out := make([]Heartbeat, 0, len(bySeq))
	for _, hb := range bySeq {
		out = append(out, hb)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out
}

// Nodes returns all known node identifiers, sorted.
func (s *Store) Nodes() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.nodes))
	for n := range s.nodes {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

func appendFrame(dst []byte, typ byte, payload []byte) []byte {
	dst = append(dst, frameMagic...)
	dst = append(dst, typ)
	dst = append(dst, u32be(uint32(len(payload)))...)
	dst = append(dst, payload...)
	protected := dst[len(dst)-(5+len(payload)):]
	dst = append(dst, u32be(crc32.ChecksumIEEE(protected))...)
	return dst
}

func u32be(v uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return b
}

func u64be(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("打开数据目录同步失败: %w", err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("同步数据目录失败: %w", err)
	}
	return nil
}
