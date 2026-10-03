package edgefleet

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"syscall"
	"time"
)

// formatMarker identifies the on-disk file format version.
const formatMarker = "edgefleet-heartbeats-v1"

// onlineWindow is the maximum age of the latest telemetry for a node to be
// considered online. Exactly onlineWindow is still online.
const onlineWindow = 60 * time.Second

// CorruptError means the on-disk data failed parsing or verification. The
// store refuses to read or write while corruption is present.
type CorruptError struct {
	Path string
	Err  error
}

func (e *CorruptError) Error() string {
	return fmt.Sprintf("data corruption detected in %s: %v", e.Path, e.Err)
}

func (e *CorruptError) Unwrap() error { return e.Err }

// IsCorrupt reports whether err is (or wraps) a CorruptError.
func IsCorrupt(err error) bool {
	var ce *CorruptError
	return errors.As(err, &ce)
}

// nodeFile is the on-disk layout for one node. It is used for writing;
// reading is done by parseNodeFile, which never lets Go zero values stand in
// for unsaved telemetry.
type nodeFile struct {
	Format   string      `json:"format"`
	Checksum string      `json:"checksum"`
	Records  []Heartbeat `json:"records"`
}

// Store persists heartbeats as per-node JSON files under a data directory.
// Multiple Store instances (in the same or different processes) may safely
// access the same directory concurrently: a flock on the directory lock file
// serialises writers and lets queries take a shared lock, so a query never
// observes a partially written batch.
type Store struct {
	dir      string
	nodesDir string
	lockPath string
}

// OpenStore opens (and if needed creates) a data directory.
func OpenStore(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("cannot create data directory %s: %w", dir, err)
	}
	nodesDir := filepath.Join(dir, "nodes")
	if err := os.MkdirAll(nodesDir, 0o755); err != nil {
		return nil, fmt.Errorf("cannot create nodes directory %s: %w", nodesDir, err)
	}
	return &Store{dir: dir, nodesDir: nodesDir, lockPath: filepath.Join(dir, "lock")}, nil
}

// Dir returns the data directory path.
func (s *Store) Dir() string { return s.dir }

// nodePath maps a node id to its per-node file. Hex encoding keeps arbitrary
// node ids safe as filenames while remaining reversible.
func (s *Store) nodePath(nodeID string) string {
	return filepath.Join(s.nodesDir, hex.EncodeToString([]byte(nodeID))+".json")
}

// lock acquires the directory lock. Exclusive for writes, shared for reads.
// The returned closure releases the lock.
func (s *Store) lock(exclusive bool) (func(), error) {
	f, err := os.OpenFile(s.lockPath, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("cannot open lock file %s: %w", s.lockPath, err)
	}
	how := syscall.LOCK_SH
	if exclusive {
		how = syscall.LOCK_EX
	}
	if err := syscall.Flock(int(f.Fd()), how); err != nil {
		f.Close()
		return nil, fmt.Errorf("cannot acquire data lock: %w", err)
	}
	return func() {
		syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		f.Close()
	}, nil
}

// checksumRecords returns a stable hash of the records. json.Marshal of a
// struct slice is deterministic for a given set of records.
func checksumRecords(records []Heartbeat) string {
	data, _ := json.Marshal(records)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// loadNodeFile reads and verifies the file for one node. A missing file means
// the node has no records (returns nil, nil). Any parse, checksum or
// structural failure is reported as a CorruptError.
//
// Every saved record is re-checked with the same strict rules applied at
// submission: node, seq, collected_at, version, height and missed must each
// appear exactly once with a non-null value. A file that omits a field (or
// writes null) with a checksum matching the resulting zero values is therefore
// still rejected — a zero value must have been explicitly saved as 0, and
// duplicate JSON keys never collapse to their last occurrence.
func loadNodeFile(path string) (*nodeFile, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, &CorruptError{Path: path, Err: err}
	}
	nf, err := parseNodeFile(data)
	if err != nil {
		return nil, &CorruptError{Path: path, Err: err}
	}
	return nf, nil
}

// parseNodeFile strictly parses one on-disk node file and verifies its
// checksum. All structural validation happens before the checksum is even
// looked at, so a checksum that matches the file after missing fields were
// filled with zero values cannot make damaged records acceptable.
func parseNodeFile(data []byte) (*nodeFile, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	tok, err := dec.Token()
	if err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}
	if d, ok := tok.(json.Delim); !ok || d != '{' {
		return nil, fmt.Errorf("invalid JSON: top-level value must be an object")
	}

	envelope := make(map[string]json.RawMessage)
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return nil, fmt.Errorf("invalid JSON: %w", err)
		}
		key, ok := keyTok.(string)
		if !ok {
			return nil, fmt.Errorf("invalid JSON: field name must be a string")
		}
		if _, exists := envelope[key]; exists {
			return nil, fmt.Errorf("duplicate field %q in file header", key)
		}
		var val json.RawMessage
		if err := dec.Decode(&val); err != nil {
			return nil, fmt.Errorf("invalid JSON: invalid value for field %q: %w", key, err)
		}
		envelope[key] = val
	}
	if _, err := dec.Token(); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}
	// Reject trailing data after the object (json.Decoder stops at the first
	// valid value); the file must contain exactly one envelope object.
	if _, err := dec.Token(); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("invalid JSON: trailing data after file object")
		}
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}

	for _, f := range []string{"format", "checksum", "records"} {
		if _, ok := envelope[f]; !ok {
			return nil, fmt.Errorf("missing required file field %q", f)
		}
	}
	for k := range envelope {
		switch k {
		case "format", "checksum", "records":
		default:
			return nil, fmt.Errorf("unknown file field %q", k)
		}
	}

	if bytes.Equal(envelope["format"], []byte("null")) {
		return nil, fmt.Errorf(`file field "format" must not be null`)
	}
	var format string
	if err := json.Unmarshal(envelope["format"], &format); err != nil {
		return nil, fmt.Errorf(`file field "format" must be a string: %w`, err)
	}
	if format != formatMarker {
		return nil, fmt.Errorf("unknown format marker %q", format)
	}

	if bytes.Equal(envelope["checksum"], []byte("null")) {
		return nil, fmt.Errorf(`file field "checksum" must not be null`)
	}
	var checksum string
	if err := json.Unmarshal(envelope["checksum"], &checksum); err != nil {
		return nil, fmt.Errorf(`file field "checksum" must be a string: %w`, err)
	}

	rawRecords := envelope["records"]
	if bytes.Equal(rawRecords, []byte("null")) {
		return nil, fmt.Errorf(`file field "records" must not be null`)
	}
	rd := json.NewDecoder(bytes.NewReader(rawRecords))
	tok, err = rd.Token()
	if err != nil {
		return nil, fmt.Errorf(`file field "records" must be an array: %w`, err)
	}
	if d, ok := tok.(json.Delim); !ok || d != '[' {
		return nil, fmt.Errorf(`file field "records" must be an array`)
	}

	records := []Heartbeat{}
	idx := 0
	for rd.More() {
		var raw json.RawMessage
		if err := rd.Decode(&raw); err != nil {
			return nil, fmt.Errorf("record %d: invalid JSON value: %w", idx+1, err)
		}
		h, err := parseHeartbeatFields(raw)
		if err != nil {
			return nil, fmt.Errorf("record %d: %w", idx+1, err)
		}
		if idx > 0 && records[idx-1].Seq >= h.Seq {
			return nil, fmt.Errorf("records not sorted by ascending seq at position %d", idx+1)
		}
		records = append(records, h)
		idx++
	}
	if _, err := rd.Token(); err != nil {
		return nil, fmt.Errorf(`file field "records": %w`, err)
	}

	if got := checksumRecords(records); got != checksum {
		return nil, fmt.Errorf("checksum mismatch: stored %s, computed %s", checksum, got)
	}
	return &nodeFile{Format: format, Checksum: checksum, Records: records}, nil
}

// writeNodeFile persists records atomically: a temp file in the same
// directory is fsynced and renamed over the target, so a crash never leaves
// a half-written file.
func writeNodeFile(path string, records []Heartbeat) error {
	if records == nil {
		records = []Heartbeat{}
	}
	nf := nodeFile{
		Format:   formatMarker,
		Checksum: checksumRecords(records),
		Records:  records,
	}
	data, err := json.MarshalIndent(nf, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomic(path, data)
}

func writeFileAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		os.Remove(tmpName)
		return err
	}
	// Best-effort fsync of the directory so the rename itself survives.
	if d, err := os.Open(dir); err == nil {
		d.Sync()
		d.Close()
	}
	return nil
}

// Submit persists a batch of heartbeats. The whole batch is validated and
// checked for duplicates/conflicts against stored data (and within the batch)
// before anything is written: a single invalid or conflicting record fails
// the batch with no changes. Duplicates are counted, not written. A
// successful return means the batch was durably persisted; a persistence
// failure is reported and previously queryable data is left intact.
func (s *Store) Submit(records []Heartbeat, receiveTime time.Time) (newCount, dupCount int, err error) {
	if len(records) == 0 {
		return 0, 0, fmt.Errorf("no heartbeat records provided")
	}
	for _, r := range records {
		if err := ValidateHeartbeat(r, receiveTime); err != nil {
			return 0, 0, fmt.Errorf("invalid heartbeat for node %q seq %d: %w", r.NodeID, r.Seq, err)
		}
	}

	unlock, err := s.lock(true)
	if err != nil {
		return 0, 0, err
	}
	defer unlock()

	// Deduplicate within the batch: same node+seq with equal fields counts
	// as a duplicate; same node+seq with differing content is a conflict.
	merged := make(map[string][]Heartbeat)
	for _, r := range records {
		list := merged[r.NodeID]
		found := -1
		for i, e := range list {
			if e.Seq == r.Seq {
				found = i
				break
			}
		}
		if found >= 0 {
			if !list[found].Equal(r) {
				return 0, 0, fmt.Errorf("conflicting records for node %q seq %d in batch: content differs", r.NodeID, r.Seq)
			}
			dupCount++
			continue
		}
		merged[r.NodeID] = append(list, r)
	}

	// Merge with stored data per node, collecting new file contents.
	type update struct {
		path    string
		records []Heartbeat
	}
	var updates []update
	for nodeID, batch := range merged {
		path := s.nodePath(nodeID)
		nf, err := loadNodeFile(path)
		if err != nil {
			return 0, 0, err
		}
		existing := []Heartbeat{}
		if nf != nil {
			existing = nf.Records
		}
		for _, r := range batch {
			found := -1
			for i, e := range existing {
				if e.Seq == r.Seq {
					found = i
					break
				}
			}
			if found >= 0 {
				if !existing[found].Equal(r) {
					return 0, 0, fmt.Errorf("conflicting record for node %q seq %d: content differs from stored record", nodeID, r.Seq)
				}
				dupCount++
				continue
			}
			existing = append(existing, r)
			newCount++
		}
		sort.Slice(existing, func(i, j int) bool { return existing[i].Seq < existing[j].Seq })
		updates = append(updates, update{path: path, records: existing})
	}

	// Persist. All updates are appends to previously verified data, so a
	// failure here leaves every previously queryable record intact.
	for _, u := range updates {
		if err := writeNodeFile(u.path, u.records); err != nil {
			return 0, 0, fmt.Errorf("failed to persist heartbeat batch: %w", err)
		}
	}
	return newCount, dupCount, nil
}

// HealthResult is the health judgement for one node based on real telemetry.
type HealthResult struct {
	NodeID      string
	Status      string // "online", "offline", "notelemetry"
	Seq         int64
	CollectedAt time.Time
	Version     string
	Height      int64
	Missed      int64 // cumulative missed duties at the latest heartbeat
	Findings    []string

	// Baseline-relative missed duties, populated only by HealthSince.
	// BaselineSeq is the seq of the heartbeat the query was anchored to;
	// BaselineMissed is the cumulative count at that heartbeat. When
	// NewMissedKnown is false the cumulative counter went backwards
	// somewhere between the baseline and the latest record, so the number of
	// newly missed duties cannot be derived; Findings then reports the
	// rollback. Missed always remains the latest cumulative count.
	BaselineSeq    int64
	BaselineMissed int64
	NewMissed      int64
	NewMissedKnown bool
}

// rollbackFinding warns that the cumulative missed-duty counter decreased
// between two adjacent saved heartbeats (typically a node counter reset),
// which makes the endpoint difference unusable as the new-miss count.
const rollbackFinding = "累计漏签数回退"

// Health judges a node from its latest telemetry (the record with the
// greatest seq). A node with no heartbeats returns Status "notelemetry". A
// query time earlier than the record's collection time is an error. Online
// status uses collection age against the query time: at most 60s is online
// (exactly 60s still online). Version skew and missed-duty rules follow the
// existing Evaluate rules.
func (s *Store) Health(nodeID string, at time.Time, expectedVersion string, toleratedMisses int) (HealthResult, error) {
	return s.health(nodeID, at, expectedVersion, toleratedMisses, 0, false)
}

// HealthSince judges a node the same way Health does, but the missed-duty
// alarm is measured against a user-supplied baseline heartbeat instead of the
// cumulative counter alone. baselineSeq must be the seq (> 0) of a record
// already saved for the node: the latest heartbeat stays the record with the
// greatest seq, and new missed duties are the latest cumulative count minus
// the count at the baseline record. Only a strict excess over
// toleratedMisses alarms; equality does not.
//
// The baseline affects this query only: stored history is never modified.
// Seqs between the baseline and the latest may have gaps. If the cumulative
// count decreases between any two adjacent saved records in that interval the
// new count is reported as unknown (NewMissedKnown == false) and a rollback
// finding is emitted, even if the latest count later climbs back past the
// baseline; decreases before the baseline are irrelevant. A node without
// telemetry, an unknown baseline seq, or a baseline newer than the latest
// record is an error — no other record is substituted and the baseline count
// is never assumed to be zero.
func (s *Store) HealthSince(nodeID string, at time.Time, expectedVersion string, toleratedMisses int, baselineSeq int64) (HealthResult, error) {
	if baselineSeq <= 0 {
		return HealthResult{}, fmt.Errorf("--missed-since-seq must be a positive integer, got %d", baselineSeq)
	}
	return s.health(nodeID, at, expectedVersion, toleratedMisses, baselineSeq, true)
}

func (s *Store) health(nodeID string, at time.Time, expectedVersion string, toleratedMisses int, baselineSeq int64, useBaseline bool) (HealthResult, error) {
	unlock, err := s.lock(false)
	if err != nil {
		return HealthResult{}, err
	}
	defer unlock()

	nf, err := loadNodeFile(s.nodePath(nodeID))
	if err != nil {
		return HealthResult{}, err
	}
	if nf == nil || len(nf.Records) == 0 {
		if useBaseline {
			return HealthResult{}, fmt.Errorf("node %q has no heartbeats; cannot use --missed-since-seq=%d as baseline", nodeID, baselineSeq)
		}
		return HealthResult{NodeID: nodeID, Status: "notelemetry", Findings: []string{"无遥测"}}, nil
	}

	records := nf.Records // ascending seq, unique
	latest := records[len(records)-1]

	if useBaseline {
		if baselineSeq > latest.Seq {
			return HealthResult{}, fmt.Errorf("--missed-since-seq=%d is newer than the latest heartbeat seq %d for node %q", baselineSeq, latest.Seq, nodeID)
		}
	}

	if at.Before(latest.CollectedAt) {
		return HealthResult{}, fmt.Errorf("query time %s is earlier than collection time %s for node %q",
			at.Format(time.RFC3339), latest.CollectedAt.Format(time.RFC3339), nodeID)
	}

	age := at.Sub(latest.CollectedAt)
	online := age <= onlineWindow

	findings := []string{}
	if !online {
		findings = append(findings, "offline")
	}
	if latest.Version != expectedVersion {
		findings = append(findings, "version skew: "+latest.Version+" != "+expectedVersion)
	}

	result := HealthResult{
		NodeID:      nodeID,
		Status:      "online",
		Seq:         latest.Seq,
		CollectedAt: latest.CollectedAt,
		Version:     latest.Version,
		Height:      latest.Height,
		Missed:      latest.Missed,
	}
	if !online {
		result.Status = "offline"
	}

	if !useBaseline {
		// Cumulative judgement: the current counter must strictly exceed the
		// tolerance (mirrors Evaluate for callers that do not use a baseline).
		node := Node{
			ID:      nodeID,
			Version: latest.Version,
			Height:  latest.Height,
			Missed:  int(latest.Missed),
			Online:  online,
		}
		result.Findings = Evaluate(node, expectedVersion, toleratedMisses).Findings
		return result, nil
	}

	baselineIdx := -1
	for i, r := range records {
		if r.Seq == baselineSeq {
			baselineIdx = i
			break
		}
	}
	if baselineIdx < 0 {
		return HealthResult{}, fmt.Errorf("no saved heartbeat with seq %d for node %q; cannot use it as --missed-since-seq baseline", baselineSeq, nodeID)
	}
	baseline := records[baselineIdx]
	result.BaselineSeq = baseline.Seq
	result.BaselineMissed = baseline.Missed

	// Walk only saved records from the baseline up to the latest. Records are
	// stored ascending by seq and seqs may legitimately have gaps, so any
	// decrease between two adjacent saved records in this interval proves the
	// cumulative counter reset somewhere in it; the endpoint difference alone
	// must not be trusted even if the final count recovers.
	rolledBack := false
	for i := baselineIdx + 1; i < len(records); i++ {
		if records[i].Missed < records[i-1].Missed {
			rolledBack = true
			break
		}
	}
	if rolledBack {
		result.NewMissedKnown = false
		findings = append(findings, rollbackFinding)
	} else {
		result.NewMissedKnown = true
		// No decrease was observed, so cumulative counts cannot have wrapped
		// backwards within int64 range; the difference stays exact.
		result.NewMissed = latest.Missed - baseline.Missed
		if result.NewMissed > int64(toleratedMisses) {
			findings = append(findings, "missed duties above tolerance")
		}
	}
	result.Findings = findings
	return result, nil
}

// History returns all heartbeats for a node in ascending seq order, without
// duplicates. A node with no records returns an empty slice.
func (s *Store) History(nodeID string) ([]Heartbeat, error) {
	unlock, err := s.lock(false)
	if err != nil {
		return nil, err
	}
	defer unlock()

	nf, err := loadNodeFile(s.nodePath(nodeID))
	if err != nil {
		return nil, err
	}
	if nf == nil {
		return []Heartbeat{}, nil
	}
	return nf.Records, nil
}
