package edgefleet

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
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

// nodeFile is the on-disk layout for one node.
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
func loadNodeFile(path string) (*nodeFile, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, &CorruptError{Path: path, Err: err}
	}
	var nf nodeFile
	if err := json.Unmarshal(data, &nf); err != nil {
		return nil, &CorruptError{Path: path, Err: fmt.Errorf("invalid JSON: %w", err)}
	}
	if nf.Format != formatMarker {
		return nil, &CorruptError{Path: path, Err: fmt.Errorf("unknown format marker %q", nf.Format)}
	}
	if nf.Records == nil {
		nf.Records = []Heartbeat{}
	}
	if err := validateStoredRecords(nf.Records); err != nil {
		return nil, &CorruptError{Path: path, Err: err}
	}
	if got := checksumRecords(nf.Records); got != nf.Checksum {
		return nil, &CorruptError{Path: path, Err: fmt.Errorf("checksum mismatch: stored %s, computed %s", nf.Checksum, got)}
	}
	return &nf, nil
}

// validateStoredRecords checks the structural invariants of a stored record
// set: sorted by ascending seq, no duplicate seq, valid field values.
func validateStoredRecords(records []Heartbeat) error {
	for i, r := range records {
		if r.NodeID == "" {
			return fmt.Errorf("record %d: empty node id", i)
		}
		if r.Seq <= 0 {
			return fmt.Errorf("record %d: seq must be a positive integer, got %d", i, r.Seq)
		}
		if r.Version == "" {
			return fmt.Errorf("record %d: empty version", i)
		}
		if r.Height < 0 {
			return fmt.Errorf("record %d: negative height %d", i, r.Height)
		}
		if r.Missed < 0 {
			return fmt.Errorf("record %d: negative missed %d", i, r.Missed)
		}
		if r.CollectedAt.IsZero() {
			return fmt.Errorf("record %d: missing collected_at", i)
		}
		if i > 0 && records[i-1].Seq >= r.Seq {
			return fmt.Errorf("records not sorted by ascending seq at position %d", i)
		}
	}
	return nil
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
	Missed      int64
	Findings    []string
}

// Health judges a node from its latest telemetry (the record with the
// greatest seq). A node with no heartbeats returns Status "notelemetry". A
// query time earlier than the record's collection time is an error. Online
// status uses collection age against the query time: at most 60s is online
// (exactly 60s still online). Version skew and missed-duty rules follow the
// existing Evaluate rules.
func (s *Store) Health(nodeID string, at time.Time, expectedVersion string, toleratedMisses int) (HealthResult, error) {
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
		return HealthResult{NodeID: nodeID, Status: "notelemetry", Findings: []string{"无遥测"}}, nil
	}

	latest := nf.Records[0]
	for _, r := range nf.Records[1:] {
		if r.Seq > latest.Seq {
			latest = r
		}
	}

	if at.Before(latest.CollectedAt) {
		return HealthResult{}, fmt.Errorf("query time %s is earlier than collection time %s for node %q",
			at.Format(time.RFC3339), latest.CollectedAt.Format(time.RFC3339), nodeID)
	}

	age := at.Sub(latest.CollectedAt)
	online := age <= onlineWindow

	node := Node{
		ID:      nodeID,
		Version: latest.Version,
		Height:  latest.Height,
		Missed:  int(latest.Missed),
		Online:  online,
	}
	h := Evaluate(node, expectedVersion, toleratedMisses)

	status := "online"
	if !online {
		status = "offline"
	}
	return HealthResult{
		NodeID:      nodeID,
		Status:      status,
		Seq:         latest.Seq,
		CollectedAt: latest.CollectedAt,
		Version:     latest.Version,
		Height:      latest.Height,
		Missed:      latest.Missed,
		Findings:    h.Findings,
	}, nil
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
