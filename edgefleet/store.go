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
	"strings"
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
//
// Node files live in two places chosen solely by the byte length of the node
// id:
//
//   - Short ids keep their file at nodes/hex(nodeID)+".json", exactly as
//     always; the file name itself decodes to the owning node id.
//   - Long ids (those whose hex name would exceed the local file-name limit)
//     keep their file at slots/<sha256(nodeID)>.json: a fixed-length content
//     name that never grows with the id. The hash is computed over the whole
//     id and only addresses the file — it never identifies the node — so a
//     slot is loaded for a specific queried id and every record in it must
//     declare that same id, the same ownership check a named hex file gets.
//     A different id whose well-formed file is found at this slot (a planted
//     foreign file) is refused as corruption rather than merged or read as
//     this node's telemetry.
type Store struct {
	dir      string
	nodesDir string
	slotsDir string
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
	slotsDir := filepath.Join(dir, "slots")
	if err := os.MkdirAll(slotsDir, 0o755); err != nil {
		return nil, fmt.Errorf("cannot create slots directory %s: %w", slotsDir, err)
	}
	return &Store{
		dir:      dir,
		nodesDir: nodesDir,
		slotsDir: slotsDir,
		lockPath: filepath.Join(dir, "lock"),
	}, nil
}

// Dir returns the data directory path.
func (s *Store) Dir() string { return s.dir }

// maxNodeFileNameBytes is the local file-system name limit the store targets:
// the base name of a short-id file is two hex characters per id byte plus the
// ".json" suffix, and may use the full limit — every such name was writable
// before long-id support existed, and keeping the limit at exactly this value
// means every id that previously saved still uses its original file (no
// rename, move or resubmit). Ids whose hex name would exceed the limit use a
// fixed-length slot name instead; the limit never rejects an id — it only
// selects where the file is placed.
const maxNodeFileNameBytes = 255

// nodeLocation is the one on-disk file holding one node's records.
type nodeLocation struct {
	path  string
	owner string // the node id every record in the file must declare
	named bool   // true: a legacy hex file whose name carries its owner; false: a hash-addressed slot whose owner is the queried id
}

// nodeLocationFor maps a node id to its storage location. The mapping depends
// only on the exact id bytes — no trimming, whitespace handling or character
// replacement — so two ids are stored apart unless their bytes are identical:
// ids sharing a long prefix and differing at the end hash differently, and an
// id submitted literally or via equivalent JSON escapes decodes to the same
// bytes and maps to the same file. A short id's file name is its own hex,
// which reversibly carries its owner; a long id's slot name is the full hash
// of its bytes and carries no id text at all, so its owner is the queried id
// itself.
func (s *Store) nodeLocationFor(nodeID string) nodeLocation {
	raw := []byte(nodeID)
	hexName := hex.EncodeToString(raw) + ".json"
	if len(hexName) <= maxNodeFileNameBytes {
		return nodeLocation{
			path:  filepath.Join(s.nodesDir, hexName),
			owner: nodeID,
			named: true,
		}
	}
	sum := sha256.Sum256(raw)
	return nodeLocation{
		path:  filepath.Join(s.slotsDir, hex.EncodeToString(sum[:])+".json"),
		owner: nodeID,
	}
}

// nodePath maps a node id to its per-node file. Short ids use a reversible hex
// file name; ids too long for a file name use a fixed-length content-addressed
// slot, so arbitrary-length legal Unicode ids never depend on a file name
// being able to hold them.
func (s *Store) nodePath(nodeID string) string {
	return s.nodeLocationFor(nodeID).path
}

// nodeOwnerFromPath recovers the node id a *short-id* store file belongs to
// from its file name: short-id files are named hex(nodeID)+".json", so the
// owner is exactly the string the base name decodes to. Slot file names are
// hashes that carry no node id; their owner is supplied by the caller (the id
// being queried), not inferred from the name.
func nodeOwnerFromPath(path string) (string, error) {
	base := filepath.Base(path)
	if !strings.HasSuffix(base, ".json") {
		return "", fmt.Errorf("file name %q is not a per-node heartbeat file", base)
	}
	raw, err := hex.DecodeString(strings.TrimSuffix(base, ".json"))
	if err != nil {
		return "", fmt.Errorf("file name %q does not encode a node id: %w", base, err)
	}
	return string(raw), nil
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

// loadNodeFile reads and verifies the file at loc. A missing file means the
// node has no records (returns nil, nil). Any parse, checksum, ownership
// or structural failure is reported as a CorruptError.
//
// Ownership: the file belongs to exactly one node — loc.owner — and every
// record in it must declare that same node id. A file whose records name
// another node (for example another node's file copied over this one, format
// and checksum still intact) is not that node's telemetry: the whole file is
// refused as corrupt, no matter where the foreign record sits in the history
// and even if the latest record happens to name the right node.
//
// The two storage layouts prove ownership differently but enforce the exact
// same rule. A legacy file under nodes/ is named hex(owner), so its owner is
// decoded from the file name itself; that decoded id must also equal the id
// being queried. A slot file under slots/ has a hash for a name that reveals
// no node id, so its owner is the exact id whose slot this is — the full text
// the user queried or submitted, never a truncated or normalised form.
// Copying another node's complete file into a slot therefore fails just as
// copying it onto a legacy file name does.
//
// The file is decoded with the same strict rules as submit input: every
// telemetry field must be present, non-null and written once per record. A
// checksum that matches the zero-filled interpretation of a tampered record
// therefore cannot make a missing value look like real telemetry — the
// record is rejected before the checksum is consulted.
func loadNodeFile(loc nodeLocation) (*nodeFile, error) {
	path := loc.path
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, &CorruptError{Path: path, Err: err}
	}
	nf, err := decodeStrictNodeFile(data)
	if err != nil {
		return nil, &CorruptError{Path: path, Err: err}
	}
	if nf.Format != formatMarker {
		return nil, &CorruptError{Path: path, Err: fmt.Errorf("unknown format marker %q", nf.Format)}
	}
	if err := validateStoredRecords(nf.Records); err != nil {
		return nil, &CorruptError{Path: path, Err: err}
	}
	if got := checksumRecords(nf.Records); got != nf.Checksum {
		return nil, &CorruptError{Path: path, Err: fmt.Errorf("checksum mismatch: stored %s, computed %s", nf.Checksum, got)}
	}
	owner := loc.owner
	if loc.named {
		namedOwner, err := nodeOwnerFromPath(path)
		if err != nil {
			return nil, &CorruptError{Path: path, Err: err}
		}
		// The queried id and the file name must name the same owner.
		if namedOwner != owner {
			return nil, &CorruptError{Path: path, Err: fmt.Errorf(
				"file name encodes node %q, but it was loaded for node %q",
				namedOwner, owner)}
		}
	}
	for i, r := range nf.Records {
		if r.NodeID != owner {
			return nil, &CorruptError{Path: path, Err: fmt.Errorf(
				"record %d: heartbeat declares node %q, but this file belongs to node %q",
				i+1, r.NodeID, owner)}
		}
	}
	return nf, nil
}

// decodeStrictNodeFile parses the on-disk envelope without letting
// encoding/json defaults hide missing or duplicated data. The envelope has
// exactly three keys — format, checksum, records — each present, non-null and
// written once; the uniqueness judgement is the shared objectFields rule, so
// a Unicode-escaped spelling of the same key collides exactly as it does in a
// submitted heartbeat object. Every element of records is decoded with
// decodeStrictHeartbeatObject, so a missing, null or duplicated telemetry
// field is an error naming the record and field rather than a zero value.
// Whitespace and key ordering carry no meaning and never cause a failure.
func decodeStrictNodeFile(data []byte) (*nodeFile, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	tok, err := dec.Token()
	if err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}
	if d, ok := tok.(json.Delim); !ok || d != '{' {
		return nil, fmt.Errorf("invalid JSON: file must be a JSON object")
	}

	var (
		format   string
		checksum string
		records  []json.RawMessage
	)
	fields := objectFields{}
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return nil, fmt.Errorf("invalid JSON: %w", err)
		}
		key, ok := keyTok.(string)
		if !ok {
			return nil, fmt.Errorf("invalid JSON: envelope field name must be a string")
		}
		var val json.RawMessage
		if err := dec.Decode(&val); err != nil {
			return nil, fmt.Errorf("invalid value for envelope field %q: %w", key, err)
		}
		if fields.check(key) {
			return nil, fmt.Errorf("duplicate envelope field %q", key)
		}
		switch key {
		case "format":
			if bytes.Equal(val, []byte("null")) {
				return nil, fmt.Errorf("envelope field %q must not be null", key)
			}
			if err := json.Unmarshal(val, &format); err != nil {
				return nil, fmt.Errorf("format must be a string: %w", err)
			}
		case "checksum":
			if bytes.Equal(val, []byte("null")) {
				return nil, fmt.Errorf("envelope field %q must not be null", key)
			}
			if err := json.Unmarshal(val, &checksum); err != nil {
				return nil, fmt.Errorf("checksum must be a string: %w", err)
			}
		case "records":
			if bytes.Equal(val, []byte("null")) {
				return nil, fmt.Errorf("envelope field %q must not be null", key)
			}
			if err := json.Unmarshal(val, &records); err != nil {
				return nil, fmt.Errorf("records must be an array of heartbeat objects: %w", err)
			}
		default:
			return nil, fmt.Errorf("unknown envelope field %q", key)
		}
	}
	// Consume the closing brace.
	if _, err := dec.Token(); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}
	// Nothing may follow the single envelope object.
	if _, err := dec.Token(); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("invalid JSON: unexpected data after the envelope object")
		}
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}

	if !fields.has("format") {
		return nil, fmt.Errorf("missing required envelope field %q", "format")
	}
	if !fields.has("checksum") {
		return nil, fmt.Errorf("missing required envelope field %q", "checksum")
	}
	if !fields.has("records") {
		return nil, fmt.Errorf("missing required envelope field %q", "records")
	}

	nf := &nodeFile{Format: format, Checksum: checksum, Records: make([]Heartbeat, 0, len(records))}
	for i, raw := range records {
		h, err := decodeStrictHeartbeatObject(raw)
		if err != nil {
			return nil, fmt.Errorf("record %d: %w", i+1, err)
		}
		nf.Records = append(nf.Records, h)
	}
	return nf, nil
}

// validateStoredRecords checks the structural invariants of a strictly
// decoded record set: each record must satisfy the same field-value rules as
// submit input (one shared rule set — see checkHeartbeatValues), and records
// must be sorted by ascending seq with no duplicate seq. The stored-data
// wording and 1-based position prefix are kept by storedError. Position
// numbers are 1-based to match decoder errors.
func validateStoredRecords(records []Heartbeat) error {
	for i, r := range records {
		if e := checkHeartbeatValues(r); e != nil {
			return e.storedError(i + 1)
		}
		if i > 0 && records[i-1].Seq >= r.Seq {
			return fmt.Errorf("records not sorted by ascending seq at record %d", i+1)
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

// mergeRecord applies the single dedup/conflict rule shared by both submit
// phases (records within the batch, and the batch against stored history): a
// record's identity is its node id and seq together, and known must contain
// only records of that same node. If known already holds the identity, the
// incoming record is a duplicate when Heartbeat.Equal says the content is
// identical — collection time compared as an instant, node id and version
// compared as-is — and a conflict when any field differs. A conflict is an
// error naming the node and seq and changes nothing; a duplicate leaves
// known untouched and reports dup. Otherwise the record is appended and
// returned in the updated slice.
func mergeRecord(known []Heartbeat, r Heartbeat) (updated []Heartbeat, dup bool, err error) {
	for _, e := range known {
		if e.Seq == r.Seq {
			if !e.Equal(r) {
				return nil, false, fmt.Errorf("conflicting record for node %q seq %d: content differs", r.NodeID, r.Seq)
			}
			return known, true, nil
		}
	}
	return append(known, r), false, nil
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

	// Deduplicate within the batch: same node+seq with equal content counts
	// as a duplicate; same node+seq with differing content is a conflict.
	merged := make(map[string][]Heartbeat)
	for _, r := range records {
		list, dup, err := mergeRecord(merged[r.NodeID], r)
		if err != nil {
			return 0, 0, err
		}
		if dup {
			dupCount++
			continue
		}
		merged[r.NodeID] = list
	}

	// Merge with stored data per node, collecting new file contents.
	type update struct {
		loc     nodeLocation
		records []Heartbeat
	}
	var updates []update
	for nodeID, batch := range merged {
		loc := s.nodeLocationFor(nodeID)
		nf, err := loadNodeFile(loc)
		if err != nil {
			return 0, 0, err
		}
		existing := []Heartbeat{}
		if nf != nil {
			existing = nf.Records
		}
		for _, r := range batch {
			var dup bool
			existing, dup, err = mergeRecord(existing, r)
			if err != nil {
				return 0, 0, err
			}
			if dup {
				dupCount++
			} else {
				newCount++
			}
		}
		sort.Slice(existing, func(i, j int) bool { return existing[i].Seq < existing[j].Seq })
		updates = append(updates, update{loc: loc, records: existing})
	}

	// Persist. All updates are appends to previously verified data, so a
	// failure here leaves every previously queryable record intact.
	for _, u := range updates {
		if err := writeNodeFile(u.loc.path, u.records); err != nil {
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

	nf, err := loadNodeFile(s.nodeLocationFor(nodeID))
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
		// tolerance. Evaluate holds this rule — and the shared online/version
		// rules — in exactly one place for the demo and both query paths.
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

	// Baseline judgement: the online and version-skew findings come from the
	// same shared rules as the cumulative query above; only the missed-duty
	// statistic differs — it counts duties missed after the baseline record
	// instead of the latest cumulative total.
	findings := append([]string{}, livenessVersionFindings(online, latest.Version, expectedVersion)...)

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
			findings = append(findings, missedAboveToleranceFinding)
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

	nf, err := loadNodeFile(s.nodeLocationFor(nodeID))
	if err != nil {
		return nil, err
	}
	if nf == nil {
		return []Heartbeat{}, nil
	}
	return nf.Records, nil
}
