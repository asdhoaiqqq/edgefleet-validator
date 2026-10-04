package edgefleet

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"
)

// Heartbeat is one telemetry record reported by a node.
//
// JSON input uses snake_case field names:
//
//	node          string   node id (required, non-empty)
//	seq           integer  sequence number (required, > 0)
//	collected_at  string   RFC3339 with timezone, e.g. 2026-10-01T12:00:00+08:00 (required)
//	version       string   version (required, non-empty)
//	height        integer  block height (required, >= 0)
//	missed        integer  cumulative missed duties (required, >= 0)
type Heartbeat struct {
	NodeID      string    `json:"node"`
	Seq         int64     `json:"seq"`
	CollectedAt time.Time `json:"collected_at"`
	Version     string    `json:"version"`
	Height      int64     `json:"height"`
	Missed      int64     `json:"missed"`
}

// heartbeatFields enumerates every field that must be explicitly provided.
var heartbeatFields = []string{"node", "seq", "collected_at", "version", "height", "missed"}

// ValidateHeartbeat checks the semantic rules for one record. receiveTime is
// the moment the platform received the batch; a collection time later than
// receive time is rejected.
func ValidateHeartbeat(h Heartbeat, receiveTime time.Time) error {
	if h.NodeID == "" {
		return fmt.Errorf("node id is empty")
	}
	if h.Seq <= 0 {
		return fmt.Errorf("seq must be a positive integer, got %d", h.Seq)
	}
	if h.Version == "" {
		return fmt.Errorf("version is empty")
	}
	if h.Height < 0 {
		return fmt.Errorf("height must be >= 0, got %d", h.Height)
	}
	if h.Missed < 0 {
		return fmt.Errorf("missed must be >= 0, got %d", h.Missed)
	}
	if h.CollectedAt.IsZero() {
		return fmt.Errorf("collected_at is missing or invalid")
	}
	if h.CollectedAt.After(receiveTime) {
		return fmt.Errorf("collected_at %s is later than receive time %s",
			h.CollectedAt.Format(time.RFC3339), receiveTime.Format(time.RFC3339))
	}
	return nil
}

// Equal reports whether two records are the same telemetry. Collection times
// are compared as instants, so the same moment expressed in different
// timezones compares equal.
func (h Heartbeat) Equal(o Heartbeat) bool {
	return h.NodeID == o.NodeID &&
		h.Seq == o.Seq &&
		h.Version == o.Version &&
		h.Height == o.Height &&
		h.Missed == o.Missed &&
		h.CollectedAt.Equal(o.CollectedAt)
}

// heartbeatIdentity identifies one record independently of where it comes
// from: the node id and seq together. Different nodes sharing a seq, or the
// same node using different seqs, are different identities. Both components
// are compared as their exact values — node ids are never trimmed, cased or
// merged. Only batch/history classification uses it; Heartbeat.Equal stays the
// single full-content comparison.
type heartbeatIdentity struct {
	NodeID string
	Seq    int64
}

func identityOf(h Heartbeat) heartbeatIdentity {
	return heartbeatIdentity{NodeID: h.NodeID, Seq: h.Seq}
}

// ConflictError means two heartbeats carry the same identity (node id and
// seq) but differ in content — version, height, cumulative missed count or
// collection instant. It is returned whether the conflict is between two
// records of one submitted batch or between a submitted record and stored
// history; InBatch reports which. A conflict always fails the whole batch and
// never overwrites either record.
type ConflictError struct {
	NodeID  string
	Seq     int64
	InBatch bool
}

func (e *ConflictError) Error() string {
	if e.InBatch {
		return fmt.Sprintf("conflicting records for node %q seq %d in batch: content differs", e.NodeID, e.Seq)
	}
	return fmt.Sprintf("conflicting record for node %q seq %d: content differs from stored record", e.NodeID, e.Seq)
}

// IsConflict reports whether err is (or wraps) a ConflictError.
func IsConflict(err error) bool {
	var ce *ConflictError
	return errors.As(err, &ce)
}

// foldedBatch is one batch reduced to a single record per identity, in first
// appearance order. dupCount counts the extra occurrences removed while
// folding (batch-internal duplicates).
type foldedBatch struct {
	first map[heartbeatIdentity]Heartbeat
	order []heartbeatIdentity
	dup   int
}

// foldBatch applies the duplicate/conflict rule within one batch. It is
// phase 1 of mergeHeartbeats and deliberately needs no stored data, so a
// batch that contradicts itself is rejected on those grounds before any
// history file is consulted — matching the submit order where an in-batch
// conflict takes precedence over an unrelated node's corrupt history.
func foldBatch(batch []Heartbeat) (foldedBatch, error) {
	fb := foldedBatch{first: make(map[heartbeatIdentity]Heartbeat, len(batch))}
	for _, r := range batch {
		id := identityOf(r)
		if established, ok := fb.first[id]; ok {
			if !established.Equal(r) {
				return foldedBatch{}, &ConflictError{NodeID: r.NodeID, Seq: r.Seq, InBatch: true}
			}
			fb.dup++
			continue
		}
		fb.first[id] = r
		fb.order = append(fb.order, id)
	}
	return fb, nil
}

// mergeFolded applies the duplicate/conflict rule between a folded batch and
// saved history — phase 2 of mergeHeartbeats. The identity lookup and the
// Equal comparison are the same primitives foldBatch uses, so intra-batch and
// against-history judgements can never drift apart.
//
// history holds the currently saved records grouped by node id and is assumed
// unique per identity (the store verifies that invariant on load). It is read
// only: its records are copied into the returned map, which owns all its
// slices. Every node's result stays ascending by seq with gaps preserved, so
// inserting a late, smaller seq never displaces the max-seq record health
// queries use.
func mergeFolded(fb foldedBatch, history map[string][]Heartbeat) (merged map[string][]Heartbeat, newCount, dupCount int, err error) {
	merged = make(map[string][]Heartbeat, len(history)+1)
	known := make(map[heartbeatIdentity]Heartbeat, len(fb.order))
	for nodeID, records := range history {
		merged[nodeID] = append([]Heartbeat(nil), records...)
		for _, e := range records {
			known[identityOf(e)] = e
		}
	}
	for _, id := range fb.order {
		r := fb.first[id]
		if saved, ok := known[id]; ok {
			if !saved.Equal(r) {
				return nil, 0, 0, &ConflictError{NodeID: id.NodeID, Seq: id.Seq, InBatch: false}
			}
			dupCount++
			continue
		}
		merged[id.NodeID] = append(merged[id.NodeID], r)
		newCount++
	}
	for nodeID, records := range merged {
		sort.Slice(records, func(i, j int) bool { return records[i].Seq < records[j].Seq })
		merged[nodeID] = records
	}
	return merged, newCount, dupCount + fb.dup, nil
}

// mergeHeartbeats classifies one submitted batch against the already saved
// history. The business judgement lives in exactly two shared primitives —
// heartbeatIdentity (node id and seq, compared as exact values) and
// Heartbeat.Equal (full content, with collection times compared as instants
// and integers compared exactly) — and both duplicate sources run through
// them:
//
//  1. foldBatch: within the batch, repeats of one identity must be identical;
//     equal repeats are duplicates counted per occurrence, a differing repeat
//     is an in-batch conflict.
//  2. mergeFolded: the one record the batch carries for each identity must
//     equal the saved record under that identity, else a conflict with
//     history; an identity absent from history is new exactly once.
//
// Counts therefore follow input occurrences, not distinct identities: a new
// identity arriving three times reports one new and two duplicates, and all
// three occurrences of an already saved identity are duplicates. Any conflict
// rejects the whole merge with nothing added.
func mergeHeartbeats(batch []Heartbeat, history map[string][]Heartbeat) (map[string][]Heartbeat, int, int, error) {
	fb, err := foldBatch(batch)
	if err != nil {
		return nil, 0, 0, err
	}
	return mergeFolded(fb, history)
}

// ParseHeartbeats parses a JSON array of heartbeat objects. Every field must
// be explicitly provided; missing or unknown fields are rejected with a
// specific reason.
func ParseHeartbeats(data []byte, receiveTime time.Time) ([]Heartbeat, error) {
	var raw []json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("input must be a JSON array of heartbeat objects: %w", err)
	}
	if len(raw) == 0 {
		return nil, fmt.Errorf("no heartbeat records provided")
	}
	records := make([]Heartbeat, 0, len(raw))
	for i, r := range raw {
		h, err := parseOneHeartbeat(r, receiveTime)
		if err != nil {
			return nil, fmt.Errorf("record %d: %w", i+1, err)
		}
		records = append(records, h)
	}
	return records, nil
}

// rejectNull reports an error when a required field's JSON value is null.
// A null value means the node did not report the field, which must not be
// silently saved as the zero value.
func rejectNull(raw json.RawMessage, field string) error {
	if bytes.Equal(raw, []byte("null")) {
		return fmt.Errorf("%s must not be null", field)
	}
	return nil
}

// decodeStrictHeartbeatObject converts one JSON object's raw bytes into a
// Heartbeat while enforcing the field rules at every trust boundary: all
// fields must be present and non-null, unknown fields are rejected, and a
// field may never appear twice in the same object — even with the same
// value, and even when one spelling uses a JSON Unicode escape denoting the
// same key. A plain struct/map unmarshal would instead keep the last
// occurrence and let an absent field masquerade as the zero value. Both the
// submit input path and the stored-file read path must use it.
func decodeStrictHeartbeatObject(raw json.RawMessage) (Heartbeat, error) {
	// Walk the object with a streaming decoder so duplicate field names are
	// detected. Field names are compared as the JSON string they represent:
	// "missed" and "m\u0069ssed" are the same field. A map-based unmarshal
	// would silently keep only the last occurrence.
	dec := json.NewDecoder(bytes.NewReader(raw))
	tok, err := dec.Token()
	if err != nil {
		return Heartbeat{}, fmt.Errorf("record must be a JSON object: %w", err)
	}
	if d, ok := tok.(json.Delim); !ok || d != '{' {
		return Heartbeat{}, fmt.Errorf("record must be a JSON object")
	}

	fields := make(map[string]json.RawMessage)
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return Heartbeat{}, fmt.Errorf("invalid field: %w", err)
		}
		key, ok := keyTok.(string)
		if !ok {
			return Heartbeat{}, fmt.Errorf("field name must be a string")
		}
		if _, exists := fields[key]; exists {
			return Heartbeat{}, fmt.Errorf("duplicate field %q", key)
		}
		var val json.RawMessage
		if err := dec.Decode(&val); err != nil {
			return Heartbeat{}, fmt.Errorf("invalid value for field %q: %w", key, err)
		}
		fields[key] = val
	}
	// Consume the closing brace.
	if _, err := dec.Token(); err != nil {
		return Heartbeat{}, fmt.Errorf("invalid record: %w", err)
	}

	for _, f := range heartbeatFields {
		if _, ok := fields[f]; !ok {
			return Heartbeat{}, fmt.Errorf("missing required field %q", f)
		}
	}
	for k := range fields {
		if !containsString(heartbeatFields, k) {
			return Heartbeat{}, fmt.Errorf("unknown field %q", k)
		}
	}

	var h Heartbeat

	if err := rejectNull(fields["node"], "node"); err != nil {
		return Heartbeat{}, err
	}
	if err := json.Unmarshal(fields["node"], &h.NodeID); err != nil {
		return Heartbeat{}, fmt.Errorf("node must be a string: %w", err)
	}
	if h.NodeID == "" {
		return Heartbeat{}, fmt.Errorf("node must not be empty")
	}

	if err := rejectNull(fields["seq"], "seq"); err != nil {
		return Heartbeat{}, err
	}
	if err := json.Unmarshal(fields["seq"], &h.Seq); err != nil {
		return Heartbeat{}, fmt.Errorf("seq must be an integer: %w", err)
	}
	if h.Seq <= 0 {
		return Heartbeat{}, fmt.Errorf("seq must be a positive integer, got %d", h.Seq)
	}

	if err := rejectNull(fields["collected_at"], "collected_at"); err != nil {
		return Heartbeat{}, err
	}
	var collectedStr string
	if err := json.Unmarshal(fields["collected_at"], &collectedStr); err != nil {
		return Heartbeat{}, fmt.Errorf("collected_at must be a string: %w", err)
	}
	t, err := time.Parse(time.RFC3339, collectedStr)
	if err != nil {
		return Heartbeat{}, fmt.Errorf("collected_at must be RFC3339 with timezone (e.g. 2026-10-01T12:00:00+08:00): %w", err)
	}
	h.CollectedAt = t

	if err := rejectNull(fields["version"], "version"); err != nil {
		return Heartbeat{}, err
	}
	if err := json.Unmarshal(fields["version"], &h.Version); err != nil {
		return Heartbeat{}, fmt.Errorf("version must be a string: %w", err)
	}
	if h.Version == "" {
		return Heartbeat{}, fmt.Errorf("version must not be empty")
	}

	if err := rejectNull(fields["height"], "height"); err != nil {
		return Heartbeat{}, err
	}
	if err := json.Unmarshal(fields["height"], &h.Height); err != nil {
		return Heartbeat{}, fmt.Errorf("height must be an integer: %w", err)
	}
	if h.Height < 0 {
		return Heartbeat{}, fmt.Errorf("height must be >= 0, got %d", h.Height)
	}

	if err := rejectNull(fields["missed"], "missed"); err != nil {
		return Heartbeat{}, err
	}
	if err := json.Unmarshal(fields["missed"], &h.Missed); err != nil {
		return Heartbeat{}, fmt.Errorf("missed must be an integer: %w", err)
	}
	if h.Missed < 0 {
		return Heartbeat{}, fmt.Errorf("missed must be >= 0, got %d", h.Missed)
	}

	return h, nil
}

// parseOneHeartbeat decodes one submit-input record and applies the
// submit-only rule that collection time cannot be later than receive time.
func parseOneHeartbeat(raw json.RawMessage, receiveTime time.Time) (Heartbeat, error) {
	h, err := decodeStrictHeartbeatObject(raw)
	if err != nil {
		return Heartbeat{}, err
	}
	if h.CollectedAt.After(receiveTime) {
		return Heartbeat{}, fmt.Errorf("collected_at %s is later than receive time %s",
			h.CollectedAt.Format(time.RFC3339), receiveTime.Format(time.RFC3339))
	}
	return h, nil
}

func containsString(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}
