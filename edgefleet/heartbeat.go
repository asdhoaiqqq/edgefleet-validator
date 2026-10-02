package edgefleet

import (
	"bytes"
	"encoding/json"
	"fmt"
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

func parseOneHeartbeat(raw json.RawMessage, receiveTime time.Time) (Heartbeat, error) {
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
