package edgefleet

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
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

func parseOneHeartbeat(raw json.RawMessage, receiveTime time.Time) (Heartbeat, error) {
	// Parse the object token by token instead of via map[string]RawMessage:
	// that map silently keeps the last value for a repeated key (and decodes
	// \uXXXX escapes first), neither of which is acceptable here.
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()

	tok, err := dec.Token()
	if err != nil {
		return Heartbeat{}, fmt.Errorf("record must be a JSON object: %w", err)
	}
	if d, ok := tok.(json.Delim); !ok || d != '{' {
		return Heartbeat{}, fmt.Errorf("record must be a JSON object")
	}

	values := make(map[string]json.RawMessage, len(heartbeatFields))
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return Heartbeat{}, fmt.Errorf("invalid heartbeat object: %w", err)
		}
		key, ok := keyTok.(string)
		if !ok {
			return Heartbeat{}, fmt.Errorf("invalid heartbeat object: field name must be a string")
		}
		// Field names are identified by their decoded JSON text, so "missed"
		// written as "missed" names the same field and repeats it.
		if _, seen := values[key]; seen {
			return Heartbeat{}, fmt.Errorf("duplicate field %q in record", key)
		}
		var value json.RawMessage
		if err := dec.Decode(&value); err != nil {
			return Heartbeat{}, fmt.Errorf("invalid value for field %q: %w", key, err)
		}
		values[key] = value
	}
	if _, err := dec.Token(); err != nil { // closing '}'
		return Heartbeat{}, fmt.Errorf("invalid heartbeat object: %w", err)
	}
	if _, err := dec.Token(); err != io.EOF {
		return Heartbeat{}, fmt.Errorf("invalid heartbeat object: trailing content after object")
	}

	for _, f := range heartbeatFields {
		v, ok := values[f]
		if !ok {
			return Heartbeat{}, fmt.Errorf("missing required field %q", f)
		}
		if isJSONNull(v) {
			return Heartbeat{}, fmt.Errorf("field %q must not be null", f)
		}
	}
	for k := range values {
		if !containsString(heartbeatFields, k) {
			return Heartbeat{}, fmt.Errorf("unknown field %q", k)
		}
	}

	var h Heartbeat

	if err := json.Unmarshal(values["node"], &h.NodeID); err != nil {
		return Heartbeat{}, fmt.Errorf("node must be a string: %w", err)
	}
	if h.NodeID == "" {
		return Heartbeat{}, fmt.Errorf("node must not be empty")
	}

	if err := decodeInt(values["seq"], &h.Seq); err != nil {
		return Heartbeat{}, fmt.Errorf("seq must be an integer: %w", err)
	}
	if h.Seq <= 0 {
		return Heartbeat{}, fmt.Errorf("seq must be a positive integer, got %d", h.Seq)
	}

	var collectedStr string
	if err := json.Unmarshal(values["collected_at"], &collectedStr); err != nil {
		return Heartbeat{}, fmt.Errorf("collected_at must be a string: %w", err)
	}
	t, err := time.Parse(time.RFC3339, collectedStr)
	if err != nil {
		return Heartbeat{}, fmt.Errorf("collected_at must be RFC3339 with timezone (e.g. 2026-10-01T12:00:00+08:00): %w", err)
	}
	h.CollectedAt = t

	if err := json.Unmarshal(values["version"], &h.Version); err != nil {
		return Heartbeat{}, fmt.Errorf("version must be a string: %w", err)
	}
	if h.Version == "" {
		return Heartbeat{}, fmt.Errorf("version must not be empty")
	}

	if err := decodeInt(values["height"], &h.Height); err != nil {
		return Heartbeat{}, fmt.Errorf("height must be an integer: %w", err)
	}
	if h.Height < 0 {
		return Heartbeat{}, fmt.Errorf("height must be >= 0, got %d", h.Height)
	}

	if err := decodeInt(values["missed"], &h.Missed); err != nil {
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

// isJSONNull reports whether a raw JSON value is exactly null.
func isJSONNull(v json.RawMessage) bool {
	return string(bytes.TrimSpace(v)) == "null"
}

// decodeInt decodes a JSON value into an int64, accepting only integer
// numbers: floats, booleans, strings and null are rejected, as are values
// outside the int64 range. A genuine 0 decodes to 0 like any other value.
func decodeInt(raw json.RawMessage, dst *int64) error {
	var n json.Number
	if err := json.Unmarshal(raw, &n); err != nil {
		return err
	}
	parsed, err := n.Int64()
	if err != nil {
		return fmt.Errorf("not an integer: %w", err)
	}
	*dst = parsed
	return nil
}

func containsString(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}
