package edgefleet

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"
	"unicode/utf8"
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

// fieldIssue classifies the way a field value violates a rule. Together with
// the field name and the offending numeric value it identifies the rule that
// failed, without carrying any wording. Each trust boundary renders the
// failure in its own message style and locates it in its own context, but the
// rules themselves — which values are legal — live in the check* functions
// below exactly once.
type fieldIssue int

const (
	issueEmpty          fieldIssue = iota // a required string is ""
	issueInvalidUnicode                   // node id is not lossless text
	issueNotPositive                      // integer must be > 0
	issueNegative                         // integer must be >= 0
	issueMissingInstant                   // collected_at is the zero instant
)

// fieldValueError names the field whose value rule was violated.
type fieldValueError struct {
	field string
	issue fieldIssue
	value int64
}

// checkNodeIDValue holds the single node-id value rule: non-empty, valid
// Unicode text. The raw-JSON boundary additionally checks the un-decoded
// token (see validateRawNodeText); for a value already decoded from such a
// token the Unicode branch can never fire, but both submit entries and the
// stored-data read enforce the same rule through this one function.
func checkNodeIDValue(node string) *fieldValueError {
	if node == "" {
		return &fieldValueError{field: "node", issue: issueEmpty}
	}
	if !utf8.ValidString(node) {
		return &fieldValueError{field: "node", issue: issueInvalidUnicode}
	}
	return nil
}

// checkNonEmptyString holds the single non-empty-string value rule, shared by
// every field that uses it (currently version).
func checkNonEmptyString(field, value string) *fieldValueError {
	if value == "" {
		return &fieldValueError{field: field, issue: issueEmpty}
	}
	return nil
}

// checkPositiveInt holds the single positive-integer rule (seq).
func checkPositiveInt(field string, value int64) *fieldValueError {
	if value <= 0 {
		return &fieldValueError{field: field, issue: issueNotPositive, value: value}
	}
	return nil
}

// checkNonNegativeInt holds the single non-negative-integer rule (height,
// missed). An explicit zero is genuine telemetry and passes.
func checkNonNegativeInt(field string, value int64) *fieldValueError {
	if value < 0 {
		return &fieldValueError{field: field, issue: issueNegative, value: value}
	}
	return nil
}

// checkCollectedAtValue holds the single rule that a collection instant must
// actually be present (not the zero time).
func checkCollectedAtValue(collected time.Time) *fieldValueError {
	if collected.IsZero() {
		return &fieldValueError{field: "collected_at", issue: issueMissingInstant}
	}
	return nil
}

// checkHeartbeatValues runs every context-free field-value rule in the single
// rejection order shared by the direct-submit and stored-data boundaries:
// node, seq, version, height, missed, then collected_at. It deliberately does
// NOT compare against a receive time: reading saved history has no receive
// time, and that submit-only rule lives in checkCollectedAtTiming.
func checkHeartbeatValues(h Heartbeat) *fieldValueError {
	if e := checkNodeIDValue(h.NodeID); e != nil {
		return e
	}
	if e := checkPositiveInt("seq", h.Seq); e != nil {
		return e
	}
	if e := checkNonEmptyString("version", h.Version); e != nil {
		return e
	}
	if e := checkNonNegativeInt("height", h.Height); e != nil {
		return e
	}
	if e := checkNonNegativeInt("missed", h.Missed); e != nil {
		return e
	}
	return checkCollectedAtValue(h.CollectedAt)
}

// checkCollectedAtTiming holds the single receive-time rule, which belongs to
// submission only: a collection time later than the receive time is refused;
// the same instant expressed in another timezone is equal and allowed. The
// stored-data read path never calls this, so reading history introduces no
// current time and no extra time limit.
func checkCollectedAtTiming(collected, receiveTime time.Time) error {
	if collected.After(receiveTime) {
		return fmt.Errorf("collected_at %s is later than receive time %s",
			collected.Format(time.RFC3339), receiveTime.Format(time.RFC3339))
	}
	return nil
}

// directError renders a field rule failure in ValidateHeartbeat's wording,
// used when a caller submits an already constructed Heartbeat.
func (e *fieldValueError) directError() error {
	switch {
	case e.field == "node" && e.issue == issueEmpty:
		return fmt.Errorf("node id is empty")
	case e.field == "node" && e.issue == issueInvalidUnicode:
		return fmt.Errorf("node id is not valid Unicode text: contains invalid UTF-8")
	case e.field == "seq":
		return fmt.Errorf("seq must be a positive integer, got %d", e.value)
	case e.field == "version":
		return fmt.Errorf("version is empty")
	case e.field == "height":
		return fmt.Errorf("height must be >= 0, got %d", e.value)
	case e.field == "missed":
		return fmt.Errorf("missed must be >= 0, got %d", e.value)
	case e.field == "collected_at":
		return fmt.Errorf("collected_at is missing or invalid")
	default:
		return fmt.Errorf("field %q has an invalid value", e.field)
	}
}

// decodeError renders a field rule failure in the wording used while decoding
// a strict JSON object (submit input and stored records alike).
func (e *fieldValueError) decodeError() error {
	switch {
	case e.field == "node" && e.issue == issueInvalidUnicode:
		return fmt.Errorf("node id is not valid Unicode text: contains invalid UTF-8")
	case e.field == "node":
		return fmt.Errorf("node must not be empty")
	case e.field == "seq":
		return fmt.Errorf("seq must be a positive integer, got %d", e.value)
	case e.field == "version":
		return fmt.Errorf("version must not be empty")
	case e.field == "height":
		return fmt.Errorf("height must be >= 0, got %d", e.value)
	case e.field == "missed":
		return fmt.Errorf("missed must be >= 0, got %d", e.value)
	default:
		return fmt.Errorf("invalid value for field %q", e.field)
	}
}

// storedError renders a field rule failure for a record read from a saved
// file, prefixing the 1-based record position as stored-data errors do.
func (e *fieldValueError) storedError(position int) error {
	switch {
	case e.field == "node" && e.issue == issueInvalidUnicode:
		return fmt.Errorf("record %d: field %q is not valid Unicode text", position, e.field)
	case e.field == "node" || e.field == "version":
		return fmt.Errorf("record %d: field %q must not be empty", position, e.field)
	case e.field == "seq":
		return fmt.Errorf("record %d: field %q must be a positive integer, got %d", position, e.field, e.value)
	case e.field == "height" || e.field == "missed":
		return fmt.Errorf("record %d: field %q must be >= 0, got %d", position, e.field, e.value)
	case e.field == "collected_at":
		return fmt.Errorf("record %d: field %q is missing or invalid", position, e.field)
	default:
		return fmt.Errorf("record %d: field %q has an invalid value", position, e.field)
	}
}

// ValidateHeartbeat checks the semantic rules for one record. receiveTime is
// the moment the platform received the batch; a collection time later than
// receive time is rejected. The field-value rules are shared with JSON
// parsing and the stored-file read path via checkHeartbeatValues; only the
// receive-time comparison is submit-specific.
func ValidateHeartbeat(h Heartbeat, receiveTime time.Time) error {
	if e := checkHeartbeatValues(h); e != nil {
		return e.directError()
	}
	return checkCollectedAtTiming(h.CollectedAt, receiveTime)
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
	if err := validateRawNodeText(fields["node"]); err != nil {
		return Heartbeat{}, err
	}
	if e := checkNodeIDValue(h.NodeID); e != nil {
		return Heartbeat{}, e.decodeError()
	}

	if err := rejectNull(fields["seq"], "seq"); err != nil {
		return Heartbeat{}, err
	}
	if err := json.Unmarshal(fields["seq"], &h.Seq); err != nil {
		return Heartbeat{}, fmt.Errorf("seq must be an integer: %w", err)
	}
	if e := checkPositiveInt("seq", h.Seq); e != nil {
		return Heartbeat{}, e.decodeError()
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
	if e := checkNonEmptyString("version", h.Version); e != nil {
		return Heartbeat{}, e.decodeError()
	}

	if err := rejectNull(fields["height"], "height"); err != nil {
		return Heartbeat{}, err
	}
	if err := json.Unmarshal(fields["height"], &h.Height); err != nil {
		return Heartbeat{}, fmt.Errorf("height must be an integer: %w", err)
	}
	if e := checkNonNegativeInt("height", h.Height); e != nil {
		return Heartbeat{}, e.decodeError()
	}

	if err := rejectNull(fields["missed"], "missed"); err != nil {
		return Heartbeat{}, err
	}
	if err := json.Unmarshal(fields["missed"], &h.Missed); err != nil {
		return Heartbeat{}, fmt.Errorf("missed must be an integer: %w", err)
	}
	if e := checkNonNegativeInt("missed", h.Missed); e != nil {
		return Heartbeat{}, e.decodeError()
	}

	return h, nil
}

// validateRawNodeText checks the raw JSON string token of the node field
// before its decoded value is trusted. encoding/json silently rewrites
// invalid UTF-8 bytes and unpaired \u surrogate escapes to U+FFFD, which
// would merge distinct node ids into one replacement-char id and break the
// ownership check against stored files. The raw token must decode
// losslessly: every literal byte sequence must be valid UTF-8, and every \u
// escape must be a valid scalar value or a properly paired high/low
// surrogate pair. A literal "�" (or its \uFFFD escape) is valid text the
// user typed and is accepted; only undecodable input is rejected.
func validateRawNodeText(raw json.RawMessage) error {
	raw = bytes.TrimSpace(raw)
	if len(raw) < 2 || raw[0] != '"' || raw[len(raw)-1] != '"' {
		return fmt.Errorf("node must be a string")
	}
	s := raw[1 : len(raw)-1]
	for i := 0; i < len(s); {
		c := s[i]
		if c == '\\' {
			if i+1 >= len(s) {
				return fmt.Errorf("node contains a truncated escape")
			}
			if s[i+1] != 'u' {
				i += 2
				continue
			}
			if i+6 > len(s) {
				return fmt.Errorf("node contains a truncated \\u escape")
			}
			r, err := parseHex4(s[i+2 : i+6])
			if err != nil {
				return fmt.Errorf("node contains an invalid \\u escape: %w", err)
			}
			switch {
			case r >= 0xD800 && r <= 0xDBFF:
				// A high surrogate is only valid when immediately followed
				// by a low-surrogate \u escape.
				if i+12 > len(s) || s[i+6] != '\\' || s[i+7] != 'u' {
					return fmt.Errorf("node contains an unpaired high surrogate \\u%04X", r)
				}
				lo, err := parseHex4(s[i+8 : i+12])
				if err != nil {
					return fmt.Errorf("node contains an invalid \\u escape: %w", err)
				}
				if lo < 0xDC00 || lo > 0xDFFF {
					return fmt.Errorf("node contains an unpaired high surrogate \\u%04X", r)
				}
				i += 12
			case r >= 0xDC00 && r <= 0xDFFF:
				return fmt.Errorf("node contains an unpaired low surrogate \\u%04X", r)
			default:
				i += 6
			}
			continue
		}
		if c < utf8.RuneSelf {
			i++
			continue
		}
		// A valid multi-byte rune decodes with size >= 2; size 1 means the
		// bytes are not valid UTF-8 (including surrogate encodings).
		_, size := utf8.DecodeRune(s[i:])
		if size == 1 {
			return fmt.Errorf("node contains invalid UTF-8 at byte offset %d", i)
		}
		i += size
	}
	return nil
}

// parseHex4 parses exactly four hexadecimal digits as a 16-bit value.
func parseHex4(b []byte) (rune, error) {
	v := 0
	for _, c := range b {
		var d int
		switch {
		case '0' <= c && c <= '9':
			d = int(c - '0')
		case 'a' <= c && c <= 'f':
			d = int(c-'a') + 10
		case 'A' <= c && c <= 'F':
			d = int(c-'A') + 10
		default:
			return 0, fmt.Errorf("invalid hex digit %q", c)
		}
		v = v*16 + d
	}
	return rune(v), nil
}

// parseOneHeartbeat decodes one submit-input record and applies the
// submit-only rule that collection time cannot be later than receive time.
func parseOneHeartbeat(raw json.RawMessage, receiveTime time.Time) (Heartbeat, error) {
	h, err := decodeStrictHeartbeatObject(raw)
	if err != nil {
		return Heartbeat{}, err
	}
	if err := checkCollectedAtTiming(h.CollectedAt, receiveTime); err != nil {
		return Heartbeat{}, err
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
