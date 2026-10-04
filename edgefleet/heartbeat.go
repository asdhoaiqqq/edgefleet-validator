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

// fieldViolation names the one field whose value is invalid and which rule
// it breaks. It carries no wording of its own: every trust boundary renders
// the same violation with the message and location context its callers
// expect, while the rule itself lives only here.
type fieldViolation struct {
	field string
	kind  violationKind
	value int64
}

type violationKind int

const (
	violationEmpty violationKind = iota
	violationNotUnicode
	violationNotPositive
	violationNegative
	violationMissingTime
)

// fieldValidators is the single source of truth for what makes a heartbeat
// field value legal. Field order defines both the check order at every
// boundary and the order in which a record with several bad values reports
// them. Node text rules (empty, valid Unicode) apply to the decoded string;
// the raw JSON token is checked separately before decoding because
// encoding/json rewrites invalid bytes and unpaired surrogates to U+FFFD.
var fieldValidators = []struct {
	field string
	check func(Heartbeat) (violationKind, int64, bool)
}{
	{"node", func(h Heartbeat) (violationKind, int64, bool) {
		if h.NodeID == "" {
			return violationEmpty, 0, false
		}
		if !utf8.ValidString(h.NodeID) {
			return violationNotUnicode, 0, false
		}
		return 0, 0, true
	}},
	{"seq", func(h Heartbeat) (violationKind, int64, bool) {
		if h.Seq <= 0 {
			return violationNotPositive, h.Seq, false
		}
		return 0, 0, true
	}},
	{"version", func(h Heartbeat) (violationKind, int64, bool) {
		if h.Version == "" {
			return violationEmpty, 0, false
		}
		return 0, 0, true
	}},
	{"height", func(h Heartbeat) (violationKind, int64, bool) {
		if h.Height < 0 {
			return violationNegative, h.Height, false
		}
		return 0, 0, true
	}},
	{"missed", func(h Heartbeat) (violationKind, int64, bool) {
		if h.Missed < 0 {
			return violationNegative, h.Missed, false
		}
		return 0, 0, true
	}},
	{"collected_at", func(h Heartbeat) (violationKind, int64, bool) {
		if h.CollectedAt.IsZero() {
			return violationMissingTime, 0, false
		}
		return 0, 0, true
	}},
}

// checkHeartbeatFields applies the shared field-value rules and returns the
// first violation, or nil when every field value is legal. It does not know
// whether the record came from JSON submit input, a direct Go caller or a
// saved file; the receive-time rule and all raw-JSON/file checks live at
// those boundaries instead.
func checkHeartbeatFields(h Heartbeat) *fieldViolation {
	for _, v := range fieldValidators {
		if kind, value, ok := v.check(h); !ok {
			return &fieldViolation{field: v.field, kind: kind, value: value}
		}
	}
	return nil
}

// submitFieldError renders a violation with the wording of the submit
// boundaries: ValidateHeartbeat for directly constructed records and the
// strict JSON decoder for parsed records share these phrasings.
func submitFieldError(v fieldViolation) error {
	switch v.kind {
	case violationEmpty:
		if v.field == "node" {
			return fmt.Errorf("node id is empty")
		}
		return fmt.Errorf("%s is empty", v.field)
	case violationNotUnicode:
		return fmt.Errorf("node id is not valid Unicode text: contains invalid UTF-8")
	case violationNotPositive:
		return fmt.Errorf("%s must be a positive integer, got %d", v.field, v.value)
	case violationNegative:
		return fmt.Errorf("%s must be >= 0, got %d", v.field, v.value)
	case violationMissingTime:
		return fmt.Errorf("collected_at is missing or invalid")
	default:
		return fmt.Errorf("%s is invalid", v.field)
	}
}

// storedFieldError renders a violation the way stored-file validation
// reports it: record position is added by the caller and the message names
// the field in quotes.
func storedFieldError(v fieldViolation) error {
	switch v.kind {
	case violationEmpty:
		return fmt.Errorf("field %q must not be empty", v.field)
	case violationNotPositive:
		return fmt.Errorf("field %q must be a positive integer, got %d", v.field, v.value)
	case violationNegative:
		return fmt.Errorf("field %q must be >= 0, got %d", v.field, v.value)
	case violationMissingTime:
		return fmt.Errorf("field %q is missing or invalid", v.field)
	default:
		return fmt.Errorf("field %q is invalid", v.field)
	}
}

// fieldValueViolation runs just one named field's shared value rule. The
// strict decoder uses it field by field as each value is decoded, so a bad
// value is reported at the same point as before rather than only after the
// whole object has been decoded.
func fieldValueViolation(name string, h Heartbeat) *fieldViolation {
	for _, v := range fieldValidators {
		if v.field == name {
			if kind, value, ok := v.check(h); !ok {
				return &fieldViolation{field: name, kind: kind, value: value}
			}
			return nil
		}
	}
	return nil
}

// decodeFieldError renders a violation with the strict JSON decoder's
// wording. violationNotUnicode cannot occur here — invalid raw node text is
// rejected by validateRawNodeText before decoding and json.Unmarshal only
// yields valid strings — and neither can violationMissingTime, because a
// bad collected_at value fails time.Parse instead. Both are handled anyway
// so the renderer stays exhaustive.
func decodeFieldError(v fieldViolation) error {
	switch v.kind {
	case violationEmpty:
		return fmt.Errorf("%s must not be empty", v.field)
	case violationNotUnicode:
		return fmt.Errorf("node contains invalid UTF-8")
	case violationNotPositive:
		return fmt.Errorf("%s must be a positive integer, got %d", v.field, v.value)
	case violationNegative:
		return fmt.Errorf("%s must be >= 0, got %d", v.field, v.value)
	case violationMissingTime:
		return fmt.Errorf("collected_at is missing or invalid")
	default:
		return fmt.Errorf("%s is invalid", v.field)
	}
}

// checkCollectedAtReceiveTime is the submit-only rule: a record collected
// later than the moment the platform received its batch is rejected. Equal
// instants are allowed. It is deliberately separate from
// checkHeartbeatFields — saved history has no receive time and must not be
// judged against the current clock.
func checkCollectedAtReceiveTime(h Heartbeat, receiveTime time.Time) error {
	if h.CollectedAt.After(receiveTime) {
		return fmt.Errorf("collected_at %s is later than receive time %s",
			h.CollectedAt.Format(time.RFC3339), receiveTime.Format(time.RFC3339))
	}
	return nil
}

// ValidateHeartbeat checks the semantic rules for one directly constructed
// record. receiveTime is the moment the platform received the batch; a
// collection time later than receive time is rejected.
func ValidateHeartbeat(h Heartbeat, receiveTime time.Time) error {
	if v := checkHeartbeatFields(h); v != nil {
		return submitFieldError(*v)
	}
	return checkCollectedAtReceiveTime(h, receiveTime)
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

	// Each field keeps its own boundary order — null check, type decode,
	// raw-token/format check — while the value rules themselves come from
	// the single shared set in fieldValidators. Rendering uses the strict
	// decoder's wording ("X must not be empty"), which both the submit and
	// stored-file paths already share through this function.
	if err := rejectNull(fields["node"], "node"); err != nil {
		return Heartbeat{}, err
	}
	if err := json.Unmarshal(fields["node"], &h.NodeID); err != nil {
		return Heartbeat{}, fmt.Errorf("node must be a string: %w", err)
	}
	if err := validateRawNodeText(fields["node"]); err != nil {
		return Heartbeat{}, err
	}
	if v := fieldValueViolation("node", h); v != nil {
		return Heartbeat{}, decodeFieldError(*v)
	}

	if err := rejectNull(fields["seq"], "seq"); err != nil {
		return Heartbeat{}, err
	}
	if err := json.Unmarshal(fields["seq"], &h.Seq); err != nil {
		return Heartbeat{}, fmt.Errorf("seq must be an integer: %w", err)
	}
	if v := fieldValueViolation("seq", h); v != nil {
		return Heartbeat{}, decodeFieldError(*v)
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
	if v := fieldValueViolation("version", h); v != nil {
		return Heartbeat{}, decodeFieldError(*v)
	}

	if err := rejectNull(fields["height"], "height"); err != nil {
		return Heartbeat{}, err
	}
	if err := json.Unmarshal(fields["height"], &h.Height); err != nil {
		return Heartbeat{}, fmt.Errorf("height must be an integer: %w", err)
	}
	if v := fieldValueViolation("height", h); v != nil {
		return Heartbeat{}, decodeFieldError(*v)
	}

	if err := rejectNull(fields["missed"], "missed"); err != nil {
		return Heartbeat{}, err
	}
	if err := json.Unmarshal(fields["missed"], &h.Missed); err != nil {
		return Heartbeat{}, fmt.Errorf("missed must be an integer: %w", err)
	}
	if v := fieldValueViolation("missed", h); v != nil {
		return Heartbeat{}, decodeFieldError(*v)
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
	if err := checkCollectedAtReceiveTime(h, receiveTime); err != nil {
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
