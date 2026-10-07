package edgefleet

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

// Heartbeat is one telemetry record reported by a node.
//
// JSON input uses snake_case field names:
//
//	node          string   node id (required, non-empty)
//	seq           integer  sequence number (required, > 0)
//	collected_at  string   RFC3339 with timezone, e.g. 2026-10-01T12:00:00+08:00
//	                      (required; year 0000-9999, numeric offset hour 00-23
//	                      and minute 00-59 on the minute — +24:00 and a folded
//	                      offset such as +00:60 are rejected, not normalised;
//	                      a fractional second, written with either "." or ","
//	                      as its decimal separator, must be exactly
//	                      representable at nanosecond precision — more than
//	                      nine digits is accepted only when every digit past
//	                      the ninth is zero, never truncated or rounded)
//	version       string   version (required, non-empty, valid Unicode text)
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
	issueEmpty            fieldIssue = iota // a required string is ""
	issueInvalidUnicode                     // node id or version is not lossless text
	issueNotPositive                        // integer must be > 0
	issueNegative                           // integer must be >= 0
	issueMissingInstant                     // collected_at is the zero instant
	issueYearOutOfRange                     // collected_at year cannot be written by the saved format
	issueOffsetOutOfRange                   // collected_at timezone offset is not a whole minute within ±23:59
)

// fieldValueError names the field whose value rule was violated.
type fieldValueError struct {
	field string
	issue fieldIssue
	value int64
}

// checkNodeIDValue holds the single node-id value rule: non-empty, valid
// Unicode text. The raw-JSON boundary additionally checks the un-decoded
// token (see validateRawText); for a value already decoded from such a
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

// checkVersionValue holds the single version value rule: non-empty, valid
// Unicode text. The version is opaque text — Chinese, emoji and a literal "�"
// the user typed are all legal, and no format (digits and dots) is imposed —
// but it must be losslessly representable: a platform that silently rewrites
// invalid bytes to U+FFFD would merge distinct versions into one and make the
// version-skew judgement compare text the node never reported. The raw-JSON
// boundary additionally checks the un-decoded token (see validateRawText);
// for a value already decoded from such a token the Unicode branch can never
// fire, but both submit entries and the stored-data read enforce the same
// rule through this one function.
func checkVersionValue(version string) *fieldValueError {
	if version == "" {
		return &fieldValueError{field: "version", issue: issueEmpty}
	}
	if !utf8.ValidString(version) {
		return &fieldValueError{field: "version", issue: issueInvalidUnicode}
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

// checkCollectedAtValue holds the context-free collected_at rules shared by
// every boundary:
//
//   - the instant must actually be present (not the zero time);
//   - its year must be 0000..9999, the four-digit range the saved RFC3339
//     form can write — a year outside it cannot be persisted at all;
//   - its UTC offset must be a whole number of minutes no larger than
//     ±23:59, the offsets the saved form can write exactly. A ±24:00 offset
//     is outside the 00..23 hour bound, and a sub-minute offset (e.g.
//     +08:00:30) would silently land on disk as a different instant.
//
// Checking the decoded instant covers a Heartbeat built directly in Go;
// values decoded from JSON text additionally pass validateCollectedAtText,
// because time.Parse folds an out-of-range textual offset (+00:60 -> +01:00)
// before an instant ever exists to inspect.
func checkCollectedAtValue(collected time.Time) *fieldValueError {
	if collected.IsZero() {
		return &fieldValueError{field: "collected_at", issue: issueMissingInstant}
	}
	if year := collected.Year(); year < minSavedYear || year > maxSavedYear {
		return &fieldValueError{field: "collected_at", issue: issueYearOutOfRange, value: int64(year)}
	}
	if _, offset := collected.Zone(); !isWritableOffset(offset) {
		return &fieldValueError{field: "collected_at", issue: issueOffsetOutOfRange, value: int64(offset)}
	}
	return nil
}

// Year bounds of the four-digit RFC3339 year the saved format writes.
const (
	minSavedYear = 0
	maxSavedYear = 9999
)

// maxWritableOffsetSeconds is 23:59, the largest magnitude UTC offset whose
// hour and minute fields both fit the RFC3339 numeric offset (hour 00..23,
// minute 00..59). 24:00 is out of range, not an alias for the next day.
const maxWritableOffsetSeconds = 23*3600 + 59*60

// isWritableOffset reports whether a UTC offset in seconds can be written by
// the on-disk RFC3339 form without losing or folding anything: it must be a
// whole number of minutes (a sub-minute offset has no representation), and
// its magnitude must be no greater than ±23:59.
func isWritableOffset(offsetSeconds int) bool {
	if offsetSeconds%60 != 0 {
		return false
	}
	if offsetSeconds < 0 {
		offsetSeconds = -offsetSeconds
	}
	return offsetSeconds <= maxWritableOffsetSeconds
}

// formatOffsetSeconds renders a UTC offset in seconds the way it was
// declared: ±HH:MM for a whole-minute offset, ±HH:MM:SS when a sub-minute
// part is present, so an error can show the exact offset that could not be
// saved (e.g. +08:00:45 or -24:00).
func formatOffsetSeconds(seconds int) string {
	sign := "+"
	if seconds < 0 {
		sign = "-"
		seconds = -seconds
	}
	hour := seconds / 3600
	minute := (seconds % 3600) / 60
	if rem := seconds % 60; rem != 0 {
		return fmt.Sprintf("%s%02d:%02d:%02d", sign, hour, minute, rem)
	}
	return fmt.Sprintf("%s%02d:%02d", sign, hour, minute)
}

// validateCollectedAtText guards the one gap in time.Parse(RFC3339) that the
// instant-level check cannot see: the standard library folds an out-of-range
// numeric timezone offset instead of rejecting it — "+00:60" parses as
// +01:00 and "+23:60" as +24:00 — and it accepts "+24:00"/"-24:00" even
// though the RFC3339 write layout cannot express them. The string has
// already passed time.Parse, so its shape is RFC3339 with a "Z" suffix or a
// six-character ±HH:MM suffix; here the offset's own hour and minute fields
// are required to be 00..23 and 00..59 with no carry-over between them.
func validateCollectedAtText(s string) error {
	// "Z" is the only sign-less offset form; it denotes the zero offset.
	if strings.HasSuffix(s, "Z") {
		return nil
	}
	if len(s) < 6 {
		return fmt.Errorf("timezone offset must be Z or ±HH:MM in %q", s)
	}
	off := s[len(s)-6:]
	if (off[0] != '+' && off[0] != '-') || off[3] != ':' {
		return fmt.Errorf("timezone offset must be Z or ±HH:MM, got %q", s)
	}
	hour, ok1 := twoDecimalDigits(off[1:3])
	minute, ok2 := twoDecimalDigits(off[4:6])
	if !ok1 || !ok2 {
		return fmt.Errorf("timezone offset must be Z or ±HH:MM, got %q", s)
	}
	if hour > 23 {
		return fmt.Errorf("timezone offset %s has hour %02d, must be 00-23", off, hour)
	}
	if minute > 59 {
		return fmt.Errorf("timezone offset %s has minute %02d, must be 00-59 (out-of-range minutes are not folded into the hour)", off, minute)
	}
	return nil
}

// twoDecimalDigits parses exactly two ASCII decimal digits.
func twoDecimalDigits(s string) (int, bool) {
	if len(s) != 2 || s[0] < '0' || s[0] > '9' || s[1] < '0' || s[1] > '9' {
		return 0, false
	}
	return int(s[0]-'0')*10 + int(s[1]-'0'), true
}

// validateCollectedAtFraction guards the second gap in time.Parse(RFC3339)
// that the instant-level check cannot see: the standard library silently
// truncates a fractional second beyond nine digits — with either of the
// decimal separators it accepts, "." and "," — so ".1234567891" and
// ".1234567892", and likewise ",1234567891" and ",1234567892", both decode
// to the same nanosecond instant even though the node reported two different
// moments. Heartbeat collection times must be preserved exactly, so such
// input is refused under the same rule no matter which separator was written,
// never truncated or rounded. A fraction of at most nine digits always parses
// exactly, and a longer fraction is accepted only when every digit past the
// ninth is zero — the text then denotes the same instant as its nine-digit
// prefix (".1234567890" and ",123456789000" are both ".123456789"). The
// string has already passed time.Parse, so the "." or "," found here can only
// introduce the fractional second and what follows it up to the zone suffix
// is all digits.
func validateCollectedAtFraction(s string) error {
	sep := strings.IndexAny(s, ".,")
	if sep < 0 {
		return nil
	}
	end := sep + 1
	for end < len(s) && s[end] >= '0' && s[end] <= '9' {
		end++
	}
	frac := s[sep:end] // separator plus digits, quoted verbatim in errors
	digits := frac[1:]
	if len(digits) <= 9 {
		return nil
	}
	for i := 9; i < len(digits); i++ {
		if digits[i] != '0' {
			return fmt.Errorf("fractional second %s cannot be represented exactly at nanosecond precision: digit %d is not zero, and digits past the ninth must all be zero (the instant is never truncated or rounded)", frac, i+1)
		}
	}
	return nil
}

// ParseHealthQueryTime parses an explicitly given health-query instant (the
// CLI's --at) under exactly the text rules a collected_at timestamp is held
// to, because the query instant is compared against saved collection times
// and must be the precise moment the user wrote: RFC3339 with an explicit
// timezone; a numeric offset whose hour is 00-23 and minute 00-59 with no
// carry-over (±24:00 and a folded offset such as +00:60 are refused, while
// "Z", "-00:00" and ±23:59 stay legal); and a fractional second — written
// with "." or "," — that is exact at nanosecond precision (more than nine
// digits only when every digit past the ninth is zero, never truncated or
// rounded). time.Parse alone folds out-of-range offsets and truncates long
// fractions, which would let a moment the user never wrote judge a node
// online or offline, so both text-level guards run after it.
func ParseHealthQueryTime(s string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("must be RFC3339 with timezone (e.g. 2026-10-01T12:00:00+08:00): %w", err)
	}
	if err := validateCollectedAtText(s); err != nil {
		return time.Time{}, err
	}
	if err := validateCollectedAtFraction(s); err != nil {
		return time.Time{}, err
	}
	return t, nil
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
	if e := checkVersionValue(h.Version); e != nil {
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
	case e.field == "version" && e.issue == issueInvalidUnicode:
		return fmt.Errorf("version is not valid Unicode text: contains invalid UTF-8")
	case e.field == "version":
		return fmt.Errorf("version is empty")
	case e.field == "height":
		return fmt.Errorf("height must be >= 0, got %d", e.value)
	case e.field == "missed":
		return fmt.Errorf("missed must be >= 0, got %d", e.value)
	case e.field == "collected_at":
		switch e.issue {
		case issueMissingInstant:
			return fmt.Errorf("collected_at is missing or invalid")
		case issueYearOutOfRange:
			return fmt.Errorf("collected_at year %d cannot be saved: year must be 0000-9999", e.value)
		case issueOffsetOutOfRange:
			return fmt.Errorf("collected_at timezone offset %s cannot be saved exactly: offset must be a whole minute within -23:59 to +23:59", formatOffsetSeconds(int(e.value)))
		}
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
	case e.field == "version" && e.issue == issueInvalidUnicode:
		return fmt.Errorf("version is not valid Unicode text: contains invalid UTF-8")
	case e.field == "version":
		return fmt.Errorf("version must not be empty")
	case e.field == "height":
		return fmt.Errorf("height must be >= 0, got %d", e.value)
	case e.field == "missed":
		return fmt.Errorf("missed must be >= 0, got %d", e.value)
	case e.field == "collected_at":
		switch e.issue {
		case issueMissingInstant:
			return fmt.Errorf("collected_at is missing or invalid")
		case issueYearOutOfRange:
			return fmt.Errorf("collected_at year %d is out of range: year must be 0000-9999", e.value)
		case issueOffsetOutOfRange:
			return fmt.Errorf("collected_at timezone offset %s is out of range: offset must be a whole minute within -23:59 to +23:59", formatOffsetSeconds(int(e.value)))
		}
		return fmt.Errorf("collected_at is missing or invalid")
	default:
		return fmt.Errorf("invalid value for field %q", e.field)
	}
}

// storedError renders a field rule failure for a record read from a saved
// file, prefixing the 1-based record position as stored-data errors do.
func (e *fieldValueError) storedError(position int) error {
	switch {
	case (e.field == "node" || e.field == "version") && e.issue == issueInvalidUnicode:
		return fmt.Errorf("record %d: field %q is not valid Unicode text", position, e.field)
	case e.field == "node" || e.field == "version":
		return fmt.Errorf("record %d: field %q must not be empty", position, e.field)
	case e.field == "seq":
		return fmt.Errorf("record %d: field %q must be a positive integer, got %d", position, e.field, e.value)
	case e.field == "height" || e.field == "missed":
		return fmt.Errorf("record %d: field %q must be >= 0, got %d", position, e.field, e.value)
	case e.field == "collected_at":
		switch e.issue {
		case issueYearOutOfRange:
			return fmt.Errorf("record %d: field %q year %d is out of range: year must be 0000-9999", position, e.field, e.value)
		case issueOffsetOutOfRange:
			return fmt.Errorf("record %d: field %q timezone offset %s is out of range: offset must be a whole minute within -23:59 to +23:59", position, e.field, formatOffsetSeconds(int(e.value)))
		}
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
// same key (the shared objectFields rule). A plain struct/map unmarshal
// would instead keep the last occurrence and let an absent field masquerade
// as the zero value. Both the submit input path and the stored-file read
// path must use it.
func decodeStrictHeartbeatObject(raw json.RawMessage) (Heartbeat, error) {
	// The shared walker streams the object and renders its structural
	// failures through plainError; this callback adds only the submit-side
	// duplicate-name rule, which must fire before the member's value is
	// decoded. A map-based unmarshal would silently keep only the last
	// occurrence.
	fields, err := walkStrictObject(raw, false, func(fields objectFields, key string, _ func() (json.RawMessage, error)) error {
		if fields.check(key) {
			return &strictObjectError{issue: issueDuplicateField, field: key}
		}
		return nil
	})
	if err != nil {
		return Heartbeat{}, asStrictObjectError(err, (*strictObjectError).plainError)
	}

	if f, ok := missingField(fields, heartbeatFields); ok {
		return Heartbeat{}, fmt.Errorf("missing required field %q", f)
	}
	if k, ok := unknownField(fields, heartbeatFields); ok {
		return Heartbeat{}, fmt.Errorf("unknown field %q", k)
	}

	var h Heartbeat

	if err := rejectNull(fields["node"], "node"); err != nil {
		return Heartbeat{}, err
	}
	if err := json.Unmarshal(fields["node"], &h.NodeID); err != nil {
		return Heartbeat{}, fmt.Errorf("node must be a string: %w", err)
	}
	if err := validateRawText(fields["node"], "node"); err != nil {
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
	// time.Parse folds out-of-range numeric offsets (+00:60 -> +01:00) and
	// accepts +24:00; verify the written offset before trusting the fold.
	if err := validateCollectedAtText(collectedStr); err != nil {
		return Heartbeat{}, fmt.Errorf("collected_at must be RFC3339 with timezone (e.g. 2026-10-01T12:00:00+08:00): %w", err)
	}
	// time.Parse also silently truncates a fractional second beyond nine
	// digits for both accepted separators ("." and ","); the collection
	// moment must be exactly representable.
	if err := validateCollectedAtFraction(collectedStr); err != nil {
		return Heartbeat{}, fmt.Errorf("collected_at must be RFC3339 with timezone (e.g. 2026-10-01T12:00:00+08:00): %w", err)
	}
	h.CollectedAt = t

	if err := rejectNull(fields["version"], "version"); err != nil {
		return Heartbeat{}, err
	}
	if err := json.Unmarshal(fields["version"], &h.Version); err != nil {
		return Heartbeat{}, fmt.Errorf("version must be a string: %w", err)
	}
	if err := validateRawText(fields["version"], "version"); err != nil {
		return Heartbeat{}, err
	}
	if e := checkVersionValue(h.Version); e != nil {
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

// validateRawText checks the raw JSON string token of a text field (node or
// version) before its decoded value is trusted. encoding/json silently rewrites
// invalid UTF-8 bytes and unpaired \u surrogate escapes to U+FFFD, which
// would merge distinct texts into one replacement-char value: distinct node
// ids would collapse and break the ownership check against stored files, and
// distinct versions would collapse into text the node never reported,
// corrupting dedup and the version-skew judgement. The raw token must decode
// losslessly: every literal byte sequence must be valid UTF-8, and every \u
// escape must be a valid scalar value or a properly paired high/low
// surrogate pair. A literal "�" (or its \uFFFD escape) is valid text the
// user typed and is accepted; only undecodable input is rejected. A \\
// escape is consumed as a unit, so text that genuinely contains a backslash
// followed by "u" and hex digits is never re-interpreted as an escape.
func validateRawText(raw json.RawMessage, field string) error {
	raw = bytes.TrimSpace(raw)
	if len(raw) < 2 || raw[0] != '"' || raw[len(raw)-1] != '"' {
		return fmt.Errorf("%s must be a string", field)
	}
	s := raw[1 : len(raw)-1]
	for i := 0; i < len(s); {
		c := s[i]
		if c == '\\' {
			if i+1 >= len(s) {
				return fmt.Errorf("%s contains a truncated escape", field)
			}
			if s[i+1] != 'u' {
				i += 2
				continue
			}
			if i+6 > len(s) {
				return fmt.Errorf("%s contains a truncated \\u escape", field)
			}
			r, err := parseHex4(s[i+2 : i+6])
			if err != nil {
				return fmt.Errorf("%s contains an invalid \\u escape: %w", field, err)
			}
			switch {
			case r >= 0xD800 && r <= 0xDBFF:
				// A high surrogate is only valid when immediately followed
				// by a low-surrogate \u escape.
				if i+12 > len(s) || s[i+6] != '\\' || s[i+7] != 'u' {
					return fmt.Errorf("%s contains an unpaired high surrogate \\u%04X", field, r)
				}
				lo, err := parseHex4(s[i+8 : i+12])
				if err != nil {
					return fmt.Errorf("%s contains an invalid \\u escape: %w", field, err)
				}
				if lo < 0xDC00 || lo > 0xDFFF {
					return fmt.Errorf("%s contains an unpaired high surrogate \\u%04X", field, r)
				}
				i += 12
			case r >= 0xDC00 && r <= 0xDFFF:
				return fmt.Errorf("%s contains an unpaired low surrogate \\u%04X", field, r)
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
			return fmt.Errorf("%s contains invalid UTF-8 at byte offset %d", field, i)
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
