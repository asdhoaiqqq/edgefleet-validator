package edgefleet

import (
	"encoding/json"
	"fmt"
)

// This file holds the single implementation of the strict JSON object field
// rules shared by every trust boundary that decodes a JSON object: the
// submit-input record decoder (decodeStrictHeartbeatObject) and the
// stored-file envelope decoder (decodeStrictNodeFile). The rules that live
// here exactly once are:
//
//   - field identification: a field name is the JSON string its token
//     decodes to, so a plain spelling and a Unicode-escaped spelling of the
//     same name ("missed" and "misséd") are the same field;
//   - field uniqueness: within one object a field name may appear only once,
//     whether the repeated value is identical or different;
//   - required-field presence and known-field membership, checked against a
//     boundary-supplied field list.
//
// Each boundary keeps its own error wording and its own check ordering: the
// walker below renders structural failures through a boundary-supplied
// function (the same idiom as fieldValueError's directError/decodeError/
// storedError) and lets the boundary choose where the uniqueness check and
// the per-field value handling sit in the stream.

// objectFields collects the fields of one JSON object while enforcing the
// single field-uniqueness rule shared by every strict decoder in this
// package: within one object, a field name may appear only once. Names are
// compared as the JSON strings they decode to, so a plain spelling and a
// Unicode-escaped spelling of the same name collide, whether the repeated
// value is identical or different. The rule is scoped to the current object:
// the same field name appearing in another heartbeat object is unrelated.
//
// A nil map value means the name has been seen but its value not decoded
// yet; once decoded, the value is stored under the name.
type objectFields map[string]json.RawMessage

// check records one occurrence of a field name and reports whether the name
// already appeared in this object (duplicate == true).
func (o objectFields) check(name string) (duplicate bool) {
	if _, exists := o[name]; exists {
		return true
	}
	o[name] = nil
	return false
}

// has reports whether the field name appeared in this object.
func (o objectFields) has(name string) bool {
	_, exists := o[name]
	return exists
}

// requireEach enforces the shared required-field presence rule: every name
// in names must have appeared in the object. Names are checked in the given
// order, so the first missing one is reported through the boundary's own
// missing-field message.
func (o objectFields) requireEach(names []string, missing func(name string) error) error {
	for _, n := range names {
		if !o.has(n) {
			return missing(n)
		}
	}
	return nil
}

// rejectUnknown enforces the shared known-field rule: every name that
// appeared in the object must be listed in known. The first offending name
// is reported through the boundary's own unknown-field message.
func (o objectFields) rejectUnknown(known []string, unknown func(name string) error) error {
	for k := range o {
		if !containsString(known, k) {
			return unknown(k)
		}
	}
	return nil
}

// walkIssue classifies a structural failure found while walking one strict
// JSON object. Like fieldIssue for the value rules, it carries no wording:
// each trust boundary renders the failure in its own message style through
// the render functions below.
type walkIssue int

const (
	walkOpenRead     walkIssue = iota // the opening token could not be read
	walkNotObject                     // the value is not a JSON object
	walkKeyRead                       // a field-name token could not be read
	walkKeyNotString                  // a field name is not a string
	walkDuplicate                     // a field name repeats within this object
	walkValueRead                     // a field's value could not be decoded
	walkCloseRead                     // the closing brace could not be read
)

// objectWalkError describes one structural failure of the object walk: the
// issue that occurred, the field name it concerns (for walkDuplicate and
// walkValueRead), and the underlying decoder error when there is one.
type objectWalkError struct {
	issue walkIssue
	field string
	err   error
}

// heartbeatError renders a walk failure in the submit-record decoder's
// wording: the object is one heartbeat record.
func (e *objectWalkError) heartbeatError() error {
	switch e.issue {
	case walkOpenRead:
		return fmt.Errorf("record must be a JSON object: %w", e.err)
	case walkNotObject:
		return fmt.Errorf("record must be a JSON object")
	case walkKeyRead:
		return fmt.Errorf("invalid field: %w", e.err)
	case walkKeyNotString:
		return fmt.Errorf("field name must be a string")
	case walkDuplicate:
		return fmt.Errorf("duplicate field %q", e.field)
	case walkValueRead:
		return fmt.Errorf("invalid value for field %q: %w", e.field, e.err)
	case walkCloseRead:
		return fmt.Errorf("invalid record: %w", e.err)
	default:
		return fmt.Errorf("invalid record")
	}
}

// envelopeError renders a walk failure in the stored-file envelope decoder's
// wording: the object is the node-file envelope, and its failures are
// reported as invalid stored JSON.
func (e *objectWalkError) envelopeError() error {
	switch e.issue {
	case walkOpenRead:
		return fmt.Errorf("invalid JSON: %w", e.err)
	case walkNotObject:
		return fmt.Errorf("invalid JSON: file must be a JSON object")
	case walkKeyRead:
		return fmt.Errorf("invalid JSON: %w", e.err)
	case walkKeyNotString:
		return fmt.Errorf("invalid JSON: envelope field name must be a string")
	case walkDuplicate:
		return fmt.Errorf("duplicate envelope field %q", e.field)
	case walkValueRead:
		return fmt.Errorf("invalid value for envelope field %q: %w", e.field, e.err)
	case walkCloseRead:
		return fmt.Errorf("invalid JSON: %w", e.err)
	default:
		return fmt.Errorf("invalid JSON")
	}
}

// strictObjectWalk configures walkStrictObject for one trust boundary. The
// walking skeleton — object framing, field-name decoding, the objectFields
// uniqueness rule — is shared; these knobs are exactly the points where the
// two boundaries legitimately differ.
type strictObjectWalk struct {
	// render translates each structural failure of the walk into the
	// boundary's own message style.
	render func(*objectWalkError) error

	// dupBeforeValue places the uniqueness check relative to decoding the
	// field's value. The submit-record boundary rejects a repeated name
	// before its value is read (true); the stored-envelope boundary reads
	// the value first (false). Each side keeps its established check order.
	dupBeforeValue bool

	// handle, when non-nil, runs for each field in document order right
	// after its uniqueness check, so a boundary can decode the value and
	// reject unknown names inline (the stored envelope). A nil handle
	// leaves all value decoding to the caller after the walk (submit
	// records, which decode the collected objectFields field by field).
	// Errors returned by handle are already boundary-worded and pass
	// through unwrapped.
	handle func(name string, value json.RawMessage) error
}

// walkStrictObject streams one JSON object from dec and collects its fields,
// enforcing the shared field rules exactly once for both trust boundaries:
// the value must be an object, every field name must be a string, and a
// field name may occur only once per object (compared as the decoded JSON
// string, so a Unicode-escaped respelling of a name still collides). The
// opening "{" and closing "}" are consumed here; anything before or after
// the object remains the caller's concern.
func walkStrictObject(dec *json.Decoder, spec strictObjectWalk) (objectFields, error) {
	fail := func(issue walkIssue, field string, err error) (objectFields, error) {
		return nil, spec.render(&objectWalkError{issue: issue, field: field, err: err})
	}

	tok, err := dec.Token()
	if err != nil {
		return fail(walkOpenRead, "", err)
	}
	if d, ok := tok.(json.Delim); !ok || d != '{' {
		return fail(walkNotObject, "", nil)
	}

	fields := objectFields{}
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return fail(walkKeyRead, "", err)
		}
		key, ok := keyTok.(string)
		if !ok {
			return fail(walkKeyNotString, "", nil)
		}
		if spec.dupBeforeValue && fields.check(key) {
			return fail(walkDuplicate, key, nil)
		}
		var val json.RawMessage
		if err := dec.Decode(&val); err != nil {
			return fail(walkValueRead, key, err)
		}
		if !spec.dupBeforeValue && fields.check(key) {
			return fail(walkDuplicate, key, nil)
		}
		fields[key] = val
		if spec.handle != nil {
			if err := spec.handle(key, val); err != nil {
				return nil, err
			}
		}
	}
	// Consume the closing brace.
	if _, err := dec.Token(); err != nil {
		return fail(walkCloseRead, "", err)
	}
	return fields, nil
}

// isNullJSON reports whether a raw JSON value is the null literal. A null
// where a required field's value belongs means the field was not reported,
// which must not be silently read as the zero value.
func isNullJSON(raw json.RawMessage) bool {
	return string(raw) == "null"
}

// containsString reports whether list contains s.
func containsString(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}
