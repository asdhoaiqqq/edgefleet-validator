package edgefleet

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// This file holds the single structural rule set shared by every strict JSON
// object decoder in this package — the submit-input heartbeat objects and the
// on-disk node-file envelope alike:
//
//   - the value must be one JSON object;
//   - within one object, a field name may appear only once, where names are
//     compared as the JSON strings they decode to, so a plain "missed" and a
//     Unicode-escaped "missed" spelling denote the same field;
//   - the caller decides which fields are required or known, in its own order
//     and with its own wording.
//
// What the two boundaries do NOT share is the meaning and ordering of a
// rejection: submitted input is illegal user content ("record N: ...", a
// non-zero command exit, no success count), while a node file that fails the
// same rules is saved-data corruption. The rules therefore live here once;
// walkStrictObject reports them as a structured strictObjectError, and each
// context renders that failure in its own message style: plainError for a
// heartbeat object at either boundary (a stored record additionally receives
// its "record N:" prefix and corruption wrapper from loadNodeFile), and
// envelopeError for the on-disk file envelope itself.

// objectFields collects the fields of one JSON object while enforcing the
// single field-uniqueness rule shared by every strict decoder in this
// package. Names are compared as the JSON strings they decode to, so a plain
// spelling and a Unicode-escaped spelling of the same name collide, whether
// the repeated value is identical or different. The rule is scoped to the
// current object: the same field name appearing in another heartbeat object
// is unrelated. Both the submit-input decoder and the stored-file envelope
// decoder apply the rule through this type; each boundary keeps its own error
// wording and its own check ordering for the rejection.
//
// A nil map value means the name has been seen but its value not captured
// yet; walkStrictObject stores the raw value there once the member has been
// fully read.
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

// missingField returns the first name in required (in the given order) that
// did not appear in the object. Both boundaries list required fields in a
// fixed order so a multi-field omission names the same field every time.
func missingField(fields objectFields, required []string) (string, bool) {
	for _, f := range required {
		if !fields.has(f) {
			return f, true
		}
	}
	return "", false
}

// unknownField returns one name that appeared in the object but is not in
// known. It preserves the submit decoder's post-decode unknown-field check;
// the envelope rejects an unknown field inline, in document order.
func unknownField(fields objectFields, known []string) (string, bool) {
	for name := range fields {
		if !containsString(known, name) {
			return name, true
		}
	}
	return "", false
}

func containsString(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// strictObjectIssue classifies a structural violation found while walking one
// JSON object, carrying no wording of its own. Each trust boundary renders it
// itself (plainError versus envelopeError), the same split fieldValueError
// uses for field-value rules.
type strictObjectIssue int

const (
	issueOpenInvalid        strictObjectIssue = iota // cannot read the first token
	issueNotObject                                   // the first token is not '{'
	issueFieldNameInvalid                            // cannot read a member name token
	issueFieldNameNotString                          // member name is not a JSON string
	issueValueInvalid                                // member value cannot be read as raw JSON
	issueDuplicateField                              // a decoded field name repeats in the object
	issueCloseInvalid                                // cannot read the closing '}'
	issueTrailingData                                // content follows the single object
)

// strictObjectError is a structural rule failure from walkStrictObject. Field
// carries the offending member name for the per-field issues; err carries the
// underlying encoding/json error.
type strictObjectError struct {
	issue strictObjectIssue
	field string
	err   error
}

func (e *strictObjectError) Error() string { return e.plainError().Error() }

// plainError renders the failure in the wording used while decoding a strict
// heartbeat object, at either trust boundary; a stored-record failure
// additionally gets its "record N:" prefix and CorruptError wrapper from
// loadNodeFile.
func (e *strictObjectError) plainError() error {
	switch e.issue {
	case issueOpenInvalid:
		return fmt.Errorf("record must be a JSON object: %w", e.err)
	case issueNotObject:
		return fmt.Errorf("record must be a JSON object")
	case issueFieldNameInvalid:
		return fmt.Errorf("invalid field: %w", e.err)
	case issueFieldNameNotString:
		return fmt.Errorf("field name must be a string")
	case issueValueInvalid:
		return fmt.Errorf("invalid value for field %q: %w", e.field, e.err)
	case issueDuplicateField:
		return fmt.Errorf("duplicate field %q", e.field)
	case issueCloseInvalid:
		return fmt.Errorf("invalid record: %w", e.err)
	case issueTrailingData:
		if e.err != nil {
			return fmt.Errorf("invalid record: %w", e.err)
		}
		return fmt.Errorf("invalid record: unexpected data after the object")
	default:
		return fmt.Errorf("invalid record")
	}
}

// envelopeError renders the failure as saved-data corruption wording, where
// the object is the node-file envelope rather than a submitted record.
func (e *strictObjectError) envelopeError() error {
	switch e.issue {
	case issueOpenInvalid:
		return fmt.Errorf("invalid JSON: %w", e.err)
	case issueNotObject:
		return fmt.Errorf("invalid JSON: file must be a JSON object")
	case issueFieldNameInvalid:
		return fmt.Errorf("invalid JSON: %w", e.err)
	case issueFieldNameNotString:
		return fmt.Errorf("invalid JSON: envelope field name must be a string")
	case issueValueInvalid:
		return fmt.Errorf("invalid value for envelope field %q: %w", e.field, e.err)
	case issueDuplicateField:
		return fmt.Errorf("duplicate envelope field %q", e.field)
	case issueCloseInvalid:
		return fmt.Errorf("invalid JSON: %w", e.err)
	case issueTrailingData:
		if e.err != nil {
			return fmt.Errorf("invalid JSON: %w", e.err)
		}
		return fmt.Errorf("invalid JSON: unexpected data after the envelope object")
	default:
		return fmt.Errorf("invalid JSON")
	}
}

// asStrictObjectError renders a walker failure with the boundary's renderer,
// leaving any other error (a boundary-specific wording produced inside the
// field callback) untouched.
func asStrictObjectError(err error, render func(*strictObjectError) error) error {
	var se *strictObjectError
	if errors.As(err, &se) {
		return render(se)
	}
	return err
}

// objectFieldFunc inspects one object member in document order. name is the
// JSON-decoded member name. decodeValue reads the member's raw JSON value and
// advances the stream; it is idempotent and walkStrictObject calls it once
// more after the callback returns, so a callback may choose WHEN to read the
// value relative to its own checks — the submit decoder rejects a duplicate
// name before decoding its value, while the envelope decoder reads the value
// first — but the value is always consumed exactly once and semantic decoding
// stays entirely the callback's job. A callback's own non-structural error
// (distinct wording such as a null/type rejection) is passed through
// unchanged.
type objectFieldFunc func(fields objectFields, name string, decodeValue func() (json.RawMessage, error)) error

// walkStrictObject streams exactly one JSON object from raw, invoking visit
// once per member in document order. It enforces the shared structural rules
// (object shape, field-name decoding, raw-value readability) and records each
// decoded name in the returned objectFields, where visit applies the shared
// uniqueness rule through fields.check. When rejectTrailing is true, nothing
// may follow the object's closing brace; submitted record elements never set
// it (each array element is already exactly one value), the on-disk envelope
// does. Structural failures come back as *strictObjectError, rendered by each
// boundary; errors produced by visit pass through verbatim.
func walkStrictObject(raw json.RawMessage, rejectTrailing bool, visit objectFieldFunc) (objectFields, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	tok, err := dec.Token()
	if err != nil {
		return nil, &strictObjectError{issue: issueOpenInvalid, err: err}
	}
	if d, ok := tok.(json.Delim); !ok || d != '{' {
		return nil, &strictObjectError{issue: issueNotObject}
	}

	fields := objectFields{}
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return nil, &strictObjectError{issue: issueFieldNameInvalid, err: err}
		}
		key, ok := keyTok.(string)
		if !ok {
			return nil, &strictObjectError{issue: issueFieldNameNotString}
		}

		var (
			rawVal    json.RawMessage
			valueRead bool
			valueErr  *strictObjectError
		)
		decodeValue := func() (json.RawMessage, error) {
			if !valueRead {
				valueRead = true
				if err := dec.Decode(&rawVal); err != nil {
					valueErr = &strictObjectError{issue: issueValueInvalid, field: key, err: err}
				}
			}
			if valueErr != nil {
				return rawVal, valueErr
			}
			return rawVal, nil
		}

		if err := visit(fields, key, decodeValue); err != nil {
			return nil, err
		}
		// On success the callback may have declined to read the value (the
		// submit duplicate check happens before reading); decodeValue is
		// idempotent and fills rawVal, so this either fetches it now or
		// reuses the cached one.
		if _, err := decodeValue(); err != nil {
			return nil, err
		}
		fields[key] = rawVal
	}

	// Consume the closing brace.
	if _, err := dec.Token(); err != nil {
		return nil, &strictObjectError{issue: issueCloseInvalid, err: err}
	}
	if rejectTrailing {
		// Nothing may follow the single object.
		if _, err := dec.Token(); err != io.EOF {
			if err == nil {
				return nil, &strictObjectError{issue: issueTrailingData}
			}
			return nil, &strictObjectError{issue: issueTrailingData, err: err}
		}
	}
	return fields, nil
}
