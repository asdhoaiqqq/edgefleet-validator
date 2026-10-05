package edgefleet

import (
	"encoding/json"
	"fmt"
)

// strictObjectWording is the error text a trust boundary supplies while a
// strict JSON object is read. readStrictJSONObject only renders the wording
// the boundary previously produced itself; every boundary keeps its own
// distinction between bad input and corrupt saved data.
type strictObjectWording struct {
	// tokenError wraps a decoder failure before the opening brace, e.g.
	// "record must be a JSON object" or "invalid JSON".
	tokenError func(err error) error
	// notObject reports that the value is not a JSON object at all.
	notObject func() error
	// keyError wraps a decoder failure while reading the next field name.
	keyError func(err error) error
	// keyNotString reports a field name that did not decode to a string.
	keyNotString func() error
	// duplicate renders a field name seen for the second time in the same
	// object. The key is the JSON string the two spellings denote, so the
	// boundary can quote the decoded name.
	duplicate func(key string) error
	// valueError wraps a decoder failure while reading a field value.
	valueError func(key string, err error) error
	// closeError wraps a decoder failure while consuming the closing brace.
	closeError func(err error) error
}

// readStrictJSONObject streams exactly one JSON object from dec and collects
// its fields as raw JSON, enforcing the single rule shared by every strict
// object boundary in the package: within one object, a decoded field name may
// appear at most once. Keys are compared as the JSON strings they denote, so
// a plain key and a Unicode-escaped spelling of it ("missed" vs m\u0069ssed)
// collide, while the same name in two different objects (two records, or a
// record and its envelope) never does. Two occurrences are rejected whether
// their values are identical or different. The raw value of each key is
// captured on first sight, exactly as a manual streaming loop would.
//
// visit, when non-nil, runs in document order right after each value is
// decoded, letting a boundary validate each field inline (null, type and
// unknown-key checks) before the next key is read. Its error is returned
// unwrapped. Whitespace and key ordering carry no meaning.
//
// readStrictJSONObject consumes only the object and leaves dec positioned
// immediately after the closing brace; anything required past it (for example
// "nothing may follow the envelope") stays the caller's check on the same
// decoder.
func readStrictJSONObject(dec *json.Decoder, wording strictObjectWording, visit func(key string, val json.RawMessage) error) (map[string]json.RawMessage, error) {
	tok, err := dec.Token()
	if err != nil {
		return nil, wording.tokenError(err)
	}
	if d, ok := tok.(json.Delim); !ok || d != '{' {
		return nil, wording.notObject()
	}

	fields := make(map[string]json.RawMessage)
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return nil, wording.keyError(err)
		}
		key, ok := keyTok.(string)
		if !ok {
			return nil, wording.keyNotString()
		}
		// Compare the decoded names: a JSON Unicode escape denoting the same
		// key (m\u0069ssed == missed) is the same field and must not be kept
		// by last-write-wins. A map-based unmarshal would do exactly that and
		// let a repeated field masquerade as one field.
		if _, exists := fields[key]; exists {
			return nil, wording.duplicate(key)
		}
		var val json.RawMessage
		if err := dec.Decode(&val); err != nil {
			return nil, wording.valueError(key, err)
		}
		fields[key] = val
		if visit != nil {
			if err := visit(key, val); err != nil {
				return nil, err
			}
		}
	}
	// Consume the closing brace.
	if _, err := dec.Token(); err != nil {
		return nil, wording.closeError(err)
	}
	return fields, nil
}

// recordObjectWording is the wording used while decoding one strict heartbeat
// object in submit input and in stored records.
func recordObjectWording() strictObjectWording {
	return strictObjectWording{
		tokenError: func(err error) error {
			return fmt.Errorf("record must be a JSON object: %w", err)
		},
		notObject: func() error {
			return fmt.Errorf("record must be a JSON object")
		},
		keyError: func(err error) error {
			return fmt.Errorf("invalid field: %w", err)
		},
		keyNotString: func() error {
			return fmt.Errorf("field name must be a string")
		},
		duplicate: func(key string) error {
			return fmt.Errorf("duplicate field %q", key)
		},
		valueError: func(key string, err error) error {
			return fmt.Errorf("invalid value for field %q: %w", key, err)
		},
		closeError: func(err error) error {
			return fmt.Errorf("invalid record: %w", err)
		},
	}
}

// envelopeObjectWording is the wording used while decoding the on-disk
// envelope object (format/checksum/records).
func envelopeObjectWording() strictObjectWording {
	return strictObjectWording{
		tokenError: func(err error) error {
			return fmt.Errorf("invalid JSON: %w", err)
		},
		notObject: func() error {
			return fmt.Errorf("invalid JSON: file must be a JSON object")
		},
		keyError: func(err error) error {
			return fmt.Errorf("invalid JSON: %w", err)
		},
		keyNotString: func() error {
			return fmt.Errorf("invalid JSON: envelope field name must be a string")
		},
		duplicate: func(key string) error {
			return fmt.Errorf("duplicate envelope field %q", key)
		},
		valueError: func(key string, err error) error {
			return fmt.Errorf("invalid value for envelope field %q: %w", key, err)
		},
	}
}
