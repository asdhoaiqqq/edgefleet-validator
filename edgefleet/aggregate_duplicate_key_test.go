package edgefleet

import (
	"errors"
	"strings"
	"testing"
)

// Duplicate top-level "key" validation for event records. A record whose JSON
// carries the name "key" twice at its top level must fail fatally instead of
// silently running with the last value -- regardless of the two values, their
// textual order, or how the field name is escaped. Duplicate detection is
// based on the JSON-decoded field name, sees only the event's top level, and
// runs before any field value, partition-state or late-event judgement.

// wireUnicode builds the six JSON bytes backslash-u followed by hex4 (for
// example wireUnicode("006b") is the JSON escape for the letter k) at run
// time, so this source never has to carry a literal backslash-u sequence that
// tooling might rewrite while saving the file. hex4 is always ASCII hex.
func wireUnicode(hex4 string) string {
	return string(rune(0x5c)) + "u" + hex4
}

// escapedKeyField is the JSON member spelling of a top-level key field whose
// name uses the U+006B (lowercase k) escape: quoted, backslash-u-0-0-6-b then
// "ey".
func escapedKeyField() string {
	return `"` + wireUnicode("006b") + `ey"`
}

// wantDuplicateKeyError checks that err is the duplicate-key *InputError on
// the given physical line: never an output error, never another field reason,
// never an idle or late wording.
func wantDuplicateKeyError(t *testing.T, err error, line int) {
	t.Helper()
	if err == nil {
		t.Fatalf("a repeated top-level key must fail fatally; got nil")
	}
	var inputErr *InputError
	if !errors.As(err, &inputErr) {
		t.Fatalf("want *InputError, got %T: %v", err, err)
	}
	var outputErr *OutputError
	if errors.As(err, &outputErr) {
		t.Fatalf("a duplicate key is a record error, not an output failure: %v", err)
	}
	if inputErr.Line != line {
		t.Errorf("InputError.Line = %d, want %d", inputErr.Line, line)
	}
	wantReason := `field "key" appears more than once at the top level of an event record; remove the duplicate field`
	if inputErr.Reason != wantReason {
		t.Errorf("reason = %q, want %q", inputErr.Reason, wantReason)
	}
}

// TestAggregateDuplicateKeyBasicShapes covers the fixed-window entry point:
// identical and different values, either textual position, and the field name
// spelled with an equivalent JSON Unicode escape are all the same name.
func TestAggregateDuplicateKeyBasicShapes(t *testing.T) {
	escName := escapedKeyField()
	cases := []struct {
		name   string
		record string
	}{
		{"identical values", `{"type":"event","key":"a","key":"a","time":100,"value":1}`},
		{"different values", `{"type":"event","key":"a","key":"b","time":100,"value":1}`},
		{"second occurrence first in text", `{"type":"event","key":"b","time":100,"value":1,"key":"a"}`},
		{"key between the other fields", `{"type":"event","time":100,"key":"a","value":1,"key":"b"}`},
		{"escaped name second", `{"type":"event","key":"a",` + escName + `:"b","time":100,"value":1}`},
		{"escaped name first", `{"type":"event",` + escName + `:"a","key":"b","time":100,"value":1}`},
		{"both names escaped identically", `{"type":"event",` + escName + `:"a",` + escName + `:"a","time":100,"value":1}`},
		{"three occurrences", `{"type":"event","key":"a","key":"b","key":"c","time":100,"value":1}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			input := strings.Join([]string{
				`{"type":"event","key":"ok","time":100,"value":2}`,
				`{"type":"watermark","time":1000}`,
				``, // blank line 3 still counts
				tc.record,
				`{"type":"watermark","time":2000}`, // never read
			}, "\n") + "\n"
			stdout, stderr, err := runAggregate(t, input, 1000)
			wantDuplicateKeyError(t, err, 4)
			if stderr != "" {
				t.Errorf("a duplicate key is fatal, never a late notice; got %q", stderr)
			}
			// Only the window closed on line 2 survives; the fatal event
			// contributed nothing and line 5 was never processed.
			results := parseResultLines(t, stdout)
			if len(results) != 1 || results[0].Key != "ok" || results[0].Count != 1 || results[0].Sum != 2 {
				t.Errorf("stdout = %q, want only the earlier ok window", stdout)
			}
		})
	}
}

// TestAggregateDuplicateKeyNameComparedAfterDecoding pins the exact
// name-equality rule: the member name is decoded first, so a Unicode escape
// spelling lowercase k (U+006B) repeats "key", while one spelling capital K
// (U+004B) gives the different name "Key" and stays an ignored extra field.
func TestAggregateDuplicateKeyNameComparedAfterDecoding(t *testing.T) {
	lowerK := wireUnicode("006b") // spells lowercase k
	capK := wireUnicode("004b")   // spells capital K: "Key" != "key"

	t.Run("capital K escape is a different field name", func(t *testing.T) {
		record := `{"type":"event","key":"a","` + capK + `ey":"ignored","time":100,"value":2}`
		input := strings.Join([]string{record, `{"type":"watermark","time":1000}`}, "\n") + "\n"
		stdout, stderr, err := runAggregate(t, input, 1000)
		if err != nil {
			t.Fatalf("a differently-named field must not be a duplicate: %v", err)
		}
		if stderr != "" {
			t.Fatalf("unexpected late log: %q", stderr)
		}
		results := parseResultLines(t, stdout)
		if len(results) != 1 || results[0].Key != "a" || results[0].Count != 1 || results[0].Sum != 2 {
			t.Fatalf("stdout = %q, want one window for key a with count 1 sum 2", stdout)
		}
	})

	t.Run("one escaped name without a literal is the single key", func(t *testing.T) {
		record := `{"type":"event","` + lowerK + `ey":"a","time":100,"value":2}`
		input := strings.Join([]string{record, `{"type":"watermark","time":1000}`}, "\n") + "\n"
		stdout, _, err := runAggregate(t, input, 1000)
		if err != nil {
			t.Fatalf("one escaped key name is just a normal key: %v", err)
		}
		results := parseResultLines(t, stdout)
		if len(results) != 1 || results[0].Key != "a" {
			t.Fatalf("stdout = %q, want one window for decoded key a", stdout)
		}
	})
}

// TestAggregateDuplicateKeyTopLevelScopeOnly ensures only the event record's
// own top-level members count: a "key" inside an attached object or array, or
// the letters within a string value, do not repeat the top-level field.
func TestAggregateDuplicateKeyTopLevelScopeOnly(t *testing.T) {
	nestedName := escapedKeyField() // an escaped key spelling nested in an object
	cases := []struct {
		name    string
		record  string
		wantKey string // decoded key of the single expected window
	}{
		{
			"key nested in an attached object",
			`{"type":"event","key":"a","time":100,"value":1,"extra":{"key":"b"}}`,
			"a",
		},
		{
			"escaped key name nested in an attached object",
			`{"type":"event","key":"a","time":100,"value":1,"extra":{` + nestedName + `:"b"}}`,
			"a",
		},
		{
			"key nested in an object inside an array",
			`{"type":"event","key":"a","time":100,"value":1,"extra":[{"key":"b"},{"key":"c"}]}`,
			"a",
		},
		{
			"nested object before the top-level key",
			`{"type":"event","extra":{"key":"b"},"key":"a","time":100,"value":1}`,
			"a",
		},
		{
			"key letters inside the key's own string value",
			// The value is the text "key"; it is one event, not a repeated field.
			`{"type":"event","key":"key","time":100,"value":1}`,
			"key",
		},
		{
			"key letters inside another string field",
			`{"type":"event","key":"a","note":"mention the key field","time":100,"value":1}`,
			"a",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			input := strings.Join([]string{tc.record, `{"type":"watermark","time":1000}`}, "\n") + "\n"
			stdout, stderr, err := runAggregate(t, input, 1000)
			if err != nil {
				t.Fatalf("only top-level members can repeat key: %v", err)
			}
			if stderr != "" {
				t.Fatalf("unexpected late log: %q", stderr)
			}
			results := parseResultLines(t, stdout)
			if len(results) != 1 || results[0].Key != tc.wantKey || results[0].Count != 1 || results[0].Sum != 1 {
				t.Fatalf("stdout = %q, want exactly one count for top-level key %q", stdout, tc.wantKey)
			}
		})
	}
}

// TestAggregateDuplicateKeyBeatsFieldValidation: the duplicate is rejected
// before either key value is decoded, and before time/value are checked. An
// empty, mistyped or character-damaged occurrence paired with a legal one
// must still report the duplicate, not the value-specific failure.
func TestAggregateDuplicateKeyBeatsFieldValidation(t *testing.T) {
	surrogate := wireUnicode("d800") // unpaired high surrogate when inside a JSON string
	cases := []struct {
		name   string
		record string
	}{
		{"first key empty, second legal", `{"type":"event","key":"","key":"b","time":100,"value":1}`},
		{"first key mistyped, second legal", `{"type":"event","key":7,"key":"b","time":100,"value":1}`},
		{"first key surrogate-damaged, second legal", `{"type":"event","key":"a` + surrogate + `b","key":"b","time":100,"value":1}`},
		{"first key has raw invalid UTF-8, second legal", "{\"type\":\"event\",\"key\":\"a\xffb\",\"key\":\"b\",\"time\":100,\"value\":1}"},
		{"first legal, second empty", `{"type":"event","key":"b","key":"","time":100,"value":1}`},
		{"first legal, second mistyped", `{"type":"event","key":"b","key":7,"time":100,"value":1}`},
		{"first legal, second surrogate-damaged", `{"type":"event","key":"b","key":"a` + surrogate + `z","time":100,"value":1}`},
		{"time also missing", `{"type":"event","key":"a","key":"b","value":1}`},
		{"value also wrong type", `{"type":"event","key":"a","key":"b","time":100,"value":"x"}`},
		{"time negative as well", `{"type":"event","key":"a","key":"b","time":-5,"value":1}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, stderr, err := runAggregate(t, tc.record+"\n", 1000)
			wantDuplicateKeyError(t, err, 1)
			if stderr != "" {
				t.Errorf("fatal record produces no late notice, got %q", stderr)
			}
		})
	}
}

// TestAggregateDuplicateKeyBeatsPartitionFieldCheck uses partitioned mode: a
// duplicate key with the partition missing, out of range or mistyped still
// reports the duplicate, never the partition field error.
func TestAggregateDuplicateKeyBeatsPartitionFieldCheck(t *testing.T) {
	cases := []struct {
		name   string
		record string
	}{
		{"partition missing", `{"type":"event","key":"a","key":"b","time":100,"value":1}`},
		{"partition out of range", `{"type":"event","key":"a","key":"b","time":100,"value":1,"partition":9}`},
		{"partition mistyped", `{"type":"event","key":"a","key":"b","time":100,"value":1,"partition":"x"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			input := strings.Join([]string{
				`{"type":"watermark","time":2000,"partition":0}`,
				`{"type":"watermark","time":2000,"partition":1}`,
				tc.record,
			}, "\n") + "\n"
			_, stderr, err := runPartitionedSliding(t, input, 1000, 1000, 2)
			wantDuplicateKeyError(t, err, 3)
			if stderr != "" {
				t.Errorf("fatal record produces no late notice, got %q", stderr)
			}
		})
	}
}

// TestAggregateDuplicateKeyBeatsLateAndIdle is the state precedence: a
// duplicate-key event that is also strictly below the current watermark, or
// that belongs to an idle partition, must fail as the duplicate record error
// -- never as a late-event skip (exit 0 with a notice) and never as the
// idle-partition failure.
func TestAggregateDuplicateKeyBeatsLateAndIdle(t *testing.T) {
	t.Run("fixed windows: late duplicate is still fatal", func(t *testing.T) {
		input := strings.Join([]string{
			`{"type":"event","key":"ok","time":100,"value":2}`,
			`{"type":"watermark","time":1000}`,                          // closes [0,1000), watermark now 1000
			`{"type":"event","key":"a","key":"b","time":999,"value":1}`, // duplicate AND late
			`{"type":"watermark","time":2000}`,                          // never read
		}, "\n") + "\n"
		stdout, stderr, err := runAggregate(t, input, 1000)
		wantDuplicateKeyError(t, err, 3)
		if stderr != "" {
			t.Errorf("a duplicate must not become a late-event notice, got %q", stderr)
		}
		if strings.Contains(err.Error(), "late event") {
			t.Errorf("error must not use the late-event wording: %q", err.Error())
		}
		results := parseResultLines(t, stdout)
		if len(results) != 1 || results[0].Key != "ok" {
			t.Errorf("stdout = %q, want only the earlier ok window", stdout)
		}
	})

	t.Run("partitioned sliding: idle partition duplicate is the field error", func(t *testing.T) {
		lines := append(idleFieldOrderFixtureLines(),
			// Line 7: partition 1 is idle, time 999 is below the effective
			// watermark 1500, and key is repeated -- three failures at once.
			`{"type":"event","key":"a","key":"b","time":999,"value":1,"partition":1}`,
		)
		lines = append(lines, idleFieldOrderDeadSuffix()...)
		stdout, stderr, err := runPartitionedSliding(t, strings.Join(lines, "\n"), 1000, 600, 2)
		wantDuplicateKeyError(t, err, 7)
		if strings.Contains(err.Error(), "idle partition") {
			t.Errorf("duplicate field error must precede the idle-partition rule: %q", err.Error())
		}
		if strings.Contains(err.Error(), "late event") {
			t.Errorf("duplicate field error must precede the late-event rule: %q", err.Error())
		}
		if stderr != "" {
			t.Errorf("fatal record and its dead suffix produce no late notice, got %q", stderr)
		}
		if stdout != idleFieldOrderWantFirstWindow {
			t.Errorf("window output mismatch:\n got: %q\nwant: %q", stdout, idleFieldOrderWantFirstWindow)
		}
	})
}

// TestAggregateDuplicateKeyMalformedJSONKeepsFormatError ensures the existing
// format error still wins when the JSON itself is broken, even when the text
// visibly repeats the key.
func TestAggregateDuplicateKeyMalformedJSONKeepsFormatError(t *testing.T) {
	cases := []struct {
		name string
		line string
	}{
		{"unterminated object", `{"type":"event","key":"a","key":"b"`},
		{"trailing comma", `{"type":"event","key":"a","key":"b",}`},
		{"missing value", `{"type":"event","key":"a","key":}`},
		{"not an object", `["type","event"]`},
		{"bare text", `not json`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := runAggregate(t, tc.line+"\n", 1000)
			var inputErr *InputError
			if !errors.As(err, &inputErr) {
				t.Fatalf("want *InputError, got %T: %v", err, err)
			}
			if inputErr.Line != 1 {
				t.Errorf("line = %d, want 1", inputErr.Line)
			}
			if !strings.Contains(inputErr.Reason, "invalid JSON record") {
				t.Errorf("reason = %q, want the existing invalid JSON record reason", inputErr.Reason)
			}
			if strings.Contains(inputErr.Reason, "more than once") {
				t.Errorf("malformed JSON must keep its format error, got the duplicate wording: %q", inputErr.Reason)
			}
		})
	}
}

// TestAggregateDuplicateKeyAppliesToEveryEntryPoint runs one duplicate record
// through fixed windows, sliding windows and partitioned aggregate: each must
// reject it on the same *InputError path.
func TestAggregateDuplicateKeyAppliesToEveryEntryPoint(t *testing.T) {
	const dupLegacy = `{"type":"event","key":"a","key":"b","time":100,"value":1}`
	const dupPartitioned = `{"type":"event","key":"a","key":"b","time":100,"value":1,"partition":1}`

	t.Run("fixed windows", func(t *testing.T) {
		_, _, err := runAggregate(t, dupLegacy+"\n", 1000)
		wantDuplicateKeyError(t, err, 1)
	})

	t.Run("sliding windows", func(t *testing.T) {
		_, _, err := runSliding(t, dupLegacy+"\n", 1000, 600)
		wantDuplicateKeyError(t, err, 1)
	})

	t.Run("partitioned fixed windows", func(t *testing.T) {
		lines := strings.Join([]string{
			`{"type":"watermark","time":2000,"partition":0}`,
			`{"type":"watermark","time":2000,"partition":1}`,
			dupPartitioned,
		}, "\n") + "\n"
		_, _, err := runPartitioned(t, lines, 1000, 2)
		wantDuplicateKeyError(t, err, 3)
	})

	t.Run("partitioned sliding windows", func(t *testing.T) {
		lines := strings.Join([]string{
			`{"type":"watermark","time":2000,"partition":0}`,
			`{"type":"watermark","time":2000,"partition":1}`,
			dupPartitioned,
		}, "\n") + "\n"
		_, _, err := runPartitionedSliding(t, lines, 1000, 600, 2)
		wantDuplicateKeyError(t, err, 3)
	})
}

// TestAggregateDuplicateKeyIgnoredOnWatermarkAndIdle keeps the historical
// extra-field policy outside event records: a stray, even repeated, "key" on
// a watermark or idle declaration is ignored.
func TestAggregateDuplicateKeyIgnoredOnWatermarkAndIdle(t *testing.T) {
	t.Run("legacy watermark with repeated stray key", func(t *testing.T) {
		input := strings.Join([]string{
			`{"type":"event","key":"a","time":100,"value":2}`,
			`{"type":"watermark","time":1000,"key":"x","key":"y"}`,
		}, "\n") + "\n"
		stdout, stderr, err := runAggregate(t, input, 1000)
		if err != nil {
			t.Fatalf("extra fields on a watermark stay ignored: %v", err)
		}
		if stderr != "" {
			t.Fatalf("unexpected late log: %q", stderr)
		}
		results := parseResultLines(t, stdout)
		if len(results) != 1 || results[0].Key != "a" || results[0].Count != 1 || results[0].Sum != 2 {
			t.Fatalf("stdout = %q, want the window closed by the watermark", stdout)
		}
	})

	t.Run("partitioned watermark and idle carrying a stray key", func(t *testing.T) {
		input := strings.Join([]string{
			`{"type":"watermark","time":1600,"partition":0,"key":"x"}`,
			`{"type":"idle","partition":1,"key":"x"}`,
		}, "\n") + "\n"
		_, _, err := runPartitionedSliding(t, input, 1000, 1000, 2)
		if err != nil {
			t.Fatalf("a stray key on watermark/idle stays an extra field: %v", err)
		}
	})

	t.Run("escaped stray key on an idle record is ignored", func(t *testing.T) {
		input := `{"type":"idle","partition":1,` + escapedKeyField() + `:"x"}` + "\n"
		// One reported partition plus the idle one: the stray key must not
		// turn the declaration into an event duplicate error.
		_, _, err := runPartitionedSliding(t, input, 1000, 1000, 2)
		if err != nil {
			t.Fatalf("an escaped stray key on an idle record stays an extra field: %v", err)
		}
	})
}

// TestAggregateSingleKeyBehaviorUnchanged is the control: records with
// exactly one top-level key keep working exactly as before -- escape
// spellings of the field name and of the key value decode normally, and the
// usual missing/empty/wrong-typed/damaged failures are unchanged.
func TestAggregateSingleKeyBehaviorUnchanged(t *testing.T) {
	t.Run("literal and escaped field name are one key across records", func(t *testing.T) {
		input := strings.Join([]string{
			`{"type":"event","key":"a","time":100,"value":2}`,
			`{"type":"event",` + escapedKeyField() + `:"a","time":200,"value":3}`,
			`{"type":"watermark","time":1000}`,
		}, "\n") + "\n"
		stdout, _, err := runAggregate(t, input, 1000)
		if err != nil {
			t.Fatalf("a single key, however its name is escaped, must work: %v", err)
		}
		results := parseResultLines(t, stdout)
		if len(results) != 1 || results[0].Key != "a" || results[0].Count != 2 || results[0].Sum != 5 {
			t.Fatalf("stdout = %q, want merged count 2 sum 5 for key a", stdout)
		}
	})

	surrogate := wireUnicode("d800")
	cases := []struct {
		name      string
		record    string
		wantCause string
	}{
		{"missing key", `{"type":"event","time":100,"value":1}`, `missing required string field "key"`},
		{"empty key", `{"type":"event","key":"","time":100,"value":1}`, `field "key" must be a non-empty string`},
		{"wrong-typed key", `{"type":"event","key":7,"time":100,"value":1}`, `field "key" must be a JSON string, got 7`},
		{"damaged key", `{"type":"event","key":"a` + surrogate + `b","time":100,"value":1}`, `unpaired high surrogate`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := runAggregate(t, tc.record+"\n", 1000)
			var inputErr *InputError
			if !errors.As(err, &inputErr) {
				t.Fatalf("want *InputError, got %T: %v", err, err)
			}
			if !strings.Contains(inputErr.Reason, tc.wantCause) {
				t.Errorf("reason = %q, want it to contain %q", inputErr.Reason, tc.wantCause)
			}
			if strings.Contains(inputErr.Reason, "more than once") {
				t.Errorf("a single key must not hit the duplicate wording: %q", inputErr.Reason)
			}
		})
	}
}

// TestAggregateDuplicateKeyOnlyKeyFieldIsRestricted pins the rule's exact
// boundary: only a repeated top-level "key" must fail. Repeating any other
// field name (time, value, type, an extra field) keeps the historical
// last-value-wins behavior, because the new validation is scoped to key.
func TestAggregateDuplicateKeyOnlyKeyFieldIsRestricted(t *testing.T) {
	cases := []struct {
		name   string
		record string
	}{
		{"repeated time keeps the last value", `{"type":"event","key":"a","time":100,"time":200,"value":4}`},
		{"repeated value keeps the last value", `{"type":"event","key":"a","time":200,"value":1,"value":4}`},
		{"repeated type keeps event", `{"type":"watermark","type":"event","key":"a","time":200,"value":4}`},
		{"repeated unknown extra field ignored", `{"type":"event","key":"a","time":200,"value":4,"x":1,"x":2}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			input := strings.Join([]string{tc.record, `{"type":"watermark","time":1000}`}, "\n") + "\n"
			stdout, stderr, err := runAggregate(t, input, 1000)
			if err != nil {
				t.Fatalf("repeating a non-key field keeps last-value-wins: %v", err)
			}
			if stderr != "" {
				t.Fatalf("unexpected late log: %q", stderr)
			}
			results := parseResultLines(t, stdout)
			if len(results) != 1 || results[0].Key != "a" || results[0].Count != 1 || results[0].Sum != 4 {
				t.Fatalf("stdout = %q, want one event using the last value (sum 4)", stdout)
			}
		})
	}
}
