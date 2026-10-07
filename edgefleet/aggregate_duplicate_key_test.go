package edgefleet

import (
	"strings"
	"testing"
)

// scanAggregateObject counts top-level members by their decoded name: direct
// and escaped spellings of key merge, case does not, and a key nested in
// another member's object or array (or merely appearing inside a string) is
// not top-level.
func TestScanAggregateObjectCountsDecodedTopLevelKeys(t *testing.T) {
	cases := []struct {
		name     string
		line     string
		wantKeys int
		wantErr  bool
	}{
		{"single key", `{"type":"event","key":"a","time":1,"value":1}`, 1, false},
		{"no key", `{"type":"watermark","time":1}`, 0, false},
		{"duplicate key", `{"key":"a","key":"b"}`, 2, false},
		{"triple key", `{"key":"a","key":"b","key":"c"}`, 3, false},
		{"escaped name after direct name", "{\"key\":\"a\",\"\\u006bey\":\"b\"}", 2, false},
		{"direct name after escaped name", "{\"\\u006bey\":\"b\",\"key\":\"a\"}", 2, false},
		{"case differs", `{"KEY":"a","key":"b"}`, 1, false},
		{"keys nested in object", `{"key":"a","meta":{"key":"x","key":"y"}}`, 1, false},
		{"keys nested in array of objects", `{"key":"a","x":[{"key":1},{"key":2}]}`, 1, false},
		{"letters inside a string", `{"type":"event","note":"a \"key\" and another key","key":"a"}`, 1, false},
		{"missing value", `{"key":"a","key":}`, 0, true},
		{"unterminated object", `{"key":"a","key":"b"`, 0, true},
		{"trailing data", `{"key":"a","key":"b"}x`, 0, true},
		{"trailing comma", `{"key":"a","key":"b",}`, 0, true},
		{"not an object", `[1,2]`, 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			obj, got, err := scanAggregateObject(tc.line)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("want a parse error, got obj=%v count=%d", obj, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected parse error: %v", err)
			}
			if got != tc.wantKeys {
				t.Errorf("key count = %d, want %d", got, tc.wantKeys)
			}
		})
	}
}

// Every way of writing the same top-level field twice is a fatal record error
// naming the physical line and the key field, even when the two values are
// identical or one member name uses an escape.
func TestAggregateDuplicateKeyIsFatal(t *testing.T) {
	cases := []struct {
		name string
		line string
	}{
		{"different values", `{"type":"event","key":"a","key":"b","time":1,"value":1}`},
		{"identical values", `{"type":"event","key":"a","key":"a","time":1,"value":1}`},
		{"direct then escaped name", "{\"type\":\"event\",\"key\":\"a\",\"\\u006bey\":\"b\",\"time\":1,\"value\":1}"},
		{"escaped then direct name", "{\"type\":\"event\",\"\\u006bey\":\"b\",\"key\":\"a\",\"time\":1,\"value\":1}"},
		{"members before type", `{"key":"a","key":"b","type":"event","time":1,"value":1}`},
		{"three keys", `{"type":"event","key":"a","key":"b","key":"c","time":1,"value":1}`},
		{"whitespace around names", `{"type":"event", "key" : "a", "key" : "b", "time":1, "value":1}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			input := strings.Join([]string{
				`{"type":"event","key":"prior","time":1,"value":9}`, // line 1
				``,                                 // line 2 blank, still counted
				tc.line,                            // line 3
				`{"type":"watermark","time":1000}`, // line 4 must never be read
			}, "\n") + "\n"
			stdout, stderr, err := runAggregate(t, input, 1000)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			inputErr, ok := err.(*InputError)
			if !ok {
				t.Fatalf("expected *InputError, got %T: %v", err, err)
			}
			if inputErr.Line != 3 {
				t.Errorf("line = %d, want 3 (blank line 2 counts)", inputErr.Line)
			}
			if !strings.Contains(inputErr.Reason, `duplicate field "key"`) {
				t.Errorf("reason = %q, want the duplicate key field reason", inputErr.Reason)
			}
			if stdout != "" || stderr != "" {
				t.Errorf("the duplicate record opens and closes no window; got stdout=%q stderr=%q", stdout, stderr)
			}
		})
	}
}

// The duplicate decision is about the field name, not the value: an empty
// first key, a non-string first key and a damaged first or second key are all
// reported as the duplicate rather than the field-level failure.
func TestAggregateDuplicateKeyBeatsFieldLegality(t *testing.T) {
	cases := []struct {
		name        string
		line        string
		mustNotHave string
	}{
		{"empty earlier key", `{"type":"event","key":"","key":"b","time":1,"value":1}`, "non-empty"},
		{"non-string earlier key", `{"type":"event","key":5,"key":"b","time":1,"value":1}`, "JSON string"},
		{"damaged earlier key", `{"type":"event","key":"\uDC00","key":"b","time":1,"value":1}`, "surrogate"},
		{"damaged later key", `{"type":"event","key":"a","key":"\uDC00","time":1,"value":1}`, "surrogate"},
		{"wrong time type also present", `{"type":"event","key":"a","key":"b","time":"oops","value":1}`, `"time"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stdout, stderr, err := runAggregate(t, tc.line+"\n", 1000)
			inputErr, ok := err.(*InputError)
			if !ok {
				t.Fatalf("expected *InputError, got %T: %v", err, err)
			}
			if !strings.Contains(inputErr.Reason, `duplicate field "key"`) {
				t.Fatalf("reason = %q, want the duplicate key reason", inputErr.Reason)
			}
			if strings.Contains(inputErr.Reason, tc.mustNotHave) {
				t.Errorf("reason = %q must not be downgraded to a %q field error", inputErr.Reason, tc.mustNotHave)
			}
			if stdout != "" || stderr != "" {
				t.Errorf("got stdout=%q stderr=%q", stdout, stderr)
			}
		})
	}
}

// The duplicate is rejected before the watermark and partition state rules:
// below the current watermark it is not a late-event notice, and on an idle
// partition it is not an idle-partition error.
func TestAggregateDuplicateKeyBeatsLateAndIdle(t *testing.T) {
	t.Run("below the watermark is not a late notice", func(t *testing.T) {
		input := strings.Join([]string{
			`{"type":"watermark","time":1000}`,
			``, // line 2 blank, still counted
			`{"type":"event","key":"a","key":"b","time":500,"value":1}`,
			`{"type":"event","key":"later","time":1500,"value":1}`, // never read
		}, "\n") + "\n"
		stdout, stderr, err := runAggregate(t, input, 1000)
		inputErr, ok := err.(*InputError)
		if !ok {
			t.Fatalf("expected *InputError, got %T: %v", err, err)
		}
		if inputErr.Line != 3 {
			t.Errorf("line = %d, want 3", inputErr.Line)
		}
		if !strings.Contains(inputErr.Reason, `duplicate field "key"`) {
			t.Fatalf("reason = %q, want duplicate field", inputErr.Reason)
		}
		if strings.Contains(stderr, "late event") {
			t.Errorf("duplicate key must not be reported late, stderr = %q", stderr)
		}
		if stdout != "" {
			t.Errorf("no window may close after the fatal record, got %q", stdout)
		}
	})

	t.Run("on an idle partition is not an idle-partition error", func(t *testing.T) {
		var stdout, stderr strings.Builder
		input := strings.Join([]string{
			`{"type":"event","key":"k","time":100,"value":2,"partition":0}`,
			`{"type":"watermark","time":1000,"partition":0}`,
			`{"type":"idle","partition":0}`,
			`{"type":"event","key":"a","key":"b","time":500,"value":1,"partition":0}`,
		}, "\n") + "\n"
		err := RunAggregatePartitioned(strings.NewReader(input), 1000, 1, &stdout, &stderr)
		inputErr, ok := err.(*InputError)
		if !ok {
			t.Fatalf("expected *InputError, got %T: %v", err, err)
		}
		if !strings.Contains(inputErr.Reason, `duplicate field "key"`) {
			t.Fatalf("reason = %q, want duplicate field", inputErr.Reason)
		}
		if strings.Contains(inputErr.Reason, "idle partition") {
			t.Errorf("reason = %q must not be downgraded to an idle-partition error", inputErr.Reason)
		}
		if strings.Contains(stderr.String(), "late event") {
			t.Errorf("must not produce a late notice, stderr = %q", stderr.String())
		}
	})
}

// Only an event's own top level is checked: a key nested in another member's
// object or array and the letters inside a string are not repetitions.
func TestAggregateDuplicateKeyScopeIsTopLevelEvent(t *testing.T) {
	lines := []string{
		`{"type":"event","key":"a","meta":{"key":"x","key":"y"},"time":1,"value":7}`,
		`{"type":"event","key":"a","x":[{"key":"1"},{"key":"2"}],"time":2,"value":1}`,
		`{"type":"event","key":"a","note":"the key field is mentioned here","time":3,"value":2}`,
		`{"type":"watermark","time":1000}`,
	}
	stdout, stderr, err := runAggregate(t, strings.Join(lines, "\n")+"\n", 1000)
	if err != nil {
		t.Fatalf("nested and quoted keys are not duplicates: %v", err)
	}
	if stderr != "" {
		t.Fatalf("unexpected stderr: %q", stderr)
	}
	want := `{"key":"a","start":0,"end":1000,"count":3,"sum":10}` + "\n"
	if stdout != want {
		t.Fatalf("stdout = %q, want %q", stdout, want)
	}
}

// A duplicate key on a watermark or idle record is still just an ignored
// extra field: only events carry a key.
func TestAggregateDuplicateKeyOnNonEventIgnored(t *testing.T) {
	t.Run("legacy watermark", func(t *testing.T) {
		input := strings.Join([]string{
			`{"type":"event","key":"k","time":1,"value":5}`,
			`{"type":"watermark","time":1000,"key":"a","key":"b"}`,
		}, "\n") + "\n"
		stdout, stderr, err := runAggregate(t, input, 1000)
		if err != nil {
			t.Fatalf("extra keys on a watermark stay ignored: %v", err)
		}
		if stderr != "" {
			t.Fatalf("unexpected stderr: %q", stderr)
		}
		want := `{"key":"k","start":0,"end":1000,"count":1,"sum":5}` + "\n"
		if stdout != want {
			t.Fatalf("stdout = %q, want %q", stdout, want)
		}
	})

	t.Run("partitioned watermark and idle", func(t *testing.T) {
		var stdout, stderr strings.Builder
		input := strings.Join([]string{
			`{"type":"event","key":"k","time":100,"value":2,"partition":0}`,
			`{"type":"event","key":"k","time":200,"value":3,"partition":1}`,
			`{"type":"watermark","time":1000,"partition":0,"key":"a","key":"b"}`,
			`{"type":"watermark","time":1000,"partition":1}`,
			`{"type":"idle","partition":1,"key":"a","key":"b"}`,
		}, "\n") + "\n"
		err := RunAggregatePartitioned(strings.NewReader(input), 1000, 2, &stdout, &stderr)
		if err != nil {
			t.Fatalf("extra keys on watermark/idle stay ignored: %v", err)
		}
		if stderr.Len() != 0 {
			t.Fatalf("unexpected stderr: %q", stderr.String())
		}
		if !strings.Contains(stdout.String(), `"count":2`) {
			t.Fatalf("the idle record should still close the merged window, got %q", stdout.String())
		}
	})
}

// A line that is not one complete JSON object keeps the format error even
// when the visible members repeat key; the duplicate rule starts only after
// the JSON is recognized as a complete event.
func TestAggregateMalformedJSONBeatsDuplicateKey(t *testing.T) {
	cases := []string{
		`{"type":"event","key":"a","key":}`,         // missing second value
		`{"type":"event","key":"a","key":"b"`,       // unterminated
		`{"type":"event","key":"a","key":"b",}`,     // trailing comma
		`{"type":"event","key":"a","key":"b"} junk`, // trailing data
		`{"type":"event","key":"a","key":1} [1]`,    // trailing JSON
	}
	for _, line := range cases {
		t.Run(line, func(t *testing.T) {
			_, _, err := runAggregate(t, line+"\n", 1000)
			inputErr, ok := err.(*InputError)
			if !ok {
				t.Fatalf("expected *InputError, got %T: %v", err, err)
			}
			if !strings.Contains(inputErr.Reason, "invalid JSON") {
				t.Errorf("reason = %q, want the existing invalid JSON reason", inputErr.Reason)
			}
			if strings.Contains(inputErr.Reason, "duplicate") {
				t.Errorf("malformed JSON must not be reported as a duplicate: %q", inputErr.Reason)
			}
		})
	}
}

// Non-object JSON keeps the behavior it had before the streaming scan: a
// valid non-object value is an invalid JSON record, while JSON null unmarshals
// into a nil map without error and therefore reaches the missing-type report.
func TestAggregateNonObjectJSONKeepsLegacyError(t *testing.T) {
	cases := []struct {
		name      string
		line      string
		wantInMsg string
	}{
		{"null", `null`, `missing required string field "type"`},
		{"empty object", `{}`, `missing required string field "type"`},
		{"boolean", `true`, "invalid JSON"},
		{"number", `42`, "invalid JSON"},
		{"string", `"event"`, "invalid JSON"},
		{"array", `[1,2]`, "invalid JSON"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := runAggregate(t, tc.line+"\n", 1000)
			inputErr, ok := err.(*InputError)
			if !ok {
				t.Fatalf("expected *InputError, got %T: %v", err, err)
			}
			if !strings.Contains(inputErr.Reason, tc.wantInMsg) {
				t.Fatalf("reason = %q, want substring %q", inputErr.Reason, tc.wantInMsg)
			}
			if strings.Contains(inputErr.Reason, "duplicate") {
				t.Fatalf("a non-object must never be a duplicate: %q", inputErr.Reason)
			}
		})
	}
}

// The run stops at the duplicate record: the event contributes no count or
// sum, later records are not processed, still-open windows are not flushed,
// and the already fully written window results stay on standard output.
func TestAggregateDuplicateKeyStopsRunAndKeepsPriorOutput(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":1,"value":2}`,
		`{"type":"watermark","time":1000}`,                 // closes [0,1000)
		`{"type":"event","key":"k","time":1500,"value":3}`, // opens [1000,2000)
		`{"type":"event","key":"k","time":1600,"value":4}`,
		`{"type":"event","key":"k","key":"dup","time":1700,"value":99}`, // fatal, line 5
		`{"type":"watermark","time":2000}`,                              // must never be processed
	}, "\n") + "\n"
	stdout, stderr, err := runAggregate(t, input, 1000)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if _, ok := err.(*InputError); !ok {
		t.Fatalf("expected *InputError, got %T: %v", err, err)
	}
	want := `{"key":"k","start":0,"end":1000,"count":1,"sum":2}` + "\n"
	if stdout != want {
		t.Fatalf("stdout = %q, want only the previously closed window %q", stdout, want)
	}
	if stderr != "" {
		t.Fatalf("a structural field error produces no late notice, got %q", stderr)
	}
}

// A single key keeps its existing behavior in every entry point: direct
// characters and equivalent JSON escapes merge by the decoded string, while a
// duplicate key is rejected by each.
func TestAggregateDuplicateKeyAcrossEntryPoints(t *testing.T) {
	bad := `{"type":"event","key":"a","key":"b","time":1,"value":1,"partition":0}` + "\n"
	good := strings.Join([]string{
		"{\"type\":\"event\",\"key\":\"\\u0061\",\"time\":1,\"value\":1,\"partition\":0}", // escaped "a": same key
		`{"type":"event","key":"a","time":2,"value":2,"partition":0}`,
		`{"type":"watermark","time":1000,"partition":0}`,
	}, "\n") + "\n"
	runs := []struct {
		name string
		run  func(input string) error
	}{
		{"fixed", func(input string) error {
			return RunAggregate(strings.NewReader(input), 1000, &strings.Builder{}, &strings.Builder{})
		}},
		{"sliding", func(input string) error {
			return RunAggregateSliding(strings.NewReader(input), 1000, 600, &strings.Builder{}, &strings.Builder{})
		}},
		{"partitioned", func(input string) error {
			return RunAggregatePartitioned(strings.NewReader(input), 1000, 1, &strings.Builder{}, &strings.Builder{})
		}},
	}
	for _, r := range runs {
		t.Run(r.name, func(t *testing.T) {
			err := r.run(bad)
			inputErr, ok := err.(*InputError)
			if !ok {
				t.Fatalf("expected *InputError, got %T: %v", err, err)
			}
			if inputErr.Line != 1 || !strings.Contains(inputErr.Reason, `duplicate field "key"`) {
				t.Fatalf("error = %v, want line 1 duplicate key", err)
			}
			var out, late strings.Builder
			switch r.name {
			case "fixed":
				err = RunAggregate(strings.NewReader(good), 1000, &out, &late)
			case "sliding":
				err = RunAggregateSliding(strings.NewReader(good), 1000, 600, &out, &late)
			case "partitioned":
				err = RunAggregatePartitioned(strings.NewReader(good), 1000, 1, &out, &late)
			}
			if err != nil {
				t.Fatalf("a single escaped/direct key must still merge: %v", err)
			}
			if !strings.Contains(out.String(), `"count":2`) {
				t.Fatalf("the escaped and direct key must merge, got %q", out.String())
			}
		})
	}
}
