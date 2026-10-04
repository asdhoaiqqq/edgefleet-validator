package edgefleet

import (
	"strings"
	"testing"
)

// A damaged event key -- invalid UTF-8 bytes or a \u escape with an unpaired
// or mispaired surrogate -- must be a fatal input error carrying the physical
// line number, never silently repaired to U+FFFD and counted.

func TestAggregateDamagedKeyIsFatal(t *testing.T) {
	cases := []struct {
		name      string
		key       string // raw JSON string literal, quotes included
		wantInMsg string
	}{
		{"invalid utf-8 byte", "\"a\xffb\"", "UTF-8"},
		{"truncated utf-8 sequence", "\"\xe6\xb8\"", "UTF-8"},
		{"lone high surrogate", `"\uD800"`, "surrogate"},
		{"lone low surrogate", `"\uDC00"`, "surrogate"},
		{"high surrogate then text", `"\uD800x"`, "surrogate"},
		{"high surrogate then high surrogate", `"\uD800\uD800"`, "surrogate"},
		{"high surrogate then non-surrogate escape", `"\uD800A"`, "surrogate"},
		{"low surrogate after valid pair", `"😀\uDC00"`, "surrogate"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			input := `{"type":"event","key":` + tc.key + `,"time":1,"value":1}` + "\n"
			stdout, stderr, err := runAggregate(t, input, 1000)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			inputErr, ok := err.(*InputError)
			if !ok {
				t.Fatalf("expected *InputError, got %T: %v", err, err)
			}
			if inputErr.Line != 1 {
				t.Errorf("line = %d, want 1", inputErr.Line)
			}
			if !strings.Contains(inputErr.Reason, `"key"`) {
				t.Errorf("reason = %q, want it to name the key field", inputErr.Reason)
			}
			if !strings.Contains(inputErr.Reason, tc.wantInMsg) {
				t.Errorf("reason = %q, want substring %q", inputErr.Reason, tc.wantInMsg)
			}
			if stdout != "" {
				t.Errorf("damaged key must not produce output, got %q", stdout)
			}
			if stderr != "" {
				t.Errorf("damaged key must not produce a late notice, got %q", stderr)
			}
		})
	}
}

// The damaged key is fatal even when the event time is below the current
// watermark: it must not be downgraded to a late-event notice.
func TestAggregateDamagedKeyBelowWatermarkIsFatalNotLate(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"watermark","time":1000}`,
		``, // line 2 blank, still counted
		`{"type":"event","key":"\uD800","time":500,"value":1}`,
		`{"type":"event","key":"k","time":1500,"value":1}`, // must never be read
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
	if !strings.Contains(inputErr.Reason, "surrogate") {
		t.Errorf("reason = %q, want it to mention the surrogate problem", inputErr.Reason)
	}
	if strings.Contains(stderr, "late event") {
		t.Errorf("damaged key must not be reported as a late event, stderr = %q", stderr)
	}
	if stdout != "" {
		t.Errorf("no window may close after the fatal record, got %q", stdout)
	}
}

// A damaged key changes no window's count or sum: the run stops at once,
// earlier completed output is retained and still-open windows are not
// flushed.
func TestAggregateDamagedKeyChangesNoWindow(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":1,"value":2}`,
		`{"type":"watermark","time":1000}`,                 // closes [0,1000)
		`{"type":"event","key":"k","time":1500,"value":3}`, // opens [1000,2000)
		`{"type":"event","key":"\uDC00","time":1600,"value":5}`,
		`{"type":"watermark","time":2000}`, // must never be processed
	}, "\n") + "\n"
	stdout, _, err := runAggregate(t, input, 1000)
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
}

// Valid characters aggregate by their decoded value: a literal supplementary
// plane character and its surrogate-pair escape are the same key, and a
// genuine U+FFFD -- literal or escaped -- is a normal key.
func TestAggregateValidUnicodeKeysMerge(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"温度","time":100,"value":1}`,
		`{"type":"event","key":"😀","time":200,"value":2}`,
		"{\"type\":\"event\",\"key\":\"\\uD83D\\uDE00\",\"time\":300,\"value\":3}", // escaped pair: same key as the line above
		`{"type":"event","key":"�","time":400,"value":4}`,
		"{\"type\":\"event\",\"key\":\"\\uFFFD\",\"time\":500,\"value\":5}", // escaped form: same key as the line above
		`{"type":"watermark","time":1000}`,
	}, "\n") + "\n"
	stdout, _, err := runAggregate(t, input, 1000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := strings.Join([]string{
		`{"key":"温度","start":0,"end":1000,"count":1,"sum":1}`,
		`{"key":"�","start":0,"end":1000,"count":2,"sum":9}`,
		`{"key":"😀","start":0,"end":1000,"count":2,"sum":5}`,
		``,
	}, "\n")
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
}

// An escaped backslash makes the following text ordinary characters: the key
// is the literal text D800, not an unpaired surrogate.
func TestAggregateEscapedBackslashKeyIsLiteralText(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"\\uD800","time":100,"value":1}`,
		`{"type":"watermark","time":1000}`,
	}, "\n") + "\n"
	stdout, _, err := runAggregate(t, input, 1000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := `{"key":"\\uD800","start":0,"end":1000,"count":1,"sum":1}` + "\n"
	if stdout != want {
		t.Fatalf("stdout = %q, want %q", stdout, want)
	}
}

// The strict key check applies to the sliding and partitioned entry points
// too, while valid keys keep their existing behavior there.
func TestAggregateDamagedKeyFatalInAllEntryPoints(t *testing.T) {
	bad := `{"type":"event","key":"\uD800","time":1,"value":1,"partition":0}` + "\n"
	good := `{"type":"event","key":"k","time":1,"value":1,"partition":0}` + "\n" +
		`{"type":"watermark","time":1000,"partition":0}` + "\n"
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
			if inputErr.Line != 1 || !strings.Contains(inputErr.Reason, "surrogate") {
				t.Fatalf("error = %v, want line 1 surrogate problem", err)
			}
			if err := r.run(good); err != nil {
				t.Fatalf("valid input must still succeed: %v", err)
			}
		})
	}
}
