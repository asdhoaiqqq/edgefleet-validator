package edgefleet

import (
	"errors"
	"strings"
	"testing"
)

// Regression safeguards for the error priority when an IDLE partition
// receives an event in partitioned sliding mode (window 1000, slide 600, two
// partitions, key "k"). The protected order is:
//
//  1. The record's own content is validated first, in the existing field
//     order key, time, value, partition -- a damaged key (invalid UTF-8
//     bytes, an unpaired or mispaired \u surrogate) or a bad value (missing,
//     wrong type, outside signed 64-bit range) is the reported error even
//     though the event's partition is idle AND its event time is below the
//     effective watermark while still inside an unclosed overlapping window.
//  2. Only with every field legal does the idle-partition rule fire: the
//     event is fatal with the existing "send a watermark to resume it first"
//     reason -- never a late-event skip -- and an event time exactly equal
//     to the effective watermark is not accepted either.
//  3. Only an event that is both legal and from an active partition is
//     judged late against the effective watermark.
//
// The fixture arranges the exact collision: the event time 900 belongs to
// two overlapping windows, [0,1000) (already closed and emitted) and
// [600,1600) (still open); partition 0 has declared idle and partition 1
// has since pushed the effective watermark to 1500, so 900 is also strictly
// below the watermark. Every failure is the existing *InputError on the
// physical line (blank lines count), ends input processing at once, keeps
// the already emitted window exactly as published, never flushes the open
// window, and writes no late-event notice for the bad record or any record
// after it.

// idleEventFixtureLines is the shared history, physical lines 1-7:
//
//	line 1: blank (physical lines count)
//	line 2: event k time=900 value=7 partition 0 -> [0,1000) and [600,1600)
//	line 3: watermark 1000 partition 0
//	line 4: watermark 1000 partition 1 -> effective = 1000; closes [0,1000)
//	                                        (count 1, sum 7); [600,1600) open
//	line 5: idle partition 0            -> p0 idle; p1 keeps effective 1000
//	line 6: watermark 1500 partition 1  -> effective = 1500; [600,1600) end
//	                                        1600 > 1500 stays open
//	line 7: blank
//
// so a time=900 event from idle partition 0 on line 8 sits below the
// effective watermark 1500 while belonging to the still-open [600,1600).
func idleEventFixtureLines() []string {
	return []string{
		``, // line 1 blank
		`{"type":"event","key":"k","time":900,"value":7,"partition":0}`, // line 2
		`{"type":"watermark","time":1000,"partition":0}`,                // line 3
		`{"type":"watermark","time":1000,"partition":1}`,                // line 4: effective = 1000
		`{"type":"idle","partition":0}`,                                 // line 5: p0 idle
		`{"type":"watermark","time":1500,"partition":1}`,                // line 6: effective = 1500
		``, // line 7 blank
	}
}

// idleEventDeadSuffix are the records that must stay completely dead after a
// fatal error on physical line 8:
//
//	line 9:  legal event time=1500 partition 1, would join [600,1600)
//	line 10: legal event time=1400 partition 1, would log a late notice
//	line 11: watermark 1600 partition 1, would close [600,1600)
func idleEventDeadSuffix() []string {
	return []string{
		`{"type":"event","key":"k","time":1500,"value":3,"partition":1}`,
		`{"type":"event","key":"k","time":1400,"value":1,"partition":1}`,
		`{"type":"watermark","time":1600,"partition":1}`,
	}
}

var idleEventWantFirstWindow = `{"key":"k","start":0,"end":1000,"count":1,"sum":7}` + "\n"

// runIdleEventLine8 runs the shared fixture plus the given line-8 record and
// the dead suffix, returning the run's outputs and error.
func runIdleEventLine8(t *testing.T, record string) (string, string, error) {
	t.Helper()
	lines := append(idleEventFixtureLines(), record)
	lines = append(lines, idleEventDeadSuffix()...)
	return runPartitionedSliding(t, strings.Join(lines, "\n"), 1000, 600, 2)
}

// assertIdleEventFieldError checks the common contract of every record whose
// own content is invalid on physical line 8: an *InputError (never an
// *OutputError) on line 8 (blank physical lines 1 and 7 count), a reason
// naming wantField and stating wantCause, with no trace of the idle-partition
// rule or a late-event skip; the late log stays empty and stdout keeps only
// the window closed before line 8 -- the still-open [600,1600) is not
// flushed and the dead suffix adds nothing.
func assertIdleEventFieldError(t *testing.T, stdout, stderr string, err error, wantField, wantCause string) {
	t.Helper()
	if err == nil {
		t.Fatalf("an invalid record must fail fatally; got nil, stdout=%q stderr=%q", stdout, stderr)
	}
	var inputErr *InputError
	if !errors.As(err, &inputErr) {
		t.Fatalf("want *InputError, got %T: %v", err, err)
	}
	var outputErr *OutputError
	if errors.As(err, &outputErr) {
		t.Fatalf("a malformed record must not be reported as an output failure: %v", err)
	}
	if inputErr.Line != 8 {
		t.Errorf("InputError.Line = %d, want 8 (blank physical lines 1 and 7 count)", inputErr.Line)
	}
	if !strings.Contains(inputErr.Reason, wantField) {
		t.Errorf("reason %q must name field %s", inputErr.Reason, wantField)
	}
	if !strings.Contains(inputErr.Reason, wantCause) {
		t.Errorf("reason %q must state the concrete cause %q", inputErr.Reason, wantCause)
	}
	// Record content is validated before the idle-partition rule and the
	// late-event judgement: neither may mask the field error.
	for _, sub := range []string{"idle", "resume", "late event", "watermark"} {
		if strings.Contains(inputErr.Reason, sub) {
			t.Errorf("a field error must not be masked by the idle or late rules, reason %q mentions %q", inputErr.Reason, sub)
		}
	}
	if stderr != "" {
		t.Errorf("an invalid record and its dead suffix must write no late-event notice, got %q", stderr)
	}
	if stdout != idleEventWantFirstWindow {
		t.Errorf("window output mismatch:\n got: %q\nwant: %q", stdout, idleEventWantFirstWindow)
	}
}

// TestSlidingIdleEventDamagedKeyIsInputError covers keys that would lose
// characters when decoded -- invalid UTF-8 bytes and \u escapes with unpaired
// or mispaired surrogate halves -- on an event sent to idle partition 0 at
// time 900 (below the effective watermark 1500, inside the open [600,1600)).
// Each must be the fatal record error on physical line 8, never repaired to
// U+FFFD, never reported as the idle-partition violation and never absorbed
// as a late-event skip.
func TestSlidingIdleEventDamagedKeyIsInputError(t *testing.T) {
	cases := []struct {
		name      string
		key       string // raw JSON string literal, quotes included
		wantCause string
	}{
		{"invalid utf-8 byte", "\"a\xffb\"", `field "key" contains invalid UTF-8 bytes in its character encoding`},
		{"truncated utf-8 sequence", "\"\xe6\xb8\"", `field "key" contains invalid UTF-8 bytes in its character encoding`},
		{"lone high surrogate", `"\uD800"`, `field "key" has an unpaired high surrogate in a Unicode escape`},
		{"lone low surrogate", `"\uDC00"`, `field "key" has an unpaired low surrogate in a Unicode escape`},
		{"high surrogate then text", `"\uD800x"`, `field "key" has an unpaired high surrogate in a Unicode escape`},
		{"high surrogate then non-surrogate escape", `"\uD800\u0041"`, `field "key" has a high surrogate not followed by a low surrogate in a Unicode escape`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			record := `{"type":"event","key":` + tc.key + `,"time":900,"value":1,"partition":0}`
			stdout, stderr, err := runIdleEventLine8(t, record)
			assertIdleEventFieldError(t, stdout, stderr, err, `"key"`, tc.wantCause)
		})
	}
}

// TestSlidingIdleEventInvalidValueIsInputError covers the value field on the
// same idle-partition, below-watermark, open-window event: missing, written
// as a string or a boolean, and beyond either end of the signed 64-bit
// range. Each is the reported error on physical line 8; the idle declaration
// and the watermark must not mask it.
func TestSlidingIdleEventInvalidValueIsInputError(t *testing.T) {
	cases := []struct {
		name      string
		record    string
		wantCause string
	}{
		{
			name:      "value missing",
			record:    `{"type":"event","key":"k","time":900,"partition":0}`,
			wantCause: `missing required integer field "value"`,
		},
		{
			name:      "value written as string",
			record:    `{"type":"event","key":"k","time":900,"value":"1","partition":0}`,
			wantCause: `field "value" must be a signed 64-bit integer, got "1"`,
		},
		{
			name:      "value written as boolean",
			record:    `{"type":"event","key":"k","time":900,"value":true,"partition":0}`,
			wantCause: `field "value" must be a signed 64-bit integer, got true`,
		},
		{
			name:      "value above signed 64-bit range",
			record:    `{"type":"event","key":"k","time":900,"value":9223372036854775808,"partition":0}`,
			wantCause: `field "value" integer out of signed 64-bit range: 9223372036854775808`,
		},
		{
			name:      "value below signed 64-bit range",
			record:    `{"type":"event","key":"k","time":900,"value":-9223372036854775809,"partition":0}`,
			wantCause: `field "value" integer out of signed 64-bit range: -9223372036854775809`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stdout, stderr, err := runIdleEventLine8(t, tc.record)
			assertIdleEventFieldError(t, stdout, stderr, err, `"value"`, tc.wantCause)
		})
	}
}

// TestSlidingIdleEventFieldCheckOrderIgnoresJSONOrder pins the first-error
// rule when one record carries several field problems: the existing
// validation order key, time, value, partition decides which error is
// reported, and rewriting the same fields in a different JSON order must
// yield the identical physical line and reason. A record whose only problem
// is the partition field is included to show that check, too, runs before
// the idle-partition rule.
func TestSlidingIdleEventFieldCheckOrderIgnoresJSONOrder(t *testing.T) {
	cases := []struct {
		name      string
		forward   string
		reversed  string
		wantField string
		wantCause string
	}{
		{
			name:      "damaged key and string value",
			forward:   `{"type":"event","key":"\uD800","time":900,"value":"x","partition":0}`,
			reversed:  `{"type":"event","partition":0,"value":"x","time":900,"key":"\uD800"}`,
			wantField: `"key"`,
			wantCause: `field "key" has an unpaired high surrogate in a Unicode escape`,
		},
		{
			name:      "negative time and string value",
			forward:   `{"type":"event","key":"k","time":-1,"value":"x","partition":0}`,
			reversed:  `{"type":"event","partition":0,"value":"x","time":-1,"key":"k"}`,
			wantField: `"time"`,
			wantCause: `field "time" must be a non-negative integer, got -1`,
		},
		{
			name:      "string value and out-of-range partition",
			forward:   `{"type":"event","key":"k","time":900,"value":"x","partition":9}`,
			reversed:  `{"type":"event","partition":9,"value":"x","time":900,"key":"k"}`,
			wantField: `"value"`,
			wantCause: `field "value" must be a signed 64-bit integer, got "x"`,
		},
		{
			name:      "all four fields broken",
			forward:   `{"type":"event","key":"\uDC00","time":-1,"value":"x","partition":9}`,
			reversed:  `{"type":"event","partition":9,"value":"x","time":-1,"key":"\uDC00"}`,
			wantField: `"key"`,
			wantCause: `field "key" has an unpaired low surrogate in a Unicode escape`,
		},
		{
			name:      "only partition out of range",
			forward:   `{"type":"event","key":"k","time":900,"value":1,"partition":9}`,
			reversed:  `{"type":"event","partition":9,"value":1,"time":900,"key":"k"}`,
			wantField: `"partition"`,
			wantCause: `field "partition" must be an integer in range [0,2), got 9`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stdout, stderr, err := runIdleEventLine8(t, tc.forward)
			assertIdleEventFieldError(t, stdout, stderr, err, tc.wantField, tc.wantCause)
			first := inputErrorFrom(t, err)

			stdout, stderr, err = runIdleEventLine8(t, tc.reversed)
			assertIdleEventFieldError(t, stdout, stderr, err, tc.wantField, tc.wantCause)
			second := inputErrorFrom(t, err)

			if first.Reason != second.Reason {
				t.Errorf("JSON field order changed the first error:\n forward:  %q\n reversed: %q", first.Reason, second.Reason)
			}
		})
	}
}

// TestSlidingIdleEventLegalFieldsReportIdlePartition is the second stage:
// with every field legal, the event for idle partition 0 is fatal with the
// existing idle reason that tells the sender to resume the partition with a
// watermark first. This holds both below the effective watermark (time 900)
// and exactly at it (time 1500): equality with the watermark does not let
// the event in, and neither record becomes a late-event skip. The already
// emitted window stays as published, the open [600,1600) is not flushed,
// and the dead suffix produces nothing.
func TestSlidingIdleEventLegalFieldsReportIdlePartition(t *testing.T) {
	cases := []struct {
		name   string
		record string
	}{
		{
			name:   "event time below the effective watermark",
			record: `{"type":"event","key":"k","time":900,"value":1,"partition":0}`,
		},
		{
			name:   "event time equal to the effective watermark",
			record: `{"type":"event","key":"k","time":1500,"value":1,"partition":0}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stdout, stderr, err := runIdleEventLine8(t, tc.record)
			inputErr := inputErrorFrom(t, err)
			if inputErr.Line != 8 {
				t.Errorf("InputError.Line = %d, want 8 (blank physical lines 1 and 7 count)", inputErr.Line)
			}
			for _, sub := range []string{
				"event for idle partition 0 is not allowed",
				"send a watermark to resume it first",
			} {
				if !strings.Contains(inputErr.Reason, sub) {
					t.Errorf("reason %q must contain %q", inputErr.Reason, sub)
				}
			}
			if strings.Contains(err.Error(), "late event") {
				t.Errorf("an idle-partition event must not be described as a late-event skip: %q", err.Error())
			}
			if stderr != "" {
				t.Errorf("the rejected event and its dead suffix must write no late-event notice, got %q", stderr)
			}
			if stdout != idleEventWantFirstWindow {
				t.Errorf("window output mismatch:\n got: %q\nwant: %q", stdout, idleEventWantFirstWindow)
			}
		})
	}
}

// TestSlidingIdleEventControlActivePartitionLateAndAtWatermark is the normal
// control for the cases above: the same history, but the line-8 event goes
// to the ACTIVE partition 1. Below the effective watermark (time 900) it is
// skipped with exactly one late notice naming line 8 and processing
// continues -- the suffix's legal event joins [600,1600), its own late
// event logs a second notice, and the closing watermark emits the window.
// Exactly at the watermark (time 1500) the event is counted normally in
// every unclosed window containing it.
func TestSlidingIdleEventControlActivePartitionLateAndAtWatermark(t *testing.T) {
	t.Run("below the effective watermark logs late and continues", func(t *testing.T) {
		lines := append(idleEventFixtureLines(),
			`{"type":"event","key":"k","time":900,"value":1,"partition":1}`, // line 8: late vs 1500
		)
		lines = append(lines, idleEventDeadSuffix()...)
		stdout, stderr, err := runPartitionedSliding(t, strings.Join(lines, "\n"), 1000, 600, 2)
		if err != nil {
			t.Fatalf("a legal event on the active partition must not fail: %v", err)
		}
		wantNotices := strings.Join([]string{
			"line 8: late event time=900 below current watermark 1500, skipped",
			"line 10: late event time=1400 below current watermark 1500, skipped",
			"",
		}, "\n")
		if stderr != wantNotices {
			t.Fatalf("late notices mismatch:\n got: %q\nwant: %q", stderr, wantNotices)
		}
		want := strings.Join([]string{
			`{"key":"k","start":0,"end":1000,"count":1,"sum":7}`,
			// The skipped events add nothing; line 9's time=1500 value=3
			// joins the pre-idle contribution: count 2, sum 10.
			`{"key":"k","start":600,"end":1600,"count":2,"sum":10}`,
			``,
		}, "\n")
		if stdout != want {
			t.Fatalf("window output mismatch:\n got: %q\nwant: %q", stdout, want)
		}
	})

	t.Run("equal to the effective watermark is counted", func(t *testing.T) {
		lines := append(idleEventFixtureLines(),
			`{"type":"event","key":"k","time":1500,"value":5,"partition":1}`, // line 8: == watermark, joins [600,1600) and [1200,2200)
			`{"type":"watermark","time":1600,"partition":1}`,                 // line 9: closes [600,1600)
		)
		stdout, stderr, err := runPartitionedSliding(t, strings.Join(lines, "\n"), 1000, 600, 2)
		if err != nil {
			t.Fatalf("an at-watermark event on the active partition must not fail: %v", err)
		}
		if stderr != "" {
			t.Fatalf("an at-watermark event must produce no late notice, got %q", stderr)
		}
		want := strings.Join([]string{
			`{"key":"k","start":0,"end":1000,"count":1,"sum":7}`,
			// The at-watermark event counts fully: count 2, sum 12.
			`{"key":"k","start":600,"end":1600,"count":2,"sum":12}`,
			``,
		}, "\n")
		if stdout != want {
			t.Fatalf("window output mismatch:\n got: %q\nwant: %q", stdout, want)
		}
		// [1200,2200) also holds the time=1500 event but is still open at end
		// of input and must never be flushed.
		if strings.Contains(stdout, `"start":1200`) {
			t.Errorf("[1200,2200) is still open at end of input and must never be emitted: %q", stdout)
		}
	})
}
