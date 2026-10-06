package edgefleet

import (
	"errors"
	"strings"
	"testing"
)

// Regression safeguards for the error priority when an event arrives for a
// partition that has declared itself idle in partitioned sliding mode
// (window 1000, slide 600, two partitions, one key "k"). The ordering under
// protection is the intake order the public entry point has always applied:
//
//  1. The record's own content is interpreted and checked first -- key, then
//     time, then value, then partition (parseEventRecord).
//  2. Only afterwards is the partition's idle state consulted (acceptEvent).
//  3. The late-event judgement against the overall (minimum) watermark runs
//     last.
//
// So an event on an idle partition whose own fields are damaged must fail as a
// record error, never be downgraded to the "event for idle partition" failure
// or to a late-event notice -- even when its event time is strictly below the
// overall watermark while still falling in an overlapping window that has not
// closed. Integrators rely on the returned reason to tell corrupt data apart
// from a send-ordering mistake.
//
// The shared history (idleFieldOrderFixtureLines):
//
//	line 1: p0 event time=700  value=2 -> [0,1000) and [600,1600)
//	line 2: p1 event time=1000 value=3 -> [600,1600)
//	line 3: p0 watermark 1000
//	line 4: p1 declares idle           -> overall watermark 1000; closes [0,1000)
//	line 5: p0 watermark 1500          -> p1 idle, overall watermark 1500;
//	                                      [600,1600) (end 1600) stays open
//	line 6: blank (physical lines count)
//
// Afterwards partition 1 is idle, the overall watermark is 1500, [0,1000) has
// already been emitted (count 1, sum 2) and the overlapping [600,1600) still
// waits with the two pre-idle events (count 2, sum 5). A time=999 record on
// physical line 7 is both (a) on idle partition 1, (b) strictly below the
// overall watermark 1500, and (c) still inside the open [600,1600). This is
// exactly the three-way overlap the priority rule has to disambiguate.

// idleFieldOrderFixtureLines builds the shared history described above.
func idleFieldOrderFixtureLines() []string {
	return []string{
		`{"type":"event","key":"k","time":700,"value":2,"partition":0}`,  // line 1
		`{"type":"event","key":"k","time":1000,"value":3,"partition":1}`, // line 2
		`{"type":"watermark","time":1000,"partition":0}`,                 // line 3
		`{"type":"idle","partition":1}`,                                  // line 4: overall = 1000, closes [0,1000)
		`{"type":"watermark","time":1500,"partition":0}`,                 // line 5: p1 idle; overall = 1500
		``, // line 6: blank, still occupies a physical line number
	}
}

// idleFieldOrderBadLine is the physical line (7) carrying the record under
// test; idleFieldOrderDeadSuffix are the records that must stay completely
// dead after a fatal line-7 failure:
//
//	line 7 would hold the bad record (supplied by the caller)
//	line 8: legal p0 event at time == watermark 1500, would enter [600,1600)
//	line 9: legal p0 event at time 998, would log another late notice if reached
//	line 10: p0 watermark 2200, would close [600,1600) (and [1200,2200)) if reached
func idleFieldOrderDeadSuffix() []string {
	return []string{
		`{"type":"event","key":"k","time":1500,"value":4,"partition":0}`, // line 8
		`{"type":"event","key":"k","time":998,"value":8,"partition":0}`,  // line 9: late if reached
		`{"type":"watermark","time":2200,"partition":0}`,                 // line 10
	}
}

var idleFieldOrderWantFirstWindow = `{"key":"k","start":0,"end":1000,"count":1,"sum":2}` + "\n"

// assertIdleFieldRecordError checks the contract shared by every malformed
// record on the idle partition: an *InputError (never an *OutputError) on
// physical line 7, whose reason names wantField and states wantCause, with no
// trace of the idle-partition rule, a late-event skip or window accumulation.
// The bad record and the dead suffix write no late notice, and stdout keeps
// only the window closed before line 7; the still-open overlapping window is
// never back-filled.
func assertIdleFieldRecordError(t *testing.T, stdout, stderr string, err error, wantField, wantCause string) {
	t.Helper()
	if err == nil {
		t.Fatalf("a malformed idle-partition record must fail fatally; got nil, stdout=%q stderr=%q", stdout, stderr)
	}
	var inputErr *InputError
	if !errors.As(err, &inputErr) {
		t.Fatalf("want *InputError, got %T: %v", err, err)
	}
	var outputErr *OutputError
	if errors.As(err, &outputErr) {
		t.Fatalf("a malformed record must not be reported as an output failure: %v", err)
	}
	// Blank physical line 6 still occupies its number.
	if inputErr.Line != 7 {
		t.Errorf("InputError.Line = %d, want 7 (blank physical line 6 counts)", inputErr.Line)
	}
	if wantField != "" && !strings.Contains(inputErr.Reason, wantField) {
		t.Errorf("reason %q must name field %s", inputErr.Reason, wantField)
	}
	if !strings.Contains(inputErr.Reason, wantCause) {
		t.Errorf("reason %q must state the concrete cause %q", inputErr.Reason, wantCause)
	}
	// Content validation wins over both state-based judgements: the reason
	// must not be the idle hint or the late-event wording.
	if strings.Contains(inputErr.Reason, "idle partition") {
		t.Errorf("a field error must be reported before the idle-partition rule, reason %q", inputErr.Reason)
	}
	if strings.Contains(err.Error(), "late event") {
		t.Errorf("a damaged record must not be described as a late-event skip: %q", err.Error())
	}
	// The record never reached window accumulation; it must not be blamed on a
	// sum overflow or a window interval.
	if strings.Contains(inputErr.Reason, "overflow") || strings.Contains(inputErr.Reason, "window [") {
		t.Errorf("a field error must not be blamed on window accumulation: %q", inputErr.Reason)
	}
	// No late notice for the bad record itself nor for the dead line 9.
	if stderr != "" {
		t.Errorf("late log must stay empty for a damaged record and its dead suffix; got %q", stderr)
	}
	// The fully written [0,1000) result stays exactly as published; the open
	// [600,1600) is not flushed and the dead line-10 watermark never closes it.
	if stdout != idleFieldOrderWantFirstWindow {
		t.Errorf("window output mismatch:\n got: %q\nwant: %q", stdout, idleFieldOrderWantFirstWindow)
	}
}

// TestSlidingIdlePartitionDamagedRecordBeatsIdleAndLate is the core case. The
// line-7 event is on idle partition 1, has time 999 (strictly below the
// overall watermark 1500) yet still lies in the unclosed overlapping
// [600,1600). Every damaged-key and bad-field shape must nevertheless be the
// *InputError on physical line 7 naming the offending field and reason: a
// corrupt key (invalid UTF-8 bytes, an unpaired or mispaired Unicode
// surrogate, a missing or non-string key), a bad time, a missing /
// wrong-typed / out-of-int64-range value, and a missing or out-of-range
// partition. None may be masked by the idle hint or by a late notice.
func TestSlidingIdlePartitionDamagedRecordBeatsIdleAndLate(t *testing.T) {
	cases := []struct {
		name      string
		record    string
		wantField string
		wantCause string
	}{
		// --- key damage: character-encoding corruption, reported as data error
		{
			name:      "invalid raw UTF-8 bytes in key",
			record:    "{\"type\":\"event\",\"key\":\"a\xffb\",\"time\":999,\"value\":1,\"partition\":1}",
			wantField: `"key"`,
			wantCause: `field "key" contains invalid UTF-8 bytes in its character encoding`,
		},
		{
			name:      "unpaired high surrogate in key",
			record:    `{"type":"event","key":"bad\ud800k","time":999,"value":1,"partition":1}`,
			wantField: `"key"`,
			wantCause: `field "key" has an unpaired high surrogate in a Unicode escape`,
		},
		{
			name:      "unpaired low surrogate in key",
			record:    `{"type":"event","key":"bad\udc00k","time":999,"value":1,"partition":1}`,
			wantField: `"key"`,
			wantCause: `field "key" has an unpaired low surrogate in a Unicode escape`,
		},
		{
			name:      "high surrogate not followed by a low surrogate in key",
			record:    `{"type":"event","key":"bad\ud800Ak","time":999,"value":1,"partition":1}`,
			wantField: `"key"`,
			wantCause: `field "key" has an unpaired high surrogate in a Unicode escape`,
		},
		{
			name:      "key missing",
			record:    `{"type":"event","time":999,"value":1,"partition":1}`,
			wantField: `"key"`,
			wantCause: `missing required string field "key"`,
		},
		{
			name:      "key not a JSON string",
			record:    `{"type":"event","key":7,"time":999,"value":1,"partition":1}`,
			wantField: `"key"`,
			wantCause: `field "key" must be a JSON string, got 7`,
		},
		{
			name:      "empty key",
			record:    `{"type":"event","key":"","time":999,"value":1,"partition":1}`,
			wantField: `"key"`,
			wantCause: `field "key" must be a non-empty string`,
		},
		// --- the remaining fields, after a legal key
		{
			name:      "time missing",
			record:    `{"type":"event","key":"z","value":1,"partition":1}`,
			wantField: `"time"`,
			wantCause: `missing required integer field "time"`,
		},
		{
			name:      "value missing",
			record:    `{"type":"event","key":"z","time":999,"partition":1}`,
			wantField: `"value"`,
			wantCause: `missing required integer field "value"`,
		},
		{
			name:      "value written as a string",
			record:    `{"type":"event","key":"z","time":999,"value":"oops","partition":1}`,
			wantField: `"value"`,
			wantCause: `field "value" must be a signed 64-bit integer, got "oops"`,
		},
		{
			name:      "value above signed 64-bit range",
			record:    `{"type":"event","key":"z","time":999,"value":9223372036854775808,"partition":1}`,
			wantField: `"value"`,
			wantCause: `field "value" integer out of signed 64-bit range: 9223372036854775808`,
		},
		{
			name:      "value below signed 64-bit range",
			record:    `{"type":"event","key":"z","time":999,"value":-9223372036854775809,"partition":1}`,
			wantField: `"value"`,
			wantCause: `field "value" integer out of signed 64-bit range: -9223372036854775809`,
		},
		{
			name:      "partition missing with legal value",
			record:    `{"type":"event","key":"z","time":999,"value":1}`,
			wantField: `"partition"`,
			wantCause: `missing required integer field "partition"`,
		},
		{
			name:      "partition out of configured range with legal value",
			record:    `{"type":"event","key":"z","time":999,"value":1,"partition":9}`,
			wantField: `"partition"`,
			wantCause: `field "partition" must be an integer in range [0,2), got 9`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lines := append(idleFieldOrderFixtureLines(), tc.record)
			lines = append(lines, idleFieldOrderDeadSuffix()...)
			stdout, stderr, err := runPartitionedSliding(t, strings.Join(lines, "\n"), 1000, 600, 2)
			assertIdleFieldRecordError(t, stdout, stderr, err, tc.wantField, tc.wantCause)
		})
	}
}

// TestSlidingIdlePartitionFieldOrderIsKeyTimeValuePartition pins the
// validation order when a single line-7 event carries several field errors at
// once: the first error encountered in the fixed key, time, value, partition
// order is the one returned, regardless of which other fields are also wrong.
// Reordering how the fields are written in the JSON must not change the line
// number or the reason -- the textual order is irrelevant.
func TestSlidingIdlePartitionFieldOrderIsKeyTimeValuePartition(t *testing.T) {
	cases := []struct {
		name      string
		a         string // one JSON field spelling
		b         string // the same fields in a different textual order
		wantField string
		wantCause string
	}{
		{
			name:      "broken key and broken value: key wins",
			a:         `{"type":"event","key":"bad\ud800k","time":999,"value":"x","partition":1}`,
			b:         `{"type":"event","partition":1,"value":"x","time":999,"key":"bad\ud800k"}`,
			wantField: `"key"`,
			wantCause: `unpaired high surrogate`,
		},
		{
			name:      "legal key, broken time and broken value: time wins",
			a:         `{"type":"event","key":"z","time":"999","value":"x","partition":1}`,
			b:         `{"type":"event","partition":1,"value":"x","time":"999","key":"z"}`,
			wantField: `"time"`,
			wantCause: `field "time" must be a signed 64-bit integer, got "999"`,
		},
		{
			name:      "legal key and time, broken value and broken partition: value wins",
			a:         `{"type":"event","key":"z","time":999,"value":"x","partition":9}`,
			b:         `{"type":"event","partition":9,"value":"x","time":999,"key":"z"}`,
			wantField: `"value"`,
			wantCause: `field "value" must be a signed 64-bit integer, got "x"`,
		},
		{
			name:      "legal key time value, partition out of range: partition is the first error",
			a:         `{"type":"event","key":"z","time":999,"value":1,"partition":9}`,
			b:         `{"type":"event","partition":9,"value":1,"time":999,"key":"z"}`,
			wantField: `"partition"`,
			wantCause: `field "partition" must be an integer in range [0,2), got 9`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			first := runIdleFieldError(t, tc.a)
			second := runIdleFieldError(t, tc.b)
			if first.Line != 7 || second.Line != 7 {
				t.Errorf("line numbers = %d and %d, want 7 for both JSON field orders", first.Line, second.Line)
			}
			if first.Reason != second.Reason {
				t.Errorf("JSON field order changed the first error:\n order a: %q\n order b: %q", first.Reason, second.Reason)
			}
			if !strings.Contains(first.Reason, tc.wantField) {
				t.Errorf("reason %q must name %s", first.Reason, tc.wantField)
			}
			if !strings.Contains(first.Reason, tc.wantCause) {
				t.Errorf("reason %q must state %q", first.Reason, tc.wantCause)
			}
			if strings.Contains(first.Reason, "idle partition") {
				t.Errorf("the field error must precede the idle-partition rule: %q", first.Reason)
			}
		})
	}
}

// TestSlidingIdlePartitionLegalFieldsReportsIdle is the second-priority rule:
// when every field is legal but the partition is still idle, the run fails on
// the idle rule with the resume hint -- not a late notice -- even for an event
// time strictly below the overall watermark (999 < 1500) and for an event time
// exactly equal to it. Equality with the overall watermark must not let the
// event slip through: an idle partition can only be resumed by a watermark
// record. The dead suffix stays dead and the open overlapping window is never
// back-filled.
func TestSlidingIdlePartitionLegalFieldsReportsIdle(t *testing.T) {
	cases := []struct {
		name string
		time string
	}{
		{name: "event time strictly below overall watermark", time: "999"},
		{name: "event time exactly equal to overall watermark", time: "1500"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			record := `{"type":"event","key":"z","time":` + tc.time + `,"value":1,"partition":1}`
			lines := append(idleFieldOrderFixtureLines(), record)
			lines = append(lines, idleFieldOrderDeadSuffix()...)
			stdout, stderr, err := runPartitionedSliding(t, strings.Join(lines, "\n"), 1000, 600, 2)
			if err == nil {
				t.Fatalf("an event for an idle partition must fail; got nil, stdout=%q", stdout)
			}
			var inputErr *InputError
			if !errors.As(err, &inputErr) {
				t.Fatalf("want *InputError, got %T: %v", err, err)
			}
			if inputErr.Line != 7 {
				t.Errorf("InputError.Line = %d, want 7", inputErr.Line)
			}
			for _, sub := range []string{
				"event for idle partition 1 is not allowed",
				"send a watermark to resume it first",
			} {
				if !strings.Contains(inputErr.Reason, sub) {
					t.Errorf("reason %q must contain %q", inputErr.Reason, sub)
				}
			}
			// The record's fields are legal: no field must be blamed.
			for _, f := range []string{`"key"`, `"time"`, `"value"`, `"partition"`} {
				if strings.Contains(inputErr.Reason, f) {
					t.Errorf("an idle-partition failure must not blame field %s: %q", f, inputErr.Reason)
				}
			}
			if stderr != "" {
				t.Errorf("an idle-partition event is fatal, not a late skip; got late log %q", stderr)
			}
			if stdout != idleFieldOrderWantFirstWindow {
				t.Errorf("the open overlapping window must not be back-filled:\n got: %q\nwant: %q", stdout, idleFieldOrderWantFirstWindow)
			}
		})
	}
}

// TestSlidingIdlePartitionActiveControlLateSkipsAndEqualCounts is the normal
// control using the identical history and physical line 7, but sent to the
// still-active partition 0 rather than idle partition 1:
//
//   - A field-legal event with time 999 < overall watermark 1500 is only
//     recorded as late (one notice naming line 7) and processing continues; it
//     adds nothing to any window even though 999 is inside the open
//     [600,1600).
//   - A field-legal event with time exactly 1500 is not late: it joins the
//     unclosed [600,1600) (1500 is its only containing window), moving count
//     and sum.
//   - Partition 0's watermark reaching 1600 then closes [600,1600) with the
//     two pre-idle events plus the time==1500 event (count 3, sum 2+3+4 = 9);
//     the skipped 999/6 contributes nothing.
//
// No idle error appears because partition 0 never idled, and resume/window
// rules are otherwise unchanged.
func TestSlidingIdlePartitionActiveControlLateSkipsAndEqualCounts(t *testing.T) {
	lines := append(idleFieldOrderFixtureLines(),
		`{"type":"event","key":"k","time":999,"value":6,"partition":0}`,  // line 7: late vs overall 1500
		`{"type":"event","key":"k","time":1500,"value":4,"partition":0}`, // line 8: equal, joins [600,1600)
		`{"type":"watermark","time":1600,"partition":0}`,                 // line 9: closes [600,1600)
	)
	stdout, stderr, err := runPartitionedSliding(t, strings.Join(lines, "\n"), 1000, 600, 2)
	if err != nil {
		t.Fatalf("the active partition's legal events must not fail: %v", err)
	}
	wantNotice := "line 7: late event time=999 below current watermark 1500, skipped\n"
	if stderr != wantNotice {
		t.Fatalf("late notice mismatch:\n got: %q\nwant: %q", stderr, wantNotice)
	}
	if strings.Count(stderr, "skipped\n") != 1 {
		t.Errorf("the late event must be reported exactly once, got %q", stderr)
	}
	if strings.Contains(stderr, "idle") {
		t.Errorf("an active-partition event must not mention idleness: %q", stderr)
	}
	want := strings.Join([]string{
		`{"key":"k","start":0,"end":1000,"count":1,"sum":2}`,
		// Pre-idle 700/2 and 1000/3, plus the equal-to-watermark 1500/4; the
		// skipped 999/6 adds nothing: count 3 / sum 9.
		`{"key":"k","start":600,"end":1600,"count":3,"sum":9}`,
		``,
	}, "\n")
	if stdout != want {
		t.Fatalf("window output mismatch:\n got: %q\nwant: %q", stdout, want)
	}
	got := decodeAggregateResults(t, stdout)
	wantRows := []AggregateResult{
		{Key: "k", Start: 0, End: 1000, Count: 1, Sum: 2},
		{Key: "k", Start: 600, End: 1600, Count: 3, Sum: 9},
	}
	if len(got) != len(wantRows) {
		t.Fatalf("got %d rows, want %d: %v", len(got), len(wantRows), got)
	}
	for i := range wantRows {
		if got[i] != wantRows[i] {
			t.Errorf("row %d = %+v, want %+v", i, got[i], wantRows[i])
		}
	}
}

// TestSlidingIdlePartitionResumeKeepsWindowClosureCompatible proves the
// existing resume and window-closure rules stay compatible alongside the
// priority guarantee: idle partition 1 resumes with a watermark at least the
// overall watermark (1500, equality resumes), a post-resume event at 1500
// joins the open [600,1600), and the window then closes on the merged minimum
// 1600 with both partitions' contributions (count 3, sum 9). No field error,
// idle error or late notice occurs on the normal path.
func TestSlidingIdlePartitionResumeKeepsWindowClosureCompatible(t *testing.T) {
	lines := append(idleFieldOrderFixtureLines(),
		`{"type":"watermark","time":1500,"partition":1}`,                 // line 7: resume at the overall watermark
		`{"type":"event","key":"k","time":1500,"value":4,"partition":1}`, // line 8: joins [600,1600)
		`{"type":"watermark","time":1600,"partition":0}`,                 // line 9: min stays 1500, closes nothing new
		`{"type":"watermark","time":1600,"partition":1}`,                 // line 10: min = 1600, closes [600,1600)
	)
	stdout, stderr, err := runPartitionedSliding(t, strings.Join(lines, "\n"), 1000, 600, 2)
	if err != nil {
		t.Fatalf("the resume and merged close sequence must succeed: %v", err)
	}
	if stderr != "" {
		t.Fatalf("the normal resume path must produce no notice, got %q", stderr)
	}
	want := strings.Join([]string{
		`{"key":"k","start":0,"end":1000,"count":1,"sum":2}`,
		// Pre-idle 700/2 and 1000/3, plus the resumed partition's 1500/4.
		`{"key":"k","start":600,"end":1600,"count":3,"sum":9}`,
		``,
	}, "\n")
	if stdout != want {
		t.Fatalf("window output mismatch:\n got: %q\nwant: %q", stdout, want)
	}
}

// runIdleFieldError runs the shared history plus the given line-7 record (the
// dead suffix is unnecessary when only the error is compared) and returns the
// *InputError, failing the test on any other outcome.
func runIdleFieldError(t *testing.T, record string) *InputError {
	t.Helper()
	lines := append(idleFieldOrderFixtureLines(), record)
	_, _, err := runPartitionedSliding(t, strings.Join(lines, "\n"), 1000, 600, 2)
	return inputErrorFrom(t, err)
}
