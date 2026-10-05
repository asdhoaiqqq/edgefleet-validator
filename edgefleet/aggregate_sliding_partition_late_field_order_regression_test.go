package edgefleet

import (
	"errors"
	"math"
	"strings"
	"testing"
)

// Regression safeguards for the ordering between record validation and the
// late-event judgement in partitioned sliding mode (window 1000, slide 600,
// two partitions whose watermarks already differ and whose effective
// (minimum) watermark is established):
//
//   - A record whose own fields are invalid must fail with *InputError at its
//     physical line even when its event time is strictly below the effective
//     watermark (and still inside an unclosed overlapping window). It must
//     never be absorbed as a normal late-event skip, and it must never be
//     blamed on a window cumulative-sum overflow that the record never reached.
//   - A record with every field legal is judged late against the EFFECTIVE
//     watermark and skipped with exactly one notice, changing no window. Its
//     value may be a legal integer that would overflow the open window's
//     accumulated sum: the skip still wins over the sum check.
//   - An event with time exactly equal to the effective watermark is not late:
//     it joins the window containing it and the window closes with the right
//     count and sum once the minimum watermark allows. The fast partition's
//     own, larger watermark can neither judge lateness nor close windows by
//     itself.
//
// Blank physical lines count toward the line number in every scenario.

// lateFieldOrderPrefix builds the shared history (window 1000, slide 600,
// partitions 2):
//
//	line 1: blank (physical lines count)
//	line 2: event k time=900 value=7 partition 0 -> [0,1000) and [600,1600)
//	line 3: watermark 5000 partition 0           -> fast partition races ahead
//	line 4: watermark 1000 partition 1           -> effective min = 1000;
//	                                               closes only [0,1000);
//	                                               [600,1600) stays open
//	line 5: blank
//
// so a time=999 record on line 6 is strictly below the effective watermark
// 1000 (yet still inside the open [600,1600)), and partition 0's own
// watermark 5000 is not the value that judges it.
func lateFieldOrderPrefix() []string {
	return []string{
		``, // line 1 blank
		`{"type":"event","key":"k","time":900,"value":7,"partition":0}`, // line 2
		`{"type":"watermark","time":5000,"partition":0}`,                // line 3
		`{"type":"watermark","time":1000,"partition":1}`,                // line 4: effective = 1000
		``, // line 5 blank
	}
}

// lateFieldOrderSuffix are the records that must be completely dead after a
// fatal record error on line 6:
//
//	line 6 would hold the bad record (supplied by the caller)
//	line 7: legal event at time == watermark 1000, would enter [600,1600)
//	line 8: legal time=998 event, would log another late notice if reached
//	line 9: watermark 1600 partition 1, would close [600,1600) if reached
func lateFieldOrderSuffix() []string {
	return []string{
		`{"type":"event","key":"k","time":1000,"value":3,"partition":1}`, // line 7
		`{"type":"event","key":"k","time":998,"value":1,"partition":1}`,  // line 8 late if reached
		`{"type":"watermark","time":1600,"partition":1}`,                 // line 9
	}
}

var lateFieldOrderWantFirstWindow = `{"key":"k","start":0,"end":1000,"count":1,"sum":7}` + "\n"

// TestSlidingPartitionInvalidLateRecordIsInputError covers every invalid
// field shape on an event that is unambiguously late (time 999 < effective
// watermark 1000) and still inside the open [600,1600): value missing, value
// written as a string, value outside signed 64-bit range, and (with a legal
// value) partition missing or out of the configured range. Each must be an
// *InputError on physical line 6 naming the offending field and the concrete
// reason -- not a late-event skip, not a cumulative sum overflow. Earlier
// fully written output is retained, the open window is not retroactively
// emitted, and no later result or late notice may appear.
func TestSlidingPartitionInvalidLateRecordIsInputError(t *testing.T) {
	cases := []struct {
		name      string
		record    string
		wantField string // the field name the reason must quote
		wantCause string // a distinctive substring of the concrete reason
	}{
		{
			name:      "value missing",
			record:    `{"type":"event","key":"k","time":999,"partition":1}`,
			wantField: `"value"`,
			wantCause: `missing required integer field "value"`,
		},
		{
			name:      "value written as string",
			record:    `{"type":"event","key":"k","time":999,"value":"oops","partition":1}`,
			wantField: `"value"`,
			wantCause: `field "value" must be a signed 64-bit integer, got "oops"`,
		},
		{
			name:      "value above signed 64-bit range",
			record:    `{"type":"event","key":"k","time":999,"value":9223372036854775808,"partition":1}`,
			wantField: `"value"`,
			wantCause: `field "value" integer out of signed 64-bit range: 9223372036854775808`,
		},
		{
			name:      "value below signed 64-bit range",
			record:    `{"type":"event","key":"k","time":999,"value":-9223372036854775809,"partition":1}`,
			wantField: `"value"`,
			wantCause: `field "value" integer out of signed 64-bit range: -9223372036854775809`,
		},
		{
			name:      "partition missing with legal value",
			record:    `{"type":"event","key":"k","time":999,"value":1}`,
			wantField: `"partition"`,
			wantCause: `missing required integer field "partition"`,
		},
		{
			name:      "partition out of configured range with legal value",
			record:    `{"type":"event","key":"k","time":999,"value":1,"partition":9}`,
			wantField: `"partition"`,
			wantCause: `field "partition" must be an integer in range [0,2), got 9`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lines := append(lateFieldOrderPrefix(), tc.record)
			lines = append(lines, lateFieldOrderSuffix()...)
			stdout, stderr, err := runPartitionedSliding(t, strings.Join(lines, "\n"), 1000, 600, 2)

			if err == nil {
				t.Fatalf("an invalid late record must fail fatally; got nil, stdout=%q stderr=%q", stdout, stderr)
			}
			var inputErr *InputError
			if !errors.As(err, &inputErr) {
				t.Fatalf("want *InputError, got %T: %v", err, err)
			}
			var outputErr *OutputError
			if errors.As(err, &outputErr) {
				t.Fatalf("a malformed record must not be reported as an output failure: %v", err)
			}
			// Blank lines 1 and 5 still occupy physical numbers.
			if inputErr.Line != 6 {
				t.Errorf("InputError.Line = %d, want 6 (blank physical lines count)", inputErr.Line)
			}
			if !strings.Contains(inputErr.Reason, tc.wantField) {
				t.Errorf("reason %q must name field %s", inputErr.Reason, tc.wantField)
			}
			if !strings.Contains(inputErr.Reason, tc.wantCause) {
				t.Errorf("reason %q must state the concrete cause %q", inputErr.Reason, tc.wantCause)
			}
			// The record never reached window accumulation: it must not be
			// mislabelled as a cumulative sum overflow, and the physical line
			// must carry the field error rather than anything window-shaped.
			if strings.Contains(inputErr.Reason, "cumulative sum overflow") ||
				strings.Contains(inputErr.Reason, "window [") {
				t.Errorf("a field error below the watermark must not be blamed on window accumulation: %q", inputErr.Reason)
			}
			if strings.Contains(err.Error(), "late event") {
				t.Errorf("an invalid record must not be described as a late-event skip: %q", err.Error())
			}

			// No late notice for the bad record itself nor for the dead line 8.
			if stderr != "" {
				t.Errorf("late log must stay empty when the record itself is invalid; got %q", stderr)
			}
			// The fully written [0,1000) result stays exactly as published; the
			// still-open [600,1600) is not flushed and line 9 never closes it.
			if stdout != lateFieldOrderWantFirstWindow {
				t.Errorf("window output mismatch:\n got: %q\nwant: %q", stdout, lateFieldOrderWantFirstWindow)
			}
		})
	}
}

// TestSlidingPartitionValidLateRecordSkipsWithoutSumError is the control for
// the cases above: the same history and the same physical line 6, but the
// record's fields are all legal. It is judged against the effective watermark
// 1000 (partition 0 is already at 5000 and must not substitute that), and the
// open [600,1600) already holds math.MaxInt64, so its value 1 would overflow
// the cumulative sum if it were ever added. The late check wins: exactly one
// notice naming line 6, time 999 and watermark 1000 is written, processing
// continues, and no window count or sum changes due to that event. A legal
// event at time exactly 1000 then joins [600,1600) (value 0 moves only the
// count); repeating partition 0's watermark 5000 closes nothing, and the
// minimum watermark reaching 1600 emits the correct totals.
func TestSlidingPartitionValidLateRecordSkipsWithoutSumError(t *testing.T) {
	const M = math.MaxInt64
	lines := []string{
		``, // line 1 blank
		// Enters [0,1000) and [600,1600); both accumulate to MaxInt64.
		`{"type":"event","key":"big","time":900,"value":9223372036854775807,"partition":0}`, // line 2
		`{"type":"watermark","time":5000,"partition":0}`,                                    // line 3: fast partition ahead
		`{"type":"watermark","time":1000,"partition":1}`,                                    // line 4: effective 1000
		``, // line 5 blank
		// Fields legal; 999 < effective 1000 although 999 is inside the open
		// [600,1600), and value 1 would take that window's MaxInt64 over the
		// top. It must be skipped with one notice, never a sum error.
		`{"type":"event","key":"big","time":999,"value":1,"partition":1}`, // line 6 late
		// Time exactly equal to the effective watermark: not late, joins the
		// only window containing 1000, [600,1600); value 0 moves count only.
		`{"type":"event","key":"big","time":1000,"value":0,"partition":1}`, // line 7
		`{"type":"watermark","time":5000,"partition":0}`,                   // line 8: min stays 1000, closes nothing
		`{"type":"watermark","time":1600,"partition":1}`,                   // line 9: min = 1600 closes [600,1600)
	}
	stdout, stderr, err := runPartitionedSliding(t, strings.Join(lines, "\n"), 1000, 600, 2)
	if err != nil {
		t.Fatalf("a legal late event must skip, not fail even at the sum boundary: %v", err)
	}
	wantNotice := "line 6: late event time=999 below current watermark 1000, skipped\n"
	if stderr != wantNotice {
		t.Fatalf("late notice mismatch:\n got: %q\nwant: %q", stderr, wantNotice)
	}
	// The notice judges against the effective minimum, never p0's own 5000.
	if strings.Count(stderr, "skipped\n") != 1 {
		t.Errorf("late event must be reported exactly once, got %q", stderr)
	}
	if strings.Contains(stderr, "5000") {
		t.Errorf("latency must be judged by the effective watermark 1000, not the fast partition's 5000: %q", stderr)
	}
	want := strings.Join([]string{
		`{"key":"big","start":0,"end":1000,"count":1,"sum":9223372036854775807}`,
		// The skipped event added neither count nor value; the time==1000 event
		// moved only the count, and the sum rests exactly on MaxInt64.
		`{"key":"big","start":600,"end":1600,"count":2,"sum":9223372036854775807}`,
		``,
	}, "\n")
	if stdout != want {
		t.Fatalf("window output mismatch:\n got: %q\nwant: %q", stdout, want)
	}
	// Reparse to pin the exact signed 64-bit totals the notices must not touch.
	got := decodeAggregateResults(t, stdout)
	wantRows := []AggregateResult{
		{Key: "big", Start: 0, End: 1000, Count: 1, Sum: M},
		{Key: "big", Start: 600, End: 1600, Count: 2, Sum: M},
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

// TestSlidingPartitionInvalidLateRecordControlIsActuallyLate proves the
// history used above genuinely sits below the watermark: with the bad field
// repaired, the line-6 record is skipped as a normal late event exactly once
// and aggregation proceeds, closing [600,1600) later on the legitimate
// contributions alone (count 2, sum 10 -- the skipped value 1 adds nothing).
func TestSlidingPartitionInvalidLateRecordControlIsActuallyLate(t *testing.T) {
	lines := append(lateFieldOrderPrefix(),
		`{"type":"event","key":"k","time":999,"value":1,"partition":1}`, // line 6: same shape, now valid
	)
	lines = append(lines, lateFieldOrderSuffix()...)
	stdout, stderr, err := runPartitionedSliding(t, strings.Join(lines, "\n"), 1000, 600, 2)
	if err != nil {
		t.Fatalf("control record is valid and must not fail: %v", err)
	}
	wantNotices := strings.Join([]string{
		"line 6: late event time=999 below current watermark 1000, skipped",
		"line 8: late event time=998 below current watermark 1000, skipped",
		"",
	}, "\n")
	if stderr != wantNotices {
		t.Fatalf("late notices mismatch:\n got: %q\nwant: %q", stderr, wantNotices)
	}
	want := strings.Join([]string{
		`{"key":"k","start":0,"end":1000,"count":1,"sum":7}`,
		// line 2's 7 and line 7's time==watermark 3; the late lines add
		// nothing: count 2 / sum 10.
		`{"key":"k","start":600,"end":1600,"count":2,"sum":10}`,
		``,
	}, "\n")
	if stdout != want {
		t.Fatalf("window output mismatch:\n got: %q\nwant: %q", stdout, want)
	}
}
