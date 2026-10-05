package edgefleet

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
)

// Regression safeguards for the ordering between record validation and the
// late-event check in RunAggregatePartitionedSliding with overlapping sliding
// windows. Field validation runs first: an event whose event time is strictly
// below the effective (minimum) watermark is skipped as late ONLY when the
// whole record is legal. A malformed record must fail with *InputError --
// naming the physical line (blank lines count), the offending field and the
// field's own reason -- even when its event time still falls inside an open
// overlapping window, and even when a legal value at that time would overflow
// that window's accumulated sum; it must never be downgraded to a normal late
// skip or blamed on the cumulative sum. Scope is the public
// RunAggregatePartitionedSliding entry point with window length 1000 and slide
// interval 600; no new entry points or options are introduced.
//
// Shared stream prefix (two partitions at different watermarks):
//
//	line 1: blank (physical lines count)
//	line 2: event k time=700  value=7 partition 0 -> [0,1000) and [600,1600)
//	line 3: watermark time=2000 partition 0       -> fast partition; effective still unknown
//	line 4: watermark time=1000 partition 1       -> effective = min(2000,1000) = 1000;
//	                                                  closes only [0,1000); [600,1600) stays open
//	line 5: blank
//	line 6: event k time=1200 value=5 partition 0 -> open [600,1600) (count 2, sum 12)
//	                                                  and [1200,2200) (count 1, sum 5)
//	line 7: blank
//
// Every bad record below arrives on line 8 with event time 900, which is
// strictly below the effective watermark 1000 yet still inside the unclosed
// [600,1600). The fast partition's own 2000 must never be the judging mark.
func partitionedSlidingLateOrderPrefix() []string {
	return []string{
		``, // line 1 blank
		`{"type":"event","key":"k","time":700,"value":7,"partition":0}`, // line 2
		`{"type":"watermark","time":2000,"partition":0}`,                // line 3: p0 races ahead
		`{"type":"watermark","time":1000,"partition":1}`,                // line 4: effective 1000, closes [0,1000)
		``, // line 5 blank
		`{"type":"event","key":"k","time":1200,"value":5,"partition":0}`, // line 6
		``, // line 7 blank
	}
}

// Records following the fatal line 8: a legal event and watermarks that would
// close the open overlapping windows if any of them were processed. A fatal
// *InputError stops the run at once, so they must add neither results nor late
// notices.
func partitionedSlidingLateOrderDeadTail() []string {
	return []string{
		`{"type":"event","key":"k","time":1300,"value":9,"partition":1}`, // line 9: legal, never processed
		`{"type":"watermark","time":5000,"partition":1}`,                 // line 10
		`{"type":"watermark","time":5000,"partition":0}`,                 // line 11
	}
}

// partitionedSlidingLateOrderClosedOutput is the single result that was fully
// written before line 8 and must stay exactly as published afterwards.
var partitionedSlidingLateOrderClosedOutput = `{"key":"k","start":0,"end":1000,"count":1,"sum":7}` + "\n"

func TestAggregatePartitionedSlidingMalformedRecordBelowWatermarkIsInputError(t *testing.T) {
	cases := []struct {
		name   string
		record string
		// wantSubs all must appear in the InputError reason.
		wantSubs []string
	}{
		{
			name:     "value missing",
			record:   `{"type":"event","key":"k","time":900,"partition":0}`,
			wantSubs: []string{`missing required integer field "value"`},
		},
		{
			name:     "value written as string",
			record:   `{"type":"event","key":"k","time":900,"value":"7","partition":0}`,
			wantSubs: []string{`field "value" must be a signed 64-bit integer, got "7"`},
		},
		{
			name:     "value above signed 64-bit range",
			record:   `{"type":"event","key":"k","time":900,"value":9223372036854775808,"partition":0}`,
			wantSubs: []string{`field "value" integer out of signed 64-bit range: 9223372036854775808`},
		},
		{
			name:     "value below signed 64-bit range",
			record:   `{"type":"event","key":"k","time":900,"value":-9223372036854775809,"partition":0}`,
			wantSubs: []string{`field "value" integer out of signed 64-bit range: -9223372036854775809`},
		},
		{
			name:     "partition missing with legal value",
			record:   `{"type":"event","key":"k","time":900,"value":1}`,
			wantSubs: []string{`missing required integer field "partition"`},
		},
		{
			name:     "partition above configured range with legal value",
			record:   `{"type":"event","key":"k","time":900,"value":1,"partition":2}`,
			wantSubs: []string{`field "partition" must be an integer in range [0,2), got 2`},
		},
		{
			name:     "partition negative with legal value",
			record:   `{"type":"event","key":"k","time":900,"value":1,"partition":-1}`,
			wantSubs: []string{`field "partition" must be an integer in range [0,2), got -1`},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lines := append(partitionedSlidingLateOrderPrefix(), tc.record)
			lines = append(lines, partitionedSlidingLateOrderDeadTail()...)
			stdout, stderr, err := runPartitionedSliding(t, strings.Join(lines, "\n"), 1000, 600, 2)
			if err == nil {
				t.Fatal("expected *InputError for the malformed below-watermark record, got nil")
			}
			var inputErr *InputError
			if !errors.As(err, &inputErr) {
				t.Fatalf("expected *InputError, got %T: %v", err, err)
			}
			// Blank physical lines 1, 5 and 7 count: the bad record is line 8.
			if inputErr.Line != 8 {
				t.Errorf("InputError.Line = %d, want 8 (blank lines must count)", inputErr.Line)
			}
			for _, sub := range tc.wantSubs {
				if !strings.Contains(inputErr.Reason, sub) {
					t.Errorf("reason = %q, want substring %q", inputErr.Reason, sub)
				}
			}
			// The failure is the record's own field problem, never a late skip
			// and never the window's cumulative sum -- the late check and the
			// window updates are both downstream of field validation.
			if strings.Contains(inputErr.Reason, "cumulative sum overflow") {
				t.Errorf("a malformed record must not be blamed on the cumulative sum: %q", inputErr.Reason)
			}
			if strings.Contains(inputErr.Reason, "late event") {
				t.Errorf("a malformed record must not be reported as a late event: %q", inputErr.Reason)
			}
			if stderr != "" {
				t.Fatalf("the malformed record must produce no late-event notice, got stderr %q", stderr)
			}
			// The fully written [0,1000) result stays exactly as published; the
			// still-open overlapping windows are not flushed early, and the
			// trailing legal event and watermarks are dead after the failure.
			if stdout != partitionedSlidingLateOrderClosedOutput {
				t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, partitionedSlidingLateOrderClosedOutput)
			}
		})
	}
}

// TestAggregatePartitionedSlidingLegalLateEventOnlyNoticesAndNeverAggregates is
// the compatible control: a fully legal record strictly below the effective
// watermark is skipped with exactly one notice naming the physical line, the
// event time and the effective watermark (not the fast partition's own 2000),
// processing continues, and the skipped event changes neither count nor sum of
// the open overlapping window. Its value is math.MaxInt64, which would push
// the open window's accumulated sum (12) out of range were it ever added --
// the late skip happens before any window update, so the run must not fail
// with a cumulative-sum overflow. An event at a time exactly equal to the
// effective watermark still enters the window containing it, and an event
// below the fast partition's own mark but not below the effective watermark is
// accepted as well; the windows close with the correct counts and sums once
// the effective watermark allows it.
func TestAggregatePartitionedSlidingLegalLateEventOnlyNoticesAndNeverAggregates(t *testing.T) {
	lines := append(partitionedSlidingLateOrderPrefix(),
		fmt.Sprintf(`{"type":"event","key":"k","time":900,"value":%d,"partition":0}`, math.MaxInt64), // line 8: legal but late (900 < effective 1000)
		``, // line 9 blank
		// 1000 == effective watermark: equality is on time, so the event enters
		// [600,1600) even though 1000 is far below partition 0's own 2000.
		`{"type":"event","key":"k","time":1000,"value":3,"partition":1}`, // line 10
		// 1500 is below the fast partition's 2000 but not below the effective
		// 1000: the minimum watermark is the only judging mark, so it is
		// counted into [600,1600) and [1200,2200).
		`{"type":"event","key":"k","time":1500,"value":4,"partition":1}`, // line 11
		`{"type":"watermark","time":1600,"partition":1}`,                 // line 12: effective 1600 closes [600,1600)
		``, // line 13 blank
		`{"type":"watermark","time":2200,"partition":1}`, // line 14: min(2000,2200)=2000 closes nothing new
		`{"type":"watermark","time":2200,"partition":0}`, // line 15: effective 2200 closes [1200,2200)
	)
	stdout, stderr, err := runPartitionedSliding(t, strings.Join(lines, "\n"), 1000, 600, 2)
	if err != nil {
		t.Fatalf("a legal late event skipped before aggregation must not fail the run: %v", err)
	}

	// Exactly one notice: the late event at line 8, judged against the
	// effective watermark 1000 -- never the fast partition's 2000.
	wantNotice := "line 8: late event time=900 below current watermark 1000, skipped\n"
	if stderr != wantNotice {
		t.Fatalf("late notice mismatch:\n got: %q\nwant: %q", stderr, wantNotice)
	}
	if n := strings.Count(stderr, "\n"); n != 1 {
		t.Errorf("exactly one late notice expected, got %d: %q", n, stderr)
	}

	want := strings.Join([]string{
		// Closed before the late event arrived; unchanged.
		`{"key":"k","start":0,"end":1000,"count":1,"sum":7}`,
		// t=700 v=7, t=1200 v=5, t=1000 v=3 and t=1500 v=4 merge here. The
		// late t=900 with value MaxInt64 adds neither a fourth/... count nor
		// its value: count 4, sum 19, never an overflow failure.
		`{"key":"k","start":600,"end":1600,"count":4,"sum":19}`,
		// t=1200 v=5 and t=1500 v=4 only; the late t=900 is not in this window.
		`{"key":"k","start":1200,"end":2200,"count":2,"sum":9}`,
		``,
	}, "\n")
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}

	got := decodeAggregateResults(t, stdout)
	wantRows := []AggregateResult{
		{Key: "k", Start: 0, End: 1000, Count: 1, Sum: 7},
		{Key: "k", Start: 600, End: 1600, Count: 4, Sum: 19},
		{Key: "k", Start: 1200, End: 2200, Count: 2, Sum: 9},
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
