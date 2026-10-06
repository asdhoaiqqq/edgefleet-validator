package edgefleet

import (
	"strconv"
	"strings"
	"testing"
)

// Regression safeguards for the interaction between idle declarations and the
// effective (minimum) watermark when several active partitions tie on the
// minimum in partitioned sliding mode (window 1000, slide 600, three
// partitions, one key "k"):
//
//   - With the per-partition watermarks 1600/600/600 the effective watermark
//     is 600 and no window has closed. Declaring ONE of the two slow
//     partitions idle must not advance the effective watermark: the other,
//     still-active partition still pins it at 600, so no window may close
//     early. Only when that last holder of the minimum declares idle does the
//     remaining partition's 1600 become the effective watermark and close the
//     windows.
//   - While one minimum holder is idle, a legal event arriving from the other,
//     still-active minimum holder (time 1000, equal to the would-be window
//     edge and well above the effective 600) is judged against the effective
//     watermark 600 -- never against the fast partition's own 1600 -- so it is
//     neither reported late nor dropped; it joins the still-open overlapping
//     window.
//   - The window that closes last keeps the events both partitions
//     contributed before they went idle. The event at 1300 also belongs to
//     [1200,2200), which stays open at end of input and is never flushed.
//   - The rule is symmetric in which of the two tied partitions idles first,
//     and a repeated idle declaration for an already-idle partition is a
//     no-op: it neither advances the watermark nor re-emits any result while
//     the other slow partition stays active.
//
// Every check goes through the public RunAggregatePartitionedSliding entry
// point; output stays the existing line-per-JSON format with no partition
// field, and the normal path produces neither late notices nor errors.

// tiedMinIdleBaseLines is the shared history physical lines 1-6:
//
//	line 1: p0 event time=700  value=2 -> [0,1000) and [600,1600)
//	line 2: p1 event time=1000 value=3 -> [600,1600)
//	line 3: p2 event time=1300 value=5 -> [600,1600) and [1200,2200)
//	line 4: p0 watermark 1600           -> fast partition races ahead
//	line 5: p1 watermark 600
//	line 6: p2 watermark 600            -> effective min = 600; nothing closes
//
// Windows in play start at 0, 600, 1200, ... with length 1000.
func tiedMinIdleBaseLines() []string {
	return []string{
		`{"type":"event","key":"k","time":700,"value":2,"partition":0}`,
		`{"type":"event","key":"k","time":1000,"value":3,"partition":1}`,
		`{"type":"event","key":"k","time":1300,"value":5,"partition":2}`,
		`{"type":"watermark","time":1600,"partition":0}`,
		`{"type":"watermark","time":600,"partition":1}`,
		`{"type":"watermark","time":600,"partition":2}`,
	}
}

// tiedMinIdleWantOutput is the exact, order-pinned output the full sequence
// must produce, with no partition field:
//
//   - [0,1000) holds only partition 0's time=700 event (the time=1000 events
//     sit on the right edge and must not leak back): count 1, sum 2.
//   - [600,1600) keeps both idle partitions' pre-idle contributions (2 from
//     p0, 3 from p1, 5 from p2) plus the still-active holder's legal event
//     after the first idle declaration (4): count 4, sum 14.
//
// [1200,2200) (holding the time=1300 event, count 1, sum 5) is not among the
// rows: it is still open when input ends.
var tiedMinIdleWantOutput = strings.Join([]string{
	`{"key":"k","start":0,"end":1000,"count":1,"sum":2}`,
	`{"key":"k","start":600,"end":1600,"count":4,"sum":14}`,
	``,
}, "\n")

// TestSlidingPartitionTiedMinWatermarkNoClosureBeforeLastHolderIdles pins the
// output timing: after the three watermarks establish 600 as the effective
// minimum, neither that state nor the first idle declaration (nor repeated
// declarations of the same already-idle partition) may close any window,
// because the other slow partition is still active at 600. A legal event from
// that still-active partition after the first idle declaration is accepted
// with no late notice even though partition 0's own watermark is 1600.
func TestSlidingPartitionTiedMinWatermarkNoClosureBeforeLastHolderIdles(t *testing.T) {
	cases := []struct {
		name      string
		firstIdle int64 // the slow partition that declares idle first (1 or 2)
		active    int64 // the other slow partition, still active at 600
	}{
		{name: "partition 1 idles first, partition 2 holds the minimum", firstIdle: 1, active: 2},
		{name: "partition 2 idles first, partition 1 holds the minimum", firstIdle: 2, active: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lines := tiedMinIdleBaseLines()
			lines = append(lines,
				`{"type":"idle","partition":`+pid(tc.firstIdle)+`}`,
				// Repeating the already-idle declaration must stay a no-op.
				`{"type":"idle","partition":`+pid(tc.firstIdle)+`}`,
				`{"type":"idle","partition":`+pid(tc.firstIdle)+`}`,
				// time 1000 >= effective 600: legal, joins [600,1600), closes
				// nothing. It must not be judged late against p0's own 1600.
				`{"type":"event","key":"k","time":1000,"value":4,"partition":`+pid(tc.active)+`}`,
			)
			stdout, stderr, err := runPartitionedSliding(t, strings.Join(lines, "\n"), 1000, 600, 3)
			if err != nil {
				t.Fatalf("unexpected error while the last minimum holder stays active: %v", err)
			}
			if stderr != "" {
				t.Errorf("the active holder's time=1000 event must not be late vs effective 600, got notice %q", stderr)
			}
			if stdout != "" {
				t.Errorf("no window may close while active partition %d still pins the effective watermark at 600, got output %q", tc.active, stdout)
			}
		})
	}
}

// TestSlidingPartitionTiedMinWatermarkIdleSequence is the primary scenario:
// once the second (last active) holder of the minimum declares idle,
// partition 0's 1600 becomes the effective watermark and closes [0,1000) and
// [600,1600) in that order with the merged counts and sums, preserving the
// events both partitions contributed before idling. [1200,2200) stays open at
// end of input. The whole run produces no late notice and no error.
func TestSlidingPartitionTiedMinWatermarkIdleSequence(t *testing.T) {
	lines := tiedMinIdleBaseLines()
	lines = append(lines,
		`{"type":"idle","partition":1}`,                                  // p1 leaves; p2 still active at 600
		`{"type":"event","key":"k","time":1000,"value":4,"partition":2}`, // legal: joins [600,1600)
		`{"type":"idle","partition":2}`,                                  // last minimum holder leaves: effective = 1600
	)
	stdout, stderr, err := runPartitionedSliding(t, strings.Join(lines, "\n"), 1000, 600, 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stderr != "" {
		t.Fatalf("normal processing must produce no late notice, got %q", stderr)
	}
	if stdout != tiedMinIdleWantOutput {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, tiedMinIdleWantOutput)
	}

	// Pin the exact rows as structured values, and make sure the still-open
	// [1200,2200) window (which holds the time=1300 event) is never emitted.
	got := decodeAggregateResults(t, stdout)
	wantRows := []AggregateResult{
		{Key: "k", Start: 0, End: 1000, Count: 1, Sum: 2},
		{Key: "k", Start: 600, End: 1600, Count: 4, Sum: 14},
	}
	if len(got) != len(wantRows) {
		t.Fatalf("got %d rows, want %d (no [1200,2200) flush at end of input): %v", len(got), len(wantRows), got)
	}
	for i := range wantRows {
		if got[i] != wantRows[i] {
			t.Errorf("row %d = %+v, want %+v", i, got[i], wantRows[i])
		}
	}
}

// TestSlidingPartitionTiedMinWatermarkIdleOrderSymmetric proves both holders
// of the tied minimum obey the same rule regardless of which one declares
// idle first: the legal event after the first declaration is sent by the
// partition that remains active, and the second declaration is what finally
// lets partition 0's 1600 close the windows. Both orderings yield the same two
// result lines, no other output, and no late notices.
func TestSlidingPartitionTiedMinWatermarkIdleOrderSymmetric(t *testing.T) {
	cases := []struct {
		name       string
		firstIdle  int64
		secondIdle int64
		active     int64 // sends the legal time=1000 event between the two declarations
	}{
		{name: "p1 idles then active p2 contributes then idles", firstIdle: 1, secondIdle: 2, active: 2},
		{name: "p2 idles then active p1 contributes then idles", firstIdle: 2, secondIdle: 1, active: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lines := tiedMinIdleBaseLines()
			lines = append(lines,
				`{"type":"idle","partition":`+pid(tc.firstIdle)+`}`,
				`{"type":"event","key":"k","time":1000,"value":4,"partition":`+pid(tc.active)+`}`,
				`{"type":"idle","partition":`+pid(tc.secondIdle)+`}`,
			)
			stdout, stderr, err := runPartitionedSliding(t, strings.Join(lines, "\n"), 1000, 600, 3)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if stderr != "" {
				t.Fatalf("normal processing must produce no late notice, got %q", stderr)
			}
			if stdout != tiedMinIdleWantOutput {
				t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, tiedMinIdleWantOutput)
			}
		})
	}
}

// TestSlidingPartitionTiedMinRepeatedIdleNeverAdvancesWatermark combines the
// no-op guarantee with the timing guarantee: after partition 1 idles, further
// idle declarations for the same already-idle partition 1 must neither close
// windows nor duplicate results while partition 2 still holds the minimum at
// 600. The active holder's time=1000 event still aggregates normally, and the
// single later idle declaration for partition 2 is the one record that closes
// the windows -- exactly once each.
func TestSlidingPartitionTiedMinRepeatedIdleNeverAdvancesWatermark(t *testing.T) {
	lines := tiedMinIdleBaseLines()
	lines = append(lines,
		`{"type":"idle","partition":1}`,
		`{"type":"idle","partition":1}`, // repeated: still-active p2 keeps effective 600
		`{"type":"idle","partition":1}`, // repeated again: same no-op
		`{"type":"event","key":"k","time":1000,"value":4,"partition":2}`,
		`{"type":"idle","partition":2}`, // last minimum holder leaves: closes both windows
		`{"type":"idle","partition":2}`, // repeated after closure: must not re-emit
	)
	stdout, stderr, err := runPartitionedSliding(t, strings.Join(lines, "\n"), 1000, 600, 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stderr != "" {
		t.Fatalf("normal processing must produce no late notice, got %q", stderr)
	}
	if stdout != tiedMinIdleWantOutput {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, tiedMinIdleWantOutput)
	}
	// Each window appears exactly once: a repeated idle declaration must never
	// add or duplicate a result line.
	if n := strings.Count(stdout, `"start":0,"end":1000`); n != 1 {
		t.Errorf("[0,1000) emitted %d times, want exactly 1", n)
	}
	if n := strings.Count(stdout, `"start":600,"end":1600`); n != 1 {
		t.Errorf("[600,1600) emitted %d times, want exactly 1", n)
	}
	if strings.Contains(stdout, `"start":1200`) {
		t.Errorf("[1200,2200) is still open at end of input and must never be emitted: %q", stdout)
	}
}

// pid formats a partition index for embedding in a JSON record.
func pid(p int64) string {
	return strconv.FormatInt(p, 10)
}
