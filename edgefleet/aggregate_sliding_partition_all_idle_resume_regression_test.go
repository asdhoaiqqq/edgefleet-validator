package edgefleet

import (
	"errors"
	"strings"
	"testing"
)

// Regression coverage for the "every partition idle, then one resumes"
// sequence in partitioned sliding mode. Window length 1000, slide 600, two
// partitions, one key "k", so the windows in play are the genuinely
// overlapping [0,1000), [600,1600), [1200,2200), [1800,2800), ...
//
// The guarantee under test is that all-idle is neither end of input nor an
// infinite watermark:
//
//   - When an effective watermark had already been produced, the last active
//     partition's idle declaration must retain that watermark and the open
//     windows' accumulated contents, emit nothing that is not yet closable and
//     never re-emit a closed window.
//   - When one partition later resumes with a legal watermark while the other
//     stays idle, closure is driven by that partition alone, and the merged
//     count and sum still include BOTH partitions' pre-idle contributions plus
//     the resumed partition's new legal events.
//   - A resume time that clears the partition's own old watermark but lies
//     below the retained overall watermark is still the existing input error
//     (physical line, partition and violated overall lower bound); prior
//     output survives and records after the failed one cannot close windows.
//     Time exactly equal to the retained overall watermark resumes.
//   - When no partition ever reported a watermark, buffering survives the
//     idle declarations (which must not conjure output), and the first
//     partition to resume with a watermark closes eligible windows alone,
//     without waiting for the partition that stays idle.
//
// Every check goes through the public RunAggregatePartitionedSliding entry
// point; output stays the existing line-per-JSON format ordered by window end
// with no partition field, and the normal paths produce no late notices.

// allIdleResumeFixtureLines builds physical lines 1-9 of the history in which
// an effective watermark has already been produced and one window already
// closed before every partition goes idle:
//
//	line 1: p0 event k time=700  value=2 -> [0,1000) and [600,1600)
//	line 2: p0 event k time=1300 value=4 -> [600,1600) and [1200,2200)
//	line 3: p1 event k time=1000 value=3 -> [600,1600)
//	line 4: p1 event k time=1300 value=5 -> [600,1600) and [1200,2200)
//	line 5: p0 watermark 1000            -> effective still unknown (p1 silent)
//	line 6: p1 watermark 500             -> effective = 500, closes nothing
//	line 7: p1 idle                      -> only p0 active: effective = 1000, closes [0,1000)
//	line 8: blank                        -> counts toward physical line numbers
//	line 9: p0 idle                      -> all partitions idle: retain 1000
//
// At the all-idle point:
//
//   - [0,1000) is already closed with count 1, sum 2 (p0's time=700 event).
//   - [600,1600) is still open with BOTH partitions' pre-idle contributions:
//     count 4, sum 14 (2+4 from p0, 3+5 from p1).
//   - [1200,2200) is still open with count 2, sum 9 (4 from p0, 5 from p1).
func allIdleResumeFixtureLines() []string {
	return []string{
		`{"type":"event","key":"k","time":700,"value":2,"partition":0}`,
		`{"type":"event","key":"k","time":1300,"value":4,"partition":0}`,
		`{"type":"event","key":"k","time":1000,"value":3,"partition":1}`,
		`{"type":"event","key":"k","time":1300,"value":5,"partition":1}`,
		`{"type":"watermark","time":1000,"partition":0}`,
		`{"type":"watermark","time":500,"partition":1}`,
		`{"type":"idle","partition":1}`,
		``,
		`{"type":"idle","partition":0}`,
	}
}

// allIdleWantFirstWindow is the one and only result the history may have
// produced by the time every partition is idle: [0,1000) closed on line 7.
var allIdleWantFirstWindow = `{"key":"k","start":0,"end":1000,"count":1,"sum":2}` + "\n"

// TestSlidingPartitionAllIdleRetainsWatermarkAndOpenWindows pins the all-idle
// instant itself: the last active partition's idle declaration must not close
// [600,1600) or [1200,2200) (their ends are beyond the retained 1000), must
// not flush them as if input had ended, must not push the watermark to
// infinity, and must not re-emit the already closed [0,1000). Repeated idle
// declarations after that stay no-ops with identical output.
func TestSlidingPartitionAllIdleRetainsWatermarkAndOpenWindows(t *testing.T) {
	stdout, stderr, err := runPartitionedSliding(t, strings.Join(allIdleResumeFixtureLines(), "\n"), 1000, 600, 2)
	if err != nil {
		t.Fatalf("going idle on every partition must not be an error: %v", err)
	}
	if stderr != "" {
		t.Fatalf("the all-idle declarations must produce no late notice, got %q", stderr)
	}
	if stdout != allIdleWantFirstWindow {
		t.Fatalf("all-idle output mismatch:\n got: %q\nwant: %q", stdout, allIdleWantFirstWindow)
	}

	// Repeated idle declarations for either already-idle partition change
	// neither the retained watermark nor the output, and still never close the
	// open windows or duplicate the closed one.
	lines := append(allIdleResumeFixtureLines(),
		`{"type":"idle","partition":0}`,
		`{"type":"idle","partition":1}`,
		`{"type":"idle","partition":0}`,
	)
	stdout, stderr, err = runPartitionedSliding(t, strings.Join(lines, "\n"), 1000, 600, 2)
	if err != nil {
		t.Fatalf("repeated idle declarations must stay legal: %v", err)
	}
	if stderr != "" {
		t.Fatalf("repeated idle declarations must produce no notice, got %q", stderr)
	}
	if stdout != allIdleWantFirstWindow {
		t.Fatalf("repeated idle declarations changed the output:\n got: %q\nwant: %q", stdout, allIdleWantFirstWindow)
	}
	if n := strings.Count(stdout, `"start":0,"end":1000`); n != 1 {
		t.Errorf("[0,1000) emitted %d times, want exactly 1", n)
	}
	if strings.Contains(stdout, `"start":600`) || strings.Contains(stdout, `"start":1200`) {
		t.Errorf("windows still open at the retained watermark 1000 must not be flushed: %q", stdout)
	}
}

// TestSlidingPartitionAllIdleResumeOnePartitionDrivesClosure is the primary
// resume scenario: after every partition went idle with 1000 retained,
// partition 1 resumes exactly at that watermark while partition 0 stays idle
// forever. Partition 1's advancing watermark alone closes the windows; the
// resulting rows merge both partitions' pre-idle contributions with the
// resumed partition's new legal events, arrive ordered by window end, carry no
// partition field, and leave [1800,2800) unflushed at end of input.
func TestSlidingPartitionAllIdleResumeOnePartitionDrivesClosure(t *testing.T) {
	lines := append(allIdleResumeFixtureLines(),
		// line 10: resume at the retained overall watermark (and above p1's
		// own old 500). Effective = 1000 from p1 alone; no new window closes
		// and [0,1000) is not re-emitted.
		`{"type":"watermark","time":1000,"partition":1}`,
		// line 11: legal new event on the resumed partition (1300 >= 1000, so
		// no late notice); joins the still-open overlapping windows.
		`{"type":"event","key":"k","time":1300,"value":6,"partition":1}`,
		// line 12: p1 alone advances to 1600 while p0 stays idle; this must be
		// enough to close [600,1600).
		`{"type":"watermark","time":1600,"partition":1}`,
		// line 13: one more legal event feeding both [1200,2200) and the new
		// [1800,2800), which stays open at end of input.
		`{"type":"event","key":"k","time":1800,"value":7,"partition":1}`,
		// line 14: closes [1200,2200); [1800,2800) stays open.
		`{"type":"watermark","time":2200,"partition":1}`,
	)
	stdout, stderr, err := runPartitionedSliding(t, strings.Join(lines, "\n"), 1000, 600, 2)
	if err != nil {
		t.Fatalf("resume and close sequence must succeed: %v", err)
	}
	if stderr != "" {
		t.Fatalf("the normal resume path must produce no late notice, got %q", stderr)
	}
	want := strings.Join([]string{
		`{"key":"k","start":0,"end":1000,"count":1,"sum":2}`,
		// Pre-idle 2+4 from p0 and 3+5 from p1, plus p1's post-resume 6:
		// count 5, sum 20. Dropping either idle partition's pre-idle
		// contributions would change both numbers.
		`{"key":"k","start":600,"end":1600,"count":5,"sum":20}`,
		// Pre-idle 4 from p0 and 5 from p1, plus p1's post-resume events 6
		// (time 1300) and 7 (time 1800, which belongs to this window too).
		`{"key":"k","start":1200,"end":2200,"count":4,"sum":22}`,
		``,
	}, "\n")
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}

	// Structured pin: exactly three rows in end order, no [1800,2800) flush at
	// end of input, and every row carrying the merged totals.
	got := decodeAggregateResults(t, stdout)
	wantRows := []AggregateResult{
		{Key: "k", Start: 0, End: 1000, Count: 1, Sum: 2},
		{Key: "k", Start: 600, End: 1600, Count: 5, Sum: 20},
		{Key: "k", Start: 1200, End: 2200, Count: 4, Sum: 22},
	}
	if len(got) != len(wantRows) {
		t.Fatalf("got %d rows, want %d (no end-of-input flush of [1800,2800)): %v", len(got), len(wantRows), got)
	}
	for i := range wantRows {
		if got[i] != wantRows[i] {
			t.Errorf("row %d = %+v, want %+v", i, got[i], wantRows[i])
		}
	}
	if strings.Contains(stdout, `"partition"`) {
		t.Errorf("output rows must never carry a partition field: %q", stdout)
	}
}

// TestSlidingPartitionAllIdleResumeBelowRetainedWatermark covers resume
// rejection against the retained overall watermark once every partition is
// idle. Partition 1's own old watermark is 500 and the retained overall
// watermark is 1000:
//
//   - 700 clears the partition's own bound but is below the retained overall
//     1000, so it is the existing input error naming physical line 10 (blank
//     line 8 counts), partition 1 and the violated overall lower bound. It is
//     not a late-event notice; the output already produced stays; the watermark
//     record after it is never processed and therefore cannot close
//     [600,1600).
//   - 400 violates the partition's own old 500, which is still the bound
//     reported first, never the overall bound.
//   - 1000 exactly equals the retained overall watermark (and is above the
//     partition's own 500): the resume succeeds, emits nothing by itself, and
//     a later advance on the resumed partition closes the merged window
//     normally.
func TestSlidingPartitionAllIdleResumeBelowRetainedWatermark(t *testing.T) {
	t.Run("below retained overall but above own old is the input error", func(t *testing.T) {
		lines := append(allIdleResumeFixtureLines(),
			`{"type":"watermark","time":700,"partition":1}`,  // line 10: >= own 500, < retained 1000
			`{"type":"watermark","time":1600,"partition":1}`, // line 11: dead; would close [600,1600)
		)
		stdout, stderr, err := runPartitionedSliding(t, strings.Join(lines, "\n"), 1000, 600, 2)
		var inputErr *InputError
		if err == nil {
			t.Fatalf("resume at 700 must fail against the retained watermark 1000; stdout=%q", stdout)
		}
		if !errors.As(err, &inputErr) {
			t.Fatalf("want *InputError, got %T: %v", err, err)
		}
		if inputErr.Line != 10 {
			t.Errorf("Line = %d, want 10 (blank physical line 8 counts)", inputErr.Line)
		}
		for _, sub := range []string{
			"resume watermark 700",
			"partition 1",
			"current effective watermark 1000",
		} {
			if !strings.Contains(inputErr.Reason, sub) {
				t.Errorf("reason = %q, want substring %q", inputErr.Reason, sub)
			}
		}
		// 700 is above p1's own old 500, so that bound must not be the named one.
		if strings.Contains(inputErr.Reason, "previous watermark") {
			t.Errorf("the partition's own old bound is satisfied; reason must name the retained overall watermark: %q", inputErr.Reason)
		}
		if stderr != "" {
			t.Errorf("a rejected resume record must not become a late notice, got %q", stderr)
		}
		// Prior output survives verbatim and the dead line 11 closes nothing.
		if stdout != allIdleWantFirstWindow {
			t.Fatalf("output after the failed resume:\n got: %q\nwant: %q", stdout, allIdleWantFirstWindow)
		}
		if strings.Contains(stdout, `"start":600`) {
			t.Errorf("the record after the failed resume must not close [600,1600): %q", stdout)
		}
	})

	t.Run("below own old still reports the partition bound", func(t *testing.T) {
		lines := append(allIdleResumeFixtureLines(),
			`{"type":"watermark","time":400,"partition":1}`, // line 10: below p1's own old 500
		)
		_, stderr, err := runPartitionedSliding(t, strings.Join(lines, "\n"), 1000, 600, 2)
		inputErr := inputErrorFrom(t, err)
		if inputErr.Line != 10 {
			t.Errorf("Line = %d, want 10", inputErr.Line)
		}
		for _, sub := range []string{"resume watermark 400", "partition 1", "previous watermark 500"} {
			if !strings.Contains(inputErr.Reason, sub) {
				t.Errorf("reason = %q, want substring %q", inputErr.Reason, sub)
			}
		}
		if strings.Contains(inputErr.Reason, "effective watermark") {
			t.Errorf("the own-old-watermark bound is checked first: %q", inputErr.Reason)
		}
		if stderr != "" {
			t.Errorf("a rejected resume record must not become a late notice, got %q", stderr)
		}
	})

	t.Run("exactly the retained overall watermark resumes", func(t *testing.T) {
		lines := append(allIdleResumeFixtureLines(),
			`{"type":"watermark","time":1000,"partition":1}`,                  // line 10: boundary resume
			`{"type":"event","key":"k","time":1300,"value":6,"partition":1}`,  // line 11: legal, joins open windows
			`{"type":"watermark","time":1600,"partition":1}`,                  // line 12: closes [600,1600)
			`{"type":"event","key":"k","time":1300,"value":99,"partition":0}`, // line 13: p0 still idle: fatal
		)
		// First, the resume boundary alone emits nothing new.
		hold := append(allIdleResumeFixtureLines(),
			`{"type":"watermark","time":1000,"partition":1}`,
		)
		stdout, stderr, err := runPartitionedSliding(t, strings.Join(hold, "\n"), 1000, 600, 2)
		if err != nil {
			t.Fatalf("resume exactly at the retained watermark 1000 must succeed: %v", err)
		}
		if stderr != "" {
			t.Fatalf("the boundary resume must produce no notice, got %q", stderr)
		}
		if stdout != allIdleWantFirstWindow {
			t.Fatalf("resume at 1000 must not re-emit [0,1000) or close open windows:\n got: %q\nwant: %q", stdout, allIdleWantFirstWindow)
		}

		// Then the resumed partition advances alone and the merged window
		// closes; p0 never reports again and stays idle.
		full := append(allIdleResumeFixtureLines(),
			`{"type":"watermark","time":1000,"partition":1}`,
			`{"type":"event","key":"k","time":1300,"value":6,"partition":1}`,
			`{"type":"watermark","time":1600,"partition":1}`,
		)
		stdout, stderr, err = runPartitionedSliding(t, strings.Join(full, "\n"), 1000, 600, 2)
		if err != nil {
			t.Fatalf("closure after the boundary resume must succeed: %v", err)
		}
		if stderr != "" {
			t.Fatalf("normal processing must produce no notice, got %q", stderr)
		}
		want := strings.Join([]string{
			`{"key":"k","start":0,"end":1000,"count":1,"sum":2}`,
			`{"key":"k","start":600,"end":1600,"count":5,"sum":20}`,
			``,
		}, "\n")
		if stdout != want {
			t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
		}

		// Sanity on the kept tail shape: an event on the still-idle p0 stays
		// fatal, proving the boundary resume resumed exactly one partition.
		_, _, err = runPartitionedSliding(t, strings.Join(lines, "\n"), 1000, 600, 2)
		if err == nil {
			t.Fatal("expected an event on the still-idle partition 0 to be fatal")
		}
		inputErr := inputErrorFrom(t, err)
		if inputErr.Line != 13 {
			t.Errorf("Line = %d, want 13", inputErr.Line)
		}
		if !strings.Contains(inputErr.Reason, "idle partition 0") {
			t.Errorf("reason = %q, want it to name idle partition 0", inputErr.Reason)
		}
	})
}

// TestSlidingPartitionAllIdleBeforeAnyWatermarkBuffersAndResumeCloses covers
// the second situation: both partitions are declared idle before either one
// has ever reported a watermark. Buffered events must survive (the idle
// declarations emit nothing and are not end of input); when one partition
// reports for the first time and thereby resumes, it closes eligible windows
// without waiting for the partition that remains idle, and the output fully
// reflects the events received before the idle declarations. The scenario is
// symmetric in which never-reported partition resumes.
func TestSlidingPartitionAllIdleBeforeAnyWatermarkBuffersAndResumeCloses(t *testing.T) {
	// line 1: p0 event k time=700  value=2 -> [0,1000), [600,1600)
	// line 2: p1 event k time=1000 value=3 -> [600,1600)
	// line 3: p1 event k time=1300 value=5 -> [600,1600), [1200,2200)
	// line 4: blank
	// line 5: p0 idle (never reported)
	// line 6: p1 idle (never reported) -> all idle, watermark never produced
	buffered := []string{
		`{"type":"event","key":"k","time":700,"value":2,"partition":0}`,
		`{"type":"event","key":"k","time":1000,"value":3,"partition":1}`,
		`{"type":"event","key":"k","time":1300,"value":5,"partition":1}`,
		``,
		`{"type":"idle","partition":0}`,
		`{"type":"idle","partition":1}`,
	}
	stdout, stderr, err := runPartitionedSliding(t, strings.Join(buffered, "\n"), 1000, 600, 2)
	if err != nil {
		t.Fatalf("idle declarations before any watermark must be legal: %v", err)
	}
	if stderr != "" {
		t.Fatalf("idle declarations must produce no notice, got %q", stderr)
	}
	if stdout != "" {
		t.Fatalf("all-idle with no watermark ever produced must not conjure window output, got %q", stdout)
	}

	cases := []struct {
		name   string
		resume int64 // the partition that first reports and resumes (0 or 1)
	}{
		{name: "partition 0 resumes alone", resume: 0},
		{name: "partition 1 resumes alone", resume: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := pid(tc.resume)
			lines := append(buffered,
				// line 7: first-ever watermark resumes this partition; the
				// other one stays idle and must NOT be waited on.
				`{"type":"watermark","time":1000,"partition":`+p+`}`,
				// line 8: legal new event on the active partition joins the
				// still-open overlapping windows.
				`{"type":"event","key":"k","time":1300,"value":6,"partition":`+p+`}`,
				// line 9: active partition alone closes [600,1600).
				`{"type":"watermark","time":1600,"partition":`+p+`}`,
			)
			stdout, stderr, err := runPartitionedSliding(t, strings.Join(lines, "\n"), 1000, 600, 2)
			if err != nil {
				t.Fatalf("resume without the other partition reporting must succeed: %v", err)
			}
			if stderr != "" {
				t.Fatalf("the normal path must produce no late notice, got %q", stderr)
			}
			want := strings.Join([]string{
				// p0's pre-idle time=700 event, closed by the lone resumer's
				// first watermark without waiting for the still-idle p1.
				`{"key":"k","start":0,"end":1000,"count":1,"sum":2}`,
				// Both partitions' pre-idle events (2 from p0, 3+5 from p1)
				// plus the resumer's new 6: count 4, sum 16.
				`{"key":"k","start":600,"end":1600,"count":4,"sum":16}`,
				``,
			}, "\n")
			if stdout != want {
				t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
			}
			got := decodeAggregateResults(t, stdout)
			wantRows := []AggregateResult{
				{Key: "k", Start: 0, End: 1000, Count: 1, Sum: 2},
				{Key: "k", Start: 600, End: 1600, Count: 4, Sum: 16},
			}
			if len(got) != len(wantRows) {
				t.Fatalf("got %d rows, want %d (no flush of the still-open [1200,2200)): %v", len(got), len(wantRows), got)
			}
			for i := range wantRows {
				if got[i] != wantRows[i] {
					t.Errorf("row %d = %+v, want %+v", i, got[i], wantRows[i])
				}
			}
			if strings.Contains(stdout, `"partition"`) {
				t.Errorf("output rows must never carry a partition field: %q", stdout)
			}
		})
	}
}
