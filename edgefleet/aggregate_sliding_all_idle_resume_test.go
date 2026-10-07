package edgefleet

import (
	"errors"
	"strings"
	"testing"
)

// Regression coverage for every partition being idle at once and the stream
// afterwards resuming, over sliding windows (window 1000, slide 600, two
// partitions, one key "k"). All-idle is neither end of input nor an infinite
// watermark: the last produced effective watermark and every still-open
// window's accumulated content are retained, nothing is emitted while no
// active partition exists, and closure afterwards is driven solely by the
// resumed (active) partitions' watermarks. Pre-idle events from both
// partitions land in shared windows and in several overlapping windows at
// once, so the results prove those contributions survive the idle period.

// slidingAllIdleFixtureLines is the shared prefix for the retained-watermark
// scenarios: both partitions contribute to the overlapping windows before any
// idle declaration. Partition 0's event at 700 lands in [0,1000) and
// [600,1600); partition 1's events at 1000 and 1300 land in [600,1600) only
// and in [600,1600)+[1200,2200) respectively. Once partition 0 reports 1600
// and partition 1 reports 1000 the effective watermark is 1000: [0,1000)
// closes (count 1, sum 2) while [600,1600) and [1200,2200) stay open with
// both partitions' contributions buffered. The two idle declarations then
// take every partition out of the effective watermark, retaining 1000.
func slidingAllIdleFixtureLines() []string {
	return []string{
		`{"type":"event","key":"k","time":700,"value":2,"partition":0}`,  // line 1: [0,1000) and [600,1600)
		`{"type":"event","key":"k","time":1000,"value":3,"partition":1}`, // line 2: [600,1600) only
		`{"type":"event","key":"k","time":1300,"value":5,"partition":1}`, // line 3: [600,1600) and [1200,2200)
		`{"type":"watermark","time":1600,"partition":0}`,                 // line 4: p0 ahead, effective still unknown
		`{"type":"watermark","time":1000,"partition":1}`,                 // line 5: effective = 1000, closes [0,1000)
		`{"type":"idle","partition":0}`,                                  // line 6: p1 keeps effective at 1000
		`{"type":"idle","partition":1}`,                                  // line 7: all idle; retained 1000, no output
	}
}

// With an effective watermark already produced, the moment the last active
// partition declares idle nothing may be emitted: windows whose end is still
// above the retained watermark stay open with their contents, and the already
// closed [0,1000) is not emitted again. One partition then resumes with a
// legal watermark while the other stays idle, so closure depends only on the
// resumed partition: its watermark 1600 closes [600,1600) with both
// partitions' pre-idle contributions merged (count 3, sum 10), its new event
// at 1900 joins the still-open overlapping [1200,2200) and [1800,2800), and
// its watermark 2200 closes [1200,2200) (count 2, sum 12). The window still
// open at end of input is not flushed, and the whole run produces no
// late-event notice.
func TestAggregateSlidingAllIdleThenResumeRetainsState(t *testing.T) {
	// The all-idle prefix on its own: only the window closed before the idle
	// declarations exists; the last idle declaration itself emits nothing.
	prefixOut, prefixErr, err := runPartitionedSliding(t, strings.Join(slidingAllIdleFixtureLines(), "\n"), 1000, 600, 2)
	if err != nil {
		t.Fatalf("unexpected error on the all-idle prefix: %v", err)
	}
	if prefixErr != "" {
		t.Fatalf("unexpected stderr on the all-idle prefix: %q", prefixErr)
	}
	closed := `{"key":"k","start":0,"end":1000,"count":1,"sum":2}` + "\n"
	if prefixOut != closed {
		t.Fatalf("all-idle must neither emit open windows nor re-emit closed ones:\n got: %q\nwant: %q", prefixOut, closed)
	}

	lines := append(slidingAllIdleFixtureLines(),
		`{"type":"watermark","time":1600,"partition":0}`,                 // line 8: resume at p0's own old watermark; effective = 1600, closes [600,1600)
		`{"type":"event","key":"k","time":1900,"value":7,"partition":0}`, // line 9: joins open [1200,2200) and [1800,2800)
		`{"type":"watermark","time":2200,"partition":0}`,                 // line 10: closes [1200,2200); p1 still idle throughout
	)
	stdout, stderr, err := runPartitionedSliding(t, strings.Join(lines, "\n"), 1000, 600, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stderr != "" {
		t.Fatalf("unexpected stderr (no late-event notice on the normal path): %q", stderr)
	}
	want := strings.Join([]string{
		`{"key":"k","start":0,"end":1000,"count":1,"sum":2}`,
		// Both partitions' pre-idle contributions (2 from p0, 3+5 from p1)
		// survive the all-idle period; the result carries no partition field.
		`{"key":"k","start":600,"end":1600,"count":3,"sum":10}`,
		// Partition 1's pre-idle event at 1300 merges with the resumed
		// partition's post-resume event at 1900.
		`{"key":"k","start":1200,"end":2200,"count":2,"sum":12}`,
		``,
	}, "\n")
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
}

// Both partitions go idle before either ever reported a watermark: no
// effective watermark ever existed, so the idle declarations must not conjure
// window output, and the events buffered beforehand stay buffered. When one
// partition then resumes with its first-ever watermark, it closes every
// window its value allows without waiting for the still-idle partition to
// report, and the results fully reflect the events received before the idle
// declarations.
func TestAggregateSlidingAllIdleBeforeAnyWatermarkThenResume(t *testing.T) {
	prefix := []string{
		`{"type":"event","key":"k","time":700,"value":2,"partition":0}`,  // [0,1000) and [600,1600)
		`{"type":"event","key":"k","time":1000,"value":3,"partition":1}`, // [600,1600) only
		`{"type":"event","key":"k","time":1300,"value":5,"partition":1}`, // [600,1600) and [1200,2200)
		`{"type":"idle","partition":0}`,                                  // never reported; p1 still unreported, effective stays unknown
		`{"type":"idle","partition":1}`,                                  // all idle, no effective watermark ever: still no output
	}
	stdout, stderr, err := runPartitionedSliding(t, strings.Join(prefix, "\n"), 1000, 600, 2)
	if err != nil {
		t.Fatalf("unexpected error on the all-idle prefix: %v", err)
	}
	if stderr != "" {
		t.Fatalf("unexpected stderr on the all-idle prefix: %q", stderr)
	}
	if stdout != "" {
		t.Fatalf("idle declarations must not trigger window output out of thin air, got %q", stdout)
	}

	lines := append(append([]string{}, prefix...),
		`{"type":"watermark","time":1600,"partition":0}`,                 // first report ever: active set {p0}, effective = 1600
		`{"type":"event","key":"k","time":1900,"value":7,"partition":0}`, // joins open [1200,2200) and [1800,2800)
		`{"type":"watermark","time":2200,"partition":0}`,                 // closes [1200,2200); p1 never reported and stays idle
	)
	stdout, stderr, err = runPartitionedSliding(t, strings.Join(lines, "\n"), 1000, 600, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stderr != "" {
		t.Fatalf("unexpected stderr: %q", stderr)
	}
	want := strings.Join([]string{
		// Closed by the resuming partition's first watermark alone, without
		// waiting for the still-idle partition 1; the counts and sums are
		// exactly the events buffered before the idle declarations.
		`{"key":"k","start":0,"end":1000,"count":1,"sum":2}`,
		`{"key":"k","start":600,"end":1600,"count":3,"sum":10}`,
		`{"key":"k","start":1200,"end":2200,"count":2,"sum":12}`,
		``,
	}, "\n")
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
}

// slidingAllIdleRetained2000Lines builds an all-idle state whose retained
// effective watermark (2000) sits above partition 1's own pre-idle watermark
// (1000): partition 1 idles first, partition 0 alone advances the effective
// watermark to 2000 (closing [600,1600) with count 3, sum 10 and leaving
// [1200,2200) open), and only then does partition 0 idle as well. The blank
// physical line 6 counts toward line numbers.
func slidingAllIdleRetained2000Lines() []string {
	return []string{
		`{"type":"event","key":"k","time":700,"value":2,"partition":0}`,  // line 1: [0,1000) and [600,1600)
		`{"type":"event","key":"k","time":1000,"value":3,"partition":1}`, // line 2: [600,1600) only
		`{"type":"event","key":"k","time":1300,"value":5,"partition":0}`, // line 3: [600,1600) and [1200,2200)
		`{"type":"watermark","time":1000,"partition":0}`,                 // line 4
		`{"type":"watermark","time":1000,"partition":1}`,                 // line 5: effective = 1000, closes [0,1000)
		``,                              // line 6: blank, counts toward physical line numbers
		`{"type":"idle","partition":1}`, // line 7: p0 keeps effective at 1000
		`{"type":"watermark","time":2000,"partition":0}`, // line 8: effective = 2000, closes [600,1600)
		`{"type":"idle","partition":0}`,                  // line 9: all idle; retained 2000
	}
}

// slidingAllIdleRetained2000Output is what the fixture above emits before any
// resume attempt: the two windows closed while the effective watermark
// advanced to 2000, in end-ascending order, with no partition field.
const slidingAllIdleRetained2000Output = `{"key":"k","start":0,"end":1000,"count":1,"sum":2}` + "\n" +
	`{"key":"k","start":600,"end":1600,"count":3,"sum":10}` + "\n"

// A resume record that clears the partition's own previous watermark but sits
// below the retained effective watermark is still the existing fatal input
// error: it names the physical line (blank lines count), the partition and
// the violated effective-watermark lower bound. The output produced before
// the failure is retained, the failure is not downgraded to a late-event
// notice, and records after the failed one are never processed, so they
// cannot close the still-open [1200,2200).
func TestAggregateSlidingAllIdleResumeBelowRetainedEffective(t *testing.T) {
	lines := append(slidingAllIdleRetained2000Lines(),
		`{"type":"watermark","time":1500,"partition":1}`, // line 10: >= p1's own 1000 but < retained effective 2000
		`{"type":"watermark","time":2200,"partition":0}`, // line 11: must never run (would close [1200,2200))
	)
	stdout, stderr, err := runPartitionedSliding(t, strings.Join(lines, "\n"), 1000, 600, 2)

	var inputErr *InputError
	if err == nil {
		t.Fatal("expected an input error for the resume below the retained effective watermark, got nil")
	}
	if !errors.As(err, &inputErr) {
		t.Fatalf("expected *InputError, got %T: %v", err, err)
	}
	if inputErr.Line != 10 {
		t.Errorf("Line = %d, want 10 (blank physical line 6 counts)", inputErr.Line)
	}
	// The violated bound is the retained effective watermark, not the
	// partition's own previous watermark (1500 already clears that one).
	for _, sub := range []string{
		"resume watermark 1500",
		"partition 1",
		"effective watermark 2000",
	} {
		if !strings.Contains(inputErr.Reason, sub) {
			t.Errorf("reason = %q, want substring %q", inputErr.Reason, sub)
		}
	}
	if strings.Contains(inputErr.Reason, "previous watermark") {
		t.Errorf("the partition's own previous-watermark bound must not be the reported one: %q", inputErr.Reason)
	}
	if stderr != "" {
		t.Errorf("a rejected resume record must produce no late-event notice, got %q", stderr)
	}
	if stdout != slidingAllIdleRetained2000Output {
		t.Fatalf("output before the failure must be retained and nothing after it emitted:\n got: %q\nwant: %q", stdout, slidingAllIdleRetained2000Output)
	}
}

// Resuming exactly at the retained effective watermark succeeds once the
// partition's own previous watermark is cleared: the resume itself emits
// nothing new (the retained 2000 closes no further window and re-emits none),
// the resumed partition's later events join the still-open overlapping
// windows, and its next watermark closes [1200,2200) with the pre-idle and
// post-resume contributions merged. The window still open at end of input is
// not flushed.
func TestAggregateSlidingAllIdleResumeAtRetainedEffective(t *testing.T) {
	lines := append(slidingAllIdleRetained2000Lines(),
		`{"type":"watermark","time":2000,"partition":1}`,                 // line 10: == retained effective, >= own 1000: resumes
		`{"type":"event","key":"k","time":2100,"value":4,"partition":1}`, // line 11: joins open [1200,2200) and [1800,2800)
		`{"type":"watermark","time":2200,"partition":1}`,                 // line 12: closes [1200,2200); p0 still idle
	)
	stdout, stderr, err := runPartitionedSliding(t, strings.Join(lines, "\n"), 1000, 600, 2)
	if err != nil {
		t.Fatalf("resume exactly at the retained effective watermark must succeed, got %v", err)
	}
	if stderr != "" {
		t.Fatalf("unexpected stderr (no late-event notice on the normal path): %q", stderr)
	}
	want := slidingAllIdleRetained2000Output +
		// Partition 0's pre-idle event at 1300 merges with the resumed
		// partition's post-resume event at 2100.
		`{"key":"k","start":1200,"end":2200,"count":2,"sum":9}` + "\n"
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
}
