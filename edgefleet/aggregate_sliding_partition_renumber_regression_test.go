package edgefleet

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

// Regression safeguard: partition numbers are only labels of the input source.
// Renaming every source by a one-to-one permutation -- keeping the partition
// count, each source's records, their content and the overall input order
// exactly the same -- must not change what the aggregation computes. These
// tests run the SAME business stream once under the baseline numbering and
// once under a bijection that swaps the source labels, then pin byte-identical
// window results (content, row count and order), identical late-event notices
// (event time, watermark and physical line number) and the same success.
//
// The stream is window 1000, slide 600 (600 does not divide 1000, so events
// contribute to overlapping windows), three partitions and one key "k", with
// the three sources genuinely playing different roles in the baseline
// numbering:
//
//   - partition 0 is the source that races ahead (its own watermark reaches
//     5000 while the others lag); it never idles.
//   - partition 2 is the source that limits the overall (minimum) watermark:
//     every window closure is timed by its reports at 1000, 1600 and 2200.
//   - partition 1 is the source that idles and later resumes by a legal
//     watermark, after which it rejoins the minimum computation.
//
// The roles therefore land on the zero index, the maximum legal index (2) and
// the middle index (1). Under the renumbering fast<->limiting the same three
// roles instead sit on 2, 0 and 1 respectively, which is exactly the
// situation the invariance must survive. Events for the one key come from
// different sources, a watermark closes only earlier windows while a later
// overlapping window keeps receiving events, an event exactly at the overall
// watermark is still counted while one strictly below it is skipped wholesale
// even though it lies inside an unclosed overlapping window, and the late
// judgement always uses the overall effective watermark -- never a single
// source's own. A separate stream keeps the failed-resume branch: a resume
// watermark below that source's old watermark (and below the current overall
// watermark) still fails at the same physical line, naming the remapped
// partition and the actually violated bound, with earlier output retained and
// later records producing neither results nor late notices.

// recordSpec describes one physical input line independently of how the
// source roles are numbered. Only lines with a partition carry one; blank
// lines carry none and still occupy a physical line number. time/value are
// only meaningful for the matching kind.
type recordSpec struct {
	kind  string // "event", "watermark" or "idle"
	src   roleID // source role owning the record (events, watermarks, idle)
	time  int64
	value int64
	blank bool // true: an empty physical line, no partition
}

type roleID int

const (
	roleFast roleID = iota // races its watermark ahead; never idles
	roleIdle               // idles, then resumes by a legal watermark
	roleSlow               // limits the overall minimum watermark
)

// renumberFixture is the success stream (window 1000, slide 600, 3
// partitions, one key "k"). Every line is listed in physical order; the
// partition each record shows is decided by the role -> index map passed to
// renumberRender. Blank physical lines (13 and 15) keep the numbers stable.
//
// Timeline under the baseline numbering (fast=0, idle=1, slow=2):
//
//	line 1  p0 event time=700  value=2  -> [0,1000) and [600,1600)
//	line 2  p2 event time=1000 value=3  -> [600,1600) only (right edge of [0,1000))
//	line 3  p1 event time=1300 value=5  -> [600,1600) and [1200,2200)
//	line 4  p0 watermark 5000           -> fast source ahead; effective still unknown
//	line 5  p2 watermark 1000           -> effective unknown until p1 reports
//	line 6  p1 watermark 1000           -> effective = 1000; closes only [0,1000)
//	line 7  p0 event time=999  value=9  -> strictly below overall 1000: skipped, notice
//	                                       (still inside the open [600,1600), proves the
//	                                       global minimum rules, not the fast source's 5000)
//	line 8  p2 event time=1000 value=4  -> exactly at overall 1000: counted, [600,1600)
//	line 9  p2 watermark 1600           -> p1 still active at 1000: effective stays 1000
//	line 10 p1 idle                     -> effective = min(5000,1600) = 1600; closes [600,1600)
//	line 11 p0 event time=1700 value=8  -> 1700 >= effective 1600 (accepted); [600,1600) is
//	                                       already closed, so it joins only the later open
//	                                       window [1200,2200)
//	line 12 p0 watermark 5000           -> repeat; effective stays 1600
//	line 13 blank
//	line 14 p1 watermark 2200           -> legal resume: 2200 >= own old 1000 and >= 1600
//	line 15 blank
//	line 16 p2 watermark 2200           -> effective = min(5000,2200,2200) = 2200; closes [1200,2200)
//	line 17 p0 event time=2300 value=6  -> [1800,2800) only; stays open at EOF, never flushed
//
// Window totals, directly recheckable from the valid events (the skipped
// line 7 contributes nowhere; the partition field never reaches output):
//
//	[0,1000)    : line 1 (value 2)                                 -> count 1, sum 2
//	[600,1600)  : lines 1(2),2(3),3(5),8(4)                        -> count 4, sum 14
//	              (line 11 time=1700 arrives after this window closed)
//	[1200,2200) : lines 3(5),11(8)                                 -> count 2, sum 13
func renumberFixture() []recordSpec {
	return []recordSpec{
		{kind: "event", src: roleFast, time: 700, value: 2},
		{kind: "event", src: roleSlow, time: 1000, value: 3},
		{kind: "event", src: roleIdle, time: 1300, value: 5},
		{kind: "watermark", src: roleFast, time: 5000},
		{kind: "watermark", src: roleSlow, time: 1000},
		{kind: "watermark", src: roleIdle, time: 1000},
		{kind: "event", src: roleFast, time: 999, value: 9},
		{kind: "event", src: roleSlow, time: 1000, value: 4},
		{kind: "watermark", src: roleSlow, time: 1600},
		{kind: "idle", src: roleIdle},
		{kind: "event", src: roleFast, time: 1700, value: 8},
		{kind: "watermark", src: roleFast, time: 5000},
		{blank: true},
		{kind: "watermark", src: roleIdle, time: 2200},
		{blank: true},
		{kind: "watermark", src: roleSlow, time: 2200},
		{kind: "event", src: roleFast, time: 2300, value: 6},
	}
}

// renumberWantStdout is the exact, order-pinned output every numbering must
// produce: three rows, no partition field anywhere.
var renumberWantStdout = strings.Join([]string{
	`{"key":"k","start":0,"end":1000,"count":1,"sum":2}`,
	`{"key":"k","start":600,"end":1600,"count":4,"sum":14}`,
	`{"key":"k","start":1200,"end":2200,"count":2,"sum":13}`,
	``,
}, "\n")

// renumberWantLate is the single late notice. Its event time, watermark and
// physical line must be identical after renumbering.
var renumberWantLate = "line 7: late event time=999 below current watermark 1000, skipped\n"

// renumberRender turns the role-agnostic fixture into literal input, mapping
// each source role to a concrete partition index. Because every rendering
// goes through the same specs, the record content, per-source assignment and
// overall line order cannot drift between numberings -- only the partition
// numbers change.
func renumberRender(lines []recordSpec, roleToIndex [3]int64) string {
	rendered := make([]string, len(lines))
	for i, rec := range lines {
		if rec.blank {
			rendered[i] = ""
			continue
		}
		p := roleToIndex[rec.src]
		switch rec.kind {
		case "event":
			rendered[i] = fmt.Sprintf(
				`{"type":"event","key":"k","time":%d,"value":%d,"partition":%d}`,
				rec.time, rec.value, p)
		case "watermark":
			rendered[i] = fmt.Sprintf(
				`{"type":"watermark","time":%d,"partition":%d}`, rec.time, p)
		case "idle":
			rendered[i] = fmt.Sprintf(`{"type":"idle","partition":%d}`, p)
		default:
			panic("unknown record kind: " + rec.kind)
		}
	}
	return strings.Join(rendered, "\n")
}

// isBijection reports that m is a one-to-one mapping of the three source
// roles onto the legal partition indices [0,3): every source survives, none
// is merged with another and every target is in range. The guarantee under
// test only holds for such a renaming.
func isBijection(m [3]int64, count int64) bool {
	seen := make(map[int64]bool, len(m))
	for _, p := range m {
		if p < 0 || p >= count {
			return false
		}
		if seen[p] {
			return false // two sources collapsed onto one partition number
		}
		seen[p] = true
	}
	return int64(len(seen)) == count
}

// TestSlidingPartitionRenumberingInvariant is the core safeguard: the same
// stream run under the baseline numbering and under a non-trivial bijection
// (fast <-> limiting; the idle/resume source keeps the middle index) must
// produce byte-identical window rows, the same late notice and the same
// success. The roles in the baseline numbering sit on zero (fast), max
// index 2 (limiting) and middle 1 (idle/resume); under the renumbering they
// sit on 2, 0, 1 respectively.
func TestSlidingPartitionRenumberingInvariant(t *testing.T) {
	const window, slide, partitions int64 = 1000, 600, 3

	numberings := []struct {
		name        string
		roleToIndex [3]int64
	}{
		{name: "baseline fast=0 idle=1 slow=2", roleToIndex: [3]int64{0, 1, 2}},
		{name: "swapped fast=2 idle=1 slow=0", roleToIndex: [3]int64{2, 1, 0}},
	}
	for _, nb := range numberings {
		if !isBijection(nb.roleToIndex, partitions) {
			t.Fatalf("internal: %s is not a bijection over [0,%d)", nb.name, partitions)
		}
	}

	fixture := renumberFixture()
	var baseOut, baseLate string
	for i, nb := range numberings {
		input := renumberRender(fixture, nb.roleToIndex)
		stdout, stderr, err := runPartitionedSliding(t, input, window, slide, partitions)

		if err != nil {
			t.Fatalf("%s: normal processing must succeed, got %v", nb.name, err)
		}
		if stdout != renumberWantStdout {
			t.Fatalf("%s: stdout mismatch:\n got: %q\nwant: %q", nb.name, stdout, renumberWantStdout)
		}
		if stderr != renumberWantLate {
			t.Fatalf("%s: late notice mismatch:\n got: %q\nwant: %q", nb.name, stderr, renumberWantLate)
		}
		// The source number must never leak into any output row.
		if strings.Contains(stdout, "partition") {
			t.Fatalf("%s: output must carry no partition field: %q", nb.name, stdout)
		}

		if i == 0 {
			baseOut, baseLate = stdout, stderr
			continue
		}
		// Renumbering changes nothing about the aggregation: byte-identical
		// results and notices, same physical line in the notice.
		if stdout != baseOut {
			t.Fatalf("%s vs baseline: window results must be identical after renumbering:\nbase: %q\ngot : %q",
				nb.name, baseOut, stdout)
		}
		if stderr != baseLate {
			t.Fatalf("%s vs baseline: late notices must be identical after renumbering:\nbase: %q\ngot : %q",
				nb.name, baseLate, stderr)
		}
	}
}

// TestSlidingPartitionRenumberingWindowTotals independently re-checks the
// counts and sums of the renumbered run from the valid events per source, so
// the equality is not merely one rendering compared against another: each
// window's count is the number of valid events it contains and its sum is
// each event's full value added once per window it belongs to. The row count
// is exactly three, in end-ascending order, and the still-open window is
// absent.
func TestSlidingPartitionRenumberingWindowTotals(t *testing.T) {
	const window, slide, partitions int64 = 1000, 600, 3
	roleToIndex := [3]int64{2, 1, 0} // the non-baseline bijection
	input := renumberRender(renumberFixture(), roleToIndex)
	stdout, _, err := runPartitionedSliding(t, input, window, slide, partitions)
	if err != nil {
		t.Fatalf("normal processing must succeed: %v", err)
	}

	wantRows := []AggregateResult{
		{Key: "k", Start: 0, End: 1000, Count: 1, Sum: 2},
		{Key: "k", Start: 600, End: 1600, Count: 4, Sum: 14},
		{Key: "k", Start: 1200, End: 2200, Count: 2, Sum: 13},
	}
	got := decodeAggregateResults(t, stdout)
	if len(got) != len(wantRows) {
		t.Fatalf("got %d rows, want %d (the open [1800,2800) window must never be emitted): %v",
			len(got), len(wantRows), got)
	}
	for i := range wantRows {
		if got[i] != wantRows[i] {
			t.Errorf("row %d = %+v, want %+v", i, got[i], wantRows[i])
		}
	}
}

// resumeFailureSpec is the failed-resume stream for the own-old-watermark
// bound. All three sources report before the idle/resume source goes idle, so
// an overall watermark exists. That source reports 2500 before idling, so its
// resume at 2400 -- after the fast source has advanced to 3000 -- is below
// BOTH its own old watermark and the current overall watermark; the engine
// checks the own-old-watermark bound first, so that is the one reported.
//
//	line 1 p(fast) event time=100 value=5 -> [0,1000)
//	line 2 p(fast) watermark 3000         -> fast source ahead; effective still unknown
//	line 3 p(slow) watermark 2500         -> effective still unknown (idle source not reported)
//	line 4 p(idle) watermark 2500         -> effective = 2500; closes [0,1000)
//	line 5 p(idle) idle                   -> effective = min(3000,2500) = 2500; nothing new closes
//	line 6 p(idle) watermark 2400         -> resume below own old 2500 (and < overall 2500): error
//	line 7 p(slow) event time=100 value=7 -> dead: would be a late notice if reached
//	line 8 p(slow) watermark 3000         -> dead: would close more windows
func resumeFailureSpec() []recordSpec {
	return []recordSpec{
		{kind: "event", src: roleFast, time: 100, value: 5},
		{kind: "watermark", src: roleFast, time: 3000},
		{kind: "watermark", src: roleSlow, time: 2500},
		{kind: "watermark", src: roleIdle, time: 2500},
		{kind: "idle", src: roleIdle},
		{kind: "watermark", src: roleIdle, time: 2400}, // line 6: illegal resume
		{kind: "event", src: roleSlow, time: 100, value: 7},
		{kind: "watermark", src: roleSlow, time: 3000},
	}
}

// resumeEffectiveFailureSpec is the failed-resume stream for the other bound:
// the resume watermark clears the source's own previous watermark but is
// below the overall effective watermark the two active sources advanced to
// while it slept. That effective-watermark bound is then the reported reason.
//
//	line 1  p(fast) event time=100 value=5 -> [0,1000)
//	line 2  p(idle) watermark 1000         -> idle source reports its old watermark
//	line 3  p(slow) watermark 1000
//	line 4  p(fast) watermark 1000         -> effective = 1000; closes [0,1000)
//	line 5  p(idle) idle                   -> active sources both at 1000; effective stays 1000
//	line 6  p(fast) watermark 2500         -> min(2500,1000) = 1000
//	line 7  p(slow) watermark 2500         -> only fast source active: effective = 2500
//	line 8  p(idle) watermark 2000         -> 2000 >= own old 1000 but < overall 2500: error
//	line 9  p(slow) event time=100 value=7 -> dead
//	line 10 p(slow) watermark 3000         -> dead
func resumeEffectiveFailureSpec() []recordSpec {
	return []recordSpec{
		{kind: "event", src: roleFast, time: 100, value: 5},
		{kind: "watermark", src: roleIdle, time: 1000},
		{kind: "watermark", src: roleSlow, time: 1000},
		{kind: "watermark", src: roleFast, time: 1000},
		{kind: "idle", src: roleIdle},
		{kind: "watermark", src: roleFast, time: 2500},
		{kind: "watermark", src: roleSlow, time: 2500},
		{kind: "watermark", src: roleIdle, time: 2000}, // line 8: resume below overall 2500
		{kind: "event", src: roleSlow, time: 100, value: 7},
		{kind: "watermark", src: roleSlow, time: 3000},
	}
}

// TestSlidingPartitionRenumberingFailedResumeErrorsRemappedPartition keeps
// both resume-failure branches under the renumbering convention. The same
// illegal resume fails at the SAME physical line under every bijection, the
// reason names the actually violated bound (the source's own previous
// watermark in one case, the current overall effective watermark in the
// other) and -- crucially -- names the partition index as it appears AFTER
// renumbering. Earlier fully written output is retained; no later record may
// add a window result or a late notice.
func TestSlidingPartitionRenumberingFailedResumeErrorsRemappedPartition(t *testing.T) {
	const window, slide, partitions int64 = 1000, 600, 3

	cases := []struct {
		name        string
		fixture     []recordSpec
		failLine    int
		resumeTime  int64
		wantBound   string // substring naming the actually violated bound
		forbidBound string // substring that must NOT appear (the other, not-taken bound)
	}{
		{
			name:        "resume below own previous watermark",
			fixture:     resumeFailureSpec(),
			failLine:    6,
			resumeTime:  2400,
			wantBound:   "previous watermark 2500",
			forbidBound: "effective watermark",
		},
		{
			name:        "resume below current effective watermark",
			fixture:     resumeEffectiveFailureSpec(),
			failLine:    8,
			resumeTime:  2000,
			wantBound:   "current effective watermark 2500",
			forbidBound: "previous watermark",
		},
	}

	numberings := []struct {
		name        string
		roleToIndex [3]int64
		wantIdleIdx int64 // concrete partition index the idle/resume role maps to
	}{
		{name: "baseline idle=1", roleToIndex: [3]int64{0, 1, 2}, wantIdleIdx: 1},
		{name: "idle role renumbered to 2", roleToIndex: [3]int64{1, 2, 0}, wantIdleIdx: 2},
	}
	wantOutput := `{"key":"k","start":0,"end":1000,"count":1,"sum":5}` + "\n"

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var baseReason string
			for i, nb := range numberings {
				if !isBijection(nb.roleToIndex, partitions) {
					t.Fatalf("internal: %s is not a bijection over [0,%d)", nb.name, partitions)
				}
				input := renumberRender(tc.fixture, nb.roleToIndex)
				stdout, stderr, err := runPartitionedSliding(t, input, window, slide, partitions)

				var inputErr *InputError
				if err == nil {
					t.Fatalf("%s: expected an input error for the too-low resume, got nil", nb.name)
				}
				if !errors.As(err, &inputErr) {
					t.Fatalf("%s: want *InputError, got %T: %v", nb.name, err, err)
				}
				if inputErr.Line != tc.failLine {
					t.Errorf("%s: failing line = %d, want %d (the physical line is unchanged by renumbering)",
						nb.name, inputErr.Line, tc.failLine)
				}
				for _, sub := range []string{
					fmt.Sprintf("resume watermark %d", tc.resumeTime),
					fmt.Sprintf("partition %d", nb.wantIdleIdx),
					tc.wantBound,
				} {
					if !strings.Contains(inputErr.Reason, sub) {
						t.Errorf("%s: reason = %q, want substring %q", nb.name, inputErr.Reason, sub)
					}
				}
				if strings.Contains(inputErr.Reason, tc.forbidBound) {
					t.Errorf("%s: reason must name the actually violated bound, not %q: %q",
						nb.name, tc.forbidBound, inputErr.Reason)
				}

				// Earlier fully written output stays exactly as published; dead
				// records after the failure add neither a result nor a notice.
				if stdout != wantOutput {
					t.Errorf("%s: earlier output must be retained and no later output added:\n got: %q\nwant: %q",
						nb.name, stdout, wantOutput)
				}
				if stderr != "" {
					t.Errorf("%s: a rejected resume produces no late notices and dead records add none, got %q",
						nb.name, stderr)
				}

				if i == 0 {
					baseReason = inputErr.Reason
					continue
				}
				// Sanity: the reason text is the same except for the remapped
				// partition number (1 -> 2), proving only the label moved.
				wantReason := strings.Replace(baseReason, "partition 1", "partition 2", 1)
				if inputErr.Reason != wantReason {
					t.Fatalf("%s: reason must differ from baseline only in the partition number:\nbase: %q\nwant: %q\ngot : %q",
						nb.name, baseReason, wantReason, inputErr.Reason)
				}
			}
		})
	}
}
