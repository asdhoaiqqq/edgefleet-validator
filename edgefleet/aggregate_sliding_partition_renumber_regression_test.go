package edgefleet

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// Regression safeguard for partition numbering as a mere source label.
//
// A partition number only identifies which independent input source a record
// came from; it must never enter the aggregate semantics: relabeling every
// partition number with a single bijection -- one that stays inside [0,N),
// neither merges two sources nor drops one, and is applied to EVERY record
// kind that carries a partition (events, watermarks and idle declarations) --
// must leave the window results byte-for-byte identical in content, row count
// and order, must leave every late notice identical in its event time, the
// watermark cited and the physical line number, and must keep a normal run
// successful. The only text allowed to change is the partition number named
// in an input error, which must be the relabeled number on the same physical
// line.
//
// The scenario is one business stream (window 1000, slide 600 -- 600 does not
// divide 1000) merged over THREE sources, one key "k", and the three sources
// play genuinely different roles that land on the zero, the maximum legal and
// a middle number:
//
//   - partition 0 (the zero index) is the source that races ahead;
//   - partition 2 (the maximum legal index for --partitions 3) is the source
//     that pins the overall effective watermark by lagging, then catches up;
//   - partition 1 (a middle index) is the source that goes idle and later
//     resumes with a legal watermark.
//
// The same key's events arrive from all three sources and, because the slide
// interval does not divide the window length, several of them land in more
// than one overlapping window. The stream first closes only the early window
// while the later overlapping window keeps receiving events. Source numbers
// never appear in the output. Everything goes through the public
// RunAggregatePartitionedSliding entry point; no production code, public entry
// point, result format or error convention is changed, and no renumbering
// command is introduced.

// partitionRenumberPermutation is the relabeling under test. It is a
// bijection of {0,1,2} onto itself that moves every role off its original
// number: the fast source 0 -> 2 (now the maximum legal index), the idle
// source 1 -> 0 (now the zero index), the limiter 2 -> 1 (now a middle
// index), so each of zero/max/middle is exercised by a different source than
// before, while the total partition count stays 3.
var partitionRenumberPermutation = map[int64]int64{0: 2, 1: 0, 2: 1}

// renumberFixtureSuccessLines is the shared 17-line stream. Partition numbers
// shown are the ORIGINAL labels; renumberPartitionFields applies the
// bijection. The blank-free layout keeps physical line numbers obvious.
//
//	line 1:  p0 event t=700  v=2 -> [0,1000) and [600,1600)
//	line 2:  p1 event t=1000 v=3 -> [600,1600)
//	line 3:  p2 event t=1300 v=5 -> [600,1600) and [1200,2200)
//	line 4:  p0 watermark 1600       (fast source races ahead)
//	line 5:  p1 watermark 1600
//	line 6:  p2 watermark 600        (laggard pins the effective minimum at 600)
//	line 7:  p2 watermark 1000       (effective advances to 1000: closes only [0,1000))
//	line 8:  p1 declares idle        (excluded from the minimum; effective stays 1000)
//	line 9:  p0 event t=999  v=9     (strictly below effective 1000: late, skipped whole)
//	line 10: p0 event t=1200 v=6     (1200 < p0's OWN watermark 1600, yet accepted:
//	                                  lateness uses the overall effective 1000, not the
//	                                  source's own watermark; joins [600,1600),[1200,2200))
//	line 11: p2 event t=1000 v=4     (equal to effective 1000: still counted; joins [600,1600))
//	line 12: p1 watermark 1600       (legal resume: >= old 1600 and >= effective 1000)
//	line 13: p2 watermark 1600       (effective = min(1600,1600,1600) = 1600: closes [600,1600))
//	line 14: p1 event t=1900 v=7     (resumed source participates again; -> [1200,2200) and [1800,2800))
//	line 15: p0 watermark 2200
//	line 16: p1 watermark 2200
//	line 17: p2 watermark 2200       (effective 2200: closes [1200,2200); [1800,2800) stays open)
func renumberFixtureSuccessLines() []string {
	return []string{
		`{"type":"event","key":"k","time":700,"value":2,"partition":0}`,
		`{"type":"event","key":"k","time":1000,"value":3,"partition":1}`,
		`{"type":"event","key":"k","time":1300,"value":5,"partition":2}`,
		`{"type":"watermark","time":1600,"partition":0}`,
		`{"type":"watermark","time":1600,"partition":1}`,
		`{"type":"watermark","time":600,"partition":2}`,
		`{"type":"watermark","time":1000,"partition":2}`,
		`{"type":"idle","partition":1}`,
		`{"type":"event","key":"k","time":999,"value":9,"partition":0}`,
		`{"type":"event","key":"k","time":1200,"value":6,"partition":0}`,
		`{"type":"event","key":"k","time":1000,"value":4,"partition":2}`,
		`{"type":"watermark","time":1600,"partition":1}`,
		`{"type":"watermark","time":1600,"partition":2}`,
		`{"type":"event","key":"k","time":1900,"value":7,"partition":1}`,
		`{"type":"watermark","time":2200,"partition":0}`,
		`{"type":"watermark","time":2200,"partition":1}`,
		`{"type":"watermark","time":2200,"partition":2}`,
	}
}

// Expected window accounting, independently re-checkable per source from the
// valid (non-late) events. The late t=999 event contributes nowhere.
//
//	[0,1000):   p0 {2}                            -> count 1, sum 2
//	[600,1600): p0 {2,6}=8, p1 {3}=3, p2 {5,4}=9  -> count 5, sum 20
//	[1200,2200): p0 {6}=6, p1 {7}=7, p2 {5}=5     -> count 3, sum 18
var renumberFixtureWantRows = []AggregateResult{
	{Key: "k", Start: 0, End: 1000, Count: 1, Sum: 2},
	{Key: "k", Start: 600, End: 1600, Count: 5, Sum: 20},
	{Key: "k", Start: 1200, End: 2200, Count: 3, Sum: 18},
}

const renumberFixtureWantStdout = `{"key":"k","start":0,"end":1000,"count":1,"sum":2}` + "\n" +
	`{"key":"k","start":600,"end":1600,"count":5,"sum":20}` + "\n" +
	`{"key":"k","start":1200,"end":2200,"count":3,"sum":18}` + "\n"

const renumberFixtureWantLate = "line 9: late event time=999 below current watermark 1000, skipped\n"

// acceptedSourceEvents lists every event the stream actually accepts (the
// late t=999 record is excluded), keyed by the ORIGINAL source number, so the
// expected per-window counts and sums can be recomputed from each source's
// valid events independently of the watermark/idle machinery and
// independently of the labels the run used.
var acceptedSourceEvents = []struct {
	source int64
	time   int64
	value  int64
}{
	{0, 700, 2},
	{1, 1000, 3},
	{2, 1300, 5},
	{0, 1200, 6},
	{2, 1000, 4},
	{1, 1900, 7},
}

// renumberWindowStarts are the windows closed by the final effective
// watermark 2200, enumerated directly (length 1000, left-closed right-open).
// [1800,2800) is intentionally absent: it stays open at end of input and must
// never be flushed.
var renumberWindowStarts = []int64{0, 600, 1200}

const renumberWindowLength int64 = 1000

// recomputeRowsBySource rebuilds the merged window rows from
// acceptedSourceEvents using plain interval containment (a different
// formulation than the engine's slide arithmetic), and additionally reports
// each source's own count/sum contribution per window start so the merged
// totals can be checked source by source.
func recomputeRowsBySource(t *testing.T) (map[int64]AggregateResult, map[int64]map[int64]struct{ count, sum int64 }) {
	t.Helper()
	rows := make(map[int64]AggregateResult)
	perSource := make(map[int64]map[int64]struct{ count, sum int64 })
	for _, start := range renumberWindowStarts {
		rows[start] = AggregateResult{Key: "k", Start: start, End: start + renumberWindowLength}
		perSource[start] = make(map[int64]struct{ count, sum int64 })
	}
	for _, ev := range acceptedSourceEvents {
		for _, start := range renumberWindowStarts {
			if ev.time >= start && ev.time < start+renumberWindowLength {
				r := rows[start]
				r.Count++
				r.Sum += ev.value
				rows[start] = r
				c := perSource[start][ev.source]
				c.count++
				c.sum += ev.value
				perSource[start][ev.source] = c
			}
		}
	}
	return rows, perSource
}

// TestSlidingPartitionRenumberingInvariantSuccess is the core guarantee:
// running the same business stream under the original labels and under the
// relabeling bijection produces exactly the same window output (content, row
// count and order) and the same late notices (event time, watermark and
// physical line number), and both runs succeed. The relabeling touches
// events, watermarks and idle declarations alike and never merges sources or
// leaves a record kind behind.
func TestSlidingPartitionRenumberingInvariantSuccess(t *testing.T) {
	original := renumberFixtureSuccessLines()
	relabeled := renumberPartitionFields(t, original, partitionRenumberPermutation)
	// Guard the guard: the relabeling really is a same-cardinality bijection
	// and really rewrote every partition-bearing record of every kind.
	assertPermutation(t, partitionRenumberPermutation, 3)
	assertEveryPartitionFieldMapped(t, original, relabeled, partitionRenumberPermutation)

	controlOut, controlLate, controlErr := runPartitionedSliding(t, strings.Join(original, "\n")+"\n", 1000, 600, 3)
	movedOut, movedLate, movedErr := runPartitionedSliding(t, strings.Join(relabeled, "\n")+"\n", 1000, 600, 3)

	if controlErr != nil {
		t.Fatalf("the control run must succeed: %v", controlErr)
	}
	if movedErr != nil {
		t.Fatalf("the relabeled run must succeed: %v", movedErr)
	}
	if controlOut != renumberFixtureWantStdout {
		t.Fatalf("control stdout mismatch:\n got: %q\nwant: %q", controlOut, renumberFixtureWantStdout)
	}
	if movedOut != controlOut {
		t.Fatalf("renumbering changed the window results:\n original: %q\nrelabeled: %q", controlOut, movedOut)
	}
	if controlLate != renumberFixtureWantLate {
		t.Fatalf("control late notice mismatch:\n got: %q\nwant: %q", controlLate, renumberFixtureWantLate)
	}
	if movedLate != controlLate {
		t.Fatalf("renumbering changed the late notice (event time/watermark/physical line must stay put):\n original: %q\nrelabeled: %q", controlLate, movedLate)
	}
	if strings.Contains(movedOut, "partition") {
		t.Errorf("the source number must never enter the output: %q", movedOut)
	}

	got := decodeAggregateResults(t, movedOut)
	if len(got) != len(renumberFixtureWantRows) {
		t.Fatalf("row count = %d, want %d", len(got), len(renumberFixtureWantRows))
	}
	for i := range renumberFixtureWantRows {
		if got[i] != renumberFixtureWantRows[i] {
			t.Errorf("row %d = %+v, want %+v", i, got[i], renumberFixtureWantRows[i])
		}
	}

	// Independently recompute every closed window from each source's valid
	// events; the merged rows and the per-source split must both reconcile.
	rows, perSource := recomputeRowsBySource(t)
	for _, start := range renumberWindowStarts {
		want := rows[start]
		var gotRow *AggregateResult
		for i := range got {
			if got[i].Start == start {
				gotRow = &got[i]
			}
		}
		if gotRow == nil {
			t.Errorf("window starting %d was not emitted", start)
			continue
		}
		if *gotRow != want {
			t.Errorf("window [%d,%d) = %+v, want %+v from per-source events", start, want.End, gotRow, want)
		}
		var sCount, sSum int64
		for _, c := range perSource[start] {
			sCount += c.count
			sSum += c.sum
		}
		if sCount != want.Count || sSum != want.Sum {
			t.Errorf("per-source contributions to [%d,%d) sum to count %d/sum %d, want %d/%d", start, want.End, sCount, sSum, want.Count, want.Sum)
		}
	}
	// The overlapping windows (slide 600 does not divide length 1000) are
	// where the same key's events from all three sources actually merge; the
	// early [0,1000) window legitimately holds only the fast source's event.
	for _, start := range []int64{600, 1200} {
		if len(perSource[start]) != 3 {
			t.Errorf("overlapping window starting %d should merge the same key's events from all 3 sources, got %d", start, len(perSource[start]))
		}
	}
}

// TestSlidingPartitionRenumberingResumeFailureBelowOwnOldWatermark keeps the
// branch where a sleeping source's resume watermark is below its OWN previous
// watermark. The failure must remain an *InputError on the same physical line
// (line 12) naming the actually violated bound ("previous watermark 1600"),
// while the number named for the source follows the relabeling (partition 1
// -> partition 0). Output already produced before the failure stays exactly
// as it was, and records after the failing line produce neither results nor
// late notices -- under either labeling.
func TestSlidingPartitionRenumberingResumeFailureBelowOwnOldWatermark(t *testing.T) {
	lines := renumberFixtureSuccessLines()[:11]
	lines = append(lines,
		`{"type":"watermark","time":900,"partition":1}`, // line 12: 900 < the source's own old 1600
		// Dead suffix: must never run after the fatal line 12.
		`{"type":"event","key":"k","time":998,"value":11,"partition":0}`, // line 13: would be late if reached
		`{"type":"watermark","time":1600,"partition":1}`,                 // line 14
		`{"type":"watermark","time":2200,"partition":0}`,                 // line 15
		`{"type":"watermark","time":2200,"partition":2}`,                 // line 16
	)
	cases := []struct {
		name       string
		recs       []string
		wantSource string
	}{
		{name: "original labels", recs: lines, wantSource: "partition 1"},
		{name: "relabeled 1->0", recs: renumberPartitionFields(t, lines, partitionRenumberPermutation), wantSource: "partition 0"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, late, err := runPartitionedSliding(t, strings.Join(tc.recs, "\n")+"\n", 1000, 600, 3)
			ie := inputErrorFrom(t, err)
			if ie.Line != 12 {
				t.Errorf("InputError.Line = %d, want 12 (same physical line under relabeling)", ie.Line)
			}
			for _, sub := range []string{
				"resume watermark 900",
				tc.wantSource,
				"previous watermark 1600",
			} {
				if !strings.Contains(ie.Reason, sub) {
					t.Errorf("reason %q must contain %q", ie.Reason, sub)
				}
			}
			if strings.Contains(ie.Reason, "effective watermark") {
				t.Errorf("the violated limit is the source's own previous watermark, not the effective one: %q", ie.Reason)
			}
			// Pre-failure output and the line-9 late notice stay; the dead
			// suffix adds neither a window row nor another notice.
			wantOut := `{"key":"k","start":0,"end":1000,"count":1,"sum":2}` + "\n"
			if out != wantOut {
				t.Errorf("stdout mismatch:\n got: %q\nwant: %q", out, wantOut)
			}
			if late != renumberFixtureWantLate {
				t.Errorf("late notices mismatch:\n got: %q\nwant: %q", late, renumberFixtureWantLate)
			}
		})
	}
}

// TestSlidingPartitionRenumberingResumeFailureBelowEffectiveWatermark keeps
// the other failure cause: a source that goes idle BEFORE ever reporting a
// watermark has no previous watermark of its own, so a resume watermark below
// the current OVERALL effective watermark is rejected citing that overall
// bound -- resume legality is never decided from one source's own watermark.
// The error again stays on the same physical line and names the relabeled
// source number.
func TestSlidingPartitionRenumberingResumeFailureBelowEffectiveWatermark(t *testing.T) {
	lines := []string{
		`{"type":"event","key":"k","time":100,"value":1,"partition":0}`, // line 1
		`{"type":"event","key":"k","time":200,"value":2,"partition":2}`, // line 2
		`{"type":"watermark","time":1000,"partition":0}`,                // line 3
		`{"type":"watermark","time":1000,"partition":2}`,                // line 4
		`{"type":"idle","partition":1}`,                                 // line 5: never-reported source idles
		`{"type":"watermark","time":2000,"partition":0}`,                // line 6
		`{"type":"watermark","time":2000,"partition":2}`,                // line 7: overall effective = 2000
		`{"type":"watermark","time":1500,"partition":1}`,                // line 8: 1500 < overall 2000
		// Dead suffix.
		`{"type":"event","key":"k","time":1900,"value":8,"partition":1}`, // line 9
		`{"type":"watermark","time":2000,"partition":1}`,                 // line 10
	}
	cases := []struct {
		name       string
		recs       []string
		wantSource string
	}{
		{name: "original labels", recs: lines, wantSource: "partition 1"},
		{name: "relabeled 1->0", recs: renumberPartitionFields(t, lines, partitionRenumberPermutation), wantSource: "partition 0"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, late, err := runPartitionedSliding(t, strings.Join(tc.recs, "\n")+"\n", 1000, 600, 3)
			ie := inputErrorFrom(t, err)
			if ie.Line != 8 {
				t.Errorf("InputError.Line = %d, want 8 (same physical line under relabeling)", ie.Line)
			}
			for _, sub := range []string{
				"resume watermark 1500",
				tc.wantSource,
				"current effective watermark 2000",
			} {
				if !strings.Contains(ie.Reason, sub) {
					t.Errorf("reason %q must contain %q", ie.Reason, sub)
				}
			}
			if strings.Contains(ie.Reason, "previous watermark") {
				t.Errorf("a never-reported source has no previous watermark to cite: %q", ie.Reason)
			}
			wantOut := `{"key":"k","start":0,"end":1000,"count":2,"sum":3}` + "\n"
			if out != wantOut {
				t.Errorf("stdout mismatch:\n got: %q\nwant: %q", out, wantOut)
			}
			if late != "" {
				t.Errorf("a rejected resume produces no late notice, got %q", late)
			}
		})
	}
}

// TestSlidingPartitionRenumberingPermutationIsABijection pins the labeling
// constraint itself: the relabeling must be a one-to-one correspondence onto
// {0,1,2}, stay in range and keep using the zero and the maximum legal index,
// so sources can never be merged or dropped.
func TestSlidingPartitionRenumberingPermutationIsABijection(t *testing.T) {
	if !isPermutation(partitionRenumberPermutation, 3) {
		t.Fatalf("the renumbering under test must be a bijection: %v", partitionRenumberPermutation)
	}
	// Maps that merge sources, drop a source, leave the range or keep a
	// boundary role fixed must all be rejected by the same predicate.
	for name, bad := range map[string]map[int64]int64{
		"two sources merge into one": {0: 0, 1: 0, 2: 1},
		"a source is dropped":        {0: 2, 1: 0},
		"target out of range":        {0: 2, 1: 3, 2: 1},
		"zero role never moves":      {0: 0, 1: 2, 2: 1},
		"max role never moves":       {0: 1, 1: 0, 2: 2},
	} {
		if isPermutation(bad, 3) {
			t.Errorf("%s must not be accepted as a renumbering bijection: %v", name, bad)
		}
	}
}

// isPermutation reports whether m is a bijection of {0,...,n-1} onto itself:
// every source is covered and maps to a distinct in-range target, the zero and
// maximum legal indices are both reused, and neither boundary role keeps its
// original number (so the safeguard genuinely moves the boundary roles).
func isPermutation(m map[int64]int64, n int64) bool {
	if int64(len(m)) != n {
		return false
	}
	seen := make(map[int64]bool)
	for old, next := range m {
		if old < 0 || old >= n || next < 0 || next >= n {
			return false
		}
		if seen[next] {
			return false
		}
		seen[next] = true
	}
	if !seen[0] || !seen[n-1] {
		return false
	}
	if m[0] == 0 || m[n-1] == n-1 {
		return false
	}
	return true
}

// assertPermutation fails the test unless m is a renumbering bijection.
func assertPermutation(t *testing.T, m map[int64]int64, n int64) {
	t.Helper()
	if !isPermutation(m, n) {
		t.Fatalf("renumbering %v is not a bijection of [0,%d) onto itself", m, n)
	}
}

// assertEveryPartitionFieldMapped compares two physical input streams that
// differ only in partition labels: every record keeps its type and line
// position, and each partition-bearing record (event, watermark and idle
// alike) carries exactly f(original). This proves no record kind was missed
// and no source was merged or relabeled inconsistently.
func assertEveryPartitionFieldMapped(t *testing.T, original, relabeled []string, f map[int64]int64) {
	t.Helper()
	if len(original) != len(relabeled) {
		t.Fatalf("renumbering changed the record count: %d -> %d", len(original), len(relabeled))
	}
	kinds := map[string]int{}
	for i, before := range original {
		var o, rl map[string]json.RawMessage
		if err := json.Unmarshal([]byte(before), &o); err != nil {
			t.Fatalf("original line %d is not JSON: %v", i+1, err)
		}
		if err := json.Unmarshal([]byte(relabeled[i]), &rl); err != nil {
			t.Fatalf("relabeled line %d is not JSON: %v", i+1, err)
		}
		if string(o["type"]) != string(rl["type"]) {
			t.Errorf("line %d record type changed: %s -> %s", i+1, o["type"], rl["type"])
		}
		raw, ok := o["partition"]
		if !ok {
			t.Errorf("line %d (%s) carries no partition field to relabel", i+1, o["type"])
			continue
		}
		var oldP int64
		if err := json.Unmarshal(raw, &oldP); err != nil {
			t.Fatalf("line %d partition is not an integer: %v", i+1, err)
		}
		var newP int64
		if err := json.Unmarshal(rl["partition"], &newP); err != nil {
			t.Fatalf("line %d relabeled partition is not an integer: %v", i+1, err)
		}
		if newP != f[oldP] {
			t.Errorf("line %d (%s): partition %d relabeled to %d, want %d", i+1, o["type"], oldP, newP, f[oldP])
		}
		kinds[string(o["type"])]++
	}
	for _, kind := range []string{`"event"`, `"watermark"`, `"idle"`} {
		if kinds[kind] == 0 {
			t.Errorf("the stream must include %s records so that kind cannot be skipped by renumbering", kind)
		}
	}
}

// renumberPartitionFields returns the physical records with every record's
// partition field replaced by f(its old value). It rewrites events,
// watermarks and idle declarations with the single correspondence and leaves
// record order and all other fields untouched. Re-marshaling changes only
// formatting, never decoded content, so the engine sees an identical stream
// apart from source labels.
func renumberPartitionFields(t *testing.T, records []string, f map[int64]int64) []string {
	t.Helper()
	out := make([]string, len(records))
	for i, line := range records {
		var obj map[string]json.RawMessage
		if err := json.Unmarshal([]byte(line), &obj); err != nil {
			t.Fatalf("line %d is not JSON: %v", i+1, err)
		}
		raw, ok := obj["partition"]
		if !ok {
			t.Fatalf("line %d has no partition field to renumber: %s", i+1, line)
		}
		var p int64
		if err := json.Unmarshal(raw, &p); err != nil {
			t.Fatalf("line %d partition is not an integer: %v", i+1, err)
		}
		next, ok := f[p]
		if !ok {
			t.Fatalf("line %d: no renumbering target for source %d", i+1, p)
		}
		obj["partition"] = json.RawMessage(fmt.Sprintf("%d", next))
		b, err := json.Marshal(obj)
		if err != nil {
			t.Fatalf("line %d could not be re-marshaled: %v", i+1, err)
		}
		out[i] = string(b)
	}
	return out
}
