package edgefleet

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// Regression safeguard for inter-partition arrival-order invariance in
// partitioned sliding mode (window length 1000, slide interval 600, two
// partitions, keys "a" and "é").
//
// The guarantee under test: merge the per-partition record streams into one
// RunAggregatePartitionedSliding run in every possible way -- keeping each
// partition's own record order fixed and changing ONLY how the two streams
// interleave -- and every such run must
//
//   - succeed with no late-event notice at all,
//   - emit exactly the same result lines, with the same counts and sums,
//   - emit them in the same final order (window end ascending, then the
//     DECODED key in UTF-8 byte order),
//   - count every event exactly once in each window that contains it and emit
//     every window exactly once,
//   - never name a partition number in the output.
//
// Different interleavings are allowed to move WHERE a window is emitted and
// HOW MANY windows close on one trigger; the final table must not move.
//
// The fixture plays the real watermark-progress asymmetry the guarantee cares
// about:
//
//   - Partition 0 reports early and races clearly ahead (watermark 1600 while
//     partition 1 has never reported); that high watermark alone must not close
//     a single window.
//   - Partition 1 reports late for the first time (1000) and then pins the
//     overall effective watermark; only the per-partition minimum may close
//     due windows.
//   - Both partitions finish on the SAME watermark 2800, which closes every
//     event-bearing window, and then input ends. Repeated watermarks
//     (1600 on partition 0, 1000 on partition 1) must never add results.
//
// Every input satisfies the usage preconditions the guarantee is scoped to:
// per-partition watermarks only repeat or advance; after a partition reports a
// watermark its later events are not below the most recent value (equality is
// allowed); events nevertheless arrive out of event-time order within a
// partition; all fields are legal; values never overflow; no idle/resume is
// involved. Lateness keeps being judged against the OVERALL effective
// watermark -- nothing here switches to a per-partition-watermark drop rule;
// the preconditions are exactly why all legal interleavings are late-free.

// interleaveStep is one per-partition record before JSON rendering. Keeping
// the fixture as typed steps makes the fixture precondition checks and the
// independent per-window recomputation use the same single source of truth as
// the records actually fed to the public entry point.
type interleaveStep struct {
	kind      string // "event" or "watermark"
	partition int64
	time      int64
	key       string // events only
	value     int64  // events only
}

const (
	interleaveWindowLength int64 = 1000
	interleaveSlideStep    int64 = 600
	interleavePartitions   int64 = 2
	interleaveFinalMark    int64 = 2800 // same final watermark on every partition
)

// interleaveFixtureSteps is the whole 18-record stream, nine records per
// partition. Partition indices in comments are 0-based step positions WITHIN
// that partition (not physical input lines, which depend on the interleaving).
//
// Partition 0 (fast, reports early and clearly ahead):
//
//	p0[0] event "a"  t=600  v=2    -> [0,1000) and [600,1600) (window edge)
//	p0[1] event "é" t=1000 v=4    -> [600,1600) only (right edge of [0,1000))
//	p0[2] watermark 1600          -> races ahead while p1 is still unreported
//	p0[3] event "a"  t=1600 v=8    -> [1200,2200) only (right edge of [600,1600))
//	p0[4] event "é" t=1900 v=16   -> [1200,2200) and [1800,2800) (overlap)
//	p0[5] watermark 1600          -> repeat: nothing may close
//	p0[6] event "a"  t=2000 v=32   -> [1200,2200) and [1800,2800)
//	p0[7] event "a"  t=1800 v=64   -> arrives AFTER t=2000 (out of time order),
//	                                  t == 1800 edge of [1800,2800)
//	p0[8] watermark 2800          -> final, shared with p1
//
// Partition 1 (late first report, then the limiter):
//
//	p1[0] event "a"  t=700  v=3    -> [0,1000) and [600,1600) (overlap)
//	p1[1] event "é" t=1200 v=9    -> [600,1600) and [1200,2200) (window edge)
//	p1[2] watermark 1000          -> first report, late: pins the minimum at 1000
//	p1[3] event "a"  t=1500 v=27   -> [600,1600) and [1200,2200)
//	p1[4] event "é" t=1000 v=81   -> arrives AFTER t=1500 (out of time order);
//	                                  t == watermark 1000 is still accepted into
//	                                  [600,1600)
//	p1[5] watermark 1000          -> repeat: minimum stays 1000, nothing closes
//	p1[6] watermark 1600          -> minimum advances to 1600
//	p1[7] event "a"  t=2200 v=243  -> [1800,2800) only (right edge of [1200,2200))
//	p1[8] watermark 2800          -> final, shared with p0
var interleaveFixtureSteps = []interleaveStep{
	// Partition 0.
	{kind: "event", partition: 0, time: 600, key: "a", value: 2},
	{kind: "event", partition: 0, time: 1000, key: "é", value: 4},
	{kind: "watermark", partition: 0, time: 1600},
	{kind: "event", partition: 0, time: 1600, key: "a", value: 8},
	{kind: "event", partition: 0, time: 1900, key: "é", value: 16},
	{kind: "watermark", partition: 0, time: 1600},
	{kind: "event", partition: 0, time: 2000, key: "a", value: 32},
	{kind: "event", partition: 0, time: 1800, key: "a", value: 64},
	{kind: "watermark", partition: 0, time: interleaveFinalMark},
	// Partition 1.
	{kind: "event", partition: 1, time: 700, key: "a", value: 3},
	{kind: "event", partition: 1, time: 1200, key: "é", value: 9},
	{kind: "watermark", partition: 1, time: 1000},
	{kind: "event", partition: 1, time: 1500, key: "a", value: 27},
	{kind: "event", partition: 1, time: 1000, key: "é", value: 81},
	{kind: "watermark", partition: 1, time: 1000},
	{kind: "watermark", partition: 1, time: 1600},
	{kind: "event", partition: 1, time: 2200, key: "a", value: 243},
	{kind: "watermark", partition: 1, time: interleaveFinalMark},
}

// interleavePartitionSteps returns one partition's record stream in its fixed
// internal order.
func interleavePartitionSteps(p int64) []interleaveStep {
	var out []interleaveStep
	for _, st := range interleaveFixtureSteps {
		if st.partition == p {
			out = append(out, st)
		}
	}
	return out
}

// renderInterleaveStep renders one fixture step in the public line-delimited
// JSON input format; the aggregate entry point and error conventions are
// untouched.
func renderInterleaveStep(st interleaveStep) string {
	if st.kind == "event" {
		return fmt.Sprintf(
			`{"type":"event","key":%s,"time":%d,"value":%d,"partition":%d}`,
			strconv.Quote(st.key), st.time, st.value, st.partition)
	}
	return fmt.Sprintf(`{"type":"watermark","time":%d,"partition":%d}`, st.time, st.partition)
}

func renderInterleaveSteps(steps []interleaveStep) []string {
	lines := make([]string, len(steps))
	for i, st := range steps {
		lines[i] = renderInterleaveStep(st)
	}
	return lines
}

// interleaveWindowStarts are the starts of the windows the final shared
// watermark 2800 can close (ends 1000, 1600, 2200, 2800). The fixture puts
// events in every one of them; [2400,3400) and later stay uncreated, and no
// open window is flushed at end of input.
var interleaveWindowStarts = []int64{0, 600, 1200, 1800}

// recomputeInterleaveRows rebuilds the expected result table independently of
// the engine's slide arithmetic, using plain half-open interval containment
// (start <= t < start+length) and contributing one count and the FULL value
// per containing window. Rows come back ordered by end and then by decoded key
// in UTF-8 byte order, exactly as the engine must publish them.
func recomputeInterleaveRows(t *testing.T) []AggregateResult {
	t.Helper()
	type totals struct {
		count int64
		sum   int64
	}
	acc := make(map[int64]map[string]totals)
	for _, start := range interleaveWindowStarts {
		acc[start] = make(map[string]totals)
	}
	for _, st := range interleaveFixtureSteps {
		if st.kind != "event" {
			continue
		}
		contributed := 0
		for _, start := range interleaveWindowStarts {
			if st.time >= start && st.time < start+interleaveWindowLength {
				c := acc[start][st.key]
				c.count++
				c.sum += st.value
				acc[start][st.key] = c
				contributed++
			}
		}
		// Every fixture event must actually land in at least one window the
		// final watermark closes, so each event participates in the
		// cross-interleaving equality.
		if contributed == 0 {
			t.Fatalf("fixture event t=%d lands in no closed window", st.time)
		}
	}
	var rows []AggregateResult
	for start, byKey := range acc {
		for key, c := range byKey {
			rows = append(rows, AggregateResult{
				Key:   key,
				Start: start,
				End:   start + interleaveWindowLength,
				Count: c.count,
				Sum:   c.sum,
			})
		}
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].End != rows[j].End {
			return rows[i].End < rows[j].End
		}
		return bytes.Compare([]byte(rows[i].Key), []byte(rows[j].Key)) < 0
	})
	return rows
}

// canonicalInterleaveOutput renders the expected rows in the engine's exact
// one-JSON-object-per-line format.
func canonicalInterleaveOutput(t *testing.T, rows []AggregateResult) string {
	t.Helper()
	var b strings.Builder
	for _, r := range rows {
		encoded, err := json.Marshal(r)
		if err != nil {
			t.Fatalf("cannot marshal expected row %+v: %v", r, err)
		}
		b.Write(encoded)
		b.WriteByte('\n')
	}
	return b.String()
}

// TestSlidingPartitionInterleavingFixturePreconditions guards the guard: the
// shared fixture itself must satisfy the usage conditions the invariance is
// scoped to, and must actually exercise the required shapes. It also pins the
// independently recomputed expected table and its end/key ordering.
func TestSlidingPartitionInterleavingFixturePreconditions(t *testing.T) {
	p0 := interleavePartitionSteps(0)
	p1 := interleavePartitionSteps(1)
	if len(p0) != 9 || len(p1) != 9 {
		t.Fatalf("fixture shape changed: p0=%d records, p1=%d records, want 9 each", len(p0), len(p1))
	}

	for p, stream := range map[int64][]interleaveStep{0: p0, 1: p1} {
		var lastWatermark int64
		haveWatermark := false
		sawOutOfOrderArrival := false
		prevEventTime := int64(-1)
		sawRepeatedWatermark := false
		sawEventAtWatermarkEquality := false
		for _, st := range stream {
			switch st.kind {
			case "watermark":
				if haveWatermark {
					if st.time < lastWatermark {
						t.Fatalf("partition %d watermark moved backwards %d -> %d", p, lastWatermark, st.time)
					}
					if st.time == lastWatermark {
						sawRepeatedWatermark = true
					}
				}
				lastWatermark, haveWatermark = st.time, true
			case "event":
				if haveWatermark && st.time < lastWatermark {
					t.Fatalf("partition %d event t=%d follows its watermark %d", p, st.time, lastWatermark)
				}
				if haveWatermark && st.time == lastWatermark {
					sawEventAtWatermarkEquality = true
				}
				if prevEventTime >= 0 && st.time < prevEventTime {
					sawOutOfOrderArrival = true
				}
				prevEventTime = st.time
			}
			if st.partition != p {
				t.Fatalf("partition stream %d contains a record labeled %d", p, st.partition)
			}
		}
		if !sawRepeatedWatermark {
			t.Errorf("partition %d must include a repeated watermark so repeats are covered", p)
		}
		if !sawOutOfOrderArrival {
			t.Errorf("partition %d must include events arriving out of event-time order", p)
		}
		// Partition 1 carries the t == most-recent-watermark case (t=1000).
		if p == 1 && !sawEventAtWatermarkEquality {
			t.Errorf("partition 1 must include an event exactly at its reported watermark")
		}
	}

	// The asymmetric progress the guarantee names: one source's first report
	// is clearly ahead of the other's, and the final mark closes all
	// event-bearing windows (the latest containing window ends exactly on it).
	if p0[2].kind != "watermark" || p0[2].time != 1600 {
		t.Fatalf("partition 0 must race ahead with an early watermark 1600, got %+v", p0[2])
	}
	if p1[2].kind != "watermark" || p1[2].time != 1000 {
		t.Fatalf("partition 1 must report late for the first time at 1000, got %+v", p1[2])
	}
	latestEnd := interleaveWindowStarts[len(interleaveWindowStarts)-1] + interleaveWindowLength
	if latestEnd != interleaveFinalMark {
		t.Fatalf("final watermark %d must close the last event-bearing window ending %d", interleaveFinalMark, latestEnd)
	}

	rows := recomputeInterleaveRows(t)
	want := []AggregateResult{
		{Key: "a", Start: 0, End: 1000, Count: 2, Sum: 5},      // 600/2 (p0), 700/3 (p1)
		{Key: "a", Start: 600, End: 1600, Count: 3, Sum: 32},   // 600/2, 700/3, 1500/27
		{Key: "é", Start: 600, End: 1600, Count: 3, Sum: 94},   // 1000/4, 1200/9, 1000/81
		{Key: "a", Start: 1200, End: 2200, Count: 4, Sum: 131}, // 1500/27, 1600/8, 2000/32, 1800/64
		{Key: "é", Start: 1200, End: 2200, Count: 2, Sum: 25},  // 1200/9, 1900/16
		{Key: "a", Start: 1800, End: 2800, Count: 3, Sum: 339}, // 2000/32, 1800/64, 2200/243
		{Key: "é", Start: 1800, End: 2800, Count: 1, Sum: 16},  // 1900/16
	}
	if len(rows) != len(want) {
		t.Fatalf("recomputed %d rows, want %d: %+v", len(rows), len(want), rows)
	}
	for i := range want {
		if rows[i] != want[i] {
			t.Errorf("recomputed row %d = %+v, want %+v", i, rows[i], want[i])
		}
	}
	// Explicit ordering contract: end ascending, then decoded key UTF-8 bytes
	// ("a" 0x61 precedes "é" 0xC3 0xA9).
	for i := 1; i < len(rows); i++ {
		prev, cur := rows[i-1], rows[i]
		if prev.End > cur.End || (prev.End == cur.End && bytes.Compare([]byte(prev.Key), []byte(cur.Key)) >= 0) {
			t.Errorf("expected order violated between %+v and %+v", prev, cur)
		}
	}
}

// enumerateInterleavings invokes fn with every merge of a and b that preserves
// each input slice's internal order. Merges are generated in place and consumed
// synchronously, so the corpus costs O(len(a)+len(b)) memory regardless of the
// binomial number of leaves.
func enumerateInterleavings(a, b []string, fn func(merged []string)) {
	merged := make([]string, len(a)+len(b))
	var rec func(i, j int)
	rec = func(i, j int) {
		if i == len(a) && j == len(b) {
			fn(merged)
			return
		}
		pos := i + j
		if i < len(a) {
			merged[pos] = a[i]
			rec(i+1, j)
		}
		if j < len(b) {
			merged[pos] = b[j]
			rec(i, j+1)
		}
	}
	rec(0, 0)
}

// TestSlidingPartitionInterleavingInvariantAcrossAllMerges is the core
// guarantee: across EVERY legal interleaving of the two partition streams
// (all C(18,9) merges, per-partition order fixed), the run succeeds, the late
// log stays empty, and the published window table is byte-for-byte the same
// canonical output -- same rows, same counts and sums, same end/key order --
// with each window emitted exactly once and no partition number in the output.
func TestSlidingPartitionInterleavingInvariantAcrossAllMerges(t *testing.T) {
	p0 := renderInterleaveSteps(interleavePartitionSteps(0))
	p1 := renderInterleaveSteps(interleavePartitionSteps(1))
	wantRows := recomputeInterleaveRows(t)
	wantOutput := canonicalInterleaveOutput(t, wantRows)

	const wantMerges = 48620 // binomial(18,9): guards that the enumeration stays exhaustive
	merges := 0
	enumerateInterleavings(p0, p1, func(merged []string) {
		merges++
		input := strings.Join(merged, "\n") + "\n"
		stdout, stderr, err := runPartitionedSliding(t, input,
			interleaveWindowLength, interleaveSlideStep, interleavePartitions)
		if err != nil {
			t.Fatalf("interleaving #%d %v must succeed: %v", merges, merged, err)
		}
		if stderr != "" {
			t.Fatalf("interleaving #%d %v must produce no late notice, got %q", merges, merged, stderr)
		}
		if stdout != wantOutput {
			t.Fatalf("interleaving #%d changed the final window table:\n order: %q\n got:  %q\nwant: %q",
				merges, merged, stdout, wantOutput)
		}
		if strings.Contains(stdout, "partition") {
			t.Fatalf("interleaving #%d leaked a partition number into the output: %q", merges, stdout)
		}
		got := decodeAggregateResults(t, stdout)
		if len(got) != len(wantRows) {
			t.Fatalf("interleaving #%d emitted %d rows, want %d", merges, len(got), len(wantRows))
		}
		seen := make(map[[2]string]bool, len(got))
		for i, r := range got {
			if r != wantRows[i] {
				t.Errorf("interleaving #%d row %d = %+v, want %+v", merges, i, r, wantRows[i])
			}
			key := [2]string{strconv.FormatInt(r.Start, 10), r.Key}
			if seen[key] {
				t.Errorf("interleaving #%d emitted window start=%d key=%q more than once", merges, r.Start, r.Key)
			}
			seen[key] = true
		}
	})
	if merges != wantMerges {
		t.Fatalf("enumerated %d interleavings, want all %d", merges, wantMerges)
	}
}

// closureProfile runs every prefix of one merged stream and reports, for each
// result row (identified by window start and decoded key), the 1-based
// physical input position at which that row first appeared, together with the
// number of rows first emitted at each position (the closure batch size).
// Rows that appear only once therefore get exactly one position.
func closureProfile(t *testing.T, merged []string) (positions map[string]int, batchSize map[int]int) {
	t.Helper()
	positions = make(map[string]int)
	batchSize = make(map[int]int)
	seen := make(map[string]bool)
	for n := 1; n <= len(merged); n++ {
		stdout, stderr, err := runPartitionedSliding(t,
			strings.Join(merged[:n], "\n")+"\n",
			interleaveWindowLength, interleaveSlideStep, interleavePartitions)
		if err != nil {
			t.Fatalf("prefix of %d records failed: %v (stderr=%q)", n, err, stderr)
		}
		if stderr != "" {
			t.Fatalf("prefix of %d records produced a late notice: %q", n, stderr)
		}
		for _, r := range decodeAggregateResults(t, stdout) {
			id := fmt.Sprintf("%d|%s", r.Start, r.Key)
			if !seen[id] {
				seen[id] = true
				positions[id] = n
				batchSize[n]++
			}
		}
	}
	return positions, batchSize
}

func rowID(start int64, key string) string {
	return fmt.Sprintf("%d|%s", start, key)
}

// TestSlidingPartitionInterleavingMovesPositionsNotResults pins down WHERE the
// invariance allows differences: the same window may close at different
// physical input positions and one trigger may close a different NUMBER of
// windows, while one partition's high watermark alone never closes anything
// and repeated watermarks never add results. Three named interleavings are
// profiled record-by-record.
func TestSlidingPartitionInterleavingMovesPositionsNotResults(t *testing.T) {
	p0 := renderInterleaveSteps(interleavePartitionSteps(0))
	p1 := renderInterleaveSteps(interleavePartitionSteps(1))

	// Fast source fully ahead of the laggard: p0 already sits at watermark
	// 2800 (its records occupy positions 1-9) before p1 ever reports, yet
	// nothing may close until p1's first report at position 12.
	fastFirst := append(append([]string{}, p0...), p1...)
	// Laggard fully ahead: p1 reaches 2800 at position 9 while p0 is
	// completely unreported; the effective watermark stays unknown.
	laggardFirst := append(append([]string{}, p1...), p0...)
	// Round-robin: p0 reports 1600 at position 5 with p1 unreported; p1's
	// first report at position 6 is what closes the first window.
	alternating := make([]string, 0, len(p0)+len(p1))
	for i := 0; i < len(p0); i++ {
		alternating = append(alternating, p0[i], p1[i])
	}

	for _, merged := range [][]string{fastFirst, laggardFirst, alternating} {
		if len(merged) != 18 {
			t.Fatalf("named interleaving must keep all 18 records, got %d", len(merged))
		}
	}

	fastPos, fastBatch := closureProfile(t, fastFirst)
	lagPos, lagBatch := closureProfile(t, laggardFirst)
	altPos, altBatch := closureProfile(t, alternating)

	// Every profile must end with the same seven rows, each emitted once...
	wantRows := recomputeInterleaveRows(t)
	for name, positions := range map[string]map[string]int{
		"fast-first": fastPos, "laggard-first": lagPos, "alternating": altPos,
	} {
		if len(positions) != len(wantRows) {
			t.Fatalf("%s emitted %d distinct rows, want %d: %v", name, len(positions), len(wantRows), positions)
		}
		for _, r := range wantRows {
			if _, ok := positions[rowID(r.Start, r.Key)]; !ok {
				t.Errorf("%s never emitted window start=%d key=%q", name, r.Start, r.Key)
			}
		}
	}

	// ...and a high watermark on the only reporting source closes nothing:
	// no row may appear while the other partition is still fully unreported
	// (positions 1-9 in both block orderings).
	for name, positions := range map[string]map[string]int{"fast-first": fastPos, "laggard-first": lagPos} {
		for id, pos := range positions {
			if pos <= 9 {
				t.Errorf("%s closed %s at position %d while one partition had never reported", name, id, pos)
			}
		}
	}

	// The same window closes at different physical positions depending on the
	// interleaving: [0,1000) key "a" at 12 in both block orders but at 6
	// round-robin; [600,1600) key "a" lands on three different positions.
	if fastPos[rowID(0, "a")] != 12 || lagPos[rowID(0, "a")] != 12 || altPos[rowID(0, "a")] != 6 {
		t.Errorf("[0,1000)/a positions = fast %d, laggard %d, alternating %d, want 12/12/6",
			fastPos[rowID(0, "a")], lagPos[rowID(0, "a")], altPos[rowID(0, "a")])
	}
	w1 := rowID(600, "a")
	if fastPos[w1] != 16 || lagPos[w1] != 12 || altPos[w1] != 14 {
		t.Errorf("[600,1600)/a positions = fast %d, laggard %d, alternating %d, want 16/12/14",
			fastPos[w1], lagPos[w1], altPos[w1])
	}

	// One trigger may close a different number of windows: when the laggard
	// finally reports 1600 at position 12 of laggard-first, the dammed
	// [0,1000) and [600,1600) close together (3 rows); in fast-first the same
	// position 12 closes only the one [0,1000) row. Round-robin spreads the
	// first two closures over positions 6 (1 row) and 14 (2 rows).
	if fastBatch[12] != 1 {
		t.Errorf("fast-first position 12 must close 1 row, got %d", fastBatch[12])
	}
	if lagBatch[12] != 3 {
		t.Errorf("laggard-first position 12 must close 3 rows at once, got %d", lagBatch[12])
	}
	if altBatch[6] != 1 || altBatch[14] != 2 {
		t.Errorf("alternating batch sizes = pos6 %d (want 1), pos14 %d (want 2)", altBatch[6], altBatch[14])
	}
	// The final shared watermark closes the last two windows together (four
	// rows: two keys per window) at position 18 in every ordering.
	for name, batch := range map[string]map[int]int{
		"fast-first": fastBatch, "laggard-first": lagBatch, "alternating": altBatch,
	} {
		if batch[18] != 4 {
			t.Errorf("%s final trigger must close 4 rows at position 18, got %d", name, batch[18])
		}
	}

	// Repeated watermarks add no results: their physical positions must be
	// absent from the batch maps. In fast-first p0 repeats 1600 at position 6
	// and p1 repeats 1000 at 15; in laggard-first those swap to 6 (p1) and
	// 15 (p0); round-robin they sit at 11 (p0) and 12 (p1).
	if fastBatch[6] != 0 {
		t.Errorf("fast-first: repeated p0 watermark 1600 at position 6 added %d rows", fastBatch[6])
	}
	if fastBatch[15] != 0 {
		t.Errorf("fast-first: repeated p1 watermark 1000 at position 15 added %d rows", fastBatch[15])
	}
	if lagBatch[6] != 0 || lagBatch[15] != 0 {
		t.Errorf("laggard-first: repeated watermarks at positions 6/15 added %d/%d rows", lagBatch[6], lagBatch[15])
	}
	if altBatch[11] != 0 || altBatch[12] != 0 {
		t.Errorf("alternating: repeated watermarks at positions 11/12 added %d/%d rows", altBatch[11], altBatch[12])
	}
}

// TestSlidingPartitionInterleavingNamedOrdersAreCanonical is the direct
// end-to-end check on the three named interleavings: despite closures moving
// and batching differently, the complete final stdout is byte-for-byte
// identical and carries no late notice or partition label.
func TestSlidingPartitionInterleavingNamedOrdersAreCanonical(t *testing.T) {
	p0 := renderInterleaveSteps(interleavePartitionSteps(0))
	p1 := renderInterleaveSteps(interleavePartitionSteps(1))
	alternating := make([]string, 0, len(p0)+len(p1))
	for i := 0; i < len(p0); i++ {
		alternating = append(alternating, p0[i], p1[i])
	}
	want := canonicalInterleaveOutput(t, recomputeInterleaveRows(t))
	for name, merged := range map[string][]string{
		"fast-first":    append(append([]string{}, p0...), p1...),
		"laggard-first": append(append([]string{}, p1...), p0...),
		"alternating":   alternating,
	} {
		stdout, stderr, err := runPartitionedSliding(t, strings.Join(merged, "\n")+"\n",
			interleaveWindowLength, interleaveSlideStep, interleavePartitions)
		if err != nil {
			t.Fatalf("%s must succeed: %v", name, err)
		}
		if stderr != "" {
			t.Fatalf("%s must produce no late notice, got %q", name, stderr)
		}
		if stdout != want {
			t.Fatalf("%s final output mismatch:\n got: %q\nwant: %q", name, stdout, want)
		}
	}
}
