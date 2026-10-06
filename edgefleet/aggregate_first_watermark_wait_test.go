package edgefleet

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"
)

// These tests pin the waiting phase that precedes the first overall
// watermark in partitioned mode: deciding whether every partition has now
// spoken must not revisit the partitions already confirmed, and the first
// overall watermark -- plus the windows its establishing record closes --
// must depend only on the per-partition values, never on the order in which
// the partitions first speak. The minimum rule after the watermark exists is
// covered by the other aggregate tests; this file stays scoped to the wait.

// waitPermLines builds a stream where every partition holds one event at
// time 100 for key "k" and then reports its first watermark exactly once in
// the given partition order. The watermark values are fixed per partition,
// so reordering the reports changes only the wait bookkeeping. The final
// effective watermark is the minimum assigned value.
func waitPermLines(watermarks []int64, order []int64) string {
	var b strings.Builder
	for p := range watermarks {
		fmt.Fprintf(&b, `{"type":"event","key":"k","time":100,"value":1,"partition":%d}`+"\n", p)
	}
	for _, p := range order {
		fmt.Fprintf(&b, `{"type":"watermark","time":%d,"partition":%d}`+"\n", watermarks[p], p)
	}
	return b.String()
}

// perm reports whether seq is a permutation of 0..n-1.
func isIndexPermutation(seq []int64, n int) bool {
	if len(seq) != n {
		return false
	}
	seen := make(map[int64]bool, n)
	for _, v := range seq {
		if v < 0 || int(v) >= n || seen[v] {
			return false
		}
		seen[v] = true
	}
	return true
}

// TestAggregateFirstWatermarkIndependentOfReportOrder: with identical pending
// events and identical per-partition first watermarks, the order in which the
// partitions make their first report changes neither the first overall
// watermark value nor the windows its establishing record closes. In each
// permutation the last still-silent partition's report is the one record that
// establishes the minimum and immediately closes [0,1000) with every waiting
// event merged.
func TestAggregateFirstWatermarkIndependentOfReportOrder(t *testing.T) {
	const n = 5
	watermarks := []int64{1000, 4000, 2000, 5000, 3000} // min is partition 0's 1000
	orders := [][]int64{
		{0, 1, 2, 3, 4}, // ascending: minimum holder speaks first, last index last
		{4, 3, 2, 1, 0}, // descending: minimum holder is the final report
		{3, 0, 4, 1, 2}, // scrambled
		{2, 4, 1, 3, 0},
	}
	want := `{"key":"k","start":0,"end":1000,"count":5,"sum":5}` + "\n"
	for _, order := range orders {
		if !isIndexPermutation(order, n) {
			t.Fatalf("test setup: %v is not a permutation", order)
		}
		stdout, stderr, err := runPartitioned(t, waitPermLines(watermarks, order), 1000, n)
		if err != nil {
			t.Fatalf("order %v: unexpected error: %v", order, err)
		}
		if stderr != "" {
			t.Fatalf("order %v: events accepted during the wait must never be late, got %q", order, stderr)
		}
		if stdout != want {
			t.Fatalf("order %v:\n got: %q\nwant: %q", order, stdout, want)
		}
	}
}

// TestAggregateFirstWatermarkIdleOrderInvariance covers the other way a
// partition makes its first statement: an idle declaration. Whether the
// never-reported partition idles before the others report or as the very
// last statement, the first overall watermark is the same minimum over the
// reporters and the same window closes.
func TestAggregateFirstWatermarkIdleOrderInvariance(t *testing.T) {
	// Three partitions: p0 -> 1000, p1 -> 3000 report; p2 never reports and
	// is declared idle. Pending events for all three merge into [0,1000).
	events := strings.Join([]string{
		`{"type":"event","key":"k","time":100,"value":1,"partition":0}`,
		`{"type":"event","key":"k","time":200,"value":2,"partition":1}`,
		`{"type":"event","key":"k","time":300,"value":4,"partition":2}`,
	}, "\n") + "\n"
	want := `{"key":"k","start":0,"end":1000,"count":3,"sum":7}` + "\n"
	cases := map[string]string{
		"idle last": events + strings.Join([]string{
			`{"type":"watermark","time":1000,"partition":0}`,
			`{"type":"watermark","time":3000,"partition":1}`,
			`{"type":"idle","partition":2}`, // last statement: min over reporters = 1000
		}, "\n") + "\n",
		"idle first": events + strings.Join([]string{
			`{"type":"idle","partition":2}`, // never-reported partition leaves first
			`{"type":"watermark","time":1000,"partition":0}`,
			`{"type":"watermark","time":3000,"partition":1}`, // final report closes
		}, "\n") + "\n",
		"idle between reports": events + strings.Join([]string{
			`{"type":"watermark","time":3000,"partition":1}`,
			`{"type":"idle","partition":2}`, // p0 still silent: wait continues
			`{"type":"watermark","time":1000,"partition":0}`,
		}, "\n") + "\n",
	}
	for name, input := range cases {
		t.Run(name, func(t *testing.T) {
			stdout, stderr, err := runPartitioned(t, input, 1000, 3)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if stderr != "" {
				t.Fatalf("unexpected late notice: %q", stderr)
			}
			if stdout != want {
				t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
			}
		})
	}
}

// TestAggregateWaitRepeatsAreNotPartitionStatements: while one active
// partition stays silent, repeated watermarks from confirmed partitions and
// repeated idle declarations neither satisfy the wait early nor change its
// outcome. The result is byte-identical to the same history with every
// repetition removed, and the silent partition's first statement (a report
// in one case, an idle declaration in the other) is alone what establishes
// the watermark.
func TestAggregateWaitRepeatsAreNotPartitionStatements(t *testing.T) {
	events := strings.Join([]string{
		`{"type":"event","key":"k","time":100,"value":1,"partition":0}`,
		`{"type":"event","key":"k","time":100,"value":1,"partition":1}`,
		`{"type":"event","key":"k","time":100,"value":1,"partition":2}`,
	}, "\n") + "\n"
	want := `{"key":"k","start":0,"end":1000,"count":3,"sum":3}` + "\n"

	t.Run("repeated watermarks while straggler is silent", func(t *testing.T) {
		input := events + strings.Join([]string{
			`{"type":"watermark","time":5000,"partition":0}`,
			`{"type":"watermark","time":5000,"partition":0}`, // repeat
			`{"type":"watermark","time":5000,"partition":0}`, // repeat
			`{"type":"watermark","time":5000,"partition":1}`,
			`{"type":"watermark","time":5000,"partition":1}`, // repeat
			`{"type":"watermark","time":5000,"partition":0}`, // repeat
			`{"type":"watermark","time":1000,"partition":2}`, // straggler reports: min = 1000
			`{"type":"watermark","time":1000,"partition":2}`, // repeat after establishment: no re-emit
		}, "\n") + "\n"
		stdout, stderr, err := runPartitioned(t, input, 1000, 3)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if stderr != "" {
			t.Fatalf("unexpected late notice: %q", stderr)
		}
		if stdout != want {
			t.Fatalf("stdout = %q, want %q", stdout, want)
		}
	})

	t.Run("repeated idle is a no-op during the wait", func(t *testing.T) {
		input := events + strings.Join([]string{
			`{"type":"idle","partition":2}`, // straggler leaves without reporting
			`{"type":"idle","partition":2}`, // repeat: still two reporters pending
			`{"type":"watermark","time":5000,"partition":0}`,
			`{"type":"idle","partition":2}`, // repeat: p1 still silent
			`{"type":"watermark","time":1000,"partition":1}`,
			`{"type":"idle","partition":2}`, // repeat after closure: no re-emit
		}, "\n") + "\n"
		stdout, stderr, err := runPartitioned(t, input, 1000, 3)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if stderr != "" {
			t.Fatalf("unexpected late notice: %q", stderr)
		}
		if stdout != want {
			t.Fatalf("stdout = %q, want %q", stdout, want)
		}
	})
}

// TestAggregateWaitNeverJudgesFromReportedMinimum: while any active partition
// is still silent, the minimum over the partitions that already reported must
// not be used as the watermark -- neither to close a window nor to call an
// event late. An event below a reported partition's watermark is accepted
// during the wait and counted in the window that closes once the wait ends.
func TestAggregateWaitNeverJudgesFromReportedMinimum(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"watermark","time":9000,"partition":0}`,                // one partition races ahead
		`{"type":"event","key":"k","time":100,"value":1,"partition":1}`, // below 9000, yet legal
		`{"type":"event","key":"k","time":100,"value":1,"partition":2}`, // below 9000, yet legal
		`{"type":"event","key":"k","time":100,"value":1,"partition":0}`,
		`{"type":"watermark","time":2000,"partition":1}`,
		`{"type":"watermark","time":1000,"partition":2}`, // establishes 1000: closes [0,1000)
	}, "\n") + "\n"
	stdout, stderr, err := runPartitioned(t, input, 1000, 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stderr != "" {
		t.Fatalf("no event may be judged late before the wait ends, got %q", stderr)
	}
	want := `{"key":"k","start":0,"end":1000,"count":3,"sum":3}` + "\n"
	if stdout != want {
		t.Fatalf("stdout = %q, want %q", stdout, want)
	}
}

// TestAggregateAllIdleBeforeFirstWatermarkKeepsEvents: every partition goes
// idle without an effective watermark ever being produced, so it stays
// unknown: pending events are neither dropped nor judged late and no window
// closes at end of input. A later single-partition resume then establishes
// the watermark from that one partition and closes the window with the
// retained events.
func TestAggregateAllIdleBeforeFirstWatermarkKeepsEvents(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":100,"value":3,"partition":0}`,
		`{"type":"event","key":"k","time":200,"value":4,"partition":1}`,
		`{"type":"idle","partition":0}`,
		`{"type":"idle","partition":1}`, // all idle, nothing ever produced
	}, "\n") + "\n"
	stdout, stderr, err := runPartitioned(t, input, 1000, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stdout != "" {
		t.Fatalf("no window may close while the watermark never existed, got %q", stdout)
	}
	if stderr != "" {
		t.Fatalf("pending events must not be judged late against a missing watermark, got %q", stderr)
	}

	resumed := input + strings.Join([]string{
		`{"type":"watermark","time":1000,"partition":0}`, // resume alone: effective = 1000
	}, "\n") + "\n"
	stdout, stderr, err = runPartitioned(t, resumed, 1000, 2)
	if err != nil {
		t.Fatalf("unexpected error on resume: %v", err)
	}
	if stderr != "" {
		t.Fatalf("unexpected late notice: %q", stderr)
	}
	want := `{"key":"k","start":0,"end":1000,"count":2,"sum":7}` + "\n"
	if stdout != want {
		t.Fatalf("retained events must close once a watermark exists:\n got: %q\nwant: %q", stdout, want)
	}
}

// TestAggregateInvalidRecordDuringWaitSatisfiesNothing: an out-of-range
// partition record arriving while the wait is still open is fatal with its
// physical line number, closes nothing and cannot be the statement that
// completes the partition set.
func TestAggregateInvalidRecordDuringWaitSatisfiesNothing(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"watermark","time":9000,"partition":0}`,
		`{"type":"watermark","time":9000,"partition":9}`, // line 2: partition 9 not in [0,3)
		`{"type":"watermark","time":1000,"partition":1}`,
		`{"type":"watermark","time":1000,"partition":2}`,
	}, "\n") + "\n"
	var stdout bytes.Buffer
	err := RunAggregatePartitioned(strings.NewReader(input), 1000, 3, &stdout, &bytes.Buffer{})
	if err == nil {
		t.Fatal("expected an input error")
	}
	inputErr, ok := err.(*InputError)
	if !ok {
		t.Fatalf("expected *InputError, got %T: %v", err, err)
	}
	if inputErr.Line != 2 {
		t.Errorf("line = %d, want 2", inputErr.Line)
	}
	if !strings.Contains(inputErr.Reason, "[0,3)") {
		t.Errorf("reason = %q, want the valid partition range", inputErr.Reason)
	}
	if stdout.String() != "" {
		t.Fatalf("an invalid partition record must close nothing, got %q", stdout.String())
	}
}

// waitWorkload builds the complexity-guard workload. Both variants have the
// same partition count, the same number of records and the same records'
// content apart from which confirmed partition repeats: all first watermarks
// equal 5000 and the final straggler report is also 5000, so both runs are
// correct, produce no output and no errors.
//
//   - ascending: partitions 0..P-2 report in ascending order, then a
//     confirmed partition repeats R times while the highest-indexed
//     partition stays silent. A rescan-from-zero implementation walks all
//     P-1 confirmed partitions on every one of those R records (and an
//     ever-longer prefix during the first reports), so the wait is quadratic
//     in the partition count at a fixed record count.
//   - descending: partitions P-1..1 report first while partition 0 stays
//     silent; the same R repetitions then return from the very first
//     partition check, i.e. constant work per record.
func waitWorkload(partitions, repeats int64, ascending bool) string {
	var b strings.Builder
	if ascending {
		for p := int64(0); p < partitions-1; p++ {
			fmt.Fprintf(&b, `{"type":"watermark","time":5000,"partition":%d}`+"\n", p)
		}
		for i := int64(0); i < repeats; i++ {
			b.WriteString(`{"type":"watermark","time":5000,"partition":0}` + "\n")
		}
		fmt.Fprintf(&b, `{"type":"watermark","time":5000,"partition":%d}`+"\n", partitions-1)
	} else {
		for p := partitions - 1; p >= 1; p-- {
			fmt.Fprintf(&b, `{"type":"watermark","time":5000,"partition":%d}`+"\n", p)
		}
		for i := int64(0); i < repeats; i++ {
			// Partition 1 is the lowest-numbered reporter, so its stale
			// candidates sit at the heap root and prune exactly like
			// partition 0's do in the ascending variant.
			b.WriteString(`{"type":"watermark","time":5000,"partition":1}` + "\n")
		}
		b.WriteString(`{"type":"watermark","time":5000,"partition":0}` + "\n")
	}
	return b.String()
}

func runWaitWorkload(input string) {
	var out, late bytes.Buffer
	if err := RunAggregatePartitioned(strings.NewReader(input), 1000, waitGuardPartitions, &out, &late); err != nil {
		panic(err)
	}
	if out.Len() != 0 || late.Len() != 0 {
		panic("wait guard workload must produce no output")
	}
}

const (
	waitGuardPartitions int64 = 2000
	waitGuardRepeats    int64 = 12000
)

// bestWaitTime measures repeated runs of one workload, taking the
// lowest-noise average once the batch is long enough. The two workloads have
// identical record and byte counts, so their ratio isolates the per-record
// wait bookkeeping.
func bestWaitTime(input string, minBatch time.Duration) time.Duration {
	iters := 1
	best := time.Duration(1<<63 - 1)
	for attempt := 0; attempt < 6; attempt++ {
		begin := time.Now()
		for i := 0; i < iters; i++ {
			runWaitWorkload(input)
		}
		batch := time.Since(begin)
		if avg := batch / time.Duration(iters); avg < best {
			best = avg
		}
		if batch >= minBatch {
			return best
		}
		iters *= 4
	}
	return best
}

// TestAggregateFirstWatermarkWaitCostIsLinear is the complexity guard: the
// cost of receiving watermarks while one active partition is still silent
// must not grow with the number of partitions already confirmed. The
// ascending workload (each repeated record would rescan every confirmed
// partition) and the descending workload (the silent partition is seen
// immediately) have the same size and records, so an incremental wait costs
// essentially the same; the old rescan made the ascending workload thousands
// of times slower.
func TestAggregateFirstWatermarkWaitCostIsLinear(t *testing.T) {
	if testing.Short() {
		t.Skip("timing guard skipped in -short mode")
	}
	ascending := waitWorkload(waitGuardPartitions, waitGuardRepeats, true)
	descending := waitWorkload(waitGuardPartitions, waitGuardRepeats, false)
	if len(ascending) != len(descending) {
		t.Fatalf("test setup: workloads must be the same byte size, %d vs %d", len(ascending), len(descending))
	}
	// Sanity: both workloads are correct.
	runWaitWorkload(ascending)
	runWaitWorkload(descending)

	asc := bestWaitTime(ascending, 30*time.Millisecond)
	desc := bestWaitTime(descending, 30*time.Millisecond)
	const factor = 6.0
	if ratio := float64(asc) / float64(desc); ratio > factor {
		t.Fatalf("wait cost grows with the number of confirmed partitions: "+
			"ascending took %v/run, descending %v/run (ratio %.2f, want <= %.1f)",
			asc, desc, ratio, factor)
	}
	if asc > 2*time.Second {
		t.Fatalf("wait over %d partitions and %d records took %v/run, want under 2s",
			waitGuardPartitions, waitGuardPartitions-1+waitGuardRepeats+1, asc)
	}
}

// BenchmarkFirstWatermarkWaitAscending documents the linear wait cost in the
// layout that used to be quadratic: ascending first reports followed by many
// repeated records while the last partition stays silent.
func BenchmarkFirstWatermarkWaitAscending(b *testing.B) {
	input := waitWorkload(1000, 5000, true)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var out, late bytes.Buffer
		if err := RunAggregatePartitioned(strings.NewReader(input), 1000, 1000, &out, &late); err != nil {
			b.Fatal(err)
		}
	}
}
