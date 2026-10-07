package edgefleet

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"testing"
)

// Interleaving invariance for partitioned sliding windows: when the records
// of several partitions are merged into one aggregation run, each partition
// keeping its own record order, the choice of inter-partition interleaving
// must not change the final result. Every interleaving is a valid input on
// its own (per-partition watermarks only repeat or advance, and an event
// sent after its partition's watermark never predates it), so every one of
// them must succeed with an empty late log and produce the same result rows,
// counts, sums and output order.
//
// The fixture uses window length 1000 and slide 600, so windows overlap:
// [0,1000), [600,1600), [1200,2200). The same key "k" is contributed by both
// partitions, with events in the overlap regions (700, 1300) and exactly on
// a window boundary (1000, which belongs to [600,1600) but not [0,1000)).
// Events arrive out of time order within each partition. Partition 0
// reports early and far ahead (2200) while partition 1 has not reported at
// all, then repeats that watermark; partition 1 reports late and then limits
// the effective watermark (1000, 1600, 2200). Both partitions end on the
// same watermark 2200, which closes every window that has events.
//
// Why no interleaving can produce a late event here: an event that precedes
// its own partition's first watermark is processed while that partition is
// still unreported, so no effective watermark exists yet and nothing is
// late; an event after its partition's watermark w has time >= w >= the
// effective (minimum) watermark. Lateness keeps being judged against the
// overall effective watermark -- these inputs simply never violate it.

// interleavePart0 is partition 0's record sequence, in its fixed order. It
// reports its first watermark early and far ahead of the events, then
// repeats it; the repeat must never add results.
var interleavePart0 = []string{
	`{"type":"event","key":"k","time":700,"value":2,"partition":0}`,  // overlap: [0,1000) and [600,1600)
	`{"type":"event","key":"k","time":1000,"value":3,"partition":0}`, // boundary: only [600,1600)
	`{"type":"event","key":"z","time":100,"value":1,"partition":0}`,  // out of time order: [0,1000)
	`{"type":"watermark","time":2200,"partition":0}`,                 // early and clearly ahead
	`{"type":"watermark","time":2200,"partition":0}`,                 // repeat: no new results
}

// interleavePart1 is partition 1's record sequence, in its fixed order. It
// reports its first watermark late and then limits the effective watermark;
// its event at time 1000 is sent after reporting 1000, which the
// preconditions allow (equal is not late).
var interleavePart1 = []string{
	`{"type":"event","key":"k","time":1300,"value":7,"partition":1}`,  // overlap: [600,1600) and [1200,2200)
	`{"type":"event","key":"k","time":100,"value":5,"partition":1}`,   // out of time order: [0,1000)
	`{"type":"event","key":"a","time":200,"value":4,"partition":1}`,   // [0,1000)
	`{"type":"watermark","time":1000,"partition":1}`,                  // first report: effective = 1000
	`{"type":"event","key":"k","time":1000,"value":11,"partition":1}`, // time == last reported watermark
	`{"type":"watermark","time":1600,"partition":1}`,
	`{"type":"watermark","time":2200,"partition":1}`, // same final watermark as partition 0
}

// interleaveWant is the single result every interleaving must produce: rows
// ordered by window end and then by decoded key in UTF-8 byte order, with no
// partition field. Each event contributes one count and its full value to
// every window containing its time:
//
//   - [0,1000):    a=200/4; k=700/2 + 100/5; z=100/1
//   - [600,1600):  k=700/2 + 1000/3 + 1300/7 + 1000/11
//   - [1200,2200): k=1300/7
const interleaveWant = `{"key":"a","start":0,"end":1000,"count":1,"sum":4}` + "\n" +
	`{"key":"k","start":0,"end":1000,"count":2,"sum":7}` + "\n" +
	`{"key":"z","start":0,"end":1000,"count":1,"sum":1}` + "\n" +
	`{"key":"k","start":600,"end":1600,"count":4,"sum":23}` + "\n" +
	`{"key":"k","start":1200,"end":2200,"count":1,"sum":7}` + "\n"

// interleavings returns every merge of a and b that keeps each slice's own
// record order: exactly the inputs the invariance guarantee covers.
func interleavings(a, b []string) [][]string {
	var out [][]string
	var rec func(ai, bi int, prefix []string)
	rec = func(ai, bi int, prefix []string) {
		if ai == len(a) && bi == len(b) {
			out = append(out, append([]string(nil), prefix...))
			return
		}
		if ai < len(a) {
			rec(ai+1, bi, append(prefix, a[ai]))
		}
		if bi < len(b) {
			rec(ai, bi+1, append(prefix, b[bi]))
		}
	}
	rec(0, 0, nil)
	return out
}

// Every interleaving of the two partition sequences succeeds, leaves the
// late log empty and produces byte-identical final output.
func TestAggregatePartitionedSlidingInterleaveInvariance(t *testing.T) {
	merges := interleavings(interleavePart0, interleavePart1)
	if got, want := len(merges), 792; got != want { // C(12,5): 5 records of p0 placed among 12
		t.Fatalf("interleaving count = %d, want %d", got, want)
	}
	for i, records := range merges {
		input := strings.Join(records, "\n") + "\n"
		stdout, stderr, err := runPartitionedSliding(t, input, 1000, 600, 2)
		if err != nil {
			t.Fatalf("interleaving %d: unexpected error: %v\ninput:\n%s", i, err, input)
		}
		if stderr != "" {
			t.Fatalf("interleaving %d: late log must be empty, got %q\ninput:\n%s", i, stderr, input)
		}
		if stdout != interleaveWant {
			t.Fatalf("interleaving %d changed the result:\n got: %q\nwant: %q\ninput:\n%s", i, stdout, interleaveWant, input)
		}
	}
}

// lineAtATimeReader delivers exactly one record (with its newline) per Read,
// so while a record is being processed next holds its physical line number.
type lineAtATimeReader struct {
	lines []string
	next  int
}

func (r *lineAtATimeReader) Read(p []byte) (int, error) {
	if r.next >= len(r.lines) {
		return 0, io.EOF
	}
	n := copy(p, r.lines[r.next]+"\n")
	r.next++
	return n, nil
}

// emission is one window-result write together with the physical input line
// whose processing triggered it.
type emission struct {
	line int
	text string
}

// emissionWriter records each window result against the input line that
// caused it, exposing the emission schedule: at which input position each
// window closed and how many windows one closure batch contained.
type emissionWriter struct {
	reader *lineAtATimeReader
	log    []emission
}

func (w *emissionWriter) Write(p []byte) (int, error) {
	w.log = append(w.log, emission{line: w.reader.next, text: string(p)})
	return len(p), nil
}

// runInterleaveSchedule runs one interleaving and returns its emission
// schedule. The run must succeed with an empty late log.
func runInterleaveSchedule(t *testing.T, records []string) []emission {
	t.Helper()
	reader := &lineAtATimeReader{lines: records}
	writer := &emissionWriter{reader: reader}
	var late bytes.Buffer
	if err := RunAggregatePartitionedSliding(reader, 1000, 600, 2, writer, &late); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if late.Len() != 0 {
		t.Fatalf("late log must be empty, got %q", late.String())
	}
	return writer.log
}

// emissionsAt returns the result lines emitted while processing one physical
// input line.
func emissionsAt(log []emission, line int) []string {
	var out []string
	for _, e := range log {
		if e.line == line {
			out = append(out, e.text)
		}
	}
	return out
}

// Interleavings may legitimately differ in when a window is emitted and in
// how many windows one watermark closes, and the fixture must actually
// exercise those differences: partition 0's high watermark alone closes
// nothing while partition 1 is unreported, a repeated watermark closes
// nothing, and depending on the merge the same window closes at different
// input positions, alone or in a multi-window batch. What must not differ is
// the emitted content.
func TestAggregatePartitionedSlidingInterleaveEmissionSchedule(t *testing.T) {
	// Partition 0 fully first: its watermark 2200 (line 4) and the repeat
	// (line 5) close nothing while partition 1 is unreported. Partition 1
	// then closes one window per watermark: [0,1000) at line 9, [600,1600)
	// at line 11, [1200,2200) at line 12.
	part0First := append(append([]string(nil), interleavePart0...), interleavePart1...)
	log := runInterleaveSchedule(t, part0First)
	for line := 1; line <= 8; line++ {
		if got := emissionsAt(log, line); len(got) != 0 {
			t.Fatalf("partition 0 alone must not close windows, but line %d emitted %q", line, got)
		}
	}
	if got := emissionsAt(log, 9); len(got) != 3 {
		t.Fatalf("line 9 (p1 watermark 1000) must close [0,1000) for a, k, z: got %q", got)
	}
	if got := emissionsAt(log, 11); len(got) != 1 {
		t.Fatalf("line 11 (p1 watermark 1600) must close only [600,1600): got %q", got)
	}
	if got := emissionsAt(log, 12); len(got) != 1 {
		t.Fatalf("line 12 (p1 watermark 2200) must close only [1200,2200): got %q", got)
	}

	// Partition 1 fully first: its watermarks close nothing while partition
	// 0 is unreported. Partition 0's first watermark (line 11) then closes
	// all three windows in one batch, and its repeat (line 12) adds nothing.
	part1First := append(append([]string(nil), interleavePart1...), interleavePart0...)
	log = runInterleaveSchedule(t, part1First)
	for line := 1; line <= 10; line++ {
		if got := emissionsAt(log, line); len(got) != 0 {
			t.Fatalf("no effective watermark exists before line 11, but line %d emitted %q", line, got)
		}
	}
	if got := emissionsAt(log, 11); len(got) != 5 {
		t.Fatalf("line 11 (p0 watermark 2200) must close all three windows (5 result lines) in one batch: got %q", got)
	}
	if got := emissionsAt(log, 12); len(got) != 0 {
		t.Fatalf("repeated watermark must not re-emit closed windows, got %q", got)
	}

	// Across every interleaving the emitted content is always exactly the
	// expected five lines in order, while the schedules genuinely diverge:
	// the same window is emitted at different input positions and closure
	// batches of different sizes occur.
	schedules := make(map[string]int)
	for i, records := range interleavings(interleavePart0, interleavePart1) {
		log := runInterleaveSchedule(t, records)
		var content, schedule strings.Builder
		for _, e := range log {
			content.WriteString(e.text)
			fmt.Fprintf(&schedule, "%d:%s", e.line, e.text)
		}
		if content.String() != interleaveWant {
			t.Fatalf("interleaving %d emitted %q, want %q", i, content.String(), interleaveWant)
		}
		schedules[schedule.String()]++
	}
	if len(schedules) < 2 {
		t.Fatalf("fixture must exercise diverging emission schedules, got a single schedule for all interleavings")
	}
}
