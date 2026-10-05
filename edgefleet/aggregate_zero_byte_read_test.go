package edgefleet

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
)

// This file guards the zero-byte-read stall contract of the aggregate input
// boundary. A library entry point's reader may legitimately return (0, nil)
// while one record is still halfway through: that is neither end of input nor
// a read error, and the half-record must survive. Up to 100 consecutive such
// reads are tolerated and simply repeated; ANY read delivering bytes (even a
// fragment too short to complete a record) resets the consecutive count; the
// 101st consecutive zero-byte read ends the run with io.ErrNoProgress.
//
// Everything observable through that stall must stay byte-identical to an
// uninterrupted run: per-partition watermark merging, sliding-window results,
// output ordering and the physical line numbers on late notices. Once the
// progress limit fails the run, already complete output stays, open windows
// are not emitted late, a newline-less tail (even valid JSON) never becomes a record,
// and no further read is attempted. All scenarios are scripted in-memory, so
// they reproduce offline with no timing dependency.

// scriptedStep is one programmed Read behavior. When zero > 0 the next Reads
// return (0, nil) that many times first; then data (if any) is handed out
// across however many Reads the caller's buffer needs; err is returned alone
// as (0, err) once the step is reached, poisoning any Read the aggregate was
// not supposed to make.
type scriptedStep struct {
	data []byte
	zero int
	err  error
}

// scriptedReader serves a fixed Read script instead of relying on timing or
// goroutines, so stall behavior is deterministic offline.
type scriptedReader struct {
	steps []scriptedStep
	si    int
	off   int
}

func (r *scriptedReader) Read(p []byte) (int, error) {
	for r.si < len(r.steps) {
		st := &r.steps[r.si]
		if st.zero > 0 {
			st.zero--
			return 0, nil
		}
		if st.err != nil {
			return 0, st.err
		}
		if r.off < len(st.data) {
			n := copy(p, st.data[r.off:])
			r.off += n
			if r.off == len(st.data) {
				r.si++
				r.off = 0
			}
			return n, nil
		}
		r.si++
		r.off = 0
	}
	return 0, io.EOF
}

func stepData(s string) scriptedStep    { return scriptedStep{data: []byte(s)} }
func stepZeroReads(n int) scriptedStep  { return scriptedStep{zero: n} }
func stepPoison(err error) scriptedStep { return scriptedStep{err: err} }

// fragmentedSteps cuts s into fixed-size byte pieces, inserting shortPause
// zero-byte reads after every piece, so every record (and every multibyte key
// byte) is delivered split across many Reads with stalls in between. After
// the piece containing each requested offset an extra long pause is inserted,
// landing deliberately in the middle of a chosen event or watermark record.
type longPauseOffset struct {
	offset int
	pause  int
}

func fragmentedSteps(s string, size, shortPause int, longs ...longPauseOffset) []scriptedStep {
	var steps []scriptedStep
	for i := 0; i < len(s); i += size {
		j := i + size
		if j > len(s) {
			j = len(s)
		}
		steps = append(steps, stepData(s[i:j]))
		pause := shortPause
		for _, lp := range longs {
			if lp.offset >= i && lp.offset < j {
				pause = lp.pause
			}
		}
		steps = append(steps, stepZeroReads(pause))
	}
	return steps
}

// runScriptedPartitionedSliding runs the length-1000/step-600/two-partition
// sliding aggregation over a scripted reader, capturing both outputs.
func runScriptedPartitionedSliding(t *testing.T, steps []scriptedStep) (string, string, error) {
	t.Helper()
	var out, late bytes.Buffer
	err := RunAggregatePartitionedSliding(&scriptedReader{steps: steps}, 1000, 600, 2, &out, &late)
	return out.String(), late.String(), err
}

// stallScenarioLines is the reference stream for this file: length 1000ms,
// slide 600ms, two active partitions, one key shared across partitions.
//
//	line 1: partition 0 event 温度 time 700 value 2 -> [0,1000), [600,1600)
//	line 2: blank physical line
//	line 3: partition 1 event 温度 time 1000 value 3 -> [600,1600) only
//	line 4: partition 0 watermark 1600 (effective watermark still unknown)
//	line 5: partition 1 watermark 1000 (min 1000; [0,1000) closes)
//	line 6: blank physical line
//	line 7: partition 1 event time 999 value 9, late vs effective 1000
//	line 8: partition 1 watermark 1600 (min 1600; [600,1600) closes)
//
// Two blank lines are interleaved on purpose: they consume physical line
// numbers, while zero-byte reads must not, so the late notice has to name
// line 7 through every stall schedule below.
func stallScenarioLines() []string {
	return []string{
		`{"type":"event","key":"温度","time":700,"value":2,"partition":0}`,
		``,
		`{"type":"event","key":"温度","time":1000,"value":3,"partition":1}`,
		`{"type":"watermark","time":1600,"partition":0}`,
		`{"type":"watermark","time":1000,"partition":1}`,
		``,
		`{"type":"event","key":"温度","time":999,"value":9,"partition":1}`,
		`{"type":"watermark","time":1600,"partition":1}`,
	}
}

func stallScenarioInput() string {
	return strings.Join(stallScenarioLines(), "\n") + "\n"
}

var stallScenarioWindows = strings.Join([]string{
	`{"key":"温度","start":0,"end":1000,"count":1,"sum":2}`,
	`{"key":"温度","start":600,"end":1600,"count":2,"sum":5}`,
	``,
}, "\n")

const stallScenarioLate = "line 7: late event time=999 below current watermark 1000, skipped\n"

// TestZeroByteReadBaseline pins the uninterrupted result the stalled schedules
// below must reproduce byte for byte.
func TestZeroByteReadBaseline(t *testing.T) {
	var out, late bytes.Buffer
	if err := RunAggregatePartitionedSliding(
		strings.NewReader(stallScenarioInput()), 1000, 600, 2, &out, &late); err != nil {
		t.Fatalf("uninterrupted run: unexpected error: %v", err)
	}
	if out.String() != stallScenarioWindows {
		t.Fatalf("window output:\n got: %q\nwant: %q", out.String(), stallScenarioWindows)
	}
	if late.String() != stallScenarioLate {
		t.Fatalf("late notice:\n got: %q\nwant: %q", late.String(), stallScenarioLate)
	}
}

// TestZeroByteReadBoundaryInPrimitive pins the 100/101 boundary directly on
// readAggregateLines: 100 consecutive zero-byte reads are tolerated any number
// of times and advance nothing, the 101st ends the run with io.ErrNoProgress,
// and receiving bytes resets the consecutive count.
func TestZeroByteReadBoundaryInPrimitive(t *testing.T) {
	t.Run("exactly 100 stalls twice still completes", func(t *testing.T) {
		var got []collectedLine
		err := readAggregateLines(&scriptedReader{steps: []scriptedStep{
			stepData("a\n"),
			stepZeroReads(100),
			stepData("b\n"),
			stepZeroReads(100),
			stepData("c"),
		}}, func(line string, no int) error {
			got = append(got, collectedLine{line, no})
			return nil
		})
		if err != nil {
			t.Fatalf("100 zero-byte reads must be tolerated, got: %v", err)
		}
		want := []collectedLine{{"a", 1}, {"b", 2}, {"c", 3}}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("lines = %v, want %v", got, want)
		}
	})

	t.Run("101st consecutive zero-byte read is io.ErrNoProgress", func(t *testing.T) {
		var got []collectedLine
		err := readAggregateLines(&scriptedReader{steps: []scriptedStep{
			stepData("a\n"),
			stepZeroReads(101),
			stepPoison(errSentinelChunkedRead), // must never be reached
		}}, func(line string, no int) error {
			got = append(got, collectedLine{line, no})
			return nil
		})
		if !errors.Is(err, io.ErrNoProgress) {
			t.Fatalf("error = %v, want io.ErrNoProgress", err)
		}
		if err != io.ErrNoProgress {
			t.Fatalf("error must be the bare sentinel, got %T: %v", err, err)
		}
		if errors.Is(err, errSentinelChunkedRead) {
			t.Fatalf("reading must stop at the limit; the poison read was reached")
		}
		want := []collectedLine{{"a", 1}}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("lines = %v, want only %v", got, want)
		}
	})

	t.Run("partial record bytes reset the count", func(t *testing.T) {
		// Two runs of 100 zero-byte reads separated only by a few bytes of an
		// as-yet incomplete record: 200 zero-byte reads happen in total, but
		// neither streak reaches 101, so the run completes once the newline
		// arrives. The limit guards consecutive lack of progress, not a
		// lifetime total.
		var got []collectedLine
		err := readAggregateLines(&scriptedReader{steps: []scriptedStep{
			stepData("a\n"),
			stepZeroReads(100),
			stepData("half"),
			stepZeroReads(100),
			stepData("-record\n"),
		}}, func(line string, no int) error {
			got = append(got, collectedLine{line, no})
			return nil
		})
		if err != nil {
			t.Fatalf("bytes received between two 100-stalls must reset the count, got: %v", err)
		}
		want := []collectedLine{{"a", 1}, {"half-record", 2}}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("lines = %v, want %v", got, want)
		}
	})
}

// TestZeroByteReadMidRecordDoesNotAdvanceWindows drives the full
// length-1000/slide-600/two-partition scenario with zero-byte stalls inserted
// constantly, including a 100-read pause in the middle of every watermark and
// of the late event. Results, ordering and late-notice line number must be
// byte-identical to receiving the same stream without stalls: a half-record
// pause can neither double-count an event, emit a late notice early, nor close
// a window ahead of the record's newline.
func TestZeroByteReadMidRecordDoesNotAdvanceWindows(t *testing.T) {
	lines := stallScenarioLines()
	var steps []scriptedStep
	for i, line := range lines {
		// Include each line and its delimiter; the physical blank lines (2 and
		// 6) are a bare "\n".
		chunk := line + "\n"
		var longs []longPauseOffset
		if i == 3 || i == 4 || i == 6 { // both watermarks and the late event
			// Offset 2 lands inside the first 3-byte piece of these records
			// (`{"t`), i.e. before the record is in any sense complete.
			longs = append(longs, longPauseOffset{offset: 2, pause: 100})
		}
		steps = append(steps, fragmentedSteps(chunk, 3, 5, longs...)...)
	}

	out, late, err := runScriptedPartitionedSliding(t, steps)
	if err != nil {
		t.Fatalf("stalls between record fragments must not fail the run: %v", err)
	}
	if out != stallScenarioWindows {
		t.Fatalf("window results across stalls:\n got: %q\nwant: %q", out, stallScenarioWindows)
	}
	if late != stallScenarioLate {
		t.Fatalf("late notice across stalls (blank lines count, zero-byte reads do not):\n got: %q\nwant: %q", late, stallScenarioLate)
	}
}

// TestZeroByteReadOnePartitionWatermarkCannotCloseThenMinCloses focuses on the
// cross-partition merge rule under stalls: with both partitions contributing
// events to the same key, one partition reporting a watermark closes nothing,
// even when the stream then stalls to the progress limit; a newline-less
// watermark for the other partition -- complete-looking JSON sitting in the
// buffer through 101 zero-byte reads -- is not processed either, so nothing
// closes and the caller gets io.ErrNoProgress rather than a parse error.
func TestZeroByteReadOnePartitionWatermarkCannotCloseThenMinCloses(t *testing.T) {
	throughFirstWatermark := strings.Join([]string{
		`{"type":"event","key":"温度","time":700,"value":2,"partition":0}`,
		`{"type":"event","key":"温度","time":1000,"value":3,"partition":1}`,
		`{"type":"watermark","time":1600,"partition":0}`, // only partition 0
		``,
	}, "\n")
	unterminatedOther := `{"type":"watermark","time":1000,"partition":1}` // valid JSON, no newline

	out, late, err := runScriptedPartitionedSliding(t, []scriptedStep{
		stepData(throughFirstWatermark),
		stepZeroReads(90),
		stepData(unterminatedOther[:7]), // record fragments while stalled...
		stepZeroReads(80),               // ...bytes reset the count
		stepData(unterminatedOther[7:]),
		stepZeroReads(101), // now the consecutive lack of progress fails
		stepPoison(errSentinelChunkedRead),
	})
	if !errors.Is(err, io.ErrNoProgress) {
		t.Fatalf("error = %v, want io.ErrNoProgress", err)
	}
	if _, ok := err.(*InputError); ok {
		t.Fatalf("the buffered newline-less tail must not be reported as an input error: %v", err)
	}
	if out != "" {
		t.Fatalf("one partition's watermark and an unterminated watermark must close nothing:\n got: %q", out)
	}
	if late != "" {
		t.Fatalf("no late notices expected, got %q", late)
	}

	// Positive control: the same records but the second partition's watermark
	// gets its newline before the stall -- the min (1000) then closes
	// [0,1000), and the 100 zero-byte reads that follow are tolerated to a
	// clean EOF, retaining the output.
	out2, late2, err2 := runScriptedPartitionedSliding(t, []scriptedStep{
		stepData(throughFirstWatermark),
		stepZeroReads(100),
		stepData(unterminatedOther + "\n"),
		stepZeroReads(100),
	})
	if err2 != nil {
		t.Fatalf("terminated watermark then stall: unexpected error: %v", err2)
	}
	want := `{"key":"温度","start":0,"end":1000,"count":1,"sum":2}` + "\n"
	if out2 != want {
		t.Fatalf("min watermark after both partitions report:\n got: %q\nwant: %q", out2, want)
	}
	if late2 != "" {
		t.Fatalf("unexpected late notices: %q", late2)
	}
}

// TestZeroByteReadFailureKeepsEmittedOutputAndStops covers the failure side:
// once io.ErrNoProgress fires, previously fully written windows and late
// notices stay, the still-open overlap window is not emitted, a newline-less tail
// that happens to be valid JSON never becomes a watermark, data the reader
// only provides afterwards produces nothing, and no further Read happens.
func TestZeroByteReadFailureKeepsEmittedOutputAndStops(t *testing.T) {
	lines := stallScenarioLines()
	throughLate := strings.Join(lines[:7], "\n") + "\n" // lines 1..7, late event closed
	// Buffered tail at failure time: valid JSON watermark WITHOUT a newline.
	// Were it processed it would close [600,1600); it must be ignored.
	tail := lines[7]
	// Data only "provided later": a complete terminating newline that would
	// finish the tail, a further watermark and an event -- none may be read.
	after := "\n" +
		`{"type":"watermark","time":2600,"partition":0}` + "\n" +
		`{"type":"event","key":"温度","time":2100,"value":4,"partition":0}` + "\n"

	wantOut := `{"key":"温度","start":0,"end":1000,"count":1,"sum":2}` + "\n"

	out, late, err := runScriptedPartitionedSliding(t, []scriptedStep{
		stepData(throughLate),
		stepZeroReads(50),
		stepData(tail[:10]), // a few watermark bytes reset the count once...
		stepZeroReads(60),
		stepData(tail[10:]), // ...rest of the valid JSON, still newline-less
		stepZeroReads(101),  // consecutive no-progress limit
		stepData(after),     // must never be handed to the aggregator
		stepPoison(errSentinelChunkedRead),
	})
	if !errors.Is(err, io.ErrNoProgress) {
		t.Fatalf("error = %v, want io.ErrNoProgress", err)
	}
	if err != io.ErrNoProgress {
		t.Fatalf("error must be the bare sentinel, got %T: %v", err, err)
	}
	if errors.Is(err, errSentinelChunkedRead) {
		t.Fatalf("the run must stop reading at the limit; the poison read was reached")
	}
	if _, ok := err.(*InputError); ok {
		t.Fatalf("a progress failure must not surface as an input-line parse error: %v", err)
	}
	if out != wantOut {
		t.Fatalf("emitted window retained; open overlap window and post-failure data must add nothing:\n got: %q\nwant: %q", out, wantOut)
	}
	if late != stallScenarioLate {
		t.Fatalf("late notice written before the failure must be retained:\n got: %q\nwant: %q", late, stallScenarioLate)
	}
}

// TestZeroByteReadCounterNotCumulativeAcrossRun covers two separate runs of
// exactly 100 zero-byte reads with only a few record bytes between them: the
// run later continues to a clean EOF with the full canonical result. The limit
// is consecutive, so 200 zero-byte reads spread across the run must not
// accumulate into a failure, even though a single run of 101 would.
func TestZeroByteReadCounterNotCumulativeAcrossRun(t *testing.T) {
	input := canonicalChunkedScenario()
	firstLineEnd := strings.IndexByte(input, '\n') + 1
	secondLinePrefix := 4 // bytes `{"ty` of the second record

	var steps []scriptedStep
	steps = append(steps,
		stepData(input[:firstLineEnd]),
		stepZeroReads(100),
		stepData(input[firstLineEnd:firstLineEnd+secondLinePrefix]),
		stepZeroReads(100),
	)
	// The remainder arrives in small pieces with short stalls, ending at EOF.
	rest := input[firstLineEnd+secondLinePrefix:]
	steps = append(steps, fragmentedSteps(rest, 5, 13)...)

	out, late, err := runScriptedPartitionedSliding(t, steps)
	if err != nil {
		t.Fatalf("two 100-stalls separated by record bytes must not accumulate: %v", err)
	}
	if out != canonicalChunkedWindows {
		t.Fatalf("window output:\n got: %q\nwant: %q", out, canonicalChunkedWindows)
	}
	if late != canonicalChunkedLate {
		t.Fatalf("late notice:\n got: %q\nwant: %q", late, canonicalChunkedLate)
	}
}

// TestZeroByteReadBlankLinesCountButStallsDoNot focuses on physical line
// numbering in legacy single-watermark mode: a blank line moves the counter,
// arbitrarily many zero-byte reads do not, and the late notice keeps the
// accurate physical number.
func TestZeroByteReadBlankLinesCountButStallsDoNot(t *testing.T) {
	wm := `{"type":"watermark","time":500}`
	ev := `{"type":"event","key":"k","time":100,"value":2}`

	var steps []scriptedStep
	steps = append(steps, stepZeroReads(37)) // stalls before any byte: no line 1
	steps = append(steps, stepData("\n"))    // physical line 1: blank
	steps = append(steps, stepZeroReads(53))
	steps = append(steps, fragmentedSteps(wm+"\n", 4, 11)...) // line 2
	steps = append(steps, stepZeroReads(88))
	steps = append(steps, stepData(ev+"\n"))  // line 3: late
	steps = append(steps, stepZeroReads(100)) // tolerated; next Read is clean EOF

	var out, late bytes.Buffer
	err := RunAggregate(&scriptedReader{steps: steps}, 1000, &out, &late)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.String() != "" {
		t.Fatalf("watermark 500 closes no window, got %q", out.String())
	}
	wantLate := "line 3: late event time=100 below current watermark 500, skipped\n"
	if late.String() != wantLate {
		t.Fatalf("late notice line number (blank lines count, zero-byte reads do not):\n got: %q\nwant: %q", late.String(), wantLate)
	}
}

// TestZeroByteReadFailureAcrossEntryPoints makes sure the legacy fixed-window
// and sliding-only entries surface the same bare sentinel with the same
// retention rules, since every entry funnels through the same reader.
func TestZeroByteReadFailureAcrossEntryPoints(t *testing.T) {
	legacyEvents := strings.Join([]string{
		`{"type":"event","key":"k","time":100,"value":2}`,
		`{"type":"watermark","time":1000}`, // [0,1000) fully emitted
		``,
	}, "\n")
	legacyTail := `{"type":"watermark","time":2000}` // valid JSON, no newline
	partitionedEvents := strings.Join([]string{
		`{"type":"event","key":"k","time":100,"value":2,"partition":0}`,
		`{"type":"watermark","time":1000,"partition":0}`,
		`{"type":"idle","partition":1}`, // lets the effective watermark become 1000
		``,
	}, "\n")
	partitionedTail := `{"type":"watermark","time":2000,"partition":0}`

	entries := []struct {
		name  string
		input string
		tail  string
		run   func(io.Reader, io.Writer, io.Writer) error
	}{
		{"fixed", legacyEvents, legacyTail,
			func(r io.Reader, o, l io.Writer) error { return RunAggregate(r, 1000, o, l) }},
		{"sliding", legacyEvents, legacyTail,
			func(r io.Reader, o, l io.Writer) error { return RunAggregateSliding(r, 1000, 600, o, l) }},
		{"partitioned", partitionedEvents, partitionedTail,
			func(r io.Reader, o, l io.Writer) error {
				return RunAggregatePartitionedSliding(r, 1000, 1000, 2, o, l)
			}},
	}
	for _, fx := range entries {
		fx := fx
		t.Run(fx.name, func(t *testing.T) {
			steps := []scriptedStep{
				stepData(fx.input),
				stepZeroReads(101),
				stepData(fx.tail + "\n"), // would close/open more; must never be read
				stepPoison(errSentinelChunkedRead),
			}
			var out, late bytes.Buffer
			err := fx.run(&scriptedReader{steps: steps}, &out, &late)
			if !errors.Is(err, io.ErrNoProgress) {
				t.Fatalf("error = %v, want io.ErrNoProgress", err)
			}
			if errors.Is(err, errSentinelChunkedRead) {
				t.Fatalf("reading must stop at the progress limit")
			}
			if _, ok := err.(*InputError); ok {
				t.Fatalf("progress failure must not become an input error: %v", err)
			}
			want := `{"key":"k","start":0,"end":1000,"count":1,"sum":2}` + "\n"
			if out.String() != want {
				t.Fatalf("already emitted window stays, nothing else:\n got: %q\nwant: %q", out.String(), want)
			}
			if late.String() != "" {
				t.Fatalf("no late events in this stream, got %q", late.String())
			}
		})
	}
}
