package edgefleet

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
)

// This file guards the behavior when the aggregate's input source briefly has
// no data: an io.Reader is allowed to return (0, nil) while a record is still
// in flight. That is neither end of input nor a dropped half-record. Up to 100
// consecutive zero-byte reads are tolerated; the 101st ends the run with
// io.ErrNoProgress. Any read that delivers even one byte -- whether or not the
// bytes complete a record -- restarts the consecutive count. Window results,
// their order, late-event notices and their physical line numbers must remain
// identical to an uninterrupted delivery of the same bytes.

// pauseStep is one piece of an explicit read schedule: idle contributes that
// many zero-byte, nil-error reads; bytes contributes one contiguous delivery
// (possibly split over several Read calls by a small caller buffer). Either
// field may be empty. Within a step, idles run first; the helpers below keep
// one concern per step so ordering stays obvious at call sites.
type pauseStep struct {
	idles int
	bytes string
}

// pausePhase is a flattened pauseStep: either idle zero-byte reads or one byte
// delivery.
type pausePhase struct {
	idle int
	data string
}

// pauseReader serves a fixed script of byte deliveries and zero-byte reads,
// then returns io.EOF, so tests decide exactly where the pauses fall without
// conflating a pause with the end of input.
type pauseReader struct {
	phases []pausePhase
	pi     int
	reads  int // total Read calls
}

func newPauseReader(steps ...pauseStep) *pauseReader {
	r := &pauseReader{}
	for _, s := range steps {
		if s.idles > 0 {
			r.phases = append(r.phases, pausePhase{idle: s.idles})
		}
		if len(s.bytes) > 0 {
			r.phases = append(r.phases, pausePhase{data: s.bytes})
		}
	}
	return r
}

func (r *pauseReader) Read(p []byte) (int, error) {
	r.reads++
	for r.pi < len(r.phases) {
		ph := &r.phases[r.pi]
		if ph.idle > 0 {
			ph.idle--
			if ph.idle == 0 {
				r.pi++
			}
			return 0, nil
		}
		if len(ph.data) > 0 {
			n := copy(p, ph.data)
			ph.data = ph.data[n:]
			if len(ph.data) == 0 {
				r.pi++
			}
			return n, nil
		}
		r.pi++
	}
	return 0, io.EOF
}

// pauseMark injects idles zero-byte reads after exactly `after` bytes of the
// real input have been delivered.
type pauseMark struct {
	after int
	idles int
}

// newPausingReader serves input byte-for-byte but inserts each mark's zero-byte
// reads at the marked offset. The served byte stream is therefore provably
// identical to input; only read timing changes.
func newPausingReader(input string, marks ...pauseMark) *pauseReader {
	marks = append([]pauseMark(nil), marks...)
	for i := 1; i < len(marks); i++ {
		// Insertion sort; schedules here have a handful of marks.
		for j := i; j > 0 && marks[j-1].after > marks[j].after; j-- {
			marks[j-1], marks[j] = marks[j], marks[j-1]
		}
	}
	r := &pauseReader{}
	prev := 0
	emit := func(seg string, idles int) {
		if len(seg) > 0 {
			r.phases = append(r.phases, pausePhase{data: seg})
		}
		if idles > 0 {
			r.phases = append(r.phases, pausePhase{idle: idles})
		}
	}
	for _, m := range marks {
		emit(input[prev:m.after], m.idles)
		prev = m.after
	}
	emit(input[prev:], 0)
	return r
}

// lockedReader serves prefix, then reports zero-byte, nil-error reads while
// locked. idles < 0 locks forever; idles == N unlocks after exactly N zero
// reads, serves suffix, then io.EOF. It records how far the aggregate read, so
// tests can prove the run stops at the failure boundary and never consumes
// bytes offered only afterwards.
type lockedReader struct {
	prefix   []byte
	pos      int
	idles    int
	suffix   []byte
	suffixAt int
	reads    int
	zeroRead int
}

func newLockedReader(prefix string, idles int, suffix string) *lockedReader {
	return &lockedReader{prefix: []byte(prefix), idles: idles, suffix: []byte(suffix)}
}

func (r *lockedReader) Read(p []byte) (int, error) {
	r.reads++
	if r.pos < len(r.prefix) {
		n := copy(p, r.prefix[r.pos:])
		r.pos += n
		return n, nil
	}
	if r.idles != 0 {
		r.zeroRead++
		if r.idles > 0 {
			r.idles--
		}
		return 0, nil
	}
	if r.suffixAt < len(r.suffix) {
		n := copy(p, r.suffix[r.suffixAt:])
		r.suffixAt += n
		return n, nil
	}
	return 0, io.EOF
}

// runPartitionedSlidingPaused runs the reference window/slide/partition setup
// (1000ms window, 600ms slide, two active partitions) over a scripted reader.
func runPartitionedSlidingPaused(t *testing.T, r io.Reader) (string, string, error) {
	t.Helper()
	var out, late bytes.Buffer
	err := RunAggregatePartitionedSliding(r, 1000, 600, 2, &out, &late)
	return out.String(), late.String(), err
}

// TestZeroByteReadPrimitiveBoundary pins the read loop contract directly:
// exactly 100 consecutive zero-byte reads are tolerated, the 101st returns
// io.ErrNoProgress, and delivering any byte restarts the count -- even bytes
// that do not yet complete a line.
func TestZeroByteReadPrimitiveBoundary(t *testing.T) {
	t.Run("100 empty reads then EOF is a clean run", func(t *testing.T) {
		got, err := collectLines(t, newPauseReader(
			pauseStep{bytes: "a\n"},
			pauseStep{idles: 100},
		))
		if err != nil {
			t.Fatalf("100 zero-byte reads must be tolerated, got %v", err)
		}
		want := []collectedLine{{"a", 1}}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("lines = %v, want %v", got, want)
		}
	})
	t.Run("101st empty read fails with the bare io.ErrNoProgress sentinel", func(t *testing.T) {
		r := newLockedReader("", -1, "")
		_, err := collectLines(t, r)
		if err != io.ErrNoProgress {
			t.Fatalf("error = %T %v, want the bare io.ErrNoProgress sentinel", err, err)
		}
		if r.zeroRead != 101 || r.reads != 101 {
			t.Fatalf("reads=%d zeroReads=%d, want failure on the 101st consecutive empty read", r.reads, r.zeroRead)
		}
	})
	t.Run("100 empty reads after a complete line still tolerate EOF", func(t *testing.T) {
		r := newLockedReader("a\nb\n", 100, "")
		got, err := collectLines(t, r)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := []collectedLine{{"a", 1}, {"b", 2}}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("lines = %v, want %v", got, want)
		}
	})
	t.Run("one partial byte resets the consecutive count", func(t *testing.T) {
		// "a" and "b" do not form a complete line until the very end, yet each
		// resets the count: 100 empties, "a", 100 empties, "b", 100 empties,
		// newline, clean EOF. 300 zero-byte reads across the run all succeed.
		r := newPauseReader(
			pauseStep{bytes: "a"},
			pauseStep{idles: 100},
			pauseStep{bytes: "b"},
			pauseStep{idles: 100},
			pauseStep{bytes: "\n"},
			pauseStep{idles: 100},
		)
		got, err := collectLines(t, r)
		if err != nil {
			t.Fatalf("progress bytes must reset the streak even mid-record, got %v", err)
		}
		want := []collectedLine{{"ab", 1}}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("lines = %v, want %v", got, want)
		}
	})
	t.Run("a later streak fails at its own 101st, never accumulating", func(t *testing.T) {
		// Two tolerated 100-streaks broken by single progress bytes, then a new
		// 101-streak: the limit counts consecutive empties, not the run total.
		r := newPauseReader(
			pauseStep{idles: 100},
			pauseStep{bytes: "x"},
			pauseStep{idles: 100},
			pauseStep{bytes: "y"},
			pauseStep{idles: 101},
		)
		_, err := collectLines(t, r)
		if err != io.ErrNoProgress {
			t.Fatalf("error = %v, want io.ErrNoProgress on the new streak's 101st empty read", err)
		}
	})
	t.Run("zero-byte reads never add physical line numbers", func(t *testing.T) {
		r := newPauseReader(
			pauseStep{bytes: "a\n"},
			pauseStep{idles: 100},
			pauseStep{bytes: "\n"}, // blank physical line 2
			pauseStep{idles: 50},
			pauseStep{bytes: "c\n"},
		)
		got, err := collectLines(t, r)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := []collectedLine{{"a", 1}, {"", 2}, {"c", 3}}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("lines = %v, want %v", got, want)
		}
	})
}

// TestZeroByteReadPausesAroundPartitionedSlidingScenario drives the canonical
// cross-partition sliding-window scenario (window 1000ms, slide 600ms, two
// active partitions, one shared key) with up-to-100-read pauses landing inside
// event and watermark records and between them. The results must be
// byte-identical to uninterrupted delivery: same cross-partition merges, same
// output order, same late notice.
func TestZeroByteReadPausesAroundPartitionedSlidingScenario(t *testing.T) {
	input := canonicalChunkedScenario()
	lines := strings.Split(input, "\n")
	starts := make([]int, len(lines))
	pos := 0
	for i, l := range lines {
		starts[i] = pos
		pos += len(l) + 1 // record bytes plus its '\n' (last entry is the trailing blank)
	}

	// Guard the fragments this test splits at, including a cut through the
	// three-byte runes of 温度 and through watermark/field bytes.
	eventFrag1 := `{"type":"event","key":"温`
	wmFrag1 := `{"type":"watermark","time":16`
	lateFrag1 := `{"type":"event","key":"温度","time":99`
	if lines[0][:len(eventFrag1)] != eventFrag1 {
		t.Fatalf("test drift: line 1 no longer starts with %q", eventFrag1)
	}
	if lines[2][:len(wmFrag1)] != wmFrag1 {
		t.Fatalf("test drift: line 3 no longer starts with %q", wmFrag1)
	}
	if lines[4][:len(lateFrag1)] != lateFrag1 {
		t.Fatalf("test drift: line 5 no longer starts with %q", lateFrag1)
	}

	r := newPausingReader(input,
		// Inside the first event's multibyte key.
		pauseMark{after: starts[0] + len(eventFrag1), idles: 100},
		// Right at the start of the second partition's event.
		pauseMark{after: starts[1], idles: 77},
		// Inside partition 0's watermark (effective watermark still unknown).
		pauseMark{after: starts[2] + len(wmFrag1), idles: 100},
		// At the boundary that lets the minimum watermark close [0,1000).
		pauseMark{after: starts[4], idles: 33},
		// Inside the late-event record.
		pauseMark{after: starts[4] + len(lateFrag1), idles: 100},
	)
	out, late, err := runPartitionedSlidingPaused(t, r)
	if err != nil {
		t.Fatalf("pauses inside records must not change the run, got %v", err)
	}
	if out != canonicalChunkedWindows {
		t.Fatalf("window output differs from uninterrupted delivery:\n got: %q\nwant: %q", out, canonicalChunkedWindows)
	}
	if late != canonicalChunkedLate {
		t.Fatalf("late notice differs:\n got: %q\nwant: %q", late, canonicalChunkedLate)
	}
}

// TestZeroByteReadSinglePartitionWatermarkCannotCloseAcrossPause: while only
// one partition has ever reported a watermark, no window may close -- not
// after 100 empty reads, and not when the other partition's watermark bytes
// are already buffered without their terminating newline, even if those bytes
// are complete valid JSON.
func TestZeroByteReadSinglePartitionWatermarkCannotCloseAcrossPause(t *testing.T) {
	records := []string{
		`{"type":"event","key":"温度","time":700,"value":2,"partition":0}`,
		`{"type":"event","key":"温度","time":1000,"value":3,"partition":1}`,
		`{"type":"watermark","time":1600,"partition":0}`, // effective watermark still unknown
	}
	prefix := strings.Join(records, "\n") + "\n"
	second := `{"type":"watermark","time":1000,"partition":1}`

	cases := map[string]string{
		"stall before the other partition reports":    "",
		"stall mid watermark record":                  second[:len(second)/2],
		"complete-looking watermark JSON, no newline": second,
	}
	for name, tail := range cases {
		tail := tail
		t.Run(name, func(t *testing.T) {
			// suffix is what a later read could deliver; under a permanent lock
			// it must never be reached.
			r := newLockedReader(prefix+tail, -1, second[len(tail):]+"\n")
			out, late, err := runPartitionedSlidingPaused(t, r)
			if !errors.Is(err, io.ErrNoProgress) {
				t.Fatalf("error = %T %v, want io.ErrNoProgress recognizable via errors.Is", err, err)
			}
			if _, ok := err.(*InputError); ok {
				t.Fatalf("a stalled reader must not surface a parse error for buffered bytes: %v", err)
			}
			if out != "" {
				t.Fatalf("one partition's watermark cannot close a window across the pause: %q", out)
			}
			if late != "" {
				t.Fatalf("no late notice may fire without an effective watermark: %q", late)
			}
			if r.suffixAt != 0 || r.zeroRead != 101 {
				t.Fatalf("run must stop at the 101st empty read without touching later bytes: zeroReads=%d suffixConsumed=%d", r.zeroRead, r.suffixAt)
			}
		})
	}
}

// TestZeroByteReadFailureKeepsClosedOutputsAndDropsTail: when the no-progress
// boundary fires, fully emitted windows and late notices stay, still-open
// overlapping windows are not backfilled, and an unterminated tail -- even one
// that is complete, valid JSON -- is neither parsed as a record nor reported
// as an input error. Bytes offered only after the boundary produce nothing.
func TestZeroByteReadFailureKeepsClosedOutputsAndDropsTail(t *testing.T) {
	records := []string{
		`{"type":"event","key":"温度","time":700,"value":2,"partition":0}`,
		`{"type":"event","key":"温度","time":1000,"value":3,"partition":1}`,
		`{"type":"watermark","time":1600,"partition":0}`,
		`{"type":"watermark","time":1000,"partition":1}`,                 // line 4: min=1000 closes [0,1000)
		`{"type":"event","key":"温度","time":999,"value":9,"partition":1}`, // line 5: late
	}
	prefix := strings.Join(records, "\n") + "\n"
	// Line 6 sits buffered as complete JSON, but its newline never arrives
	// before the lock. It must not close the open [600,1600).
	tail := `{"type":"watermark","time":1600,"partition":1}`
	// Bytes that only exist "after" the failure: the line-6 newline plus more
	// records. None may be read or produce results.
	suffix := "\n" +
		`{"type":"event","key":"温度","time":1500,"value":4,"partition":0}` + "\n" +
		`{"type":"watermark","time":3000,"partition":0}` + "\n" +
		`{"type":"watermark","time":3000,"partition":1}` + "\n"

	r := newLockedReader(prefix+tail, -1, suffix)
	out, late, err := runPartitionedSlidingPaused(t, r)
	if err != io.ErrNoProgress {
		t.Fatalf("error = %T %v, want the bare io.ErrNoProgress, not a wrapped or per-line error", err, err)
	}
	if _, ok := err.(*InputError); ok {
		t.Fatalf("unterminated tail must not become a JSON parse error: %v", err)
	}
	wantOut := `{"key":"温度","start":0,"end":1000,"count":1,"sum":2}` + "\n"
	if out != wantOut {
		t.Fatalf("only the previously closed window survives; open [600,1600) must not be backfilled:\n got: %q\nwant: %q", out, wantOut)
	}
	if late != canonicalChunkedLate {
		t.Fatalf("the late notice emitted before the stall must be retained:\n got: %q\nwant: %q", late, canonicalChunkedLate)
	}
	if r.suffixAt != 0 {
		t.Fatalf("bytes offered only after the failure must never be read, consumed %d", r.suffixAt)
	}
	if r.zeroRead != 101 {
		t.Fatalf("zeroReads = %d, want stop exactly at 101", r.zeroRead)
	}
}

// TestZeroByteReadTwoStreaksDoNotAccumulate: two separate streaks of 100 empty
// reads, separated by only a few record bytes (not even a complete record the
// first time), must not add up. The stream then finishes normally with the
// canonical windows and late notice.
func TestZeroByteReadTwoStreaksDoNotAccumulate(t *testing.T) {
	input := canonicalChunkedScenario()
	lines := strings.Split(input, "\n")
	line1End := len(lines[0]) + 1

	r := newPausingReader(input,
		pauseMark{after: line1End, idles: 100},
		pauseMark{after: line1End + 3, idles: 100},
	)
	out, late, err := runPartitionedSlidingPaused(t, r)
	if err != nil {
		t.Fatalf("two non-consecutive 100-streaks must not accumulate, got %v", err)
	}
	if out != canonicalChunkedWindows {
		t.Fatalf("window output:\n got: %q\nwant: %q", out, canonicalChunkedWindows)
	}
	if late != canonicalChunkedLate {
		t.Fatalf("late notice:\n got: %q\nwant: %q", late, canonicalChunkedLate)
	}

	// Negative control: the second streak hitting 101 fails there regardless
	// of the first streak having been a full 100.
	bad := newPausingReader(input,
		pauseMark{after: line1End, idles: 100},
		pauseMark{after: line1End + 3, idles: 101},
	)
	if _, _, err := runPartitionedSlidingPaused(t, bad); err != io.ErrNoProgress {
		t.Fatalf("error = %v, want io.ErrNoProgress once the second streak hits 101", err)
	}
}

// TestZeroByteReadBlankLinesKeepLateNoticeLineNumber: blank physical lines
// still count toward line numbering while zero-byte reads do not. With blank
// lines and long pauses interleaved, the late event must be reported on its
// true physical line (8) and the window results must be unchanged.
func TestZeroByteReadBlankLinesKeepLateNoticeLineNumber(t *testing.T) {
	physical := []string{
		``, // line 1 blank
		`{"type":"event","key":"温度","time":700,"value":2,"partition":0}`,
		``, // line 3 blank
		`{"type":"event","key":"温度","time":1000,"value":3,"partition":1}`,
		`{"type":"watermark","time":1600,"partition":0}`,
		`{"type":"watermark","time":1000,"partition":1}`,
		``, // line 7 blank
		`{"type":"event","key":"温度","time":999,"value":9,"partition":1}`, // line 8 late
		`{"type":"watermark","time":1600,"partition":1}`,
		``, // trailing blank: line 10
	}
	input := strings.Join(physical, "\n")
	starts := make([]int, len(physical))
	pos := 0
	for i, l := range physical {
		starts[i] = pos
		pos += len(l) + 1
	}

	// Reference: uninterrupted delivery.
	refOut, refLate, err := runPartitionedSlidingPaused(t, strings.NewReader(input))
	if err != nil {
		t.Fatalf("reference run: %v", err)
	}
	wantLate := "line 8: late event time=999 below current watermark 1000, skipped\n"
	if refLate != wantLate {
		t.Fatalf("reference late notice:\n got: %q\nwant: %q", refLate, wantLate)
	}
	if refOut != canonicalChunkedWindows {
		t.Fatalf("reference window output:\n got: %q\nwant: %q", refOut, canonicalChunkedWindows)
	}

	r := newPausingReader(input,
		pauseMark{after: 1, idles: 100},                           // after blank line 1's '\n'
		pauseMark{after: starts[3] + 11, idles: 100},              // inside line 4's event
		pauseMark{after: starts[6], idles: 50},                    // right at blank line 7
		pauseMark{after: starts[6] + 1, idles: 100},               // after blank line 7's '\n'
		pauseMark{after: starts[7] + 12, idles: 100},              // inside line 8 (the late event)
		pauseMark{after: starts[7] + len(physical[7]), idles: 40}, // line 8 buffered, its newline pending
	)
	out, late, err := runPartitionedSlidingPaused(t, r)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if late != wantLate {
		t.Fatalf("late notice must keep physical line 8:\n got: %q\nwant: %q", late, wantLate)
	}
	if out != canonicalChunkedWindows {
		t.Fatalf("window output:\n got: %q\nwant: %q", out, canonicalChunkedWindows)
	}
}

// TestZeroByteReadAcrossEntryPoints repeats the pause invariant for the other
// public entry shapes, so zero-byte reads can never become record boundaries
// in legacy fixed-window or single-watermark sliding modes either.
func TestZeroByteReadAcrossEntryPoints(t *testing.T) {
	t.Run("legacy fixed window", func(t *testing.T) {
		input := strings.Join([]string{
			`{"type":"event","key":"k","time":100,"value":2}`,
			`{"type":"event","key":"k","time":200,"value":3}`,
			`{"type":"watermark","time":1000}`,
			``,
		}, "\n")
		lines := strings.Split(input, "\n")
		r := newPausingReader(input,
			pauseMark{after: 7, idles: 100},                     // inside the first event
			pauseMark{after: len(lines[0]) + 1 + 9, idles: 100}, // inside the second event
			pauseMark{after: len(input) - 5, idles: 100},        // inside the final watermark
		)
		var out, late bytes.Buffer
		if err := RunAggregate(r, 1000, &out, &late); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := `{"key":"k","start":0,"end":1000,"count":2,"sum":5}` + "\n"
		if out.String() != want {
			t.Fatalf("window output:\n got: %q\nwant: %q", out.String(), want)
		}
		if late.String() != "" {
			t.Fatalf("unexpected late notices: %q", late.String())
		}
	})
	t.Run("legacy sliding window stalls before failing at the boundary", func(t *testing.T) {
		input := `{"type":"event","key":"k","time":700,"value":2}` + "\n"
		r := newLockedReader(input, -1, `{"type":"watermark","time":1000}`+"\n")
		var out, late bytes.Buffer
		err := RunAggregateSliding(r, 1000, 600, &out, &late)
		if err != io.ErrNoProgress {
			t.Fatalf("error = %T %v, want the bare io.ErrNoProgress", err, err)
		}
		if out.String() != "" || late.String() != "" {
			t.Fatalf("nothing may close during a permanent stall: out=%q late=%q", out.String(), late.String())
		}
		if r.suffixAt != 0 {
			t.Fatalf("later watermark bytes must never be read, consumed %d", r.suffixAt)
		}
	})
}
