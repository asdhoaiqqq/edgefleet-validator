package edgefleet

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
)

// This file guards the read boundary: real callers may deliver one record
// split over several Read calls, or several records in one Read call. As long
// as the byte stream and record order are identical, window results, late
// notices and the final success/failure must be identical to a one-shot read;
// read chunk sizes are not record boundaries.

// chunkReader serves the same byte stream as strings.Reader but fixes the size
// of each Read: cuts are explicit byte offsets.
type chunkReader struct {
	data []byte
	cuts []int // cumulative offsets at which a Read ends; the tail is a final read
	pos  int
}

func newChunkReader(data string, cuts ...int) *chunkReader {
	return &chunkReader{data: []byte(data), cuts: cuts}
}

func (r *chunkReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	end := len(r.data)
	if len(r.cuts) > 0 && r.cuts[0] < end {
		end = r.cuts[0]
	}
	n := copy(p, r.data[r.pos:end])
	r.pos += n
	// The cut is reached only once its full prefix has been delivered, so a
	// small caller buffer simply continues toward the same cut.
	if len(r.cuts) > 0 && r.pos >= r.cuts[0] {
		r.cuts = r.cuts[1:]
	}
	if r.pos >= len(r.data) {
		return n, io.EOF
	}
	return n, nil
}

// runChunked executes the partitioned-sliding aggregation with input delivered
// in the fixed-size reads described by cuts, returning captured outputs.
func runChunked(t *testing.T, input string, cuts []int) (string, string, error) {
	t.Helper()
	var out, late bytes.Buffer
	err := RunAggregatePartitionedSliding(newChunkReader(input, cuts...), 1000, 600, 2, &out, &late)
	return out.String(), late.String(), err
}

// canonicalChunkedScenario is the reference stream for partitioned sliding
// windows (length 1000ms, slide 600ms, two partitions), one JSON object per
// physical line:
//
//	line 1: partition 0, key 温度, time 700, value 2
//	line 2: partition 1, key 温度, time 1000, value 3
//	line 3: partition 0 reports watermark 1600 (effective watermark still unknown)
//	line 4: partition 1 reports watermark 1000 (min becomes 1000; [0,1000) closes)
//	line 5: partition 1, key 温度, time 999, value 9 (late vs effective 1000)
//	line 6: partition 1 reports watermark 1600 (min becomes 1600; [600,1600) closes)
//
// The late event falls in the still-open overlap window [600,1600) but must
// not alter it: the window's value 9 never enters any count or sum.
func canonicalChunkedScenario() string {
	return strings.Join([]string{
		`{"type":"event","key":"温度","time":700,"value":2,"partition":0}`,
		`{"type":"event","key":"温度","time":1000,"value":3,"partition":1}`,
		`{"type":"watermark","time":1600,"partition":0}`,
		`{"type":"watermark","time":1000,"partition":1}`,
		`{"type":"event","key":"温度","time":999,"value":9,"partition":1}`,
		`{"type":"watermark","time":1600,"partition":1}`,
		``,
	}, "\n")
}

var canonicalChunkedWindows = strings.Join([]string{
	`{"key":"温度","start":0,"end":1000,"count":1,"sum":2}`,
	`{"key":"温度","start":600,"end":1600,"count":2,"sum":5}`,
	``,
}, "\n")

const canonicalChunkedLate = "line 5: late event time=999 below current watermark 1000, skipped\n"

// allCuts returns every meaningful single-cut partition of a stream (the cut
// is the byte length of the first Read), plus the no-cut one-shot read.
func allCuts(n int) [][]int {
	cuts := make([][]int, 0, n)
	cuts = append(cuts, nil) // one Read carrying every record at once
	for at := 1; at < n; at++ {
		cuts = append(cuts, []int{at})
	}
	return cuts
}

// uniformCuts splits a stream into equal-size reads of n bytes (the last read
// may be shorter).
func uniformCuts(total, n int) []int {
	var cuts []int
	for at := n; at < total; at += n {
		cuts = append(cuts, at)
	}
	return cuts
}

// TestChunkedArrivalBaseline pins the one-shot reference result that every
// fragmented/coalesced delivery below must reproduce.
func TestChunkedArrivalBaseline(t *testing.T) {
	var out, late bytes.Buffer
	if err := RunAggregatePartitionedSliding(strings.NewReader(canonicalChunkedScenario()), 1000, 600, 2, &out, &late); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.String() != canonicalChunkedWindows {
		t.Fatalf("window output mismatch:\n got: %q\nwant: %q", out.String(), canonicalChunkedWindows)
	}
	if late.String() != canonicalChunkedLate {
		t.Fatalf("late notice mismatch:\n got: %q\nwant: %q", late.String(), canonicalChunkedLate)
	}
}

// TestChunkedArrivalSingleByteCuts delivers one byte per Read, so the
// three-byte UTF-8 encoding of 温度, every JSON field and every newline are
// repeatedly split across reads. Results must be byte-identical.
func TestChunkedArrivalSingleByteCuts(t *testing.T) {
	input := canonicalChunkedScenario()
	cuts := make([]int, 0, len(input)-1)
	for at := 1; at < len(input); at++ {
		cuts = append(cuts, at)
	}
	out, late, err := runChunked(t, input, cuts)
	if err != nil {
		t.Fatalf("single-byte reads must not change the result, got error: %v", err)
	}
	if out != canonicalChunkedWindows {
		t.Fatalf("single-byte reads changed window output:\n got: %q\nwant: %q", out, canonicalChunkedWindows)
	}
	if late != canonicalChunkedLate {
		t.Fatalf("single-byte reads changed late notice:\n got: %q\nwant: %q", late, canonicalChunkedLate)
	}
}

// TestChunkedArrivalEveryTwoReadCuts exhaustively splits the stream at one
// byte offset using a two-Read delivery (first record may be half a record,
// second read may carry several records), covering every multibyte boundary.
func TestChunkedArrivalEveryTwoReadCuts(t *testing.T) {
	input := canonicalChunkedScenario()
	for _, cuts := range allCuts(len(input)) {
		cuts := cuts
		name := "oneshot"
		if cuts != nil {
			name = fmt.Sprintf("cut@%d", cuts[0])
		}
		t.Run(name, func(t *testing.T) {
			out, late, err := runChunked(t, input, cuts)
			if err != nil {
				t.Fatalf("cut %v: unexpected error: %v", cuts, err)
			}
			if out != canonicalChunkedWindows {
				t.Fatalf("cut %v: window output:\n got: %q\nwant: %q", cuts, out, canonicalChunkedWindows)
			}
			if late != canonicalChunkedLate {
				t.Fatalf("cut %v: late notice:\n got: %q\nwant: %q", cuts, late, canonicalChunkedLate)
			}
		})
	}
}

// TestChunkedArrivalUniformSizes repeats the whole stream under several fixed
// read sizes (one read may bring several records; a record may span many
// reads), deliberately including sizes that do not align with record or
// multibyte boundaries.
func TestChunkedArrivalUniformSizes(t *testing.T) {
	input := canonicalChunkedScenario()
	for _, size := range []int{1, 2, 3, 5, 7, 13, 31, 64, 100, 256} {
		size := size
		t.Run(fmt.Sprintf("size%d", size), func(t *testing.T) {
			out, late, err := runChunked(t, input, uniformCuts(len(input), size))
			if err != nil {
				t.Fatalf("size %d: unexpected error: %v", size, err)
			}
			if out != canonicalChunkedWindows {
				t.Fatalf("size %d: window output:\n got: %q\nwant: %q", size, out, canonicalChunkedWindows)
			}
			if late != canonicalChunkedLate {
				t.Fatalf("size %d: late notice:\n got: %q\nwant: %q", size, late, canonicalChunkedLate)
			}
		})
	}
}

// TestChunkedArrivalCutsOnDelimitersAndFields places cuts exactly around the
// structural points most tempting to mistake for record boundaries: start and
// end of the multibyte key, JSON punctuation, field contents and newlines.
func TestChunkedArrivalCutsOnDelimitersAndFields(t *testing.T) {
	input := canonicalChunkedScenario()
	cases := map[string][]int{
		"after each record newline": {
			len(`{"type":"event","key":"温度","time":700,"value":2,"partition":0}`) + 1,
			len(`{"type":"event","key":"温度","time":700,"value":2,"partition":0}`) + 1 +
				len(`{"type":"event","key":"温度","time":1000,"value":3,"partition":1}`) + 1,
		},
		"inside every record": func() []int {
			var cuts []int
			pos := 0
			for _, line := range strings.Split(strings.TrimSuffix(input, "\n"), "\n") {
				mid := pos + len(line)/2 // splits the key bytes for the event records
				cuts = append(cuts, mid)
				pos += len(line) + 1
				cuts = append(cuts, pos-1) // one byte before each newline: '\n' arrives alone
			}
			return cuts
		}(),
		"newline delivered one byte late": func() []int {
			var cuts []int
			pos := 0
			for _, line := range strings.Split(strings.TrimSuffix(input, "\n"), "\n") {
				pos += len(line)
				cuts = append(cuts, pos) // read ends right before '\n'
				pos++
				cuts = append(cuts, pos) // read ends right after '\n'
			}
			return cuts
		}(),
	}
	for name, cuts := range cases {
		cuts := cuts
		t.Run(name, func(t *testing.T) {
			out, late, err := runChunked(t, input, cuts)
			if err != nil {
				t.Fatalf("%s: unexpected error: %v", name, err)
			}
			if out != canonicalChunkedWindows {
				t.Fatalf("%s: window output:\n got: %q\nwant: %q", name, out, canonicalChunkedWindows)
			}
			if late != canonicalChunkedLate {
				t.Fatalf("%s: late notice:\n got: %q\nwant: %q", name, late, canonicalChunkedLate)
			}
		})
	}
}

// TestChunkedArrivalBlankLinesCountPhysicalLines interleaves blank lines:
// they create no records but still consume physical line numbers, so the late
// notice must name line 8 regardless of how the bytes are chunked.
func TestChunkedArrivalBlankLinesCountPhysicalLines(t *testing.T) {
	lines := []string{
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
	input := strings.Join(lines, "\n")
	wantLate := "line 8: late event time=999 below current watermark 1000, skipped\n"

	schedule := func(t *testing.T, r io.Reader) (string, string, error) {
		var out, late bytes.Buffer
		err := RunAggregatePartitionedSliding(r, 1000, 600, 2, &out, &late)
		return out.String(), late.String(), err
	}

	oneOut, oneLate, err := schedule(t, strings.NewReader(input))
	if err != nil {
		t.Fatalf("one-shot: unexpected error: %v", err)
	}
	if oneLate != wantLate {
		t.Fatalf("one-shot late notice:\n got: %q\nwant: %q", oneLate, wantLate)
	}
	if oneOut != canonicalChunkedWindows {
		t.Fatalf("one-shot window output:\n got: %q\nwant: %q", oneOut, canonicalChunkedWindows)
	}

	// Single-byte delivery puts every blank-line '\n' in its own read.
	cuts := make([]int, 0, len(input)-1)
	for at := 1; at < len(input); at++ {
		cuts = append(cuts, at)
	}
	chunkOut, chunkLate, err := schedule(t, newChunkReader(input, cuts...))
	if err != nil {
		t.Fatalf("chunked: unexpected error: %v", err)
	}
	if chunkLate != wantLate {
		t.Fatalf("chunked late notice:\n got: %q\nwant: %q", chunkLate, wantLate)
	}
	if chunkOut != oneOut {
		t.Fatalf("chunked window output:\n got: %q\nwant: %q", chunkOut, oneOut)
	}
}

// TestChunkedArrivalCRLFDelimiters uses carriage-return+newline separators and
// no trailing newline on the final (window-closing watermark) record. Neither
// delimiter style nor the missing final newline may change aggregation, drop
// the final watermark, or corrupt the key/fields when split mid-sequence.
func TestChunkedArrivalCRLFDelimiters(t *testing.T) {
	records := []string{
		`{"type":"event","key":"温度","time":700,"value":2,"partition":0}`,
		`{"type":"event","key":"温度","time":1000,"value":3,"partition":1}`,
		`{"type":"watermark","time":1600,"partition":0}`,
		`{"type":"watermark","time":1000,"partition":1}`,
		`{"type":"event","key":"温度","time":999,"value":9,"partition":1}`,
		`{"type":"watermark","time":1600,"partition":1}`, // last complete record, no newline
	}
	input := strings.Join(records[:len(records)-1], "\r\n") + "\r\n" + records[len(records)-1]

	var oneOut, oneLate bytes.Buffer
	if err := RunAggregatePartitionedSliding(strings.NewReader(input), 1000, 600, 2, &oneOut, &oneLate); err != nil {
		t.Fatalf("one-shot: unexpected error: %v", err)
	}
	if oneOut.String() != canonicalChunkedWindows {
		t.Fatalf("one-shot window output:\n got: %q\nwant: %q", oneOut.String(), canonicalChunkedWindows)
	}
	if oneLate.String() != canonicalChunkedLate {
		t.Fatalf("one-shot late notice:\n got: %q\nwant: %q", oneLate.String(), canonicalChunkedLate)
	}

	// Single-byte reads split '\r' from '\n' and leave the final record unterminated.
	cuts := make([]int, 0, len(input)-1)
	for at := 1; at < len(input); at++ {
		cuts = append(cuts, at)
	}
	var chOut, chLate bytes.Buffer
	if err := RunAggregatePartitionedSliding(newChunkReader(input, cuts...), 1000, 600, 2, &chOut, &chLate); err != nil {
		t.Fatalf("chunked CRLF: unexpected error: %v", err)
	}
	if chOut.String() != canonicalChunkedWindows {
		t.Fatalf("chunked CRLF window output (final watermark must not be lost):\n got: %q\nwant: %q", chOut.String(), canonicalChunkedWindows)
	}
	if chLate.String() != canonicalChunkedLate {
		t.Fatalf("chunked CRLF late notice:\n got: %q\nwant: %q", chLate.String(), canonicalChunkedLate)
	}
}

// failingAfterReader serves its prefix in reads of at most chunk bytes until
// exactly failAt bytes have been delivered. The read that reaches failAt
// returns the final bytes together with err when errOnBoundary is true;
// otherwise it returns them cleanly and the following read (which delivers no
// record) fails. Either shape matches how real readers (net.Conn, pipes)
// report an upstream failure; the bytes past failAt are never handed out.
type failingAfterReader struct {
	data          []byte
	pos           int
	failAt        int
	err           error
	chunk         int
	errOnBoundary bool
}

var errSentinelChunkedRead = errors.New("synthetic upstream read failure")

func (r *failingAfterReader) Read(p []byte) (int, error) {
	if r.pos >= r.failAt {
		return 0, r.err
	}
	limit := len(p)
	if r.chunk > 0 && r.chunk < limit {
		limit = r.chunk
	}
	end := r.pos + limit
	if end > r.failAt {
		end = r.failAt
	}
	n := copy(p, r.data[r.pos:end])
	r.pos += n
	if r.pos >= r.failAt && r.errOnBoundary {
		return n, r.err
	}
	return n, nil
}

// TestChunkedArrivalIncompleteFinalLineIsInputError: a final line with valid
// prefix but no closing brace/newline is an input error on that physical line,
// whether it arrives whole or unterminated or one byte at a time. Previously
// written windows stay; still-open windows are not emitted.
func TestChunkedArrivalIncompleteFinalLineIsInputError(t *testing.T) {
	complete := strings.Join([]string{
		`{"type":"event","key":"温度","time":700,"value":2,"partition":0}`,
		`{"type":"event","key":"温度","time":1000,"value":3,"partition":1}`,
		`{"type":"watermark","time":1600,"partition":0}`,
		`{"type":"watermark","time":1000,"partition":1}`, // line 4 closes [0,1000)
		``,
	}, "\n")
	// Line 5 is unterminated JSON (deliberately missing the closing brace).
	input := complete + `{"type":"watermark","time":1600,"partition":1`
	wantFirstWindow := `{"key":"温度","start":0,"end":1000,"count":1,"sum":2}` + "\n"

	check := func(t *testing.T, r io.Reader) {
		var out, late bytes.Buffer
		err := RunAggregatePartitionedSliding(r, 1000, 600, 2, &out, &late)
		var ie *InputError
		if !errors.As(err, &ie) {
			t.Fatalf("expected *InputError, got %T: %v", err, err)
		}
		if ie.Line != 5 {
			t.Errorf("InputError.Line = %d, want physical line 5", ie.Line)
		}
		if !strings.Contains(ie.Reason, "invalid JSON") {
			t.Errorf("reason = %q, want invalid JSON", ie.Reason)
		}
		if out.String() != wantFirstWindow {
			t.Errorf("previously emitted windows must be retained and open ones not emitted:\n got: %q\nwant: %q", out.String(), wantFirstWindow)
		}
		if late.String() != "" {
			t.Errorf("no late events in this stream, got %q", late.String())
		}
	}

	t.Run("one-shot unterminated", func(t *testing.T) {
		check(t, strings.NewReader(input))
	})
	t.Run("one byte per read", func(t *testing.T) {
		cuts := make([]int, 0, len(input)-1)
		for at := 1; at < len(input); at++ {
			cuts = append(cuts, at)
		}
		check(t, newChunkReader(input, cuts...))
	})
	t.Run("incomplete bytes separate from newline", func(t *testing.T) {
		// Deliver the four complete lines, then the partial line a few bytes at a time.
		check(t, newChunkReader(input, len(complete), len(complete)+1, len(complete)+5, len(complete)+12))
	})
}

// TestChunkedArrivalReadErrorAfterCompleteRecordsPropagates: once several
// complete, valid records have been delivered, a Read failure must surface as
// the reader's own error (not a silent clean EOF). Completed window output
// stays; open windows are not emitted at the failure.
func TestChunkedArrivalReadErrorAfterCompleteRecordsPropagates(t *testing.T) {
	records := []string{
		`{"type":"event","key":"温度","time":700,"value":2,"partition":0}`,
		`{"type":"event","key":"温度","time":1000,"value":3,"partition":1}`,
		`{"type":"watermark","time":1600,"partition":0}`,
		`{"type":"watermark","time":1000,"partition":1}`,                 // line 4 closes [0,1000)
		`{"type":"event","key":"温度","time":999,"value":9,"partition":1}`, // line 5 late, then read fails
		`{"type":"watermark","time":1600,"partition":1}`,                 // must never be reached
	}
	full := strings.Join(records, "\n")
	goodBoundary := len(strings.Join(records[:5], "\n")) + 1 // through line 5 and its '\n'

	check := func(t *testing.T, r io.Reader) {
		var out, late bytes.Buffer
		err := RunAggregatePartitionedSliding(r, 1000, 600, 2, &out, &late)
		if !errors.Is(err, errSentinelChunkedRead) {
			t.Fatalf("error must be the reader's original error, got %T: %v", err, err)
		}
		if _, ok := err.(*InputError); ok {
			t.Fatalf("a read failure must not be reported as an *InputError: %v", err)
		}
		wantOut := `{"key":"温度","start":0,"end":1000,"count":1,"sum":2}` + "\n"
		if out.String() != wantOut {
			t.Errorf("completed output retained, open [600,1600) not emitted:\n got: %q\nwant: %q", out.String(), wantOut)
		}
		if late.String() != canonicalChunkedLate {
			t.Errorf("late notice before the failure must be written:\n got: %q\nwant: %q", late.String(), canonicalChunkedLate)
		}
	}

	t.Run("error alone on the next read", func(t *testing.T) {
		// The read completing line 5 succeeds; the very next read fails having
		// delivered nothing.
		check(t, &failingAfterReader{data: []byte(full), failAt: goodBoundary, err: errSentinelChunkedRead})
	})
	t.Run("last good bytes and error together", func(t *testing.T) {
		// One read returns line 5's trailing newline together with the error,
		// as a net.Conn may. The complete record still counts; the error wins.
		check(t, &failingAfterReader{data: []byte(full), failAt: goodBoundary, err: errSentinelChunkedRead, errOnBoundary: true})
	})
	t.Run("one byte per read before failure", func(t *testing.T) {
		// Fragmented delivery up to the boundary, last byte plus error; the
		// result must be identical to a large-buffer read.
		check(t, &failingAfterReader{data: []byte(full), failAt: goodBoundary, err: errSentinelChunkedRead, chunk: 1, errOnBoundary: true})
	})
}

// TestChunkedArrivalReadErrorDiscardsUnterminatedTail: a read failure in the
// middle of a record is not end of input. The bytes received for that record
// never got their newline, so they must not be parsed as a complete line:
// no event, watermark or idle declaration comes out of them, no window closes
// and no late notice is written, however well-formed the fragment happens to
// be. Complete records already received are still processed first, and the
// run then fails with the reader's original error.
func TestChunkedArrivalReadErrorDiscardsUnterminatedTail(t *testing.T) {
	eventLine := `{"type":"event","key":"k","time":100,"value":2}` + "\n"
	watermarkLine := `{"type":"watermark","time":1000}` // closes [0,1000) once complete
	wantWindow := `{"key":"k","start":0,"end":1000,"count":1,"sum":2}` + "\n"

	t.Run("parseable tail without newline produces no window", func(t *testing.T) {
		// The full watermark JSON arrived but its newline did not, then the
		// reader failed: the watermark never completed, so [0,1000) stays open.
		full := eventLine + watermarkLine
		for _, shape := range []struct {
			name string
			r    *failingAfterReader
		}{
			{"error on the boundary read", &failingAfterReader{data: []byte(full), failAt: len(full), err: errSentinelChunkedRead, errOnBoundary: true}},
			{"error on the next read", &failingAfterReader{data: []byte(full), failAt: len(full), err: errSentinelChunkedRead}},
			{"one byte per read", &failingAfterReader{data: []byte(full), failAt: len(full), err: errSentinelChunkedRead, chunk: 1, errOnBoundary: true}},
		} {
			t.Run(shape.name, func(t *testing.T) {
				var out, late bytes.Buffer
				err := RunAggregate(shape.r, 1000, &out, &late)
				if !errors.Is(err, errSentinelChunkedRead) {
					t.Fatalf("error must be the reader's original error, got %T: %v", err, err)
				}
				if ie, ok := err.(*InputError); ok {
					t.Fatalf("the unterminated tail must not be parsed as a record: %v", ie)
				}
				if out.String() != "" {
					t.Fatalf("the incomplete watermark must not close [0,1000):\n got: %q\nwant: %q", out.String(), "")
				}
				if late.String() != "" {
					t.Fatalf("no late notice may come from the incomplete record, got %q", late.String())
				}
			})
		}
	})

	t.Run("newline completed before the error closes the window", func(t *testing.T) {
		// The watermark's newline arrived in the same read that reported the
		// failure: the record is complete, so [0,1000) closes with count 1,
		// sum 2 before the read failure is returned.
		full := eventLine + watermarkLine + "\n"
		var out, late bytes.Buffer
		err := RunAggregate(&failingAfterReader{data: []byte(full), failAt: len(full), err: errSentinelChunkedRead, errOnBoundary: true}, 1000, &out, &late)
		if !errors.Is(err, errSentinelChunkedRead) {
			t.Fatalf("error must be the reader's original error, got %T: %v", err, err)
		}
		if out.String() != wantWindow {
			t.Fatalf("the completed watermark must close [0,1000) before failing:\n got: %q\nwant: %q", out.String(), wantWindow)
		}
	})

	t.Run("broken tail does not mask the read error", func(t *testing.T) {
		// The fragment is not even valid JSON; the read failure, not a JSON
		// input error, is the outcome.
		broken := eventLine + `{"type":"watermark","time":1000`
		var out, late bytes.Buffer
		err := RunAggregate(&failingAfterReader{data: []byte(broken), failAt: len(broken), err: errSentinelChunkedRead, errOnBoundary: true}, 1000, &out, &late)
		if !errors.Is(err, errSentinelChunkedRead) {
			t.Fatalf("error must be the reader's original error, got %T: %v", err, err)
		}
		if ie, ok := err.(*InputError); ok {
			t.Fatalf("the broken tail must not be reported as an *InputError: %v", ie)
		}
		if out.String() != "" {
			t.Fatalf("no window output may come from the broken tail, got %q", out.String())
		}
	})

	t.Run("earlier record error wins over the read failure", func(t *testing.T) {
		// Line 2 is complete and invalid; its input error happened before the
		// read failure was known and stays the result.
		bad := eventLine + "not json\n" + watermarkLine
		var out, late bytes.Buffer
		err := RunAggregate(&failingAfterReader{data: []byte(bad), failAt: len(bad), err: errSentinelChunkedRead, errOnBoundary: true}, 1000, &out, &late)
		var ie *InputError
		if !errors.As(err, &ie) {
			t.Fatalf("the earlier record error must be kept, got %T: %v", err, err)
		}
		if ie.Line != 2 {
			t.Fatalf("InputError.Line = %d, want physical line 2", ie.Line)
		}
	})
}

// TestChunkedArrivalDiscriminatesCorrectAggregates ensures the invariant tests
// above would actually fail on the regressions they target: missed counts,
// double counting, and a late event admitted into the still-open overlap
// window all change the output even when the input is delivered one byte at a
// time. This is a characterization guard for the assertions used throughout
// the file (expected outputs are computed from the spec, not echoed back).
func TestChunkedArrivalDiscriminatesCorrectAggregates(t *testing.T) {
	type window struct {
		key        string
		start, end int64
		count      int64
		sum        int64
	}
	correct := []window{
		{"温度", 0, 1000, 1, 2},
		{"温度", 600, 1600, 2, 5},
	}
	missedSecondEvent := []window{
		{"温度", 0, 1000, 1, 2},
		{"温度", 600, 1600, 1, 2}, // event time=1000 lost
	}
	doubleFirstEvent := []window{
		{"温度", 0, 1000, 2, 4}, // time=700 counted twice
		{"温度", 600, 1600, 3, 7},
	}
	lateAdmitted := []window{
		{"温度", 0, 1000, 1, 2},
		{"温度", 600, 1600, 3, 14}, // late time=999/value=9 wrongly added
	}
	render := func(ws []window) string {
		var b strings.Builder
		for _, w := range ws {
			fmt.Fprintf(&b, `{"key":%q,"start":%d,"end":%d,"count":%d,"sum":%d}`+"\n", w.key, w.start, w.end, w.count, w.sum)
		}
		return b.String()
	}
	want := render(correct)
	if want != canonicalChunkedWindows {
		t.Fatalf("test harness render drifted from the pinned expectation:\n got: %q\nwant: %q", want, canonicalChunkedWindows)
	}
	for name, ws := range map[string][]window{
		"missed event":  missedSecondEvent,
		"double count":  doubleFirstEvent,
		"late admitted": lateAdmitted,
	} {
		if got := render(ws); got == want {
			t.Fatalf("mutated scenario %q unexpectedly matched the correct output", name)
		}
	}

	// End-to-end: deliver the real stream one byte at a time and confirm the
	// late value 9 never appears in any aggregate sum.
	input := canonicalChunkedScenario()
	cuts := make([]int, 0, len(input)-1)
	for at := 1; at < len(input); at++ {
		cuts = append(cuts, at)
	}
	out, _, err := runChunked(t, input, cuts)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Contains(out, `"sum":14`) || strings.Contains(out, `"sum":9`) {
		t.Fatalf("late event value leaked into a window: %q", out)
	}
}

// TestChunkedArrivalAcrossEntryPoints repeats the fragmentation invariant for
// the other public entry shapes, so read-batching can never become a record
// boundary in fixed/sliding/partitioned-only modes either.
func TestChunkedArrivalAcrossEntryPoints(t *testing.T) {
	type fixture struct {
		name string
		run  func(io.Reader, io.Writer, io.Writer) error
		in   string
		want string
	}
	events := func(records ...string) string { return strings.Join(records, "\n") + "\n" }

	fixed := events(
		`{"type":"event","key":"温度","time":100,"value":2}`,
		`{"type":"event","key":"温度","time":200,"value":3}`,
		`{"type":"watermark","time":1000}`,
	)
	sliding := events(
		`{"type":"event","key":"温度","time":700,"value":2}`,
		`{"type":"watermark","time":1000}`,
	)
	partitioned := events(
		`{"type":"event","key":"温度","time":100,"value":2,"partition":0}`,
		`{"type":"event","key":"温度","time":200,"value":3,"partition":1}`,
		`{"type":"watermark","time":1000,"partition":0}`,
		`{"type":"watermark","time":1000,"partition":1}`,
	)
	fxs := []fixture{
		{"fixed", func(r io.Reader, o, l io.Writer) error { return RunAggregate(r, 1000, o, l) }, fixed,
			`{"key":"温度","start":0,"end":1000,"count":2,"sum":5}` + "\n"},
		{"sliding", func(r io.Reader, o, l io.Writer) error { return RunAggregateSliding(r, 1000, 600, o, l) }, sliding,
			`{"key":"温度","start":0,"end":1000,"count":1,"sum":2}` + "\n"},
		{"partitioned", func(r io.Reader, o, l io.Writer) error { return RunAggregatePartitioned(r, 1000, 2, o, l) }, partitioned,
			`{"key":"温度","start":0,"end":1000,"count":2,"sum":5}` + "\n"},
	}

	for _, fx := range fxs {
		fx := fx
		t.Run(fx.name, func(t *testing.T) {
			// Reference one-shot result.
			var refOut, refLate bytes.Buffer
			if err := fx.run(strings.NewReader(fx.in), &refOut, &refLate); err != nil {
				t.Fatalf("one-shot: unexpected error: %v", err)
			}
			if refOut.String() != fx.want {
				t.Fatalf("one-shot output:\n got: %q\nwant: %q", refOut.String(), fx.want)
			}
			// Every two-read cut and several uniform sizes.
			schedules := append(allCuts(len(fx.in)),
				uniformCuts(len(fx.in), 1),
				uniformCuts(len(fx.in), 3),
				uniformCuts(len(fx.in), 7),
			)
			for _, cuts := range schedules {
				var out, late bytes.Buffer
				if err := fx.run(newChunkReader(fx.in, cuts...), &out, &late); err != nil {
					t.Fatalf("cuts %v: unexpected error: %v", cuts, err)
				}
				if out.String() != refOut.String() {
					t.Fatalf("cuts %v: window output:\n got: %q\nwant: %q", cuts, out.String(), refOut.String())
				}
				if late.String() != refLate.String() {
					t.Fatalf("cuts %v: late output:\n got: %q\nwant: %q", cuts, late.String(), refLate.String())
				}
			}
		})
	}
}
