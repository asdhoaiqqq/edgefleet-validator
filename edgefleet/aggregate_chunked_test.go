package edgefleet

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
)

// chunkReader delivers data in fixed-size pieces, simulating input that
// arrives in batches unrelated to record boundaries: a read may end in the
// middle of a multi-byte UTF-8 key, a JSON field, or a line separator, and
// one read may equally carry several records at once.
type chunkReader struct {
	data []byte
	size int
}

func (r *chunkReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	n := r.size
	if n > len(r.data) {
		n = len(r.data)
	}
	if n > len(p) {
		n = len(p)
	}
	copy(p, r.data[:n])
	r.data = r.data[n:]
	return n, nil
}

// errAfterReader delivers data in fixed-size pieces and then reports err
// instead of io.EOF.
type errAfterReader struct {
	data []byte
	size int
	err  error
}

func (r *errAfterReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, r.err
	}
	n := r.size
	if n > len(r.data) {
		n = len(r.data)
	}
	if n > len(p) {
		n = len(p)
	}
	copy(p, r.data[:n])
	r.data = r.data[n:]
	return n, nil
}

// The reference partitioned sliding scenario: window length 1000ms, slide
// 600ms, two partitions feeding the multi-byte key "温度". Partition 0's
// watermark 1600 arrives first but cannot determine the effective watermark
// alone; only partition 1's 1000 closes [0,1000). The late event at 999 must
// be skipped even though the overlapping [600,1600) window covering its time
// is still open, and partition 1's later 1600 closes [600,1600).
var canonicalLines = []string{
	`{"type":"event","key":"温度","time":700,"value":2,"partition":0}`,
	`{"type":"event","key":"温度","time":1000,"value":3,"partition":1}`,
	`{"type":"watermark","time":1600,"partition":0}`,
	`{"type":"watermark","time":1000,"partition":1}`,
	`{"type":"event","key":"温度","time":999,"value":9,"partition":1}`, // line 5: late
	`{"type":"watermark","time":1600,"partition":1}`,
}

var canonicalInput = strings.Join(canonicalLines, "\n") + "\n"

const canonicalStdout = `{"key":"温度","start":0,"end":1000,"count":1,"sum":2}` + "\n" +
	`{"key":"温度","start":600,"end":1600,"count":2,"sum":5}` + "\n"

const canonicalStderr = "line 5: late event time=999 below current watermark 1000, skipped\n"

func runChunked(t *testing.T, r io.Reader) (string, string, error) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	err := RunAggregatePartitionedSliding(r, 1000, 600, 2, &stdout, &stderr)
	return stdout.String(), stderr.String(), err
}

func checkCanonical(t *testing.T, r io.Reader) {
	t.Helper()
	stdout, stderr, err := runChunked(t, r)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stdout != canonicalStdout {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, canonicalStdout)
	}
	if stderr != canonicalStderr {
		t.Fatalf("stderr mismatch:\n got: %q\nwant: %q", stderr, canonicalStderr)
	}
}

// Baseline: the whole input handed over in one read.
func TestPartitionedSlidingChunkedBaseline(t *testing.T) {
	checkCanonical(t, strings.NewReader(canonicalInput))
}

// The same bytes delivered in fixed-size pieces must produce exactly the
// same windows, late notice and success as the one-shot run. Size 1 splits
// every multi-byte UTF-8 sequence of "温度", every JSON field and every
// newline across two reads; larger sizes carry several records per read.
func TestPartitionedSlidingChunkSizesInvariant(t *testing.T) {
	for _, size := range []int{1, 2, 3, 4, 5, 7, 11, 31, 64, 256, 1 << 20} {
		t.Run(fmt.Sprintf("size%d", size), func(t *testing.T) {
			r := &chunkReader{data: []byte(canonicalInput), size: size}
			checkCanonical(t, r)
		})
	}
}

// Splitting the input into exactly two reads at every possible byte offset
// must never change the result: a read boundary is not a record boundary.
func TestPartitionedSlidingEverySplitPoint(t *testing.T) {
	for i := 0; i <= len(canonicalInput); i++ {
		t.Run(fmt.Sprintf("split%d", i), func(t *testing.T) {
			r := io.MultiReader(
				strings.NewReader(canonicalInput[:i]),
				strings.NewReader(canonicalInput[i:]),
			)
			checkCanonical(t, r)
		})
	}
}

// CRLF separators and a final record with no trailing newline must not
// change the aggregation or drop the last watermark, even in pieces.
func TestPartitionedSlidingCRLFAndMissingFinalNewline(t *testing.T) {
	crlf := strings.Join(canonicalLines, "\r\n") // no terminator after the last record
	t.Run("one-shot", func(t *testing.T) {
		checkCanonical(t, strings.NewReader(crlf))
	})
	t.Run("bytewise", func(t *testing.T) {
		checkCanonical(t, &chunkReader{data: []byte(crlf), size: 1})
	})
	t.Run("lf-no-final-newline", func(t *testing.T) {
		lf := strings.Join(canonicalLines, "\n")
		checkCanonical(t, &chunkReader{data: []byte(lf), size: 3})
	})
}

// Blank lines produce no records but still count as physical lines for the
// late-event notice, however the input is chunked.
func TestPartitionedSlidingBlankLinesCountedInPieces(t *testing.T) {
	lines := []string{
		``,                // line 1 blank
		canonicalLines[0], // line 2
		``,                // line 3 blank
		canonicalLines[1], // line 4
		canonicalLines[2], // line 5
		canonicalLines[3], // line 6
		``,                // line 7 blank
		canonicalLines[4], // line 8: late event
		canonicalLines[5], // line 9
	}
	input := strings.Join(lines, "\n") + "\n"
	wantStderr := "line 8: late event time=999 below current watermark 1000, skipped\n"
	for _, size := range []int{1, 5, 1 << 20} {
		t.Run(fmt.Sprintf("size%d", size), func(t *testing.T) {
			stdout, stderr, err := runChunked(t, &chunkReader{data: []byte(input), size: size})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if stdout != canonicalStdout {
				t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, canonicalStdout)
			}
			if stderr != wantStderr {
				t.Fatalf("stderr = %q, want %q (blank lines count toward line numbers)", stderr, wantStderr)
			}
		})
	}
}

// An incomplete JSON record on the last physical line is an input error
// naming that line: windows already closed stay written, the still-open
// [600,1600) window is not flushed, and the late notice already logged stays.
func TestPartitionedSlidingTruncatedLastLineFails(t *testing.T) {
	truncated := strings.Join(canonicalLines[:5], "\n") + "\n" +
		`{"type":"watermark","time":1600,"part` // line 6: incomplete, no newline
	wantStdout := `{"key":"温度","start":0,"end":1000,"count":1,"sum":2}` + "\n"
	for _, size := range []int{1, 1 << 20} {
		t.Run(fmt.Sprintf("size%d", size), func(t *testing.T) {
			stdout, stderr, err := runChunked(t, &chunkReader{data: []byte(truncated), size: size})
			inputErr, ok := err.(*InputError)
			if !ok {
				t.Fatalf("expected *InputError, got %T: %v", err, err)
			}
			if inputErr.Line != 6 {
				t.Errorf("line = %d, want 6", inputErr.Line)
			}
			if !strings.Contains(inputErr.Reason, "invalid JSON") {
				t.Errorf("reason = %q, want invalid JSON", inputErr.Reason)
			}
			if stdout != wantStdout {
				t.Errorf("closed window must be retained, open window must not flush:\n got: %q\nwant: %q", stdout, wantStdout)
			}
			if stderr != canonicalStderr {
				t.Errorf("stderr = %q, want %q", stderr, canonicalStderr)
			}
		})
	}
}

// A partial line buffered when the read fails is still processed as that
// physical line's record, exactly as if the input ended there: the input
// error for the broken line surfaces, prior output stays.
func TestPartitionedSlidingReadErrorMidLineReportsLine(t *testing.T) {
	data := strings.Join(canonicalLines[:4], "\n") + "\n" + `{"type":"event","key":"温`
	r := &errAfterReader{data: []byte(data), size: 1, err: errSentinelRead}
	stdout, stderr, err := runChunked(t, r)
	inputErr, ok := err.(*InputError)
	if !ok {
		t.Fatalf("expected *InputError, got %T: %v", err, err)
	}
	if inputErr.Line != 5 {
		t.Errorf("line = %d, want 5", inputErr.Line)
	}
	wantStdout := `{"key":"温度","start":0,"end":1000,"count":1,"sum":2}` + "\n"
	if stdout != wantStdout {
		t.Errorf("closed window must be retained, got %q want %q", stdout, wantStdout)
	}
	if stderr != "" {
		t.Errorf("stderr = %q, want empty", stderr)
	}
}

var errSentinelRead = errors.New("synthetic read failure")

// A read error after complete valid records were delivered must surface as
// the run's own error, not be treated as a clean end of input. Output
// completed before the error stays; still-open windows are not flushed.
func TestPartitionedSlidingReadErrorPropagates(t *testing.T) {
	closedOnly := strings.Join(canonicalLines[:4], "\n") + "\n" // [0,1000) closed, [600,1600) open
	wantFirstWindow := `{"key":"温度","start":0,"end":1000,"count":1,"sum":2}` + "\n"

	cases := []struct {
		name       string
		data       string
		wantStdout string
		wantStderr string
	}{
		{"error after closed window", closedOnly, wantFirstWindow, ""},
		{"error after all records", canonicalInput, canonicalStdout, canonicalStderr},
	}
	for _, tc := range cases {
		for _, size := range []int{1, 1 << 20} {
			t.Run(fmt.Sprintf("%s/size%d", tc.name, size), func(t *testing.T) {
				r := &errAfterReader{data: []byte(tc.data), size: size, err: errSentinelRead}
				stdout, stderr, err := runChunked(t, r)
				if !errors.Is(err, errSentinelRead) {
					t.Fatalf("read error must be returned, not treated as EOF; got %v", err)
				}
				var inputErr *InputError
				if errors.As(err, &inputErr) {
					t.Fatalf("read error must not be reported as an input error, got %v", err)
				}
				if stdout != tc.wantStdout {
					t.Errorf("stdout mismatch:\n got: %q\nwant: %q", stdout, tc.wantStdout)
				}
				if stderr != tc.wantStderr {
					t.Errorf("stderr = %q, want %q", stderr, tc.wantStderr)
				}
			})
		}
	}
}
