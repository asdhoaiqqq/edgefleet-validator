// Command aggregate-read is a fully offline, self-contained example of using
// the aggregate feature through its Go library entry point instead of the
// `edgefleet aggregate` command: records come from an in-memory io.Reader and
// window results and late-event notices go into in-memory bytes.Buffers.
//
// It feeds the SAME byte stream to edgefleet.RunAggregate (1000 millisecond
// fixed windows) three times to show how the caller tells a clean end of input
// apart from a read failure when the reader returns a batch of bytes together
// with an error:
//
//  1. clean end: the final Read returns the bare io.EOF sentinel. The last
//     watermark arrived without a trailing newline and is still a record, so
//     both windows close.
//  2. read failure: the final batch arrives together with the input source's
//     own non-EOF error. Only records whose newline was already received
//     (including records carried by that same Read) are processed, the
//     unterminated watermark tail is discarded, and the source error is
//     returned unchanged.
//  3. wrapped io.EOF: errors.Is(err, io.EOF) is true, but the error is not the
//     bare io.EOF sentinel, so this behaves exactly like case 2.
//
// A fourth run shows that a format error on a complete record delivered inside
// the failing batch takes precedence over the read error and is located by
// physical line number, blank lines included; later records and the
// unterminated tail are not processed.
//
// Run it offline with:
//
//	go run ./examples/aggregate-read
package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/asdhoaiqqq/edgefleet-validator/edgefleet"
)

// errUpstream stands in for the failure a real input source (a net.Conn, a
// pipe, a message broker client) reports while handing over its last buffered
// bytes. A run returns it unchanged so errors.Is keeps working for the caller.
var errUpstream = errors.New("upstream connection reset by peer")

// input is the shared line-delimited JSON stream. The first three physical
// lines are newline-terminated; the final watermark deliberately is not.
//
//	line 1: event key=sensor-a time=100  value=5  -> window [0,1000)
//	line 2: watermark 1000 (has newline)         -> closes [0,1000)
//	line 3: event key=sensor-a time=1100 value=7 -> window [1000,2000)
//	line 4: watermark 2000 (NO trailing newline) -> closes [1000,2000)
//	  only if the stream ends with the bare io.EOF
const input = "" +
	`{"type":"event","key":"sensor-a","time":100,"value":5}` + "\n" +
	`{"type":"watermark","time":1000}` + "\n" +
	`{"type":"event","key":"sensor-a","time":1100,"value":7}` + "\n" +
	`{"type":"watermark","time":2000}`

// endWithReader hands out data and reports the stream ending on the Read that
// delivers the final byte: end == nil means the bare io.EOF sentinel (a clean
// end of input), while any non-nil end is returned together with the final
// bytes and again on every later Read -- exactly as a connection may return
// buffered bytes and a failure from one Read call. Splitting data over several
// reads would change nothing: record boundaries are newlines, not Read calls.
type endWithReader struct {
	data []byte
	end  error // nil means the bare io.EOF at the end
	pos  int
}

func (r *endWithReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		if r.end != nil {
			return 0, r.end
		}
		return 0, io.EOF
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	if r.pos < len(r.data) {
		return n, nil
	}
	if r.end != nil {
		return n, r.end // last bytes and the failure in one Read
	}
	return n, io.EOF // last bytes and the bare EOF in one Read
}

func main() {
	fmt.Println("shared input (4 physical lines; line 4 has no trailing newline):")
	printNumbered(input)
	fmt.Println()

	// 1. Clean end: bare io.EOF. The unterminated final watermark is processed.
	runScenario(
		"1. clean end: the final Read returns the bare io.EOF sentinel",
		&endWithReader{data: []byte(input)},
	)

	// 2. Read failure: bytes and error in the same final Read. Complete lines
	// 1-3 take effect; the line-4 watermark tail is discarded.
	runScenario(
		"2. read failure: the final batch arrives together with the source error",
		&endWithReader{data: []byte(input), end: errUpstream},
	)

	// 3. A wrapped io.EOF is still a failure, even though errors.Is can reach
	// io.EOF through its chain.
	wrappedEOF := fmt.Errorf("gateway closed while delivering the last batch: %w", io.EOF)
	runScenario(
		"3. wrapped io.EOF: errors.Is finds io.EOF, but it is not the bare sentinel",
		&endWithReader{data: []byte(input), end: wrappedEOF},
	)

	// 4. A complete but malformed record in the same failing batch wins over
	// the read error and is located by physical line (the blank line 2
	// counts); line 4 and the unterminated tail are never processed.
	fmt.Println("=== 4. malformed complete record inside the failing batch wins ===")
	badInput := strings.Join([]string{
		`{"type":"event","key":"sensor-a","time":100,"value":5}`, // physical line 1
		``,                                 // physical line 2: blank, still consumes a line number
		`not-json`,                         // physical line 3: newline-terminated but malformed
		`{"type":"watermark","time":1000}`, // physical line 4: unterminated tail
	}, "\n")
	printNumbered(badInput)
	fmt.Println()
	runScenario(
		"4. record error takes precedence over the read error",
		&endWithReader{data: []byte(badInput), end: errUpstream},
	)
}

// runScenario runs one fixed-window aggregation over r with in-memory outputs
// and prints exactly what the caller observed: the window result stream, the
// late-event notice stream and the returned error, plus the error
// classification the caller should use.
func runScenario(title string, r io.Reader) {
	var results, late bytes.Buffer
	err := edgefleet.RunAggregate(r, 1000, &results, &late)

	fmt.Println("=== " + title + " ===")
	fmt.Print("window result stream (out):\n")
	printBlock(results.String())
	fmt.Printf("late-event notice stream (lateLog): %s\n", emptyMarker(late.String()))
	fmt.Printf("returned error: %v\n", errString(err))

	// A clean end (the reader returned the bare io.EOF sentinel) comes back
	// from RunAggregate as a plain nil: the sentinel is consumed inside the
	// library, so the caller's success test is the usual err == nil. The
	// bare-io.EOF rule matters when YOU implement the io.Reader: return
	// io.EOF itself for a clean end, never a wrapped copy of it.
	fmt.Printf("  run succeeded (err == nil)  -> %v\n", err == nil)
	fmt.Printf("  errors.Is(err, io.EOF)      -> %v (RunAggregate never returns io.EOF, even on a clean end)\n", errors.Is(err, io.EOF))
	fmt.Printf("  errors.Is(err, errUpstream) -> %v\n", errors.Is(err, errUpstream))

	// A read failure is returned as the source's own error, never as an
	// *edgefleet.InputError: the discarded tail must not be misreported as a
	// malformed JSON line. A malformed record that DID receive its newline is
	// the opposite case and surfaces as *edgefleet.InputError first.
	var inputErr *edgefleet.InputError
	if errors.As(err, &inputErr) {
		fmt.Printf("  errors.As(*edgefleet.InputError) -> line %d: %s\n", inputErr.Line, inputErr.Reason)
	} else {
		fmt.Println("  errors.As(*edgefleet.InputError) -> false (not a record/JSON problem)")
	}
	fmt.Println()
}

// printNumbered prints content with 1-based physical line numbers, marking
// whether the stream ends with a newline.
func printNumbered(content string) {
	lines := strings.Split(content, "\n")
	terminated := strings.HasSuffix(content, "\n")
	last := len(lines) - 1
	if terminated {
		last-- // a trailing '\n' is a separator, not an extra empty record
	}
	for i := 0; i <= last; i++ {
		ending := `  + "\n"`
		if i == last && !terminated {
			ending = "  (no newline)"
		}
		fmt.Printf("  line %d: %s%s\n", i+1, lines[i], ending)
	}
	fmt.Printf("  (stream end: %s)\n", endingDescription(content))
}

func endingDescription(content string) string {
	if strings.HasSuffix(content, "\n") {
		return "ends with a newline"
	}
	return "NO trailing newline"
}

func printBlock(content string) {
	if content == "" {
		fmt.Println("  (none)")
		return
	}
	for _, line := range strings.Split(strings.TrimRight(content, "\n"), "\n") {
		fmt.Println("  " + line)
	}
}

func emptyMarker(content string) string {
	if content == "" {
		return "(none)"
	}
	return "\n" + indent(strings.TrimRight(content, "\n"))
}

func indent(s string) string {
	var b strings.Builder
	for _, line := range strings.Split(s, "\n") {
		b.WriteString("  " + line + "\n")
	}
	return strings.TrimRight(b.String(), "\n")
}

func errString(err error) string {
	if err == nil {
		return "<nil>"
	}
	return err.Error()
}
