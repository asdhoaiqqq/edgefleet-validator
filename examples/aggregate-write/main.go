// Command aggregate-write is a fully offline, self-contained example of how a
// caller tells a successful aggregation run apart from a WRITE-side failure:
// records come from an in-memory io.Reader (strings.Reader) and window results
// and late-event notices go into in-memory io.Writer implementations. Nothing
// touches the network, files or the current time.
//
// It feeds the SAME 1000 ms fixed-window input to edgefleet.RunAggregate four
// times:
//
//  1. control: the result writer is a plain bytes.Buffer. The line-6
//     watermark closes three windows at once and all three result lines come
//     back, then the run returns nil.
//  2. writer accepts the whole result line (JSON + newline) on the failing
//     call and returns its own error anyway: the run still fails with
//     *edgefleet.OutputError; receiving every byte never cancels the error.
//  3. writer accepts only a prefix and returns nil: io.ErrShortWrite marks
//     the line incomplete.
//  4. writer accepts only a prefix and returns its own error: that original
//     error is kept and errors.Is reaches it through *edgefleet.OutputError.
//
// In every failing case the caller can see exactly what the writer already
// held: earlier fully written result lines are valid window results; the
// failed write's accepted bytes (a complete-looking line or a fragment) are
// retained but NOT an acknowledged window result. The engine neither
// retries/resends the failed content nor writes the other windows that same
// watermark had also closed, reads no further records, and end of input never
// flushes still-open windows.
//
// Run it offline with:
//
//	go run ./examples/aggregate-write
package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/asdhoaiqqq/edgefleet-validator/edgefleet"
)

// errSinkBroken stands in for the failure a real result destination (a broken
// network connection, a quota-exhausted log pipeline, a vanished pipe) reports
// after accepting some bytes. The run returns it inside *edgefleet.OutputError
// so errors.Is keeps working for the caller.
var errSinkBroken = errors.New("result sink connection broken")

// sharedInput is the one input every scenario uses, line-delimited JSON,
// 1000 ms fixed windows, one key sensor-a, 7 physical lines. A blank line
// consumes physical line 3 so the error's Line field maps 1:1 to the input
// shown below; blank records are ignored but keep their line number.
//
//	line 1: event time=100  value=1  -> [0,1000)
//	line 2: event time=150  value=2  -> [0,1000)
//	line 3: (blank)
//	line 4: event time=1100 value=3  -> [1000,2000)
//	line 5: event time=2100 value=4  -> [2000,3000)
//	line 6: watermark 3000 -> closes [0,1000), [1000,2000), [2000,3000) at once
//	line 7: event time=3100 value=5 -> [3000,4000), never closed (no later watermark)
const sharedInput = "" +
	`{"type":"event","key":"sensor-a","time":100,"value":1}` + "\n" +
	`{"type":"event","key":"sensor-a","time":150,"value":2}` + "\n" +
	`` + "\n" +
	`{"type":"event","key":"sensor-a","time":1100,"value":3}` + "\n" +
	`{"type":"event","key":"sensor-a","time":2100,"value":4}` + "\n" +
	`{"type":"watermark","time":3000}` + "\n" +
	`{"type":"event","key":"sensor-a","time":3100,"value":5}`

// flakyWriter is an io.Writer that buffers every accepted byte while being
// scripted to fail exactly once: on its failAtCall-th Write it keeps at most
// accept bytes of that one write and returns (kept, failErr); every other
// write is accepted in full. The failed call is the last interaction with the
// writer because the engine stops at once, so its accepted bytes are always a
// suffix of the buffered content. Counting calls rather than matching content
// makes the trigger deterministic given the documented emission order (window
// end ascending, then key in UTF-8 byte order).
type flakyWriter struct {
	got bytes.Buffer

	call       int
	failAtCall int
	accept     int   // bytes to keep on the failing call (<= offered length)
	failErr    error // error to return on the failing call

	// results of the failing call, recorded for the report:
	failedCallSeen bool
	acceptedOnFail int
	offeredOnFail  int
}

func (w *flakyWriter) Write(p []byte) (int, error) {
	w.call++
	if w.call != w.failAtCall {
		return w.got.Write(p)
	}
	k := w.accept
	if k > len(p) {
		k = len(p)
	}
	if k > 0 {
		w.got.Write(p[:k])
	}
	w.failedCallSeen = true
	w.acceptedOnFail = k
	w.offeredOnFail = len(p)
	return k, w.failErr
}

func main() {
	fmt.Println("shared input (7 physical lines; line 3 is blank, line 7 has no trailing newline):")
	printNumbered(sharedInput)
	fmt.Println()
	fmt.Println("window parameters: RunAggregate(r, windowMillis=1000, out, lateLog)")
	fmt.Println("line 6 watermark 3000 closes three windows in one batch, in end-ascending order:")
	fmt.Println("  [0,1000)  [1000,2000)  [2000,3000)   <- one Write call per result line")
	fmt.Println("  [3000,4000) stays open; after a failure line 7 is never read")
	fmt.Println()

	// 1. Control: everything writes.
	runWriteScenario(
		"1. control: a healthy bytes.Buffer writer",
		&flakyWriter{failAtCall: 0}, // failAtCall 0 never matches a real call
	)

	// 2. Whole line received on the failing call, the writer's own error
	// returned nonetheless.
	runWriteScenario(
		"2. failing call offers/keeps the whole line (JSON + newline) and returns its own error",
		&flakyWriter{failAtCall: 2, accept: 1 << 20, failErr: errSinkBroken},
	)

	// 3. Prefix received, nil error -> io.ErrShortWrite.
	runWriteScenario(
		"3. failing call keeps only a 12-byte prefix and returns nil",
		&flakyWriter{failAtCall: 2, accept: 12, failErr: nil},
	)

	// 4. Prefix received and own error -> original error preserved.
	runWriteScenario(
		"4. failing call keeps only a 12-byte prefix and returns its own error",
		&flakyWriter{failAtCall: 2, accept: 12, failErr: errSinkBroken},
	)
}

// runWriteScenario runs one aggregation over a fresh strings.Reader of the
// shared input with in-memory writers, then prints exactly what the caller
// observed: acknowledged complete result rows, whatever the failed write left
// behind (a complete-looking but unacknowledged line or a fragment), the late
// notice stream and the returned error with its structured classification.
func runWriteScenario(title string, out *flakyWriter) {
	var late bytes.Buffer
	err := edgefleet.RunAggregate(strings.NewReader(sharedInput), 1000, out, &late)

	data := out.got.String()
	acknowledged, leftover := partitionReceived(data, out)

	fmt.Println("=== " + title + " ===")
	fmt.Println("result lines fully written by EARLIER, successful calls (valid window results):")
	printRows(acknowledged)
	fmt.Printf("bytes the failing call itself left behind: %s\n", describeLeftover(leftover, out))
	fmt.Printf("late-event notice stream (lateLog): %s\n", emptyMarker(late.String()))
	fmt.Printf("returned error: %v\n", errString(err))

	if err == nil {
		fmt.Println("  err == nil -> run succeeded; every window the watermark closed was written")
		fmt.Println()
		return
	}

	var oe *edgefleet.OutputError
	if !errors.As(err, &oe) {
		fmt.Println("  errors.As(*edgefleet.OutputError) -> false (not an output write failure)")
		fmt.Println()
		return
	}
	fmt.Println("  errors.As(*edgefleet.OutputError) -> true")
	fmt.Printf("    triggering input line  : %d (the blank physical line 3 kept its line number)\n", oe.Line)
	fmt.Printf("    result category (Kind) : %q\n", oe.Kind)
	fmt.Printf("    record detail          : %s\n", oe.Detail)
	fmt.Printf("    writer error (Err)     : %v\n", oe.Err)
	fmt.Printf("  errors.Is(err, errSinkBroken)  -> %v (the writer's own error exposed through Unwrap)\n", errors.Is(err, errSinkBroken))
	fmt.Printf("  errors.Is(err, io.ErrShortWrite) -> %v\n", errors.Is(err, io.ErrShortWrite))
	fmt.Println()
}

// partitionReceived splits the buffered bytes into what earlier, successful
// Write calls delivered and what the failing call itself accepted. The failing
// call is the last one the engine made, so its bytes are the last
// acceptedOnFail bytes of the buffer. When no call failed (the control run)
// every buffered byte is an acknowledged result line and there is no leftover.
func partitionReceived(data string, out *flakyWriter) (rows []string, leftover string) {
	if !out.failedCallSeen {
		rows = splitCompleteRows(data)
		return rows, ""
	}
	cut := len(data) - out.acceptedOnFail
	if cut < 0 {
		cut = 0
	}
	rows = splitCompleteRows(data[:cut])
	leftover = data[cut:]
	return rows, leftover
}

// splitCompleteRows breaks a stream that ends in a newline into its complete
// rows. Earlier successful calls always delivered whole JSON+newline lines.
func splitCompleteRows(data string) []string {
	data = strings.TrimRight(data, "\n")
	if data == "" {
		return nil
	}
	return strings.Split(data, "\n")
}

// describeLeftover explains the bytes the failing call accepted: a complete
// line plus newline is still NOT acknowledged (the write reported failure);
// a shorter prefix is a residual fragment; nothing at all means zero bytes.
func describeLeftover(leftover string, out *flakyWriter) string {
	if !out.failedCallSeen {
		return "(none — no call failed)"
	}
	accepted, offered := out.acceptedOnFail, out.offeredOnFail
	head := fmt.Sprintf("%q (%d of %d bytes offered)", leftover, accepted, offered)
	switch {
	case accepted == 0:
		return "(none — the writer accepted 0 bytes)"
	case accepted >= offered && strings.HasSuffix(leftover, "\n"):
		return head + " — the WHOLE result line including its newline is physically present, but the Write returned an error: it is retained yet NOT an acknowledged window result"
	default:
		return head + " — a residual fragment, NOT a valid window result"
	}
}

// printNumbered prints content with 1-based physical line numbers, marking
// whether each line carried its newline.
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
		label := lines[i]
		if label == "" {
			label = "(blank line)"
		}
		fmt.Printf("  line %d: %s%s\n", i+1, label, ending)
	}
}

func printRows(rows []string) {
	if len(rows) == 0 {
		fmt.Println("  (none)")
		return
	}
	for _, row := range rows {
		fmt.Println("  " + row)
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
