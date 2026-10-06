// Command aggregate-write is a fully offline, self-contained companion to the
// aggregate-read example: that one shows how RunAggregate reports a failure on
// the INPUT side, this one shows how it reports a failure on the OUTPUT side
// when a window result cannot be written in full.
//
// Everything is in memory: records come from a strings.Reader and window
// results and late-event notices go into in-memory writers, so the example
// needs no files, no network and never reads the current time.
//
// The SAME line-delimited JSON input is fed to edgefleet.RunAggregate four
// times with 1000 millisecond fixed windows. One watermark closes several
// windows at once: the results before a middle one are written successfully,
// the middle result hits a writer problem, and a later window from the same
// batch is never emitted.
//
//  1. healthy control: every result lands in full and RunAggregate returns
//     nil.
//  2. the result writer accepts the WHOLE result line (the complete JSON plus
//     its trailing newline) and returns its OWN error anyway. The full byte
//     count does not pay for the error: the run still fails and the original
//     error stays errors.Is-able.
//  3. the result writer accepts only a PREFIX of the result line and returns a
//     nil error: the engine reports io.ErrShortWrite.
//  4. the result writer accepts only a prefix AND returns its own error: the
//     partial bytes stay put and, with the prefix present, the original error
//     is kept (it is not replaced by io.ErrShortWrite).
//
// The output separates content confirmed as complete result lines from the
// raw bytes the failing write accepted: a truncated fragment is shown for what
// it is, never mistaken for a valid window result.
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
	"regexp"
	"strings"

	"github.com/asdhoaiqqq/edgefleet-validator/edgefleet"
)

// errSink stands in for the failure a real result destination (a broken
// network connection, a pipe whose reader went away, a full disk) returns from
// Write. A run keeps it unchanged so errors.Is exposes it to the caller, even
// after the writer accepted the full result line.
var errSink = errors.New("sink connection broken while acknowledging result")

// input is the shared line-delimited JSON stream, 11 physical lines. Blank
// lines 2 and 8 are intentional: they are ignored but still consume a physical
// line number, so the error line numbers stay aligned with the numbered
// listing. The two watermarks each close several windows at once.
//
//	line  1: event key=a time=100   value=11 -> window [0,1000)   for key a
//	line  2: (blank)
//	line  3: event key=b time=200   value=22 -> window [0,1000)   for key b
//	line  4: event key=c time=300   value=33 -> window [0,1000)   for key c
//	line  5: watermark 1000                    -> closes key a, key b, key c in [0,1000)
//	line  6: event key=a time=1100  value=44 -> window [1000,2000) for key a
//	line  7: event key=b time=1200  value=55 -> window [1000,2000) for key b
//	line  8: (blank)
//	line  9: event key=c time=1300  value=66 -> window [1000,2000) for key c
//	line 10: event key=a time=2100  value=77 -> window [2000,3000) for key a
//	line 11: watermark 3000                    -> closes [1000,2000) for a,b,c
//	                                           and [2000,3000) for a
//
// All three keys live in the same fixed windows; within one closure results
// are ordered by end ascending and then by key in UTF-8 byte order, so each
// batch emits key a, then key b, then key c. The input ends right after the
// line-11 watermark; nothing after the failure is ever processed.
const input = "" +
	`{"type":"event","key":"a","time":100,"value":11}` + "\n" +
	`` + "\n" +
	`{"type":"event","key":"b","time":200,"value":22}` + "\n" +
	`{"type":"event","key":"c","time":300,"value":33}` + "\n" +
	`{"type":"watermark","time":1000}` + "\n" +
	`{"type":"event","key":"a","time":1100,"value":44}` + "\n" +
	`{"type":"event","key":"b","time":1200,"value":55}` + "\n" +
	`` + "\n" +
	`{"type":"event","key":"c","time":1300,"value":66}` + "\n" +
	`{"type":"event","key":"a","time":2100,"value":77}` + "\n" +
	`{"type":"watermark","time":3000}` + "\n"

// resultLine is one complete, newline-terminated window result line the engine
// hands the result writer in a single Write call. The values are only used to
// script the example's injected writer behavior.
var resultLineRe = regexp.MustCompile(`^\{"key":"([^"]+)","start":(\d+),"end":(\d+),"count":(-?\d+),"sum":(-?\d+)\}\n$`)

// scriptedResultWriter is the in-memory result destination. When armed it
// behaves normally (buffering every byte) until it is asked to write the one
// target result line, where it applies an injected fault:
//
//   - accept < 0 means accept the whole line (len(p) bytes);
//   - otherwise accept up to that many bytes of the line.
//
// It then returns fault (which may be nil) for that one Write call, and stays
// broken on every later call in case the engine ever called again (it must
// not). Every byte it accepts is kept verbatim in got so the example can show
// exactly what landed. It records how many Write calls reached it. An unarmed
// writer is just an in-memory buffer, used for the healthy control.
type scriptedResultWriter struct {
	armed     bool
	targetKey string
	accept    int // < 0 = full length
	fault     error
	got       bytes.Buffer
	calls     int
	struck    bool
	// What the injected failing Write returned: struckN of struckTotal bytes
	// and struckErr; zero values mean the fault was never triggered.
	struckN     int
	struckTotal int
	struckErr   error
}

func (w *scriptedResultWriter) Write(p []byte) (int, error) {
	w.calls++
	if !w.armed || (!w.struck && !w.isTarget(p)) {
		// Normal behavior, including every result before the target line.
		return w.got.Write(p)
	}
	if w.struck {
		// The fault already happened: stay broken and accept nothing more.
		return 0, w.postFaultError()
	}
	w.struck = true
	w.struckTotal = len(p)
	k := w.accept
	if k < 0 || k > len(p) {
		k = len(p)
	}
	if k > 0 {
		w.got.Write(p[:k])
	}
	w.struckN = k
	w.struckErr = w.fault
	if k < len(p) {
		return k, w.fault // partial prefix: fault may be nil (short write)
	}
	return k, w.fault // full line accepted, but fault is still honored
}

// postFaultError is what the already-broken writer returns on a later call:
// the stored fault, or io.ErrShortWrite when the original fault was a nil-error
// short acceptance. It is only defensive; the engine never writes again after
// an output failure.
func (w *scriptedResultWriter) postFaultError() error {
	if w.fault != nil {
		return w.fault
	}
	return io.ErrShortWrite
}

// isTarget reports whether this write carries the scripted target result line.
// Only complete, correctly shaped window result lines ever match, so a prefix
// fragment on a later call could not be mistaken for the target.
func (w *scriptedResultWriter) isTarget(p []byte) bool {
	m := resultLineRe.FindSubmatch(p)
	return m != nil && string(m[1]) == w.targetKey
}

func main() {
	fmt.Println("shared input (11 physical lines; lines 2 and 8 are blank):")
	printNumbered(input)
	fmt.Println()
	fmt.Println("window parameters: RunAggregate(reader, windowMillis=1000, out, lateLog)")
	fmt.Println("fixed windows [0,1000) [1000,2000) [2000,3000), left-closed right-open")
	fmt.Println("closure order within a batch: end ascending, then key in UTF-8 byte order (a, b, c)")
	fmt.Println()

	// 1. Healthy control: every window closes and is written in full. An
	// unarmed writer is just an in-memory buffer with no injected fault.
	runWriteScenario(
		"1. healthy control: every result is written in full",
		&scriptedResultWriter{},
	)

	// 2. The writer accepts the WHOLE target line (JSON plus newline) and still
	// returns its own error. The complete byte count does not cancel the error.
	runWriteScenario(
		"2. whole result line accepted, but the writer returns its own error",
		&scriptedResultWriter{armed: true, targetKey: "b", accept: -1, fault: errSink},
	)

	// 3. The writer accepts only a prefix of the target line and returns nil:
	// the missing tail is reported as io.ErrShortWrite.
	runWriteScenario(
		"3. only a prefix accepted with a nil error: io.ErrShortWrite",
		&scriptedResultWriter{armed: true, targetKey: "b", accept: 20, fault: nil},
	)

	// 4. The writer accepts only a prefix AND returns its own error: the prefix
	// stays in place and the original error is kept, not replaced.
	runWriteScenario(
		"4. prefix accepted together with the writer's own error",
		&scriptedResultWriter{armed: true, targetKey: "b", accept: 20, fault: errSink},
	)
}

// runWriteScenario runs one fixed-window aggregation over the shared input with
// in-memory outputs and prints exactly what the caller observed: which result
// lines came from successful writes, the failing Write call's (n, err)
// contract and the bytes it nevertheless accepted, the late notices, the
// returned error and its classification.
func runWriteScenario(title string, out *scriptedResultWriter) {
	var late bytes.Buffer
	err := edgefleet.RunAggregate(strings.NewReader(input), 1000, out, &late)

	fmt.Println("=== " + title + " ===")

	// The failing call accepted exactly struckN bytes at the tail of got;
	// everything before that came from successful writes. Splitting on the
	// recorded count -- not on newlines -- keeps a byte-complete line from an
	// ERRORED call out of the confirmed set.
	all := out.got.Bytes()
	succeeded := all
	var failedBytes []byte
	if out.struck {
		succeeded = all[:len(all)-out.struckN]
		failedBytes = all[len(all)-out.struckN:]
	}
	fmt.Print("complete result lines retained from SUCCESSFUL writes:\n")
	printLines(succeeded)
	if out.struck {
		fmt.Printf("failing Write returned: n=%d of %d byte(s), err=%v\n", out.struckN, out.struckTotal, errString(out.struckErr))
		fmt.Print("bytes retained from the FAILED write (not a confirmed result):\n")
		printFailedBytes(failedBytes)
	} else {
		fmt.Print("failed write: (none)\n")
	}
	fmt.Printf("result writer Write calls attempted: %d\n", out.calls)
	fmt.Printf("late-event notice stream (lateLog): %s\n", emptyMarker(late.String()))
	fmt.Printf("returned error: %v\n", errString(err))

	classifyOutputError(err)
	fmt.Println()
}

// printLines prints one newline-terminated result line per row. Successful
// writes always leave complete lines, so p only ever ends on a newline here.
func printLines(p []byte) {
	if len(p) == 0 {
		fmt.Println("  (none)")
		return
	}
	for _, l := range strings.Split(strings.TrimRight(string(p), "\n"), "\n") {
		fmt.Println("  " + l)
	}
}

// printFailedBytes renders the bytes a failing Write accepted and states why
// they are not a confirmed result: a whole line handed over together with an
// error is still a failed call, while a prefix is a truncated fragment.
func printFailedBytes(p []byte) {
	if len(p) == 0 {
		fmt.Println("  (zero bytes accepted)")
		return
	}
	fmt.Printf("  raw: %q\n", string(p))
	if bytes.HasSuffix(p, []byte("\n")) {
		fmt.Printf("  decoded: %s\n", strings.TrimRight(string(p), "\n"))
		fmt.Println("  -> the full JSON object AND its trailing newline are physically present, but the Write")
		fmt.Println("     returned an error; the complete byte count does not cancel it and the engine treats")
		fmt.Println("     this window as not written.")
		return
	}
	fmt.Printf("  decoded prefix: %s\n", string(p))
	fmt.Println("  -> a truncated fragment with no terminating newline; it is not a valid window result line.")
}

// classifyOutputError demonstrates the caller-side decision the section
// documents: assert *edgefleet.OutputError, read the triggering physical input
// line, the result category and the record detail, then use errors.Is to tell
// the writer's own error apart from a nil-error short write.
func classifyOutputError(err error) {
	var oe *edgefleet.OutputError
	if !errors.As(err, &oe) {
		fmt.Println("  errors.As(*edgefleet.OutputError) -> false (the run was not stopped by an output failure)")
		return
	}
	fmt.Printf("  errors.As(*edgefleet.OutputError) -> true\n")
	fmt.Printf("    oe.Line   = %d (the physical input line that triggered this write; blank lines count)\n", oe.Line)
	fmt.Printf("    oe.Kind   = %q\n", oe.Kind)
	fmt.Printf("    oe.Detail = %q\n", oe.Detail)
	if key, start, end, ok := parseWindowDetail(oe.Detail); ok {
		fmt.Printf("    -> window result for key=%q interval=[%d,%d)\n", key, start, end)
	}
	fmt.Printf("    oe.Err    = %v\n", oe.Err)
	fmt.Printf("  errors.Is(err, errSink)      -> %v (the result writer's own error)\n", errors.Is(err, errSink))
	fmt.Printf("  errors.Is(err, io.ErrShortWrite) -> %v (nil error but the full result was not accepted)\n", errors.Is(err, io.ErrShortWrite))
}

var windowDetailRe = regexp.MustCompile(`^key "([^"]*)" window \[(-?\d+),(-?\d+)\)$`)

// parseWindowDetail pulls the key and the [start,end) interval out of an
// OutputError.Detail shaped like: key "b" window [1000,2000). The late-event
// notice kind uses a different detail shape and does not match.
func parseWindowDetail(detail string) (key string, start, end int64, ok bool) {
	m := windowDetailRe.FindStringSubmatch(detail)
	if m == nil {
		return "", 0, 0, false
	}
	fmt.Sscan(m[2], &start)
	fmt.Sscan(m[3], &end)
	return m[1], start, end, true
}

// printNumbered prints content with 1-based physical line numbers, marking the
// trailing newline state of each line.
func printNumbered(content string) {
	lines := strings.Split(content, "\n")
	last := len(lines) - 2 // content always ends with '\n', so the final element is not a line
	for i := 0; i <= last; i++ {
		marker := `  + "\n"`
		if lines[i] == "" {
			marker += "  (blank line)"
		}
		fmt.Printf("  line %2d: %s%s\n", i+1, lines[i], marker)
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
