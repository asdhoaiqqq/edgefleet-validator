package edgefleet

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
)

var errBoom = errors.New("boom: output exploded")

// scriptedWriter accepts bytes until failAfter total bytes have been
// accepted; afterwards every write is rejected. When fail is non-nil the
// failing write returns (acceptedPrefixBytes, fail); when fail is nil it
// returns (acceptedPrefixBytes, nil), simulating a silent short write.
type scriptedWriter struct {
	buf       bytes.Buffer
	total     int
	failAfter int
	fail      error
}

func (w *scriptedWriter) Write(p []byte) (int, error) {
	room := w.failAfter - w.total
	if room <= 0 {
		if w.fail != nil {
			return 0, w.fail
		}
		return 0, nil
	}
	take := room
	if take > len(p) {
		take = len(p)
	}
	w.buf.Write(p[:take])
	w.total += take
	if take < len(p) {
		return take, w.fail // nil fail models a silent short write
	}
	return take, nil
}

func firstResultLine(key string, start, end int64, count, sum int64) string {
	return fmt.Sprintf(`{"key":%q,"start":%d,"end":%d,"count":%d,"sum":%d}`+"\n", key, start, end, count, sum)
}

func TestOutputErrorWindowResultZeroBytesRawError(t *testing.T) {
	// Watermark is line 2 and triggers one window result; the writer rejects
	// every byte with a raw error.
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":1,"value":7}`,
		`{"type":"watermark","time":1000}`,
		`{"type":"event","key":"k","time":2000,"value":1}`, // must never be processed
	}, "\n")
	out := &scriptedWriter{failAfter: 0, fail: errBoom}
	var late bytes.Buffer
	err := RunAggregate(strings.NewReader(input), 1000, out, &late)

	var oe *OutputError
	if !errors.As(err, &oe) {
		t.Fatalf("want *OutputError, got %T: %v", err, err)
	}
	if !errors.Is(err, errBoom) {
		t.Fatalf("errors.Is must expose raw writer error, got %v", err)
	}
	if oe.Kind != "window result" {
		t.Fatalf("Kind = %q, want window result", oe.Kind)
	}
	if oe.Line != 2 {
		t.Fatalf("Line = %d, want 2", oe.Line)
	}
	msg := err.Error()
	for _, want := range []string{"window result", "line 2", `"k"`, "0", "1000"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("error %q missing %q", msg, want)
		}
	}
	if out.buf.Len() != 0 {
		t.Fatalf("no bytes should be retained, got %q", out.buf.String())
	}
}

func TestOutputErrorWindowResultPartialPrefix(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":1,"value":7}`,
		`{"type":"watermark","time":1000}`,
	}, "\n")
	out := &scriptedWriter{failAfter: 6, fail: errBoom}
	var late bytes.Buffer
	err := RunAggregate(strings.NewReader(input), 1000, out, &late)
	if !errors.Is(err, errBoom) {
		t.Fatalf("want raw error via errors.Is, got %v", err)
	}
	// The accepted prefix stays exactly as received.
	if got := out.buf.String(); got != `{"key"` {
		t.Fatalf("accepted prefix = %q, want %q", got, `{"key"`)
	}
}

func TestOutputErrorWindowResultShortWrite(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":1,"value":7}`,
		`{"type":"watermark","time":1000}`,
		`not json`, // would be an InputError if processing continued
	}, "\n")
	out := &scriptedWriter{failAfter: 4} // silent short write, no error
	var late bytes.Buffer
	err := RunAggregate(strings.NewReader(input), 1000, out, &late)
	var oe *OutputError
	if !errors.As(err, &oe) {
		t.Fatalf("want *OutputError for short write, got %T: %v", err, err)
	}
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("short write must be io.ErrShortWrite via errors.Is, got %v", err)
	}
	if errors.Is(err, errBoom) {
		t.Fatalf("short write must not carry errBoom")
	}
	if got := out.buf.String(); got != `{"ke` {
		t.Fatalf("truncated prefix retained = %q", got)
	}
}

func TestOutputErrorStopsFurtherOutputAndProcessing(t *testing.T) {
	// Two windows become ready on the same watermark, ordered by key: a then z.
	// Failing while writing a means z must never be emitted.
	input := strings.Join([]string{
		`{"type":"event","key":"z","time":1,"value":1}`,
		`{"type":"event","key":"a","time":1,"value":1}`,
		`{"type":"watermark","time":1000}`,
		`{"type":"event","key":"k","time":2000,"value":1}`,
		`{"type":"watermark","time":9999}`,
		`not json`,
	}, "\n")
	first := firstResultLine("a", 0, 1000, 1, 1)
	out := &scriptedWriter{failAfter: len(first) + 3, fail: errBoom}
	var late bytes.Buffer
	err := RunAggregate(strings.NewReader(input), 1000, out, &late)

	var oe *OutputError
	if !errors.As(err, &oe) {
		t.Fatalf("want *OutputError, got %T: %v", err, err)
	}
	if oe.Kind != "window result" || oe.Line != 3 {
		t.Fatalf("unexpected OutputError: %+v", oe)
	}
	got := out.buf.String()
	if !strings.HasPrefix(got, first) {
		t.Fatalf("fully written earlier result must be retained:\n got %q\nwant prefix %q", got, first)
	}
	if strings.Contains(got, `"z"`) {
		t.Fatalf("later ready window must not be emitted after a failed write:\n%s", got)
	}
	if strings.Contains(got, `"start":2000`) {
		t.Fatalf("later watermark must not produce output after a failed write:\n%s", got)
	}
}

func TestOutputErrorLateNoticeRawError(t *testing.T) {
	// Line 2 is a blank line (still counted); the late event is physical
	// line 3, event time 999 vs watermark 1000.
	input := strings.Join([]string{
		`{"type":"watermark","time":1000}`,
		``,
		`{"type":"event","key":"k","time":999,"value":1}`,
		`not json`, // must not be reached
	}, "\n")
	var out bytes.Buffer
	late := &scriptedWriter{failAfter: 0, fail: errBoom}
	err := RunAggregate(strings.NewReader(input), 1000, &out, late)

	var oe *OutputError
	if !errors.As(err, &oe) {
		t.Fatalf("want *OutputError, got %T: %v", err, err)
	}
	if !errors.Is(err, errBoom) {
		t.Fatalf("errors.Is must expose raw lateLog error, got %v", err)
	}
	if oe.Kind != "late-event notice" {
		t.Fatalf("Kind = %q, want late-event notice", oe.Kind)
	}
	if oe.Line != 3 {
		t.Fatalf("Line = %d, want 3 (blank line counted)", oe.Line)
	}
	msg := err.Error()
	for _, want := range []string{"late-event notice", "line 3", "time=999", "watermark 1000"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("error %q missing %q", msg, want)
		}
	}
	if out.String() != "" {
		t.Fatalf("stdout must stay empty, got %q", out.String())
	}
}

func TestOutputErrorLateNoticeShortWrite(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"watermark","time":1000}`,
		`{"type":"event","key":"k","time":999,"value":1}`,
	}, "\n")
	var out bytes.Buffer
	late := &scriptedWriter{failAfter: 5} // accepts only "line " then silently shorts
	err := RunAggregate(strings.NewReader(input), 1000, &out, late)
	var oe *OutputError
	if !errors.As(err, &oe) {
		t.Fatalf("want *OutputError for late short write, got %T: %v", err, err)
	}
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("short late notice must be io.ErrShortWrite, got %v", err)
	}
	if got := late.buf.String(); got != "line " {
		t.Fatalf("late prefix retained = %q, want %q", got, "line ")
	}
}

func TestLateNoticeSuccessStillSkipsAndContinues(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"watermark","time":1000}`,
		`{"type":"event","key":"k","time":999,"value":1}`,  // late, fully reported
		`{"type":"event","key":"k","time":1000,"value":5}`, // valid
		`{"type":"watermark","time":2000}`,
	}, "\n")
	var out, late bytes.Buffer
	if err := RunAggregate(strings.NewReader(input), 1000, &out, &late); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	wantNotice := "line 2: late event time=999 below current watermark 1000, skipped\n"
	if late.String() != wantNotice {
		t.Fatalf("late notice = %q, want %q", late.String(), wantNotice)
	}
	wantOut := firstResultLine("k", 1000, 2000, 1, 5)
	if out.String() != wantOut {
		t.Fatalf("window result = %q, want %q", out.String(), wantOut)
	}
}

func TestOutputErrorSlidingWindowResult(t *testing.T) {
	// Window 1000, slide 600: event at 700 lands in [0,1000) and [600,1600).
	// Watermark 1600 closes both, ordered by end; the failing first write
	// reports [0,1000) and [600,1600) is never emitted.
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":700,"value":3}`,
		`{"type":"watermark","time":1600}`,
	}, "\n")
	out := &scriptedWriter{failAfter: 0, fail: errBoom}
	var late bytes.Buffer
	err := RunAggregateSliding(strings.NewReader(input), 1000, 600, out, &late)
	var oe *OutputError
	if !errors.As(err, &oe) {
		t.Fatalf("want *OutputError, got %T: %v", err, err)
	}
	msg := err.Error()
	for _, want := range []string{"window result", "line 2", `"k"`, "[0,1000)"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("error %q missing %q", msg, want)
		}
	}
	if strings.Contains(msg, "[600,1600)") {
		t.Fatalf("still-open window [600,1600) must not be reported on failure: %q", msg)
	}
}

func TestOutputErrorPartitionedWindowResult(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":1,"value":2,"partition":0}`,
		`{"type":"watermark","time":1000,"partition":0}`,
		`{"type":"watermark","time":1000,"partition":1}`,
	}, "\n")
	out := &scriptedWriter{failAfter: 2, fail: errBoom}
	var late bytes.Buffer
	err := RunAggregatePartitioned(strings.NewReader(input), 1000, 2, out, &late)
	var oe *OutputError
	if !errors.As(err, &oe) {
		t.Fatalf("want *OutputError, got %T: %v", err, err)
	}
	if oe.Line != 3 || oe.Kind != "window result" {
		t.Fatalf("unexpected OutputError: %+v", oe)
	}
}

func TestOutputErrorIdleRecordClosesWindow(t *testing.T) {
	// Partition 0 reports a watermark that closes a window once partition 1
	// is declared idle on physical line 3; the idle record's closure write
	// fails and must be attributed to line 3.
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":1,"value":2,"partition":0}`,
		`{"type":"watermark","time":1000,"partition":0}`,
		`{"type":"idle","partition":1}`,
		`{"type":"watermark","time":5000,"partition":0}`,
	}, "\n")
	out := &scriptedWriter{failAfter: 0, fail: errBoom}
	var late bytes.Buffer
	err := RunAggregatePartitionedSliding(strings.NewReader(input), 1000, 1000, 2, out, &late)
	var oe *OutputError
	if !errors.As(err, &oe) {
		t.Fatalf("want *OutputError, got %T: %v", err, err)
	}
	if oe.Line != 3 {
		t.Fatalf("idle-triggered failure Line = %d, want 3", oe.Line)
	}
	if oe.Kind != "window result" {
		t.Fatalf("Kind = %q, want window result", oe.Kind)
	}
	if got := out.buf.Len(); got != 0 {
		t.Fatalf("no output bytes expected, got %d", got)
	}
}

func TestOutputErrorPartitionedLateNoticeShortWrite(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"watermark","time":1000,"partition":0}`,
		`{"type":"watermark","time":2000,"partition":1}`,
		`{"type":"event","key":"k","time":999,"value":1,"partition":0}`,
	}, "\n")
	var out bytes.Buffer
	late := &scriptedWriter{failAfter: 1}
	err := RunAggregatePartitioned(strings.NewReader(input), 1000, 2, &out, late)
	var oe *OutputError
	if !errors.As(err, &oe) {
		t.Fatalf("want *OutputError, got %T: %v", err, err)
	}
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("want io.ErrShortWrite, got %v", err)
	}
	if oe.Kind != "late-event notice" || oe.Line != 3 {
		t.Fatalf("unexpected OutputError: %+v", oe)
	}
}

func TestOpenWindowsStillNotEmittedAtEOF(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":1,"value":1}`,
		`{"type":"event","key":"q","time":2,"value":1}`,
	}, "\n")
	out := &scriptedWriter{failAfter: 0, fail: errBoom} // would fail if anything were written
	var late bytes.Buffer
	if err := RunAggregate(strings.NewReader(input), 1000, out, &late); err != nil {
		t.Fatalf("end of input must not flush open windows, got %v", err)
	}
	if out.buf.Len() != 0 {
		t.Fatalf("end of input wrote %q", out.buf.String())
	}
}
