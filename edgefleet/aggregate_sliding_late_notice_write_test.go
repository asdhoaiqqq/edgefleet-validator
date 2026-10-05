package edgefleet

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

// This file is the regression guard for one late-event notice failure shape:
// the lateLog writer accepts the COMPLETE notice line, including its trailing
// newline, and returns a non-nil error from that same Write call. The notice
// has already arrived in full, but the write must still count as failed: the
// run must not judge success by the accepted byte count, aggregate as if the
// notice had landed, or report a short write.
//
// Every scenario here uses the single-watermark sliding entry
// (RunAggregateSliding) with a slide interval (600ms) that does not divide the
// window length (1000ms): windows are [0,1000), [600,1600), [1200,2200), ...
// The first watermark closes an early window while an overlap window stays
// open, so the late event below lands strictly below the watermark yet inside
// the still-open window -- the case where wrongly continuing to aggregate
// would corrupt the open window.

// fullAcceptErrWriter accepts the whole payload -- buffering every byte,
// reporting n == len(p) -- and still returns err from the same Write call.
// This is the writer contract the implementation must not misread as success.
type fullAcceptErrWriter struct {
	err   error
	calls int
	got   bytes.Buffer
}

func (w *fullAcceptErrWriter) Write(p []byte) (int, error) {
	w.calls++
	w.got.Write(p)
	return len(p), w.err
}

// slidingLateNoticeScenario builds the input used by both the failure and the
// fully-written control cases. Physical layout (blank lines count):
//
//	line 1: blank
//	line 2: event key=a time=600  value=4  -> [0,1000), [600,1600)
//	line 3: watermark 1000                 -> closes only [0,1000)
//	line 4: event key=a time=1400 value=7  -> [600,1600), [1200,2200)
//	line 5: blank
//	line 6: event key=a time=900  value=9  -> 900 < watermark 1000: late,
//	                                          although still inside [600,1600)
//	line 7: watermark 2000                 -> would close [600,1600) and more
//	line 8: malformed record               -> would be an *InputError
//
// throughLateLine ends with line 6's newline; the read-fault variant stops
// there. controlInput additionally carries the newline-terminated line 7.
// failureInput carries both lines 7 and 8 newline-terminated, so reaching
// either after the notice failure can never be blamed on a missing delimiter.
func slidingLateNoticeScenario() (throughLateLine, controlInput, failureInput string) {
	head := strings.Join([]string{
		``, // line 1 blank
		`{"type":"event","key":"a","time":600,"value":4}`,
		`{"type":"watermark","time":1000}`,
		`{"type":"event","key":"a","time":1400,"value":7}`,
		``, // line 5 blank
		`{"type":"event","key":"a","time":900,"value":9}`,
	}, "\n") + "\n"
	closingLine := `{"type":"watermark","time":2000}` + "\n"
	malformedLine := `not json` + "\n"
	return head, head + closingLine, head + closingLine + malformedLine
}

const slidingLateFirstWindow = `{"key":"a","start":0,"end":1000,"count":1,"sum":4}` + "\n"

func slidingLateWantNotice() string {
	return "line 6: late event time=900 below current watermark 1000, skipped\n"
}

// assertLateNoticeOutputError checks the failure is the late-event notice
// OutputError for physical line 6, naming the event time and the judging
// watermark, and that it is neither a short write nor a record-format error.
func assertLateNoticeOutputError(t *testing.T, err error) {
	t.Helper()
	var oe *OutputError
	if !errors.As(err, &oe) {
		t.Fatalf("expected *OutputError, got %T: %v", err, err)
	}
	if oe.Line != 6 {
		t.Errorf("OutputError.Line = %d, want 6 (blank lines count as physical lines)", oe.Line)
	}
	if oe.Kind != "late-event notice" {
		t.Errorf("OutputError.Kind = %q, want %q", oe.Kind, "late-event notice")
	}
	msg := err.Error()
	if !strings.Contains(msg, "event time 900") || !strings.Contains(msg, "watermark 1000") {
		t.Errorf("error %q must identify the skipped event time 900 and watermark 1000", msg)
	}
	if !errors.Is(err, errSentinelOutput) {
		t.Errorf("errors.Is must expose the writer's original error, got %v", err)
	}
	if errors.Is(err, io.ErrShortWrite) {
		t.Errorf("a fully accepted write that still errored must not be reported as a short write: %v", err)
	}
	var ie *InputError
	if errors.As(err, &ie) {
		t.Errorf("a notice write failure must not be reported as a record/input error: %v", err)
	}
}

// TestSlidingLateNoticeFullyAcceptedThenErrorFails: the writer accepts the
// complete notice (newline included) and returns its error on that same call.
// The run must fail with the late-notice OutputError, even though every byte
// landed. The accepted notice stays in place exactly once, the earlier closed
// window stays, and no later record is processed -- in particular the line-7
// watermark must not close the open overlap window and the malformed line 8
// must not surface as an InputError.
func TestSlidingLateNoticeFullyAcceptedThenErrorFails(t *testing.T) {
	_, _, failureInput := slidingLateNoticeScenario()
	late := &fullAcceptErrWriter{err: errSentinelOutput}
	// failAt is far past the one expected window write, so out only records;
	// its call count proves no further window result is attempted.
	out := &failOnCallWriter{failAt: 1000, err: errSentinelOutput}

	err := RunAggregateSliding(strings.NewReader(failureInput), 1000, 600, out, late)
	assertLateNoticeOutputError(t, err)

	if got := late.got.String(); got != slidingLateWantNotice() {
		t.Errorf("accepted notice must remain verbatim:\n got: %q\nwant: %q", got, slidingLateWantNotice())
	}
	if late.calls != 1 {
		t.Errorf("late writer was called %d times, want exactly 1 (the failed notice is never rewritten)", late.calls)
	}
	if got := out.got.String(); got != slidingLateFirstWindow {
		t.Errorf("previously closed window must stay and nothing may be appended:\n got: %q\nwant: %q", got, slidingLateFirstWindow)
	}
	if out.calls != 1 {
		t.Errorf("window writer was called %d times, want exactly 1 (later watermark must add no result)", out.calls)
	}
}

// TestSlidingLateNoticeFullyAcceptedErrorWithReadFailureWins: the Read that
// delivers the newline-complete late record's batch also reports a read
// failure. The record's newline was received, so it is processed, its notice
// write fails (fully accepted, then error), and that OutputError must be what
// the caller gets: the read failure learned in the same batch must not
// overwrite it, and errors.Is must reach the writer error rather than the
// read error.
func TestSlidingLateNoticeFullyAcceptedErrorWithReadFailureWins(t *testing.T) {
	throughLateLine, _, _ := slidingLateNoticeScenario()

	cases := []struct {
		name  string
		chunk int
	}{
		{"notice batch and read fault in one read", 0},
		{"one byte per read, fault with last byte", 1},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			r := &failingAfterReader{
				data:   []byte(throughLateLine),
				failAt: len(throughLateLine),
				err:    errSentinelChunkedRead,
				chunk:  tc.chunk,
				// The read that hands over the late record's trailing newline
				// returns the read fault together with those bytes.
				errOnBoundary: true,
			}
			late := &fullAcceptErrWriter{err: errSentinelOutput}
			out := &failOnCallWriter{failAt: 1000, err: errSentinelOutput}

			err := RunAggregateSliding(r, 1000, 600, out, late)
			assertLateNoticeOutputError(t, err)

			if errors.Is(err, errSentinelChunkedRead) {
				t.Errorf("the notice write error must replace the read fault discovered afterwards: %v", err)
			}
			if got := late.got.String(); got != slidingLateWantNotice() {
				t.Errorf("accepted notice must remain verbatim, once:\n got: %q\nwant: %q", got, slidingLateWantNotice())
			}
			if late.calls != 1 {
				t.Errorf("late writer was called %d times, want exactly 1", late.calls)
			}
			if got := out.got.String(); got != slidingLateFirstWindow {
				t.Errorf("closed window stays, open window is not flushed:\n got: %q\nwant: %q", got, slidingLateFirstWindow)
			}
			if out.calls != 1 {
				t.Errorf("window writer was called %d times, want exactly 1", out.calls)
			}
		})
	}
}

// TestSlidingLateNoticeFullyWrittenControlContinues is the compatible
// control behavior: when the complete notice is accepted with no error, the input
// keeps being processed. The late event contributes neither a count nor a sum
// to the still-open overlap window, and the later watermark closes that window
// from the normal events alone.
func TestSlidingLateNoticeFullyWrittenControlContinues(t *testing.T) {
	_, controlInput, _ := slidingLateNoticeScenario()
	var out, late bytes.Buffer

	if err := RunAggregateSliding(strings.NewReader(controlInput), 1000, 600, &out, &late); err != nil {
		t.Fatalf("a fully written notice must not fail the run: %v", err)
	}
	if got := late.String(); got != slidingLateWantNotice() {
		t.Errorf("late notice:\n got: %q\nwant: %q", got, slidingLateWantNotice())
	}
	wantOut := slidingLateFirstWindow +
		// [600,1600) holds the time=600 and time=1400 events only: count 2,
		// sum 11. The skipped time=900/value=9 must not appear.
		`{"key":"a","start":600,"end":1600,"count":2,"sum":11}` + "\n"
	if got := out.String(); got != wantOut {
		t.Errorf("window output:\n got: %q\nwant: %q", got, wantOut)
	}
	if strings.Contains(out.String(), `"sum":20`) || strings.Contains(out.String(), `"count":3`) {
		t.Errorf("the late event leaked into an open window: %q", out.String())
	}
}
