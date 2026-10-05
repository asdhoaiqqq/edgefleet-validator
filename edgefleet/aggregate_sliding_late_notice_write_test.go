package edgefleet

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

// This file is the regression guard for a late-event notice whose recipient
// accepts the ENTIRE notice -- the complete line including its trailing
// newline -- and returns a non-nil error from that same Write. The notice did
// land in full, but a write that came back with an error is still a failed
// write: the run must report *OutputError instead of treating the accepted
// byte count as success and continuing to aggregate the skipped late event's
// window. The other notice-write shapes (rejected outright, prefix accepted
// then error, nil-error short write) are covered in aggregate_output_test.go.
//
// All scenarios use the single-watermark sliding entry point with a slide
// interval that does not divide the window length (window 1000, slide 600),
// so the first watermark closes [0,1000) while the overlapping window
// [600,1600) stays open with live events; the late event has an event time
// strictly below the current watermark yet still inside that open window, so
// it must be skipped wholesale rather than counted.

// fullAcceptErrWriter accepts the entire payload of every Write and returns a
// non-nil error in the same call. It models a sink that confirms receipt of a
// complete record (including its trailing newline) and then reports that its
// downstream failed: n == len(p), but the write still failed. The accepted
// bytes are buffered so tests can pin what the recipient already holds.
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

// slidingLateNoticeScenarioInput is the shared stream (window 1000, slide 600,
// single watermark):
//
//	line 1: blank (physical lines count)
//	line 2: event k time=900 value=7  -> [0,1000) and [600,1600)
//	line 3: watermark 1000            -> closes only [0,1000); [600,1600) stays open
//	line 4: blank
//	line 5: event k time=999 value=1  -> late: 999 < watermark 1000, although
//	                                        999 is still inside open [600,1600)
//	line 6: event k time=1000 value=3 -> would enter [600,1600) if processing continued
//	line 7: watermark 1600            -> would close [600,1600) if reached
//	line 8: malformed record          -> must never become an *InputError
func slidingLateNoticeScenarioInput() string {
	return strings.Join([]string{
		``, // line 1 blank
		`{"type":"event","key":"k","time":900,"value":7}`, // line 2
		`{"type":"watermark","time":1000}`,                // line 3
		``,                                                // line 4 blank
		`{"type":"event","key":"k","time":999,"value":1}`, // line 5 late
		`{"type":"event","key":"k","time":1000,"value":3}`,
		`{"type":"watermark","time":1600}`,
		`not json`, // line 8 must not be reached
	}, "\n")
}

const slidingLateFailingLine = 5

var slidingLateWantNotice = []byte(
	"line 5: late event time=999 below current watermark 1000, skipped\n",
)

var slidingLateWantFirstWindow = []byte(
	`{"key":"k","start":0,"end":1000,"count":1,"sum":7}` + "\n",
)

// TestSlidingLateNoticeFullyAcceptedThenErrorFailsWrite: the notice recipient
// accepts the whole notice, newline included, and still errors. The caller
// must get the existing *OutputError for a late-event notice naming physical
// line 5 (blank lines count), the event time 999 and the judging watermark
// 1000, with errors.Is reaching the recipient's own error. It must not be
// reported as a short write, a record-format error or a clean success.
func TestSlidingLateNoticeFullyAcceptedThenErrorFailsWrite(t *testing.T) {
	late := &fullAcceptErrWriter{err: errSentinelOutput}
	out := &failOnCallWriter{failAt: 100, err: errSentinelOutput}

	err := RunAggregateSliding(
		strings.NewReader(slidingLateNoticeScenarioInput()), 1000, 600, out, late,
	)

	var oe *OutputError
	if !errors.As(err, &oe) {
		t.Fatalf("expected *OutputError even though every notice byte was accepted, got %T: %v", err, err)
	}
	if oe.Line != slidingLateFailingLine {
		t.Errorf("OutputError.Line = %d, want %d (blank physical lines count)", oe.Line, slidingLateFailingLine)
	}
	if oe.Kind != "late-event notice" {
		t.Errorf("OutputError.Kind = %q, want %q", oe.Kind, "late-event notice")
	}
	wantDetail := "event time 999 below current watermark 1000"
	if oe.Detail != wantDetail {
		t.Errorf("OutputError.Detail = %q, want %q", oe.Detail, wantDetail)
	}
	if !strings.Contains(err.Error(), "event time 999") ||
		!strings.Contains(err.Error(), "watermark 1000") {
		t.Errorf("error %q must name the skipped event time 999 and the watermark 1000", err.Error())
	}
	if !errors.Is(err, errSentinelOutput) {
		t.Errorf("errors.Is must expose the recipient's original error, got %v", err)
	}
	if errors.Is(err, io.ErrShortWrite) {
		t.Errorf("a fully accepted write that errors must not be reclassified as a short write: %v", err)
	}
	var ie *InputError
	if errors.As(err, &ie) {
		t.Errorf("a notice write failure must not be reported as a record/input error: %v", err)
	}

	// The complete notice the recipient already accepted must stay exactly as
	// written, and it must be attempted exactly once: neither rewritten nor
	// resent after the failure.
	if got := late.got.Bytes(); !bytes.Equal(got, slidingLateWantNotice) {
		t.Errorf("accepted notice mismatch:\n got: %q\nwant: %q", got, slidingLateWantNotice)
	}
	if late.calls != 1 {
		t.Errorf("notice writer was called %d times, want exactly 1 (no rewrite)", late.calls)
	}

	// Previously fully written window results stay; after the failure no later
	// record is processed: line 7's watermark must not close [600,1600), the
	// open window is not emitted as a tail result, and line 8's malformed
	// record must not surface as an *InputError.
	if got := out.got.Bytes(); !bytes.Equal(got, slidingLateWantFirstWindow) {
		t.Errorf("window output mismatch:\n got: %q\nwant only the earlier result %q", got, slidingLateWantFirstWindow)
	}
	if out.calls != 1 {
		t.Errorf("window writer was called %d times, want exactly 1 (nothing after the failure)", out.calls)
	}
}

// TestSlidingLateNoticeFullAcceptErrorWinsOverCoReadFailure covers the read
// boundary condition: the Read that delivers the newline-terminated late
// record also returns a read failure. The record is complete (its newline
// arrived) so its notice is attempted first; when that fully accepted notice
// write fails, the write error is what surfaces -- the read failure learned
// from the same Read must not overwrite it.
func TestSlidingLateNoticeFullAcceptErrorWinsOverCoReadFailure(t *testing.T) {
	// Three newline-terminated lines; the third Read batch ends exactly on the
	// late record's newline and carries the upstream failure as well.
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":900,"value":7}`, // line 1
		`{"type":"watermark","time":1000}`,                // line 2 closes [0,1000)
		`{"type":"event","key":"k","time":999,"value":1}`, // line 3 complete and late
		``,
	}, "\n")
	wantNotice := []byte("line 3: late event time=999 below current watermark 1000, skipped\n")

	t.Run("notice bytes and read error in the same read", func(t *testing.T) {
		r := &failingAfterReader{
			data:   []byte(input),
			failAt: len(input),
			err:    errSentinelChunkedRead,
			// Whole batch delivered at once together with the error.
			errOnBoundary: true,
		}
		late := &fullAcceptErrWriter{err: errSentinelOutput}
		var out bytes.Buffer
		err := RunAggregateSliding(r, 1000, 600, &out, late)

		var oe *OutputError
		if !errors.As(err, &oe) || oe.Line != 3 || oe.Kind != "late-event notice" {
			t.Fatalf("expected late-notice *OutputError at line 3, got %T: %v", err, err)
		}
		if !errors.Is(err, errSentinelOutput) {
			t.Errorf("errors.Is must identify the write failure cause, got %v", err)
		}
		if errors.Is(err, errSentinelChunkedRead) {
			t.Errorf("the co-arriving read failure must not overwrite the write error: %v", err)
		}
		if errors.Is(err, io.ErrShortWrite) {
			t.Errorf("fully accepted notice must not be a short write: %v", err)
		}
		if got := late.got.Bytes(); !bytes.Equal(got, wantNotice) {
			t.Errorf("accepted notice mismatch:\n got: %q\nwant: %q", got, wantNotice)
		}
		if late.calls != 1 {
			t.Errorf("notice writer was called %d times, want 1", late.calls)
		}
		if got := out.Bytes(); !bytes.Equal(got, slidingLateWantFirstWindow) {
			t.Errorf("earlier window result must be retained:\n got: %q\nwant: %q", got, slidingLateWantFirstWindow)
		}
	})

	t.Run("one byte per read, error arrives with the newline", func(t *testing.T) {
		r := &failingAfterReader{
			data:          []byte(input),
			failAt:        len(input),
			err:           errSentinelChunkedRead,
			chunk:         1,
			errOnBoundary: true,
		}
		late := &fullAcceptErrWriter{err: errSentinelOutput}
		var out bytes.Buffer
		err := RunAggregateSliding(r, 1000, 600, &out, late)

		var oe *OutputError
		if !errors.As(err, &oe) || oe.Line != 3 || oe.Kind != "late-event notice" {
			t.Fatalf("expected late-notice *OutputError at line 3, got %T: %v", err, err)
		}
		if !errors.Is(err, errSentinelOutput) {
			t.Errorf("errors.Is must identify the write failure cause, got %v", err)
		}
		if errors.Is(err, errSentinelChunkedRead) {
			t.Errorf("the read failure must not mask the earlier notice write failure: %v", err)
		}
		if got := late.got.Bytes(); !bytes.Equal(got, wantNotice) {
			t.Errorf("accepted notice mismatch:\n got: %q\nwant: %q", got, wantNotice)
		}
		if got := out.Bytes(); !bytes.Equal(got, slidingLateWantFirstWindow) {
			t.Errorf("earlier window result must be retained:\n got: %q\nwant: %q", got, slidingLateWantFirstWindow)
		}
	})

	t.Run("control: healthy notice write lets the read failure surface", func(t *testing.T) {
		// Same failing batch, but the notice is written in full without error.
		// The skipped event still contributes nothing and the run then ends on
		// the reader's own failure (no [600,1600) result).
		r := &failingAfterReader{
			data:          []byte(input),
			failAt:        len(input),
			err:           errSentinelChunkedRead,
			errOnBoundary: true,
		}
		var out, late bytes.Buffer
		err := RunAggregateSliding(r, 1000, 600, &out, &late)
		if !errors.Is(err, errSentinelChunkedRead) {
			t.Fatalf("with a healthy notice writer the reader's error must surface, got %v", err)
		}
		if errors.Is(err, errSentinelOutput) {
			t.Fatalf("no write failure occurred, yet the sentinel is reachable: %v", err)
		}
		if got := late.Bytes(); !bytes.Equal(got, wantNotice) {
			t.Errorf("notice mismatch:\n got: %q\nwant: %q", got, wantNotice)
		}
		if got := out.Bytes(); !bytes.Equal(got, slidingLateWantFirstWindow) {
			t.Errorf("only the earlier window may be output:\n got: %q\nwant: %q", got, slidingLateWantFirstWindow)
		}
	})
}

// TestSlidingLateNoticeFullyWrittenWithoutErrorContinues is the compatible
// control behavior: when the complete notice is accepted with no error,
// processing continues exactly as before. The late event adds neither a count
// nor a sum to the still-open overlapping window; later watermarks close it on
// the legitimate events' contributions alone.
func TestSlidingLateNoticeFullyWrittenWithoutErrorContinues(t *testing.T) {
	input := strings.Join([]string{
		``, // line 1 blank
		`{"type":"event","key":"k","time":900,"value":7}`, // line 2
		`{"type":"watermark","time":1000}`,                // line 3 closes [0,1000)
		``,                                                // line 4 blank
		`{"type":"event","key":"k","time":999,"value":1}`, // line 5 late, skipped
		`{"type":"event","key":"k","time":1000,"value":3}`,
		`{"type":"watermark","time":1600}`, // line 7 closes [600,1600)
	}, "\n")

	var out, late bytes.Buffer
	if err := RunAggregateSliding(strings.NewReader(input), 1000, 600, &out, &late); err != nil {
		t.Fatalf("a fully written notice must not fail the run: %v", err)
	}
	if got := late.Bytes(); !bytes.Equal(got, slidingLateWantNotice) {
		t.Errorf("late notice mismatch:\n got: %q\nwant: %q", got, slidingLateWantNotice)
	}
	want := strings.Join([]string{
		`{"key":"k","start":0,"end":1000,"count":1,"sum":7}`,
		// t=900 and t=1000 feed [600,1600); the late t=999 must add neither a
		// count nor its value: count stays 2 / sum 10, never 3 / 11.
		`{"key":"k","start":600,"end":1600,"count":2,"sum":10}`,
		``,
	}, "\n")
	if out.String() != want {
		t.Errorf("window output mismatch:\n got: %q\nwant: %q", out.String(), want)
	}
}
