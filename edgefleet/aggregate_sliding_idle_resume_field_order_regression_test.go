package edgefleet

import (
	"errors"
	"strings"
	"testing"
)

// Regression safeguards for the ordering between record content validation
// and the idle-partition resume judgement in partitioned sliding mode. The
// history is slidingIdleResumeFixtureLines (window 1000, slide 600, two
// partitions, key "sensor-a"): partition 0's event at 700/2 lands in
// [0,1000) and [600,1600), partition 1's event at 1000/3 lands only in
// [600,1600); after watermarks 2000 on partition 0 and 1000 on partition 1,
// [0,1000) is already closed (count 1, sum 2) while [600,1600) stays open.
// Partition 0 then declares idle on physical line 6: the effective
// watermark stays 1000 (partition 1 is active there), but partition 0's own
// old watermark remains 2000.
//
// The next watermark record (physical line 8) is the partition's resume
// record, and the ordering under protection is:
//
//  1. The "time" field must first parse as a non-negative signed 64-bit
//     integer -- missing, string-typed, negative or out-of-range is the
//     record's first error even when the same record's "partition" field is
//     also wrong, regardless of the two fields' textual order in the JSON.
//  2. Only after time is legal may "partition" be read and range-checked;
//     a missing, string-typed or out-of-range partition is the reported
//     problem even when that legal time (1600) is below the partition's own
//     old watermark 2000.
//  3. Only with both fields legal do the resume bounds apply, in their
//     existing order: the partition's own previous watermark first (so 1600
//     fails against 2000) and never the effective watermark 1000.
//
// Every failure is the existing *InputError naming physical line 8 (blank
// physical lines 5 and 7 count) with a distinguishable reason. It must not
// become a late-event notice, the window already emitted stays exactly as
// published, and the dead records after it (a watermark that would close
// [600,1600), then an event that would log a late notice) take no effect.

// slidingIdleResumeDeadSuffix are the records that must stay completely dead
// after a fatal resume-record error on physical line 8:
//
//	line 9: partition 1 watermark 1600 -- would close [600,1600) if reached
//	line 10: partition 1 event time=999 -- would log a late notice if reached
func slidingIdleResumeDeadSuffix() []string {
	return []string{
		`{"type":"watermark","time":1600,"partition":1}`,
		`{"type":"event","key":"sensor-a","time":999,"value":1,"partition":1}`,
	}
}

var slidingIdleResumeWantFirstWindow = `{"key":"sensor-a","start":0,"end":1000,"count":1,"sum":2}` + "\n"

// assertResumeFieldError checks the common contract of every malformed resume
// record: an *InputError (never an *OutputError) on physical line 8, a reason
// naming wantField and stating wantCause, with no trace of the other field,
// the resume-bound comparisons, a late-event skip or window accumulation;
// stderr stays empty and stdout keeps only the window closed before line 8.
func assertResumeFieldError(t *testing.T, stdout, stderr string, err error, wantField, wantCause string) {
	t.Helper()
	if err == nil {
		t.Fatalf("a malformed resume record must fail fatally; got nil, stdout=%q stderr=%q", stdout, stderr)
	}
	var inputErr *InputError
	if !errors.As(err, &inputErr) {
		t.Fatalf("want *InputError, got %T: %v", err, err)
	}
	var outputErr *OutputError
	if errors.As(err, &outputErr) {
		t.Fatalf("a malformed record must not be reported as an output failure: %v", err)
	}
	if inputErr.Line != 8 {
		t.Errorf("InputError.Line = %d, want 8 (blank physical lines 5 and 7 count)", inputErr.Line)
	}
	if !strings.Contains(inputErr.Reason, wantField) {
		t.Errorf("reason %q must name field %s", inputErr.Reason, wantField)
	}
	if !strings.Contains(inputErr.Reason, wantCause) {
		t.Errorf("reason %q must state the concrete cause %q", inputErr.Reason, wantCause)
	}
	// Field validation happens before any idle-resume state is consulted:
	// neither resume bound may appear in the reason.
	for _, sub := range []string{"resume watermark", "previous watermark", "effective watermark"} {
		if strings.Contains(inputErr.Reason, sub) {
			t.Errorf("a field error must be reported before the resume bounds, reason %q mentions %q", inputErr.Reason, sub)
		}
	}
	if strings.Contains(err.Error(), "late event") {
		t.Errorf("an invalid resume record must not be described as a late-event skip: %q", err.Error())
	}
	if stderr != "" {
		t.Errorf("a rejected resume record and its dead suffix must write no late-event notice, got %q", stderr)
	}
	if stdout != slidingIdleResumeWantFirstWindow {
		t.Errorf("window output mismatch:\n got: %q\nwant: %q", stdout, slidingIdleResumeWantFirstWindow)
	}
}

// TestSlidingIdleResumeInvalidTimeFailsBeforePartition covers every invalid
// time shape on a resume record. Each record also carries an invalid
// partition (out of range or string-typed) to prove the time field is
// settled first: missing time, a string time, a negative time and times
// beyond either end of signed 64-bit range are each the concrete first
// error on physical line 8.
func TestSlidingIdleResumeInvalidTimeFailsBeforePartition(t *testing.T) {
	cases := []struct {
		name      string
		record    string
		wantField string
		wantCause string
	}{
		{
			name:      "time missing while partition is out of range",
			record:    `{"type":"watermark","partition":9}`,
			wantField: `"time"`,
			wantCause: `missing required integer field "time"`,
		},
		{
			name:      "time written as string while partition is also a string",
			record:    `{"type":"watermark","time":"1600","partition":"0"}`,
			wantField: `"time"`,
			wantCause: `field "time" must be a signed 64-bit integer, got "1600"`,
		},
		{
			name:      "time negative while partition is out of range",
			record:    `{"type":"watermark","time":-1,"partition":9}`,
			wantField: `"time"`,
			wantCause: `field "time" must be a non-negative integer, got -1`,
		},
		{
			name:      "time above signed 64-bit range while partition is out of range",
			record:    `{"type":"watermark","time":9223372036854775808,"partition":9}`,
			wantField: `"time"`,
			wantCause: `field "time" integer out of signed 64-bit range: 9223372036854775808`,
		},
		{
			name:      "time below signed 64-bit range while partition is out of range",
			record:    `{"type":"watermark","time":-9223372036854775809,"partition":9}`,
			wantField: `"time"`,
			wantCause: `field "time" integer out of signed 64-bit range: -9223372036854775809`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lines := append(slidingIdleResumeFixtureLines(), tc.record)
			lines = append(lines, slidingIdleResumeDeadSuffix()...)
			stdout, stderr, err := runPartitionedSliding(t, strings.Join(lines, "\n"), 1000, 600, 2)
			assertResumeFieldError(t, stdout, stderr, err, tc.wantField, tc.wantCause)
			// The other field is broken too; it must not be the named one.
			if inputErr := inputErrorFrom(t, err); strings.Contains(inputErr.Reason, `"partition"`) {
				t.Errorf("time must be checked before partition, reason = %q", inputErr.Reason)
			}
		})
	}
}

// TestSlidingIdleResumeFieldOrderDoesNotChangeFirstError proves the first
// error is fixed by validation order (time, then partition, then resume
// bounds), not by the order the two fields happen to be written in the JSON.
// Swapping the textual order must yield the identical physical line and
// reason, both when time is the broken field and when a legal-but-low time
// accompanies a broken partition.
func TestSlidingIdleResumeFieldOrderDoesNotChangeFirstError(t *testing.T) {
	cases := []struct {
		name      string
		timeFirst string
		partFirst string
		wantField string
		wantCause string
	}{
		{
			name:      "string time and string partition",
			timeFirst: `{"type":"watermark","time":"1600","partition":"0"}`,
			partFirst: `{"type":"watermark","partition":"0","time":"1600"}`,
			wantField: `"time"`,
			wantCause: `field "time" must be a signed 64-bit integer, got "1600"`,
		},
		{
			name:      "negative out-of-range time and out-of-range partition",
			timeFirst: `{"type":"watermark","time":-1,"partition":9}`,
			partFirst: `{"type":"watermark","partition":9,"time":-1}`,
			wantField: `"time"`,
			wantCause: `field "time" must be a non-negative integer, got -1`,
		},
		{
			name:      "legal low time with string partition",
			timeFirst: `{"type":"watermark","time":1600,"partition":"0"}`,
			partFirst: `{"type":"watermark","partition":"0","time":1600}`,
			wantField: `"partition"`,
			wantCause: `field "partition" must be a signed 64-bit integer, got "0"`,
		},
		{
			name:      "legal low time with out-of-range partition",
			timeFirst: `{"type":"watermark","time":1600,"partition":9}`,
			partFirst: `{"type":"watermark","partition":9,"time":1600}`,
			wantField: `"partition"`,
			wantCause: `field "partition" must be an integer in range [0,2), got 9`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			first := runResumeFieldError(t, tc.timeFirst)
			second := runResumeFieldError(t, tc.partFirst)
			if first.Line != 8 || second.Line != 8 {
				t.Errorf("line numbers = %d and %d, want 8 for both field orders", first.Line, second.Line)
			}
			if first.Reason != second.Reason {
				t.Errorf("field order changed the first error:\n time-first: %q\npart-first: %q", first.Reason, second.Reason)
			}
			if !strings.Contains(first.Reason, tc.wantField) {
				t.Errorf("reason %q must name %s", first.Reason, tc.wantField)
			}
			if !strings.Contains(first.Reason, tc.wantCause) {
				t.Errorf("reason %q must state %q", first.Reason, tc.wantCause)
			}
		})
	}
}

// TestSlidingIdleResumeInvalidPartitionAfterLegalTime covers the second
// stage: once time parses legally, partition problems are reported even
// though the time 1600 is below partition 0's own old watermark 2000. The
// resume-bound comparison must not run first and "rescue" the record into a
// resume-watermark error.
func TestSlidingIdleResumeInvalidPartitionAfterLegalTime(t *testing.T) {
	cases := []struct {
		name      string
		record    string
		wantCause string
	}{
		{
			name:      "partition missing",
			record:    `{"type":"watermark","time":1600}`,
			wantCause: `missing required integer field "partition"`,
		},
		{
			name:      "partition written as string",
			record:    `{"type":"watermark","time":1600,"partition":"0"}`,
			wantCause: `field "partition" must be a signed 64-bit integer, got "0"`,
		},
		{
			name:      "partition above configured range",
			record:    `{"type":"watermark","time":1600,"partition":9}`,
			wantCause: `field "partition" must be an integer in range [0,2), got 9`,
		},
		{
			name:      "partition negative",
			record:    `{"type":"watermark","time":1600,"partition":-1}`,
			wantCause: `field "partition" must be an integer in range [0,2), got -1`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lines := append(slidingIdleResumeFixtureLines(), tc.record)
			lines = append(lines, slidingIdleResumeDeadSuffix()...)
			stdout, stderr, err := runPartitionedSliding(t, strings.Join(lines, "\n"), 1000, 600, 2)
			assertResumeFieldError(t, stdout, stderr, err, `"partition"`, tc.wantCause)
			if inputErr := inputErrorFrom(t, err); strings.Contains(inputErr.Reason, `"time"`) {
				t.Errorf("time 1600 is legal and must not be blamed: %q", inputErr.Reason)
			}
		})
	}
}

// TestSlidingIdleResumeLegalFieldsFailsAgainstOwnOldWatermark is the third
// stage: with both fields legal the resume bounds apply, and the partition's
// own previous watermark (2000) is the one violated by time 1600 -- not the
// effective watermark 1000, which 1600 already clears. This is checked with
// both JSON field orders; the dead suffix must neither close [600,1600) nor
// produce a late-event notice.
func TestSlidingIdleResumeLegalFieldsFailsAgainstOwnOldWatermark(t *testing.T) {
	for _, record := range []string{
		`{"type":"watermark","time":1600,"partition":0}`,
		`{"type":"watermark","partition":0,"time":1600}`,
	} {
		lines := append(slidingIdleResumeFixtureLines(), record)
		lines = append(lines, slidingIdleResumeDeadSuffix()...)
		stdout, stderr, err := runPartitionedSliding(t, strings.Join(lines, "\n"), 1000, 600, 2)
		if err == nil {
			t.Fatalf("resume at 1600 must fail against partition 0's old watermark 2000; got nil, stdout=%q", stdout)
		}
		var inputErr *InputError
		if !errors.As(err, &inputErr) {
			t.Fatalf("want *InputError, got %T: %v", err, err)
		}
		if inputErr.Line != 8 {
			t.Errorf("InputError.Line = %d, want 8", inputErr.Line)
		}
		for _, sub := range []string{
			"resume watermark 1600",
			"partition 0",
			"previous watermark 2000",
		} {
			if !strings.Contains(inputErr.Reason, sub) {
				t.Errorf("reason %q must contain %q", inputErr.Reason, sub)
			}
		}
		if strings.Contains(inputErr.Reason, "effective watermark 1000") {
			t.Errorf("the violated bound must be the partition's own old watermark, not the effective 1000: %q", inputErr.Reason)
		}
		if stderr != "" {
			t.Errorf("no late-event notice may be written, got %q", stderr)
		}
		if stdout != slidingIdleResumeWantFirstWindow {
			t.Errorf("the closed window stays and [600,1600) must not be flushed by the dead suffix:\n got: %q\nwant: %q", stdout, slidingIdleResumeWantFirstWindow)
		}
	}
}

// TestSlidingIdleResumeAtOldWatermarkWaitsForOtherPartition pins the legal
// boundary: resuming partition 0 exactly at its own old watermark 2000
// succeeds while partition 1 still pins the effective watermark at 1000, so
// the resume itself emits nothing -- [600,1600) must not close early and the
// already closed [0,1000) must not be re-emitted. The window closes only
// once partition 1 advances to 1600, merging the two pre-idle events
// (count 2, sum 5, no partition field); a later fast-partition watermark
// must not re-emit closed windows or flush still-open ones.
func TestSlidingIdleResumeAtOldWatermarkWaitsForOtherPartition(t *testing.T) {
	// Truncated run: the resume record is the final record.
	hold := append(slidingIdleResumeFixtureLines(),
		`{"type":"watermark","time":2000,"partition":0}`, // line 8: resume at the old watermark
	)
	stdout, stderr, err := runPartitionedSliding(t, strings.Join(hold, "\n"), 1000, 600, 2)
	if err != nil {
		t.Fatalf("resume exactly at the partition's old watermark must succeed: %v", err)
	}
	if stderr != "" {
		t.Fatalf("resume at the boundary must produce no notice, got %q", stderr)
	}
	if stdout != slidingIdleResumeWantFirstWindow {
		t.Fatalf("partition 1 still at 1000 must hold [600,1600) open after the resume:\n got: %q\nwant: %q", stdout, slidingIdleResumeWantFirstWindow)
	}

	// Full run: partition 1 catches up, then partition 0 jumps ahead.
	lines := append(hold,
		`{"type":"watermark","time":1600,"partition":1}`, // line 9: effective = 1600, closes [600,1600)
		`{"type":"watermark","time":5000,"partition":0}`, // line 10: min stays 1600; nothing new
	)
	stdout, stderr, err = runPartitionedSliding(t, strings.Join(lines, "\n"), 1000, 600, 2)
	if err != nil {
		t.Fatalf("the catch-up sequence must succeed: %v", err)
	}
	if stderr != "" {
		t.Fatalf("the boundary sequence must produce no late notice, got %q", stderr)
	}
	want := strings.Join([]string{
		`{"key":"sensor-a","start":0,"end":1000,"count":1,"sum":2}`,
		// Only the two pre-idle events: 700/2 belongs to both windows,
		// 1000/3 belongs only to this one. No post-resume event is sent.
		`{"key":"sensor-a","start":600,"end":1600,"count":2,"sum":5}`,
		``,
	}, "\n")
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
	// Each closed window appears exactly once; later windows stay open.
	if n := strings.Count(stdout, `"start":0,"end":1000`); n != 1 {
		t.Errorf("[0,1000) emitted %d times, want exactly 1", n)
	}
	if n := strings.Count(stdout, `"start":600,"end":1600`); n != 1 {
		t.Errorf("[600,1600) emitted %d times, want exactly 1", n)
	}
	if strings.Contains(stdout, `"start":1200`) {
		t.Errorf("[1200,2200) is still open at end of input and must never be emitted: %q", stdout)
	}
	got := decodeAggregateResults(t, stdout)
	wantRows := []AggregateResult{
		{Key: "sensor-a", Start: 0, End: 1000, Count: 1, Sum: 2},
		{Key: "sensor-a", Start: 600, End: 1600, Count: 2, Sum: 5},
	}
	if len(got) != len(wantRows) {
		t.Fatalf("got %d rows, want %d: %v", len(got), len(wantRows), got)
	}
	for i := range wantRows {
		if got[i] != wantRows[i] {
			t.Errorf("row %d = %+v, want %+v", i, got[i], wantRows[i])
		}
	}
}

// runResumeFieldError runs the shared history plus the given line-8 resume
// record (no dead suffix needed when only the error is compared) and returns
// the *InputError, failing the test on any other outcome.
func runResumeFieldError(t *testing.T, record string) *InputError {
	t.Helper()
	lines := append(slidingIdleResumeFixtureLines(), record)
	_, _, err := runPartitionedSliding(t, strings.Join(lines, "\n"), 1000, 600, 2)
	return inputErrorFrom(t, err)
}

// inputErrorFrom extracts the *InputError from err, failing the test when the
// run did not produce one.
func inputErrorFrom(t *testing.T, err error) *InputError {
	t.Helper()
	if err == nil {
		t.Fatalf("expected an *InputError, got nil")
	}
	var inputErr *InputError
	if !errors.As(err, &inputErr) {
		t.Fatalf("want *InputError, got %T: %v", err, err)
	}
	return inputErr
}
