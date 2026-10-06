package edgefleet

import (
	"errors"
	"strings"
	"testing"
)

// Regression safeguards for the ordering between a resume watermark record's
// own content validation and the idle-resume lower bounds, in partitioned
// sliding mode. The history is slidingIdleResumeFixtureLines (window 1000,
// slide 600, two partitions): at physical line 8 partition 0 is idle, its own
// old watermark is retained at 2000 while the effective watermark stays 1000,
// [0,1000) has already been emitted (count 1, sum 2) and [600,1600) is still
// open. A resume record on line 8 therefore has to clear three stages in
// order:
//
//  1. "time" must be present and be a non-negative signed 64-bit integer; a
//     problem there is the first error even when "partition" is also wrong,
//     regardless of which field is written first in the JSON.
//  2. Only after time is legal may "partition" be read and range-checked; a
//     partition problem is reported as the field problem even when the legal
//     time is already below the partition's own old watermark 2000.
//  3. Only with both fields legal do the resume bounds apply -- the
//     partition's own previous watermark first, then the effective
//     watermark. That last ordering for legal records is pinned by
//     TestAggregateSlidingIdleResumeBelowOwnOldWatermark.
//
// Every rejected record is the existing *InputError naming physical line 8
// (blank lines 5 and 7 count). Records after it are never processed: the
// trailing watermark cannot close [600,1600) and the trailing event cannot
// write a late-event notice, so the already published [0,1000) result is the
// complete output and the late log stays empty.

// idleResumeFieldFailureSuffix are the records that must be dead after the
// line-8 failure:
//
//	line 9: partition 1 advancing to 1600 would close [600,1600) if reached.
//	line 10: a time=999 event on partition 1 would be skipped with a late
//	        notice against the effective watermark 1000 if reached.
//
// Neither a window result nor a late notice may therefore follow a fatal
// line-8 field error.
func idleResumeFieldFailureSuffix() []string {
	return []string{
		`{"type":"watermark","time":1600,"partition":1}`,                       // line 9
		`{"type":"event","key":"sensor-a","time":999,"value":1,"partition":1}`, // line 10
	}
}

var idleResumeWantFirstWindow = `{"key":"sensor-a","start":0,"end":1000,"count":1,"sum":2}` + "\n"

// TestSlidingIdleResumeInvalidTimeReportedBeforePartition exercises every
// illegal "time" shape on the resume record -- missing, string, negative and
// outside the signed 64-bit range -- each with "partition" also wrong. The
// time problem is the single reported cause, and swapping the order in which
// the two fields are written in the JSON must not change that first error.
func TestSlidingIdleResumeInvalidTimeReportedBeforePartition(t *testing.T) {
	cases := []struct {
		name      string
		pair      string // cases sharing a pair ID must report identical reasons
		record    string
		wantCause string
	}{
		{
			name:      "time missing with partition missing as well",
			record:    `{"type":"watermark"}`,
			wantCause: `missing required integer field "time"`,
		},
		{
			name:      "time missing even though partition is written first as a string",
			record:    `{"type":"watermark","partition":"0"}`,
			wantCause: `missing required integer field "time"`,
		},
		{
			name:      "time as a string written before the bad partition",
			pair:      "time string",
			record:    `{"type":"watermark","time":"1600","partition":"0"}`,
			wantCause: `field "time" must be a signed 64-bit integer, got "1600"`,
		},
		{
			name:      "time as a string written after the bad partition",
			pair:      "time string",
			record:    `{"type":"watermark","partition":"0","time":"1600"}`,
			wantCause: `field "time" must be a signed 64-bit integer, got "1600"`,
		},
		{
			name:      "negative time written before the out-of-range partition",
			pair:      "time negative",
			record:    `{"type":"watermark","time":-1,"partition":9}`,
			wantCause: `field "time" must be a non-negative integer, got -1`,
		},
		{
			name:      "negative time written after the out-of-range partition",
			pair:      "time negative",
			record:    `{"type":"watermark","partition":9,"time":-1}`,
			wantCause: `field "time" must be a non-negative integer, got -1`,
		},
		{
			name:      "time above signed 64-bit range written before the bad partition",
			pair:      "time above range",
			record:    `{"type":"watermark","time":9223372036854775808,"partition":9}`,
			wantCause: `field "time" integer out of signed 64-bit range: 9223372036854775808`,
		},
		{
			name:      "time above signed 64-bit range written after the bad partition",
			pair:      "time above range",
			record:    `{"type":"watermark","partition":9,"time":9223372036854775808}`,
			wantCause: `field "time" integer out of signed 64-bit range: 9223372036854775808`,
		},
		{
			name:      "time below signed 64-bit range written before the bad partition",
			pair:      "time below range",
			record:    `{"type":"watermark","time":-9223372036854775809,"partition":"0"}`,
			wantCause: `field "time" integer out of signed 64-bit range: -9223372036854775809`,
		},
		{
			name:      "time below signed 64-bit range written after the bad partition",
			pair:      "time below range",
			record:    `{"type":"watermark","partition":"0","time":-9223372036854775809}`,
			wantCause: `field "time" integer out of signed 64-bit range: -9223372036854775809`,
		},
	}

	// pairReasons[pair] maps each member case name to its reported reason; the
	// two JSON field orderings of one record shape must be indistinguishable.
	pairReasons := map[string]map[string]string{}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lines := append(slidingIdleResumeFixtureLines(), tc.record)
			lines = append(lines, idleResumeFieldFailureSuffix()...)
			stdout, stderr, err := runPartitionedSliding(t, strings.Join(lines, "\n"), 1000, 600, 2)
			var inputErr *InputError
			if !errors.As(err, &inputErr) {
				t.Fatalf("want *InputError, got %T: %v", err, err)
			}
			if inputErr.Line != 8 {
				t.Errorf("InputError.Line = %d, want 8 (blank physical lines count)", inputErr.Line)
			}
			if !strings.Contains(inputErr.Reason, tc.wantCause) {
				t.Errorf("reason = %q, want concrete cause %q", inputErr.Reason, tc.wantCause)
			}
			// A time problem must name time and never fall through to the
			// partition field or to either resume bound.
			if !strings.Contains(inputErr.Reason, `"time"`) {
				t.Errorf("reason %q must name field \"time\"", inputErr.Reason)
			}
			for _, banned := range []string{`"partition"`, "resume watermark", "previous watermark", "effective watermark"} {
				if strings.Contains(inputErr.Reason, banned) {
					t.Errorf("the time error must be reported first; reason %q must not mention %q", inputErr.Reason, banned)
				}
			}
			// The failed record and the records after it may neither close the
			// pending window nor write a late-event notice; earlier output stays.
			if stderr != "" {
				t.Errorf("no late-event notice is allowed when the resume record fails, got %q", stderr)
			}
			if stdout != idleResumeWantFirstWindow {
				t.Errorf("window output mismatch:\n got: %q\nwant: %q", stdout, idleResumeWantFirstWindow)
			}
			if tc.pair != "" {
				if pairReasons[tc.pair] == nil {
					pairReasons[tc.pair] = map[string]string{}
				}
				pairReasons[tc.pair][tc.name] = inputErr.Reason
			}
		})
	}
	for pair, members := range pairReasons {
		if len(members) != 2 {
			t.Errorf("internal test setup: pair %q has %d orderings, want 2", pair, len(members))
			continue
		}
		var first string
		for name, reason := range members {
			if first == "" {
				first = reason
				continue
			}
			if reason != first {
				t.Errorf("pair %q: JSON field order changed the first error:\n%q\nvs\n%q", pair, first, reason)
			}
			_ = name
		}
	}
}

// TestSlidingIdleResumeInvalidPartitionReportedBeforeResumeBounds covers the
// second stage: time is a legal 1600 -- which is already below partition 0's
// own old watermark 2000 -- so any partition problem must be reported as the
// field problem (missing, string, outside [0,2)) rather than as the
// below-old-watermark resume failure. Field order in JSON is again irrelevant.
func TestSlidingIdleResumeInvalidPartitionReportedBeforeResumeBounds(t *testing.T) {
	cases := []struct {
		name      string
		pair      string
		record    string
		wantCause string
	}{
		{
			name:      "partition missing with legal time",
			record:    `{"type":"watermark","time":1600}`,
			wantCause: `missing required integer field "partition"`,
		},
		{
			name:      "partition as a string written after time",
			pair:      "partition string",
			record:    `{"type":"watermark","time":1600,"partition":"0"}`,
			wantCause: `field "partition" must be a signed 64-bit integer, got "0"`,
		},
		{
			name:      "partition as a string written before time",
			pair:      "partition string",
			record:    `{"type":"watermark","partition":"0","time":1600}`,
			wantCause: `field "partition" must be a signed 64-bit integer, got "0"`,
		},
		{
			name:      "partition above range written after time",
			pair:      "partition above range",
			record:    `{"type":"watermark","time":1600,"partition":9}`,
			wantCause: `field "partition" must be an integer in range [0,2), got 9`,
		},
		{
			name:      "partition above range written before time",
			pair:      "partition above range",
			record:    `{"type":"watermark","partition":9,"time":1600}`,
			wantCause: `field "partition" must be an integer in range [0,2), got 9`,
		},
		{
			name:      "partition just past the upper boundary written after time",
			pair:      "partition at boundary",
			record:    `{"type":"watermark","time":1600,"partition":2}`,
			wantCause: `field "partition" must be an integer in range [0,2), got 2`,
		},
		{
			name:      "partition just past the upper boundary written before time",
			pair:      "partition at boundary",
			record:    `{"type":"watermark","partition":2,"time":1600}`,
			wantCause: `field "partition" must be an integer in range [0,2), got 2`,
		},
		{
			name:      "negative partition written after time",
			pair:      "partition negative",
			record:    `{"type":"watermark","time":1600,"partition":-1}`,
			wantCause: `field "partition" must be an integer in range [0,2), got -1`,
		},
		{
			name:      "negative partition written before time",
			pair:      "partition negative",
			record:    `{"type":"watermark","partition":-1,"time":1600}`,
			wantCause: `field "partition" must be an integer in range [0,2), got -1`,
		},
	}

	pairReasons := map[string]map[string]string{}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lines := append(slidingIdleResumeFixtureLines(), tc.record)
			lines = append(lines, idleResumeFieldFailureSuffix()...)
			stdout, stderr, err := runPartitionedSliding(t, strings.Join(lines, "\n"), 1000, 600, 2)

			var inputErr *InputError
			if !errors.As(err, &inputErr) {
				t.Fatalf("want *InputError, got %T: %v", err, err)
			}
			if inputErr.Line != 8 {
				t.Errorf("InputError.Line = %d, want 8 (blank physical lines count)", inputErr.Line)
			}
			if !strings.Contains(inputErr.Reason, tc.wantCause) {
				t.Errorf("reason = %q, want concrete cause %q", inputErr.Reason, tc.wantCause)
			}
			// Time 1600 is below the partition's own old watermark 2000; the
			// resume comparisons must not run until partition is legal, so the
			// bound wording must never appear.
			for _, banned := range []string{"resume watermark", "previous watermark 2000", "effective watermark 1000", "moved backwards"} {
				if strings.Contains(inputErr.Reason, banned) || strings.Contains(err.Error(), banned) {
					t.Errorf("the partition field error must precede resume-bound checks; error %q must not mention %q", err.Error(), banned)
				}
			}
			if stderr != "" {
				t.Errorf("a rejected resume record and the records after it must produce no late-event notice, got %q", stderr)
			}
			if stdout != idleResumeWantFirstWindow {
				t.Errorf("window output mismatch:\n got: %q\nwant: %q", stdout, idleResumeWantFirstWindow)
			}
			if tc.pair != "" {
				if pairReasons[tc.pair] == nil {
					pairReasons[tc.pair] = map[string]string{}
				}
				pairReasons[tc.pair][tc.name] = inputErr.Reason
			}
		})
	}
	for pair, members := range pairReasons {
		if len(members) != 2 {
			t.Errorf("internal test setup: pair %q has %d orderings, want 2", pair, len(members))
			continue
		}
		var first string
		for _, reason := range members {
			if first == "" {
				first = reason
				continue
			}
			if reason != first {
				t.Errorf("pair %q: JSON field order changed the first error:\n%q\nvs\n%q", pair, first, reason)
			}
		}
	}
}

// TestSlidingIdleResumeAtBoundaryThenOtherPartitionClosesMergedWindow covers
// the legal boundary and the preserved aggregation: resuming partition 0
// exactly at its old watermark 2000 succeeds but emits nothing while
// partition 1 still pins the effective watermark at 1000. Only once
// partition 1 advances to 1600 does [600,1600) close with the merged
// pre-idle contributions (count 2, sum 5); the earlier [0,1000) result is
// not repeated, and no other window appears.
func TestSlidingIdleResumeAtBoundaryThenOtherPartitionClosesMergedWindow(t *testing.T) {
	lines := append(slidingIdleResumeFixtureLines(),
		`{"type":"watermark","time":2000,"partition":0}`, // line 8: resume at the old watermark; p1 keeps effective 1000
		`{"type":"watermark","time":1600,"partition":1}`, // line 9: effective = 1600, closes [600,1600)
	)
	stdout, stderr, err := runPartitionedSliding(t, strings.Join(lines, "\n"), 1000, 600, 2)
	if err != nil {
		t.Fatalf("the boundary resume at the partition's own old watermark must succeed: %v", err)
	}
	if stderr != "" {
		t.Fatalf("the normal resume/close path must produce no late-event notice, got %q", stderr)
	}
	want := strings.Join([]string{
		`{"key":"sensor-a","start":0,"end":1000,"count":1,"sum":2}`,
		`{"key":"sensor-a","start":600,"end":1600,"count":2,"sum":5}`,
		``,
	}, "\n")
	if stdout != want {
		t.Fatalf("window output mismatch:\n got: %q\nwant: %q", stdout, want)
	}

	// The resume while partition 1 sat at 1000 must not have closed
	// [600,1600) early (it would otherwise be missing or duplicated now), and
	// the already closed window must never appear a second time.
	if n := strings.Count(stdout, `"start":0,"end":1000`); n != 1 {
		t.Errorf("[0,1000) emitted %d times, want exactly 1", n)
	}
	if n := strings.Count(stdout, `"start":600,"end":1600`); n != 1 {
		t.Errorf("[600,1600) emitted %d times, want exactly 1", n)
	}
	if strings.Contains(stdout, `"start":1200`) {
		t.Errorf("no event belongs to a later sliding window, got unexpected row: %q", stdout)
	}

	// Pin the merged totals as structured values, including the absence of any
	// partition field on output.
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
