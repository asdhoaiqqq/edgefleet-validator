package edgefleet

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"testing"
)

// These tests cover the optional still-open window cap (--max-open-windows at
// the command line): a bound on how many not-yet-closed (key, window
// interval) aggregate records are retained when the watermark stalls and the
// keys keep changing. The unit counted is one decoded key paired with one
// window [start,end) -- never an event record and never a partition. The cap
// is opt-in: zero reproduces the unlimited behavior. Admission of one legal,
// non-late event is atomic across all of its containing sliding windows
// (landing exactly on the cap is fine; exceeding it rejects the whole line),
// and a slot is released only once its window has closed and been written in
// full.

// runMaxOpen drives the capped sliding (and optionally partitioned) library
// entry point end to end and returns the window output, the late-notice
// output and the run error.
func runMaxOpen(t *testing.T, input string, windowMillis, slideMillis, partitions, maxOpenWindows int64) (string, string, error) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	var err error
	if partitions > 0 {
		err = RunAggregatePartitionedSlidingWithMaxOpenWindows(
			strings.NewReader(input), windowMillis, slideMillis, partitions, maxOpenWindows, &stdout, &stderr)
	} else {
		err = RunAggregateSlidingWithMaxOpenWindows(
			strings.NewReader(input), windowMillis, slideMillis, maxOpenWindows, &stdout, &stderr)
	}
	return stdout.String(), stderr.String(), err
}

// assertInputError fails unless err is an *InputError at the given physical
// line whose reason contains every wanted substring.
func assertInputError(t *testing.T, err error, line int, wantSubstrings ...string) *InputError {
	t.Helper()
	if err == nil {
		t.Fatalf("expected an *InputError at line %d, got nil", line)
	}
	inputErr, ok := err.(*InputError)
	if !ok {
		t.Fatalf("expected *InputError, got %T: %v", err, err)
	}
	if inputErr.Line != line {
		t.Errorf("error line = %d, want %d", inputErr.Line, line)
	}
	for _, want := range wantSubstrings {
		if !strings.Contains(inputErr.Reason, want) {
			t.Errorf("reason = %q, want it to contain %q", inputErr.Reason, want)
		}
	}
	return inputErr
}

// Zero means "omitted" and stays unlimited, every positive value up to
// math.MaxInt64 is sound (the cap is compared with a map size, so it never has
// to divide another parameter), and a negative value is the one rejected
// spelling, reported through the shared *AggregateParamError on the
// max-open-windows field before input is read.
func TestValidateMaxOpenWindows(t *testing.T) {
	for _, v := range []int64{0, 1, 2, math.MaxInt64} {
		if err := ValidateMaxOpenWindows(v); err != nil {
			t.Errorf("ValidateMaxOpenWindows(%d) = %v, want nil", v, err)
		}
	}
	for _, v := range []int64{-1, -100, math.MinInt64} {
		err := ValidateMaxOpenWindows(v)
		var pe *AggregateParamError
		if !errors.As(err, &pe) {
			t.Fatalf("ValidateMaxOpenWindows(%d) = %v, want *AggregateParamError", v, err)
		}
		if pe.Field != AggregateParamMaxOpenWindows || pe.Kind != AggregateParamNotPositive || pe.Value != v {
			t.Fatalf("error fields = (field=%s kind=%s value=%d), want (%s %s %d)",
				pe.Field, pe.Kind, pe.Value, AggregateParamMaxOpenWindows, AggregateParamNotPositive, v)
		}
		if !strings.Contains(err.Error(), "max open windows") {
			t.Fatalf("error text = %q, want it to name the max open windows parameter", err.Error())
		}
	}
}

// A negative cap is rejected before the reader is consulted: even a stream
// whose first line is malformed, and an empty stream alike, report the
// parameter problem, never a record error or a successful run.
func TestMaxOpenWindowsNegativeRejectedBeforeInput(t *testing.T) {
	cases := []struct {
		name  string
		input string
	}{
		{"empty", ""},
		{"malformed first record", `{"type":"event",broken` + "\n"},
		{"window-closing input", strings.Join([]string{
			`{"type":"event","key":"k","time":100,"value":1}`,
			`{"type":"watermark","time":1000}`,
		}, "\n") + "\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			err := RunAggregateWithMaxOpenWindows(strings.NewReader(tc.input), 1000, -1, &stdout, &stderr)
			var pe *AggregateParamError
			if !errors.As(err, &pe) || pe.Field != AggregateParamMaxOpenWindows {
				t.Fatalf("err = %v, want *AggregateParamError for max-open-windows", err)
			}
			if stdout.Len() != 0 || stderr.Len() != 0 {
				t.Fatalf("no output may precede a parameter error: stdout=%q stderr=%q", stdout.String(), stderr.String())
			}
		})
	}
}

// Fixed windows, the basic bound: two distinct keys in one window occupy two
// slots and are accepted exactly up to the cap; a third key that would open a
// third window fails at its own physical line, naming the key, the open count
// (2), the new windows needed (1) and the configured cap (2). No watermark
// has run, so nothing has been output and nothing is closed to make room.
func TestMaxOpenWindowsFixedRejectsWhenExceeded(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"a","time":100,"value":1}`, // line 1: opens (a,[0,1000))
		``, // line 2: blank, still counts
		`{"type":"event","key":"b","time":200,"value":2}`, // line 3: opens (b,[0,1000)), occupancy 2 == cap
		`{"type":"event","key":"c","time":300,"value":3}`, // line 4: needs 1 more, rejected
		`{"type":"event","key":"d","time":400,"value":4}`, // line 5: never read
	}, "\n")
	stdout, stderr, err := runMaxOpen(t, input, 1000, 1000, 0, 2)
	assertInputError(t, err, 4,
		`key "c"`,
		"2 window(s) currently open",
		"needs 1 new window(s)",
		"limit 2",
	)
	if stderr != "" {
		t.Fatalf("a rejected on-time event must not emit a late notice: stderr=%q", stderr)
	}
	if stdout != "" {
		t.Fatalf("nothing closed before the failure, stdout=%q", stdout)
	}
}

// Landing exactly on the cap is acceptance, not rejection: the event that
// fills the very last slot is folded in and later closed by a watermark with
// its count and sum intact.
func TestMaxOpenWindowsExactlyAtCapAccepted(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"a","time":100,"value":1}`,
		`{"type":"event","key":"b","time":100,"value":2}`,
		`{"type":"watermark","time":1000}`,
	}, "\n") + "\n"
	stdout, stderr, err := runMaxOpen(t, input, 1000, 1000, 0, 2)
	if err != nil {
		t.Fatalf("filling the cap exactly must succeed: %v", err)
	}
	if stderr != "" {
		t.Fatalf("unexpected stderr: %q", stderr)
	}
	if strings.Count(stdout, "\n") != 2 ||
		!strings.Contains(stdout, `{"key":"a","start":0,"end":1000,"count":1,"sum":1}`) ||
		!strings.Contains(stdout, `{"key":"b","start":0,"end":1000,"count":1,"sum":2}`) {
		t.Fatalf("both exactly-fitting windows must close: %q", stdout)
	}
}

// A key already in a window never takes another slot: with the cap at one the
// same key's later events all fold into the one open window and grow its
// count and sum, even though the cap is full the whole time.
func TestMaxOpenWindowsRepeatedEventForSamePairFillsOnlyOneSlot(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"a","time":100,"value":1}`,
		`{"type":"event","key":"a","time":200,"value":2}`,
		`{"type":"event","key":"a","time":300,"value":3}`,
		`{"type":"watermark","time":1000}`,
	}, "\n") + "\n"
	stdout, stderr, err := runMaxOpen(t, input, 1000, 1000, 0, 1)
	if err != nil {
		t.Fatalf("repeated events for the one open pair must keep succeeding: %v", err)
	}
	if stderr != "" {
		t.Fatalf("unexpected stderr: %q", stderr)
	}
	want := `{"key":"a","start":0,"end":1000,"count":3,"sum":6}` + "\n"
	if stdout != want {
		t.Fatalf("stdout = %q, want %q", stdout, want)
	}
}

// Sliding windows: one event can need several brand-new slots at once. With
// length 1000 and slide 600 an event at 700 belongs to [0,1000) and
// [600,1600); the first event takes both slots and is accepted exactly at a
// cap of 2, while a second key at the same time would need two more and is
// rejected as a unit -- it never lands in just one of the two.
func TestMaxOpenWindowsSlidingEventNeedsMultipleNewSlots(t *testing.T) {
	accepted := strings.Join([]string{
		`{"type":"event","key":"a","time":700,"value":1}`, // needs 2, occupancy 0 -> exactly 2 == cap
	}, "\n") + "\n"
	stdout, stderr, err := runMaxOpen(t, accepted, 1000, 600, 0, 2)
	if err != nil {
		t.Fatalf("an event filling the cap exactly must be accepted: %v", err)
	}
	if stderr != "" || stdout != "" {
		t.Fatalf("stderr=%q stdout=%q, want both empty", stderr, stdout)
	}

	rejected := strings.Join([]string{
		`{"type":"event","key":"a","time":700,"value":1}`, // line 1: 2 slots
		`{"type":"event","key":"b","time":700,"value":2}`, // line 2: needs 2 more, 2+2 > 2
	}, "\n")
	stdout, _, err = runMaxOpen(t, rejected, 1000, 600, 0, 2)
	assertInputError(t, err, 2,
		`key "b"`,
		"2 window(s) currently open",
		"needs 2 new window(s)",
		"limit 2",
	)
	if stdout != "" {
		t.Fatalf("rejected event must output nothing: %q", stdout)
	}
}

// An event whose overlapping windows partly exist and partly are new pays only
// for the new ones. Length 1000/slide 600: a@700 opens starts 0 and 600;
// a@1200 belongs to starts 600 and 1200, so it reuses 600 and adds 1200 (one
// new slot), reaching cap 3 exactly. A different key at 1200 would need two
// fresh slots and fails, entering neither window.
func TestMaxOpenWindowsSlidingOnlyBrandNewPairsCount(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"a","time":700,"value":1}`,  // opens (a,0),(a,600)
		`{"type":"event","key":"a","time":1200,"value":2}`, // reuses (a,600), opens (a,1200): occupancy 3 == cap
		`{"type":"event","key":"c","time":1200,"value":3}`, // would open (c,600),(c,1200): 3+2 > 3
	}, "\n")
	stdout, _, err := runMaxOpen(t, input, 1000, 600, 0, 3)
	assertInputError(t, err, 3, `key "c"`, "3 window(s) currently open", "needs 2 new window(s)")
	if stdout != "" {
		t.Fatalf("no window closes here, stdout=%q", stdout)
	}
}

// A rejected sliding event is all-or-nothing: after it fails none of the
// windows it would have opened exist and every earlier window keeps its
// previous count and sum. The check runs on internal state so the post-failure
// map can be inspected directly.
func TestMaxOpenWindowsSlidingRejectionChangesNoState(t *testing.T) {
	s := newCountState(1000, 600)
	s.maxOpenWindows = 2
	input := strings.Join([]string{
		`{"type":"event","key":"a","time":700,"value":1}`, // (a,0),(a,600): occupancy 2
		`{"type":"event","key":"b","time":700,"value":9}`, // needs 2 more: rejected
	}, "\n")
	_, _, err := runSlidingState(s, input)
	assertInputError(t, err, 2, `key "b"`)
	if len(s.windows) != 2 {
		t.Fatalf("occupancy = %d, want 2: the rejected event created no window", len(s.windows))
	}
	for start, wantSum := range map[int64]int64{0: 1, 600: 1} {
		got := s.windows[windowID{start: start, key: "a"}]
		if got == nil {
			t.Fatalf("window start %d for key a disappeared", start)
		}
		if got.count != 1 || got.sum != wantSum {
			t.Errorf("(a,%d) = count %d sum %d, want 1/%d: the rejected event must not have touched an existing window",
				start, got.count, got.sum, wantSum)
		}
	}
	if s.windows[windowID{start: 0, key: "b"}] != nil || s.windows[windowID{start: 600, key: "b"}] != nil {
		t.Fatal("the rejected key b must occupy neither overlapping window")
	}
}

// Partitions merge, they never take separate slots: two partitions
// contributing to the same key in the same window occupy the single pair, so
// a cap of one accepts both and the closed window reports count 2. Different
// keys -- or the same key in a different window -- are different pairs and do
// take another slot.
func TestMaxOpenWindowsPartitionsMergeIntoOneSlot(t *testing.T) {
	merged := strings.Join([]string{
		`{"type":"event","key":"a","time":100,"value":1,"partition":0}`,
		`{"type":"event","key":"a","time":200,"value":2,"partition":1}`,
		`{"type":"watermark","time":5000,"partition":0}`,
		`{"type":"watermark","time":5000,"partition":1}`,
	}, "\n") + "\n"
	stdout, stderr, err := runMaxOpen(t, merged, 1000, 1000, 2, 1)
	if err != nil {
		t.Fatalf("same key/window from two partitions must share the one slot: %v", err)
	}
	if stderr != "" {
		t.Fatalf("unexpected stderr: %q", stderr)
	}
	want := `{"key":"a","start":0,"end":1000,"count":2,"sum":3}` + "\n"
	if stdout != want {
		t.Fatalf("merged output = %q, want %q", stdout, want)
	}

	// Same key, a later fixed window is a different interval and thus a
	// different pair: with the cap at one it cannot fit.
	differentWindow := strings.Join([]string{
		`{"type":"event","key":"a","time":100,"value":1,"partition":0}`,
		`{"type":"event","key":"a","time":1100,"value":2,"partition":1}`, // (a,[1000,2000)) is new
	}, "\n")
	_, _, err = runMaxOpen(t, differentWindow, 1000, 1000, 2, 1)
	assertInputError(t, err, 2, `key "a"`, "1 window(s) currently open", "needs 1 new window(s)", "limit 1")

	// A second key from either partition is likewise a second pair.
	differentKey := strings.Join([]string{
		`{"type":"event","key":"a","time":100,"value":1,"partition":0}`,
		`{"type":"event","key":"b","time":200,"value":2,"partition":1}`,
	}, "\n")
	_, _, err = runMaxOpen(t, differentKey, 1000, 1000, 2, 1)
	assertInputError(t, err, 2, `key "b"`, "needs 1 new window(s)")
}

// A window's slot is released only after it closes under the existing
// watermark rules and its result line has been written. With a cap of one the
// first key's window must fully close before the second key can be accepted;
// the watermark that closes it also frees the slot within the same input line.
func TestMaxOpenWindowsSlotReleasedAfterClosureAndOutput(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"a","time":100,"value":1}`,  // (a,[0,1000))
		`{"type":"watermark","time":1000}`,                 // closes it, slot released
		`{"type":"event","key":"b","time":1100,"value":2}`, // (b,[1000,2000)) reuses the slot
		`{"type":"watermark","time":2000}`,
	}, "\n") + "\n"
	stdout, stderr, err := runMaxOpen(t, input, 1000, 1000, 0, 1)
	if err != nil {
		t.Fatalf("a released slot must be reusable: %v", err)
	}
	if stderr != "" {
		t.Fatalf("unexpected stderr: %q", stderr)
	}
	want := strings.Join([]string{
		`{"key":"a","start":0,"end":1000,"count":1,"sum":1}`,
		`{"key":"b","start":1000,"end":2000,"count":1,"sum":2}`,
		``,
	}, "\n")
	if stdout != want {
		t.Fatalf("stdout = %q, want %q", stdout, want)
	}
}

// Sliding closure frees exactly the windows that closed: closing the earlier
// of two overlapping windows releases one of the two slots while the later
// stays open, so a later event needing one new pair fits but an event needing
// two new pairs still does not.
func TestMaxOpenWindowsSlidingClosureFreesOnlyClosedWindows(t *testing.T) {
	// a@700 occupies starts 0 and 600; watermark 1000 closes only start 0
	// ([0,1000) end == 1000), leaving [600,1600) open: occupancy goes 2 -> 1.
	base := []string{
		`{"type":"event","key":"a","time":700,"value":1}`,
		`{"type":"watermark","time":1000}`,
	}

	// b@1200 is on time (1200 >= watermark 1000) and belongs to starts 600
	// and 1200, neither of which exists for b yet: it needs two fresh slots,
	// 1 + 2 > cap 2, so it is rejected as a unit.
	needsTwo := append(append([]string{}, base...),
		`{"type":"event","key":"b","time":1200,"value":2}`,
	)
	_, _, err := runMaxOpen(t, strings.Join(needsTwo, "\n")+"\n", 1000, 600, 0, 2)
	assertInputError(t, err, 3, `key "b"`, "1 window(s) currently open", "needs 2 new window(s)", "limit 2")

	// The same time for a folds one event into the already-open (a,600) and
	// opens only (a,1200): one new slot, 1 + 1 == 2, accepted. The later
	// watermark then closes both remaining pairs.
	needsOne := append(append([]string{}, base...),
		`{"type":"event","key":"a","time":1200,"value":2}`,
		`{"type":"watermark","time":2200}`,
	)
	stdout, stderr, err := runMaxOpen(t, strings.Join(needsOne, "\n")+"\n", 1000, 600, 0, 2)
	if err != nil {
		t.Fatalf("one freed slot accepting one new pair must succeed: %v", err)
	}
	if stderr != "" {
		t.Fatalf("unexpected stderr: %q", stderr)
	}
	want := strings.Join([]string{
		`{"key":"a","start":0,"end":1000,"count":1,"sum":1}`,
		`{"key":"a","start":600,"end":1600,"count":2,"sum":3}`,
		`{"key":"a","start":1200,"end":2200,"count":1,"sum":2}`,
		``,
	}, "\n")
	if stdout != want {
		t.Fatalf("stdout = %q, want %q", stdout, want)
	}
}

// An idle declaration by itself frees nothing: a partition that has never
// reported going idle leaves the effective watermark unknown, so no window
// closes and the cap still rejects. The slot frees only once a later idle
// declaration (or watermark) actually advances the effective watermark far
// enough to close the window and output it.
func TestMaxOpenWindowsIdleAloneDoesNotRelease(t *testing.T) {
	noRelease := strings.Join([]string{
		`{"type":"event","key":"a","time":100,"value":1,"partition":0}`,
		`{"type":"idle","partition":1}`, // effective watermark still unknown: nothing closes
		`{"type":"event","key":"b","time":200,"value":2,"partition":0}`,
	}, "\n")
	_, _, err := runMaxOpen(t, noRelease, 1000, 1000, 2, 1)
	assertInputError(t, err, 3, `key "b"`, "1 window(s) currently open", "needs 1 new window(s)")

	releases := strings.Join([]string{
		`{"type":"event","key":"a","time":100,"value":1,"partition":0}`,
		`{"type":"watermark","time":1000,"partition":0}`,
		`{"type":"idle","partition":1}`, // effective 1000: (a,[0,1000)) closes and frees
		`{"type":"event","key":"b","time":1100,"value":2,"partition":0}`,
		`{"type":"watermark","time":2000,"partition":0}`,
	}, "\n") + "\n"
	stdout, stderr, err := runMaxOpen(t, releases, 1000, 1000, 2, 1)
	if err != nil {
		t.Fatalf("idle that advances the watermark must release the slot: %v", err)
	}
	if stderr != "" {
		t.Fatalf("unexpected stderr: %q", stderr)
	}
	want := strings.Join([]string{
		`{"key":"a","start":0,"end":1000,"count":1,"sum":1}`,
		`{"key":"b","start":1000,"end":2000,"count":1,"sum":2}`,
		``,
	}, "\n")
	if stdout != want {
		t.Fatalf("stdout = %q, want %q", stdout, want)
	}
}

// Late events never reach the cap check: a strictly late event for a brand
// new key while the cap is full is skipped with the ordinary late notice, not
// reported as exceeding the cap, and opens no window. An event exactly at the
// watermark, by contrast, is on time and is subject to the cap like any
// other.
func TestMaxOpenWindowsLateEventSkippedBeforeCap(t *testing.T) {
	late := strings.Join([]string{
		`{"type":"event","key":"a","time":100,"value":1}`,
		`{"type":"watermark","time":500}`,
		`{"type":"event","key":"zzz","time":499,"value":1}`, // strictly late even though the cap is full
	}, "\n")
	stdout, stderr, err := runMaxOpen(t, late, 1000, 1000, 0, 1)
	if err != nil {
		t.Fatalf("a late event must stay a skip, not a cap failure: %v", err)
	}
	wantNotice := "line 3: late event time=499 below current watermark 500, skipped\n"
	if stderr != wantNotice {
		t.Fatalf("stderr = %q, want %q", stderr, wantNotice)
	}
	if stdout != "" {
		t.Fatalf("the late event must open no window: %q", stdout)
	}

	atWatermark := strings.Join([]string{
		`{"type":"event","key":"a","time":100,"value":1}`,
		`{"type":"watermark","time":500}`,
		`{"type":"event","key":"zzz","time":500,"value":1}`, // equal to the watermark: on time, needs a slot
	}, "\n")
	_, _, err = runMaxOpen(t, atWatermark, 1000, 1000, 0, 1)
	assertInputError(t, err, 3, `key "zzz"`, "needs 1 new window(s)")
}

// Existing record-error rules keep their precedence over the cap. With the cap
// full, a malformed event field, an event for an idle partition and a
// duplicate top-level key are all reported as their existing errors rather
// than as the cap, even though the same valid event would exceed it.
func TestMaxOpenWindowsRecordErrorsKeepPrecedence(t *testing.T) {
	cases := []struct {
		name        string
		partitions  int64
		input       string
		wantLine    int
		wantInError []string
	}{
		{
			name:       "missing value field",
			partitions: 0,
			input: strings.Join([]string{
				`{"type":"event","key":"a","time":100,"value":1}`,
				`{"type":"event","key":"b","time":200}`, // bad field AND would exceed cap
			}, "\n"),
			wantLine:    2,
			wantInError: []string{`field "value"`},
		},
		{
			name:       "idle partition event",
			partitions: 2,
			input: strings.Join([]string{
				`{"type":"event","key":"a","time":100,"value":1,"partition":0}`,
				`{"type":"idle","partition":1}`,
				`{"type":"event","key":"b","time":200,"value":2,"partition":1}`, // idle AND would exceed
			}, "\n"),
			wantLine:    3,
			wantInError: []string{"idle partition 1"},
		},
		{
			name:       "duplicate top-level key",
			partitions: 0,
			input: strings.Join([]string{
				`{"type":"event","key":"a","time":100,"value":1}`,
				`{"type":"event","key":"b","key":"b","time":200,"value":2}`, // duplicate AND would exceed
			}, "\n"),
			wantLine:    2,
			wantInError: []string{`duplicate field "key"`},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := runMaxOpen(t, tc.input, 1000, 1000, tc.partitions, 1)
			ie := assertInputError(t, err, tc.wantLine, tc.wantInError...)
			if strings.Contains(ie.Reason, "open-window limit") {
				t.Fatalf("the record error must take precedence over the cap: %q", ie.Reason)
			}
		})
	}
}

// The window-end overflow precheck still runs before the cap check: an event
// whose window end would leave the signed 64-bit range is reported as that
// overflow even when the cap is already full (seeded state, so both conditions
// hold on the same line).
func TestMaxOpenWindowsWindowEndOverflowPrecedence(t *testing.T) {
	const M = math.MaxInt64
	s := newCountState(M, M) // fixed windows of length MaxInt64
	s.maxOpenWindows = 1
	// One open pair already fills the single slot.
	seedCountWindow(s, "a", 0, M, 1, 1)
	// The new event at time M would open start M with end M+M -> overflow.
	input := `{"type":"event","key":"b","time":9223372036854775807,"value":1}` + "\n"
	_, _, err := runSlidingState(s, input)
	assertInputError(t, err, 1, `key "b"`, "window end overflow")
}

// A result fully written before the rejecting line stays on the output, and
// processing stops exactly there: the later record that would open another
// window is never read.
func TestMaxOpenWindowsRejectionRetainsEarlierOutputAndStops(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"a","time":100,"value":1}`,  // line 1
		`{"type":"event","key":"b","time":100,"value":2}`,  // line 2
		`{"type":"watermark","time":1000}`,                 // line 3: closes both, frees both slots
		`{"type":"event","key":"c","time":1100,"value":3}`, // line 4: one slot used
		`{"type":"event","key":"d","time":1100,"value":4}`, // line 5: fills the cap exactly at 2
		`{"type":"event","key":"e","time":1100,"value":5}`, // line 6: exceeds -> rejected
		`{"type":"watermark","time":2000}`,                 // line 7: never read
	}, "\n") + "\n"
	stdout, _, err := runMaxOpen(t, input, 1000, 1000, 0, 2)
	assertInputError(t, err, 6, `key "e"`, "2 window(s) currently open", "needs 1 new window(s)", "limit 2")
	lines := strings.Split(strings.TrimSuffix(stdout, "\n"), "\n")
	want := []string{
		`{"key":"a","start":0,"end":1000,"count":1,"sum":1}`,
		`{"key":"b","start":0,"end":1000,"count":1,"sum":2}`,
	}
	if len(lines) != len(want) {
		t.Fatalf("stdout has %d lines, want the two closed before the failure only:\n%s", len(lines), stdout)
	}
	for i := range want {
		if lines[i] != want[i] {
			t.Fatalf("stdout line %d = %q, want %q", i+1, lines[i], want[i])
		}
	}
}

// A window whose result cannot be written in full stays open and keeps its
// slot: the failed closure deletes nothing from the window map, so a cap
// rejection cannot be dodged by a write failure. This drives internal state
// with a writer that errors on the first byte, the same double used by the
// output-failure tests.
func TestMaxOpenWindowsWriteFailureDoesNotReleaseSlot(t *testing.T) {
	s := newCountState(1000, 1000)
	s.maxOpenWindows = 1
	s.out = &errOnlyWriter{err: io.ErrClosedPipe}
	var late bytes.Buffer
	s.lateLog = &late
	input := strings.Join([]string{
		`{"type":"event","key":"a","time":100,"value":1}`,
		`{"type":"watermark","time":1000}`, // closure write fails
	}, "\n")
	err := readAggregateLines(strings.NewReader(input), func(line string, no int) error {
		if strings.TrimSpace(line) == "" {
			return nil
		}
		return s.processLine(line, no)
	})
	var oe *OutputError
	if !errors.As(err, &oe) {
		t.Fatalf("expected *OutputError, got %v", err)
	}
	if len(s.windows) != 1 {
		t.Fatalf("open window count = %d, want 1: a window not fully written must keep its slot", len(s.windows))
	}
	if s.windows[windowID{start: 0, key: "a"}] == nil {
		t.Fatal("the unwritten window must remain in the map exactly as before")
	}
}

// Omitting the cap is unlimited in two equivalent ways: the original entry
// points (unchanged signatures) and the new entry points given zero both keep
// every key with no rejection.
func TestMaxOpenWindowsOmittedOrZeroMeansUnlimited(t *testing.T) {
	var lines []string
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		lines = append(lines, fmt.Sprintf(`{"type":"event","key":%q,"time":100,"value":1}`, k))
	}
	lines = append(lines, `{"type":"watermark","time":1000}`)
	input := strings.Join(lines, "\n") + "\n"

	var oldOut, zeroOut bytes.Buffer
	if err := RunAggregate(strings.NewReader(input), 1000, &oldOut, io.Discard); err != nil {
		t.Fatalf("RunAggregate: %v", err)
	}
	if err := RunAggregateWithMaxOpenWindows(strings.NewReader(input), 1000, 0, &zeroOut, io.Discard); err != nil {
		t.Fatalf("cap 0 must mean unlimited: %v", err)
	}
	if oldOut.String() != zeroOut.String() {
		t.Fatalf("omitted cap and explicit zero differ:\n%q\n%q", oldOut.String(), zeroOut.String())
	}
	if strings.Count(oldOut.String(), "\n") != 5 {
		t.Fatalf("all five keys must be retained without a cap: %q", oldOut.String())
	}
}
