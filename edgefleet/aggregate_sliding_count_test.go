package edgefleet

import (
	"bytes"
	"fmt"
	"math"
	"strings"
	"testing"
)

// Regression safeguards for the per-window event count of sliding windows at
// the signed 64-bit limit. count is the number of events a window actually
// received: positive, negative and zero values all add exactly one to every
// window that contains the event. An addition that lands the count exactly on
// math.MaxInt64 is legal and must survive closure as a full integer JSON
// number; only a further event into an already-saturated window fails, at that
// input line, naming the window that truly overflows. Overlapping windows keep
// independent counts, and the late-event skip runs before any count check.
//
// No finite input can really deliver 2^63 events, so these tests seed the
// internal window state near the limit (same package, same approach as the
// idle tests) and then feed genuine JSON records through processLine so the
// real parsing, late-event, overflow and closure paths all run.

// newSeededSlidingState builds a legacy single-watermark sliding state with
// the given windows already present, together with the buffers its results
// and late notices land in. A nil seed starts with no open window.
func newSeededSlidingState(t *testing.T, window, slide int64, seed map[windowID]*windowState) (s *aggregateState, out, late *bytes.Buffer) {
	t.Helper()
	out, late = &bytes.Buffer{}, &bytes.Buffer{}
	if seed == nil {
		seed = make(map[windowID]*windowState)
	}
	s = &aggregateState{
		windowMillis:  window,
		slideMillis:   slide,
		windows:       seed,
		out:           out,
		lateLog:       late,
		partWatermark: make(map[int64]*int64),
		idle:          make(map[int64]bool),
	}
	return s, out, late
}

func eventLine(key string, time, value int64) string {
	return fmt.Sprintf(`{"type":"event","key":%q,"time":%d,"value":%d}`, key, time, value)
}

func watermarkLine(time int64) string {
	return fmt.Sprintf(`{"type":"watermark","time":%d}`, time)
}

// Positive, negative and zero values each count once in every containing
// window. Three events at 700 (values 1, -1, 0) all land in both [0,1000) and
// [600,1600), so every closed window reports count 3 with sum 0.
func TestAggregateSlidingCountPositiveNegativeZeroAllCount(t *testing.T) {
	input := strings.Join([]string{
		eventLine("k", 700, 1),
		eventLine("k", 700, -1),
		eventLine("k", 700, 0),
		`{"type":"watermark","time":1000}`,
		`{"type":"watermark","time":1600}`,
	}, "\n")
	stdout, stderr, err := runSliding(t, input, 1000, 600)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stderr != "" {
		t.Fatalf("unexpected stderr: %q", stderr)
	}
	want := strings.Join([]string{
		`{"key":"k","start":0,"end":1000,"count":3,"sum":0}`,
		`{"key":"k","start":600,"end":1600,"count":3,"sum":0}`,
		``,
	}, "\n")
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
}

// A window already holding MaxInt64-1 accepted events must accept one more
// legal event (here value 0, which cannot affect the sum) and land exactly on
// MaxInt64; it must not be rejected one event early. Closed by a watermark,
// the result JSON keeps the full integer as a number: literal text is matched
// so any float rounding (9223372036854775808) or string quoting fails, and the
// value parses back as exactly math.MaxInt64 rather than going negative.
func TestAggregateSlidingCountReachesMaxInt64Exactly(t *testing.T) {
	const M = math.MaxInt64
	// t=500 with window 1000 / slide 600 belongs only to [0,1000).
	s, out, late := newSeededSlidingState(t, 1000, 600, map[windowID]*windowState{
		{start: 0, key: "k"}: {end: 1000, count: M - 1, sum: 0},
	})

	if err := s.processLine(eventLine("k", 500, 0), 1); err != nil {
		t.Fatalf("event reaching the count limit must be accepted, got %v", err)
	}
	if late.Len() != 0 {
		t.Fatalf("accepted event must not produce a late notice: %q", late.String())
	}
	if err := s.processLine(watermarkLine(1000), 2); err != nil {
		t.Fatalf("closing watermark failed: %v", err)
	}

	want := fmt.Sprintf(`{"key":"k","start":0,"end":1000,"count":%d,"sum":0}`+"\n", M)
	if out.String() != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", out.String(), want)
	}
	// The count is emitted as an unquoted number, never a string.
	if strings.Contains(out.String(), fmt.Sprintf(`"count":"%d"`, M)) {
		t.Fatalf("count must be a JSON number, not a string: %q", out.String())
	}
	got := decodeAggregateResults(t, out.String())
	if len(got) != 1 || got[0] != (AggregateResult{Key: "k", Start: 0, End: 1000, Count: M, Sum: 0}) {
		t.Fatalf("decoded row = %+v, want count exactly MaxInt64", got)
	}
}

// Once a window's count is MaxInt64, the next event it would receive fails
// immediately even when its value is 0 and cannot overflow the sum. The error
// is the existing *InputError naming the physical line (blank lines count),
// the key and the left-closed/right-open window. Events and watermarks after
// it take no effect: previously fully written results stay and still-open
// windows are not flushed.
func TestAggregateSlidingCountOverflowAtLimitFatal(t *testing.T) {
	const M = math.MaxInt64
	// [0,1000) provides a result closed before the failure; [1200,2200) is
	// saturated and stays open. t=1700 belongs only to [1200,2200).
	s, out, _ := newSeededSlidingState(t, 1000, 600, map[windowID]*windowState{
		{start: 0, key: "k"}:    {end: 1000, count: 7, sum: 7},
		{start: 1200, key: "k"}: {end: 2200, count: M, sum: 0},
	})

	// Line 1 closes [0,1000); that output must survive the later failure.
	if err := s.processLine(watermarkLine(1000), 1); err != nil {
		t.Fatalf("closing watermark failed: %v", err)
	}

	// Lines 2 and 5 are blank physical lines (the reader skips them, so
	// processLine is handed the real record at its true physical line 6).
	err := s.processLine(eventLine("k", 1700, 0), 6)
	if err == nil {
		t.Fatal("expected event count overflow, got nil")
	}
	inputErr, ok := err.(*InputError)
	if !ok {
		t.Fatalf("expected *InputError, got %T: %v", err, err)
	}
	if inputErr.Line != 6 {
		t.Errorf("line = %d, want 6 (blank lines must count)", inputErr.Line)
	}
	if !strings.Contains(inputErr.Reason, "event count overflow") {
		t.Fatalf("reason = %q, want event count overflow", inputErr.Reason)
	}
	if !strings.Contains(inputErr.Reason, `for key "k"`) {
		t.Fatalf("reason = %q, want it to name the key", inputErr.Reason)
	}
	if !strings.Contains(inputErr.Reason, "window [1200,2200)") {
		t.Fatalf("reason = %q, want it to name window [1200,2200)", inputErr.Reason)
	}

	// The run stops at that record: the test, like the real reader, processes
	// no further line. The saturated window is still open and a watermark that
	// would close it is never delivered, so no second result may appear.
	if st := s.windows[windowID{start: 1200, key: "k"}]; st == nil || st.count != M {
		t.Fatalf("saturated window must remain open with count %d, got %+v", M, st)
	}
	want := `{"key":"k","start":0,"end":1000,"count":7,"sum":7}` + "\n"
	if out.String() != want {
		t.Fatalf("only the earlier result must remain:\n got: %q\nwant: %q", out.String(), want)
	}
}

// Overlapping windows count independently. The same event is accepted by a
// window that still has room and rejected by a saturated later window; the
// error names the window that actually overflows and never the safe earlier
// one. With window 1000 / slide 600, t=700 belongs to [0,1000) (count 5,
// accepts it) and [600,1600) (already at MaxInt64, rejects it).
func TestAggregateSlidingCountOverflowNamesSaturatedWindowNotSafeOne(t *testing.T) {
	const M = math.MaxInt64
	s, _, _ := newSeededSlidingState(t, 1000, 600, map[windowID]*windowState{
		{start: 0, key: "k"}:   {end: 1000, count: 5, sum: 0},
		{start: 600, key: "k"}: {end: 1600, count: M, sum: 0},
	})

	err := s.processLine(eventLine("k", 700, 0), 2)
	if err == nil {
		t.Fatal("expected event count overflow, got nil")
	}
	inputErr, ok := err.(*InputError)
	if !ok {
		t.Fatalf("expected *InputError, got %T: %v", err, err)
	}
	if inputErr.Line != 2 {
		t.Errorf("line = %d, want 2", inputErr.Line)
	}
	if !strings.Contains(inputErr.Reason, "event count overflow") {
		t.Fatalf("reason = %q, want event count overflow", inputErr.Reason)
	}
	if !strings.Contains(inputErr.Reason, "window [600,1600)") {
		t.Fatalf("reason = %q, want it to name the saturated window [600,1600)", inputErr.Reason)
	}
	if strings.Contains(inputErr.Reason, "[0,1000)") {
		t.Errorf("reason must not blame the safe window [0,1000): %q", inputErr.Reason)
	}
}

// When several containing windows are already saturated, the failure names
// the first one reached in ascending start order; a window that still has
// room (even one sitting at MaxInt64-1 that this event fills exactly) is not
// blamed. Window 1200 / slide 400, t=1000 belongs to [0,1200),[400,1600),
// [800,2000).
func TestAggregateSlidingCountOverflowNamesEarliestSaturatedWindow(t *testing.T) {
	const M = math.MaxInt64
	s, _, _ := newSeededSlidingState(t, 1200, 400, map[windowID]*windowState{
		{start: 0, key: "k"}:   {end: 1200, count: M - 1, sum: 0}, // accepts: reaches M exactly
		{start: 400, key: "k"}: {end: 1600, count: M, sum: 0},     // first saturated window
		{start: 800, key: "k"}: {end: 2000, count: M, sum: 0},     // also saturated, reached later
	})

	err := s.processLine(eventLine("k", 1000, 0), 1)
	if err == nil {
		t.Fatal("expected event count overflow, got nil")
	}
	inputErr, ok := err.(*InputError)
	if !ok {
		t.Fatalf("expected *InputError, got %T: %v", err, err)
	}
	if !strings.Contains(inputErr.Reason, "window [400,1600)") {
		t.Fatalf("reason = %q, want the first saturated window [400,1600)", inputErr.Reason)
	}
	if strings.Contains(inputErr.Reason, "[0,1200)") {
		t.Errorf("reason must not blame the window [0,1200) that accepted the event: %q", inputErr.Reason)
	}
	if strings.Contains(inputErr.Reason, "[800,2000)") {
		t.Errorf("reason must name the earliest saturated window, not [800,2000): %q", inputErr.Reason)
	}
	// Processing runs in ascending start order, so the earlier window that
	// still had room really accepted this same event (M-1 -> M) before the
	// saturated later window rejected it.
	if st := s.windows[windowID{start: 0, key: "k"}]; st == nil || st.count != M {
		t.Errorf("window [0,1200) should have accepted the event up to count %d, got %+v", M, st)
	}
}

// A legal event strictly below the current watermark is skipped with the late
// notice even though it falls in an unclosed window already at the count
// limit: it is neither a count overflow nor counted, and the window's count
// and sum are untouched. The window later closes with its saturated count
// preserved as a full integer.
func TestAggregateSlidingCountLateEventSkippedDespiteSaturatedWindow(t *testing.T) {
	const M = math.MaxInt64
	s, out, late := newSeededSlidingState(t, 1000, 600, map[windowID]*windowState{
		{start: 600, key: "k"}: {end: 1600, count: M, sum: 42},
	})
	wm := int64(1000)
	s.watermark = &wm // current watermark 1000; [600,1600) stays open

	// t=999 < 1000 is late; it would otherwise enter the saturated window.
	if err := s.processLine(eventLine("k", 999, 0), 4); err != nil {
		t.Fatalf("a late event must be skipped, not reported as overflow: %v", err)
	}
	wantNotice := "line 4: late event time=999 below current watermark 1000, skipped\n"
	if late.String() != wantNotice {
		t.Fatalf("late notice = %q, want %q", late.String(), wantNotice)
	}
	st := s.windows[windowID{start: 600, key: "k"}]
	if st == nil {
		t.Fatal("saturated window must still exist")
	}
	if st.count != M {
		t.Errorf("count = %d, want unchanged %d", st.count, M)
	}
	if st.sum != 42 {
		t.Errorf("sum = %d, want unchanged 42", st.sum)
	}

	// It closes normally with the full integer count intact.
	if err := s.processLine(watermarkLine(1600), 5); err != nil {
		t.Fatalf("closing watermark failed: %v", err)
	}
	want := fmt.Sprintf(`{"key":"k","start":600,"end":1600,"count":%d,"sum":42}`+"\n", M)
	if out.String() != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", out.String(), want)
	}
	got := decodeAggregateResults(t, out.String())
	if len(got) != 1 || got[0].Count != M {
		t.Fatalf("decoded row = %+v, want count exactly MaxInt64", got)
	}
}

// An event time exactly equal to the current watermark is processed normally,
// so it still overflows a saturated window fatally (the late rule rejects
// only strictly lower times). t=1000 == watermark 1000 belongs only to the
// saturated [600,1600).
func TestAggregateSlidingCountEventAtWatermarkStillOverflows(t *testing.T) {
	const M = math.MaxInt64
	s, out, late := newSeededSlidingState(t, 1000, 600, map[windowID]*windowState{
		{start: 600, key: "k"}: {end: 1600, count: M, sum: 0},
	})
	wm := int64(1000)
	s.watermark = &wm

	err := s.processLine(eventLine("k", 1000, 0), 3)
	if err == nil {
		t.Fatal("expected event count overflow for an event exactly at the watermark, got nil")
	}
	inputErr, ok := err.(*InputError)
	if !ok {
		t.Fatalf("expected *InputError, got %T: %v", err, err)
	}
	if inputErr.Line != 3 {
		t.Errorf("line = %d, want 3", inputErr.Line)
	}
	if !strings.Contains(inputErr.Reason, "event count overflow") {
		t.Fatalf("reason = %q, want event count overflow", inputErr.Reason)
	}
	if !strings.Contains(inputErr.Reason, "window [600,1600)") {
		t.Fatalf("reason = %q, want it to name window [600,1600)", inputErr.Reason)
	}
	if late.Len() != 0 {
		t.Errorf("an event at the watermark is not late; unexpected notice %q", late.String())
	}
	if out.Len() != 0 {
		t.Errorf("the fatal event must emit no result, got %q", out.String())
	}
}
