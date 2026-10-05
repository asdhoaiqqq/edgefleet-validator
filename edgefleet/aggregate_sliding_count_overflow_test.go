package edgefleet

import (
	"bytes"
	"fmt"
	"math"
	"strings"
	"testing"
)

// Regression safeguards for the sliding-window event count at the signed
// 64-bit limit. Each (key, window) keeps an independent count of the events it
// actually received: a normally accepted event -- positive, negative or zero
// value alike -- counts once in every window that contains its event time,
// independently of that window's sum. An addition that lands the count exactly
// on math.MaxInt64 is legal and must close with that exact integer rendered as
// a JSON number (no rounding, no sign flip); only the NEXT accepted event,
// which would need math.MaxInt64+1 counts, is fatal at its own physical input
// line as an *InputError that says "event count overflow" and names the key
// and the offending window's left-closed/right-open interval. Overlapping
// windows fail independently, and the late-event check runs first, so a
// strictly late event is merely skipped (with the existing notice) even when a
// still-open window it lands in is already at the count limit. Scope is the
// legacy single-watermark RunAggregateSliding entry point.
//
// The windows under test are seeded straight at the boundary through package
// internals: feeding 9223372036854775806 events cannot be done at a finite
// offline scale, whereas one seeded state plus a handful of real records read
// through the production line collector exercises the exact update loop,
// physical line counting (blank lines count), watermark closure and output
// formatting.

// seedCountWindow prepares one key/window pair already holding count events
// and the given sum, marking its end so watermark closure works unchanged.
func seedCountWindow(s *aggregateState, key string, start, end, count, sum int64) {
	s.windows[windowID{start: start, key: key}] = &windowState{end: end, count: count, sum: sum}
}

// runSlidingState feeds input to an already seeded state through the normal
// line collector, so seeding never bypasses physical line counting or any
// record check. It returns the captured window output and late-event output.
func runSlidingState(s *aggregateState, input string) (string, string, error) {
	var stdout, stderr bytes.Buffer
	s.out = &stdout
	s.lateLog = &stderr
	err := readAggregateLines(strings.NewReader(input), func(line string, no int) error {
		if strings.TrimSpace(line) == "" {
			return nil
		}
		return s.processLine(line, no)
	})
	return stdout.String(), stderr.String(), err
}

// newCountState builds a legacy single-watermark sliding state with the given
// window length and slide interval and an empty window map.
func newCountState(windowMillis, slideMillis int64) *aggregateState {
	return &aggregateState{
		windowMillis: windowMillis,
		slideMillis:  slideMillis,
		windows:      make(map[windowID]*windowState),
	}
}

// A window one event below the limit accepts the next legal event and reaches
// math.MaxInt64 exactly: the event must not be rejected early, and the window
// closed by a later watermark carries the full integer as a JSON number, with
// no rounding and no sign flip. This holds for every value sign -- positive,
// negative and zero values are all worth exactly one count. The literal-text
// comparison fails if the count were ever rendered through a float64
// (9223372036854775807 would print as 9223372036854775808 or, after an integer
// wrap, a negative number).
func TestAggregateSlidingCountBoundaryReachesMaxInt64Exactly(t *testing.T) {
	const M = math.MaxInt64
	const w, sl int64 = 1000, 600
	// Seed a safe sum of 100 so each value sign is legal to add; the resulting
	// sum differs by sign but the count must land on MaxInt64 in every case.
	cases := []struct {
		name  string
		value int64
		sum   int64
	}{
		{"positive value", 1, 101},
		{"negative value", -1, 99},
		{"zero value", 0, 100},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newCountState(w, sl)
			seedCountWindow(s, "sensor-a", 600, 1600, M-1, 100)

			input := strings.Join([]string{
				fmt.Sprintf(`{"type":"event","key":"sensor-a","time":1000,"value":%d}`, tc.value), // M-1 -> M
				`{"type":"watermark","time":1600}`,
			}, "\n")
			stdout, stderr, err := runSlidingState(s, input)
			if err != nil {
				t.Fatalf("the event that reaches exactly MaxInt64 must succeed, got: %v", err)
			}
			if stderr != "" {
				t.Fatalf("unexpected stderr: %q", stderr)
			}
			want := fmt.Sprintf(`{"key":"sensor-a","start":600,"end":1600,"count":%d,"sum":%d}`+"\n", M, tc.sum)
			if stdout != want {
				t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
			}
			// Parse it back: the field must survive as the exact positive limit,
			// not a rounded or negative value.
			got := decodeAggregateResults(t, stdout)
			if len(got) != 1 || got[0] != (AggregateResult{Key: "sensor-a", Start: 600, End: 1600, Count: M, Sum: tc.sum}) {
				t.Fatalf("decoded rows = %v", got)
			}
		})
	}
}

// Once a window already holds math.MaxInt64 events, the next event that should
// otherwise be accepted fails on that line even when its value is zero and
// could not overflow the sum. The *InputError reports "event count overflow"
// with the physical line number (blank lines count), the key and the window's
// [start,end) interval. Records after the failure are not read, open windows
// are not replayed, and the result fully written before it stays as is.
func TestAggregateSlidingCountOverflowFatalWithZeroValue(t *testing.T) {
	const M = math.MaxInt64
	s := newCountState(1000, 600)
	// A window that closes and is written before the failing record.
	seedCountWindow(s, "sensor-a", 0, 1000, 1, 7)
	// The still-open window already holding the maximum number of events; its
	// sum is deliberately zero so a zero-value event cannot be a sum overflow.
	seedCountWindow(s, "sensor-a", 600, 1600, M, 0)

	input := strings.Join([]string{
		`{"type":"watermark","time":1000}`, // line 1: closes [0,1000)
		``,                                 // line 2: blank, still counts
		`{"type":"event","key":"sensor-a","time":1000,"value":0}`,  // line 3: count overflow
		`{"type":"event","key":"sensor-a","time":1100,"value":-5}`, // line 4: never read
		`{"type":"watermark","time":1600}`,                         // line 5: never read
	}, "\n")
	stdout, _, err := runSlidingState(s, input)
	if err == nil {
		t.Fatal("expected event count overflow, got nil")
	}
	inputErr, ok := err.(*InputError)
	if !ok {
		t.Fatalf("expected *InputError, got %T: %v", err, err)
	}
	if inputErr.Line != 3 {
		t.Errorf("line = %d, want 3 (blank lines must count)", inputErr.Line)
	}
	wantReason := `event count overflow for key "sensor-a" window [600,1600)`
	if inputErr.Reason != wantReason {
		t.Fatalf("reason = %q, want %q", inputErr.Reason, wantReason)
	}
	if got := err.Error(); got != "line 3: "+wantReason {
		t.Fatalf("error text = %q, want the physical line prefixed to the reason", got)
	}
	if got := s.windows[windowID{start: 600, key: "sensor-a"}]; got.count != M || got.sum != 0 {
		t.Errorf("rejected event must not change the full window, got count=%d sum=%d", got.count, got.sum)
	}
	// The pre-failure result stays, and nothing the later watermark would have
	// closed ([600,1600)) may be appended.
	want := `{"key":"sensor-a","start":0,"end":1000,"count":1,"sum":7}` + "\n"
	if stdout != want {
		t.Fatalf("existing output must be retained with nothing added:\n got: %q\nwant: %q", stdout, want)
	}
}

// The count is incremented for every accepted value sign: a negative-valued
// event at count M-1 still lands exactly on M (sum moves independently), and
// the following zero-valued event is then rejected as a count overflow --
// value zero can never masquerade as "no event".
func TestAggregateSlidingCountIncrementedForNegativeAndZeroValues(t *testing.T) {
	const M = math.MaxInt64
	s := newCountState(1000, 600)
	seedCountWindow(s, "sensor-a", 600, 1600, M-1, 41)

	input := strings.Join([]string{
		`{"type":"event","key":"sensor-a","time":1000,"value":-7}`, // count M-1 -> M, sum 41 -> 34
		`{"type":"event","key":"sensor-a","time":1100,"value":0}`,  // only [600,1600); count M -> overflow
		`{"type":"watermark","time":1600}`,                         // never read
	}, "\n")
	_, _, err := runSlidingState(s, input)
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
	wantReason := `event count overflow for key "sensor-a" window [600,1600)`
	if inputErr.Reason != wantReason {
		t.Fatalf("reason = %q, want %q", inputErr.Reason, wantReason)
	}
	got := s.windows[windowID{start: 600, key: "sensor-a"}]
	if got.count != M {
		t.Errorf("the negative-value event must have counted exactly once: count = %d, want %d", got.count, M)
	}
	if got.sum != 34 {
		t.Errorf("sum = %d, want 34 (41-7); the rejected zero-value event must not touch it", got.sum)
	}
}

// Counts of overlapping windows are independent. The same event lands in a
// window that can still take it and windows already at the limit; the failure
// names the window that actually goes over and must not blame the safe one.
// Containing windows are visited in ascending start order, so when more than
// one containing window is already full the earliest such start is named; the
// full windows reached only after that point stay exactly at the limit.
func TestAggregateSlidingCountOverflowNamesOnlyTheFullWindow(t *testing.T) {
	const M = math.MaxInt64
	s := newCountState(1000, 300)
	// The event at 1000 (window 1000, slide 300) belongs to [300,1300),
	// [600,1600) and [900,1900). The first can still take events; the other two
	// are already at the limit.
	seedCountWindow(s, "k", 300, 1300, 5, 0)
	seedCountWindow(s, "k", 600, 1600, M, 0)
	seedCountWindow(s, "k", 900, 1900, M, 0)

	input := strings.Join([]string{
		`{"type":"event","key":"k","time":1000,"value":0}`, // line 1: fails at [600,1600)
		`{"type":"event","key":"k","time":1001,"value":0}`, // line 2: never read
		`{"type":"watermark","time":1900}`,                 // line 3: never read
	}, "\n")
	stdout, _, err := runSlidingState(s, input)
	if err == nil {
		t.Fatal("expected event count overflow, got nil")
	}
	inputErr, ok := err.(*InputError)
	if !ok {
		t.Fatalf("expected *InputError, got %T: %v", err, err)
	}
	if inputErr.Line != 1 {
		t.Errorf("line = %d, want 1", inputErr.Line)
	}
	wantReason := `event count overflow for key "k" window [600,1600)`
	if inputErr.Reason != wantReason {
		t.Fatalf("reason = %q, want %q", inputErr.Reason, wantReason)
	}
	// The safe earlier window is not the offender, nor is the later full window.
	if strings.Contains(inputErr.Reason, "[300,1300)") {
		t.Errorf("reason must not blame the safe window [300,1300): %q", inputErr.Reason)
	}
	if strings.Contains(inputErr.Reason, "[900,1900)") {
		t.Errorf("reason must name the earliest full window, not [900,1900): %q", inputErr.Reason)
	}
	// Windows visited at or after the failure stay untouched at the limit.
	if full := s.windows[windowID{start: 600, key: "k"}]; full.count != M || full.sum != 0 {
		t.Errorf("[600,1600) must stay at the limit unchanged, got count=%d sum=%d", full.count, full.sum)
	}
	if later := s.windows[windowID{start: 900, key: "k"}]; later.count != M || later.sum != 0 {
		t.Errorf("[900,1900) must never be reached, got count=%d sum=%d", later.count, later.sum)
	}
	// The run aborts before line 3's watermark: every involved window is still
	// open, so none of them -- including the safe one -- can be emitted.
	if stdout != "" {
		t.Fatalf("no open window may be output after the failure, got %q", stdout)
	}
}

// A strictly late event is skipped before any window update: even when a
// still-open window it falls into already holds math.MaxInt64 events, it
// produces only the existing late-event notice -- never a count overflow --
// and changes neither count nor sum. An event exactly at the watermark is not
// late and is processed normally, so it is the one that overflows the count,
// even with value zero.
func TestAggregateSlidingCountOverflowLateEventSkippedFirst(t *testing.T) {
	const M = math.MaxInt64
	s := newCountState(1000, 600)
	wm := int64(1000)
	s.watermark = &wm
	// The still-open overlapping [600,1600) already at the count limit.
	seedCountWindow(s, "sensor-a", 600, 1600, M, 9)

	input := strings.Join([]string{
		`{"type":"event","key":"sensor-a","time":999,"value":0}`,  // line 1: strictly late, skipped
		`{"type":"event","key":"sensor-a","time":1000,"value":0}`, // line 2: equals watermark -> count overflow
		`{"type":"watermark","time":1600}`,                        // line 3: never read
	}, "\n")
	_, stderr, err := runSlidingState(s, input)
	if err == nil {
		t.Fatal("expected event count overflow at the watermark-equal event, got nil")
	}
	inputErr, ok := err.(*InputError)
	if !ok {
		t.Fatalf("expected *InputError, got %T: %v", err, err)
	}
	if inputErr.Line != 2 {
		t.Errorf("line = %d, want 2 (the strictly-late line 1 must be skipped first)", inputErr.Line)
	}
	wantReason := `event count overflow for key "sensor-a" window [600,1600)`
	if inputErr.Reason != wantReason {
		t.Fatalf("reason = %q, want %q", inputErr.Reason, wantReason)
	}
	wantNotice := "line 1: late event time=999 below current watermark 1000, skipped\n"
	if stderr != wantNotice {
		t.Fatalf("stderr = %q, want %q", stderr, wantNotice)
	}
	got := s.windows[windowID{start: 600, key: "sensor-a"}]
	if got.count != M {
		t.Errorf("late event must not change the count: got %d, want %d", got.count, M)
	}
	if got.sum != 9 {
		t.Errorf("late event must not change the sum: got %d, want 9", got.sum)
	}
}
