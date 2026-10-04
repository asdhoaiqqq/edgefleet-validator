package edgefleet

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"testing"
)

// This file guards aggregation when a single valid physical record is longer
// than one Read can carry: the line collector starts with a 64 KiB buffer and
// must compact and grow (64 -> 128 -> 256 KiB ...) without ever treating a
// read boundary as a record boundary. The long payload here is the event key
// itself, built from multibyte Chinese characters, so:
//
//   - a record over 64 KiB and one over 128 KiB must aggregate exactly like a
//     short one, with the whole key preserved;
//   - two long keys that share everything but their tail must not collapse
//     into one window the way a fixed-width truncation would;
//   - long and short records may alternate with blank lines, and a read may
//     carry one record's tail together with the next record's head, including
//     cuts in the middle of a Chinese character or next to a newline;
//   - a read failure that arrives before the long record's newline must drop
//     the whole tail (no event, no count, no late notice) while complete
//     records already terminated -- including by the same failing Read -- are
//     handled in order first, and the reader's own error must survive.
//
// Everything goes through the existing line-delimited JSON format and the
// public RunAggregate entry point (single-watermark fixed windows).

// longChineseKey builds a key of at least minBytes UTF-8 bytes out of Chinese
// characters (three bytes each), with suffix appended verbatim. Two keys made
// from the same base with different suffixes differ only at the tail, which is
// exactly the pair a fixed-width truncation would wrongly merge.
func longChineseKey(minBytes int, suffix string) string {
	var b strings.Builder
	for b.Len() < minBytes {
		b.WriteString("中事件密钥")
	}
	return b.String() + suffix
}

func eventRecord(key string, time, value int64) string {
	return fmt.Sprintf(`{"type":"event","key":%q,"time":%d,"value":%d}`, key, time, value)
}

func watermarkRecord(time int64) string {
	return fmt.Sprintf(`{"type":"watermark","time":%d}`, time)
}

// windowLine renders the expected compact output line; json.Marshal emits
// these CJK keys verbatim, so this matches AggregateResult's own encoding.
func windowLine(key string, start, end, count, sum int64) string {
	return fmt.Sprintf(`{"key":%q,"start":%d,"end":%d,"count":%d,"sum":%d}`+"\n", key, start, end, count, sum)
}

func parseWindowResults(t *testing.T, out string) []AggregateResult {
	t.Helper()
	if out == "" {
		return nil
	}
	var got []AggregateResult
	for _, line := range strings.Split(strings.TrimSuffix(out, "\n"), "\n") {
		var r AggregateResult
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			t.Fatalf("invalid output line %q: %v", line, err)
		}
		got = append(got, r)
	}
	return got
}

// longRecordSchedules are the read fragmentations worth re-running a long
// stream under: byte-at-a-time (splits every Chinese character), odd small
// sizes, and sizes straddling the collector's 64/128 KiB growth points.
func longRecordSchedules(total int) [][]int {
	return [][]int{
		uniformCuts(total, 1),
		uniformCuts(total, 3),
		uniformCuts(total, 4096),
		uniformCuts(total, 64*1024-1),
		uniformCuts(total, 64*1024),
		uniformCuts(total, 64*1024+1),
		uniformCuts(total, 128*1024),
		uniformCuts(total, 128*1024+7),
	}
}

// assertFixedWindowEverySchedule runs the 1000 ms fixed-window aggregation over
// a one-shot read and every given cut schedule, requiring identical output,
// late notices and success.
func assertFixedWindowEverySchedule(t *testing.T, input, wantOut, wantLate string, schedules [][]int) {
	t.Helper()
	check := func(t *testing.T, r io.Reader) {
		var out, late bytes.Buffer
		if err := RunAggregate(r, 1000, &out, &late); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if out.String() != wantOut {
			t.Fatalf("window output:\n got len %d %q\nwant len %d %q", out.Len(), out.String(), len(wantOut), wantOut)
		}
		if late.String() != wantLate {
			t.Fatalf("late notices:\n got %q\nwant %q", late.String(), wantLate)
		}
	}
	t.Run("oneshot", func(t *testing.T) { check(t, strings.NewReader(input)) })
	for i, cuts := range schedules {
		cuts := cuts
		t.Run(fmt.Sprintf("schedule%d", i), func(t *testing.T) { check(t, newChunkReader(input, cuts...)) })
	}
}

// TestLongRecordOver64KiB: one valid record longer than the collector's
// initial 64 KiB read buffer must aggregate like any other record. A second
// valid event with the same long key merges into the same window, and the key
// comes back whole.
func TestLongRecordOver64KiB(t *testing.T) {
	key := longChineseKey(64*1024+1, "")
	if len(key) <= 64*1024 {
		t.Fatalf("test key must exceed 64 KiB, got %d bytes", len(key))
	}
	first := eventRecord(key, 100, 5)
	if len(first) <= 64*1024 {
		t.Fatalf("record must exceed 64 KiB, got %d bytes", len(first))
	}
	input := strings.Join([]string{
		first,
		eventRecord(key, 200, 7), // same long key, second valid event
		watermarkRecord(1000),    // closes [0,1000) only when the watermark arrives
	}, "\n")
	want := windowLine(key, 0, 1000, 2, 12)

	// Pin the test renderer to the real output encoding for the long key.
	encoded, err := json.Marshal(AggregateResult{Key: key, Start: 0, End: 1000, Count: 2, Sum: 12})
	if err != nil {
		t.Fatalf("marshal expected result: %v", err)
	}
	if string(encoded)+"\n" != want {
		t.Fatalf("expected-line renderer drifted:\n got %q\nwant %q", string(encoded), want)
	}

	assertFixedWindowEverySchedule(t, input, want, "", longRecordSchedules(len(input)))

	// Independently of exact bytes, the parsed result keeps the full key.
	var out bytes.Buffer
	if err := RunAggregate(strings.NewReader(input), 1000, &out, io.Discard); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	results := parseWindowResults(t, out.String())
	if len(results) != 1 {
		t.Fatalf("got %d window results, want 1: %v", len(results), results)
	}
	if results[0].Key != key {
		t.Fatalf("key truncated or altered: got %d bytes, want %d bytes", len(results[0].Key), len(key))
	}
	if results[0].Count != 2 || results[0].Sum != 12 {
		t.Fatalf("merged count/sum = %d/%d, want 2/12", results[0].Count, results[0].Sum)
	}
}

// TestLongRecordOver128KiB forces the second buffer growth (128 -> 256 KiB).
// The long record crosses both fixed thresholds while carrying multibyte
// characters, and a later short record must still merge into the same window
// by its (short) key as well as keeping the long key's window separate.
func TestLongRecordOver128KiB(t *testing.T) {
	long := longChineseKey(128*1024+1, "")
	if len(long) <= 128*1024 {
		t.Fatalf("test key must exceed 128 KiB, got %d bytes", len(long))
	}
	first := eventRecord(long, 100, -3)
	if len(first) <= 128*1024 {
		t.Fatalf("record must exceed 128 KiB, got %d bytes", len(first))
	}
	input := strings.Join([]string{
		first,
		eventRecord(long, 900, 8), // same long key, merges: sum -3+8 = 5
		eventRecord("短", 200, 4),
		eventRecord("短", 300, 6),
		watermarkRecord(1000),
	}, "\n")
	// UTF-8 byte order puts the long key (prefix E4..) before "短" (E7..).
	want := windowLine(long, 0, 1000, 2, 5) + windowLine("短", 0, 1000, 2, 10)

	// Around the growth points; no byte-at-a-time run here, the 64 KiB case
	// already covers splitting every multibyte character.
	schedules := [][]int{
		uniformCuts(len(input), 4096),
		uniformCuts(len(input), 64*1024-1),
		uniformCuts(len(input), 64*1024),
		uniformCuts(len(input), 64*1024+1),
		uniformCuts(len(input), 128*1024-1),
		uniformCuts(len(input), 128*1024),
		uniformCuts(len(input), 128*1024+1),
	}
	assertFixedWindowEverySchedule(t, input, want, "", schedules)
}

// TestLongRecordAlternatesWithShortRecordsAndBlankLines interleaves long
// records, short records and blank physical lines around a watermark and a
// late event. Blank lines still count in the physical line numbering, so the
// late notice must name line 9 with the right event time and watermark under
// every fragmentation.
func TestLongRecordAlternatesWithShortRecordsAndBlankLines(t *testing.T) {
	long := longChineseKey(64*1024+1, "")
	lines := []string{
		"",                        // line 1 blank
		eventRecord("短", 100, 2),  // line 2
		"",                        // line 3 blank
		eventRecord(long, 200, 5), // line 4: long
		eventRecord("短", 300, 4),  // line 5: short
		eventRecord(long, 400, 7), // line 6: long, same key, merges
		"",                        // line 7 blank
		watermarkRecord(1000),     // line 8: closes [0,1000)
		eventRecord("短", 500, 9),  // line 9: late vs watermark 1000, skipped
		watermarkRecord(2000),     // line 10: [1000,2000) holds nothing
	}
	input := strings.Join(lines, "\n") + "\n\n" // terminate line 10, plus blank line 11
	wantOut := windowLine(long, 0, 1000, 2, 12) + windowLine("短", 0, 1000, 2, 6)
	wantLate := "line 9: late event time=500 below current watermark 1000, skipped\n"

	assertFixedWindowEverySchedule(t, input, wantOut, wantLate, longRecordSchedules(len(input)))

	// Explicit order/content check on the one-shot result.
	var out bytes.Buffer
	if err := RunAggregate(strings.NewReader(input), 1000, &out, io.Discard); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	results := parseWindowResults(t, out.String())
	if len(results) != 2 {
		t.Fatalf("got %d results, want 2: %v", len(results), results)
	}
	if results[0].Key != long || results[0].Count != 2 || results[0].Sum != 12 {
		t.Fatalf("long-key window wrong: %+v", results[0])
	}
	if results[1].Key != "短" || results[1].Count != 2 || results[1].Sum != 6 {
		t.Fatalf("short-key window wrong: %+v", results[1])
	}
}

// TestLongRecordTwoKeysDifferOnlyAtTail ensures the long key is not truncated
// to a fixed prefix: two keys sharing a >64 KiB Chinese prefix and differing in
// their final character must produce two windows, each preserving its key.
func TestLongRecordTwoKeysDifferOnlyAtTail(t *testing.T) {
	base := longChineseKey(64*1024+1, "")
	keyA := base + "甲"
	keyB := base + "乙"
	if len(keyA) <= 64*1024 || len(keyB) <= 64*1024 {
		t.Fatalf("both keys must exceed 64 KiB: %d, %d", len(keyA), len(keyB))
	}
	input := strings.Join([]string{
		eventRecord(keyA, 100, 3),
		eventRecord(keyB, 200, 8),
		watermarkRecord(1000),
	}, "\n")

	var refOut bytes.Buffer
	if err := RunAggregate(strings.NewReader(input), 1000, &refOut, io.Discard); err != nil {
		t.Fatalf("one-shot: unexpected error: %v", err)
	}
	results := parseWindowResults(t, refOut.String())
	if len(results) != 2 {
		t.Fatalf("tail-distinct long keys must yield two windows, got %d: %v", len(results), results)
	}
	byKey := map[string]AggregateResult{}
	for _, r := range results {
		byKey[r.Key] = r
	}
	ra, okA := byKey[keyA]
	rb, okB := byKey[keyB]
	if !okA || !okB {
		t.Fatalf("keys were truncated or merged: got keys %q and %q", results[0].Key, results[1].Key)
	}
	if ra.Count != 1 || ra.Sum != 3 || rb.Count != 1 || rb.Sum != 8 {
		t.Fatalf("counts/sums merged wrongly: %+v %+v", ra, rb)
	}

	for i, cuts := range longRecordSchedules(len(input)) {
		cuts := cuts
		t.Run(fmt.Sprintf("schedule%d", i), func(t *testing.T) {
			var out, late bytes.Buffer
			if err := RunAggregate(newChunkReader(input, cuts...), 1000, &out, &late); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if out.String() != refOut.String() {
				t.Fatalf("fragmented delivery changed the two-window result:\n got len %d\nwant len %d", out.Len(), refOut.Len())
			}
		})
	}
}

// TestLongRecordCutsInsideMultibyteAndNearDelimiters places two-read cuts and
// one combined multi-cut schedule at the spots most likely to expose a
// record-boundary bug: right on either side of every newline (so one read can
// bring a record tail and the next record's head together), around the long
// record's first bytes, and at the collector's 64/128 KiB growth points where
// a cut lands inside a Chinese character.
func TestLongRecordCutsInsideMultibyteAndNearDelimiters(t *testing.T) {
	long := longChineseKey(128*1024+1, "")
	head := eventRecord("短", 100, 2)
	lines := []string{
		head,
		eventRecord(long, 200, 5),
		eventRecord(long, 300, 7),
		watermarkRecord(1000),
	}
	input := strings.Join(lines, "\n") + "\n"

	var refOut, refLate bytes.Buffer
	if err := RunAggregate(strings.NewReader(input), 1000, &refOut, &refLate); err != nil {
		t.Fatalf("one-shot: unexpected error: %v", err)
	}
	want := refOut.String()

	offsets := map[int]bool{}
	add := func(x int) {
		if x > 0 && x < len(input) {
			offsets[x] = true
		}
	}
	for i := 0; i < len(input); i++ {
		if input[i] == '\n' {
			for d := -3; d <= 3; d++ {
				add(i + 1 + d)
			}
		}
	}
	// First bytes of the first long record (cuts inside its opening Chinese rune).
	longStart := len(head) + 1
	for d := -2; d <= 4; d++ {
		add(longStart + d)
	}
	// Collector compaction/growth thresholds.
	for _, b := range []int{
		64*1024 - 2, 64*1024 - 1, 64 * 1024, 64*1024 + 1, 64*1024 + 2,
		128*1024 - 2, 128*1024 - 1, 128 * 1024, 128*1024 + 1, 128*1024 + 2,
	} {
		add(b)
	}
	sorted := make([]int, 0, len(offsets))
	for at := range offsets {
		sorted = append(sorted, at)
	}
	sort.Ints(sorted)

	for _, at := range sorted {
		at := at
		t.Run(fmt.Sprintf("cut@%d", at), func(t *testing.T) {
			var out, late bytes.Buffer
			if err := RunAggregate(newChunkReader(input, at), 1000, &out, &late); err != nil {
				t.Fatalf("cut %d: unexpected error: %v", at, err)
			}
			if out.String() != want {
				t.Fatalf("cut %d changed output:\n got len %d\nwant len %d", at, out.Len(), len(want))
			}
			if late.String() != refLate.String() {
				t.Fatalf("cut %d changed late notices: %q", at, late.String())
			}
		})
	}
	t.Run("all boundary cuts in one stream", func(t *testing.T) {
		var out, late bytes.Buffer
		if err := RunAggregate(newChunkReader(input, sorted...), 1000, &out, &late); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if out.String() != want {
			t.Fatalf("combined boundary cuts changed output:\n got len %d\nwant len %d", out.Len(), len(want))
		}
	})
}

// TestLongRecordReadErrorDropsUnterminatedTail: when a real read error arrives
// before the long record's terminating newline, the partial long record must
// never become an event -- it neither counts nor (being behind the watermark)
// raises a late notice -- while windows already output stay in place and the
// reader's original error is what the caller sees.
func TestLongRecordReadErrorDropsUnterminatedTail(t *testing.T) {
	long := longChineseKey(64*1024+1, "")
	prefix := strings.Join([]string{
		eventRecord("短", 100, 2),
		watermarkRecord(1000), // line 2 closes [0,1000)
	}, "\n") + "\n"
	// Line 3 is a long event the watermark would judge late; it is delivered
	// without its newline and must therefore be ignored entirely.
	tail := eventRecord(long, 100, 5)
	full := prefix + tail
	wantOut := windowLine("短", 0, 1000, 1, 2)

	// Failure offsets relative to the long tail's start. The first points sit
	// inside the key's UTF-8 encoding (a rune boundary is every 3 bytes):
	// inside its first Chinese character, its middle and its last character,
	// so the dropped tail is a genuinely multibyte partial record.
	keyStart := len(`{"type":"event","key":"`)
	tailPoints := []int{
		1, 4095,
		keyStart + 1,
		keyStart + len(long)/2,
		keyStart + len(long) - 1,
		len(tail) - 3,
		len(tail), // every tail byte delivered, still no newline
	}
	var failPoints []int
	for _, at := range tailPoints {
		if at >= 1 && at <= len(tail) {
			failPoints = append(failPoints, len(prefix)+at)
		}
	}
	deliveries := []struct {
		name          string
		errOnBoundary bool
		chunk         int
	}{
		{"error with last bytes", true, 1 << 20},
		{"error on next read", false, 1 << 20},
		{"one byte at a time, error with last byte", true, 1},
	}
	for _, fp := range failPoints {
		fp := fp
		for _, dl := range deliveries {
			dl := dl
			name := fmt.Sprintf("tail+%d/%s", fp-len(prefix), dl.name)
			t.Run(name, func(t *testing.T) {
				r := &failingAfterReader{
					data: []byte(full), failAt: fp,
					err: errSentinelChunkedRead, errOnBoundary: dl.errOnBoundary, chunk: dl.chunk,
				}
				var out, late bytes.Buffer
				err := RunAggregate(r, 1000, &out, &late)
				if !errors.Is(err, errSentinelChunkedRead) {
					t.Fatalf("error = %v, want the reader's original error via errors.Is", err)
				}
				var ie *InputError
				if errors.As(err, &ie) {
					t.Fatalf("unterminated long tail must not be reported as an input error: %v", err)
				}
				if out.String() != wantOut {
					t.Fatalf("already closed window must be retained and nothing new emitted:\n got %q\nwant %q", out.String(), wantOut)
				}
				if late.String() != "" {
					t.Fatalf("unterminated long event must produce no late notice: %q", late.String())
				}
			})
		}
	}
}

// TestLongRecordReadErrorCompleteLongLineProcessedFirst covers the ordering
// rule when bytes and an error come back together: a complete long record
// whose newline IS already in the same batch must be handled first (here it is
// late, so its exact physical line number, event time and watermark appear in
// the notice), while the next long record's unterminated tail is dropped
// without a second notice; the closed window stays and the read error wins.
func TestLongRecordReadErrorCompleteLongLineProcessedFirst(t *testing.T) {
	long := longChineseKey(64*1024+1, "")
	complete := strings.Join([]string{
		eventRecord("短", 100, 2),  // line 1
		watermarkRecord(1000),     // line 2 closes [0,1000)
		eventRecord(long, 100, 5), // line 3: complete long line, late
	}, "\n") + "\n"
	unterminated := eventRecord(long, 200, 7) // line 4: tail without newline
	full := complete + unterminated
	wantOut := windowLine("短", 0, 1000, 1, 2)
	wantLate := "line 3: late event time=100 below current watermark 1000, skipped\n"

	check := func(t *testing.T, r io.Reader) {
		var out, late bytes.Buffer
		err := RunAggregate(r, 1000, &out, &late)
		if !errors.Is(err, errSentinelChunkedRead) {
			t.Fatalf("error = %v, want the reader's original error", err)
		}
		if _, ok := err.(*InputError); ok {
			t.Fatalf("read failure must not become an *InputError: %v", err)
		}
		if out.String() != wantOut {
			t.Fatalf("closed window retained, tail must add nothing:\n got %q\nwant %q", out.String(), wantOut)
		}
		if late.String() != wantLate {
			t.Fatalf("only the newline-terminated long line may be reported late:\n got %q\nwant %q", late.String(), wantLate)
		}
		if strings.Count(late.String(), "\n") != 1 {
			t.Fatalf("the unterminated tail produced an extra late notice: %q", late.String())
		}
	}

	t.Run("newline and error in the same read", func(t *testing.T) {
		check(t, &failingAfterReader{
			data: []byte(full), failAt: len(full),
			err: errSentinelChunkedRead, errOnBoundary: true,
		})
	})
	t.Run("error alone on the next read", func(t *testing.T) {
		check(t, &failingAfterReader{
			data: []byte(full), failAt: len(full),
			err: errSentinelChunkedRead,
		})
	})
	t.Run("one byte at a time through both long records", func(t *testing.T) {
		check(t, &failingAfterReader{
			data: []byte(full), failAt: len(full),
			err: errSentinelChunkedRead, errOnBoundary: true, chunk: 1,
		})
	})
	t.Run("failure cuts inside the tail's last Chinese character", func(t *testing.T) {
		// The unterminated line ends with ASCII JSON syntax; step back over
		// its closing fields so the last delivered byte splits the long key's
		// final three-byte rune.
		suffix := len(`","time":200,"value":7}`)
		check(t, &failingAfterReader{
			data: []byte(full), failAt: len(full) - suffix - 1,
			err: errSentinelChunkedRead, errOnBoundary: true,
		})
	})
}

// TestLongRecordCleanEOFDoesNotFlushOpenWindows pins normal end-of-input
// behavior with long records: a long event sitting in a window the watermark
// has not reached stays unemitted at a clean EOF, even as the already closed
// window is output, regardless of fragmentation.
func TestLongRecordCleanEOFDoesNotFlushOpenWindows(t *testing.T) {
	long := longChineseKey(64*1024+1, "")

	t.Run("only an open long window", func(t *testing.T) {
		input := eventRecord(long, 1500, 9) // window [1000,2000), no newline, no watermark
		for name, r := range map[string]io.Reader{
			"oneshot":  strings.NewReader(input),
			"bytewise": newChunkReader(input, uniformCuts(len(input), 1)...),
		} {
			name, r := name, r
			t.Run(name, func(t *testing.T) {
				var out, late bytes.Buffer
				if err := RunAggregate(r, 1000, &out, &late); err != nil {
					t.Fatalf("clean EOF must succeed: %v", err)
				}
				if out.String() != "" {
					t.Fatalf("open windows must not be emitted at EOF: %q", out.String()[:min(200, out.Len())])
				}
				if late.String() != "" {
					t.Fatalf("unexpected late notice: %q", late.String())
				}
			})
		}
	})

	t.Run("closed long window emitted, later long window stays open", func(t *testing.T) {
		input := strings.Join([]string{
			eventRecord(long, 200, 5),
			watermarkRecord(1000), // closes [0,1000)
			eventRecord(long, 1500, 9),
		}, "\n") // last long record unterminated at clean EOF
		want := windowLine(long, 0, 1000, 1, 5)
		assertFixedWindowEverySchedule(t, input, want, "", [][]int{
			uniformCuts(len(input), 1),
			uniformCuts(len(input), 64*1024),
			uniformCuts(len(input), 64*1024+1),
		})
	})
}
