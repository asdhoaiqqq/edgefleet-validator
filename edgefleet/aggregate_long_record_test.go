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

// This file guards long physical records for the legacy single-watermark
// fixed-window entry point RunAggregate. The line collector starts with a
// 64 KiB buffer and grows it, so a single valid record longer than 64 KiB or
// 128 KiB -- long because its event key is long, with multibyte (Chinese)
// characters -- must aggregate exactly like short records: same-key events
// merge with the right count and sum and the whole key is preserved, two keys
// differing only at the tail stay two windows, windows close only when the
// watermark reaches their end (never early or twice just because a line was
// long), and the byte-identical stream must give byte-identical windows, late
// notices and success/failure no matter how it is split across reads,
// including cuts inside a multibyte character or on the line delimiter.
//
// Read-failure semantics with a long line in flight get their own checks: the
// unterminated tail of a long record is never an event (no count, no late
// notice), previously emitted windows stay, and the reader's own error
// surfaces; complete records whose newline already arrived -- even in the
// same Read that carried the error -- are processed in order first.

const (
	longKeyPrefix  = "长传感器记录-"
	longKeyBlock   = "温湿度读数" // 6 runes, 18 UTF-8 bytes per repetition
	longKeySuffixA = "-甲"    // key A ending
	longKeySuffixB = "-乙"    // key B: identical length and prefix, differs only at the tail
)

// longKey builds a key whose repeated multibyte middle makes records using it
// longer than thresholdBytes. Suffix A and B keys have equal length and share
// every byte except the final rune, so a collector that truncated by length
// would wrongly merge the two.
func longKey(suffix string, thresholdBytes int) string {
	repeat := thresholdBytes/len(longKeyBlock) + 40
	return longKeyPrefix + strings.Repeat(longKeyBlock, repeat) + suffix
}

// longRecordScenario holds the reference fixed-window stream and its expected
// results. Window length is 1000ms; physical layout is:
//
//	line  1: blank
//	line  2: event key 短 time=100 value=2
//	line  3: watermark 1000                      -> closes [0,1000)
//	line  4: blank
//	line  5: LONG event keyA time=1100 value=10
//	line  6: short event key 短 time=1150 value=4
//	line  7: LONG event keyA time=1200 value=5
//	line  8: blank
//	line  9: LONG event keyB time=1150 value=7
//	line 10: short event key 短 time=100 value=8  -> late vs watermark 1000, skipped
//	line 11: watermark 2000                      -> closes [1000,2000)
//	line 12: blank
//	line 13: late event time=1500                -> late vs watermark 2000, skipped
//	line 14: event time=2500 value=99            -> open window, must never be emitted
//
// Long and short records alternate around the two 64 KiB+ lines keyA lines,
// blanks flank them, one read can carry the end of a short record together
// with the start of a long one, and both late notices sit after long records
// so their physical line numbers must survive buffer growth.
type longRecordScenario struct {
	input     string
	keyA      string
	keyB      string
	longLines []int // physical line numbers that carry the long records
	wantOut   string
	wantLate  string
}

func buildLongRecordScenario(t *testing.T, thresholdBytes int) longRecordScenario {
	t.Helper()
	keyA := longKey(longKeySuffixA, thresholdBytes)
	keyB := longKey(longKeySuffixB, thresholdBytes)
	ev := func(key string, time, value int) string {
		return fmt.Sprintf(`{"type":"event","key":%q,"time":%d,"value":%d}`, key, time, value)
	}
	lines := []string{
		``, // 1 blank
		ev("短", 100, 2),
		`{"type":"watermark","time":1000}`,
		``, // 4 blank
		ev(keyA, 1100, 10),
		ev("短", 1150, 4),
		ev(keyA, 1200, 5),
		``, // 8 blank
		ev(keyB, 1150, 7),
		ev("短", 100, 8), // 10 late below watermark 1000
		`{"type":"watermark","time":2000}`,
		``,                  // 12 blank
		ev("迟到事件", 1500, 3), // 13 late below watermark 2000
		ev("不输出窗口", 2500, 99),
	}
	input := strings.Join(lines, "\n") + "\n"

	// The long lines must actually cross the threshold; pin that rather than
	// trusting the repeat arithmetic silently.
	lineAt := func(n int) string {
		start, end := physicalLineRange(input, n)
		return input[start:end]
	}
	for _, n := range []int{5, 7, 9} {
		if len(lineAt(n)) <= thresholdBytes {
			t.Fatalf("line %d is %d bytes, want a record longer than %d", n, len(lineAt(n)), thresholdBytes)
		}
	}
	if len(keyA) != len(keyB) {
		t.Fatalf("test keys must be equal length: %d vs %d", len(keyA), len(keyB))
	}
	if !strings.HasPrefix(keyB, keyA[:len(keyA)-len(longKeySuffixA)]) {
		t.Fatalf("key B must share key A's full prefix and differ only at the tail")
	}

	// Expected windows, ordered by end then key in UTF-8 byte order. Derive the
	// [1000,2000) key order from the keys themselves instead of hardcoding it:
	// 短 sorts before the 长-prefixed long keys, and suffix 乙 (E4 B9 99) sorts
	// before suffix 甲 (E7 94 B2).
	first := AggregateResult{Key: "短", Start: 0, End: 1000, Count: 1, Sum: 2}
	secondGroup := []AggregateResult{
		{Key: keyA, Start: 1000, End: 2000, Count: 2, Sum: 15},
		{Key: keyB, Start: 1000, End: 2000, Count: 1, Sum: 7},
		{Key: "短", Start: 1000, End: 2000, Count: 1, Sum: 4},
	}
	sort.Slice(secondGroup, func(i, j int) bool { return secondGroup[i].Key < secondGroup[j].Key })
	var b strings.Builder
	for _, r := range append([]AggregateResult{first}, secondGroup...) {
		encoded, err := json.Marshal(r)
		if err != nil {
			t.Fatalf("marshal expected result: %v", err)
		}
		b.Write(encoded)
		b.WriteByte('\n')
	}
	return longRecordScenario{
		input:     input,
		keyA:      keyA,
		keyB:      keyB,
		longLines: []int{5, 7, 9},
		wantOut:   b.String(),
		wantLate: "line 10: late event time=100 below current watermark 1000, skipped\n" +
			"line 13: late event time=1500 below current watermark 2000, skipped\n",
	}
}

// physicalLineRange returns the byte offsets [start,end) of physical line n
// (1-based), excluding its trailing '\n'. Blank lines yield start == end.
func physicalLineRange(input string, n int) (start, end int) {
	line := 1
	start = 0
	for i := 0; i < len(input); i++ {
		if input[i] == '\n' {
			if line == n {
				return start, i
			}
			line++
			start = i + 1
		}
	}
	if line == n {
		return start, len(input)
	}
	return 0, 0
}

// runFixedOnce runs RunAggregate over the given delivery of the stream.
func runFixedOnce(t *testing.T, r io.Reader) (string, string, error) {
	t.Helper()
	var out, late bytes.Buffer
	err := RunAggregate(r, 1000, &out, &late)
	return out.String(), late.String(), err
}

// assertReferenceResult checks the full invariant: windows, late notices,
// success, no early/duplicate window rows and no flush of the still-open
// window, plus full key fidelity for the long keys.
func assertReferenceResult(t *testing.T, sc longRecordScenario, out, late string, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out != sc.wantOut {
		t.Fatalf("window output mismatch:\n got: %q\nwant: %q", out, sc.wantOut)
	}
	if late != sc.wantLate {
		t.Fatalf("late notice mismatch:\n got: %q\nwant: %q", late, sc.wantLate)
	}
	if strings.Contains(out, `"end":3000`) {
		t.Fatalf("the still-open window of the time=2500 event must not flush at end of input: %q", out)
	}

	// Every output object must parse; the long keys must come through whole
	// (not truncated, not mojibake) and the tail-differing keys must be
	// separate rows.
	byKey := map[string][]AggregateResult{}
	var ends []int64
	for i, line := range strings.Split(strings.TrimSuffix(out, "\n"), "\n") {
		var r AggregateResult
		if jerr := json.Unmarshal([]byte(line), &r); jerr != nil {
			t.Fatalf("output line %d is not valid JSON: %v\nline: %.80q...", i+1, jerr, line)
		}
		byKey[r.Key] = append(byKey[r.Key], r)
		ends = append(ends, r.End)
	}
	gotA, okA := byKey[sc.keyA]
	gotB, okB := byKey[sc.keyB]
	if !okA || !okB {
		t.Fatalf("long keys must be preserved in full as separate rows; got key A present=%v, key B present=%v", okA, okB)
	}
	if len(gotA) != 1 || gotA[0].Count != 2 || gotA[0].Sum != 15 {
		t.Fatalf("key A window = %+v, want one [1000,2000) row count=2 sum=15", gotA)
	}
	if len(gotB) != 1 || gotB[0].Count != 1 || gotB[0].Sum != 7 {
		t.Fatalf("key B window = %+v, want one [1000,2000) row count=1 sum=7", gotB)
	}
	if !sort.SliceIsSorted(ends, func(i, j int) bool { return ends[i] < ends[j] }) {
		t.Fatalf("window ends must be emitted ascending: %v", ends)
	}
}

// longRecordDeliverySchedules builds read schedules that stress the buffer's
// growth path, multibyte boundaries and line delimiters for this input:
// one-shot, tiny uniform reads, reads aligned (and off by one/two) with the
// 64 KiB and 128 KiB growth boundaries, and targeted two-read cuts on the
// newline before/after each long line.
func longRecordDeliverySchedules(t *testing.T, sc longRecordScenario, tinySizes []int) map[string][]int {
	t.Helper()
	schedules := map[string][]int{
		"one-shot": nil,
	}
	for _, size := range tinySizes {
		schedules[fmt.Sprintf("uniform-%d", size)] = uniformCuts(len(sc.input), size)
	}
	// Cuts around the 128 KiB boundary only matter (and are cheap enough) for
	// the >128 KiB scenario.
	for _, boundary := range []int{64 * 1024, 128 * 1024} {
		any := false
		for _, n := range sc.longLines {
			start, end := physicalLineRange(sc.input, n)
			if end-start <= boundary {
				continue
			}
			any = true
			for _, d := range []int{-2, -1, 0, 1, 2} {
				at := start + boundary + d
				schedules[fmt.Sprintf("longline%d-cut@%d", n, at)] = []int{at}
			}
		}
		if !any {
			break
		}
	}
	// Delimiter neighborhoods: split one byte before/on/after the newline of
	// every long line, and inside the first multibyte character of its key.
	for _, n := range sc.longLines {
		start, end := physicalLineRange(sc.input, n) // end is the '\n' index
		for _, at := range []int{
			start + 24, start + 25, // inside the first 3-byte rune of the key
			end - 1, end, end + 1, end + 2, // around the line delimiter
		} {
			if at > 0 && at < len(sc.input) {
				schedules[fmt.Sprintf("longline%d-near@%d", n, at)] = []int{at}
			}
		}
	}
	return schedules
}

// TestAggregateLongRecordOver64KiB runs the reference scenario with records
// longer than 64 KiB under one-byte reads (every multibyte character and every
// buffer growth is split), every other fragmentation schedule, and verifies
// the full fixed-window contract through the public RunAggregate entry point.
func TestAggregateLongRecordOver64KiB(t *testing.T) {
	sc := buildLongRecordScenario(t, 64*1024)

	// One-shot reference.
	out, late, err := runFixedOnce(t, strings.NewReader(sc.input))
	assertReferenceResult(t, sc, out, late, err)

	t.Run("single byte reads", func(t *testing.T) {
		cuts := make([]int, 0, len(sc.input)-1)
		for at := 1; at < len(sc.input); at++ {
			cuts = append(cuts, at)
		}
		out, late, err := runFixedOnce(t, newChunkReader(sc.input, cuts...))
		assertReferenceResult(t, sc, out, late, err)
	})

	for name, cuts := range longRecordDeliverySchedules(t, sc,
		[]int{2, 3, 7, 64, 4096, 65535, 65536, 65537}) {
		name, cuts := name, cuts
		if name == "one-shot" {
			continue
		}
		t.Run(name, func(t *testing.T) {
			out, late, err := runFixedOnce(t, newChunkReader(sc.input, cuts...))
			assertReferenceResult(t, sc, out, late, err)
		})
	}
}

// TestAggregateLongRecordOver128KiB repeats the guarantee with records longer
// than 128 KiB: the collector must grow past 64 KiB more than once while
// preserving the same bytes, order, windows and notices.
func TestAggregateLongRecordOver128KiB(t *testing.T) {
	sc := buildLongRecordScenario(t, 128*1024)

	out, late, err := runFixedOnce(t, strings.NewReader(sc.input))
	assertReferenceResult(t, sc, out, late, err)

	// Past 128 KiB the single-byte schedule is covered by the 64 KiB test;
	// here use sizes that still force every buffer doubling and split
	// multibyte runes without an O(n^2) run.
	for name, cuts := range longRecordDeliverySchedules(t, sc,
		[]int{3, 4096, 32768, 65536, 65537, 131072, 131073}) {
		name, cuts := name, cuts
		t.Run(name, func(t *testing.T) {
			out, late, err := runFixedOnce(t, newChunkReader(sc.input, cuts...))
			assertReferenceResult(t, sc, out, late, err)
		})
	}
}

// TestAggregateLongRecordKeysDistinctAtTail is the focused guard against
// truncation-by-length: the two long keys share every byte except the final
// rune, and they must never collapse into one window row.
func TestAggregateLongRecordKeysDistinctAtTail(t *testing.T) {
	for _, threshold := range []int{64 * 1024, 128 * 1024} {
		sc := buildLongRecordScenario(t, threshold)
		out, _, err := runFixedOnce(t, strings.NewReader(sc.input))
		if err != nil {
			t.Fatalf("threshold %d: unexpected error: %v", threshold, err)
		}
		if strings.Count(out, sc.keyA[:20]) < 2 {
			t.Fatalf("threshold %d: expected both long keys in the output, got:\n%s", threshold, out)
		}
		if strings.Count(out, `"start":1000,"end":2000`) != 3 {
			t.Fatalf("threshold %d: expected exactly three [1000,2000) rows (key A, key B, 短), got:\n%s", threshold, out)
		}
	}
}

// TestAggregateLongRecordReadErrorDropsTail: when a genuine read failure
// arrives while a long record is still missing its newline, the partial tail
// must never become an event -- no count, no window, no late notice -- no
// matter where the cut lands (including inside a Chinese rune or after the
// buffer grew past 64/128 KiB), whether the error comes with the last bytes or
// on the following read. Windows already closed by earlier complete records
// stay written, and the caller gets the reader's own error.
func TestAggregateLongRecordReadErrorDropsTail(t *testing.T) {
	ev := func(key string, time, value int) string {
		return fmt.Sprintf(`{"type":"event","key":%q,"time":%d,"value":%d}`, key, time, value)
	}
	wantFirst := `{"key":"短","start":0,"end":1000,"count":1,"sum":2}` + "\n"

	for _, threshold := range []int{64 * 1024, 128 * 1024} {
		threshold := threshold
		t.Run(fmt.Sprintf("over%d", threshold), func(t *testing.T) {
			keyA := longKey(longKeySuffixA, threshold)
			head := strings.Join([]string{
				``,
				ev("短", 100, 2),
				`{"type":"watermark","time":1000}`, // line 3 closes [0,1000)
				``,
			}, "\n") + "\n"
			tail := ev(keyA, 1100, 10) // line 5, deliberately unterminated

			// Failure points chosen relative to the record's layout: the JSON
			// prefix {"type":"event","key":" is 23 bytes, then the 22-byte key
			// prefix, then 18-byte blocks of 3-byte runes -- so a cut at block
			// start +1 lands in the middle of a Chinese rune.
			keyStart := len(head) + len(`{"type":"event","key":"`)
			inRuneAt := func(recordOffset int) int {
				q := (recordOffset - (keyStart + len(longKeyPrefix))) / len(longKeyBlock)
				return keyStart + len(longKeyPrefix) + q*len(longKeyBlock) + 1
			}
			cutPoints := []int{
				len(head) + len(tail),              // whole unterminated record delivered, error afterwards
				len(head) + len(tail) - 1,          // cut at the final '}'
				inRuneAt(keyStart + 64*1024),       // inside a rune just past 64 KiB
				len(head) + 23 + len(keyA) - 3 + 1, // middle of the key's final rune 甲
			}
			if threshold > 64*1024 {
				cutPoints = append(cutPoints, inRuneAt(keyStart+128*1024)) // inside a rune past 128 KiB
			}
			type delivery struct {
				name          string
				chunk         int
				errOnBoundary bool
			}
			deliveries := []delivery{
				{"error on following read", 0, false},
				{"bytes and error together", 0, true},
				{"fragmented then error together", 4096, true},
			}
			if threshold == 64*1024 {
				deliveries = append(deliveries, delivery{"one byte per read then error", 1, true})
			}

			for _, failAt := range cutPoints {
				for _, dl := range deliveries {
					failAt, dl := failAt, dl
					name := fmt.Sprintf("failAt%d/%s", failAt, dl.name)
					t.Run(name, func(t *testing.T) {
						if failAt < 0 || failAt > len(head)+len(tail) {
							t.Skip("cut point beyond this key size")
						}
						var out, late bytes.Buffer
						r := &failingAfterReader{
							data:          []byte(head + tail),
							failAt:        failAt,
							err:           errSentinelChunkedRead,
							chunk:         dl.chunk,
							errOnBoundary: dl.errOnBoundary,
						}
						err := RunAggregate(r, 1000, &out, &late)
						if !errors.Is(err, errSentinelChunkedRead) {
							t.Fatalf("error = %v, want the reader's original error via errors.Is", err)
						}
						if _, ok := err.(*InputError); ok {
							t.Fatalf("the partial long tail must not be reported as an input error: %v", err)
						}
						if out.String() != wantFirst {
							t.Fatalf("only the previously closed window may be present:\n got: %q\nwant: %q", out.String(), wantFirst)
						}
						if late.String() != "" {
							t.Fatalf("unterminated long tail must not produce a late notice: %q", late.String())
						}
					})
				}
			}
		})
	}
}

// TestAggregateLongRecordReadErrorProcessesCompletedLines: when the failing
// Read also delivers complete records -- a newline-terminated long event and
// the watermark that closes its window -- those must be processed in order
// first, while an unterminated long late-event tail behind them produces no
// notice and no window. The reader error is what finally surfaces.
func TestAggregateLongRecordReadErrorProcessesCompletedLines(t *testing.T) {
	ev := func(key string, time, value int) string {
		return fmt.Sprintf(`{"type":"event","key":%q,"time":%d,"value":%d}`, key, time, value)
	}
	completed := strings.Join([]string{
		ev("短", 100, 2),
		`{"type":"watermark","time":1000}`, // closes [0,1000)
		``,
	}, "\n") + "\n"
	wantFirst := `{"key":"短","start":0,"end":1000,"count":1,"sum":2}` + "\n"

	for _, threshold := range []int{64 * 1024, 128 * 1024} {
		threshold := threshold
		t.Run(fmt.Sprintf("over%d", threshold), func(t *testing.T) {
			keyA := longKey(longKeySuffixA, threshold)
			longEvent := ev(keyA, 1100, 10)
			// A long late event as the final unterminated record (no closing
			// brace, no newline): it would be judged late against watermark
			// 2000, but without its newline it must never be judged at all.
			lateTail := ev(keyA, 1500, 9)
			lateTail = lateTail[:len(lateTail)-1]
			full := completed + longEvent + "\n" +
				`{"type":"watermark","time":2000}` + "\n" +
				lateTail

			want := wantFirst + mustMarshalWindow(t, AggregateResult{
				Key: keyA, Start: 1000, End: 2000, Count: 1, Sum: 10,
			})

			type delivery struct {
				name          string
				chunk         int
				errOnBoundary bool
			}
			deliveries := []delivery{
				{"all bytes and error in one read", 0, true},
				{"tail delivered clean, error next read", 0, false},
				{"4 KiB reads, error with last bytes", 4096, true},
			}
			if threshold == 64*1024 {
				deliveries = append(deliveries, delivery{"one byte per read, error with last byte", 1, true})
			}
			for _, dl := range deliveries {
				dl := dl
				t.Run(dl.name, func(t *testing.T) {
					var out, late bytes.Buffer
					r := &failingAfterReader{
						data:          []byte(full),
						failAt:        len(full),
						err:           errSentinelChunkedRead,
						chunk:         dl.chunk,
						errOnBoundary: dl.errOnBoundary,
					}
					err := RunAggregate(r, 1000, &out, &late)
					if !errors.Is(err, errSentinelChunkedRead) {
						t.Fatalf("error = %v, want the reader's original error", err)
					}
					if out.String() != want {
						t.Fatalf("complete records up to the newlines must be processed in order first:\n got: %q\nwant: %q", out.String(), want)
					}
					if late.String() != "" {
						t.Fatalf("the unterminated long late tail must produce no notice: %q", late.String())
					}
				})
			}
		})
	}
}

// TestAggregateLongRecordCleanEOFKeepsFinalRecord pins the clean-end rule with
// a long record in the stream: a closing watermark that arrives as the
// unterminated final line at a bare io.EOF still closes the long key's window;
// no read failure is invented. Delivery one byte at a time must be identical.
func TestAggregateLongRecordCleanEOFKeepsFinalRecord(t *testing.T) {
	ev := func(key string, time, value int) string {
		return fmt.Sprintf(`{"type":"event","key":%q,"time":%d,"value":%d}`, key, time, value)
	}
	for _, threshold := range []int{64 * 1024, 128 * 1024} {
		threshold := threshold
		t.Run(fmt.Sprintf("over%d", threshold), func(t *testing.T) {
			keyA := longKey(longKeySuffixA, threshold)
			input := strings.Join([]string{
				ev("短", 100, 2),
				`{"type":"watermark","time":1000}`,
				ev(keyA, 1100, 10),
			}, "\n") + "\n" + `{"type":"watermark","time":2000}` // final record has no newline
			want := `{"key":"短","start":0,"end":1000,"count":1,"sum":2}` + "\n" +
				mustMarshalWindow(t, AggregateResult{Key: keyA, Start: 1000, End: 2000, Count: 1, Sum: 10})

			check := func(t *testing.T, r io.Reader) {
				var out, late bytes.Buffer
				if err := RunAggregate(r, 1000, &out, &late); err != nil {
					t.Fatalf("clean EOF must not fail: %v", err)
				}
				if out.String() != want {
					t.Fatalf("clean EOF output:\n got: %q\nwant: %q", out.String(), want)
				}
				if late.String() != "" {
					t.Fatalf("unexpected late notices: %q", late.String())
				}
			}
			t.Run("one-shot", func(t *testing.T) { check(t, strings.NewReader(input)) })
			if threshold == 64*1024 {
				t.Run("one byte per read", func(t *testing.T) {
					cuts := make([]int, 0, len(input)-1)
					for at := 1; at < len(input); at++ {
						cuts = append(cuts, at)
					}
					check(t, newChunkReader(input, cuts...))
				})
			}
			t.Run("64 KiB-aligned reads", func(t *testing.T) {
				check(t, newChunkReader(input, uniformCuts(len(input), 64*1024)...))
			})
		})
	}
}

// mustMarshalWindow renders one expected output line (JSON object + newline).
func mustMarshalWindow(t *testing.T, r AggregateResult) string {
	t.Helper()
	encoded, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("marshal window: %v", err)
	}
	return string(encoded) + "\n"
}
