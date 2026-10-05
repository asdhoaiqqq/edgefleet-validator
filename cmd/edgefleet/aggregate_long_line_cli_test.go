package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// This file is the command-line counterpart of the long-record library
// coverage: a legal single-line JSON record longer than 64 KiB or 128 KiB --
// long because of its multibyte event key -- must flow through the real
// "edgefleet aggregate" process exactly like short input. The long keys are
// preserved whole, two keys differing only in their final rune stay two
// groups, physical line numbers (blanks included) still name late events, and
// a closing watermark delivered as the unterminated final record still closes
// the long key's window. The library tests pin arbitrary read fragmentation
// and line-collection linearity; here the child process's os.Stdin pipe plus
// the shared library reader are exercised end to end.

func cliLongKey(suffix string, thresholdBytes int) string {
	const prefix = "长传感器记录-"
	const block = "温湿度读数" // 6 runes, 18 UTF-8 bytes per repetition
	repeat := thresholdBytes/len(block) + 40
	return prefix + strings.Repeat(block, repeat) + suffix
}

func cliEvent(key string, eventTime, value int) string {
	return fmt.Sprintf(`{"type":"event","key":%q,"time":%d,"value":%d}`, key, eventTime, value)
}

// TestAggregateCLILongRecordsOver64And128KiB drives both thresholds through
// the command with a fixed window, interleaving long/short records, blank
// physical lines and late events exactly as the library scenario does.
func TestAggregateCLILongRecordsOver64And128KiB(t *testing.T) {
	for _, threshold := range []int{64 * 1024, 128 * 1024} {
		threshold := threshold
		t.Run(fmt.Sprintf("over%dKiB", threshold/1024), func(t *testing.T) {
			keyA := cliLongKey("-甲", threshold)
			keyB := cliLongKey("-乙", threshold)
			lines := []string{
				``, // line 1 blank
				cliEvent("短", 100, 2),
				`{"type":"watermark","time":1000}`, // line 3 closes [0,1000)
				``,                                 // line 4 blank
				cliEvent(keyA, 1100, 10),
				cliEvent("短", 1150, 4),
				cliEvent(keyA, 1200, 5),
				``, // line 8 blank
				cliEvent(keyB, 1150, 7),
				cliEvent("短", 100, 8), // line 10: late below watermark 1000
				`{"type":"watermark","time":2000}`,
			}
			input := strings.Join(lines, "\n") + "\n"

			cmd := aggregateCommand(t, input, "--window-ms", "1000")
			var stdout, stderr bytes.Buffer
			cmd.Stdout = &stdout
			cmd.Stderr = &stderr
			if err := cmd.Run(); err != nil {
				t.Fatalf("aggregate exited %v; stderr: %s", err, stderr.String())
			}

			wantLate := "line 10: late event time=100 below current watermark 1000, skipped\n"
			if stderr.String() != wantLate {
				t.Fatalf("late notice:\n got: %q\nwant: %q", stderr.String(), wantLate)
			}

			got := parseWindowLines(t, stdout.String())
			want := []windowLine{
				{Key: "短", Start: 0, End: 1000, Count: 1, Sum: 2},
				{Key: keyA, Start: 1000, End: 2000, Count: 2, Sum: 15},
				{Key: keyB, Start: 1000, End: 2000, Count: 1, Sum: 7},
				{Key: "短", Start: 1000, End: 2000, Count: 1, Sum: 4},
			}
			// Expected order is end ascending then key in UTF-8 byte order;
			// derive it from the keys instead of assuming the A/B order.
			sortWindowLines(want)
			assertWindowLinesEqual(t, got, want)

			// Every long key must come through byte-for-byte, and the two
			// tail-differing keys must be distinct groups.
			byKey := map[string]windowLine{}
			for _, w := range got {
				byKey[w.Key] = w
			}
			if a, ok := byKey[keyA]; !ok || a.Count != 2 || a.Sum != 15 {
				t.Fatalf("long key A not preserved as one merged group: %+v present=%v", a, ok)
			}
			if b, ok := byKey[keyB]; !ok || b.Count != 1 || b.Sum != 7 {
				t.Fatalf("long key B not preserved as a distinct group: %+v present=%v", b, ok)
			}
		})
	}
}

// TestAggregateCLILongRecordFinalWatermarkWithoutNewline ensures the command's
// clean-EOF rule holds with a long record in the stream: the closing
// watermark arrives as the unterminated final line and still emits the long
// key's window.
func TestAggregateCLILongRecordFinalWatermarkWithoutNewline(t *testing.T) {
	keyA := cliLongKey("-甲", 64*1024)
	input := strings.Join([]string{
		cliEvent(keyA, 100, 3),
		`{"type":"watermark","time":1000}`,
	}, "\n") // no trailing newline

	cmd := aggregateCommand(t, input, "--window-ms", "1000")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("clean EOF must not fail: %v; stderr: %s", err, stderr.String())
	}
	assertWindowLines(t, stdout.String(), []windowLine{
		{Key: keyA, Start: 0, End: 1000, Count: 1, Sum: 3},
	})
	if stderr.String() != "" {
		t.Fatalf("unexpected stderr: %q", stderr.String())
	}
}

// parseWindowLines decodes every stdout line as one window result, requiring
// exactly one JSON object per physical line.
func parseWindowLines(t *testing.T, stdout string) []windowLine {
	t.Helper()
	var got []windowLine
	for i, line := range strings.Split(strings.TrimSuffix(stdout, "\n"), "\n") {
		var w windowLine
		if err := json.Unmarshal([]byte(line), &w); err != nil {
			t.Fatalf("stdout line %d is not valid JSON: %v\nline: %.80q...", i+1, err, line)
		}
		got = append(got, w)
	}
	return got
}

// assertWindowLinesEqual compares decoded window rows including the full keys.
func assertWindowLinesEqual(t *testing.T, got, want []windowLine) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("window rows = %d, want %d:\n got: %+v\nwant: %+v", len(got), len(want), got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("window row %d:\n got:  %+v\nwant: %+v", i, got[i], want[i])
		}
	}
}

// sortWindowLines orders rows by end then key, the command's emission order.
func sortWindowLines(ws []windowLine) {
	for i := 1; i < len(ws); i++ {
		for j := i; j > 0; j-- {
			if ws[j].End < ws[j-1].End || (ws[j].End == ws[j-1].End && ws[j].Key < ws[j-1].Key) {
				ws[j], ws[j-1] = ws[j-1], ws[j]
			} else {
				break
			}
		}
	}
}
