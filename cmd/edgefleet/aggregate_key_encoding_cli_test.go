package main

import (
	"strings"
	"testing"
)

// Command-line regression coverage for rejecting event keys with damaged
// character encoding. The package tests cover the decoder and all library
// entry points directly; these tests drive the real main() in a child
// process (see main_test.go for the harness) so they guard the process
// boundary: a lone surrogate in a key's \u escape or an invalid UTF-8 byte
// must print the physical input line and the encoding/Unicode-escape reason
// to standard error, exit 1, keep results already fully written, and leave
// later records unread -- the bytes are never repaired to U+FFFD and the
// event is never downgraded to a late-event notice.

// TestAggregateCLIDamagedSurrogateKeyIsFatal feeds the three example
// damaged escapes from the specification. The first window already closed on
// line 3, so its result must survive; line 4 (a blank line counts) then
// fails and line 5 must never be processed.
func TestAggregateCLIDamagedSurrogateKeyIsFatal(t *testing.T) {
	for _, tc := range []struct {
		name string
		key  string
	}{
		{"lone high", `"\uD800"`},
		{"lone low", `"\uDC00"`},
		{"high followed by text", `"\uD800x"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := strings.Join([]string{
				`{"type":"event","key":"k","time":100,"value":5}`, // line 1
				`{"type":"watermark","time":1000}`,                // line 2 closes [0,1000)
				``,                                                // line 3 blank, still counted
				`{"type":"event","key":` + tc.key + `,"time":200,"value":99}`, // line 4 fatal
				`{"type":"watermark","time":9000}`,                            // line 5 must never be read
			}, "\n") + "\n"

			code, stdout, stderr := runAggregateCLI(t, input, "--window-ms", "1000")
			if code != 1 {
				t.Fatalf("exit code = %d, want 1; stderr: %s", code, stderr)
			}
			for _, want := range []string{"line 4", "key"} {
				if !strings.Contains(stderr, want) {
					t.Fatalf("stderr = %q, want it to mention %q", stderr, want)
				}
			}
			lower := strings.ToLower(stderr)
			if !strings.Contains(lower, "surrogate") && !strings.Contains(lower, "unicode") {
				t.Fatalf("stderr = %q, want the Unicode escape problem named", stderr)
			}
			if strings.Contains(stderr, "late event") {
				t.Fatalf("damaged key must not be reported as a late event: stderr = %q", stderr)
			}
			assertWindowLines(t, stdout, []windowLine{
				{Key: "k", Start: 0, End: 1000, Count: 1, Sum: 5},
			})
		})
	}
}

// TestAggregateCLIDamagedUTF8KeyIsFatal uses a raw invalid UTF-8 byte in the
// key and verifies the error names the character encoding and the physical
// line even through the command entry point.
func TestAggregateCLIDamagedUTF8KeyIsFatal(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"ab` + "\xff" + `cd","time":100,"value":1}`, // line 1
		`{"type":"watermark","time":9000}`,                                 // line 2 must never be read
	}, "\n") + "\n"

	code, stdout, stderr := runAggregateCLI(t, input, "--window-ms", "1000")
	if code != 1 {
		t.Fatalf("exit code = %d, want 1; stderr: %s", code, stderr)
	}
	if !strings.Contains(stderr, "line 1") {
		t.Fatalf("stderr = %q, want physical line 1", stderr)
	}
	if !strings.Contains(strings.ToLower(stderr), "utf-8") {
		t.Fatalf("stderr = %q, want the invalid UTF-8 encoding named", stderr)
	}
	if stdout != "" {
		t.Fatalf("damaged first record must produce no output, got %q", stdout)
	}
}

// TestAggregateCLILiteralReplacementCharacterAndEscapedEmojiAreValid guards
// the accepted side end to end: a directly typed U+FFFD and a "�"
// escape merge into one key, a directly typed emoji merges with its matched
// surrogate pair, an escaped backslash is literal text, and distinct legal
// keys stay separate -- exit 0, empty stderr.
func TestAggregateCLILiteralReplacementCharacterAndEscapedEmojiAreValid(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"�","time":100,"value":11}`,
		`{"type":"event","key":"\uFFFD","time":200,"value":13}`,
		`{"type":"event","key":"😀","time":100,"value":1}`,
		`{"type":"event","key":"\uD83D\uDE00","time":200,"value":2}`,
		`{"type":"event","key":"\\uD800","time":100,"value":7}`, // literal text \uD800
		`{"type":"event","key":"中","time":100,"value":5}`,
		`{"type":"watermark","time":1000}`,
	}, "\n") + "\n"

	code, stdout, stderr := runAggregateCLI(t, input, "--window-ms", "1000")
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stderr: %s", code, stderr)
	}
	if stderr != "" {
		t.Fatalf("valid Unicode keys must not warn: stderr = %q", stderr)
	}
	assertWindowLines(t, stdout, []windowLine{
		{Key: `\uD800`, Start: 0, End: 1000, Count: 1, Sum: 7},
		{Key: "中", Start: 0, End: 1000, Count: 1, Sum: 5},
		{Key: "�", Start: 0, End: 1000, Count: 2, Sum: 24},
		{Key: "😀", Start: 0, End: 1000, Count: 2, Sum: 3},
	})
}
