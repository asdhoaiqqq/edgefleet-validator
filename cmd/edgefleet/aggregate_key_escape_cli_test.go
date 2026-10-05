package main

import (
	"strings"
	"testing"
)

// cliJSONUnicodeEscape is the two characters that open a JSON Unicode
// escape, built by concatenation so this source file spells every such
// sequence in test input explicitly instead of depending on one encoding
// layer to carry another untouched.
const cliJSONUnicodeEscape = "\\" + "u"

// TestAggregateCLIKeyEscapeSpellingsMerge drives the command line with the
// same character written two legal ways: a real newline in the key as the
// short escape and as its four-digit Unicode escape. Both decode to the
// same key, so the two events merge into one window result. Window length
// 1000ms, both events in the first window, values 2 and 3: count=2, sum=5.
func TestAggregateCLIKeyEscapeSpellingsMerge(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"a\nb","time":100,"value":2}`,                               // short escape
		`{"type":"event","key":"a` + cliJSONUnicodeEscape + `000Ab","time":200,"value":3}`, // Unicode escape: same key
		`{"type":"watermark","time":1000}`,
	}, "\n") + "\n"

	code, stdout, stderr := runAggregateCLI(t, input, "--window-ms", "1000")
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stderr = %q", code, stderr)
	}
	if stderr != "" {
		t.Fatalf("stderr = %q, want empty", stderr)
	}
	// assertWindowLines also proves the result is one physical line: the
	// newline inside the key must not split it, and the JSON-decoded key
	// must come back exactly as "a\nb".
	assertWindowLines(t, stdout, []windowLine{
		{Key: "a\nb", Start: 0, End: 1000, Count: 2, Sum: 5},
	})
}

// TestAggregateCLISpecialCharacterKeysRoundTrip feeds keys containing a
// newline, carriage return, tab, quote, backslash and zero character. Each
// closed result must stay on one physical output line and JSON-decoding the
// line must restore the original key exactly, including a backslash at the
// very end of the key.
func TestAggregateCLISpecialCharacterKeysRoundTrip(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"nl\nk","time":100,"value":1}`,
		`{"type":"event","key":"cr\rk","time":200,"value":1}`,
		`{"type":"event","key":"tab\tk","time":300,"value":1}`,
		`{"type":"event","key":"quote\"k","time":400,"value":1}`,
		`{"type":"event","key":"back\\slash","time":500,"value":1}`,
		`{"type":"event","key":"nul` + cliJSONUnicodeEscape + `0000k","time":600,"value":1}`,
		`{"type":"event","key":"trail\\","time":700,"value":1}`,
		`{"type":"watermark","time":1000}`,
	}, "\n") + "\n"

	code, stdout, stderr := runAggregateCLI(t, input, "--window-ms", "1000")
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stderr = %q", code, stderr)
	}
	if stderr != "" {
		t.Fatalf("stderr = %q, want empty", stderr)
	}
	// Emission order is the decoded keys' UTF-8 byte order.
	assertWindowLines(t, stdout, []windowLine{
		{Key: `back\slash`, Start: 0, End: 1000, Count: 1, Sum: 1},
		{Key: "cr\rk", Start: 0, End: 1000, Count: 1, Sum: 1},
		{Key: "nl\nk", Start: 0, End: 1000, Count: 1, Sum: 1},
		{Key: "nul\x00k", Start: 0, End: 1000, Count: 1, Sum: 1},
		{Key: `quote"k`, Start: 0, End: 1000, Count: 1, Sum: 1},
		{Key: "tab\tk", Start: 0, End: 1000, Count: 1, Sum: 1},
		{Key: `trail\`, Start: 0, End: 1000, Count: 1, Sum: 1},
	})
}

// TestAggregateCLIKeyEscapeBoundaryIsFatal covers the broken side of the
// boundary: an escape JSON does not support, and an unescaped control
// character inside the key string. Both must print the physical input line
// number and the reason to standard error, exit with code 1, keep the
// already closed window on standard output and never read the records that
// follow. The escaped newline in line 1's key adds no input line, while the
// blank line 3 does, so the broken record is line 4.
func TestAggregateCLIKeyEscapeBoundaryIsFatal(t *testing.T) {
	cases := []struct {
		name string
		key  string // raw JSON string literal, quotes included
	}{
		{"unsupported escape", `"bad\xkey"`},
		{"unescaped control character", "\"bad\x01key\""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			input := strings.Join([]string{
				`{"type":"event","key":"a\nb","time":100,"value":2}`, // line 1: escaped newline, one physical line
				`{"type":"watermark","time":1000}`,                   // line 2: closes [0,1000)
				``,                                                   // line 3: blank, still counted
				`{"type":"event","key":` + tc.key + `,"time":1500,"value":1}`, // line 4: broken
				`{"type":"watermark","time":2000}`,                            // line 5: must never be read
			}, "\n") + "\n"

			code, stdout, stderr := runAggregateCLI(t, input, "--window-ms", "1000")
			if code != 1 {
				t.Fatalf("exit code = %d, want 1; stderr = %q", code, stderr)
			}
			for _, want := range []string{"line 4", "invalid JSON"} {
				if !strings.Contains(stderr, want) {
					t.Fatalf("stderr = %q, want it to mention %q", stderr, want)
				}
			}
			// Only the window closed before the fatal record appears.
			assertWindowLines(t, stdout, []windowLine{
				{Key: "a\nb", Start: 0, End: 1000, Count: 1, Sum: 2},
			})
		})
	}
}
