package edgefleet

import (
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// jsonUnicodeEscape is the two characters that open a JSON Unicode escape,
// built by concatenation so this source file spells every such sequence in
// test input explicitly instead of depending on one encoding layer to carry
// another untouched.
const jsonUnicodeEscape = "\\" + "u"

// Regression coverage for JSON escape handling of event keys in the legacy
// single-watermark fixed-window mode: a legal escape must decode to exactly
// the character it names, and that decoded key -- not its spelling in the
// input -- is what aggregation, output and ordering see.

// The same character written with different legal JSON escapes is the same
// key: a real newline written as the short escape and as its four-digit
// Unicode escape merges into one window result. Window length 1000ms, both
// events in the first window, values 2 and 3: the closed result is
// count=2, sum=5.
func TestAggregateKeyEscapeSpellingsOfNewlineMerge(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"a\nb","time":100,"value":2}`,                            // short escape
		`{"type":"event","key":"a` + jsonUnicodeEscape + `000Ab","time":200,"value":3}`, // Unicode escape: same key
		`{"type":"watermark","time":1000}`,
	}, "\n") + "\n"
	stdout, stderr, err := runAggregate(t, input, 1000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stderr != "" {
		t.Fatalf("unexpected stderr: %q", stderr)
	}
	want := `{"key":"a\nb","start":0,"end":1000,"count":2,"sum":5}` + "\n"
	if stdout != want {
		t.Fatalf("stdout = %q, want %q", stdout, want)
	}
}

// A key holding the two characters backslash and 'n' is a different key from
// one holding a real newline: it must not be decoded a second time and
// merged with the newline key.
func TestAggregateBackslashNKeyStaysDistinctFromNewlineKey(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"a\nb","time":100,"value":2}`,  // decoded: a, newline, b
		`{"type":"event","key":"a\\nb","time":200,"value":3}`, // decoded: a, backslash, n, b
		`{"type":"watermark","time":1000}`,
	}, "\n") + "\n"
	stdout, stderr, err := runAggregate(t, input, 1000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stderr != "" {
		t.Fatalf("unexpected stderr: %q", stderr)
	}
	// Both keys close in the same window, ordered by decoded key bytes:
	// "a\nb" (0x0A) sorts before `a\nb` (0x5C).
	want := strings.Join([]string{
		`{"key":"a\nb","start":0,"end":1000,"count":1,"sum":2}`,
		`{"key":"a\\nb","start":0,"end":1000,"count":1,"sum":3}`,
		``,
	}, "\n")
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
}

// A plain solidus and an escaped solidus name the same character and merge.
func TestAggregateSolidusEscapeSpellingsMerge(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"x/y","time":100,"value":2}`,
		`{"type":"event","key":"x\/y","time":200,"value":3}`,
		`{"type":"watermark","time":1000}`,
	}, "\n") + "\n"
	stdout, stderr, err := runAggregate(t, input, 1000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stderr != "" {
		t.Fatalf("unexpected stderr: %q", stderr)
	}
	want := `{"key":"x/y","start":0,"end":1000,"count":2,"sum":5}` + "\n"
	if stdout != want {
		t.Fatalf("stdout = %q, want %q", stdout, want)
	}
}

// Keys containing a newline, carriage return, tab, quote, backslash or zero
// character are ordinary keys: every character survives decoding,
// aggregation and encoding, each closed result stays on one physical output
// line, and reading that line as JSON restores the original key exactly --
// including consecutive backslashes and a special character at the very end
// of the key.
func TestAggregateSpecialCharacterKeysRoundTrip(t *testing.T) {
	cases := []struct {
		raw     string // key as a JSON string literal, quotes included
		decoded string // the key it must decode to
	}{
		{`"tab\tk"`, "tab\tk"},
		{`"cr\rk"`, "cr\rk"},
		{`"nl\nk"`, "nl\nk"},
		{`"quote\"k"`, `quote"k`},
		{`"back\\slash"`, `back\slash`},
		{`"nul` + jsonUnicodeEscape + `0000k"`, "nul\x00k"},
		{`"two\\\\back"`, `two\\back`}, // two consecutive backslashes
		{`"trail\\"`, `trail\`},        // backslash at the end of the key
		{`"trail\n"`, "trail\n"},       // newline at the end of the key
	}
	lines := make([]string, 0, len(cases)+1)
	for i, tc := range cases {
		lines = append(lines, `{"type":"event","key":`+tc.raw+`,"time":`+strconv.Itoa(i*100)+`,"value":1}`)
	}
	lines = append(lines, `{"type":"watermark","time":1000}`)
	input := strings.Join(lines, "\n") + "\n"

	stdout, stderr, err := runAggregate(t, input, 1000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stderr != "" {
		t.Fatalf("unexpected stderr: %q", stderr)
	}

	// One complete JSON object per physical line: a newline or carriage
	// return inside a key must not split its result across lines.
	if !strings.HasSuffix(stdout, "\n") {
		t.Fatalf("stdout must end with a newline, got %q", stdout)
	}
	physical := strings.Split(strings.TrimSuffix(stdout, "\n"), "\n")
	if len(physical) != len(cases) {
		t.Fatalf("stdout has %d result line(s), want %d (one per key, no splitting):\n%s", len(physical), len(cases), stdout)
	}

	want := make([]string, 0, len(cases))
	for _, tc := range cases {
		want = append(want, tc.decoded)
	}
	sort.Strings(want) // output is ordered by decoded key bytes
	for i, text := range physical {
		var result AggregateResult
		if err := json.Unmarshal([]byte(text), &result); err != nil {
			t.Fatalf("output line %d is not a complete JSON object: %v (%q)", i+1, err, text)
		}
		if result.Key != want[i] {
			t.Errorf("output line %d key = %q, want %q", i+1, result.Key, want[i])
		}
		if result.Start != 0 || result.End != 1000 || result.Count != 1 || result.Sum != 1 {
			t.Errorf("output line %d = %+v, want window [0,1000) count=1 sum=1", i+1, result)
		}
	}
}

// Windows closing at the same instant are ordered by the decoded key's UTF-8
// byte order, no matter which escape spelling the input used or in which
// order the events arrived.
func TestAggregateClosedWindowsOrderedByDecodedKeyBytes(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"z","time":500,"value":1}`,
		`{"type":"event","key":"a` + jsonUnicodeEscape + `0061","time":100,"value":1}`, // "aa", one letter escaped
		`{"type":"event","key":"a\n","time":200,"value":1}`,                            // "a" + newline
		`{"type":"event","key":"a` + jsonUnicodeEscape + `0000","time":300,"value":1}`, // "a" + zero character
		`{"type":"event","key":"a","time":400,"value":1}`,
		`{"type":"watermark","time":1000}`,
	}, "\n") + "\n"
	stdout, stderr, err := runAggregate(t, input, 1000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stderr != "" {
		t.Fatalf("unexpected stderr: %q", stderr)
	}
	// Decoded byte order: "a" < "a\x00" (0x00) < "a\n" (0x0A) < "aa" (0x61) < "z".
	want := strings.Join([]string{
		`{"key":"a","start":0,"end":1000,"count":1,"sum":1}`,
		`{"key":"a` + jsonUnicodeEscape + `0000","start":0,"end":1000,"count":1,"sum":1}`,
		`{"key":"a\n","start":0,"end":1000,"count":1,"sum":1}`,
		`{"key":"aa","start":0,"end":1000,"count":1,"sum":1}`,
		`{"key":"z","start":0,"end":1000,"count":1,"sum":1}`,
		``,
	}, "\n")
	if stdout != want {
		t.Fatalf("stdout mismatch:\n got: %q\nwant: %q", stdout, want)
	}
}

// The boundary between legal escapes and broken input: an unescaped control
// character inside the key string is a fatal input error carrying the
// physical line number and the reason. Processing stops at once, results
// already written stay written, and later records are never read.
func TestAggregateUnescapedControlCharInKeyIsFatal(t *testing.T) {
	cases := []struct {
		name string
		char string
	}{
		{"zero character", "\x00"},
		{"control character", "\x01"},
		{"unit separator", "\x1f"},
		{"raw tab", "\t"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			input := strings.Join([]string{
				`{"type":"event","key":"ok","time":100,"value":2}`,
				`{"type":"watermark","time":1000}`, // closes [0,1000)
				`{"type":"event","key":"a` + tc.char + `b","time":1500,"value":1}`,
				`{"type":"watermark","time":2000}`, // must never be processed
			}, "\n") + "\n"
			stdout, stderr, err := runAggregate(t, input, 1000)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			inputErr, ok := err.(*InputError)
			if !ok {
				t.Fatalf("expected *InputError, got %T: %v", err, err)
			}
			if inputErr.Line != 3 {
				t.Errorf("line = %d, want 3", inputErr.Line)
			}
			if !strings.Contains(inputErr.Reason, "invalid JSON") {
				t.Errorf("reason = %q, want it to report the invalid JSON record", inputErr.Reason)
			}
			want := `{"key":"ok","start":0,"end":1000,"count":1,"sum":2}` + "\n"
			if stdout != want {
				t.Errorf("stdout = %q, want only the previously closed window %q", stdout, want)
			}
			if stderr != "" {
				t.Errorf("unexpected late notice: %q", stderr)
			}
		})
	}
}

// An escape JSON does not support is equally fatal, with the physical line
// number and reason, and stops the run the same way.
func TestAggregateUnsupportedEscapeInKeyIsFatal(t *testing.T) {
	cases := []struct {
		name   string
		escape string
	}{
		{"hex escape", `\x`},
		{"escape character", `\e`},
		{"single quote", `\'`},
		{"vertical tab", `\v`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			input := strings.Join([]string{
				`{"type":"event","key":"ok","time":100,"value":2}`,
				`{"type":"watermark","time":1000}`, // closes [0,1000)
				`{"type":"event","key":"a` + tc.escape + `b","time":1500,"value":1}`,
				`{"type":"watermark","time":2000}`, // must never be processed
			}, "\n") + "\n"
			stdout, _, err := runAggregate(t, input, 1000)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			inputErr, ok := err.(*InputError)
			if !ok {
				t.Fatalf("expected *InputError, got %T: %v", err, err)
			}
			if inputErr.Line != 3 {
				t.Errorf("line = %d, want 3", inputErr.Line)
			}
			if !strings.Contains(inputErr.Reason, "invalid JSON") {
				t.Errorf("reason = %q, want it to report the invalid JSON record", inputErr.Reason)
			}
			want := `{"key":"ok","start":0,"end":1000,"count":1,"sum":2}` + "\n"
			if stdout != want {
				t.Errorf("stdout = %q, want only the previously closed window %q", stdout, want)
			}
		})
	}
}

// A newline that arrives as a legal escape is still one physical input line:
// it does not advance the line count, while a blank input line does.
func TestAggregateEscapedNewlineDoesNotShiftLineNumber(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"a\nb","time":100,"value":2}`, // line 1: escaped newline, one physical line
		``, // line 2: blank, still counted
		`{"type":"event","key":"bad\xe","time":200,"value":1}`, // line 3: unsupported escape
		`{"type":"watermark","time":1000}`,                     // line 4: must never be processed
	}, "\n") + "\n"
	stdout, _, err := runAggregate(t, input, 1000)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	inputErr, ok := err.(*InputError)
	if !ok {
		t.Fatalf("expected *InputError, got %T: %v", err, err)
	}
	if inputErr.Line != 3 {
		t.Errorf("line = %d, want 3 (the escaped newline adds no line, the blank line adds one)", inputErr.Line)
	}
	if stdout != "" {
		t.Errorf("no window may close: the watermark on line 4 is never processed, got %q", stdout)
	}
}
