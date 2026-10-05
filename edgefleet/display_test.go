package edgefleet

import (
	"encoding/json"
	"strings"
	"testing"
)

// These tests pin the single terminal display rule used by health queries for
// node ids and versions. Identity and version comparison keep using the raw
// text; DisplayText only shapes what is printed, so the contract is:
//
//   - ordinary text (including Chinese, emoji and a literal replacement
//     character) is printed byte-for-byte unchanged and unquoted;
//   - text with whitespace, quotes, backslashes or control characters is
//     printed as a double-quoted JSON string, with \n \r \t shown as visible
//     escapes, quotes and backslashes escaped per JSON, and every other
//     control character shown as a four-hex-digit \u escape — no raw control
//     byte may reach the terminal;
//   - decoding the quoted display value as a JSON string reproduces the
//     original text, so a real newline is distinguishable from a backslash
//     followed by the letter n.

func TestDisplayTextPlainUnchanged(t *testing.T) {
	cases := []string{
		"1.26.0",
		"val-eu-1",
		"节点甲",
		"正式版-😀",
		"v2",
		"val-�", // a literal replacement character the user typed is plain text
		"v2/正式",
		"=",
		"[",
	}
	for _, in := range cases {
		if got := DisplayText(in); got != in {
			t.Errorf("DisplayText(%q) = %q, want unchanged", in, got)
		}
	}
}

func TestDisplayTextQuotesWhitespaceAndSpecialBytes(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"space", "node 1", `"node 1"`},
		{"tab", "a\tb", `"a\tb"`},
		{"newline", "a\nb", `"a\nb"`},
		{"carriage return", "a\rb", `"a\rb"`},
		{"crlf", "a\r\nb", `"a\r\nb"`},
		{"double quote", `a"b`, `"a\"b"`},
		{"backslash", `a\b`, `"a\\b"`},
		{"backslash followed by n", `a\nb`, `"a\\nb"`},
		{"leading space", " v1", `" v1"`},
		{"non-breaking space", "a b", `"a b"`},
		{"ideographic space", "a　b", `"a　b"`},
		{"newline only", "\n", `"\n"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := DisplayText(tc.in); got != tc.want {
				t.Errorf("DisplayText(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestDisplayTextControlEscapes(t *testing.T) {
	cases := []struct {
		in   rune
		want string
	}{
		{0x00, `\u0000`},
		{0x01, `\u0001`},
		{0x07, `\u0007`},
		{0x08, `\u0008`}, // backspace uses the four-hex form, not \b
		{0x0b, `\u000b`}, // vertical tab
		{0x0c, `\u000c`}, // form feed: clears screens on real terminals
		{0x1b, `\u001b`}, // escape
		{0x7f, `\u007f`}, // DEL
	}
	for _, tc := range cases {
		in := "a" + string(tc.in) + "b"
		got := DisplayText(in)
		want := `"a` + tc.want + `b"`
		if got != want {
			t.Errorf("control U+%04X: DisplayText = %q, want %q", tc.in, got, want)
		}
	}

	// Newline, CR and tab keep their dedicated visible short escapes.
	if got := DisplayText("\n\r\t"); got != `"\n\r\t"` {
		t.Errorf("line/tab escapes = %q, want %q", got, `"\n\r\t"`)
	}
}

func TestDisplayTextNeverEmitsRawControlBytes(t *testing.T) {
	for r := rune(0); r <= 0x7f; r++ {
		in := "x" + string(r) + "y"
		got := DisplayText(in)
		if r < 0x20 || r == 0x7f {
			if strings.ContainsRune(got, r) {
				t.Errorf("control U+%04X leaked raw into %q", r, got)
			}
		}
	}
}

func TestDisplayTextReadableUnicodeStaysReadable(t *testing.T) {
	// Even when quoting is forced by a space, the readable characters must not
	// be turned into \u numbering.
	in := "v2 正式版 😀 �"
	got := DisplayText(in)
	if !strings.HasPrefix(got, `"`) || !strings.HasSuffix(got, `"`) {
		t.Fatalf("space forces quoting, got %q", got)
	}
	for _, want := range []string{"正式版", "😀", "�"} {
		if !strings.Contains(got, want) {
			t.Errorf("quoted output must keep %s readable, got %q", want, got)
		}
	}
}

func TestDisplayTextQuotedValuesDecodeBackToOriginal(t *testing.T) {
	inputs := []string{
		"a\nb", `a\nb`, "a\tb", "a\r\nb", `a"b`, `a\\b`,
		"node with spaces\nsecond line",
		"v1\fv2", "v1\x00v2", "v1\x1b[2Jv2",
		"正式 版-😀\n�",
		" status=online\nnode=forged",
	}
	for _, in := range inputs {
		got := DisplayText(in)
		if !strings.HasPrefix(got, `"`) {
			t.Fatalf("expected quoted display for %q, got %q", in, got)
		}
		var decoded string
		if err := json.Unmarshal([]byte(got), &decoded); err != nil {
			t.Fatalf("display value %q is not a valid JSON string: %v", got, err)
		}
		if decoded != in {
			t.Errorf("round trip: decoded %q, want original %q", decoded, in)
		}
	}
}

func TestDisplayTextDistinguishesNewlineFromBackslashN(t *testing.T) {
	realNewline := "a\nb"
	literalBackslashN := `a\nb`
	if DisplayText(realNewline) == DisplayText(literalBackslashN) {
		t.Fatal("a real newline and backslash+n must render differently")
	}
	if got := DisplayText(realNewline); got != `"a\nb"` {
		t.Errorf("real newline = %q", got)
	}
	if got := DisplayText(literalBackslashN); got != `"a\\nb"` {
		t.Errorf("literal backslash-n = %q", got)
	}
}

func TestVersionSkewFindingUsesDisplayTextButComparesRaw(t *testing.T) {
	// A newline in the reported version is visible, not a line break.
	findings := livenessVersionFindings(true, "1.25\n.0", "1.26.0")
	if len(findings) != 1 || findings[0] != `version skew: "1.25\n.0" != 1.26.0` {
		t.Errorf("findings = %q", findings)
	}
	if strings.ContainsAny(strings.Join(findings, "|"), "\n\r\t") {
		t.Errorf("finding must contain no raw line/tab bytes: %q", findings)
	}

	// A tab and a clear-screen escape in the expected side get the same rule.
	findings = livenessVersionFindings(true, "1.0", "1.0\t\x1b[2J")
	want := `version skew: 1.0 != "1.0\t\u001b[2J"`
	if len(findings) != 1 || findings[0] != want {
		t.Errorf("findings = %q, want %q", findings, want)
	}

	// Skew is decided by the raw text: equal texts are not a skew, and texts
	// differing only by a space are.
	if fs := livenessVersionFindings(true, "1.0 ", "1.0 "); len(fs) != 0 {
		t.Errorf("equal raw versions must not skew: %q", fs)
	}
	if fs := livenessVersionFindings(true, "1.0", "1.0 "); len(fs) != 1 {
		t.Errorf("versions differing by a space must skew: %q", fs)
	}

	// No skew means no version text is emitted at all.
	if fs := livenessVersionFindings(false, "1.0", "1.0"); len(fs) != 1 || fs[0] != "offline" {
		t.Errorf("findings = %q, want [offline]", fs)
	}
}
