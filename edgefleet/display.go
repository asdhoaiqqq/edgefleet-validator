package edgefleet

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

// DisplayText renders node ids and versions for the health-query terminal
// output. It is the single display rule shared by every health text: the node
// value, the reported and expected versions (including both sides of the
// version-skew finding) and the node id shown when a node has no telemetry.
// It changes presentation only — identity, version comparison and every value
// written to storage keep using the original, untrimmed text.
//
// Text containing no whitespace, double quote, backslash or control character
// is shown exactly as reported, so ordinary ids and versions look unchanged
// and Chinese, emoji and a literal replacement character "�" stay directly
// readable rather than becoming a run of character numbers.
//
// Any other text is shown as a double-quoted JSON string: a real newline
// becomes the visible two characters \n, a carriage return \r, a tab \t,
// double quotes and backslashes follow JSON escaping, and every other control
// character (including terminal-destructive ones such as a form feed) becomes
// a four-hex-digit \u escape. No raw control byte is ever emitted. A quoted
// display value decoded as a JSON string reproduces the original text, which
// also distinguishes a real newline from a backslash followed by the letter
// n (the latter appears as \\n inside the quotes).
func DisplayText(s string) string {
	if !needsTextQuoting(s) {
		return s
	}

	var b strings.Builder
	b.Grow(len(s) + 2)
	b.WriteByte('"')
	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		switch {
		case r == utf8.RuneError && size == 1:
			// Invalid UTF-8 cannot occur at this boundary — submit input and
			// stored files both reject it. Keep the output a valid JSON string
			// rather than emitting the raw byte.
			b.WriteString(`�`)
		case r == '\n':
			b.WriteString(`\n`)
		case r == '\r':
			b.WriteString(`\r`)
		case r == '\t':
			b.WriteString(`\t`)
		case r == '"':
			b.WriteString(`\"`)
		case r == '\\':
			b.WriteString(`\\`)
		case unicode.IsControl(r):
			const hex = "0123456789abcdef"
			b.WriteString(`\u`)
			b.WriteByte(hex[(r>>12)&0xf])
			b.WriteByte(hex[(r>>8)&0xf])
			b.WriteByte(hex[(r>>4)&0xf])
			b.WriteByte(hex[r&0xf])
		default:
			b.WriteRune(r)
		}
		i += size
	}
	b.WriteByte('"')
	return b.String()
}

// needsTextQuoting reports whether s must be wrapped as a JSON string: it
// contains whitespace, a double quote, a backslash, a control character, or an
// invalid UTF-8 byte.
func needsTextQuoting(s string) bool {
	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			return true
		}
		if unicode.IsSpace(r) || unicode.IsControl(r) || r == '"' || r == '\\' {
			return true
		}
		i += size
	}
	return false
}
