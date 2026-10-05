package edgefleet

import (
	"fmt"
	"strings"
	"unicode"
)

// DisplayText renders a node id or version for one-line terminal output.
// Node ids and versions are arbitrary valid Unicode text and stay fully
// legal to submit, save and query — but they may contain newlines, tabs,
// quotes, backslashes or control characters that would split a health line
// into several lines, shift its fields, or let terminal escape sequences
// overwrite what was printed. One display rule is shared by the node id, the
// actual version and both versions of the version-skew finding:
//
//   - text without whitespace, double quotes, backslashes or control
//     characters is shown exactly as it is;
//   - anything else is shown as a double-quoted JSON string: newline,
//     carriage return and tab appear as the visible escapes \n, \r and \t,
//     double quotes and backslashes follow JSON escaping, and every other
//     control character appears as a four-digit \u escape — a raw control
//     character never reaches the terminal. Chinese, emoji and a literal "�"
//     the user typed stay directly readable inside the quotes; only the
//     characters that would break the output are escaped.
//
// The quoted form is a valid JSON string: decoding it as JSON restores the
// original text exactly, so a real newline and the two characters "\n" the
// user typed remain distinguishable, and a node id containing spaces (even
// one spelling out "status=online") stays a single node value inside the
// quotes. This is display only — node identity and version comparisons
// always use the original text, untrimmed and case-sensitive, and the
// display form is never written back to stored heartbeats.
func DisplayText(s string) string {
	if !needsDisplayQuoting(s) {
		return s
	}
	var b strings.Builder
	b.Grow(len(s) + 2)
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		default:
			if unicode.IsControl(r) {
				// Control characters are all <= U+009F, so four hex
				// digits always suffice.
				fmt.Fprintf(&b, `\u%04x`, r)
			} else {
				b.WriteRune(r)
			}
		}
	}
	b.WriteByte('"')
	return b.String()
}

// needsDisplayQuoting reports whether s contains a character that would break
// or disguise one-line terminal output: whitespace, a double quote, a
// backslash or a control character.
func needsDisplayQuoting(s string) bool {
	for _, r := range s {
		if r == '"' || r == '\\' || unicode.IsSpace(r) || unicode.IsControl(r) {
			return true
		}
	}
	return false
}
