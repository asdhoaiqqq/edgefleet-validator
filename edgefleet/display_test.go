package edgefleet

// Tests for the shared terminal display rule used by health queries for node
// ids and versions (DisplayText) and for the version-skew finding. Legal
// Unicode text stays submittable, storable and queryable; only the health
// output rendering changes: plain text shows as-is, anything containing
// whitespace, quotes, backslashes or control characters shows as a quoted
// JSON string that decodes back to the exact original.

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestDisplayTextPlainTextUnchanged(t *testing.T) {
	cases := []string{
		"val-eu-1",
		"1.26.0",
		"节点甲",
		"版本甲",
		"val-😀",
		"val-�", // a literal replacement char the user typed is printable text
		"v2正式版beta",
	}
	for _, s := range cases {
		if got := DisplayText(s); got != s {
			t.Errorf("DisplayText(%q) = %q, want the text unchanged", s, got)
		}
	}
}

func TestDisplayTextQuotesAndEscapes(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"space", "node / with spaces", `"node / with spaces"`},
		{"text mimicking a field", "x status=online", `"x status=online"`},
		{"real newline", "a\nb", `"a\nb"`},
		{"carriage return", "a\rb", `"a\rb"`},
		{"tab", "a\tb", `"a\tb"`},
		{"double quote", `a"b`, `"a\"b"`},
		{"backslash", `a\nb`, `"a\\nb"`}, // literal backslash+n, not a newline
		{"clear-screen escape", "a\x1b[2Jb", `"a\u001b[2Jb"`},
		{"bell", "a\ab", `"a\u0007b"`},
		{"vertical tab", "a\vb", `"a\u000bb"`},
		{"delete", "a\x7fb", `"a\u007fb"`},
		{"unicode space", "a b", `"a b"`}, // U+00A0 forces quoting, stays literal
		{"newline inside chinese", "节点\n甲", `"节点\n甲"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := DisplayText(tc.in)
			if got != tc.want {
				t.Errorf("DisplayText(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestDisplayTextQuotedFormDecodesToOriginal requires every quoted display
// value to be a valid JSON string that decodes back to the exact input, so a
// real newline and a typed backslash-n stay distinguishable and no raw
// control character ever reaches the terminal.
func TestDisplayTextQuotedFormDecodesToOriginal(t *testing.T) {
	inputs := []string{
		"a\nb", "a\rb", "a\tb", `a"b`, `a\nb`, "a\x1b[2Jb", "a\a", "a\vb",
		"a\x7fb", "node / with spaces", "节点\n甲😀", "v1\r\nv2",
	}
	for _, in := range inputs {
		got := DisplayText(in)
		if !strings.HasPrefix(got, `"`) {
			t.Errorf("DisplayText(%q) = %q, want a quoted JSON string", in, got)
			continue
		}
		var decoded string
		if err := json.Unmarshal([]byte(got), &decoded); err != nil {
			t.Errorf("DisplayText(%q) = %q is not a valid JSON string: %v", in, got, err)
			continue
		}
		if decoded != in {
			t.Errorf("json.Decode(DisplayText(%q)) = %q, want the original text", in, decoded)
		}
		for _, r := range got {
			if r < 0x20 || r == 0x7f {
				t.Errorf("DisplayText(%q) = %q emits a raw control character", in, got)
			}
		}
	}
}

// TestDisplayTextDistinguishesNewlineFromBackslashN pins that a real newline
// and the two printable characters "\n" render differently.
func TestDisplayTextDistinguishesNewlineFromBackslashN(t *testing.T) {
	realNewline := DisplayText("a\nb")
	typed := DisplayText(`a\nb`)
	if realNewline == typed {
		t.Errorf("real newline and literal backslash-n must display differently, both gave %q", realNewline)
	}
	if realNewline != `"a\nb"` || typed != `"a\\nb"` {
		t.Errorf("got %q and %q, want %q and %q", realNewline, typed, `"a\nb"`, `"a\\nb"`)
	}
}

// TestHealthFindingsQuoteVersions exercises the store health paths: the
// version-skew finding shows both versions through the display rule while
// the comparison itself keeps using the exact original text.
func TestHealthFindingsQuoteVersions(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	rec := hb("n1", 1, receive.Add(-time.Minute), "v2\n正式版", 100, 0)
	if _, _, err := store.Submit([]Heartbeat{rec}, receive); err != nil {
		t.Fatalf("a version with a newline is legal and must be stored: %v", err)
	}

	r, err := store.Health("n1", receive, "v1\tbeta", 0)
	if err != nil {
		t.Fatal(err)
	}
	want := `version skew: "v2\n正式版" != "v1\tbeta"`
	if len(r.Findings) != 1 || r.Findings[0] != want {
		t.Errorf("findings = %v, want [%s]", r.Findings, want)
	}

	// The exact original text as the expected version produces no skew: the
	// comparison is not done on the quoted display form.
	r, err = store.Health("n1", receive, "v2\n正式版", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Findings) != 0 {
		t.Errorf("exact expected version must not skew, findings = %v", r.Findings)
	}

	// The stored record keeps the original text; the display form is never
	// written back.
	hist, err := store.History("n1")
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 1 || hist[0].Version != "v2\n正式版" {
		t.Errorf("history = %+v, want the original version text preserved", hist)
	}
}

// TestHealthSinceFindingsQuoteVersions checks the baseline query path shares
// the same display rule and finding order.
func TestHealthSinceFindingsQuoteVersions(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	receive := testBase
	records := []Heartbeat{
		hb("n1", 1, receive.Add(-2*time.Minute), "1.0", 100, 0),
		hb("n1", 2, receive.Add(-time.Minute), "1.0\x1b[2J", 101, 3),
	}
	if _, _, err := store.Submit(records, receive); err != nil {
		t.Fatal(err)
	}
	r, err := store.HealthSince("n1", receive, "1.0", 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		`version skew: "1.0\u001b[2J" != 1.0`,
		missedAboveToleranceFinding,
	}
	if len(r.Findings) != len(want) {
		t.Fatalf("findings = %v, want %v", r.Findings, want)
	}
	for i := range want {
		if r.Findings[i] != want[i] {
			t.Errorf("findings[%d] = %q, want %q (order must be unchanged)", i, r.Findings[i], want[i])
		}
	}
	if !r.NewMissedKnown || r.NewMissed != 3 {
		t.Errorf("baseline arithmetic changed: known=%v new=%d, want true/3", r.NewMissedKnown, r.NewMissed)
	}
}
