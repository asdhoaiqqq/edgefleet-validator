package main

// Command-line regression tests for the health query display of node ids and
// versions that contain whitespace, quotes, backslashes or control
// characters. Such text is legal and stays submittable, storable and
// queryable; the health output renders it through the shared display rule
// (plain text as-is, anything else as a quoted JSON string) so that:
//
//   - a health query always prints exactly one line — two with a missed-duty
//     baseline — and node text can never masquerade as extra fields, extra
//     result lines or a fake baseline explanation;
//   - no raw control character reaches the terminal (a clear-screen escape
//     in a version cannot hide the findings);
//   - the quoted display value is a valid JSON string that decodes back to
//     the exact original text, keeping a real newline distinct from a typed
//     backslash-n;
//   - identity and version comparisons still use the original text, and the
//     stored heartbeats are never rewritten to the display form.

import (
	"encoding/json"
	"strings"
	"testing"
)

// trickyNode holds a real newline; trickyVersion holds a real tab. Both are
// legal Unicode text that the platform must accept and keep.
const (
	trickyNode    = "val\neu-1"
	trickyVersion = "1.26.0\tfix"
)

const trickyRecord = `{"node":"val\neu-1","seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0\tfix","height":100,"missed":0}`

// wantTrickyHealthLine is the exact single line for the tricky record: node
// id, version and the skew finding's actual side all quoted, the plain
// expected version unquoted.
const wantTrickyHealthLine = `node="val\neu-1" status=online seq=1 collected_at=2026-10-01T11:59:59Z version="1.26.0\tfix" height=100 missed=0 findings=[version skew: "1.26.0\tfix" != 1.26.0]`

// submitTricky stores the tricky record through the real submit command.
func submitTricky(t *testing.T, dir string) {
	t.Helper()
	submitBatch(t, dir, "["+trickyRecord+"]")
}

// assertNoRawControlChars fails if the output holds any raw control
// character other than the line-terminating newline.
func assertNoRawControlChars(t *testing.T, out string) {
	t.Helper()
	for _, r := range out {
		if (r < 0x20 && r != '\n') || r == 0x7f {
			t.Errorf("output emits a raw control character U+%04X: %q", r, out)
		}
	}
}

// TestCLIHealthQuotesNodeAndVersionDisplay pins the exact single health line
// for a node id with a real newline and a version with a real tab.
func TestCLIHealthQuotesNodeAndVersionDisplay(t *testing.T) {
	dir := t.TempDir()
	submitTricky(t, dir)

	out, errOut, code := healthQueryAt(t, dir, cliQueryAt,
		"--node", trickyNode, "--expected-version", "1.26.0", "--tolerated-misses", "0")
	if code != 0 {
		t.Fatalf("health query must succeed, exit=%d stderr=%q", code, errOut)
	}
	lines := outputLines(out)
	if len(lines) != 1 {
		t.Fatalf("a newline in the node id must not split the result, got %d lines: %q", len(lines), out)
	}
	if lines[0] != wantTrickyHealthLine {
		t.Errorf("health line = %q\nwant          %q", lines[0], wantTrickyHealthLine)
	}
	assertNoRawControlChars(t, out)
}

// TestCLIHealthDisplayedNodeDecodesToOriginal extracts the displayed node
// value and requires it to be a JSON string decoding back to the exact id,
// so a real newline and a typed backslash-n stay distinguishable.
func TestCLIHealthDisplayedNodeDecodesToOriginal(t *testing.T) {
	dir := t.TempDir()
	submitTricky(t, dir)

	out, _, code := healthQueryAt(t, dir, cliQueryAt,
		"--node", trickyNode, "--expected-version", "1.26.0", "--tolerated-misses", "0")
	if code != 0 {
		t.Fatalf("exit=%d", code)
	}
	line := outputLines(out)[0]
	if !strings.HasPrefix(line, `node="`) {
		t.Fatalf("node with a newline must be displayed quoted: %q", line)
	}
	displayed := strings.TrimPrefix(line, "node=")
	displayed = displayed[:strings.Index(displayed, `" `)+1]
	var decoded string
	if err := json.Unmarshal([]byte(displayed), &decoded); err != nil {
		t.Fatalf("displayed node value %q is not a valid JSON string: %v", displayed, err)
	}
	if decoded != trickyNode {
		t.Errorf("decoded node = %q, want the original %q", decoded, trickyNode)
	}

	// A node id that is literally backslash-n (no real newline) displays
	// differently from one with a real newline.
	other := `{"node":"val\\neu-1","seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":100,"missed":0}`
	submitBatch(t, dir, "["+other+"]")
	out, _, code = healthQueryAt(t, dir, cliQueryAt,
		"--node", `val\neu-1`, "--expected-version", "1.26.0", "--tolerated-misses", "0")
	if code != 0 {
		t.Fatalf("exit=%d", code)
	}
	line = outputLines(out)[0]
	if !strings.HasPrefix(line, `node="val\\neu-1" `) {
		t.Errorf("literal backslash-n node id must display with an escaped backslash: %q", line)
	}
	if strings.Contains(line, `node="val\neu-1" `) {
		t.Errorf("literal backslash-n must not display like a real newline: %q", line)
	}
}

// TestCLIHealthNodeMimickingFieldsStaysOneValue submits a node id spelling
// out plausible health output text and requires it to appear as one quoted
// node value, never as real fields.
func TestCLIHealthNodeMimickingFieldsStaysOneValue(t *testing.T) {
	dir := t.TempDir()
	node := "ghost status=offline missed=99"
	rec := `{"node":"ghost status=offline missed=99","seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"1.0","height":1,"missed":0}`
	submitBatch(t, dir, "["+rec+"]")

	out, _, code := healthQueryAt(t, dir, cliQueryAt,
		"--node", node, "--expected-version", "1.0", "--tolerated-misses", "0")
	if code != 0 {
		t.Fatalf("exit=%d", code)
	}
	lines := outputLines(out)
	if len(lines) != 1 {
		t.Fatalf("want exactly one line, got %q", out)
	}
	wantPrefix := `node="ghost status=offline missed=99" status=online `
	if !strings.HasPrefix(lines[0], wantPrefix) {
		t.Errorf("node id with spaces must be one quoted value, line = %q", lines[0])
	}
	if !strings.Contains(lines[0], "missed=0 ") && !strings.HasSuffix(lines[0], "missed=0") {
		t.Errorf("the real missed=0 field must be present, line = %q", lines[0])
	}
}

// TestCLIHealthBaselineOutputStaysTwoLines checks a node id with a newline
// cannot inject a fake baseline explanation: the output is exactly the
// health line plus the genuine baseline line.
func TestCLIHealthBaselineOutputStaysTwoLines(t *testing.T) {
	dir := t.TempDir()
	submitBatch(t, dir, `[
	  {"node":"val\neu-1","seq":1,"collected_at":"2026-10-01T11:59:58Z","version":"1.26.0","height":99,"missed":1},
	  {"node":"val\neu-1","seq":2,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":100,"missed":3}
	]`)

	out, _, code := healthQueryAt(t, dir, cliQueryAt,
		"--node", trickyNode, "--expected-version", "1.26.0",
		"--tolerated-misses", "1", "--missed-since-seq", "1")
	if code != 0 {
		t.Fatalf("exit=%d", code)
	}
	lines := outputLines(out)
	if len(lines) != 2 {
		t.Fatalf("baseline query must print exactly two lines, got %d: %q", len(lines), out)
	}
	if !strings.HasPrefix(lines[0], `node="val\neu-1" status=online seq=2 `) {
		t.Errorf("first line = %q", lines[0])
	}
	if lines[1] != "baseline_seq=1 baseline_missed=1 new_missed=2 tolerated_misses=1" {
		t.Errorf("second line = %q", lines[1])
	}
	assertNoRawControlChars(t, out)
}

// TestCLIHealthNotelemetryQuotesNode checks the notelemetry line follows the
// same display rule for the node id.
func TestCLIHealthNotelemetryQuotesNode(t *testing.T) {
	dir := t.TempDir()
	out, _, code := healthQueryAt(t, dir, cliQueryAt,
		"--node", "no\nsuch\tnode", "--expected-version", "1.0", "--tolerated-misses", "0")
	if code != 0 {
		t.Fatalf("notelemetry query must exit 0, got %d", code)
	}
	lines := outputLines(out)
	if len(lines) != 1 {
		t.Fatalf("notelemetry must be exactly one line, got %q", out)
	}
	want := `node="no\nsuch\tnode" status=无遥测 findings=[无遥测]`
	if lines[0] != want {
		t.Errorf("notelemetry line = %q, want %q", lines[0], want)
	}
	assertNoRawControlChars(t, out)
}

// TestCLIHealthControlCharsNeverReachTerminal submits a version carrying a
// clear-screen escape sequence and checks the health line shows it as a
// visible escape instead of emitting it.
func TestCLIHealthControlCharsNeverReachTerminal(t *testing.T) {
	dir := t.TempDir()
	rec := `{"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"1.0\u001b[2J","height":1,"missed":0}`
	submitBatch(t, dir, "["+rec+"]")

	out, _, code := healthQueryAt(t, dir, cliQueryAt,
		"--node", "n1", "--expected-version", "1.0", "--tolerated-misses", "0")
	if code != 0 {
		t.Fatalf("exit=%d", code)
	}
	assertNoRawControlChars(t, out)
	line := outputLines(out)[0]
	if !strings.Contains(line, `version="1.0\u001b[2J"`) {
		t.Errorf("version escape must display as visible escape text: %q", line)
	}
	if !strings.Contains(line, `version skew: "1.0\u001b[2J" != 1.0`) {
		t.Errorf("skew finding must use the same display rule: %q", line)
	}

	// The stored record keeps the original bytes; the display form is never
	// written back.
	raw := readNodeFile(t, dir, "n1")
	if !strings.Contains(raw, `"1.0\u001b[2J"`) {
		t.Errorf("stored file must keep the original version text: %s", raw)
	}
}
