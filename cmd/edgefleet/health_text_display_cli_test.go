package main

// Command-line regression tests for terminal-safe rendering of node ids and
// versions in `edgefleet heartbeat health` output. Node ids and versions are
// arbitrary non-empty Unicode text: submit, storage and query keep accepting
// them, and the identity/version comparison keeps using the raw text. Only
// the health presentation changes —
//
//   - text without whitespace, quotes, backslashes or control characters is
//     printed as-is (including Chinese, emoji and a literal replacement
//     character);
//   - all other text is one quoted JSON value: real newlines show as \n,
//     carriage returns as \r, tabs as \t, quotes/backslashes per JSON, and
//     every other control character (form feed, escape, ...) as \u00XX;
//   - a quoted node value can therefore never forge a second line or extra
//     key=value fields, a quoted version can never shift columns or clear the
//     screen, and the version-skew finding renders both versions by the same
//     rule;
//   - a plain query stays one physical line and a baseline query stays
//     exactly two (the result and the real baseline explanation).

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// healthRecordJSON builds one submit record, JSON-encoding the free-text node
// and version values so tests can use raw Go strings.
func healthRecordJSON(node, version string, seq int, missed int64) string {
	n, _ := json.Marshal(node)
	v, _ := json.Marshal(version)
	return fmt.Sprintf(`{"node":%s,"seq":%d,"collected_at":"2026-10-01T11:59:59Z","version":%s,"height":%d,"missed":%d}`,
		n, seq, v, seq*100, missed)
}

// assertSafeTerminalOutput fails if stdout contains a raw control byte other
// than the '\n' line terminators themselves: tabs, CRs, form feeds, escapes
// and the like must all have been rendered as visible escapes.
func assertSafeTerminalOutput(t *testing.T, out string) {
	t.Helper()
	for i := 0; i < len(out); i++ {
		c := out[i]
		if c == '\n' {
			continue
		}
		if c < 0x20 || c == 0x7f {
			t.Errorf("raw control byte 0x%02x leaked into terminal output: %q", c, out)
		}
	}
}

// TestCLIHealthNewlineInNodeIDStaysOneLine submits a node whose id contains a
// real newline and requires the health result to be a single physical line
// with the id shown as the quoted JSON string ("a\nb"), never split.
func TestCLIHealthNewlineInNodeIDStaysOneLine(t *testing.T) {
	dir := t.TempDir()
	submitBatch(t, dir, "["+healthRecordJSON("a\nb", "1.0", 1, 0)+"]")

	out, errOut, code := runCLI(t, dir, "",
		"heartbeat", "health", "--data-dir", dir, "--at", cliQueryAt,
		"--node", "a\nb", "--expected-version", "1.0", "--tolerated-misses", "0")
	if code != 0 {
		t.Fatalf("health must exit 0, got %d: stderr=%q", code, errOut)
	}
	assertSafeTerminalOutput(t, out)
	lines := outputLines(out)
	if len(lines) != 1 {
		t.Fatalf("newline in node id must not split the result, got %d lines: %q", len(lines), out)
	}
	want := `node="a\nb" status=online seq=1 collected_at=2026-10-01T11:59:59Z version=1.0 height=100 missed=0 findings=[]`
	if lines[0] != want {
		t.Errorf("line = %q\nwant   %q", lines[0], want)
	}
}

// TestCLIHealthSpacedNodeIDCannotForgeFields checks a node id containing
// spaces and key=value-looking text is one complete quoted node value; the
// embedded "status=online" is only content inside the quotes.
func TestCLIHealthSpacedNodeIDCannotForgeFields(t *testing.T) {
	dir := t.TempDir()
	nodeID := "x status=online\nnode=forged baseline_seq=9"
	submitBatch(t, dir, "["+healthRecordJSON(nodeID, "1.0", 1, 0)+"]")

	out, _, code := runCLI(t, dir, "",
		"heartbeat", "health", "--data-dir", dir, "--at", cliQueryAt,
		"--node", nodeID, "--expected-version", "1.0", "--tolerated-misses", "0")
	if code != 0 {
		t.Fatalf("health must exit 0, out=%q", out)
	}
	assertSafeTerminalOutput(t, out)
	lines := outputLines(out)
	if len(lines) != 1 {
		t.Fatalf("forged text in the node id must stay inside the quoted value, got %d lines: %q", len(lines), out)
	}
	want := `node="x status=online\nnode=forged baseline_seq=9" status=online seq=1 collected_at=2026-10-01T11:59:59Z version=1.0 height=100 missed=0 findings=[]`
	if lines[0] != want {
		t.Errorf("line = %q\nwant   %q", lines[0], want)
	}
}

// TestCLIHealthControlCharactersInVersionAreVisible submits versions carrying
// a tab, a form feed (which clears many real terminals) and an escape
// sequence; the version field and the version-skew finding must show the
// visible escapes and contain none of the raw bytes.
func TestCLIHealthControlCharactersInVersionAreVisible(t *testing.T) {
	cases := []struct {
		name    string
		version string
		display string
	}{
		{"tab", "1.0\t2.0", `"1.0\t2.0"`},
		{"form feed", "1.0\f2.0", `"1.0\u000c2.0"`},
		{"clear screen", "1.0\x1b[2J", `"1.0\u001b[2J"`},
		{"newline", "1.0\n2.0", `"1.0\n2.0"`},
		{"carriage return", "1.0\r2.0", `"1.0\r2.0"`},
		{"quote", `1.0"2.0`, `"1.0\"2.0"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			submitBatch(t, dir, "["+healthRecordJSON("n1", tc.version, 1, 0)+"]")

			// The exact same raw text as the expected version: no skew, proving
			// comparison still uses the unmodified text even though it is
			// printed quoted.
			out, errOut, code := runCLI(t, dir, "",
				"heartbeat", "health", "--data-dir", dir, "--at", cliQueryAt,
				"--node", "n1", "--expected-version", tc.version, "--tolerated-misses", "0")
			if code != 0 {
				t.Fatalf("exact raw version must match and exit 0: %d stderr=%q", code, errOut)
			}
			assertSafeTerminalOutput(t, out)
			lines := outputLines(out)
			if len(lines) != 1 {
				t.Fatalf("want one line, got %q", out)
			}
			if !strings.Contains(lines[0], "version="+tc.display+" ") {
				t.Errorf("line %q missing quoted version %s", lines[0], tc.display)
			}
			if strings.Contains(lines[0], "version skew") {
				t.Errorf("raw-equal versions must not skew: %q", lines[0])
			}

			// A different expected version renders BOTH versions by the same
			// rule inside the single skew finding.
			out, _, code = runCLI(t, dir, "",
				"heartbeat", "health", "--data-dir", dir, "--at", cliQueryAt,
				"--node", "n1", "--expected-version", "9.9\t!", "--tolerated-misses", "0")
			if code != 0 {
				t.Fatalf("skew query must exit 0: out=%q", out)
			}
			assertSafeTerminalOutput(t, out)
			lines = outputLines(out)
			if len(lines) != 1 {
				t.Fatalf("skew finding must stay on one line, got %q", out)
			}
			wantFinding := "version skew: " + tc.display + ` != "9.9\t!"`
			if !strings.Contains(lines[0], wantFinding) {
				t.Errorf("line %q missing finding %q", lines[0], wantFinding)
			}
		})
	}
}

// TestCLIHealthDistinguishesRealNewlineFromBackslashN covers the two different
// contents: a genuine newline and a backslash followed by the letter n must
// look different on screen, each quoted value decoding back to its original.
func TestCLIHealthDistinguishesRealNewlineFromBackslashN(t *testing.T) {
	dir := t.TempDir()
	submitBatch(t, dir, "["+
		healthRecordJSON("realnl", "1.0\nx", 1, 0)+","+
		healthRecordJSON(`literal`, `1.0\nx`, 2, 0)+"]")

	out, _, code := runCLI(t, dir, "",
		"heartbeat", "health", "--data-dir", dir, "--at", cliQueryAt,
		"--node", "realnl", "--expected-version", "other", "--tolerated-misses", "0")
	if code != 0 {
		t.Fatalf("query failed: %q", out)
	}
	if !strings.Contains(outputLines(out)[0], `version="1.0\nx"`) {
		t.Errorf("real newline must show as \\n inside quotes: %q", out)
	}

	out, _, code = runCLI(t, dir, "",
		"heartbeat", "health", "--data-dir", dir, "--at", cliQueryAt,
		"--node", "literal", "--expected-version", "other", "--tolerated-misses", "0")
	if code != 0 {
		t.Fatalf("query failed: %q", out)
	}
	if !strings.Contains(outputLines(out)[0], `version="1.0\\nx"`) {
		t.Errorf("literal backslash-n must show as \\\\n inside quotes: %q", out)
	}
}

// TestCLIHealthReadableUnicodeStaysBare checks Chinese, emoji and a typed
// replacement character keep printing directly, in both the node and version
// positions and inside the skew finding.
func TestCLIHealthReadableUnicodeStaysBare(t *testing.T) {
	dir := t.TempDir()
	submitBatch(t, dir, "["+healthRecordJSON("节点-😀-�", "正式版-😀-�", 1, 0)+"]")

	out, errOut, code := runCLI(t, dir, "",
		"heartbeat", "health", "--data-dir", dir, "--at", cliQueryAt,
		"--node", "节点-😀-�", "--expected-version", "正式版-😀-�", "--tolerated-misses", "0")
	if code != 0 {
		t.Fatalf("exact unicode version must match, exit=%d stderr=%q", code, errOut)
	}
	lines := outputLines(out)
	want := "node=节点-😀-� status=online seq=1 collected_at=2026-10-01T11:59:59Z version=正式版-😀-� height=100 missed=0 findings=[]"
	if len(lines) != 1 || lines[0] != want {
		t.Errorf("line = %q\nwant   %q", out, want)
	}

	// The skew finding keeps readable text bare; a newline in the expected
	// side quotes only that side.
	out, _, code = runCLI(t, dir, "",
		"heartbeat", "health", "--data-dir", dir, "--at", cliQueryAt,
		"--node", "节点-😀-�", "--expected-version", "正式版\n旧", "--tolerated-misses", "0")
	if code != 0 {
		t.Fatalf("skew query must exit 0: %q", out)
	}
	assertSafeTerminalOutput(t, out)
	lines = outputLines(out)
	if len(lines) != 1 {
		t.Fatalf("skew must stay one line, got %q", out)
	}
	wantFinding := "version skew: 正式版-😀-� != " + `"正式版\n旧"`
	if !strings.Contains(lines[0], wantFinding) {
		t.Errorf("line %q missing %q", lines[0], wantFinding)
	}
}

// TestCLIHealthBaselineNewlineNodePrintsExactlyTwoLines ensures a node id with
// a newline cannot masquerade as the baseline explanation: the result is
// quoted on line one and the real baseline line is still the only line two.
func TestCLIHealthBaselineNewlineNodePrintsExactlyTwoLines(t *testing.T) {
	dir := t.TempDir()
	nodeID := "n\nbaseline_seq=999 tolerated_misses=0"
	submitBatch(t, dir, "["+
		healthRecordJSON(nodeID, "1.0", 1, 2)+","+
		healthRecordJSON(nodeID, "1.0", 2, 5)+"]")

	out, errOut, code := runCLI(t, dir, "",
		"heartbeat", "health", "--data-dir", dir, "--at", cliQueryAt,
		"--node", nodeID, "--expected-version", "1.0",
		"--tolerated-misses", "2", "--missed-since-seq", "1")
	if code != 0 {
		t.Fatalf("baseline query must exit 0, got %d: stderr=%q", code, errOut)
	}
	assertSafeTerminalOutput(t, out)
	lines := outputLines(out)
	if len(lines) != 2 {
		t.Fatalf("want exactly two lines, got %d: %q", len(lines), out)
	}
	wantFirst := `node="n\nbaseline_seq=999 tolerated_misses=0" status=online seq=2 collected_at=2026-10-01T11:59:59Z version=1.0 height=200 missed=5 findings=[missed duties above tolerance]`
	if lines[0] != wantFirst {
		t.Errorf("first line = %q\nwant         %q", lines[0], wantFirst)
	}
	if lines[1] != "baseline_seq=1 baseline_missed=2 new_missed=3 tolerated_misses=2" {
		t.Errorf("second line = %q (the real baseline must be the only second line)", lines[1])
	}
}

// TestCLIHealthNoTelemetryNodeUsesSameDisplayRule checks the notelemetry
// answer quotes a tricky node id under the same rule and remains one line.
func TestCLIHealthNoTelemetryNodeUsesSameDisplayRule(t *testing.T) {
	dir := t.TempDir()
	out, errOut, code := runCLI(t, dir, "",
		"heartbeat", "health", "--data-dir", dir, "--at", cliQueryAt,
		"--node", "ghost\nx", "--expected-version", "1.0", "--tolerated-misses", "0")
	if code != 0 {
		t.Fatalf("notelemetry must exit 0, got %d: stderr=%q", code, errOut)
	}
	assertSafeTerminalOutput(t, out)
	lines := outputLines(out)
	if len(lines) != 1 {
		t.Fatalf("notelemetry must stay one line, got %q", out)
	}
	want := `node="ghost\nx" status=无遥测 findings=[无遥测]`
	if lines[0] != want {
		t.Errorf("line = %q\nwant   %q", lines[0], want)
	}
}
