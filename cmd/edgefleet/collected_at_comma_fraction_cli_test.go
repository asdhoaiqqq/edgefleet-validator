package main

// Command-line regression tests for the comma decimal separator in
// collected_at at the real `heartbeat submit` / `history` / `health` entry
// points. Go's time.Parse accepts both "." and "," as the fractional-second
// separator and silently truncates either form beyond nine digits. The
// validator used to check only ".", so comma fractions slipped through:
// ",1234567891" and ",1234567892" collapsed into one instant and were
// miscounted as duplicates. The comma spelling now obeys the exact same rule
// as the dot spelling, which these tests pin end to end:
//
//   - a comma fraction with a non-zero digit past the ninth rejects the whole
//     batch non-zero; stderr names the record position, collected_at and the
//     decoded fraction; stdout has no success line or counters;
//   - two comma moments that differ only past the ninth are refused, not
//     merged;
//   - a comma record that would truncate to an already-saved instant is
//     refused, never counted as a duplicate;
//   - exact comma forms (up to nine digits, or trailing zeros past the ninth)
//     submit, and ",123456789000" is a duplicate of the saved ".123456789"
//     (including another timezone spelling of the same instant);
//   - an equivalent JSON escape spelling of the comma or digits gets the same
//     accept/reject judgement on the decoded time text;
//   - a saved file carrying an unrepresentable comma fraction is corruption
//     on history/health and blocks further submits without overwriting the
//     file; the exact comma forms on disk stay readable.

import (
	"os"
	"strings"
	"testing"
)

// TestCLISubmitRejectsCommaFractionBeyondNanoseconds sends the bad comma
// record first and second; the error names its real position and the
// offending fraction.
func TestCLISubmitRejectsCommaFractionBeyondNanoseconds(t *testing.T) {
	cases := []struct {
		name  string
		stamp string
		frac  string
	}{
		{"tenth digit non-zero", "2026-10-01T11:59:00,1234567891Z", ",1234567891"},
		{"eleven digits, last non-zero", "2026-10-01T11:59:00,12345678901Z", ",12345678901"},
		{"non-zero far past the ninth", "2026-10-01T11:59:00,0000000000001Z", ",0000000000001"},
		{"non-zero tenth digit with offset", "2026-10-01T11:59:00,1234567891+08:00", ",1234567891"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bad := rec("bad", 1, tc.stamp)
			good := rec("good", 1, "2026-10-01T11:59:59Z")

			dir := t.TempDir()
			out, errOut, code := submit(t, dir, "["+bad+","+good+"]")
			assertRejectedBatch(t, out, errOut, code, "1", "collected_at")
			if !strings.Contains(errOut, tc.frac) {
				t.Errorf("stderr must quote the offending fraction %q, got %q", tc.frac, errOut)
			}
			for _, node := range []string{"bad", "good"} {
				if _, err := os.Stat(nodeFileHexPath(dir, node)); !os.IsNotExist(err) {
					t.Errorf("no file may be created for %q after rejection, stat err=%v", node, err)
				}
			}

			dir = t.TempDir()
			out, errOut, code = submit(t, dir, "["+good+","+bad+"]")
			assertRejectedBatch(t, out, errOut, code, "2", "collected_at")
			if !strings.Contains(errOut, tc.frac) {
				t.Errorf("stderr must quote the offending fraction %q, got %q", tc.frac, errOut)
			}
			for _, node := range []string{"bad", "good"} {
				if _, err := os.Stat(nodeFileHexPath(dir, node)); !os.IsNotExist(err) {
					t.Errorf("no file may be created for %q after rejection, stat err=%v", node, err)
				}
			}
		})
	}
}

// TestCLISubmitCommaTruncationCollisionRejected is the reported regression at
// the command boundary: two comma records whose moments differ only past the
// ninth digit would truncate to one instant and be miscounted as duplicates.
func TestCLISubmitCommaTruncationCollisionRejected(t *testing.T) {
	dir := t.TempDir()
	batch := "[" +
		rec("n1", 1, "2026-10-01T11:59:00,1234567891Z") + "," +
		rec("n1", 1, "2026-10-01T11:59:00,1234567892Z") +
		"]"
	out, errOut, code := submit(t, dir, batch)
	assertRejectedBatch(t, out, errOut, code, "1", "collected_at")
	if !strings.Contains(errOut, ",1234567891") {
		t.Errorf("stderr must quote the decoded comma fraction, got %q", errOut)
	}
	if _, err := os.Stat(nodeFileHexPath(dir, "n1")); !os.IsNotExist(err) {
		t.Errorf("no file may be created for n1, stat err=%v", err)
	}
}

// TestCLISubmitUnrepresentableCommaFractionNotADuplicate: the offending comma
// record truncates to an already-saved dot instant, but it must fail the
// whole batch as invalid rather than be absorbed as a duplicate.
func TestCLISubmitUnrepresentableCommaFractionNotADuplicate(t *testing.T) {
	dir := t.TempDir()
	submitBatch(t, dir, "["+rec("old", 1, "2026-10-01T11:59:00.123456789Z")+"]")
	oldBefore := readNodeFile(t, dir, "old")

	batch := "[" +
		rec("fresh", 1, "2026-10-01T11:59:58Z") + "," +
		rec("old", 2, "2026-10-01T11:59:58Z") + "," +
		// Truncates to the exact saved instant of old seq 1: position 3.
		rec("old", 1, "2026-10-01T11:59:00,1234567891Z") +
		"]"
	out, errOut, code := submit(t, dir, batch)
	assertRejectedBatch(t, out, errOut, code, "3", "collected_at")
	if !strings.Contains(errOut, ",1234567891") {
		t.Errorf("stderr must quote the offending comma fraction, got %q", errOut)
	}

	if got := readNodeFile(t, dir, "old"); got != oldBefore {
		t.Errorf("existing node file was modified during the rejected batch")
	}
	if _, err := os.Stat(nodeFileHexPath(dir, "fresh")); !os.IsNotExist(err) {
		t.Errorf("no file may be created for fresh, stat err=%v", err)
	}
	histOut, _, histCode := historyCLI(t, dir, "fresh")
	if histCode != 0 || strings.TrimRight(histOut, "\n") != "node=fresh no heartbeats" {
		t.Errorf("fresh must have no telemetry, code=%d out=%q", histCode, histOut)
	}
}

// TestCLISubmitAcceptsExactCommaForms pins the legal comma forms: nine digits
// save as new; a comma respelling with trailing zeros past the ninth and the
// dot respelling of the same instant are duplicates; a zero long fraction and
// another-zone spelling behave normally. History keeps the shared instant.
func TestCLISubmitAcceptsExactCommaForms(t *testing.T) {
	dir := t.TempDir()

	out, errOut, code := submit(t, dir, "["+rec("n1", 1, "2026-10-01T11:59:00,123456789Z")+"]")
	if code != 0 {
		t.Fatalf("nine-digit comma fraction must submit: exit=%d stderr=%q", code, errOut)
	}
	if strings.TrimRight(out, "\n") != "submitted: new=1 duplicate=0" {
		t.Errorf("stdout=%q, want new=1 duplicate=0", out)
	}

	// Same instant, comma with trailing zeros past the ninth: duplicate.
	out, errOut, code = submit(t, dir, "["+rec("n1", 1, "2026-10-01T11:59:00,123456789000Z")+"]")
	if code != 0 {
		t.Fatalf("comma trailing-zero form must submit: exit=%d stderr=%q", code, errOut)
	}
	if strings.TrimRight(out, "\n") != "submitted: new=0 duplicate=1" {
		t.Errorf("comma trailing-zero form: stdout=%q, want new=0 duplicate=1", out)
	}

	// Same instant spelled with a dot and a tenth zero: still a duplicate.
	out, errOut, code = submit(t, dir, "["+rec("n1", 1, "2026-10-01T11:59:00.1234567890Z")+"]")
	if code != 0 {
		t.Fatalf("dot trailing-zero form must submit: exit=%d stderr=%q", code, errOut)
	}
	if strings.TrimRight(out, "\n") != "submitted: new=0 duplicate=1" {
		t.Errorf("dot trailing-zero form: stdout=%q, want new=0 duplicate=1", out)
	}

	// Same instant in another zone, comma fraction: duplicate as well.
	out, errOut, code = submit(t, dir, "["+rec("n1", 1, "2026-10-01T19:59:00,123456789+08:00")+"]")
	if code != 0 {
		t.Fatalf("comma offset form must submit: exit=%d stderr=%q", code, errOut)
	}
	if strings.TrimRight(out, "\n") != "submitted: new=0 duplicate=1" {
		t.Errorf("comma offset form: stdout=%q, want new=0 duplicate=1", out)
	}

	// No fraction and an all-zero long comma fraction are ordinary input.
	out, errOut, code = submit(t, dir, "["+
		rec("n1", 2, "2026-10-01T11:59:30Z")+","+
		rec("n1", 3, "2026-10-01T11:59:40,0000000000000Z")+
		"]")
	if code != 0 {
		t.Fatalf("exact comma forms must submit: exit=%d stderr=%q", code, errOut)
	}
	if strings.TrimRight(out, "\n") != "submitted: new=2 duplicate=0" {
		t.Errorf("stdout=%q, want new=2 duplicate=0", out)
	}

	lines := historyLines(t, dir, "n1")
	if len(lines) != 3 {
		t.Fatalf("history = %q, want three records", lines)
	}
	if !strings.Contains(lines[0], "node=n1 seq=1") ||
		!strings.Contains(lines[0], "collected_at=2026-10-01T11:59:00Z") {
		t.Errorf("first history line must keep seq 1 at the shared whole-second display: %q", lines[0])
	}
	// The canonical save writes the surviving instant back with a dot; the
	// nanosecond fraction is preserved on disk regardless of input separator.
	if raw := readNodeFile(t, dir, "n1"); !strings.Contains(raw, ".123456789") {
		t.Errorf("saved file must keep the nanosecond fraction: %s", raw)
	}
}

// TestCLISubmitCommaJSONEscapeEquivalence: comma and digits written with JSON
// unicode escapes decode to the same time text, so the accept/reject rule is
// identical. The rejected message shows the decoded fraction (comma).
func TestCLISubmitCommaJSONEscapeEquivalence(t *testing.T) {
	dir := t.TempDir()

	// JSON unicode escapes in the payload: U+002C comma, U+0030 zero,
	// U+0031 one. The decoded time text is what the rule is judged on.
	escComma := "\\u002C"
	escZero := "\\u0030"
	escOne := "\\u0031"

	// Exact form with the comma escaped: accepted as new.
	exact := "2026-10-01T11:59:00" + escComma + "123456789Z"
	out, errOut, code := submit(t, dir, "["+rec("n1", 1, exact)+"]")
	if code != 0 {
		t.Fatalf("escaped comma exact form must submit: exit=%d stderr=%q", code, errOut)
	}
	if strings.TrimRight(out, "\n") != "submitted: new=1 duplicate=0" {
		t.Errorf("stdout=%q, want new=1 duplicate=0", out)
	}

	// The trailing-zero comma respelling, comma and final zero escaped, is the
	// same instant: duplicate.
	zero := "2026-10-01T11:59:00" + escComma + "123456789" + escZero + "Z"
	out, errOut, code = submit(t, dir, "["+rec("n1", 1, zero)+"]")
	if code != 0 {
		t.Fatalf("escaped trailing-zero comma form must submit: exit=%d stderr=%q", code, errOut)
	}
	if strings.TrimRight(out, "\n") != "submitted: new=0 duplicate=1" {
		t.Errorf("stdout=%q, want new=0 duplicate=1", out)
	}

	// The unrepresentable tenth digit, comma and digit escaped, is rejected on
	// the decoded text exactly like the literal comma spelling.
	bad := "2026-10-01T11:59:00" + escComma + "123456789" + escOne + "Z"
	out, errOut, code = submit(t, dir, "["+rec("n1", 2, bad)+"]")
	if code == 0 {
		t.Errorf("escaped unrepresentable comma fraction must be rejected; stdout=%q", out)
	}
	if strings.Contains(out, "submitted") || strings.Contains(out, "new=") {
		t.Errorf("rejected batch must print no success counts, stdout=%q", out)
	}
	if !strings.Contains(errOut, "record 1") || !strings.Contains(errOut, "collected_at") ||
		!strings.Contains(errOut, ",1234567891") {
		t.Errorf("stderr must locate record 1 and quote the decoded fraction, got %q", errOut)
	}
	if _, err := os.Stat(nodeFileHexPath(dir, "n1")); err != nil {
		t.Fatalf("the earlier accepted record must remain saved: %v", err)
	}
}

// TestCLIStoredUnrepresentableCommaFractionIsCorrupt forges a node file whose
// collected_at carries a non-zero digit past the ninth behind a comma, with a
// checksum matching the truncated dot interpretation. History and health
// report corruption naming the record, field and fraction; a further submit is
// refused as a whole without overwriting the file. The exact comma forms on
// disk (nine digits, trailing zeros) stay readable.
func TestCLIStoredUnrepresentableCommaFractionIsCorrupt(t *testing.T) {
	dir := t.TempDir()
	submitBatch(t, dir, "["+rec("bad", 1, "2026-10-01T11:59:59Z")+"]")

	tampered := `[{"node":"bad","seq":1,"collected_at":"2026-10-01T11:59:00,12345678901Z","version":"1.26.0","height":100,"missed":0}]`
	truncated := `[{"node":"bad","seq":1,"collected_at":"2026-10-01T11:59:00.123456789Z","version":"1.26.0","height":100,"missed":0}]`
	corruptNodeFile(t, dir, "bad", envelope(strictChecksum(truncated), tampered))
	corruptBefore := readNodeFile(t, dir, "bad")

	out, errOut, code := historyCLI(t, dir, "bad")
	if code == 0 {
		t.Errorf("history on corrupt comma node must exit non-zero; stdout=%q", out)
	}
	if out != "" {
		t.Errorf("history must print no results on corruption, stdout=%q", out)
	}
	for _, want := range []string{"corruption", "record 1", "collected_at", ",12345678901"} {
		if !strings.Contains(errOut, want) {
			t.Errorf("history stderr must contain %q, got %q", want, errOut)
		}
	}

	out, errOut, code = runCLI(t, dir, "",
		"heartbeat", "health", "--data-dir", dir, "--at", cliQueryAt,
		"--node", "bad", "--expected-version", "1.26.0", "--tolerated-misses", "0")
	if code == 0 {
		t.Errorf("health on corrupt comma node must exit non-zero; stdout=%q", out)
	}
	if out != "" {
		t.Errorf("health must print no results on corruption, stdout=%q", out)
	}
	for _, want := range []string{"corruption", "record 1", "collected_at"} {
		if !strings.Contains(errOut, want) {
			t.Errorf("health stderr must contain %q, got %q", want, errOut)
		}
	}

	out, errOut, code = submit(t, dir, "["+rec("bad", 2, "2026-10-01T11:59:59Z")+"]")
	if code == 0 {
		t.Errorf("submit to a corrupt comma node must exit non-zero; stdout=%q", out)
	}
	if strings.Contains(out, "submitted") {
		t.Errorf("refused submit must not print success counts, stdout=%q", out)
	}
	if !strings.Contains(errOut, "corruption") {
		t.Errorf("submit stderr must report the corruption, got %q", errOut)
	}
	if got := readNodeFile(t, dir, "bad"); got != corruptBefore {
		t.Errorf("corrupt file was overwritten by the refused submit")
	}

	// Exact comma forms on disk stay readable. The history display uses the
	// whole-second RFC3339 layout (as for dot fractions), so readability is
	// pinned on the successful exit and seq; the decoded nanosecond instant
	// itself is verified in package edgefleet, and the comma text must remain
	// untouched in the raw file (reads never rewrite it).
	for _, stamp := range []string{
		"2026-10-01T11:59:00,123456789Z",
		"2026-10-01T11:59:00,1234567890Z",
		"2026-10-01T11:59:00,123456789000Z",
	} {
		rawRecord := `[{"node":"bad","seq":1,"collected_at":"` + stamp + `","version":"1.26.0","height":100,"missed":0}]`
		corruptNodeFile(t, dir, "bad", envelope(strictChecksum(truncated), rawRecord))
		lines := historyLines(t, dir, "bad")
		if len(lines) != 1 || !strings.Contains(lines[0], "node=bad seq=1") {
			t.Errorf("exact comma form %s must stay readable, got %q", stamp, lines)
		}
		if raw := readNodeFile(t, dir, "bad"); !strings.Contains(raw, stamp) {
			t.Errorf("readable comma file must be left verbatim on disk: %s", raw)
		}
	}
}
