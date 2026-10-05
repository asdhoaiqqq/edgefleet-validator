package main

// Command-line regression tests for version text validity, mirroring the
// node-id coverage in main_cli_test.go. A version submitted through
// `heartbeat submit` must be losslessly representable Unicode text: invalid
// UTF-8 bytes and unpaired \u surrogate escapes are refused with a non-zero
// exit, an error naming the record position and the version field, and no
// stored change — while Chinese, emoji and a literal "�" stay accepted,
// and a saved file whose version was rewritten to replacement characters is
// reported as data corruption by every query.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestCLISubmitRejectsInvalidUTF8Version exercises the user-visible contract
// for a version that is not losslessly representable text: the JSON input
// holds a raw invalid UTF-8 byte, which a lenient reader would silently
// rewrite to "�" and merge into another version. The whole batch is refused
// with a non-zero exit, no success counts on stdout, an error naming the
// record and the version field on stderr, and no change to any stored data.
func TestCLISubmitRejectsInvalidUTF8Version(t *testing.T) {
	f := newFixture(t)
	monoBefore := readNodeFile(t, f.dir, "mono")

	goodRecord := `{"node":"mono","seq":4,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":104,"missed":15}`
	badRecord := `{"node":"newnode","seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0-` + "\xff" + `","height":1,"missed":0}`
	out, errOut, code := runCLI(t, f.dir, "["+goodRecord+","+badRecord+"]",
		"heartbeat", "submit", "--data-dir", f.dir, "--receive-time", cliReceiveAt)
	if code == 0 {
		t.Errorf("submit with an invalid UTF-8 version must exit non-zero; stdout=%q", out)
	}
	if strings.Contains(out, "submitted") {
		t.Errorf("rejected batch must not print success counts, stdout=%q", out)
	}
	if !strings.Contains(errOut, "record 2") || !strings.Contains(errOut, "version") {
		t.Errorf("stderr must name the failing record and the version field, got %q", errOut)
	}

	// The valid record in the same batch is not saved either.
	if got := readNodeFile(t, f.dir, "mono"); got != monoBefore {
		t.Errorf("existing node file changed during rejected submit")
	}
	if _, err := os.Stat(nodeFileHexPath(f.dir, "newnode")); !os.IsNotExist(err) {
		t.Errorf("no node file may be created for the rejected record, stat err=%v", err)
	}

	// Health answers for the existing node are unchanged.
	out, _, code = health(t, f,
		"--node", "mono", "--expected-version", "1.26.0", "--tolerated-misses", "9")
	if code != 0 || !strings.Contains(out, "seq=3") {
		t.Errorf("health after rejected batch changed: code=%d out=%q", code, out)
	}
}

// TestCLISubmitRejectsUnpairedSurrogateVersion covers the other silent
// rewrite: a \u escape that is an unpaired surrogate decodes to "�" instead
// of the intended character. The batch is refused and no node file is
// created.
func TestCLISubmitRejectsUnpairedSurrogateVersion(t *testing.T) {
	dir := t.TempDir()
	bs := "\\"
	for _, tc := range []struct {
		name    string
		version string
	}{
		{"lone high surrogate", "1.26.0-" + bs + "uD83D"},
		{"lone low surrogate", "1.26.0-" + bs + "uDE00"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			batch := `[{"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"` + tc.version + `","height":1,"missed":0}]`
			out, errOut, code := runCLI(t, dir, batch,
				"heartbeat", "submit", "--data-dir", dir, "--receive-time", cliReceiveAt)
			if code == 0 {
				t.Errorf("submit with an unpaired surrogate must exit non-zero; stdout=%q", out)
			}
			if !strings.Contains(errOut, "record 1") || !strings.Contains(errOut, "version") {
				t.Errorf("stderr must name the failing record and the version field, got %q", errOut)
			}
		})
	}
	// The batches are refused before the store is touched: no node file exists.
	files, err := os.ReadDir(filepath.Join(dir, "nodes"))
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if len(files) != 0 {
		t.Errorf("no node file may be created, found %d", len(files))
	}
}

// TestCLISubmitValidUnicodeVersionRoundTrip checks the legal cases keep
// working end to end: a literal "�" is valid text the user typed, an escape
// spelling of the same text is the same version (a duplicate, not a new
// record), and health keeps comparing the expected version exactly.
func TestCLISubmitValidUnicodeVersionRoundTrip(t *testing.T) {
	dir := t.TempDir()
	bs := "\\"

	// Literal replacement character in the version: legal text, accepted.
	out, _, code := runCLI(t, dir,
		`[{"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0-�","height":1,"missed":0}]`,
		"heartbeat", "submit", "--data-dir", dir, "--receive-time", cliReceiveAt)
	if code != 0 || !strings.Contains(out, "new=1") {
		t.Fatalf("literal replacement char must be accepted: code=%d out=%q", code, out)
	}

	// The same text written as a � escape is the same version: duplicate.
	out, _, code = runCLI(t, dir,
		`[{"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0-`+bs+`uFFFD","height":1,"missed":0}]`,
		"heartbeat", "submit", "--data-dir", dir, "--receive-time", cliReceiveAt)
	if code != 0 || !strings.Contains(out, "new=0 duplicate=1") {
		t.Errorf("escaped form of the same version must be a duplicate: code=%d out=%q", code, out)
	}

	// Chinese and an emoji written as a surrogate-pair escape are legal
	// versions; nothing is restricted to digits and dots.
	out, _, code = runCLI(t, dir,
		`[{"node":"n1","seq":2,"collected_at":"2026-10-01T11:59:59Z","version":"正式版-`+bs+`uD83D`+bs+`uDE00","height":2,"missed":0}]`,
		"heartbeat", "submit", "--data-dir", dir, "--receive-time", cliReceiveAt)
	if code != 0 || !strings.Contains(out, "new=1") {
		t.Fatalf("Chinese/emoji version must be accepted: code=%d out=%q", code, out)
	}

	// History shows the decoded text exactly, never re-escaped or rewritten.
	out, _, code = runCLI(t, dir, "",
		"heartbeat", "history", "--data-dir", dir, "--node", "n1")
	if code != 0 {
		t.Fatalf("history failed: code=%d", code)
	}
	if !strings.Contains(out, "version=1.26.0-�") || !strings.Contains(out, "version=正式版-😀") {
		t.Errorf("history must show the exact version texts, got %q", out)
	}

	// Health compares the expected version exactly: the same text is not a
	// skew, any other text is.
	out, _, code = runCLI(t, dir, "",
		"heartbeat", "health", "--data-dir", dir, "--node", "n1",
		"--expected-version", "正式版-😀", "--tolerated-misses", "0", "--at", cliQueryAt)
	if code != 0 {
		t.Fatalf("health failed: code=%d", code)
	}
	if strings.Contains(out, "version skew") {
		t.Errorf("exact expected version must not skew, got %q", out)
	}
	out, _, code = runCLI(t, dir, "",
		"heartbeat", "health", "--data-dir", dir, "--node", "n1",
		"--expected-version", "正式版", "--tolerated-misses", "0", "--at", cliQueryAt)
	if code != 0 || !strings.Contains(out, "version skew: 正式版-😀 != 正式版") {
		t.Errorf("different expected version must skew exactly, code=%d out=%q", code, out)
	}
}

// TestCLIStoredInvalidVersionIsCorrupt exercises the read boundary from the
// command line: a saved file whose version cannot decode losslessly is data
// corruption even when its checksum matches the replacement-char
// interpretation. History, plain health and baseline health all fail
// non-zero with nothing on stdout, a submit batch touching the node fails
// without overwriting the file, and other nodes keep working.
func TestCLIStoredInvalidVersionIsCorrupt(t *testing.T) {
	f := newFixture(t)
	bs := "\\"

	cases := []struct {
		name   string
		tamper string // raw version token stored in the file
	}{
		{"invalid UTF-8 byte", "1.26.0-" + "\xff"},
		{"unpaired surrogate escape", "1.26.0-" + bs + "uD83D"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// The forged checksum hashes the record a lenient reader would
			// reconstruct: the version rewritten to "1.26.0-�".
			tampered := `[{"node":"bad","seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"` + tc.tamper + `","height":100,"missed":0}]`
			interpreted := `[{"node":"bad","seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0-�","height":100,"missed":0}]`
			corruptNodeFile(t, f.dir, "bad", envelope(strictChecksum(interpreted), tampered))
			badBefore := readNodeFile(t, f.dir, "bad")
			monoBefore := readNodeFile(t, f.dir, "mono")

			// history: non-zero exit, reason on stderr naming the file, the
			// record and the version field, nothing on stdout.
			out, errOut, code := runCLI(t, f.dir, "",
				"heartbeat", "history", "--data-dir", f.dir, "--node", "bad")
			if code == 0 {
				t.Errorf("history on corrupt version must exit non-zero; stdout=%q", out)
			}
			if out != "" {
				t.Errorf("history must print no results on corruption, stdout=%q", out)
			}
			if !strings.Contains(errOut, "record 1") || !strings.Contains(errOut, "version") ||
				!strings.Contains(errOut, filepath.Base(nodeFileHexPath(f.dir, "bad"))) {
				t.Errorf("history stderr must name the file, record and version field, got %q", errOut)
			}

			// health, plain and with a missed-duty baseline: same contract.
			for _, extra := range [][]string{
				{},
				{"--missed-since-seq", "1"},
			} {
				args := append([]string{"--node", "bad", "--expected-version", "1.26.0", "--tolerated-misses", "0"}, extra...)
				out, errOut, code = health(t, f, args...)
				if code == 0 {
					t.Errorf("health %v on corrupt version must exit non-zero; stdout=%q", extra, out)
				}
				if out != "" {
					t.Errorf("health %v must print no results on corruption, stdout=%q", extra, out)
				}
				if !strings.Contains(errOut, "version") {
					t.Errorf("health %v stderr must name the version field, got %q", extra, errOut)
				}
			}

			// A submit batch touching the corrupt node is refused as a whole:
			// no success line, the corrupt file stays byte-for-byte intact,
			// and the valid record for a healthy node is not saved either.
			batch := `[
			  {"node":"bad","seq":2,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":101,"missed":0},
			  {"node":"mono","seq":4,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":104,"missed":15}
			]`
			out, errOut, code = runCLI(t, f.dir, batch,
				"heartbeat", "submit", "--data-dir", f.dir, "--receive-time", cliReceiveAt)
			if code == 0 {
				t.Errorf("submit touching a corrupt node must fail non-zero; stdout=%q", out)
			}
			if strings.Contains(out, "submitted") {
				t.Errorf("rejected batch must not report success, stdout=%q", out)
			}
			if !strings.Contains(errOut, "corruption") || !strings.Contains(errOut, "version") {
				t.Errorf("submit stderr must explain the corruption, got %q", errOut)
			}
			if got := readNodeFile(t, f.dir, "bad"); got != badBefore {
				t.Errorf("corrupt node file was modified during rejected submit")
			}
			if got := readNodeFile(t, f.dir, "mono"); got != monoBefore {
				t.Errorf("healthy node file was rewritten during rejected submit")
			}

			// Other nodes keep answering normally.
			out, _, code = health(t, f,
				"--node", "mono", "--expected-version", "1.26.0", "--tolerated-misses", "9")
			if code != 0 || !strings.Contains(out, "node=mono") {
				t.Errorf("healthy node must still query normally: code=%d out=%q", code, out)
			}
		})
	}
}
