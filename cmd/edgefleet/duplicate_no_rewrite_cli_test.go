package main

// End-to-end regression tests for "a node whose batch records are all already
// saved does not have its file re-saved", exercised through the real
// `heartbeat submit` command in a re-executed process.
//
// The package-level tests watch the persistence seam directly. From the
// command boundary that seam is unreachable, so these tests create the real
// outage a redundant rewrite would hit — deterministically, offline and with
// no file-time waiting:
//
//   - Short ids persist under <data>/nodes and long ids under <data>/slots.
//     Making one of those directories read-only still allows the directory
//     lock (held on <data>/lock) to be taken and saved history to be read, but
//     makes an atomic node-file replacement fail at temp-file creation.
//   - A control submit that genuinely needs a write under that condition is
//     asserted to FAIL, proving the fault really blocks writes in this setup.
//     Therefore an all-duplicate submit that SUCCEeds proves no rewrite was
//     attempted — byte-identical files alone could not show that.
//   - The two directories let a mixed batch scope the fault per node: a
//     duplicate-only short node (nodes/ read-only) must not block a genuine
//     new record for a long node writing into the writable slots/, and the
//     reverse layout behaves the same.
//
// Identity/content rules are unchanged: equivalent timezone and JSON-escape
// spellings still count as duplicates with occurrence-based counts, while the
// original stored text is preserved.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// readonlyDir chmods a directory read-only (and executable, so files in it can
// still be opened and read) and restores it before the temp directory is
// removed. It is not skipped when the tests happen to run as root: under root
// permission bits do not block writes, in which case the caller's control
// assertion fails first and reports the environment rather than passing
// vacuously.
func readonlyDir(t *testing.T, dir string) {
	t.Helper()
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatalf("chmod %s read-only: %v", dir, err)
	}
	t.Cleanup(func() {
		if err := os.Chmod(dir, 0o755); err != nil {
			t.Errorf("restore %s permissions: %v", dir, err)
		}
	})
}

// assertNoTempFiles fails if an atomic-write temp file was left (or ever
// created) in dir: a skipped rewrite must not leave .tmp-* residue.
func assertNoTempFiles(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".tmp-") {
			t.Errorf("a skipped rewrite must not create temp files, found %s in %s", e.Name(), dir)
		}
	}
}

// TestCLIDuplicateOnlySubmitSucceedsWhenNodesDirCannotRewrite covers the
// short-id layout: reconnect retransmits already-saved records while the
// nodes directory cannot accept a replacement file. The submit must still
// succeed (new=0, duplicates counted per input occurrence), history must gain
// no row and the file must be byte-identical. A control new-record submit
// proves the read-only directory really does block a needed write.
func TestCLIDuplicateOnlySubmitSucceedsWhenNodesDirCannotRewrite(t *testing.T) {
	dir := t.TempDir()
	submitBatch(t, dir, `[
	  {"node":"dup","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.26.0","height":12345,"missed":0},
	  {"node":"dup","seq":2,"collected_at":"2026-10-01T11:59:30Z","version":"1.26.0","height":12350,"missed":0}
	]`)
	before := readNodeFile(t, dir, "dup")
	nodesDir := filepath.Join(dir, "nodes")
	readonlyDir(t, nodesDir)

	// Control: a genuinely new seq cannot be persisted while nodes/ is
	// read-only. This pins the fault: if it did not fail, the success below
	// would prove nothing about whether a rewrite happened.
	out, errOut, code := runCLI(t, dir, `[
	  {"node":"dup","seq":3,"collected_at":"2026-10-01T11:59:45Z","version":"1.26.0","height":12355,"missed":0}
	]`, "heartbeat", "submit", "--data-dir", dir, "--receive-time", cliReceiveAt)
	if code == 0 {
		t.Fatalf("control: a needed write must fail with nodes/ read-only; stdout=%q", out)
	}
	if !strings.Contains(errOut, "failed to persist heartbeat batch") {
		t.Fatalf("control failure should be a persistence error, got stderr=%q (running as root makes permission bits ineffective)", errOut)
	}

	// The reconnect batch: seq 1 retransmitted twice (once with an equivalent
	// timezone spelling) and seq 2 once — three input occurrences, all saved.
	out, errOut, code = runCLI(t, dir, `[
	  {"node":"dup","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.26.0","height":12345,"missed":0},
	  {"node":"dup","seq":1,"collected_at":"2026-10-01T19:59:00+08:00","version":"1.26.0","height":12345,"missed":0},
	  {"node":"dup","seq":2,"collected_at":"2026-10-01T11:59:30Z","version":"1.26.0","height":12350,"missed":0}
	]`, "heartbeat", "submit", "--data-dir", dir, "--receive-time", cliReceiveAt)
	if code != 0 {
		t.Fatalf("an all-duplicate submit must succeed without rewriting: exit=%d stderr=%q", code, errOut)
	}
	if strings.TrimRight(out, "\n") != "submitted: new=0 duplicate=3" {
		t.Errorf("stdout=%q, want new=0 duplicate=3", out)
	}

	// No temp file was ever created in the read-only directory.
	assertNoTempFiles(t, nodesDir)

	// Restore permissions for the byte snapshot helper, then verify the file
	// is unchanged and history has exactly the original two rows in order.
	if err := os.Chmod(nodesDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if got := readNodeFile(t, dir, "dup"); got != before {
		t.Errorf("duplicate-only node file was rewritten:\nbefore=%q\nafter =%q", before, got)
	}
	lines := historyLines(t, dir, "dup")
	if len(lines) != 2 ||
		lines[0] != "node=dup seq=1 collected_at=2026-10-01T11:59:00Z version=1.26.0 height=12345 missed=0" ||
		lines[1] != "node=dup seq=2 collected_at=2026-10-01T11:59:30Z version=1.26.0 height=12350 missed=0" {
		t.Errorf("history gained or altered rows: %q", lines)
	}
}

// TestCLIDuplicateOnlyLongIDSubmitSucceedsWhenSlotsDirCannotRewrite is the
// same guarantee for the slots/ layout: a long id must not be treated as a
// new node, and its duplicate-only resubmit must not attempt a slot rewrite.
func TestCLIDuplicateOnlyLongIDSubmitSucceedsWhenSlotsDirCannotRewrite(t *testing.T) {
	dir := t.TempDir()
	// "long-node-" is 10 bytes; 13 repeats = 130 bytes, past the 125-byte
	// legacy-name limit, so the id uses the slots/ layout.
	longNode := strings.Repeat("long-node-", 13)
	submitBatch(t, dir, longIDJSON(longNode, 1, 12345, 0, "1.26.0", "2026-10-01T11:59:00Z"))
	slotPath := nodeFileHexPath(dir, longNode)
	beforeBytes, err := os.ReadFile(slotPath)
	if err != nil {
		t.Fatal(err)
	}
	slotsDir := filepath.Join(dir, "slots")
	readonlyDir(t, slotsDir)

	// Control: a new seq for the same long node genuinely needs a slot write.
	out, errOut, code := runCLI(t, dir, longIDJSON(longNode, 2, 12350, 0, "1.26.0", "2026-10-01T11:59:30Z"),
		"heartbeat", "submit", "--data-dir", dir, "--receive-time", cliReceiveAt)
	if code == 0 {
		t.Fatalf("control: a needed slot write must fail with slots/ read-only; stdout=%q", out)
	}
	if !strings.Contains(errOut, "failed to persist heartbeat batch") {
		t.Fatalf("control failure should be a persistence error, got stderr=%q", errOut)
	}

	// Duplicate retransmit: same record (emoji-free id here, literal spelling).
	out, errOut, code = runCLI(t, dir, longIDJSON(longNode, 1, 12345, 0, "1.26.0", "2026-10-01T11:59:00Z"),
		"heartbeat", "submit", "--data-dir", dir, "--receive-time", cliReceiveAt)
	if code != 0 {
		t.Fatalf("duplicate-only long-id submit must succeed without a rewrite: exit=%d stderr=%q", code, errOut)
	}
	if strings.TrimRight(out, "\n") != "submitted: new=0 duplicate=1" {
		t.Errorf("stdout=%q, want new=0 duplicate=1", out)
	}
	assertNoTempFiles(t, slotsDir)

	if err := os.Chmod(slotsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	afterBytes, err := os.ReadFile(slotPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(afterBytes) != string(beforeBytes) {
		t.Errorf("the long id's slot was rewritten despite an all-duplicate batch")
	}
	lines := historyLines(t, dir, longNode)
	if len(lines) != 1 || !strings.Contains(lines[0], "seq=1") || strings.Contains(lines[0], "seq=2") {
		t.Errorf("long-id history must keep only seq 1, got %q", lines)
	}
}

// TestCLIMixedBatchDuplicateOnlyNodeFaultDoesNotBlockOtherNode covers the two
// cross-layout combinations: the duplicate-only node's storage directory is
// unable to rewrite, while another node in the SAME batch reports a genuine
// new record in the other (writable) directory. The dup-only node must not be
// written and must not drag the legal submit down; counts, history and file
// bytes are all checked afterwards.
func TestCLIMixedBatchDuplicateOnlyNodeFaultDoesNotBlockOtherNode(t *testing.T) {
	// "new-long-node-" is 14 bytes; 10 repeats = 140 bytes -> slots/.
	longNew := strings.Repeat("new-long-node-", 10)

	cases := []struct {
		name     string
		setup    func(dir string)        // pre-saved history
		faultDir func(dir string) string // directory made read-only
		batch    string                  // mixed batch JSON
		dupNode  string
		newNode  string
		wantLine string
	}{
		{
			name: "duplicate short node blocked, new long node must still save",
			setup: func(dir string) {
				submitBatch(t, dir, `[{"node":"dup","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.26.0","height":12345,"missed":0}]`)
			},
			faultDir: func(dir string) string { return filepath.Join(dir, "nodes") },
			batch: `[
			  {"node":"dup","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.26.0","height":12345,"missed":0},
			  {"node":"` + longNew + `","seq":1,"collected_at":"2026-10-01T11:59:30Z","version":"1.0","height":77,"missed":0}
			]`,
			dupNode:  "dup",
			newNode:  longNew,
			wantLine: "height=77 missed=0",
		},
		{
			name: "duplicate long node blocked, new short node must still save",
			setup: func(dir string) {
				submitBatch(t, dir, longIDJSON(longDupNode(), 1, 12345, 0, "1.26.0", "2026-10-01T11:59:00Z"))
			},
			faultDir: func(dir string) string { return filepath.Join(dir, "slots") },
			batch: `[` +
				stripOuterBrackets(longIDJSON(longDupNode(), 1, 12345, 0, "1.26.0", "2026-10-01T11:59:00Z")) + `,
			  {"node":"newshort","seq":1,"collected_at":"2026-10-01T11:59:30Z","version":"1.0","height":88,"missed":0}
			]`,
			dupNode:  longDupNode(),
			newNode:  "newshort",
			wantLine: "height=88 missed=0",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			tc.setup(dir)
			dupBefore, err := os.ReadFile(nodeFileHexPath(dir, tc.dupNode))
			if err != nil {
				t.Fatal(err)
			}
			faultDir := tc.faultDir(dir)
			readonlyDir(t, faultDir)

			out, errOut, code := runCLI(t, dir, tc.batch,
				"heartbeat", "submit", "--data-dir", dir, "--receive-time", cliReceiveAt)
			if code != 0 {
				t.Fatalf("the dup-only node must not block the other node's legal submit: exit=%d stderr=%q", code, errOut)
			}
			if strings.TrimRight(out, "\n") != "submitted: new=1 duplicate=1" {
				t.Errorf("stdout=%q, want new=1 duplicate=1", out)
			}
			assertNoTempFiles(t, faultDir)

			// Restore so snapshot reads/cleanup behave normally.
			if err := os.Chmod(faultDir, 0o755); err != nil {
				t.Fatal(err)
			}
			dupAfter, err := os.ReadFile(nodeFileHexPath(dir, tc.dupNode))
			if err != nil {
				t.Fatal(err)
			}
			if string(dupAfter) != string(dupBefore) {
				t.Errorf("duplicate-only node file was rewritten by the mixed batch")
			}
			newLines := historyLines(t, dir, tc.newNode)
			if len(newLines) != 1 || !strings.Contains(newLines[0], "seq=1") ||
				!strings.Contains(newLines[0], tc.wantLine) {
				t.Errorf("the other node's new heartbeat must be queryable, got %q", newLines)
			}
			dupLines := historyLines(t, dir, tc.dupNode)
			if len(dupLines) != 1 || !strings.Contains(dupLines[0], "seq=1") {
				t.Errorf("dup-only node history must stay exactly as it was, got %q", dupLines)
			}
		})
	}
}

// longDupNode returns a node id long enough to use the slots/ layout
// ("dup-long-node-" is 14 bytes; 11 repeats = 154 bytes).
func longDupNode() string {
	return strings.Repeat("dup-long-node-", 11)
}

// stripOuterBrackets turns a single-element JSON array into its object text,
// so a caller can embed it inside a larger hand-written array.
func stripOuterBrackets(arrayJSON string) string {
	s := strings.TrimSpace(arrayJSON)
	s = strings.TrimPrefix(s, "[")
	s = strings.TrimSuffix(s, "]")
	return strings.TrimSpace(s)
}

// TestCLIDuplicateEquivalentSpellingsDoNotRewrite pins the observable
// identity rule at the command boundary: same instant in another timezone and
// node/version written with equivalent JSON \uXXXX escapes all repeat an
// already-saved record; each input occurrence counts as a duplicate, the file
// is byte-identical and the history keeps the original text and instant.
func TestCLIDuplicateEquivalentSpellingsDoNotRewrite(t *testing.T) {
	dir := t.TempDir()
	submitBatch(t, dir, `[{"node":"peer-a","seq":7,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":12345,"missed":0}]`)
	before := readNodeFile(t, dir, "peer-a")

	bs := "\\"
	batch := `[
	  {"node":"peer-a","seq":7,"collected_at":"2026-10-01T19:59:00+08:00","version":"1.0","height":12345,"missed":0},
	  {"node":"` + bs + `u0070eer-` + bs + `u0061","seq":7,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":12345,"missed":0},
	  {"node":"peer-a","seq":7,"collected_at":"2026-10-01T11:59:00Z","version":"` + bs + `u0031.` + bs + `u0030","height":12345,"missed":0}
	]`
	out, errOut, code := submit(t, dir, batch)
	if code != 0 {
		t.Fatalf("equivalent spellings must submit as duplicates: exit=%d stderr=%q", code, errOut)
	}
	if strings.TrimRight(out, "\n") != "submitted: new=0 duplicate=3" {
		t.Errorf("stdout=%q, want new=0 duplicate=3", out)
	}

	if got := readNodeFile(t, dir, "peer-a"); got != before {
		t.Errorf("the node file was rewritten with an equivalent spelling:\nbefore=%q\nafter =%q", before, got)
	}
	lines := historyLines(t, dir, "peer-a")
	if len(lines) != 1 {
		t.Fatalf("history must keep exactly one row, got %q", lines)
	}
	want := "node=peer-a seq=7 collected_at=2026-10-01T11:59:00Z version=1.0 height=12345 missed=0"
	if lines[0] != want {
		t.Errorf("original text/collected instant was rewritten:\n got %q\nwant %q", lines[0], want)
	}
}
