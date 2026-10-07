package main

// End-to-end command-line tests for the final stage of a heartbeat save.
//
// After a node file's bytes are fsynced and atomically renamed into place, the
// command must open the file's directory and fsync it to CONFIRM the save. If
// either directory operation fails, the submit is a save failure: the command
// exits non-zero, prints no success line or counts on stdout, and tells the
// user on stderr that the failure happened during save confirmation, naming
// the directory and the concrete reason. The complete renamed file is still
// kept (never deleted or rolled back); the same JSON resubmitted afterwards
// converges via duplicates.
//
// From the command boundary the store's fault-injection seam is unreachable,
// so these tests create the directory-open failure for real, offline and
// without root: chmod the target directory to 0300 (write+execute, no read).
// Creating the temp file and renaming it need only write+execute, so the
// replacement really lands; opening the directory then fails with EACCES —
// the exact post-rename moment the old code ignored. Files inside remain
// readable by exact name, so history after the failure proves the complete
// records were retained. (The directory-fsync failure mode is covered with an
// injected fault in the edgefleet package tests, since no plain permission
// bit makes an fsync fail.)

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// unopenableDir chmods dir to 0300: entries can still be created, renamed and
// opened by exact name, but opening the directory (the save-confirmation
// step) is denied. Normal permissions are restored at cleanup.
func unopenableDir(t *testing.T, dir string) {
	t.Helper()
	if err := os.Chmod(dir, 0o300); err != nil {
		t.Fatalf("chmod %s 0300: %v", dir, err)
	}
	t.Cleanup(func() {
		if err := os.Chmod(dir, 0o755); err != nil {
			t.Errorf("restore %s permissions: %v", dir, err)
		}
	})
}

// assertSaveConfirmFailure locks the public failure contract for a save that
// reached the directory-confirmation stage: non-zero exit, empty stdout, and a
// stderr that locates the failure at the save stage, names dir and gives a
// concrete reason — without calling it a content conflict or data corruption.
func assertSaveConfirmFailure(t *testing.T, out, errOut string, code int, dir string) {
	t.Helper()
	if code == 0 {
		t.Errorf("submit must exit non-zero when the directory cannot be confirmed; stdout=%q", out)
	}
	if out != "" {
		t.Errorf("a failed save must print no success line or counts on stdout, got %q", out)
	}
	if strings.Contains(out, "submitted") || strings.Contains(out, "new=") {
		t.Errorf("stdout must not look like a committed submission: %q", out)
	}
	for _, want := range []string{
		"error:",
		"failed to persist heartbeat batch",
		"save confirmation failed",
		dir,
		"cannot open directory",
		"permission denied",
	} {
		if !strings.Contains(errOut, want) {
			t.Errorf("stderr must contain %q to explain the save-stage failure, got %q", want, errOut)
		}
	}
	for _, banned := range []string{"conflict", "corrupt"} {
		if strings.Contains(strings.ToLower(errOut), banned) {
			t.Errorf("a save-confirmation failure must not be called %q: %q", banned, errOut)
		}
	}
}

// TestCLISubmitDirectoryOpenFailureReportsSaveError is the short-id (nodes/)
// case through the real command: seq 2 is renamed over the node file, then
// opening nodes/ to confirm fails. The command fails as a save error with no
// stdout; history afterwards still shows both complete records, and a verbatim
// resubmit converges.
func TestCLISubmitDirectoryOpenFailureReportsSaveError(t *testing.T) {
	dir := t.TempDir()
	submitBatch(t, dir, `[
	  {"node":"alpha","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":100,"missed":0}
	]`)

	nodesDir := filepath.Join(dir, "nodes")
	unopenableDir(t, nodesDir)

	seq2 := `[{"node":"alpha","seq":2,"collected_at":"2026-10-01T11:59:30Z","version":"1.0","height":101,"missed":0}]`
	out, errOut, code := runCLI(t, dir, seq2,
		"heartbeat", "submit", "--data-dir", dir, "--receive-time", cliReceiveAt)
	assertSaveConfirmFailure(t, out, errOut, code, nodesDir)

	// Clear the fault; the replacement that already landed must be the
	// complete two-record file, and no half-written temp file may remain.
	if err := os.Chmod(nodesDir, 0o755); err != nil {
		t.Fatal(err)
	}
	assertNoTempFiles(t, nodesDir)
	lines := historyLines(t, dir, "alpha")
	if len(lines) != 2 ||
		lines[0] != "node=alpha seq=1 collected_at=2026-10-01T11:59:00Z version=1.0 height=100 missed=0" ||
		lines[1] != "node=alpha seq=2 collected_at=2026-10-01T11:59:30Z version=1.0 height=101 missed=0" {
		t.Errorf("the renamed file must keep both records complete and in order, got %q", lines)
	}

	// Verbatim resubmit: the landed seq 2 is now a duplicate, counted once.
	out, errOut, code = runCLI(t, dir, seq2,
		"heartbeat", "submit", "--data-dir", dir, "--receive-time", cliReceiveAt)
	if code != 0 {
		t.Fatalf("verbatim resubmit after the fault clears must succeed: exit=%d stderr=%q", code, errOut)
	}
	if strings.TrimRight(out, "\n") != "submitted: new=0 duplicate=1" {
		t.Errorf("resubmit stdout=%q, want new=0 duplicate=1", out)
	}
}

// TestCLISubmitDirectoryOpenFailureTwoNodePartialSave follows the product
// example through the real command: alpha and beta each have seq 1, the batch
// adds seq 2 to both, and confirming nodes/ fails after one node file is
// already replaced. The batch is reported as failed with no counts; exactly
// one node's two complete records are queryable and the other keeps only seq
// 1 (write order is not promised, so the landed node is identified from the
// data). The verbatim batch then converges to new=1 duplicate=1.
func TestCLISubmitDirectoryOpenFailureTwoNodePartialSave(t *testing.T) {
	dir := t.TempDir()
	submitBatch(t, dir, `[
	  {"node":"alpha","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":100,"missed":0},
	  {"node":"beta","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":200,"missed":0}
	]`)

	batch := `[
	  {"node":"alpha","seq":2,"collected_at":"2026-10-01T11:59:30Z","version":"1.0","height":101,"missed":0},
	  {"node":"beta","seq":2,"collected_at":"2026-10-01T11:59:30Z","version":"1.0","height":201,"missed":1}
	]`
	nodesDir := filepath.Join(dir, "nodes")
	unopenableDir(t, nodesDir)

	out, errOut, code := runCLI(t, dir, batch,
		"heartbeat", "submit", "--data-dir", dir, "--receive-time", cliReceiveAt)
	assertSaveConfirmFailure(t, out, errOut, code, nodesDir)

	if err := os.Chmod(nodesDir, 0o755); err != nil {
		t.Fatal(err)
	}
	assertNoTempFiles(t, nodesDir)

	// One node landed seq 2, the other was not written. Classify by data.
	alpha := historyLines(t, dir, "alpha")
	beta := historyLines(t, dir, "beta")
	var landed, pending string
	switch {
	case len(alpha) == 2 && len(beta) == 1:
		landed, pending = "alpha", "beta"
	case len(alpha) == 1 && len(beta) == 2:
		landed, pending = "beta", "alpha"
	default:
		t.Fatalf("one node must have 2 records and the other 1, got alpha=%q beta=%q", alpha, beta)
	}
	t.Logf("node %q was replaced before the confirmation failure; %q was not", landed, pending)

	wantAlpha1 := "node=alpha seq=1 collected_at=2026-10-01T11:59:00Z version=1.0 height=100 missed=0"
	wantAlpha2 := "node=alpha seq=2 collected_at=2026-10-01T11:59:30Z version=1.0 height=101 missed=0"
	wantBeta1 := "node=beta seq=1 collected_at=2026-10-01T11:59:00Z version=1.0 height=200 missed=0"
	wantBeta2 := "node=beta seq=2 collected_at=2026-10-01T11:59:30Z version=1.0 height=201 missed=1"
	if landed == "alpha" {
		if alpha[0] != wantAlpha1 || alpha[1] != wantAlpha2 {
			t.Errorf("landed alpha records must be complete, got %q", alpha)
		}
		if beta[0] != wantBeta1 {
			t.Errorf("pending beta must keep its original seq 1, got %q", beta)
		}
	} else {
		if beta[0] != wantBeta1 || beta[1] != wantBeta2 {
			t.Errorf("landed beta records must be complete, got %q", beta)
		}
		if alpha[0] != wantAlpha1 {
			t.Errorf("pending alpha must keep its original seq 1, got %q", alpha)
		}
	}

	// Verbatim resubmit converges regardless of which node landed first.
	out, errOut, code = runCLI(t, dir, batch,
		"heartbeat", "submit", "--data-dir", dir, "--receive-time", cliReceiveAt)
	if code != 0 {
		t.Fatalf("verbatim resubmit must converge: exit=%d stderr=%q", code, errOut)
	}
	if strings.TrimRight(out, "\n") != "submitted: new=1 duplicate=1" {
		t.Errorf("resubmit stdout=%q, want new=1 duplicate=1", out)
	}
	alpha = historyLines(t, dir, "alpha")
	beta = historyLines(t, dir, "beta")
	if len(alpha) != 2 || alpha[0] != wantAlpha1 || alpha[1] != wantAlpha2 ||
		len(beta) != 2 || beta[0] != wantBeta1 || beta[1] != wantBeta2 {
		t.Errorf("both nodes must end with exactly seq 1 and 2 intact, got alpha=%q beta=%q", alpha, beta)
	}
}

// TestCLISubmitDirectoryOpenFailureLongIDSlotsLayout checks the long-id
// (slots/) layout gives the same result as nodes/: the failure names the slots
// directory, stdout stays empty, the replaced slot's complete records remain,
// and the verbatim resubmit counts the landed record as a duplicate.
func TestCLISubmitDirectoryOpenFailureLongIDSlotsLayout(t *testing.T) {
	dir := t.TempDir()
	// 126 ASCII bytes -> hex name is 252 chars + ".json" = 257 > 255, so the
	// id uses slots/.
	longNode := strings.Repeat("a", 126)
	submitBatch(t, dir, longIDJSON(longNode, 1, 100, 0, "1.0", "2026-10-01T11:59:00Z"))

	slotsDir := filepath.Join(dir, "slots")
	unopenableDir(t, slotsDir)

	seq2 := longIDJSON(longNode, 2, 101, 0, "1.0", "2026-10-01T11:59:30Z")
	out, errOut, code := runCLI(t, dir, seq2,
		"heartbeat", "submit", "--data-dir", dir, "--receive-time", cliReceiveAt)
	assertSaveConfirmFailure(t, out, errOut, code, slotsDir)

	if err := os.Chmod(slotsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	assertNoTempFiles(t, slotsDir)
	lines := historyLines(t, dir, longNode)
	if len(lines) != 2 || !strings.Contains(lines[0], "seq=1") || !strings.Contains(lines[1], "seq=2") {
		t.Errorf("the replaced slot must keep both complete records, got %q", lines)
	}

	out, errOut, code = runCLI(t, dir, seq2,
		"heartbeat", "submit", "--data-dir", dir, "--receive-time", cliReceiveAt)
	if code != 0 {
		t.Fatalf("verbatim resubmit must converge: exit=%d stderr=%q", code, errOut)
	}
	if strings.TrimRight(out, "\n") != "submitted: new=0 duplicate=1" {
		t.Errorf("resubmit stdout=%q, want new=0 duplicate=1", out)
	}
}

// TestCLISubmitAllDuplicateStillSucceedsWhenDirectoryUnopenable keeps the fix
// scoped to saving NEW records: a batch made entirely of already-saved
// duplicates does not rewrite the node file, so it must not attempt the
// directory confirmation and must still succeed while nodes/ cannot be
// opened. Counts and history are unchanged.
func TestCLISubmitAllDuplicateStillSucceedsWhenDirectoryUnopenable(t *testing.T) {
	dir := t.TempDir()
	submitBatch(t, dir, `[
	  {"node":"dup","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":100,"missed":0}
	]`)
	before := readNodeFile(t, dir, "dup")
	nodesDir := filepath.Join(dir, "nodes")
	unopenableDir(t, nodesDir)

	out, errOut, code := runCLI(t, dir, `[
	  {"node":"dup","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":100,"missed":0},
	  {"node":"dup","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":100,"missed":0}
	]`, "heartbeat", "submit", "--data-dir", dir, "--receive-time", cliReceiveAt)
	if code != 0 {
		t.Fatalf("an all-duplicate batch must succeed without a save: exit=%d stderr=%q", code, errOut)
	}
	if strings.TrimRight(out, "\n") != "submitted: new=0 duplicate=2" {
		t.Errorf("stdout=%q, want new=0 duplicate=2", out)
	}

	if err := os.Chmod(nodesDir, 0o755); err != nil {
		t.Fatal(err)
	}
	assertNoTempFiles(t, nodesDir)
	if got := readNodeFile(t, dir, "dup"); got != before {
		t.Errorf("duplicate-only node file was rewritten:\nbefore=%q\nafter =%q", before, got)
	}
	lines := historyLines(t, dir, "dup")
	if len(lines) != 1 || !strings.Contains(lines[0], "seq=1") {
		t.Errorf("history must keep only the original record, got %q", lines)
	}
}

// TestCLISubmitPreSaveRejectionsStillRejectBeforeSave confirms the new
// save-stage error does not absorb the pre-save rejections: with nodes/
// unopenable, an illegal record and a same-node/same-seq content conflict are
// still refused as before (invalid input / conflict on stderr), leave no new
// record, and are never reported as a save-confirmation failure.
func TestCLISubmitPreSaveRejectionsStillRejectBeforeSave(t *testing.T) {
	dir := t.TempDir()
	submitBatch(t, dir, `[
	  {"node":"alpha","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":100,"missed":0}
	]`)
	nodesDir := filepath.Join(dir, "nodes")
	unopenableDir(t, nodesDir)

	// Illegal field (negative height) paired with an otherwise-legal new node.
	out, errOut, code := runCLI(t, dir, `[
	  {"node":"alpha","seq":2,"collected_at":"2026-10-01T11:59:30Z","version":"1.0","height":-1,"missed":0},
	  {"node":"fresh","seq":1,"collected_at":"2026-10-01T11:59:30Z","version":"1.0","height":1,"missed":0}
	]`, "heartbeat", "submit", "--data-dir", dir, "--receive-time", cliReceiveAt)
	if code == 0 {
		t.Errorf("illegal input must fail non-zero; stdout=%q", out)
	}
	if out != "" {
		t.Errorf("rejected input must print nothing on stdout, got %q", out)
	}
	if !strings.Contains(errOut, "invalid input") || !strings.Contains(errOut, "height") {
		t.Errorf("stderr must reject the illegal field, got %q", errOut)
	}
	if strings.Contains(errOut, "save confirmation") {
		t.Errorf("pre-save validation failure must not be reported as a save-stage failure: %q", errOut)
	}

	// Same node+seq with different content: a conflict, not a save error.
	out, errOut, code = runCLI(t, dir, `[
	  {"node":"alpha","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"2.0","height":100,"missed":0}
	]`, "heartbeat", "submit", "--data-dir", dir, "--receive-time", cliReceiveAt)
	if code == 0 {
		t.Errorf("a content conflict must fail non-zero; stdout=%q", out)
	}
	if out != "" {
		t.Errorf("a conflict must print nothing on stdout, got %q", out)
	}
	if !strings.Contains(errOut, "conflict") || !strings.Contains(errOut, "alpha") || !strings.Contains(errOut, "seq 1") {
		t.Errorf("stderr must identify the conflict, got %q", errOut)
	}
	if strings.Contains(errOut, "save confirmation") {
		t.Errorf("a conflict must not be reported as a save-stage failure: %q", errOut)
	}

	if err := os.Chmod(nodesDir, 0o755); err != nil {
		t.Fatal(err)
	}
	assertNoTempFiles(t, nodesDir)
	// The legal companion was not saved and alpha keeps only its seq 1.
	if _, err := os.Stat(nodeFileHexPath(dir, "fresh")); !os.IsNotExist(err) {
		t.Errorf("the legal companion record must not be saved, stat err=%v", err)
	}
	alpha := historyLines(t, dir, "alpha")
	if len(alpha) != 1 || !strings.Contains(alpha[0], "version=1.0") {
		t.Errorf("alpha must keep its original seq 1, got %q", alpha)
	}
}
