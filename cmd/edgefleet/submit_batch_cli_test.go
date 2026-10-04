package main

// Command-line regression tests for `edgefleet heartbeat submit` around the
// duplicate/conflict rule the batch writer already enforces:
//
//   - a batch may freely mix already-saved records, repeats inside the batch,
//     and brand-new records of other nodes — counted by what genuinely changed
//     on disk (new) vs how many input rows added nothing (duplicate);
//   - duplicate vs conflict is judged by actual telemetry content: node+seq
//     with version, height, missed and the collected instant all equal is a
//     duplicate (the same instant in another timezone offset still counts),
//     any one differing is a conflict;
//   - a conflict fails the whole batch at the command boundary: non-zero exit,
//     node and seq named on stderr, no success counts on stdout, and no record
//     of the batch left in any node's history — whether the clash is between
//     two rows of the batch or between a row and saved history.
//
// The tests drive the genuine command path (flag parsing included) via the
// same re-exec harness as main_cli_test.go: runCLI/submitBatch/readNodeFile.
// Everything runs offline against a fresh temp data dir with fixed instants,
// so counts, streams and exit codes are deterministic.

import (
	"os"
	"strings"
	"testing"
)

// submit runs the real submit command with the common fixed receive instant.
func submit(t *testing.T, dir, json string) (out, errOut string, code int) {
	t.Helper()
	return runCLI(t, dir, json,
		"heartbeat", "submit", "--data-dir", dir, "--receive-time", cliReceiveAt)
}

// history invokes the real history command for a node.
func history(t *testing.T, dir, node string) (out, errOut string, code int) {
	t.Helper()
	return runCLI(t, dir, "",
		"heartbeat", "history", "--data-dir", dir, "--node", node)
}

// historyLines runs history and requires it to exit 0 with an empty stderr,
// returning its stdout lines (nil when the node has no heartbeats).
func historyLines(t *testing.T, dir, node string) []string {
	t.Helper()
	out, errOut, code := history(t, dir, node)
	if code != 0 {
		t.Fatalf("history %s exited %d: stderr=%q", node, code, errOut)
	}
	if errOut != "" {
		t.Fatalf("history %s stderr must be empty, got %q", node, errOut)
	}
	out = strings.TrimRight(out, "\n")
	if out == "" {
		return nil
	}
	lines := strings.Split(out, "\n")
	// A node with no records prints a single "node=X no heartbeats" notice;
	// that is the empty-history state, not a saved record.
	if len(lines) == 1 && strings.HasSuffix(lines[0], " no heartbeats") {
		return nil
	}
	return lines
}

// historyLineForSeq returns the printed history line carrying seq=want.
func historyLineForSeq(t *testing.T, lines []string, want string) string {
	t.Helper()
	for _, ln := range lines {
		if strings.Contains(ln, "seq="+want+" ") {
			return ln
		}
	}
	t.Fatalf("no history line with seq=%s in %q", want, strings.Join(lines, "\n"))
	return ""
}

// historySeqs extracts the seq=N tokens in printed (ascending) order.
func historySeqs(lines []string) string {
	var seqs []string
	for _, ln := range lines {
		i := strings.Index(ln, "seq=")
		if i < 0 {
			continue
		}
		rest := ln[i+4:]
		j := 0
		for j < len(rest) && rest[j] >= '0' && rest[j] <= '9' {
			j++
		}
		seqs = append(seqs, rest[:j])
	}
	return strings.Join(seqs, ",")
}

// requireHistoryEmpty asserts a node currently has no saved heartbeats.
func requireHistoryEmpty(t *testing.T, dir, node string) {
	t.Helper()
	if lines := historyLines(t, dir, node); lines != nil {
		t.Errorf("node %s history must be empty, got %q", node, strings.Join(lines, "\n"))
	}
}

// assertRejectedBatch checks the observable contract of a refused submit:
// non-zero exit, an error naming the conflicting node and seq on stderr, and
// no success line or counts on stdout.
func assertRejectedBatch(t *testing.T, out, errOut string, code int, node, seq string) {
	t.Helper()
	if code == 0 {
		t.Errorf("conflicting submit must exit non-zero; stdout=%q stderr=%q", out, errOut)
	}
	if !strings.Contains(errOut, "error:") || !strings.Contains(errOut, "conflict") {
		t.Errorf("conflict must be reported as an error on stderr, got %q", errOut)
	}
	if !strings.Contains(errOut, `"`+node+`"`) || !strings.Contains(errOut, "seq "+seq) {
		t.Errorf("stderr must name node %q and seq %s, got %q", node, seq, errOut)
	}
	if strings.Contains(out, "submitted") || strings.Contains(out, "new=") {
		t.Errorf("rejected batch must print no success counts on stdout, got %q", out)
	}
}

// TestCLISubmitMixedNewSavedAndIntraBatchDuplicates is the spec's worked
// example: node 甲 already holds seq 1; the next batch contains the same
// 甲 seq 1 again, two identical copies of 甲 seq 2, and 乙 seq 2. The
// platform must report new=2 duplicate=2, exit 0 with an empty stderr; 甲
// keeps only seq 1 and 2, 乙 only its own seq 2, each ascending, and the
// previously saved telemetry is unchanged in the query output.
func TestCLISubmitMixedNewSavedAndIntraBatchDuplicates(t *testing.T) {
	dir := t.TempDir()

	// Precondition: 甲 already has seq 1 with distinctive telemetry.
	submitBatch(t, dir, `[{"node":"甲","seq":1,"collected_at":"2026-10-01T11:59:00+08:00","version":"1.26.0","height":12345,"missed":0}]`)

	// Batch: an exact replay of 甲 seq 1 (saved duplicate), two identical
	// 甲 seq 2 rows (one new, one intra-batch duplicate), and 乙 seq 2 (new).
	batch := `[
	  {"node":"甲","seq":1,"collected_at":"2026-10-01T11:59:00+08:00","version":"1.26.0","height":12345,"missed":0},
	  {"node":"甲","seq":2,"collected_at":"2026-10-01T11:59:30+08:00","version":"1.26.0","height":12350,"missed":0},
	  {"node":"甲","seq":2,"collected_at":"2026-10-01T11:59:30+08:00","version":"1.26.0","height":12350,"missed":0},
	  {"node":"乙","seq":2,"collected_at":"2026-10-01T11:59:30+08:00","version":"1.26.0","height":777,"missed":1}
	]`
	out, errOut, code := submit(t, dir, batch)
	if code != 0 {
		t.Fatalf("successful batch must exit 0, got %d stderr=%q", code, errOut)
	}
	if errOut != "" {
		t.Errorf("successful batch stderr must be empty, got %q", errOut)
	}
	if strings.TrimSpace(out) != "submitted: new=2 duplicate=2" {
		t.Errorf("stdout = %q, want submitted: new=2 duplicate=2", out)
	}

	// 甲: only seq 1 and 2, ascending; seq 1 telemetry is unchanged.
	jia := historyLines(t, dir, "甲")
	if got := historySeqs(jia); got != "1,2" {
		t.Errorf("甲 seqs = %s, want 1,2; out=%q", got, strings.Join(jia, "\n"))
	}
	if len(jia) != 2 {
		t.Fatalf("甲 must have exactly 2 records, got %d: %q", len(jia), strings.Join(jia, "\n"))
	}
	wantFirst := "node=甲 seq=1 collected_at=2026-10-01T11:59:00+08:00 version=1.26.0 height=12345 missed=0"
	if jia[0] != wantFirst {
		t.Errorf("甲 seq 1 original telemetry changed: %q, want %q", jia[0], wantFirst)
	}
	wantSecond := "node=甲 seq=2 collected_at=2026-10-01T11:59:30+08:00 version=1.26.0 height=12350 missed=0"
	if jia[1] != wantSecond {
		t.Errorf("甲 seq 2 = %q, want %q", jia[1], wantSecond)
	}

	// 乙: only its own seq 2; the same seq on another node is a different
	// record and must not be deduped against 甲.
	yi := historyLines(t, dir, "乙")
	if got := historySeqs(yi); got != "2" {
		t.Errorf("乙 seqs = %s, want 2; out=%q", got, strings.Join(yi, "\n"))
	}
	if len(yi) != 1 {
		t.Fatalf("乙 must have exactly 1 record, got %d: %q", len(yi), strings.Join(yi, "\n"))
	}
	wantYi := "node=乙 seq=2 collected_at=2026-10-01T11:59:30+08:00 version=1.26.0 height=777 missed=1"
	if yi[0] != wantYi {
		t.Errorf("乙 seq 2 = %q, want %q", yi[0], wantYi)
	}
}

// TestCLISubmitDuplicateAcrossTimezoneOffsets pins content equality: the
// same collected instant written with another timezone offset is a
// duplicate, not a conflict; counts say new=0 duplicate=1 and history keeps
// the single original record printed in its original offset.
func TestCLISubmitDuplicateAcrossTimezoneOffsets(t *testing.T) {
	dir := t.TempDir()

	first := `[{"node":"val-eu-1","seq":7,"collected_at":"2026-10-01T11:59:00+08:00","version":"1.26.0","height":12345,"missed":0}]`
	if _, errOut, code := submit(t, dir, first); code != 0 {
		t.Fatalf("first submit must succeed: code=%d stderr=%q", code, errOut)
	}

	// 11:59 in +08:00 == 03:59Z. Same instant, different offset: duplicate.
	rebased := `[{"node":"val-eu-1","seq":7,"collected_at":"2026-10-01T03:59:00Z","version":"1.26.0","height":12345,"missed":0}]`
	out, errOut, code := submit(t, dir, rebased)
	if code != 0 {
		t.Fatalf("same instant in another offset must be a duplicate, not conflict: code=%d stderr=%q", code, errOut)
	}
	if errOut != "" {
		t.Errorf("duplicate submit stderr must be empty, got %q", errOut)
	}
	if strings.TrimSpace(out) != "submitted: new=0 duplicate=1" {
		t.Errorf("stdout = %q, want new=0 duplicate=1", out)
	}

	lines := historyLines(t, dir, "val-eu-1")
	if len(lines) != 1 {
		t.Fatalf("exact duplicate must add zero records, got %d: %q", len(lines), strings.Join(lines, "\n"))
	}
	// The stored record keeps its original content and offset — the
	// timezone-rewritten replay neither overwrites nor conflicts.
	want := "node=val-eu-1 seq=7 collected_at=2026-10-01T11:59:00+08:00 version=1.26.0 height=12345 missed=0"
	if lines[0] != want {
		t.Errorf("history = %q, want %q", lines[0], want)
	}
}

// TestCLISubmitConflictFieldsAgainstHistory checks each telemetry field that
// distinguishes a duplicate from a clash with saved history: with node+seq
// already saved, a difference in version, height, missed or the collected
// instant must reject the whole batch — never last-wins, never counted as a
// duplicate. A legal new record for another node rides along in every batch
// and must not survive either. A later valid batch must still work.
func TestCLISubmitConflictFieldsAgainstHistory(t *testing.T) {
	dir := t.TempDir()
	submitBatch(t, dir, `[
	  {"node":"val-eu-1","seq":7,"collected_at":"2026-10-01T11:59:00+08:00","version":"1.26.0","height":12345,"missed":2}
	]`)
	origLine := func(t *testing.T) string {
		t.Helper()
		return historyLineForSeq(t, historyLines(t, dir, "val-eu-1"), "7")
	}
	orig := origLine(t)

	cases := []struct {
		name string
		row  string
	}{
		{
			name: "version differs",
			row:  `{"node":"val-eu-1","seq":7,"collected_at":"2026-10-01T11:59:00+08:00","version":"1.27.0","height":12345,"missed":2}`,
		},
		{
			name: "height differs",
			row:  `{"node":"val-eu-1","seq":7,"collected_at":"2026-10-01T11:59:00+08:00","version":"1.26.0","height":99999,"missed":2}`,
		},
		{
			name: "missed differs",
			row:  `{"node":"val-eu-1","seq":7,"collected_at":"2026-10-01T11:59:00+08:00","version":"1.26.0","height":12345,"missed":3}`,
		},
		{
			name: "collected instant differs",
			row:  `{"node":"val-eu-1","seq":7,"collected_at":"2026-10-01T11:58:00+08:00","version":"1.26.0","height":12345,"missed":2}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			batch := "[" + tc.row + `,{"node":"other","seq":1,"collected_at":"2026-10-01T11:59:00+08:00","version":"1.26.0","height":1,"missed":0}]`
			out, errOut, code := submit(t, dir, batch)
			assertRejectedBatch(t, out, errOut, code, "val-eu-1", "7")

			// Existing telemetry unchanged; the other node was never created.
			if got := historyLineForSeq(t, historyLines(t, dir, "val-eu-1"), "7"); got != orig {
				t.Errorf("existing record changed after conflict: %q, orig %q", got, orig)
			}
			requireHistoryEmpty(t, dir, "other")
		})
	}

	// After every kind of conflict, a clean batch still succeeds and the
	// earlier content remains queryable.
	out, errOut, code := submit(t, dir, `[
	  {"node":"val-eu-1","seq":8,"collected_at":"2026-10-01T11:59:40+08:00","version":"1.26.0","height":12355,"missed":2},
	  {"node":"other","seq":1,"collected_at":"2026-10-01T11:59:00+08:00","version":"1.26.0","height":1,"missed":0}
	]`)
	if code != 0 {
		t.Fatalf("valid batch after conflicts must succeed: code=%d stderr=%q", code, errOut)
	}
	if strings.TrimSpace(out) != "submitted: new=2 duplicate=0" {
		t.Errorf("stdout = %q, want new=2 duplicate=0", out)
	}
	lines := historyLines(t, dir, "val-eu-1")
	if got := historySeqs(lines); got != "7,8" {
		t.Errorf("val-eu-1 seqs = %s, want 7,8", got)
	}
	if got := historyLineForSeq(t, lines, "7"); got != orig {
		t.Errorf("original seq 7 must remain intact: %q, orig %q", got, orig)
	}
}

// TestCLISubmitConflictWithinBatch rejects when two rows in the same batch
// share node+seq but disagree on content — neither last-wins nor
// counted-as-duplicate. Valid records of other nodes riding along must not be
// left behind, including when they sort before the conflicting pair, and a
// subsequent clean batch works normally.
func TestCLISubmitConflictWithinBatch(t *testing.T) {
	dir := t.TempDir()

	expectNothingSaved := func(t *testing.T) {
		t.Helper()
		for _, node := range []string{"n1", "n2", "n3"} {
			requireHistoryEmpty(t, dir, node)
		}
	}

	// Case A: conflicting pair first, another node's valid record after it.
	out, errOut, code := submit(t, dir, `[
	  {"node":"n1","seq":3,"collected_at":"2026-10-01T11:59:00+08:00","version":"1.0","height":100,"missed":0},
	  {"node":"n1","seq":3,"collected_at":"2026-10-01T11:59:00+08:00","version":"2.0","height":100,"missed":0},
	  {"node":"n2","seq":1,"collected_at":"2026-10-01T11:59:00+08:00","version":"1.0","height":1,"missed":0}
	]`)
	assertRejectedBatch(t, out, errOut, code, "n1", "3")
	expectNothingSaved(t)

	// Case B: legal rows BEFORE the conflicting pair. Rows that passed
	// earlier in the input must not be partially persisted.
	out, errOut, code = submit(t, dir, `[
	  {"node":"n2","seq":1,"collected_at":"2026-10-01T11:59:00+08:00","version":"1.0","height":1,"missed":0},
	  {"node":"n3","seq":2,"collected_at":"2026-10-01T11:59:00+08:00","version":"1.0","height":2,"missed":0},
	  {"node":"n1","seq":3,"collected_at":"2026-10-01T11:59:00+08:00","version":"1.0","height":100,"missed":0},
	  {"node":"n1","seq":3,"collected_at":"2026-10-01T11:59:00+08:00","version":"2.0","height":100,"missed":0}
	]`)
	assertRejectedBatch(t, out, errOut, code, "n1", "3")
	expectNothingSaved(t)

	// Recovery: a non-conflicting, legal batch succeeds afterwards, and the
	// rejected nodes become queryable with exactly the accepted content.
	out, errOut, code = submit(t, dir, `[
	  {"node":"n1","seq":3,"collected_at":"2026-10-01T11:59:00+08:00","version":"1.0","height":100,"missed":0},
	  {"node":"n2","seq":1,"collected_at":"2026-10-01T11:59:00+08:00","version":"1.0","height":1,"missed":0}
	]`)
	if code != 0 {
		t.Fatalf("clean batch after conflict must succeed: code=%d stderr=%q", code, errOut)
	}
	if strings.TrimSpace(out) != "submitted: new=2 duplicate=0" {
		t.Errorf("stdout = %q, want new=2 duplicate=0", out)
	}
	for _, node := range []string{"n1", "n2"} {
		if lines := historyLines(t, dir, node); len(lines) != 1 {
			t.Errorf("node %s must have exactly 1 record after recovery, got %q", node, strings.Join(lines, "\n"))
		}
	}
	requireHistoryEmpty(t, dir, "n3")
}

// TestCLISubmitConflictAgainstHistoryIsAtomic covers the second conflict
// condition from the spec: the batch vs saved history. A batch containing
// legal new records (including for a node with no prior file) and one record
// clashing with saved history must be rejected as a whole: exit non-zero,
// conflict named on stderr, no counts on stdout, existing nodes byte
// identical, the new-node file absent. A follow-up submit then works.
func TestCLISubmitConflictAgainstHistoryIsAtomic(t *testing.T) {
	dir := t.TempDir()

	submitBatch(t, dir, `[{"node":"old","seq":1,"collected_at":"2026-10-01T11:59:00+08:00","version":"1.26.0","height":12345,"missed":0}]`)
	oldBefore := readNodeFile(t, dir, "old")

	batch := `[
	  {"node":"old","seq":2,"collected_at":"2026-10-01T11:59:30+08:00","version":"1.26.0","height":12350,"missed":0},
	  {"node":"old","seq":1,"collected_at":"2026-10-01T11:59:00+08:00","version":"1.27.0","height":12345,"missed":0},
	  {"node":"fresh","seq":1,"collected_at":"2026-10-01T11:59:00+08:00","version":"1.26.0","height":1,"missed":0}
	]`
	out, errOut, code := submit(t, dir, batch)
	assertRejectedBatch(t, out, errOut, code, "old", "1")

	// Existing node file untouched: seq 2 rode the same rejected batch and
	// must not appear; the brand-new node file was never created.
	if got := readNodeFile(t, dir, "old"); got != oldBefore {
		t.Errorf("existing node file changed during rejected submit")
	}
	if lines := historyLines(t, dir, "old"); len(lines) != 1 || historySeqs(lines) != "1" {
		t.Errorf("old must keep only seq 1, got %q", strings.Join(lines, "\n"))
	}
	requireHistoryEmpty(t, dir, "fresh")
	if _, err := os.Stat(nodeFileHexPath(dir, "fresh")); !os.IsNotExist(err) {
		t.Errorf("brand-new node file must not be created, stat err=%v", err)
	}

	// Follow-up non-conflicting batch, including the previously rejected
	// brand-new node, succeeds and becomes queryable.
	out, errOut, code = submit(t, dir, `[
	  {"node":"old","seq":2,"collected_at":"2026-10-01T11:59:30+08:00","version":"1.26.0","height":12350,"missed":0},
	  {"node":"fresh","seq":1,"collected_at":"2026-10-01T11:59:00+08:00","version":"1.26.0","height":1,"missed":0}
	]`)
	if code != 0 {
		t.Fatalf("clean batch after conflict must succeed: code=%d stderr=%q", code, errOut)
	}
	if strings.TrimSpace(out) != "submitted: new=2 duplicate=0" {
		t.Errorf("stdout = %q, want new=2 duplicate=0", out)
	}
	if lines := historyLines(t, dir, "old"); historySeqs(lines) != "1,2" {
		t.Errorf("old seqs = %s, want 1,2", historySeqs(lines))
	}
	if lines := historyLines(t, dir, "fresh"); len(lines) != 1 || historySeqs(lines) != "1" {
		t.Errorf("fresh must have seq 1, got %q", strings.Join(lines, "\n"))
	}
}
