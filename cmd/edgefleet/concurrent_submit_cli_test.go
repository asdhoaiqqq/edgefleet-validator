package main

// Command-line regression tests for two independent `edgefleet heartbeat
// submit` processes writing to the same data directory at the same time —
// the reconnect scenario where the same cached heartbeat can be delivered by
// more than one submitting process. The store serialises writers with the
// directory lock; these tests pin the user-visible contract of that
// serialisation through the real command entry point:
//
//   - two processes submitting the identical record for a not-yet-saved
//     node+seq both succeed; exactly one new record and one duplicate are
//     counted across the two outputs, the history holds the record once with
//     the submitted content, and neither process prints an error — including
//     when the two submissions spell the same collection instant in different
//     timezones;
//   - two processes submitting the same node+seq with different versions
//     produce exactly one winner: the loser exits non-zero with a conflict
//     error naming the node and seq, prints no success counts, overwrites
//     nothing, and its whole batch is rejected — a legal second record for
//     another seq in the losing batch is not saved either;
//   - two processes submitting different seqs for the same node both succeed
//     and the history keeps both records ascending by seq: concurrent
//     submissions never lose each other's new content.
//
// Every assertion is derived from the actual per-request output and exit
// status — which request wins a race is not prescribed. All collection and
// receive instants are fixed, so no judgement depends on the wall clock.

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// cliResult captures one finished command invocation.
type cliResult struct {
	out, errOut string
	code        int
}

// execCLI runs the re-executed binary like runCLI but reports a launch
// failure as an error instead of calling t.Fatalf, so it is safe to invoke
// from a goroutine.
func execCLI(dir, stdin string, args ...string) (cliResult, error) {
	cmd := exec.Command(os.Args[0], args...)
	cmd.Stdin = strings.NewReader(stdin)
	// Same isolation as runCLI: HOME and the env data dir stay in the temp area.
	cmd.Env = append(os.Environ(),
		cliTestEnv+"=1",
		"HOME="+dir,
		"EDGEFLEET_DATA_DIR="+filepath.Join(dir, "env-data"),
	)
	var out, errOut strings.Builder
	cmd.Stdout = &out
	cmd.Stderr = &errOut
	err := cmd.Run()
	code := 0
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			code = exitErr.ExitCode()
		} else {
			return cliResult{}, err
		}
	}
	return cliResult{out.String(), errOut.String(), code}, nil
}

// submitConcurrently runs two submit commands against the same data directory
// at the same time (both with the shared fixed receive time) and returns both
// results once both processes have finished.
func submitConcurrently(t *testing.T, dir, stdinA, stdinB string) (cliResult, cliResult) {
	t.Helper()
	args := []string{"heartbeat", "submit", "--data-dir", dir, "--receive-time", cliReceiveAt}
	var results [2]cliResult
	var errs [2]error
	var wg sync.WaitGroup
	for i, stdin := range []string{stdinA, stdinB} {
		wg.Add(1)
		go func(i int, stdin string) {
			defer wg.Done()
			results[i], errs[i] = execCLI(dir, stdin, args...)
		}(i, stdin)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("failed to run submit process %d: %v", i+1, err)
		}
	}
	return results[0], results[1]
}

// parseSubmitCounts extracts the new/duplicate counts from a successful
// submit's stdout, failing the test if the line has any other shape.
func parseSubmitCounts(t *testing.T, out string) (newCount, dupCount int) {
	t.Helper()
	n, err := fmt.Sscanf(strings.TrimRight(out, "\n"), "submitted: new=%d duplicate=%d", &newCount, &dupCount)
	if err != nil || n != 2 {
		t.Fatalf("submit stdout %q is not a counts line (parsed %d fields: %v)", out, n, err)
	}
	return newCount, dupCount
}

// TestCLIConcurrentIdenticalSubmitsDeduplicate covers the reconnect replay:
// two processes deliver the very same cached heartbeat for a node+seq that is
// not saved yet. Both requests must succeed, the two outputs together must
// account for exactly one new record and one duplicate, and the history must
// hold the record exactly once with the submitted content.
func TestCLIConcurrentIdenticalSubmitsDeduplicate(t *testing.T) {
	dir := t.TempDir()
	const record = `{"node":"race-dup","seq":7,"collected_at":"2026-10-01T11:59:00Z","version":"1.26.0","height":12345,"missed":3}`

	a, b := submitConcurrently(t, dir, "["+record+"]", "["+record+"]")

	for i, r := range []cliResult{a, b} {
		if r.code != 0 {
			t.Errorf("request %d must succeed, exit=%d stderr=%q", i+1, r.code, r.errOut)
		}
		if r.errOut != "" {
			t.Errorf("a duplicate replay must not produce error output, request %d stderr=%q", i+1, r.errOut)
		}
	}
	newA, dupA := parseSubmitCounts(t, a.out)
	newB, dupB := parseSubmitCounts(t, b.out)
	if newA+newB != 1 || dupA+dupB != 1 {
		t.Errorf("across both requests want exactly one new and one duplicate, got new=%d+%d duplicate=%d+%d",
			newA, newB, dupA, dupB)
	}

	lines := historyLines(t, dir, "race-dup")
	want := "node=race-dup seq=7 collected_at=2026-10-01T11:59:00Z version=1.26.0 height=12345 missed=3"
	if len(lines) != 1 || lines[0] != want {
		t.Errorf("history = %q, want exactly the one submitted record %q", lines, want)
	}
}

// TestCLIConcurrentSameInstantDifferentOffsetsDeduplicate is the same race
// with the collection instant spelled in two different timezones. The
// instant, not the string, identifies the record: both requests succeed, the
// combined counts are one new and one duplicate, and no conflict is
// manufactured from the differing time text.
func TestCLIConcurrentSameInstantDifferentOffsetsDeduplicate(t *testing.T) {
	dir := t.TempDir()
	// 11:59:00Z and 19:59:00+08:00 are the same instant.
	const recordUTC = `{"node":"race-tz","seq":4,"collected_at":"2026-10-01T11:59:00Z","version":"1.26.0","height":400,"missed":1}`
	const recordPlus8 = `{"node":"race-tz","seq":4,"collected_at":"2026-10-01T19:59:00+08:00","version":"1.26.0","height":400,"missed":1}`

	a, b := submitConcurrently(t, dir, "["+recordUTC+"]", "["+recordPlus8+"]")

	for i, r := range []cliResult{a, b} {
		if r.code != 0 {
			t.Errorf("request %d must succeed, exit=%d stderr=%q", i+1, r.code, r.errOut)
		}
		if r.errOut != "" {
			t.Errorf("a timezone re-spelling must not produce error output, request %d stderr=%q", i+1, r.errOut)
		}
	}
	newA, dupA := parseSubmitCounts(t, a.out)
	newB, dupB := parseSubmitCounts(t, b.out)
	if newA+newB != 1 || dupA+dupB != 1 {
		t.Errorf("across both requests want exactly one new and one duplicate, got new=%d+%d duplicate=%d+%d",
			newA, newB, dupA, dupB)
	}

	lines := historyLines(t, dir, "race-tz")
	if len(lines) != 1 {
		t.Fatalf("history = %q, want exactly one record — the re-spelled instant must not add a second", lines)
	}
	// The stored record is whichever request landed first, so the displayed
	// collection time keeps that request's spelling; every other field is fixed.
	line := lines[0]
	for _, want := range []string{"node=race-tz", "seq=4", "version=1.26.0", "height=400", "missed=1"} {
		if !strings.Contains(line, want) {
			t.Errorf("history line %q missing %q", line, want)
		}
	}
	if !strings.Contains(line, "collected_at=2026-10-01T11:59:00Z") &&
		!strings.Contains(line, "collected_at=2026-10-01T19:59:00+08:00") {
		t.Errorf("history line %q must show the submitted instant in one of its two spellings", line)
	}
}

// TestCLIConcurrentConflictingVersionsExactlyOneWins submits the same
// node+seq with two different versions from two processes at once. Exactly
// one request may win: the loser exits non-zero with a conflict error naming
// the node and seq, prints no success or counts, and leaves nothing behind —
// including the legal record for another seq of the same node that its batch
// also carried. The saved history corresponds completely to the winning
// request.
func TestCLIConcurrentConflictingVersionsExactlyOneWins(t *testing.T) {
	dir := t.TempDir()
	// Each batch pairs the contested seq 5 (different versions) with a legal
	// record for a seq unique to that request, so the loser's batch rejection
	// is observable in the history.
	batchA := `[
	  {"node":"race-conflict","seq":5,"collected_at":"2026-10-01T11:59:00Z","version":"1.26.0","height":500,"missed":1},
	  {"node":"race-conflict","seq":9,"collected_at":"2026-10-01T11:59:10Z","version":"1.26.0","height":900,"missed":2}
	]`
	batchB := `[
	  {"node":"race-conflict","seq":5,"collected_at":"2026-10-01T11:59:00Z","version":"1.27.0","height":500,"missed":1},
	  {"node":"race-conflict","seq":11,"collected_at":"2026-10-01T11:59:20Z","version":"1.27.0","height":1100,"missed":3}
	]`

	a, b := submitConcurrently(t, dir, batchA, batchB)

	// Exactly one request succeeds; which one is not prescribed.
	var winner, loser cliResult
	var wonByA bool
	switch {
	case a.code == 0 && b.code != 0:
		winner, loser, wonByA = a, b, true
	case b.code == 0 && a.code != 0:
		winner, loser, wonByA = b, a, false
	default:
		t.Fatalf("exactly one request must succeed: A exit=%d (stderr=%q), B exit=%d (stderr=%q)",
			a.code, a.errOut, b.code, b.errOut)
	}

	// The winner saved its whole two-record batch and reported it cleanly.
	if winner.errOut != "" {
		t.Errorf("winning request must leave stderr empty, got %q", winner.errOut)
	}
	if newCount, dupCount := parseSubmitCounts(t, winner.out); newCount != 2 || dupCount != 0 {
		t.Errorf("winning request counts = new=%d duplicate=%d, want new=2 duplicate=0", newCount, dupCount)
	}

	// The loser reports the conflict on stderr, naming the node and the seq,
	// and prints no success line or counters.
	if !strings.Contains(loser.errOut, "error:") || !strings.Contains(loser.errOut, "conflict") ||
		!strings.Contains(loser.errOut, "race-conflict") || !strings.Contains(loser.errOut, "seq 5") {
		t.Errorf("losing request stderr must name the conflicting node and seq, got %q", loser.errOut)
	}
	if strings.Contains(loser.out, "submitted") || strings.Contains(loser.out, "new=") {
		t.Errorf("losing request must not report success or counts, stdout=%q", loser.out)
	}

	// The history corresponds completely to the winning request: the contested
	// seq 5 holds the winner's version, the winner's extra seq is present, and
	// the loser's extra seq never landed.
	lines := historyLines(t, dir, "race-conflict")
	if len(lines) != 2 {
		t.Fatalf("history = %q, want exactly the winning batch's two records", lines)
	}
	var wantSeq5, wantExtra, loserExtraSeq string
	if wonByA {
		wantSeq5 = "node=race-conflict seq=5 collected_at=2026-10-01T11:59:00Z version=1.26.0 height=500 missed=1"
		wantExtra = "node=race-conflict seq=9 collected_at=2026-10-01T11:59:10Z version=1.26.0 height=900 missed=2"
		loserExtraSeq = "seq=11"
	} else {
		wantSeq5 = "node=race-conflict seq=5 collected_at=2026-10-01T11:59:00Z version=1.27.0 height=500 missed=1"
		wantExtra = "node=race-conflict seq=11 collected_at=2026-10-01T11:59:20Z version=1.27.0 height=1100 missed=3"
		loserExtraSeq = "seq=9"
	}
	if lines[0] != wantSeq5 {
		t.Errorf("seq-5 history line = %q, want the winning request's record %q", lines[0], wantSeq5)
	}
	if lines[1] != wantExtra {
		t.Errorf("second history line = %q, want the winning request's extra record %q", lines[1], wantExtra)
	}
	for _, line := range lines {
		if strings.Contains(line, loserExtraSeq) {
			t.Errorf("losing request's extra record %s must not be saved, history=%q", loserExtraSeq, lines)
		}
	}
}

// TestCLIConcurrentDifferentSeqsBothSaved submits different seqs of the same
// node from two processes at once. Both requests succeed, and the history
// keeps both records ascending by seq: concurrent submissions never lose each
// other's new content.
func TestCLIConcurrentDifferentSeqsBothSaved(t *testing.T) {
	dir := t.TempDir()
	const recordA = `{"node":"race-parallel","seq":3,"collected_at":"2026-10-01T11:59:00Z","version":"1.26.0","height":300,"missed":0}`
	const recordB = `{"node":"race-parallel","seq":5,"collected_at":"2026-10-01T11:59:30Z","version":"1.26.0","height":500,"missed":1}`

	a, b := submitConcurrently(t, dir, "["+recordA+"]", "["+recordB+"]")

	for i, r := range []cliResult{a, b} {
		if r.code != 0 {
			t.Errorf("request %d must succeed, exit=%d stderr=%q", i+1, r.code, r.errOut)
		}
		if r.errOut != "" {
			t.Errorf("request %d must leave stderr empty, got %q", i+1, r.errOut)
		}
		if newCount, dupCount := parseSubmitCounts(t, r.out); newCount != 1 || dupCount != 0 {
			t.Errorf("request %d counts = new=%d duplicate=%d, want new=1 duplicate=0", i+1, newCount, dupCount)
		}
	}

	lines := historyLines(t, dir, "race-parallel")
	want := []string{
		"node=race-parallel seq=3 collected_at=2026-10-01T11:59:00Z version=1.26.0 height=300 missed=0",
		"node=race-parallel seq=5 collected_at=2026-10-01T11:59:30Z version=1.26.0 height=500 missed=1",
	}
	if len(lines) != len(want) {
		t.Fatalf("history = %q, want both records %q", lines, want)
	}
	for i := range want {
		if lines[i] != want[i] {
			t.Errorf("history line %d = %q, want %q", i+1, lines[i], want[i])
		}
	}
}
