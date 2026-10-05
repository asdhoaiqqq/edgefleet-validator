package main

// Cross-process regression tests for concurrent `heartbeat submit`.
//
// The in-package TestConcurrentSubmit* tests exercise many Store instances in
// one process (many goroutines, one address space). These tests cover the
// operational situation described by the product: after a node reconnects,
// the same cached heartbeat may be delivered by two independent submitting
// processes at once. Every request here is therefore a separate OS process
// running the genuine command entry point through TestMain — flag parsing,
// stdin decoding, the real cross-process flock, stdout/stderr placement and
// the process exit status are all part of what is verified.
//
// The two processes are released together from a stdin start gate, so they
// genuinely contend for the same data directory lock; but every assertion
// below holds for ANY lock-acquisition order and classifies results by the
// requests' actual output and exit status, so the tests never assume which
// request "wins" and cannot flake on scheduling. All collection and receive
// instants are fixed; no judgement depends on the day or time the tests run.

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	concNode = "val-eu-1"

	// Fixed business instants. concAt7UTC and concAt7CN name the SAME instant
	// in different timezones; concAt8UTC is a later heartbeat of the same node.
	concAt7UTC = "2026-10-01T11:59:00Z"
	concAt7CN  = "2026-10-01T19:59:00+08:00"
	concAt8UTC = "2026-10-01T11:59:30Z"

	concV1 = "1.26.0"
	concV2 = "1.27.0"

	concH7 = 12345
	concH8 = 12346

	concMissed7 = 3
	concMissed8 = 4

	// concRounds repeats each race against a fresh data directory so both
	// lock-acquisition orders are exercised in practice across a run.
	concRounds = 8
)

// concResult is everything observable about one submitting process.
type concResult struct {
	out    string // stdout, trailing newline trimmed
	errOut string // stderr, trailing newline trimmed
	code   int    // process exit status
}

// concRecordJSON builds one strict heartbeat object from fixed-value parts.
// Every value is plain ASCII-safe text, so Go quoting and JSON quoting agree.
func concRecordJSON(node string, seq int, collected, version string, height, missed int) string {
	return fmt.Sprintf(
		`{"node":%q,"seq":%d,"collected_at":%q,"version":%q,"height":%d,"missed":%d}`,
		node, seq, collected, version, height, missed)
}

// concHistoryLine is the exact public history line for one of the fixed
// records: node=%s seq=%d collected_at=%s version=%s height=%d missed=%d.
func concHistoryLine(seq int, collected, version string, height, missed int) string {
	return fmt.Sprintf("node=%s seq=%d collected_at=%s version=%s height=%d missed=%d",
		concNode, seq, collected, version, height, missed)
}

func concSubmittedLine(newCount, dupCount int) string {
	return fmt.Sprintf("submitted: new=%d duplicate=%d", newCount, dupCount)
}

// runConcurrentSubmitPair launches two independent helper processes that both
// run the real `heartbeat submit` command against the same data directory,
// each with its own payload on its own stdin.
//
// Each process is handed the read end of an os.Pipe as stdin and parks in
// io.ReadAll(os.Stdin) — before parsing input or opening the store. The write
// ends stay in this test process until both processes have started, then both
// payloads are written and closed at once, which is the start gate: the two
// genuine processes parse, open the store and contend on the cross-process
// flock together. The short pre-gate wait only lets both processes reach the
// gate; it is not used in any business assertion (and a shorter or longer
// wait cannot change the expected outcome, which is order-independent).
func runConcurrentSubmitPair(t *testing.T, dir, payloadA, payloadB string) [2]concResult {
	t.Helper()

	payloads := [][]byte{[]byte(payloadA), []byte(payloadB)}
	pipes := make([]*os.File, 2)
	cmds := make([]*exec.Cmd, 2)
	var stdoutBuf, stderrBuf [2]strings.Builder

	for i := range payloads {
		r, w, err := os.Pipe()
		if err != nil {
			t.Fatalf("create stdin pipe: %v", err)
		}
		pipes[i] = w

		cmd := exec.Command(os.Args[0],
			"heartbeat", "submit", "--data-dir", dir, "--receive-time", cliReceiveAt)
		cmd.Stdin = r
		cmd.Stdout = &stdoutBuf[i]
		cmd.Stderr = &stderrBuf[i]
		// Same environment contract as the serial CLI helper: HOME and the
		// data-dir env var are confined to the temp directory, and the re-exec
		// marker makes the test binary run main() itself.
		cmd.Env = append(os.Environ(),
			cliTestEnv+"=1",
			"HOME="+dir,
			"EDGEFLEET_DATA_DIR="+filepath.Join(dir, "env-data"),
		)
		cmds[i] = cmd

		// Avoid leaking a parked process if a later start or assertion aborts.
		t.Cleanup(func() {
			if cmd.Process != nil && cmd.ProcessState == nil {
				_ = cmd.Process.Kill()
			}
			_ = r.Close()
			_ = w.Close()
		})
	}

	for i, cmd := range cmds {
		if err := cmd.Start(); err != nil {
			t.Fatalf("start helper process %d: %v", i, err)
		}
	}

	// Both processes are reaching io.ReadAll(stdin); give the slower starter
	// time to park there, then release the pair simultaneously.
	time.Sleep(100 * time.Millisecond)
	var wg sync.WaitGroup
	for i := range payloads {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := pipes[i].Write(payloads[i]); err != nil {
				t.Errorf("feed payload to process %d: %v", i, err)
			}
			_ = pipes[i].Close() // EOF releases the process's ReadAll
		}(i)
	}
	wg.Wait()

	var results [2]concResult
	for i, cmd := range cmds {
		if err := cmd.Wait(); err != nil {
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) {
				t.Fatalf("helper process %d failed to run: %v", i, err)
			}
			results[i].code = exitErr.ExitCode()
		}
		results[i].out = strings.TrimRight(stdoutBuf[i].String(), "\n")
		results[i].errOut = strings.TrimRight(stderrBuf[i].String(), "\n")
	}
	return results
}

// assertSubmitSuccess locks the public success contract for one submitting
// process: exit status zero, nothing on stderr, and the exact counters line.
func assertSubmitSuccess(t *testing.T, who string, r concResult, wantLine string) {
	t.Helper()
	if r.code != 0 {
		t.Errorf("%s: expected success, exit=%d stderr=%q stdout=%q", who, r.code, r.errOut, r.out)
	}
	if r.errOut != "" {
		t.Errorf("%s: successful submit must print no error output, stderr=%q", who, r.errOut)
	}
	if r.out != wantLine {
		t.Errorf("%s: stdout=%q, want %q", who, r.out, wantLine)
	}
}

// assertConflictFailure locks the public conflict contract: non-zero exit, an
// error on stderr that identifies both the node and the seq, and no success
// line or counters on stdout (nothing may look like a committed submission).
func assertConflictFailure(t *testing.T, who string, r concResult) {
	t.Helper()
	if r.code == 0 {
		t.Errorf("%s: conflicting submit must exit non-zero; stdout=%q stderr=%q", who, r.out, r.errOut)
	}
	if strings.Contains(r.out, "submitted") || strings.Contains(r.out, "new=") {
		t.Errorf("%s: rejected submit must not report success or new counts, stdout=%q", who, r.out)
	}
	for _, want := range []string{"error:", "conflict", concNode, "seq 7"} {
		if !strings.Contains(r.errOut, want) {
			t.Errorf("%s: stderr must contain %q to identify the clash, got %q", who, want, r.errOut)
		}
	}
}

// exactlyOneSuccess reports which of the two processes succeeded, failing the
// test unless the split is exactly one winner and one loser.
func exactlyOneSuccess(t *testing.T, res [2]concResult) (winner, loser int) {
	t.Helper()
	switch {
	case res[0].code == 0 && res[1].code != 0:
		return 0, 1
	case res[1].code == 0 && res[0].code != 0:
		return 1, 0
	default:
		t.Fatalf("exactly one request must succeed, got exit codes %d and %d; stdout %q / %q; stderr %q / %q",
			res[0].code, res[1].code, res[0].out, res[1].out, res[0].errOut, res[1].errOut)
		return 0, 0
	}
}

// TestCLIConcurrentProcessesSameRecordOneNewOneDuplicate is the reconnect
// case: while the target record is not saved yet, two processes each deliver
// the very same cached heartbeat. Both requests succeed, but across the pair
// there is exactly one new record and one duplicate; each request's own
// counters match that split. History afterwards contains the node+seq once,
// with every submitted field intact.
func TestCLIConcurrentProcessesSameRecordOneNewOneDuplicate(t *testing.T) {
	payload := "[" + concRecordJSON(concNode, 7, concAt7UTC, concV1, concH7, concMissed7) + "]"
	wantHistory := concHistoryLine(7, concAt7UTC, concV1, concH7, concMissed7)

	for round := 0; round < concRounds; round++ {
		t.Run(fmt.Sprintf("round-%02d", round+1), func(t *testing.T) {
			dir := t.TempDir()
			res := runConcurrentSubmitPair(t, dir, payload, payload)

			for i, r := range res {
				if r.code != 0 {
					t.Errorf("request %d: an identical repeat must succeed, exit=%d stderr=%q", i, r.code, r.errOut)
				}
				if r.errOut != "" {
					t.Errorf("request %d: an identical repeat must produce no error output, stderr=%q", i, r.errOut)
				}
			}

			// One request saw the record absent and saved it; the other saw it
			// present and counted a duplicate. The two outcomes must split 1/1.
			bothNew := res[0].out == concSubmittedLine(1, 0) && res[1].out == concSubmittedLine(1, 0)
			bothDup := res[0].out == concSubmittedLine(0, 1) && res[1].out == concSubmittedLine(0, 1)
			if bothNew || bothDup {
				t.Fatalf("expected one new and one duplicate across the pair, got %q and %q", res[0].out, res[1].out)
			}
			for i, r := range res {
				if r.out != concSubmittedLine(1, 0) && r.out != concSubmittedLine(0, 1) {
					t.Errorf("request %d: unexpected counters %q", i, r.out)
				}
			}

			lines := historyLines(t, dir, concNode)
			if len(lines) != 1 {
				t.Fatalf("history must hold the node+seq exactly once, got %d lines: %q", len(lines), lines)
			}
			if lines[0] != wantHistory {
				t.Errorf("saved record differs from the submitted one:\n got %q\nwant %q", lines[0], wantHistory)
			}
		})
	}
}

// TestCLIConcurrentProcessesSameInstantDifferentOffsetIsDuplicate pins the
// time semantics across processes: the same collection instant written in
// different timezones is the same record. The different-looking strings must
// not be treated as a conflict, no error output is allowed, and history keeps
// one record whose instant equals the submitted moment (its timezone spelling
// is whichever request happened to save it first).
func TestCLIConcurrentProcessesSameInstantDifferentOffsetIsDuplicate(t *testing.T) {
	recUTC := "[" + concRecordJSON(concNode, 7, concAt7UTC, concV1, concH7, concMissed7) + "]"
	recCN := "[" + concRecordJSON(concNode, 7, concAt7CN, concV1, concH7, concMissed7) + "]"
	wantInstant, err := time.Parse(time.RFC3339, concAt7UTC)
	if err != nil {
		t.Fatal(err)
	}

	for round := 0; round < concRounds; round++ {
		t.Run(fmt.Sprintf("round-%02d", round+1), func(t *testing.T) {
			dir := t.TempDir()

			// Swap sides each round so either spelling can be first saved.
			payloadA, spellingA := recUTC, concAt7UTC
			payloadB, spellingB := recCN, concAt7CN
			if round%2 == 1 {
				payloadA, spellingA, payloadB, spellingB = recCN, concAt7CN, recUTC, concAt7UTC
			}

			res := runConcurrentSubmitPair(t, dir, payloadA, payloadB)
			for i, r := range res {
				if r.code != 0 {
					t.Errorf("request %d: timezone-equal repeat must succeed, exit=%d stderr=%q", i, r.code, r.errOut)
				}
				if r.errOut != "" {
					t.Errorf("request %d: timezone-equal repeat must produce no error output, stderr=%q", i, r.errOut)
				}
				if r.out != concSubmittedLine(1, 0) && r.out != concSubmittedLine(0, 1) {
					t.Errorf("request %d: unexpected counters %q", i, r.out)
				}
			}
			if (res[0].out == concSubmittedLine(1, 0)) == (res[1].out == concSubmittedLine(1, 0)) {
				t.Fatalf("expected one new and one duplicate, got %q and %q", res[0].out, res[1].out)
			}

			lines := historyLines(t, dir, concNode)
			if len(lines) != 1 {
				t.Fatalf("timezone-equal records must collapse to one, got %q", lines)
			}
			// The saved spelling belongs to the request that saved it; the
			// instant must be identical regardless of which spelling won.
			wantSpelling := spellingA
			if res[1].out == concSubmittedLine(1, 0) {
				wantSpelling = spellingB
			}
			if want := concHistoryLine(7, wantSpelling, concV1, concH7, concMissed7); lines[0] != want {
				t.Errorf("history line:\n got %q\nwant %q", lines[0], want)
			}
			fields := strings.Fields(lines[0])
			gotInstant, perr := time.Parse(time.RFC3339, strings.TrimPrefix(fields[2], "collected_at="))
			if perr != nil {
				t.Fatalf("saved collected_at is not RFC3339: %q", fields[2])
			}
			if !gotInstant.Equal(wantInstant) {
				t.Errorf("saved instant %s != submitted instant %s", gotInstant.Format(time.RFC3339), wantInstant.Format(time.RFC3339))
			}
		})
	}
}

// TestCLIConcurrentProcessesDifferentVersionConflicts covers two records of
// the same node+seq whose versions differ. Exactly one request succeeds; the
// other must exit non-zero with a conflict error naming node and seq and must
// print no success or new-count output. Either version may be saved first;
// history must then contain exactly the winning request's record, never
// overwritten by the loser.
func TestCLIConcurrentProcessesDifferentVersionConflicts(t *testing.T) {
	recV1 := "[" + concRecordJSON(concNode, 7, concAt7UTC, concV1, concH7, concMissed7) + "]"
	recV2 := "[" + concRecordJSON(concNode, 7, concAt7UTC, concV2, concH7, concMissed7) + "]"

	for round := 0; round < concRounds; round++ {
		t.Run(fmt.Sprintf("round-%02d", round+1), func(t *testing.T) {
			dir := t.TempDir()

			// Swap sides each round so both versions genuinely experience both
			// outcomes; expectations follow the observed winner.
			payloadA, versionA := recV1, concV1
			payloadB, versionB := recV2, concV2
			if round%2 == 1 {
				payloadA, versionA, payloadB, versionB = recV2, concV2, recV1, concV1
			}

			res := runConcurrentSubmitPair(t, dir, payloadA, payloadB)
			winner, loser := exactlyOneSuccess(t, res)
			assertSubmitSuccess(t, "winning request", res[winner], concSubmittedLine(1, 0))
			assertConflictFailure(t, "losing request", res[loser])

			winningVersion := versionA
			if winner == 1 {
				winningVersion = versionB
			}
			lines := historyLines(t, dir, concNode)
			if len(lines) != 1 {
				t.Fatalf("conflict must leave exactly one saved record, got %q", lines)
			}
			want := concHistoryLine(7, concAt7UTC, winningVersion, concH7, concMissed7)
			if lines[0] != want {
				t.Errorf("history must match the winner exactly:\n got %q\nwant %q (loser must not overwrite)", lines[0], want)
			}
		})
	}
}

// TestCLIConcurrentProcessesConflictRejectsWholeBatch adds the batch rule:
// when the losing request also carries a legal record for another seq of the
// same node, that companion record must not be saved either — a conflict
// rejects the whole batch. If the two-record batch wins instead, both of its
// records persist and the single-record request loses; history in that case
// holds both seqs in ascending order with the batch's version on each.
func TestCLIConcurrentProcessesConflictRejectsWholeBatch(t *testing.T) {
	// Single-record request: seq 7 on version 1.26.0.
	single := "[" + concRecordJSON(concNode, 7, concAt7UTC, concV1, concH7, concMissed7) + "]"
	// Batch request: clashing seq 7 (version 1.27.0) plus a legal seq 8.
	batch := "[" +
		concRecordJSON(concNode, 7, concAt7UTC, concV2, concH7, concMissed7) + "," +
		concRecordJSON(concNode, 8, concAt8UTC, concV2, concH8, concMissed8) + "]"

	for round := 0; round < concRounds; round++ {
		t.Run(fmt.Sprintf("round-%02d", round+1), func(t *testing.T) {
			dir := t.TempDir()

			// Swap which process carries which request; the outcome follows the
			// content, not the process position.
			payloadA, payloadB := single, batch
			batchIdx, singleIdx := 1, 0 // process A carries the single record, B the batch
			if round%2 == 1 {
				payloadA, payloadB = batch, single
				batchIdx, singleIdx = 0, 1
			}

			res := runConcurrentSubmitPair(t, dir, payloadA, payloadB)

			lines := historyLines(t, dir, concNode)
			switch {
			case res[singleIdx].code == 0 && res[batchIdx].code != 0:
				// The single seq-7 record won; the whole clashing batch was
				// rejected, so its otherwise-valid seq 8 must not exist.
				assertSubmitSuccess(t, "single-record request", res[singleIdx], concSubmittedLine(1, 0))
				assertConflictFailure(t, "clashing batch request", res[batchIdx])
				if len(lines) != 1 {
					t.Fatalf("rejected batch must leave no companion record, got %q", lines)
				}
				want := concHistoryLine(7, concAt7UTC, concV1, concH7, concMissed7)
				if lines[0] != want {
					t.Errorf("surviving record:\n got %q\nwant %q", lines[0], want)
				}

			case res[batchIdx].code == 0 && res[singleIdx].code != 0:
				// The batch won: both its records are saved and the single
				// request loses the seq-7 clash.
				assertSubmitSuccess(t, "batch request", res[batchIdx], concSubmittedLine(2, 0))
				assertConflictFailure(t, "single-record request", res[singleIdx])
				if len(lines) != 2 {
					t.Fatalf("winning batch must save both records, got %q", lines)
				}
				want7 := concHistoryLine(7, concAt7UTC, concV2, concH7, concMissed7)
				want8 := concHistoryLine(8, concAt8UTC, concV2, concH8, concMissed8)
				if lines[0] != want7 || lines[1] != want8 {
					t.Errorf("history must be ascending and match the batch:\n got %q / %q\nwant %q / %q",
						lines[0], lines[1], want7, want8)
				}

			default:
				t.Fatalf("exactly one side must succeed, got exits single=%d batch=%d; stderr %q / %q",
					res[singleIdx].code, res[batchIdx].code, res[singleIdx].errOut, res[batchIdx].errOut)
			}
		})
	}
}

// TestCLIConcurrentProcessesDifferentSeqsBothKept checks that parallel
// submissions of different seqs for the same node do not lose each other's
// new records: both requests succeed with one new record each, and history
// returns both in ascending seq order with their own submitted content.
func TestCLIConcurrentProcessesDifferentSeqsBothKept(t *testing.T) {
	rec7 := "[" + concRecordJSON(concNode, 7, concAt7UTC, concV1, concH7, concMissed7) + "]"
	rec8 := "[" + concRecordJSON(concNode, 8, concAt8UTC, concV1, concH8, concMissed8) + "]"

	for round := 0; round < concRounds; round++ {
		t.Run(fmt.Sprintf("round-%02d", round+1), func(t *testing.T) {
			dir := t.TempDir()

			payloadA, payloadB := rec7, rec8
			if round%2 == 1 {
				payloadA, payloadB = rec8, rec7
			}
			res := runConcurrentSubmitPair(t, dir, payloadA, payloadB)

			for i, r := range res {
				assertSubmitSuccess(t, fmt.Sprintf("request %d", i), r, concSubmittedLine(1, 0))
			}

			lines := historyLines(t, dir, concNode)
			if len(lines) != 2 {
				t.Fatalf("both parallel inserts must survive, got %d lines: %q", len(lines), lines)
			}
			want7 := concHistoryLine(7, concAt7UTC, concV1, concH7, concMissed7)
			want8 := concHistoryLine(8, concAt8UTC, concV1, concH8, concMissed8)
			if lines[0] != want7 {
				t.Errorf("first history line:\n got %q\nwant %q", lines[0], want7)
			}
			if lines[1] != want8 {
				t.Errorf("second history line:\n got %q\nwant %q", lines[1], want8)
			}
		})
	}
}
