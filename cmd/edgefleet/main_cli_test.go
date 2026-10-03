package main

// Command-line regression tests for `edgefleet heartbeat health`.
//
// The domain rules (new-missed arithmetic, rollback detection, baseline
// validation) already have focused tests in package edgefleet. These tests
// protect what a user actually sees at the real command entry point:
//
//   - whether --missed-since-seq takes effect at all,
//   - the exact terminal output (cumulative missed stays on the first line,
//     baseline fields on the second),
//   - stdout vs stderr placement (health findings vs usage errors),
//   - and the process exit status (a health finding is not a command failure;
//     a bad baseline is).
//
// The tests re-execute the compiled test binary, which calls main() itself
// (see TestMain), so the code under test is the genuine command path — flag
// parsing included. Everything runs offline against a temp data directory
// with fixed receive/query instants, so no wall-clock waiting or network is
// involved.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/asdhoaiqqq/edgefleet-validator/edgefleet"
)

const (
	// cliTestEnv makes the re-executed test binary run the real main() instead
	// of the test suite.
	cliTestEnv = "EDGEFLEET_CLI_TEST_HELPER_PROCESS"

	// Fixed instants: heartbeats arrive and are queried at known times, so
	// online status is deterministic without sleeping.
	cliReceiveAt = "2026-10-01T12:00:00Z"
	cliQueryAt   = "2026-10-01T12:00:00Z"
)

// fixture holds a populated, isolated data directory.
type fixture struct {
	t   *testing.T
	dir string
}

func TestMain(m *testing.M) {
	if os.Getenv(cliTestEnv) == "1" {
		// Run the genuine command entry point and let its own os.Exit calls
		// determine the helper process status.
		main()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// runCLI invokes the re-executed binary with the given arguments. stdin is
// fed to the command (used by `heartbeat submit`). It returns the captured
// stdout, stderr and exit code.
func runCLI(t *testing.T, dir, stdin string, args ...string) (string, string, int) {
	t.Helper()
	cmd := exec.Command(os.Args[0], args...)
	cmd.Stdin = strings.NewReader(stdin)
	// Keep HOME inside the temp area too, so a missing --data-dir could never
	// touch the developer's real ~/.edgefleet.
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
			t.Fatalf("failed to run helper binary: %v", err)
		}
	}
	return out.String(), errOut.String(), code
}

// submitBatch stores one JSON array via the real submit command.
func submitBatch(t *testing.T, dir, json string) {
	t.Helper()
	out, errOut, code := runCLI(t, dir, json,
		"heartbeat", "submit", "--data-dir", dir, "--receive-time", cliReceiveAt)
	if code != 0 {
		t.Fatalf("submit exited %d: stderr=%q stdout=%q", code, errOut, out)
	}
}

// health runs the health command with the common fixed query time.
func health(t *testing.T, f *fixture, args ...string) (string, string, int) {
	t.Helper()
	full := append([]string{"heartbeat", "health", "--data-dir", f.dir, "--at", cliQueryAt}, args...)
	return runCLI(t, f.dir, "", full...)
}

// outputLines splits stdout into lines, treating a trailing newline as no
// extra empty line.
func outputLines(s string) []string {
	s = strings.TrimRight(s, "\n")
	if s == "" {
		return nil
	}
	return strings.Split(s, "\n")
}

// newFixture creates a temp data directory with three nodes:
//
//	mono:   seq 1/2/3, cumulative missed 12/13/14, version 1.26.0
//	reset:  seq 1/2/3, cumulative missed 12/5/14 (counter reset), version 1.25.0
//	gappy:  seq 1/3 (seq 2 never saved), missed 0/4, version 1.26.0
//
// Latest collection time is one second before cliQueryAt, so every node is
// online at the query instant.
func newFixture(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{t: t, dir: t.TempDir()}

	submitBatch(t, f.dir, `[
	  {"node":"mono","seq":1,"collected_at":"2026-10-01T11:59:57Z","version":"1.26.0","height":101,"missed":12},
	  {"node":"mono","seq":2,"collected_at":"2026-10-01T11:59:58Z","version":"1.26.0","height":102,"missed":13},
	  {"node":"mono","seq":3,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":103,"missed":14}
	]`)
	submitBatch(t, f.dir, `[
	  {"node":"reset","seq":1,"collected_at":"2026-10-01T11:59:57Z","version":"1.25.0","height":201,"missed":12},
	  {"node":"reset","seq":2,"collected_at":"2026-10-01T11:59:58Z","version":"1.25.0","height":202,"missed":5},
	  {"node":"reset","seq":3,"collected_at":"2026-10-01T11:59:59Z","version":"1.25.0","height":203,"missed":14}
	]`)
	submitBatch(t, f.dir, `[
	  {"node":"gappy","seq":1,"collected_at":"2026-10-01T11:59:57Z","version":"1.26.0","height":301,"missed":0},
	  {"node":"gappy","seq":3,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":303,"missed":4}
	]`)
	return f
}

func TestCLIHealthBaselineTakesEffect(t *testing.T) {
	f := newFixture(t)

	// Baseline cumulative 12, latest cumulative 14, tolerance 2: the two new
	// missed duties equal the tolerance and must NOT alarm.
	out, errOut, code := health(t, f,
		"--node", "mono", "--expected-version", "1.26.0",
		"--tolerated-misses", "2", "--missed-since-seq", "1")
	if code != 0 {
		t.Fatalf("health query with a valid baseline must succeed, exit=%d stderr=%q", code, errOut)
	}
	if errOut != "" {
		t.Errorf("health findings belong on stdout; stderr should be empty, got %q", errOut)
	}

	lines := outputLines(out)
	if len(lines) != 2 {
		t.Fatalf("baseline query must print two lines, got %d: %q", len(lines), out)
	}

	// The latest heartbeat (greatest seq) is still what the first line shows,
	// and missed there stays the cumulative value (14), never the new count.
	first := lines[0]
	for _, want := range []string{
		"node=mono", "status=online", "seq=3", "version=1.26.0",
		"height=103", "missed=14", "collected_at=2026-10-01T11:59:59Z",
	} {
		if !strings.Contains(first, want) {
			t.Errorf("first line %q missing %q", first, want)
		}
	}
	if strings.Contains(first, "missed=2 ") || strings.HasSuffix(first, "missed=2") {
		t.Errorf("first line must keep cumulative missed=14, not the new count: %q", first)
	}
	if strings.Contains(first, "above tolerance") {
		t.Errorf("new=2 with tolerance 2 must not alarm: %q", first)
	}

	// The baseline basis is stated separately on the second line.
	wantSecond := "baseline_seq=1 baseline_missed=12 new_missed=2 tolerated_misses=2"
	if lines[1] != wantSecond {
		t.Errorf("second line = %q, want %q", lines[1], wantSecond)
	}
}

func TestCLIHealthWithoutBaselineKeepsCumulativeRule(t *testing.T) {
	f := newFixture(t)

	// Same history, no baseline: the judgement uses the cumulative 14 and the
	// tolerance 2, so it alarms — and no baseline explanation is printed.
	out, errOut, code := health(t, f,
		"--node", "mono", "--expected-version", "1.26.0", "--tolerated-misses", "2")
	if code != 0 {
		t.Fatalf("a health finding is not a command failure: exit=%d stderr=%q", code, errOut)
	}
	lines := outputLines(out)
	if len(lines) != 1 {
		t.Fatalf("baseless query must print exactly one line, got %q", out)
	}
	first := lines[0]
	if !strings.Contains(first, "missed=14") {
		t.Errorf("first line must show cumulative missed=14: %q", first)
	}
	if !strings.Contains(first, "missed duties above tolerance") {
		t.Errorf("cumulative 14 > tolerance 2 must be reported: %q", first)
	}
	if strings.Contains(out, "baseline_seq") || strings.Contains(out, "new_missed") {
		t.Errorf("baseless query must not explain a baseline: %q", out)
	}
}

func TestCLIHealthBaselineLowerToleranceStillExitsZero(t *testing.T) {
	f := newFixture(t)

	// Lower the tolerance to 1: new=2 now strictly exceeds it and the finding
	// is reported on stdout, but the command itself still succeeds.
	out, errOut, code := health(t, f,
		"--node", "mono", "--expected-version", "1.26.0",
		"--tolerated-misses", "1", "--missed-since-seq", "1")
	if code != 0 {
		t.Fatalf("reporting a health problem must still exit 0, got %d (stderr=%q)", code, errOut)
	}
	if errOut != "" {
		t.Errorf("health findings go to stdout, stderr=%q", errOut)
	}
	lines := outputLines(out)
	if len(lines) != 2 {
		t.Fatalf("want two lines, got %q", out)
	}
	if !strings.Contains(lines[0], "missed duties above tolerance") {
		t.Errorf("new=2 > tolerance 1 must alarm: %q", lines[0])
	}
	if !strings.Contains(lines[0], "missed=14") {
		t.Errorf("first line still shows the cumulative 14: %q", lines[0])
	}
	if lines[1] != "baseline_seq=1 baseline_missed=12 new_missed=2 tolerated_misses=1" {
		t.Errorf("second line = %q, want tolerated_misses=1", lines[1])
	}
}

func TestCLIHealthBaselineIsLatestNewZero(t *testing.T) {
	f := newFixture(t)

	out, _, code := health(t, f,
		"--node", "mono", "--expected-version", "1.26.0",
		"--tolerated-misses", "0", "--missed-since-seq", "3")
	if code != 0 {
		t.Fatalf("baseline equal to latest must succeed, exit=%d", code)
	}
	lines := outputLines(out)
	if len(lines) != 2 {
		t.Fatalf("want two lines, got %q", out)
	}
	if lines[1] != "baseline_seq=3 baseline_missed=14 new_missed=0 tolerated_misses=0" {
		t.Errorf("baseline == latest: second line = %q", lines[1])
	}
	if strings.Contains(lines[0], "above tolerance") {
		t.Errorf("zero new missed duties must not alarm: %q", lines[0])
	}
}

func TestCLIHealthRollbackShowsUnknownWithoutDroppingOtherStatus(t *testing.T) {
	f := newFixture(t)

	// Saved cumulative counts 12 -> 5 -> 14 between baseline seq 1 and the
	// latest seq 3: the new count cannot be derived.
	out, errOut, code := health(t, f,
		"--node", "reset", "--expected-version", "1.26.0",
		"--tolerated-misses", "2", "--missed-since-seq", "1")
	if code != 0 {
		t.Fatalf("an undecidable miss count is still a successful query: exit=%d stderr=%q", code, errOut)
	}
	lines := outputLines(out)
	if len(lines) != 2 {
		t.Fatalf("want two lines, got %q", out)
	}
	first := lines[0]

	// Online status, version skew and the real latest cumulative count are
	// still reported — unknown new misses must not make them disappear.
	for _, want := range []string{
		"status=online", "seq=3", "version=1.25.0", "missed=14",
		"collected_at=2026-10-01T11:59:59Z", "version skew: 1.25.0 != 1.26.0",
		"累计漏签数回退",
	} {
		if !strings.Contains(first, want) {
			t.Errorf("first line %q missing %q", first, want)
		}
	}
	if strings.Contains(first, "above tolerance") {
		t.Errorf("rollback must suppress the tolerance alarm: %q", first)
	}

	wantSecond := "baseline_seq=1 baseline_missed=12 new_missed=无法判断 tolerated_misses=2"
	if lines[1] != wantSecond {
		t.Errorf("second line = %q, want %q", lines[1], wantSecond)
	}
}

func TestCLIHealthBadBaselinesAreErrors(t *testing.T) {
	f := newFixture(t)

	cases := []struct {
		name    string
		node    string
		seq     string
		errText string
	}{
		// Explicit 0 must NOT be treated as "flag omitted".
		{"explicit zero", "mono", "0", "positive"},
		// A positive seq the node never saved: no other heartbeat substituted.
		{"missing seq in gap", "gappy", "2", "seq 2"},
		// A positive seq above the latest one is likewise an error.
		{"seq above latest", "mono", "99", "99"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, errOut, code := health(t, f,
				"--node", tc.node, "--expected-version", "1.26.0",
				"--tolerated-misses", "2", "--missed-since-seq", tc.seq)
			if code == 0 {
				t.Errorf("baseline %s must fail with non-zero exit; stdout=%q", tc.seq, out)
			}
			if !strings.Contains(errOut, "error:") {
				t.Errorf("baseline error must be reported on stderr as an error, got %q", errOut)
			}
			if !strings.Contains(errOut, tc.errText) {
				t.Errorf("stderr %q should mention %q", errOut, tc.errText)
			}
			if strings.Contains(out, "node=") || strings.Contains(out, "status=") {
				t.Errorf("a failed baseline query must not print a seemingly healthy result on stdout: %q", out)
			}
		})
	}
}

func TestCLIHealthBaselineOnNodeWithoutTelemetryFails(t *testing.T) {
	f := newFixture(t)

	out, errOut, code := health(t, f,
		"--node", "ghost", "--expected-version", "1.26.0",
		"--tolerated-misses", "2", "--missed-since-seq", "1")
	if code == 0 {
		t.Errorf("baseline on a node without telemetry must fail; stdout=%q", out)
	}
	if !strings.Contains(errOut, "no heartbeats") {
		t.Errorf("stderr should explain the node has no heartbeats, got %q", errOut)
	}
	if out != "" {
		t.Errorf("stdout must stay empty on a baseline error, got %q", out)
	}

	// The same unknown node without a baseline keeps the existing
	// notelemetry behaviour: successful query, normal output, no baseline.
	out, _, code = health(t, f,
		"--node", "ghost", "--expected-version", "1.26.0", "--tolerated-misses", "2")
	if code != 0 {
		t.Fatalf("notelemetry query must exit 0, got %d", code)
	}
	lines := outputLines(out)
	if len(lines) != 1 || !strings.Contains(lines[0], "status=无遥测") {
		t.Errorf("notelemetry output = %q", out)
	}
	if strings.Contains(out, "baseline_seq") {
		t.Errorf("notelemetry output must not mention a baseline: %q", out)
	}
}

// nodeFilePath mirrors the per-node storage layout used by the store.
func nodeFilePath(dir, node string) string {
	return filepath.Join(dir, "nodes", hex.EncodeToString([]byte(node))+".json")
}

// forgedChecksum returns the checksum the records would carry after a lenient
// JSON decoder filled missing/null fields with zero values and kept the last
// duplicate key. A corrupt file written with this checksum proves the reader
// rejects damaged records even when the checksum matches the interpretation.
func forgedChecksum(t *testing.T, recordsJSON string) string {
	t.Helper()
	var records []edgefleet.Heartbeat
	if err := json.Unmarshal([]byte(recordsJSON), &records); err != nil {
		t.Fatalf("lenient parse of %q failed: %v", recordsJSON, err)
	}
	data, err := json.Marshal(records)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// writeForgedFile overwrites a node file with recordsJSON and a checksum
// matching the lenient interpretation of those records.
func writeForgedFile(t *testing.T, dir, node, recordsJSON string) string {
	t.Helper()
	path := nodeFilePath(dir, node)
	content := `{"format":"edgefleet-heartbeats-v1","checksum":"` +
		forgedChecksum(t, recordsJSON) + `","records":` + recordsJSON + `}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

const corruptRecordMissingMissed = `[{"node":"mono","seq":3,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":103}]`

const corruptRecordDuplicateMissed = `[{"node":"mono","seq":3,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":103,"missed":14,"missed":14}]`

func TestCLIQueriesOnCorruptNodeFail(t *testing.T) {
	cases := []struct {
		name        string
		recordsJSON string
		wantInErr   string
	}{
		{"missing missed with matching checksum", corruptRecordMissingMissed, "missed"},
		{"duplicate missed with matching checksum", corruptRecordDuplicateMissed, "duplicate"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			path := writeForgedFile(t, f.dir, "mono", tc.recordsJSON)

			// Plain health: stderr explains the corruption, exit non-zero,
			// no normal result on stdout.
			out, errOut, code := health(t, f,
				"--node", "mono", "--expected-version", "1.26.0", "--tolerated-misses", "2")
			if code == 0 {
				t.Errorf("health must exit non-zero, stdout=%q", out)
			}
			if out != "" {
				t.Errorf("corrupt health must print nothing on stdout, got %q", out)
			}
			for _, want := range []string{"error:", "corruption", path, "record 1", tc.wantInErr} {
				if !strings.Contains(errOut, want) {
					t.Errorf("stderr %q should contain %q", errOut, want)
				}
			}

			// Baseline health must fail identically and never fall back to
			// another heartbeat.
			out, errOut, code = health(t, f,
				"--node", "mono", "--expected-version", "1.26.0",
				"--tolerated-misses", "2", "--missed-since-seq", "1")
			if code == 0 {
				t.Errorf("baseline health must exit non-zero, stdout=%q", out)
			}
			if out != "" {
				t.Errorf("corrupt baseline health must print nothing on stdout, got %q", out)
			}
			if !strings.Contains(errOut, "corruption") || !strings.Contains(errOut, "record 1") {
				t.Errorf("baseline stderr should report the corrupt record, got %q", errOut)
			}

			// History: same rules — no rows, clear reason on stderr.
			out, errOut, code = runCLI(t, f.dir, "",
				"heartbeat", "history", "--data-dir", f.dir, "--node", "mono")
			if code == 0 {
				t.Errorf("history must exit non-zero, stdout=%q", out)
			}
			if out != "" {
				t.Errorf("corrupt history must print nothing on stdout, got %q", out)
			}
			if !strings.Contains(errOut, "corruption") || !strings.Contains(errOut, "record 1") {
				t.Errorf("history stderr should report the corrupt record, got %q", errOut)
			}
		})
	}
}

func TestCLISubmitBatchTouchingCorruptNodeIsRejected(t *testing.T) {
	f := newFixture(t)
	path := writeForgedFile(t, f.dir, "mono", corruptRecordMissingMissed)

	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	// The batch mixes the corrupt node with a brand-new node: the whole batch
	// must be refused before anything is saved.
	input := `[
	  {"node":"mono","seq":4,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":104,"missed":15},
	  {"node":"fresh","seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":1,"missed":0}
	]`
	out, errOut, code := runCLI(t, f.dir, input,
		"heartbeat", "submit", "--data-dir", f.dir, "--receive-time", cliReceiveAt)
	if code == 0 {
		t.Errorf("submit must exit non-zero, stdout=%q", out)
	}
	if out != "" {
		t.Errorf("rejected submit must print nothing on stdout, got %q", out)
	}
	for _, want := range []string{"error:", "corruption", "record 1", "missed"} {
		if !strings.Contains(errOut, want) {
			t.Errorf("stderr %q should contain %q", errOut, want)
		}
	}

	// The corrupt file is byte-for-byte unchanged: no auto repair, no checksum
	// regeneration, no truncation.
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(before) {
		t.Errorf("corrupt file was modified by the rejected submit")
	}
	// The innocent node in the batch must have no file at all.
	if _, err := os.Stat(nodeFilePath(f.dir, "fresh")); !os.IsNotExist(err) {
		t.Errorf("fresh node file created despite rejected batch: %v", err)
	}

	// A later batch touching only healthy nodes works normally.
	submitBatch(t, f.dir, `[{"node":"fresh","seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":1,"missed":0}]`)

	// And the corrupt node is still refused — nothing about the successful
	// other-node submission patched it.
	_, _, code = runCLI(t, f.dir, "",
		"heartbeat", "history", "--data-dir", f.dir, "--node", "mono")
	if code == 0 {
		t.Errorf("corrupt node must still be refused after other-node submit")
	}
}

func TestCLICorruptNodeDoesNotAffectOtherNodes(t *testing.T) {
	f := newFixture(t)
	writeForgedFile(t, f.dir, "mono", corruptRecordMissingMissed)

	// A different node keeps full normal service.
	out, errOut, code := health(t, f,
		"--node", "reset", "--expected-version", "1.26.0",
		"--tolerated-misses", "2", "--missed-since-seq", "1")
	if code != 0 {
		t.Fatalf("healthy node query failed: code=%d stderr=%q", code, errOut)
	}
	if !strings.Contains(out, "node=reset") || !strings.Contains(out, "baseline_seq=1") {
		t.Errorf("healthy node output wrong: %q", out)
	}

	out, errOut, code = runCLI(t, f.dir, "",
		"heartbeat", "history", "--data-dir", f.dir, "--node", "gappy")
	if code != 0 {
		t.Fatalf("healthy node history failed: code=%d stderr=%q", code, errOut)
	}
	if lines := outputLines(out); len(lines) != 2 {
		t.Errorf("gappy history should still list 2 rows, got %q", out)
	}

	// Submitting only to a healthy node succeeds.
	submitBatch(t, f.dir, `[{"node":"reset","seq":4,"collected_at":"2026-10-01T12:00:00Z","version":"1.25.0","height":204,"missed":14}]`)

	// And the corrupt node still fails.
	_, _, code = health(t, f, "--node", "mono", "--expected-version", "1.26.0", "--tolerated-misses", "2")
	if code == 0 {
		t.Errorf("corrupt node query must still fail")
	}
}
