package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// These tests exercise the real command entry point as a user sees it: the
// edgefleet binary is built once and run as a subprocess, so flag parsing,
// stdout/stderr separation and process exit status are all covered. Nothing
// depends on wall-clock time or the network: every command pins its time with
// --receive-time/--at and every data directory is a per-test temp dir.

const (
	// Latest collected_at is 12:00:03Z, 27s before the pinned query time, so
	// the node is online without any test waiting.
	cliFixedTime = "2026-10-01T12:00:30Z"

	monotonicHistoryJSON = `[
 {"node":"n1","seq":1,"collected_at":"2026-10-01T12:00:01Z","version":"1.0","height":100,"missed":12},
 {"node":"n1","seq":2,"collected_at":"2026-10-01T12:00:02Z","version":"1.0","height":101,"missed":13},
 {"node":"n1","seq":3,"collected_at":"2026-10-01T12:00:03Z","version":"1.0","height":102,"missed":14}
]`

	// Counter goes 12 -> 5 (reset) -> 14 between the baseline and the latest
	// saved record; the node also reports an outdated version.
	rollbackHistoryJSON = `[
 {"node":"n1","seq":1,"collected_at":"2026-10-01T12:00:01Z","version":"0.9","height":100,"missed":12},
 {"node":"n1","seq":2,"collected_at":"2026-10-01T12:00:02Z","version":"0.9","height":101,"missed":5},
 {"node":"n1","seq":3,"collected_at":"2026-10-01T12:00:03Z","version":"0.9","height":102,"missed":14}
]`

	// Seq 2 is a gap: saved seqs are 1 and 3 only.
	gapHistoryJSON = `[
 {"node":"n1","seq":1,"collected_at":"2026-10-01T12:00:01Z","version":"1.0","height":100,"missed":12},
 {"node":"n1","seq":3,"collected_at":"2026-10-01T12:00:03Z","version":"1.0","height":102,"missed":14}
]`
)

var edgefleetBin string

func TestMain(m *testing.M) {
	binDir, err := os.MkdirTemp("", "edgefleet-cli-build")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	bin := filepath.Join(binDir, "edgefleet")
	build := exec.Command("go", "build", "-o", bin, ".")
	build.Stdout = os.Stderr
	build.Stderr = os.Stderr
	// The module has no external dependencies; forbid any network lookup so
	// the suite stays hermetic on an offline machine.
	build.Env = append(os.Environ(), "GOPROXY=off")
	if err := build.Run(); err != nil {
		fmt.Fprintln(os.Stderr, "building edgefleet for CLI tests failed:", err)
		os.RemoveAll(binDir)
		os.Exit(1)
	}
	edgefleetBin = bin
	code := m.Run()
	os.RemoveAll(binDir)
	os.Exit(code)
}

type procResult struct {
	stdout string
	stderr string
	code   int
}

// runEdgefleet executes the built binary, capturing the streams separately and
// returning the real process exit code. A failure to start the process fails
// the test; a non-zero exit status is returned for the test to assert on.
func runEdgefleet(t *testing.T, stdin io.Reader, args ...string) procResult {
	t.Helper()
	cmd := exec.Command(edgefleetBin, args...)
	if stdin != nil {
		cmd.Stdin = stdin
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	// Isolate the run from the developer's environment: the data dir is
	// always pinned explicitly, never inherited.
	env := make([]string, 0, len(os.Environ()))
	for _, kv := range os.Environ() {
		if strings.HasPrefix(kv, "EDGEFLEET_DATA_DIR=") {
			continue
		}
		env = append(env, kv)
	}
	cmd.Env = env

	err := cmd.Run()
	code := 0
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			code = exitErr.ExitCode()
		} else {
			t.Fatalf("cannot run edgefleet %v: %v", args, err)
		}
	}
	return procResult{stdout: stdout.String(), stderr: stderr.String(), code: code}
}

// seedNode submits the given JSON into a fresh data directory and returns it.
func seedNode(t *testing.T, recordsJSON string) string {
	t.Helper()
	dir := t.TempDir()
	r := runEdgefleet(t, strings.NewReader(recordsJSON),
		"heartbeat", "submit", "--data-dir", dir, "--receive-time", cliFixedTime)
	if r.code != 0 || r.stderr != "" {
		t.Fatalf("seed submit failed: code=%d stderr=%q stdout=%q", r.code, r.stderr, r.stdout)
	}
	return dir
}

// health runs a health query for n1 at the pinned query time, expecting
// version 1.0; callers pass tolerance/baseline flags.
func health(t *testing.T, dataDir string, extra ...string) procResult {
	t.Helper()
	args := []string{
		"heartbeat", "health",
		"--data-dir", dataDir,
		"--node", "n1",
		"--expected-version", "1.0",
		"--at", cliFixedTime,
	}
	args = append(args, extra...)
	return runEdgefleet(t, nil, args...)
}

func assertContains(t *testing.T, output, sub string) {
	t.Helper()
	if !strings.Contains(output, sub) {
		t.Errorf("output must contain %q, got:\n%s", sub, output)
	}
}

func assertNotContains(t *testing.T, output, sub string) {
	t.Helper()
	if strings.Contains(output, sub) {
		t.Errorf("output must not contain %q, got:\n%s", sub, output)
	}
}

// outputLines splits command stdout into trimmed non-empty lines.
func outputLines(t *testing.T, stdout string) []string {
	t.Helper()
	var lines []string
	for _, line := range strings.Split(strings.TrimSpace(stdout), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}

// TestCLIHealthBaselineQuery guards what the user actually sees when a
// historical heartbeat is named as the missed-duty baseline: the latest
// heartbeat is still the max-seq record, the first line keeps the cumulative
// count, the second line states the baseline basis, equality with the
// tolerance does not alarm, and a health finding never turns the query into a
// failed command. Omitting the baseline keeps the old cumulative judgement
// with no baseline line at all.
func TestCLIHealthBaselineQuery(t *testing.T) {
	dir := seedNode(t, monotonicHistoryJSON)

	// Baseline cumulative 12, latest cumulative 14, tolerance 2: new=2 equals
	// the tolerance, so there must be no alarm even though cumulative 14 is
	// already above the tolerance.
	r := health(t, dir, "--tolerated-misses", "2", "--missed-since-seq", "1")
	if r.code != 0 {
		t.Fatalf("baseline query exit=%d, want 0; stderr=%q", r.code, r.stderr)
	}
	if r.stderr != "" {
		t.Errorf("successful health query must not write stderr, got %q", r.stderr)
	}
	lines := outputLines(t, r.stdout)
	if len(lines) != 2 {
		t.Fatalf("baseline query must print 2 lines, got %d:\n%s", len(lines), r.stdout)
	}
	// The first line always describes the latest (max-seq) heartbeat with its
	// cumulative count; the new count must never overwrite it.
	for _, sub := range []string{
		"node=n1", "status=online", "seq=3",
		"collected_at=2026-10-01T12:00:03Z",
		"version=1.0", "height=102", "missed=14", "findings=[]",
	} {
		assertContains(t, lines[0], sub)
	}
	assertNotContains(t, lines[0], "above tolerance")
	assertNotContains(t, lines[0], "baseline")
	// The baseline basis is stated separately on the second line.
	for _, sub := range []string{
		"baseline_seq=1", "baseline_missed=12",
		"new_missed=2", "tolerated_misses=2",
	} {
		assertContains(t, lines[1], sub)
	}

	// Lower the tolerance to 1: new=2 strictly exceeds it, so the finding is
	// reported in normal output, but the query still exits successfully.
	r = health(t, dir, "--tolerated-misses", "1", "--missed-since-seq", "1")
	if r.code != 0 {
		t.Fatalf("finding must not make the command fail: exit=%d stderr=%q", r.code, r.stderr)
	}
	if r.stderr != "" {
		t.Errorf("health findings belong on stdout, stderr=%q", r.stderr)
	}
	lines = outputLines(t, r.stdout)
	if len(lines) != 2 {
		t.Fatalf("want 2 lines, got %d:\n%s", len(lines), r.stdout)
	}
	assertContains(t, lines[0], "missed=14")
	assertContains(t, lines[0], "missed duties above tolerance")
	assertContains(t, lines[1], "new_missed=2")
	assertContains(t, lines[1], "tolerated_misses=1")

	// Same history without the baseline flag: cumulative 14 is judged against
	// the tolerance, there is exactly one output line and no baseline mention.
	r = health(t, dir, "--tolerated-misses", "2")
	if r.code != 0 {
		t.Fatalf("cumulative query exit=%d, want 0; stderr=%q", r.code, r.stderr)
	}
	if r.stderr != "" {
		t.Errorf("successful health query must not write stderr, got %q", r.stderr)
	}
	lines = outputLines(t, r.stdout)
	if len(lines) != 1 {
		t.Fatalf("baseless query must print 1 line, got %d:\n%s", len(lines), r.stdout)
	}
	assertContains(t, lines[0], "seq=3")
	assertContains(t, lines[0], "missed=14")
	assertContains(t, lines[0], "missed duties above tolerance")
	assertNotContains(t, r.stdout, "baseline")
	assertNotContains(t, r.stdout, "new_missed")
}

// TestCLIHealthRollbackUnknownNewKeepsOtherRules covers a cumulative-counter
// decrease (12 -> 5 -> 14) between the baseline and the latest saved record:
// the new count must render as 无法判断, the rollback finding replaces the
// tolerance alarm, and online/version rules still follow the latest heartbeat.
func TestCLIHealthRollbackUnknownNewKeepsOtherRules(t *testing.T) {
	dir := seedNode(t, rollbackHistoryJSON)

	r := health(t, dir, "--tolerated-misses", "2", "--missed-since-seq", "1")
	if r.code != 0 {
		t.Fatalf("rollback query must succeed: exit=%d stderr=%q", r.code, r.stderr)
	}
	if r.stderr != "" {
		t.Errorf("health findings belong on stdout, stderr=%q", r.stderr)
	}
	lines := outputLines(t, r.stdout)
	if len(lines) != 2 {
		t.Fatalf("want 2 lines, got %d:\n%s", len(lines), r.stdout)
	}
	// First line: real latest cumulative and baseline counts survive, and the
	// other health rules still render (fresh telemetry -> online; 0.9 vs 1.0
	// -> version skew).
	for _, sub := range []string{
		"status=online", "seq=3", "version=0.9", "missed=14",
		"version skew", "累计漏签数回退",
	} {
		assertContains(t, lines[0], sub)
	}
	assertNotContains(t, lines[0], "missed duties above tolerance")
	// Second line: unknown new count with the real endpoint counts.
	for _, sub := range []string{
		"baseline_seq=1", "baseline_missed=12",
		"new_missed=无法判断", "tolerated_misses=2",
	} {
		assertContains(t, lines[1], sub)
	}
}

// TestCLIHealthInvalidBaselineFails guarantees bad baseline input is a command
// error, not a quietly different successful health query: explicit 0 is not
// the same as omitting the flag, an unknown positive seq (including one in a
// saved-seq gap or above the latest seq) is not replaced by another
// heartbeat, and a node without telemetry cannot anchor a baseline.
func TestCLIHealthInvalidBaselineFails(t *testing.T) {
	dir := seedNode(t, gapHistoryJSON)

	cases := []struct {
		name     string
		args     []string
		errParts []string
	}{
		{
			name:     "explicit zero is not omission",
			args:     []string{"--tolerated-misses", "2", "--missed-since-seq", "0"},
			errParts: []string{"--missed-since-seq", "positive"},
		},
		{
			name:     "positive seq missing from the saved gap",
			args:     []string{"--tolerated-misses", "2", "--missed-since-seq", "2"},
			errParts: []string{"seq 2", "baseline"},
		},
		{
			name:     "baseline above the latest seq",
			args:     []string{"--tolerated-misses", "2", "--missed-since-seq", "99"},
			errParts: []string{"99", "latest"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := health(t, dir, tc.args...)
			if r.code == 0 {
				t.Fatalf("invalid baseline must exit non-zero; stdout=\n%s", r.stdout)
			}
			if r.stdout != "" {
				t.Errorf("baseline error must not print a health result on stdout, got:\n%s", r.stdout)
			}
			if !strings.HasPrefix(r.stderr, "error:") {
				t.Errorf("error must be reported via stderr with an error prefix, got %q", r.stderr)
			}
			for _, part := range tc.errParts {
				assertContains(t, r.stderr, part)
			}
		})
	}

	// Contrast: omitting the flag on the same data dir is a normal query,
	// proving explicit 0 cannot be interpreted as "no baseline".
	r := health(t, dir, "--tolerated-misses", "2")
	if r.code != 0 || r.stderr != "" {
		t.Fatalf("omitting the baseline must succeed: code=%d stderr=%q", r.code, r.stderr)
	}
	lines := outputLines(t, r.stdout)
	if len(lines) != 1 {
		t.Fatalf("baseless query must print 1 line, got %d:\n%s", len(lines), r.stdout)
	}
	assertContains(t, lines[0], "missed=14")
	assertNotContains(t, r.stdout, "baseline")

	// A baseline on a node with no telemetry at all is an error too.
	r = runEdgefleet(t, nil,
		"heartbeat", "health", "--data-dir", dir,
		"--node", "ghost", "--expected-version", "1.0",
		"--tolerated-misses", "2", "--missed-since-seq", "1",
		"--at", cliFixedTime)
	if r.code == 0 {
		t.Fatalf("baseline for a node without telemetry must exit non-zero; stdout=\n%s", r.stdout)
	}
	if r.stdout != "" {
		t.Errorf("no health output expected for an unknown node, got:\n%s", r.stdout)
	}
	assertContains(t, r.stderr, "no heartbeats")
}

// TestCLIHealthCumulativeBoundaryUnchanged pins the existing baseless rule:
// the cumulative count alarms only when it is strictly greater than the
// tolerance, and no baseline wording ever appears in that output.
func TestCLIHealthCumulativeBoundaryUnchanged(t *testing.T) {
	dir := seedNode(t, monotonicHistoryJSON)

	r := health(t, dir, "--tolerated-misses", "14")
	if r.code != 0 || r.stderr != "" {
		t.Fatalf("equality query must succeed: code=%d stderr=%q", r.code, r.stderr)
	}
	lines := outputLines(t, r.stdout)
	if len(lines) != 1 {
		t.Fatalf("baseless query must print 1 line, got %d:\n%s", len(lines), r.stdout)
	}
	assertContains(t, lines[0], "missed=14")
	assertContains(t, lines[0], "findings=[]")
	assertNotContains(t, r.stdout, "baseline")

	r = health(t, dir, "--tolerated-misses", "13")
	if r.code != 0 {
		t.Fatalf("health finding must not fail the command: exit=%d stderr=%q", r.code, r.stderr)
	}
	assertContains(t, r.stdout, "missed=14")
	assertContains(t, r.stdout, "missed duties above tolerance")
}
