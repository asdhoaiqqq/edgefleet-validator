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
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
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

// nodeFileHexPath mirrors the per-node file naming used by the store.
func nodeFileHexPath(dir, node string) string {
	return filepath.Join(dir, "nodes", hex.EncodeToString([]byte(node))+".json")
}

// corruptNodeFile overwrites one node's saved file with raw content.
func corruptNodeFile(t *testing.T, dir, node, content string) {
	t.Helper()
	if err := os.WriteFile(nodeFileHexPath(dir, node), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

// readNodeFile returns the current raw bytes of a node's file.
func readNodeFile(t *testing.T, dir, node string) string {
	t.Helper()
	b, err := os.ReadFile(nodeFileHexPath(dir, node))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// strictChecksum replicates the store's checksum over the canonical JSON of a
// single record slice, so a test can forge a checksum matching a tampered
// interpretation. canonicalJSON must be the store's marshaled record form.
func strictChecksum(canonicalJSON string) string {
	sum := sha256.Sum256([]byte(canonicalJSON))
	return hex.EncodeToString(sum[:])
}

// envelope wraps a raw records array in a well-formed store file.
func envelope(checksum, recordsJSON string) string {
	return `{"format":"edgefleet-heartbeats-v1","checksum":"` + checksum + `","records":` + recordsJSON + `}`
}

// TestCLIStoredCorruptionRefusedByQueries exercises the user-visible contract:
// a saved record with a missing, null or duplicated telemetry field corrupts
// the node's data even when the checksum matches the interpreted values; every
// query for that node fails on stderr with a non-zero exit and prints nothing
// on stdout, while other nodes keep working.
func TestCLIStoredCorruptionRefusedByQueries(t *testing.T) {
	f := newFixture(t)

	const badRecord = `{"node":"bad","seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":100,"missed":0}`
	submitBatch(t, f.dir, "["+badRecord+"]")

	cases := []struct {
		name        string
		tamper      string // raw records array stored alongside a matching checksum
		interpreted string // canonical records a lenient reader reconstructs
		wantField   string
	}{
		{
			name:        "missing missed stays matching with original checksum",
			tamper:      `[{"node":"bad","seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":100}]`,
			interpreted: `[{"node":"bad","seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":100,"missed":0}]`,
			wantField:   "missed",
		},
		{
			name:        "null height with checksum over zero-filled value",
			tamper:      `[{"node":"bad","seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":null,"missed":0}]`,
			interpreted: `[{"node":"bad","seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":0,"missed":0}]`,
			wantField:   "height",
		},
		{
			name:        "duplicate missed same value, last-wins interpretation matches",
			tamper:      `[{"node":"bad","seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":100,"missed":0,"missed":0}]`,
			interpreted: `[{"node":"bad","seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":100,"missed":0}]`,
			wantField:   "missed",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// The forged checksum hashes the canonical record a lenient
			// reader would reconstruct (zero-filled / last value).
			corruptNodeFile(t, f.dir, "bad", envelope(strictChecksum(tc.interpreted), tc.tamper))

			// history: non-zero exit, explicit reason on stderr naming the
			// file/record/field, no normal output on stdout.
			out, errOut, code := runCLI(t, f.dir, "",
				"heartbeat", "history", "--data-dir", f.dir, "--node", "bad")
			if code == 0 {
				t.Errorf("history on corrupt node must exit non-zero; stdout=%q", out)
			}
			if out != "" {
				t.Errorf("history must print no results on corruption, stdout=%q", out)
			}
			if !strings.Contains(errOut, "error:") || !strings.Contains(errOut, "record 1") ||
				!strings.Contains(errOut, tc.wantField) {
				t.Errorf("history stderr must explain the corrupt record/field, got %q", errOut)
			}
			if !strings.Contains(errOut, filepath.Base(nodeFileHexPath(f.dir, "bad"))) {
				t.Errorf("history stderr should identify the corrupt file, got %q", errOut)
			}

			// health: same contract.
			out, errOut, code = health(t, f,
				"--node", "bad", "--expected-version", "1.26.0", "--tolerated-misses", "0")
			if code == 0 {
				t.Errorf("health on corrupt node must exit non-zero; stdout=%q", out)
			}
			if out != "" {
				t.Errorf("health must print no results on corruption, stdout=%q", out)
			}
			if !strings.Contains(errOut, "record 1") || !strings.Contains(errOut, tc.wantField) {
				t.Errorf("health stderr must explain the corrupt record/field, got %q", errOut)
			}

			// health with a missed-duty baseline must fail identically rather
			// than skipping the bad record.
			out, errOut, code = health(t, f,
				"--node", "bad", "--expected-version", "1.26.0",
				"--tolerated-misses", "0", "--missed-since-seq", "1")
			if code == 0 {
				t.Errorf("baseline health on corrupt node must exit non-zero; stdout=%q", out)
			}
			if out != "" {
				t.Errorf("baseline health must print no results on corruption, stdout=%q", out)
			}
			if !strings.Contains(errOut, tc.wantField) {
				t.Errorf("baseline health stderr must name the field, got %q", errOut)
			}
		})
	}
}

// TestCLISubmitRejectsBatchTouchingCorruptNode ensures a batch is refused
// before any new record is saved: the corrupt file stays byte-for-byte
// intact, other files in the batch are not written, and no new node file is
// created. Submitting only to healthy nodes continues to work.
func TestCLISubmitRejectsBatchTouchingCorruptNode(t *testing.T) {
	f := newFixture(t)

	const canonical = `[{"node":"bad","seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":100,"missed":0}]`
	submitBatch(t, f.dir, "["+strings.Trim(canonical, "[]")+"]")
	// Delete "missed" from the genuine record (its value was 0); keep the
	// original checksum, which still matches the zero-filled interpretation.
	corruptNodeFile(t, f.dir, "bad", envelope(strictChecksum(canonical),
		`[{"node":"bad","seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":100}]`))

	badBefore := readNodeFile(t, f.dir, "bad")
	monoBefore := readNodeFile(t, f.dir, "mono")

	batch := `[
	  {"node":"bad","seq":2,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":102,"missed":0},
	  {"node":"mono","seq":4,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":104,"missed":15},
	  {"node":"brandnew","seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":1,"missed":0}
	]`
	out, errOut, code := runCLI(t, f.dir, batch,
		"heartbeat", "submit", "--data-dir", f.dir, "--receive-time", cliReceiveAt)
	if code == 0 {
		t.Errorf("submit touching a corrupt node must fail non-zero; stdout=%q stderr=%q", out, errOut)
	}
	if strings.Contains(out, "submitted") {
		t.Errorf("rejected batch must not report success, stdout=%q", out)
	}
	if !strings.Contains(errOut, "corruption") || !strings.Contains(errOut, "missed") {
		t.Errorf("submit stderr must explain the corruption, got %q", errOut)
	}

	if got := readNodeFile(t, f.dir, "bad"); got != badBefore {
		t.Errorf("corrupt node file was modified during rejected submit")
	}
	if got := readNodeFile(t, f.dir, "mono"); got != monoBefore {
		t.Errorf("healthy node file was rewritten during rejected submit")
	}
	if _, err := os.Stat(nodeFileHexPath(f.dir, "brandnew")); !os.IsNotExist(err) {
		t.Errorf("brand-new node file must not be created, stat err=%v", err)
	}

	// Healthy nodes are unaffected and remain fully usable.
	out, _, code = health(t, f,
		"--node", "mono", "--expected-version", "1.26.0", "--tolerated-misses", "9")
	if code != 0 || !strings.Contains(out, "node=mono") {
		t.Errorf("healthy node must still query normally: code=%d out=%q", code, out)
	}
	submitBatch(t, f.dir, `[{"node":"gappy","seq":4,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":304,"missed":4}]`)
}

// TestCLIMisownedNodeFileIsRefused plants one node's complete, checksum-valid
// file under another node's name and checks the command line refuses to serve
// it as the wrong node's telemetry: health (plain and with a baseline),
// history and submit all fail non-zero with the reason on stderr, while the
// real owner keeps working.
func TestCLIMisownedNodeFileIsRefused(t *testing.T) {
	f := newFixture(t)

	// Copy mono's intact file over gappy's. Format and checksum stay valid;
	// only the ownership is wrong.
	corruptNodeFile(t, f.dir, "gappy", readNodeFile(t, f.dir, "mono"))
	misownedPath := nodeFileHexPath(f.dir, "gappy")

	for _, tc := range []struct {
		name string
		args []string
	}{
		{"health", []string{"--node", "gappy", "--expected-version", "1.26.0", "--tolerated-misses", "9"}},
		{"health with baseline", []string{"--node", "gappy", "--expected-version", "1.26.0", "--tolerated-misses", "9", "--missed-since-seq", "1"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, errOut, code := health(t, f, tc.args...)
			if code == 0 {
				t.Fatalf("misowned file must fail the query, exit=0 stdout=%q", out)
			}
			if out != "" {
				t.Errorf("no health result may be printed, stdout=%q", out)
			}
			for _, want := range []string{misownedPath, "record 1", `"gappy"`, `"mono"`} {
				if !strings.Contains(errOut, want) {
					t.Errorf("stderr must contain %s, got %q", want, errOut)
				}
			}
		})
	}

	out, errOut, code := runCLI(t, f.dir, "",
		"heartbeat", "history", "--data-dir", f.dir, "--node", "gappy")
	if code == 0 {
		t.Fatalf("history of a misowned file must fail, exit=0 stdout=%q", out)
	}
	if out != "" {
		t.Errorf("no history may be printed, stdout=%q", out)
	}
	if !strings.Contains(errOut, misownedPath) || !strings.Contains(errOut, `"mono"`) {
		t.Errorf("history stderr must name the file and the foreign node, got %q", errOut)
	}

	// A submit batch touching the misowned node is refused as a whole: no
	// success line, no counts, and the first-time node in the batch is not
	// created either.
	batch := `[
	  {"node":"gappy","seq":9,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":999,"missed":0},
	  {"node":"brandnew","seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":1,"missed":0}
	]`
	out, errOut, code = runCLI(t, f.dir, batch,
		"heartbeat", "submit", "--data-dir", f.dir, "--receive-time", cliReceiveAt)
	if code == 0 {
		t.Errorf("submit touching a misowned node must fail non-zero; stdout=%q", out)
	}
	if strings.Contains(out, "submitted") {
		t.Errorf("rejected batch must not print success or counts, stdout=%q", out)
	}
	if !strings.Contains(errOut, misownedPath) {
		t.Errorf("submit stderr must name the misowned file, got %q", errOut)
	}
	if _, err := os.Stat(nodeFileHexPath(f.dir, "brandnew")); !os.IsNotExist(err) {
		t.Errorf("brand-new node file must not be created, stat err=%v", err)
	}

	// The real owner of the copied records is unaffected.
	out, _, code = health(t, f,
		"--node", "mono", "--expected-version", "1.26.0", "--tolerated-misses", "9")
	if code != 0 || !strings.Contains(out, "node=mono") {
		t.Errorf("real owner must still query normally: code=%d out=%q", code, out)
	}
}

// TestCLISubmitRejectsInvalidUTF8Node exercises the user-visible contract for
// a node id that is not losslessly representable text: the JSON input holds
// a raw invalid UTF-8 byte, which a lenient reader would silently rewrite to
// "�" and merge into another node. The whole batch must be refused with a
// non-zero exit, no success counts on stdout, an error naming the record and
// the node field on stderr, and no change to any stored data.
func TestCLISubmitRejectsInvalidUTF8Node(t *testing.T) {
	f := newFixture(t)
	monoBefore := readNodeFile(t, f.dir, "mono")

	badRecord := `{"node":"val-` + "\xff" + `1","seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":1,"missed":0}`
	goodRecord := `{"node":"mono","seq":4,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":104,"missed":15}`
	out, errOut, code := runCLI(t, f.dir, "["+goodRecord+","+badRecord+"]",
		"heartbeat", "submit", "--data-dir", f.dir, "--receive-time", cliReceiveAt)
	if code == 0 {
		t.Errorf("submit with an invalid UTF-8 node id must exit non-zero; stdout=%q", out)
	}
	if strings.Contains(out, "submitted") {
		t.Errorf("rejected batch must not print success counts, stdout=%q", out)
	}
	if !strings.Contains(errOut, "record 2") || !strings.Contains(errOut, "node") {
		t.Errorf("stderr must name the failing record and the node field, got %q", errOut)
	}

	// The valid record in the same batch is not saved either.
	if got := readNodeFile(t, f.dir, "mono"); got != monoBefore {
		t.Errorf("existing node file changed during rejected submit")
	}
	if _, err := os.Stat(nodeFileHexPath(f.dir, "val-�1")); !os.IsNotExist(err) {
		t.Errorf("no node file may be created for the rewritten id, stat err=%v", err)
	}

	// Health answers for the existing node are unchanged.
	out, _, code = health(t, f,
		"--node", "mono", "--expected-version", "1.26.0", "--tolerated-misses", "9")
	if code != 0 || !strings.Contains(out, "seq=3") {
		t.Errorf("health after rejected batch changed: code=%d out=%q", code, out)
	}
}

// TestCLISubmitRejectsUnpairedSurrogate covers the other silent rewrite: a
// \u escape that is an unpaired surrogate decodes to "�" instead of the
// intended character. The batch is refused and no node file is created.
func TestCLISubmitRejectsUnpairedSurrogate(t *testing.T) {
	dir := t.TempDir()
	batch := `[{"node":"val-\uD83D","seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":1,"missed":0}]`
	out, errOut, code := runCLI(t, dir, batch,
		"heartbeat", "submit", "--data-dir", dir, "--receive-time", cliReceiveAt)
	if code == 0 {
		t.Errorf("submit with an unpaired surrogate must exit non-zero; stdout=%q", out)
	}
	if !strings.Contains(errOut, "record 1") || !strings.Contains(errOut, "node") {
		t.Errorf("stderr must name the failing record and the node field, got %q", errOut)
	}
	// The batch is refused before the store is touched: no node file exists.
	files, err := os.ReadDir(filepath.Join(dir, "nodes"))
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if len(files) != 0 {
		t.Errorf("no node file may be created, found %d", len(files))
	}
}

// TestCLISubmitValidUnicodeNodeRoundTrip checks the legal cases keep working
// end to end: a literal "�" is a valid character the user typed, an emoji
// written as a surrogate-pair escape is the same node as its literal form,
// and both are queryable by their original id.
func TestCLISubmitValidUnicodeNodeRoundTrip(t *testing.T) {
	dir := t.TempDir()

	// Literal replacement character: a legal id, accepted as-is.
	out, _, code := runCLI(t, dir,
		`[{"node":"val-�","seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":1,"missed":0}]`,
		"heartbeat", "submit", "--data-dir", dir, "--receive-time", cliReceiveAt)
	if code != 0 || !strings.Contains(out, "new=1") {
		t.Fatalf("literal replacement char must be accepted: code=%d out=%q", code, out)
	}

	// The same text as a � escape is the same node: duplicate, not new.
	out, _, code = runCLI(t, dir,
		`[{"node":"val-\uFFFD","seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":1,"missed":0}]`,
		"heartbeat", "submit", "--data-dir", dir, "--receive-time", cliReceiveAt)
	if code != 0 || !strings.Contains(out, "new=0 duplicate=1") {
		t.Errorf("escaped form of the same id must be a duplicate: code=%d out=%q", code, out)
	}

	// An emoji written as a surrogate-pair escape is stored under the emoji.
	out, _, code = runCLI(t, dir,
		`[{"node":"val-\uD83D\uDE00","seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":2,"missed":0}]`,
		"heartbeat", "submit", "--data-dir", dir, "--receive-time", cliReceiveAt)
	if code != 0 || !strings.Contains(out, "new=1") {
		t.Fatalf("surrogate-pair escape must be accepted: code=%d out=%q", code, out)
	}

	// Both nodes are queryable by their original ids.
	out, _, code = runCLI(t, dir, "",
		"heartbeat", "history", "--data-dir", dir, "--node", "val-�")
	if code != 0 || !strings.Contains(out, "node=val-� seq=1") {
		t.Errorf("history for literal replacement char id: code=%d out=%q", code, out)
	}
	out, _, code = runCLI(t, dir, "",
		"heartbeat", "history", "--data-dir", dir, "--node", "val-😀")
	if code != 0 || !strings.Contains(out, "node=val-😀 seq=1") {
		t.Errorf("history for emoji id: code=%d out=%q", code, out)
	}
}

// TestCLISubmitRejectsInvalidVersionText exercises the user-visible
// contract for version text at the submit entry point: an invalid UTF-8
// byte or an unpaired surrogate escape in version must fail non-zero,
// print no success counts, and name the record position and the version
// field on stderr; the valid record in the same batch is not saved.
func TestCLISubmitRejectsInvalidVersionText(t *testing.T) {
	f := newFixture(t)
	monoBefore := readNodeFile(t, f.dir, "mono")

	cases := []struct {
		name       string
		versionRaw string
	}{
		{"invalid utf-8 bytes", `"1.26.0-` + "\xff" + `"`},
		{"unpaired high surrogate", `"1.26.0-` + `\uD83D` + `"`},
		{"unpaired low surrogate", `"1.26.0-` + `\uDE00` + `"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			badRecord := `{"node":"valbad","seq":1,"collected_at":"2026-10-01T11:59:59Z","version":` +
				tc.versionRaw + `,"height":1,"missed":0}`
			goodRecord := `{"node":"mono","seq":4,"collected_at":"2026-10-01T11:59:59Z","version":"1.26.0","height":104,"missed":15}`
			out, errOut, code := runCLI(t, f.dir, "["+goodRecord+","+badRecord+"]",
				"heartbeat", "submit", "--data-dir", f.dir, "--receive-time", cliReceiveAt)
			if code == 0 {
				t.Errorf("submit with invalid version text must exit non-zero; stdout=%q", out)
			}
			if strings.Contains(out, "submitted") {
				t.Errorf("rejected batch must not print success counts, stdout=%q", out)
			}
			if !strings.Contains(errOut, "record 2") || !strings.Contains(errOut, "version") {
				t.Errorf("stderr must name the failing record (record 2) and the version field, got %q", errOut)
			}

			// The valid record in the same batch is not saved either.
			if got := readNodeFile(t, f.dir, "mono"); got != monoBefore {
				t.Errorf("existing node file changed during rejected submit")
			}
			if _, err := os.Stat(nodeFileHexPath(f.dir, "valbad")); !os.IsNotExist(err) {
				t.Errorf("no node file may be created for the invalid batch, stat err=%v", err)
			}
		})
	}

	// Health answers for the existing node are unchanged.
	out, _, code := health(t, f,
		"--node", "mono", "--expected-version", "1.26.0", "--tolerated-misses", "9")
	if code != 0 || !strings.Contains(out, "seq=3") {
		t.Errorf("health after rejected batches changed: code=%d out=%q", code, out)
	}
}

// TestCLISubmitValidUnicodeVersionRoundTrip checks the legal cases keep
// working end to end: Chinese and emoji version text, a literal
// replacement character, an emoji written as a surrogate pair counting
// as the same version as its literal form, and a version containing a
// real backslash which is never re-interpreted as an escape. No numeric
// version format is imposed; health keeps comparing versions exactly.
func TestCLISubmitValidUnicodeVersionRoundTrip(t *testing.T) {
	dir := t.TempDir()

	// Chinese version accepted and queried back verbatim.
	out, _, code := submit(t, dir,
		`[{"node":"甲","seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"版本 1.0","height":1,"missed":0}]`)
	if code != 0 || !strings.Contains(out, "new=1") {
		t.Fatalf("Chinese version must be accepted: code=%d out=%q", code, out)
	}
	out, _, code = runCLI(t, dir, "",
		"heartbeat", "history", "--data-dir", dir, "--node", "甲")
	if code != 0 || !strings.Contains(out, "version=版本 1.0") {
		t.Errorf("Chinese version must round-trip exactly: code=%d out=%q", code, out)
	}

	// Emoji as a surrogate pair is a new record; the literal emoji is
	// the same version and counts as a duplicate.
	out, _, code = submit(t, dir,
		`[{"node":"e","seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"v-😀","height":2,"missed":0}]`)
	if code != 0 || !strings.Contains(out, "new=1") {
		t.Fatalf("surrogate-pair emoji version must be accepted: code=%d out=%q", code, out)
	}
	out, _, code = submit(t, dir,
		`[{"node":"e","seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"v-😀","height":2,"missed":0}]`)
	if code != 0 || !strings.Contains(out, "new=0 duplicate=1") {
		t.Errorf("literal emoji must be the same version: code=%d out=%q", code, out)
	}

	// A literal replacement character is genuine text the user typed.
	out, _, code = submit(t, dir,
		`[{"node":"r","seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"1.0-�","height":1,"missed":0}]`)
	if code != 0 || !strings.Contains(out, "new=1") {
		t.Fatalf("literal replacement char must be accepted: code=%d out=%q", code, out)
	}

	// A version containing a real backslash is stored literally and not
	// decoded a second time: health expects the literal text exactly.
	out, _, code = submit(t, dir,
		`[{"node":"b","seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"1.0\\u0062eta","height":1,"missed":0}]`)
	if code != 0 || !strings.Contains(out, "new=1") {
		t.Fatalf("literal-backslash version must be accepted: code=%d out=%q", code, out)
	}
	out, _, code = runCLI(t, dir, "",
		"heartbeat", "health", "--data-dir", dir, "--at", cliQueryAt,
		"--node", "b", "--expected-version", "1.0\\u0062eta", "--tolerated-misses", "0")
	if code != 0 || strings.Contains(out, "version skew") {
		t.Errorf("literal-backslash version must compare exactly: code=%d out=%q", code, out)
	}
}

// submit runs one submit command and returns its full result for inspection.
func submit(t *testing.T, dir, json string) (string, string, int) {
	t.Helper()
	return runCLI(t, dir, json,
		"heartbeat", "submit", "--data-dir", dir, "--receive-time", cliReceiveAt)
}

// history runs the history command for one node.
func historyCLI(t *testing.T, dir, node string) (string, string, int) {
	t.Helper()
	return runCLI(t, dir, "",
		"heartbeat", "history", "--data-dir", dir, "--node", node)
}

// historyLines returns the non-empty history lines for a node.
func historyLines(t *testing.T, dir, node string) []string {
	t.Helper()
	out, errOut, code := historyCLI(t, dir, node)
	if code != 0 {
		t.Fatalf("history for %q exited %d: stderr=%q", node, code, errOut)
	}
	return outputLines(out)
}

// TestCLISubmitMixedBatchCountsNewAndDuplicate drives the example rule end to
// end: a batch may mix records already saved, exact repeats within the same
// batch, and new records for other nodes. new counts only records genuinely
// added this time; duplicate counts every input record that added nothing.
// The same seq used by different nodes is never deduplicated across nodes.
func TestCLISubmitMixedBatchCountsNewAndDuplicate(t *testing.T) {
	dir := t.TempDir()

	// 甲 already has seq 1 saved.
	submitBatch(t, dir, `[
	  {"node":"甲","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.26.0","height":100,"missed":0}
	]`)

	// Batch: 甲 seq1 again verbatim (duplicate against history), 甲 seq2 twice
	// identical (one new, one duplicate within the batch), 乙 seq2 (new). The
	// two nodes share seq 2 but stay distinct records.
	out, errOut, code := submit(t, dir, `[
	  {"node":"甲","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.26.0","height":100,"missed":0},
	  {"node":"甲","seq":2,"collected_at":"2026-10-01T11:59:30Z","version":"1.26.0","height":101,"missed":0},
	  {"node":"甲","seq":2,"collected_at":"2026-10-01T11:59:30Z","version":"1.26.0","height":101,"missed":0},
	  {"node":"乙","seq":2,"collected_at":"2026-10-01T11:59:30Z","version":"1.26.0","height":201,"missed":2}
	]`)
	if code != 0 {
		t.Fatalf("mixed batch must succeed, exit=%d stderr=%q", code, errOut)
	}
	if errOut != "" {
		t.Errorf("a successful submit must leave stderr empty, got %q", errOut)
	}
	if strings.TrimRight(out, "\n") != "submitted: new=2 duplicate=2" {
		t.Errorf("stdout = %q, want submitted: new=2 duplicate=2", out)
	}

	// 甲 history: exactly seq 1 and 2, ascending, original seq-1 telemetry
	// untouched and seq-2 content as submitted.
	jia := historyLines(t, dir, "甲")
	if len(jia) != 2 {
		t.Fatalf("甲 history = %q, want exactly two records", jia)
	}
	want1 := "node=甲 seq=1 collected_at=2026-10-01T11:59:00Z version=1.26.0 height=100 missed=0"
	want2 := "node=甲 seq=2 collected_at=2026-10-01T11:59:30Z version=1.26.0 height=101 missed=0"
	if jia[0] != want1 {
		t.Errorf("甲 first line = %q, want %q", jia[0], want1)
	}
	if jia[1] != want2 {
		t.Errorf("甲 second line = %q, want %q", jia[1], want2)
	}

	// 乙 has only its own seq 2; same seq number as 甲 did not dedup.
	yi := historyLines(t, dir, "乙")
	if len(yi) != 1 {
		t.Fatalf("乙 history = %q, want exactly one record", yi)
	}
	wantYi := "node=乙 seq=2 collected_at=2026-10-01T11:59:30Z version=1.26.0 height=201 missed=2"
	if yi[0] != wantYi {
		t.Errorf("乙 line = %q, want %q", yi[0], wantYi)
	}
}

// TestCLISubmitSameInstantDifferentOffsetIsDuplicate pins the time semantics
// of "identical content": the same instant written with another timezone
// offset is a duplicate, both within a batch and against saved history.
func TestCLISubmitSameInstantDifferentOffsetIsDuplicate(t *testing.T) {
	dir := t.TempDir()

	// 11:00:00Z is the same instant as 19:00:00+08:00.
	out, errOut, code := submit(t, dir, `[
	  {"node":"n1","seq":1,"collected_at":"2026-10-01T11:00:00Z","version":"1.0","height":10,"missed":0},
	  {"node":"n1","seq":1,"collected_at":"2026-10-01T19:00:00+08:00","version":"1.0","height":10,"missed":0}
	]`)
	if code != 0 {
		t.Fatalf("same-instant pair inside one batch must be accepted: exit=%d stderr=%q", code, errOut)
	}
	if strings.TrimRight(out, "\n") != "submitted: new=1 duplicate=1" {
		t.Errorf("in-batch timezone-equal record: stdout=%q, want new=1 duplicate=1", out)
	}

	// Against saved history, the offset-spelled repeat is still a duplicate.
	out, errOut, code = submit(t, dir, `[
	  {"node":"n1","seq":1,"collected_at":"2026-10-01T07:00:00-04:00","version":"1.0","height":10,"missed":0}
	]`)
	if code != 0 {
		t.Fatalf("same-instant resubmit must succeed: exit=%d stderr=%q", code, errOut)
	}
	if errOut != "" {
		t.Errorf("duplicate resubmit must leave stderr empty, got %q", errOut)
	}
	if strings.TrimRight(out, "\n") != "submitted: new=0 duplicate=1" {
		t.Errorf("stdout=%q, want new=0 duplicate=1", out)
	}

	lines := historyLines(t, dir, "n1")
	if len(lines) != 1 {
		t.Fatalf("history = %q, want the one record with no extra copy", lines)
	}
}

// TestCLISubmitContentDifferencesConflictAgainstHistory covers every field
// that makes a same-node/same-seq record a conflict rather than a duplicate:
// version, height, cumulative missed, and the collection instant (including
// equal wall-clock text with a different offset, which is a different
// instant). Nothing is overwritten and no last-write-wins occurs.
func TestCLISubmitContentDifferencesConflictAgainstHistory(t *testing.T) {
	const base = `{"node":"%s","seq":2,"collected_at":"2026-10-01T11:59:00Z","version":"1.26.0","height":100,"missed":0}`
	cases := []struct {
		name string
		// incoming is the conflicting record, same node+seq as the saved one.
		incoming string
	}{
		{"version differs",
			`{"node":"%s","seq":2,"collected_at":"2026-10-01T11:59:00Z","version":"1.27.0","height":100,"missed":0}`},
		{"height differs",
			`{"node":"%s","seq":2,"collected_at":"2026-10-01T11:59:00Z","version":"1.26.0","height":101,"missed":0}`},
		{"missed differs",
			`{"node":"%s","seq":2,"collected_at":"2026-10-01T11:59:00Z","version":"1.26.0","height":100,"missed":1}`},
		{"collected instant differs",
			`{"node":"%s","seq":2,"collected_at":"2026-10-01T11:58:00Z","version":"1.26.0","height":100,"missed":0}`},
		{"same wall clock text, different offset is a different instant",
			`{"node":"%s","seq":2,"collected_at":"2026-10-01T11:59:00+08:00","version":"1.26.0","height":100,"missed":0}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			// Each subtest gets its own data directory, so one fixed node id
			// carrying the saved seq-2 baseline is enough.
			const node = "node"
			submitBatch(t, dir, "["+fmt.Sprintf(base, node)+"]")
			before := historyLines(t, dir, node)

			out, errOut, code := submit(t, dir, "["+fmt.Sprintf(tc.incoming, node)+"]")
			if code == 0 {
				t.Fatalf("conflicting record must fail non-zero; stdout=%q", out)
			}
			// The error names the node and the seq, and goes to stderr.
			for _, want := range []string{"error:", "conflict", node, "seq 2"} {
				if !strings.Contains(errOut, want) {
					t.Errorf("stderr %q must contain %q", errOut, want)
				}
			}
			// No success line or counters on stdout.
			if strings.Contains(out, "submitted") || strings.Contains(out, "new=") {
				t.Errorf("rejected batch must not print success counts, stdout=%q", out)
			}
			// The original telemetry is intact: not replaced by the incoming
			// content, not counted as a duplicate.
			after := historyLines(t, dir, node)
			if len(after) != 1 || after[0] != before[0] {
				t.Errorf("history changed after conflict:\nbefore=%q\nafter =%q", before, after)
			}
		})
	}
}

// TestCLISubmitConflictWithinBatchRejectsEverything checks the first conflict
// condition: two records of the same node+seq with differing content inside
// one batch fail the whole batch. Legal records for other nodes — whether
// they appear before or after the conflicting pair — must not survive, and a
// pre-existing node touched by a legal record in the same batch stays as it
// was before the submit.
func TestCLISubmitConflictWithinBatchRejectsEverything(t *testing.T) {
	dir := t.TempDir()
	// Existing node 旧 with seq 1 already saved.
	submitBatch(t, dir, `[
	  {"node":"旧","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.0","height":10,"missed":0}
	]`)
	oldBefore := readNodeFile(t, dir, "旧")

	// Batch layout: legal record for a brand-new node 新, then a legal seq-2
	// for the existing node, then the in-batch conflict on 甲 seq 1, then
	// another legal new-node record. None of the legal ones may be saved.
	batch := `[
	  {"node":"新","seq":1,"collected_at":"2026-10-01T11:59:10Z","version":"1.0","height":20,"missed":0},
	  {"node":"旧","seq":2,"collected_at":"2026-10-01T11:59:20Z","version":"1.0","height":11,"missed":0},
	  {"node":"甲","seq":1,"collected_at":"2026-10-01T11:59:30Z","version":"1.0","height":100,"missed":0},
	  {"node":"甲","seq":1,"collected_at":"2026-10-01T11:59:30Z","version":"2.0","height":100,"missed":0},
	  {"node":"丙","seq":1,"collected_at":"2026-10-01T11:59:40Z","version":"1.0","height":30,"missed":0}
	]`
	out, errOut, code := submit(t, dir, batch)
	if code == 0 {
		t.Fatalf("in-batch conflict must fail non-zero; stdout=%q", out)
	}
	for _, want := range []string{"error:", "conflict", "甲", "seq 1"} {
		if !strings.Contains(errOut, want) {
			t.Errorf("stderr %q must contain %q", errOut, want)
		}
	}
	if strings.Contains(out, "submitted") || strings.Contains(out, "new=") {
		t.Errorf("rejected batch must print no success counts, stdout=%q", out)
	}

	// Existing node untouched even though its legal record came first.
	if got := readNodeFile(t, dir, "旧"); got != oldBefore {
		t.Errorf("existing node file was modified by the rejected batch")
	}
	// Brand-new nodes in the batch must have no history at all.
	for _, node := range []string{"新", "甲", "丙"} {
		out, errOut, code := historyCLI(t, dir, node)
		if code != 0 {
			t.Fatalf("history query after rejected batch must itself work for %q: %d %q", node, code, errOut)
		}
		if strings.TrimRight(out, "\n") != "node="+node+" no heartbeats" {
			t.Errorf("node %q must have no records after rejected batch, got %q", node, out)
		}
		if _, err := os.Stat(nodeFileHexPath(dir, node)); !os.IsNotExist(err) {
			t.Errorf("no file may be created for node %q, stat err=%v", node, err)
		}
	}
}

// TestCLISubmitConflictWithSavedHistoryRejectsWholeBatch checks the second
// conflict condition: a batch record whose node+seq already exists with
// different content fails the batch, including legal records placed before
// it. After the failure the store keeps accepting ordinary batches, and all
// previously saved content stays queryable.
func TestCLISubmitConflictWithSavedHistoryRejectsWholeBatch(t *testing.T) {
	dir := t.TempDir()
	// 甲 already saved seq 1 with specific telemetry.
	submitBatch(t, dir, `[
	  {"node":"甲","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.26.0","height":100,"missed":0}
	]`)
	jiaBefore := readNodeFile(t, dir, "甲")

	// Legal record for a new node FIRST, then the conflict against saved
	// history, then another legal new-node record.
	batch := `[
	  {"node":"乙","seq":1,"collected_at":"2026-10-01T11:59:10Z","version":"1.0","height":200,"missed":1},
	  {"node":"甲","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"9.9.9","height":100,"missed":0},
	  {"node":"丙","seq":1,"collected_at":"2026-10-01T11:59:20Z","version":"1.0","height":300,"missed":2}
	]`
	out, errOut, code := submit(t, dir, batch)
	if code == 0 {
		t.Fatalf("history conflict must fail non-zero; stdout=%q", out)
	}
	for _, want := range []string{"error:", "conflict", "甲", "seq 1"} {
		if !strings.Contains(errOut, want) {
			t.Errorf("stderr %q must contain %q", errOut, want)
		}
	}
	if strings.Contains(out, "submitted") || strings.Contains(out, "new=") {
		t.Errorf("rejected batch must print no success counts, stdout=%q", out)
	}

	// 甲 history is byte-identical to before the attempt.
	if got := readNodeFile(t, dir, "甲"); got != jiaBefore {
		t.Errorf("saved node file was modified by the conflicting batch")
	}
	// The legal new-node records must not have landed.
	for _, node := range []string{"乙", "丙"} {
		out, _, _ := historyCLI(t, dir, node)
		if strings.TrimRight(out, "\n") != "node="+node+" no heartbeats" {
			t.Errorf("node %q must have no records after rejected batch, got %q", node, out)
		}
		if _, err := os.Stat(nodeFileHexPath(dir, node)); !os.IsNotExist(err) {
			t.Errorf("no file may be created for node %q, stat err=%v", node, err)
		}
	}

	// After the conflict failure, a non-conflicting batch succeeds normally,
	// including a genuine new seq for 甲 and the previously rejected node.
	out, errOut, code = submit(t, dir, `[
	  {"node":"甲","seq":2,"collected_at":"2026-10-01T11:59:40Z","version":"1.26.0","height":102,"missed":0},
	  {"node":"乙","seq":1,"collected_at":"2026-10-01T11:59:10Z","version":"1.0","height":200,"missed":1}
	]`)
	if code != 0 {
		t.Fatalf("legal batch after a conflict failure must still succeed: exit=%d stderr=%q", code, errOut)
	}
	if strings.TrimRight(out, "\n") != "submitted: new=2 duplicate=0" {
		t.Errorf("recovery submit stdout=%q, want new=2 duplicate=0", out)
	}

	// 甲 keeps the original seq-1 content (not the conflict's) and gains the
	// new seq 2, ascending.
	jia := historyLines(t, dir, "甲")
	if len(jia) != 2 {
		t.Fatalf("甲 history = %q, want seq 1 and 2", jia)
	}
	if jia[0] != "node=甲 seq=1 collected_at=2026-10-01T11:59:00Z version=1.26.0 height=100 missed=0" {
		t.Errorf("original 甲 seq 1 telemetry altered: %q", jia[0])
	}
	if jia[1] != "node=甲 seq=2 collected_at=2026-10-01T11:59:40Z version=1.26.0 height=102 missed=0" {
		t.Errorf("new 甲 seq 2 line = %q", jia[1])
	}
}
