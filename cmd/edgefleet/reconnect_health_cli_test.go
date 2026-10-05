package main

// Command-line regression tests for health queries over disconnected and
// replayed telemetry. The domain rules have focused tests in package
// edgefleet; these tests pin what a user actually sees through the real
// `edgefleet heartbeat health` entry point:
//
//   - when a node stores a stale greater-seq record and a fresh smaller-seq
//     record that arrived in either order, the health line must be built
//     entirely from the greatest seq — offline, with that record's time,
//     version, height and cumulative missed — never spliced with the fresh
//     record and never flipped online by its newer collection time;
//   - a late backfill or a verbatim duplicate of the smaller seq submitted at
//     an even later receive time changes neither the result nor the saved
//     file;
//   - the online edge is exact at nanosecond precision (60s online, 60s+1ns
//     offline) while the terminal keeps printing collection time as whole
//     seconds, and timezone re-spellings of the same instants agree;
//   - a missed-since baseline changes only the alarm quantity: the first line
//     still shows the greatest-seq record and its cumulative count, and the
//     baseline never decides online status;
//   - a query time earlier than the greatest-seq record's collection time is
//     an error (non-zero exit, reason on stderr, nothing on stdout) for both
//     plain and baseline queries, even though a smaller-seq record had already
//     been collected by then; offline itself is a successful query that
//     exits zero;
//   - no query, failed query or duplicate submission rewrites the saved
//     heartbeats.

import (
	"os"
	"strings"
	"testing"
)

const (
	// The disordered scenario uses a fixed query instant (same as the other
	// CLI tests) and two records:
	//
	//	seq 10 collected 11:58:00 (2 minutes old, offline), version 1.25.0,
	//	        height 1000, cumulative missed 5;
	//	seq  5 collected 11:59:50 (10 seconds old, fresh), version 1.26.0,
	//	        height 900, cumulative missed 2.
	reconnectNode         = "backfill"
	reconnectLatestLine   = "node=backfill status=offline seq=10 collected_at=2026-10-01T11:58:00Z version=1.25.0 height=1000 missed=5 findings=[offline version skew: 1.25.0 != 1.26.0 missed duties above tolerance]"
	reconnectStaleReceive = "2026-10-01T11:58:30Z"
)

const reconnectStaleRecord = `{"node":"backfill","seq":10,"collected_at":"2026-10-01T11:58:00Z","version":"1.25.0","height":1000,"missed":5}`
const reconnectFreshRecord = `{"node":"backfill","seq":5,"collected_at":"2026-10-01T11:59:50Z","version":"1.26.0","height":900,"missed":2}`

// submitWithReceive runs one submit command with an explicit receive time.
func submitWithReceive(t *testing.T, dir, json, receiveAt string) {
	t.Helper()
	out, errOut, code := runCLI(t, dir, json,
		"heartbeat", "submit", "--data-dir", dir, "--receive-time", receiveAt)
	if code != 0 {
		t.Fatalf("submit at %s exited %d: stderr=%q stdout=%q", receiveAt, code, errOut, out)
	}
}

// healthQueryAt runs a health command with an explicit query instant.
func healthQueryAt(t *testing.T, dir, at string, args ...string) (string, string, int) {
	t.Helper()
	full := append([]string{"heartbeat", "health", "--data-dir", dir, "--at", at}, args...)
	return runCLI(t, dir, "", full...)
}

// seedReconnectStored stores the disordered pair in the "natural outage"
// arrival order: the stale seq-10 record was received during the outage
// (90 seconds after it was collected), and the fresh seq-5 record is only
// backfilled when the node reconnects at the query instant.
func seedReconnectStored(t *testing.T, dir string) {
	t.Helper()
	submitWithReceive(t, dir, "["+reconnectStaleRecord+"]", reconnectStaleReceive)
	submitWithReceive(t, dir, "["+reconnectFreshRecord+"]", cliReceiveAt)
}

// seedReconnectReversed stores the same pair with the stale greatest-seq
// record physically arriving after the fresh smaller-seq record.
func seedReconnectReversed(t *testing.T, dir string) {
	t.Helper()
	submitWithReceive(t, dir, "["+reconnectFreshRecord+"]", cliReceiveAt)
	submitWithReceive(t, dir, "["+reconnectStaleRecord+"]", cliReceiveAt)
}

// assertReconnectHealthLine checks the single output line is the exact one
// produced from the seq-10 record alone. A selector driven by receive order,
// newest collection time, whole-second display, or field splicing would
// differ: the fresh seq-5 record would show status=online, version=1.26.0
// (no skew), height=900 and missed=2 (no alarm at tolerance 4).
func assertReconnectHealthLine(t *testing.T, out, errOut string, code int) {
	t.Helper()
	if code != 0 {
		t.Fatalf("offline is a successful query and must exit 0, got %d: stderr=%q", code, errOut)
	}
	if errOut != "" {
		t.Errorf("health findings belong on stdout; stderr must be empty, got %q", errOut)
	}
	lines := outputLines(out)
	if len(lines) != 1 {
		t.Fatalf("want exactly one health line, got %d: %q", len(lines), out)
	}
	if lines[0] != reconnectLatestLine {
		t.Errorf("health line = %q\nwant            %q", lines[0], reconnectLatestLine)
	}
}

// TestCLIHealthReplayedRecordsGreatestSeqDrivesStatus saves the same two
// records in both arrival orders and requires the byte-identical health line,
// built entirely from the greatest seq.
func TestCLIHealthReplayedRecordsGreatestSeqDrivesStatus(t *testing.T) {
	var seen []string
	for _, seed := range []struct {
		name string
		fn   func(*testing.T, string)
	}{
		{"stale greatest seq received first", seedReconnectStored},
		{"stale greatest seq received last", seedReconnectReversed},
	} {
		t.Run(seed.name, func(t *testing.T) {
			dir := t.TempDir()
			seed.fn(t, dir)
			out, errOut, code := healthQueryAt(t, dir, cliQueryAt,
				"--node", reconnectNode, "--expected-version", "1.26.0", "--tolerated-misses", "4")
			assertReconnectHealthLine(t, out, errOut, code)
			seen = append(seen, out)

			// History keeps both gapped seqs ascending, each intact.
			lines := historyLines(t, dir, reconnectNode)
			if len(lines) != 2 {
				t.Fatalf("history = %q, want two records", lines)
			}
			if lines[0] != "node=backfill seq=5 collected_at=2026-10-01T11:59:50Z version=1.26.0 height=900 missed=2" {
				t.Errorf("history line 1 = %q", lines[0])
			}
			if lines[1] != "node=backfill seq=10 collected_at=2026-10-01T11:58:00Z version=1.25.0 height=1000 missed=5" {
				t.Errorf("history line 2 = %q", lines[1])
			}
		})
	}
	if len(seen) == 2 && seen[0] != seen[1] {
		t.Errorf("arrival order changed the output:\n%s\nvs\n%s", seen[0], seen[1])
	}
}

// TestCLIHealthLateDuplicateResubmitChangesNothing replays the smaller-seq
// record at a later receive time, first already covered by the stored pair
// via a fresh backfill, then as a verbatim duplicate, and checks the latest
// telemetry collection time and the offline judgement never move.
func TestCLIHealthLateDuplicateResubmitChangesNothing(t *testing.T) {
	dir := t.TempDir()
	// Only the stale greatest-seq record is known initially.
	submitWithReceive(t, dir, "["+reconnectStaleRecord+"]", reconnectStaleReceive)

	out, _, code := healthQueryAt(t, dir, cliQueryAt,
		"--node", reconnectNode, "--expected-version", "1.26.0", "--tolerated-misses", "4")
	if code != 0 || !strings.Contains(out, "seq=10") || !strings.Contains(out, "status=offline") {
		t.Fatalf("initial query: code=%d out=%q", code, out)
	}

	// The node reconnects later and backfills the smaller seq for the first
	// time. Its receive time is later, but the latest record stays seq 10.
	later := "2026-10-01T12:02:00Z"
	submitOut, errOut, submitCode := runCLI(t, dir, "["+reconnectFreshRecord+"]",
		"heartbeat", "submit", "--data-dir", dir, "--receive-time", later)
	if submitCode != 0 {
		t.Fatalf("late backfill exited %d: stderr=%q stdout=%q", submitCode, errOut, submitOut)
	}
	if !strings.Contains(submitOut, "new=1 duplicate=0") {
		t.Errorf("first backfill counts = %q, want new=1 duplicate=0", strings.TrimSpace(submitOut))
	}
	out, _, code = healthQueryAt(t, dir, later,
		"--node", reconnectNode, "--expected-version", "1.26.0", "--tolerated-misses", "4")
	assertReconnectHealthLineLater(t, out, code, later)

	// Re-submitting the same record at yet another receive time is a duplicate
	// and must not refresh the latest collection time or change any judgement.
	evenLater := "2026-10-01T12:03:00Z"
	submitOut, _, submitCode = runCLI(t, dir, "["+reconnectFreshRecord+"]",
		"heartbeat", "submit", "--data-dir", dir, "--receive-time", evenLater)
	if submitCode != 0 {
		t.Fatalf("duplicate replay exited %d: stderr=%q", submitCode, errOut)
	}
	if !strings.Contains(submitOut, "new=0 duplicate=1") {
		t.Errorf("duplicate replay counts = %q, want new=0 duplicate=1", strings.TrimSpace(submitOut))
	}
	out, _, code = healthQueryAt(t, dir, evenLater,
		"--node", reconnectNode, "--expected-version", "1.26.0", "--tolerated-misses", "4")
	assertReconnectHealthLineLater(t, out, code, evenLater)
}

// assertReconnectHealthLineLater is the same seq-10 expectation at a query
// instant other than the shared fixture constant; only the printed query frame
// changes, the health line does not.
func assertReconnectHealthLineLater(t *testing.T, out string, code int, _ string) {
	t.Helper()
	if code != 0 {
		t.Fatalf("query must exit 0, got %d", code)
	}
	lines := outputLines(out)
	if len(lines) != 1 || lines[0] != reconnectLatestLine {
		t.Fatalf("health line after late replay = %q\nwant                         %q", out, reconnectLatestLine)
	}
}

// TestCLIHealthSubSecondBoundaryAndWholeSecondDisplay protects the exact
// 60-second edge with fractional instants. The terminal prints collection
// time without its fraction, so both queries display 12:00:00 while one is
// exactly 60s old (online) and the other 60s+1ns old (offline, exit zero).
// Re-spelling the same instants in other timezones must not change either
// verdict, and the fraction must remain persisted in the saved file.
func TestCLIHealthSubSecondBoundaryAndWholeSecondDisplay(t *testing.T) {
	dir := t.TempDir()
	submitWithReceive(t, dir,
		`[{"node":"frac","seq":1,"collected_at":"2026-10-01T12:00:00.5Z","version":"1.0","height":100,"missed":0}]`,
		"2026-10-01T12:02:00Z")

	// Exactly 60 seconds old: online. Both instants carry the .5 fraction.
	out, errOut, code := healthQueryAt(t, dir, "2026-10-01T12:01:00.5Z",
		"--node", "frac", "--expected-version", "1.0", "--tolerated-misses", "0")
	if code != 0 {
		t.Fatalf("online query must exit 0, got %d: %q", code, errOut)
	}
	if !strings.Contains(out, "status=online") {
		t.Errorf("at exactly 60s want online: %q", out)
	}
	// Existing terminal format: whole seconds, no fractional digits.
	if !strings.Contains(out, "collected_at=2026-10-01T12:00:00Z") {
		t.Errorf("collection time must keep the whole-second RFC3339 display: %q", out)
	}
	if strings.Contains(out, "12:00:00.5") {
		t.Errorf("terminal display must stay whole-second compatible: %q", out)
	}

	// One nanosecond beyond the window: offline with the offline notice, still
	// a successful command.
	out, errOut, code = healthQueryAt(t, dir, "2026-10-01T12:01:00.500000001Z",
		"--node", "frac", "--expected-version", "1.0", "--tolerated-misses", "0")
	if code != 0 {
		t.Fatalf("offline is a query result, not an input error: exit=%d stderr=%q", code, errOut)
	}
	if errOut != "" {
		t.Errorf("offline notice belongs on stdout, stderr=%q", errOut)
	}
	if !strings.Contains(out, "status=offline") || !strings.Contains(out, "findings=[offline]") {
		t.Errorf("at 60s+1ns want offline with findings=[offline]: %q", out)
	}
	// Both verdicts print the identical whole-second time; judging by the
	// displayed whole seconds could not tell them apart.
	if !strings.Contains(out, "collected_at=2026-10-01T12:00:00Z") {
		t.Errorf("offline line must display the same whole-second time: %q", out)
	}

	// Same instants, different timezone spellings: the verdicts agree.
	out, _, code = healthQueryAt(t, dir, "2026-10-01T20:01:00.5+08:00",
		"--node", "frac", "--expected-version", "1.0", "--tolerated-misses", "0")
	if code != 0 || !strings.Contains(out, "status=online") {
		t.Errorf("+08:00 spelling of the exact boundary: code=%d out=%q, want online", code, out)
	}
	out, errOut, code = healthQueryAt(t, dir, "2026-10-01T08:01:00.500000001-04:00",
		"--node", "frac", "--expected-version", "1.0", "--tolerated-misses", "0")
	if code != 0 || !strings.Contains(out, "status=offline") {
		t.Errorf("-04:00 spelling of 60s+1ns: code=%d out=%q stderr=%q, want offline/exit 0", code, out, errOut)
	}

	// The fraction stays exact in stored telemetry even though the terminal
	// renders whole seconds, and history uses the same display format.
	if raw := readNodeFile(t, dir, "frac"); !strings.Contains(raw, "2026-10-01T12:00:00.5Z") {
		t.Errorf("fractional collection time must be preserved on disk: %s", raw)
	}
	lines := historyLines(t, dir, "frac")
	if len(lines) != 1 || !strings.Contains(lines[0], "collected_at=2026-10-01T12:00:00Z") {
		t.Errorf("history display = %q, want whole-second collected_at", lines)
	}
}

// TestCLIHealthBaselineKeepsGreatestSeqAndCumulative checks that a
// missed-since baseline changes only the missed-duty alarm basis: the latest
// record is still seq 10 with its stale time (offline), version skew and
// cumulative missed=5 on the first line; the baseline seq 5 contributes only
// the second line. It must never decide online status.
func TestCLIHealthBaselineKeepsGreatestSeqAndCumulative(t *testing.T) {
	dir := t.TempDir()
	seedReconnectStored(t, dir)

	// new missed = 5-2 = 3, tolerance 2: alarm; status is still offline.
	out, errOut, code := healthQueryAt(t, dir, cliQueryAt,
		"--node", reconnectNode, "--expected-version", "1.26.0",
		"--tolerated-misses", "2", "--missed-since-seq", "5")
	if code != 0 {
		t.Fatalf("baseline query must exit 0, got %d: %q", code, errOut)
	}
	lines := outputLines(out)
	if len(lines) != 2 {
		t.Fatalf("want two lines, got %q", out)
	}
	wantFirst := "node=backfill status=offline seq=10 collected_at=2026-10-01T11:58:00Z version=1.25.0 height=1000 missed=5 findings=[offline version skew: 1.25.0 != 1.26.0 missed duties above tolerance]"
	if lines[0] != wantFirst {
		t.Errorf("first line = %q\nwant          %q", lines[0], wantFirst)
	}
	if strings.Contains(lines[0], "missed=2") {
		t.Errorf("first line must show cumulative missed=5, not the baseline count: %q", lines[0])
	}
	if lines[1] != "baseline_seq=5 baseline_missed=2 new_missed=3 tolerated_misses=2" {
		t.Errorf("second line = %q", lines[1])
	}

	// new=3 with tolerance 3 silences the miss alarm but leaves offline and
	// the seq-10 version skew untouched: the baseline does not decide status.
	out, _, code = healthQueryAt(t, dir, cliQueryAt,
		"--node", reconnectNode, "--expected-version", "1.26.0",
		"--tolerated-misses", "3", "--missed-since-seq", "5")
	if code != 0 {
		t.Fatalf("exit=%d out=%q", code, out)
	}
	lines = outputLines(out)
	wantFirst = "node=backfill status=offline seq=10 collected_at=2026-10-01T11:58:00Z version=1.25.0 height=1000 missed=5 findings=[offline version skew: 1.25.0 != 1.26.0]"
	if lines[0] != wantFirst {
		t.Errorf("first line with silent miss alarm = %q\nwant                                %q", lines[0], wantFirst)
	}
	if lines[1] != "baseline_seq=5 baseline_missed=2 new_missed=3 tolerated_misses=3" {
		t.Errorf("second line = %q", lines[1])
	}

	// Baseline equal to the latest record: new missed 0, still offline.
	out, _, code = healthQueryAt(t, dir, cliQueryAt,
		"--node", reconnectNode, "--expected-version", "1.26.0",
		"--tolerated-misses", "0", "--missed-since-seq", "10")
	if code != 0 {
		t.Fatalf("exit=%d out=%q", code, out)
	}
	lines = outputLines(out)
	if !strings.Contains(lines[0], "status=offline") || !strings.Contains(lines[0], "seq=10") {
		t.Errorf("baseline==latest must stay offline on seq 10: %q", lines[0])
	}
	if lines[1] != "baseline_seq=10 baseline_missed=5 new_missed=0 tolerated_misses=0" {
		t.Errorf("second line = %q", lines[1])
	}
}

// TestCLIHealthQueryBeforeLatestSeqFailsWithoutFallback stores a node whose
// smaller-seq record genuinely exists before the requested query instant
// while the greatest-seq record does not exist yet. Both the plain query and
// a query with a valid baseline must fail non-zero with a reason on stderr
// and no health line on stdout — never silently rewinding to the older record
// and reporting it healthy.
func TestCLIHealthQueryBeforeLatestSeqFailsWithoutFallback(t *testing.T) {
	dir := t.TempDir()
	// seq 4 collected 11:57:00; seq 8 collected 11:58:00. Query at 11:57:30:
	// seq 4 has already been collected, but the query is 30s before seq 8.
	submitBatch(t, dir, `[
	  {"node":"older","seq":4,"collected_at":"2026-10-01T11:57:00Z","version":"1.0","height":400,"missed":1},
	  {"node":"older","seq":8,"collected_at":"2026-10-01T11:58:00Z","version":"1.0","height":404,"missed":3}
	]`)
	const earlyAt = "2026-10-01T11:57:30Z"

	cases := []struct {
		name string
		args []string
	}{
		{"plain query", []string{"--node", "older", "--expected-version", "1.0", "--tolerated-misses", "0"}},
		{"query with valid baseline", []string{"--node", "older", "--expected-version", "1.0",
			"--tolerated-misses", "0", "--missed-since-seq", "4"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, errOut, code := healthQueryAt(t, dir, earlyAt, tc.args...)
			if code == 0 {
				t.Errorf("query before the greatest-seq record's collection time must exit non-zero; stdout=%q", out)
			}
			if !strings.Contains(errOut, "error:") || !strings.Contains(errOut, "earlier than collection") {
				t.Errorf("stderr must explain the time ordering, got %q", errOut)
			}
			if out != "" {
				t.Errorf("no health result may be printed when the query is rejected, stdout=%q", out)
			}
		})
	}

	// At the exact greatest-seq collection instant the query succeeds and
	// reports seq 8 (age 0, online); the rejection is strictly about time.
	out, errOut, code := healthQueryAt(t, dir, "2026-10-01T11:58:00Z",
		"--node", "older", "--expected-version", "1.0", "--tolerated-misses", "9")
	if code != 0 {
		t.Fatalf("query at the exact latest collection instant must succeed: %d %q", code, errOut)
	}
	if !strings.Contains(out, "seq=8") || !strings.Contains(out, "status=online") {
		t.Errorf("exact-boundary query = %q, want seq=8 online", out)
	}
}

// TestCLIHealthQueriesAndFailuresDoNotRewriteStore snapshots the saved node
// files, exercises successful and failing reads of every kind plus a
// duplicate replay, and requires byte-identical storage and no file for a
// queried unknown node.
func TestCLIHealthQueriesAndFailuresDoNotRewriteStore(t *testing.T) {
	dir := t.TempDir()
	seedReconnectStored(t, dir)
	submitBatch(t, dir, `[
	  {"node":"older","seq":4,"collected_at":"2026-10-01T11:57:00Z","version":"1.0","height":400,"missed":1},
	  {"node":"older","seq":8,"collected_at":"2026-10-01T11:58:00Z","version":"1.0","height":404,"missed":3}
	]`)

	before := map[string]string{}
	for _, node := range []string{reconnectNode, "older"} {
		before[node] = readNodeFile(t, dir, node)
	}

	// Successful offline query, baseline variants, and a notelemetry answer.
	healthQueryAt(t, dir, cliQueryAt, "--node", reconnectNode, "--expected-version", "1.26.0", "--tolerated-misses", "4")
	healthQueryAt(t, dir, cliQueryAt, "--node", reconnectNode, "--expected-version", "1.26.0",
		"--tolerated-misses", "2", "--missed-since-seq", "5")
	out, _, code := healthQueryAt(t, dir, cliQueryAt, "--node", "ghost", "--expected-version", "1.0", "--tolerated-misses", "0")
	if code != 0 || !strings.Contains(out, "status=无遥测") {
		t.Fatalf("unknown node query: code=%d out=%q, want notelemetry/exit 0", code, out)
	}

	// Failing reads: early plain and baseline queries, unknown/missing/too-large
	// baselines, baseline on a node without telemetry.
	if _, _, c := healthQueryAt(t, dir, "2026-10-01T11:57:30Z",
		"--node", "older", "--expected-version", "1.0", "--tolerated-misses", "0"); c == 0 {
		t.Error("early plain query should exit non-zero")
	}
	if _, _, c := healthQueryAt(t, dir, "2026-10-01T11:57:30Z",
		"--node", "older", "--expected-version", "1.0", "--tolerated-misses", "0",
		"--missed-since-seq", "4"); c == 0 {
		t.Error("early baseline query should exit non-zero")
	}
	if _, _, c := healthQueryAt(t, dir, cliQueryAt, "--node", reconnectNode, "--expected-version", "1.26.0",
		"--tolerated-misses", "0", "--missed-since-seq", "7"); c == 0 {
		t.Error("baseline seq 7 (never saved) should exit non-zero")
	}
	if _, _, c := healthQueryAt(t, dir, cliQueryAt, "--node", reconnectNode, "--expected-version", "1.26.0",
		"--tolerated-misses", "0", "--missed-since-seq", "11"); c == 0 {
		t.Error("baseline above the latest seq should exit non-zero")
	}
	if _, _, c := healthQueryAt(t, dir, cliQueryAt, "--node", "ghost", "--expected-version", "1.0",
		"--tolerated-misses", "0", "--missed-since-seq", "1"); c == 0 {
		t.Error("baseline on a node without telemetry should exit non-zero")
	}

	// Querying an unknown node must not create its file.
	if _, err := os.Stat(nodeFileHexPath(dir, "ghost")); !os.IsNotExist(err) {
		t.Error("a notelemetry query must not create a node file")
	}

	// A late duplicate replay reports duplicate=1 and leaves the file bytes
	// identical (deterministic encoding of unchanged records).
	dupOut, errOut, c := runCLI(t, dir, "["+reconnectFreshRecord+"]",
		"heartbeat", "submit", "--data-dir", dir, "--receive-time", "2026-10-01T12:05:00Z")
	if c != 0 || !strings.Contains(dupOut, "new=0 duplicate=1") {
		t.Fatalf("duplicate replay: code=%d out=%q err=%q", c, dupOut, errOut)
	}

	for node, want := range before {
		if got := readNodeFile(t, dir, node); got != want {
			t.Errorf("saved file for %q changed during queries, failures or duplicate replay", node)
		}
	}

	// History semantics after all the reads: the fresh small seq is still the
	// first record, not promoted or refreshed by anything.
	lines := historyLines(t, dir, reconnectNode)
	if len(lines) != 2 || !strings.Contains(lines[0], "seq=5") || !strings.Contains(lines[1], "seq=10") {
		t.Errorf("history after read-only activity = %q", lines)
	}
}
