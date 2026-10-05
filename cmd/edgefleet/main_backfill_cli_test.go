package main

// Command-line regression tests for the relationship between backfilled
// (断连补传) heartbeats and the current online status. The domain-level rules
// have focused tests in package edgefleet; these protect what a user actually
// sees at the real command:
//
//   - a stale record with a greater seq arriving late (before or after a
//     fresher small-seq record) makes the node offline, and every field on the
//     first line comes from that one record — nothing is spliced from the
//     fresher record;
//   - re-submitting the smaller-seq record later counts as a duplicate and
//     cannot refresh the latest telemetry's collection time;
//   - the online/offline boundary is exact at sub-second precision (exactly
//     60s online, one nanosecond more offline), and the fractional collection
//     time is shown verbatim in the terminal, including under a different
//     timezone spelling;
//   - whole-second timestamps keep their existing display (no trailing dot);
//   - offline is a successful query (exit 0, empty stderr), whereas a query
//     time earlier than the greatest-seq record is a real error (non-zero
//     exit, stderr, no stdout), with no fallback to a smaller-seq record and
//     no change to saved heartbeats — with and without a baseline.

import (
	"strings"
	"testing"
)

// healthAt runs a health query against dir at an arbitrary --at instant.
func healthAt(t *testing.T, dir, at, node string, extra ...string) (string, string, int) {
	t.Helper()
	args := append([]string{
		"heartbeat", "health", "--data-dir", dir, "--at", at,
		"--node", node, "--expected-version", "1.26.0", "--tolerated-misses", "10",
	}, extra...)
	return runCLI(t, dir, "", args...)
}

// submitAt stores one JSON array via the real submit command at a given
// receive time.
func submitAt(t *testing.T, dir, receiveAt, json string) (string, string, int) {
	t.Helper()
	return runCLI(t, dir, json,
		"heartbeat", "submit", "--data-dir", dir, "--receive-time", receiveAt)
}

// TestCLIBackfilledStaleGreaterSeqIsOffline is the central regression: seq 8
// was collected two minutes ago (old telemetry, different version/height/
// missed), seq 5 only ten seconds ago but has the smaller seq. Whichever
// arrives first, the query must choose seq 8, report offline, and render
// every field from seq 8 — the newer collection time of seq 5 must not make
// the node look online, and the two records' fields must not be stitched.
func TestCLIBackfilledStaleGreaterSeqIsOffline(t *testing.T) {
	fresh5 := `{"node":"n1","seq":5,"collected_at":"2026-10-01T11:59:50Z","version":"1.26.0","height":999,"missed":2}`
	stale8 := `{"node":"n1","seq":8,"collected_at":"2026-10-01T11:58:00Z","version":"1.24.0","height":900,"missed":7}`

	for _, tc := range []struct {
		name   string
		first  string
		second string
	}{
		{"greater seq first", "[" + stale8 + "]", "[" + fresh5 + "]"},
		{"fresher record first", "[" + fresh5 + "]", "[" + stale8 + "]"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			submitBatch(t, dir, tc.first)
			submitBatch(t, dir, tc.second)

			out, errOut, code := healthAt(t, dir, cliQueryAt, "n1")
			if code != 0 {
				t.Fatalf("offline is a successful query, exit=%d stderr=%q", code, errOut)
			}
			if errOut != "" {
				t.Errorf("offline findings belong on stdout; stderr=%q", errOut)
			}
			lines := outputLines(out)
			if len(lines) != 1 {
				t.Fatalf("want one line, got %q", out)
			}
			want := "node=n1 status=offline seq=8 collected_at=2026-10-01T11:58:00Z " +
				"version=1.24.0 height=900 missed=7 findings=[offline version skew: 1.24.0 != 1.26.0]"
			if lines[0] != want {
				t.Errorf("health line mismatch:\n got %q\nwant %q", lines[0], want)
			}
			// Explicitly guard against splicing in any field from seq 5.
			for _, leaked := range []string{"height=999", "missed=2", "11:59:50Z", "status=online"} {
				if strings.Contains(lines[0], leaked) {
					t.Errorf("output must not borrow %q from the fresher seq-5 record: %q", leaked, lines[0])
				}
			}
		})
	}
}

// Before the stale record is backfilled, the node genuinely looks online on
// seq 5; after the late greater-seq record arrives the same query instant
// must flip to offline and stay on seq 8.
func TestCLILateBackfillFlipsOnlineToOffline(t *testing.T) {
	dir := t.TempDir()
	submitBatch(t, dir, `[
	  {"node":"n1","seq":5,"collected_at":"2026-10-01T11:59:50Z","version":"1.26.0","height":999,"missed":2}
	]`)
	out, _, code := healthAt(t, dir, cliQueryAt, "n1")
	if code != 0 {
		t.Fatalf("online query must succeed, stdout=%q", out)
	}
	lines := outputLines(out)
	if len(lines) != 1 || !strings.Contains(lines[0], "status=online seq=5") {
		t.Fatalf("before backfill want online seq=5, got %q", out)
	}

	// Reconnect: stale seq 8 collected during the outage arrives late.
	submitAt(t, dir, "2026-10-01T12:03:00Z", `[
	  {"node":"n1","seq":8,"collected_at":"2026-10-01T11:58:00Z","version":"1.24.0","height":900,"missed":7}
	]`)
	out, errOut, code := healthAt(t, dir, cliQueryAt, "n1")
	if code != 0 {
		t.Fatalf("offline query must exit 0, code=%d stderr=%q", code, errOut)
	}
	if !strings.Contains(out, "status=offline seq=8 collected_at=2026-10-01T11:58:00Z") {
		t.Errorf("after backfill want offline seq=8 with its time, got %q", out)
	}
	if strings.Contains(out, "height=999") || strings.Contains(out, "11:59:50Z") {
		t.Errorf("seq-5 telemetry must not leak into the result: %q", out)
	}
}

// Re-submitting the smaller, fresher record at a much later time counts as a
// duplicate and must not extend the online window or move any selected field.
func TestCLILateDuplicateResubmitCannotRefresh(t *testing.T) {
	dir := t.TempDir()
	submitBatch(t, dir, `[
	  {"node":"n1","seq":8,"collected_at":"2026-10-01T11:58:00Z","version":"1.24.0","height":900,"missed":7},
	  {"node":"n1","seq":5,"collected_at":"2026-10-01T11:59:50Z","version":"1.26.0","height":999,"missed":2}
	]`)
	storedBefore := readNodeFile(t, dir, "n1")

	// A verbatim seq-5 replay five minutes later: a duplicate, nothing new.
	out, errOut, code := submitAt(t, dir, "2026-10-01T12:05:00Z", `[
	  {"node":"n1","seq":5,"collected_at":"2026-10-01T11:59:50Z","version":"1.26.0","height":999,"missed":2}
	]`)
	if code != 0 {
		t.Fatalf("late duplicate must submit successfully: code=%d stderr=%q", code, errOut)
	}
	if strings.TrimRight(out, "\n") != "submitted: new=0 duplicate=1" {
		t.Errorf("late replay stdout=%q, want new=0 duplicate=1", out)
	}

	// Queried at that later moment, the node is still offline on stale seq 8;
	// the replay's receive time never refreshes the collection time.
	out, _, code = healthAt(t, dir, "2026-10-01T12:05:00Z", "n1")
	if code != 0 {
		t.Fatalf("health must exit 0, stdout=%q", out)
	}
	lines := outputLines(out)
	if len(lines) != 1 {
		t.Fatalf("want one line, got %q", out)
	}
	want := "node=n1 status=offline seq=8 collected_at=2026-10-01T11:58:00Z " +
		"version=1.24.0 height=900 missed=7 findings=[offline version skew: 1.24.0 != 1.26.0]"
	if lines[0] != want {
		t.Errorf("after late duplicate:\n got %q\nwant %q", lines[0], want)
	}

	// Saved history is untouched: still the two original records.
	if got := readNodeFile(t, dir, "n1"); got != storedBefore {
		t.Errorf("duplicate replay rewrote the node file")
	}
	hist := historyLines(t, dir, "n1")
	if len(hist) != 2 ||
		!strings.Contains(hist[0], "seq=5 collected_at=2026-10-01T11:59:50Z") ||
		!strings.Contains(hist[1], "seq=8 collected_at=2026-10-01T11:58:00Z") {
		t.Errorf("history changed after replay: %q", hist)
	}
}

// TestCLIFractionalSecondBoundaryAndDisplay pins the exact 60s edge with
// sub-second precision, and requires the terminal to keep the fraction. A
// display that truncates to whole seconds would both misjudge 60s+1ns and
// hide the real collection time.
func TestCLIFractionalSecondBoundaryAndDisplay(t *testing.T) {
	cases := []struct {
		name      string
		collected string
		receiveAt string
		atExact60 string // collected + exactly 60s -> online
		atOver1ns string // collected + 60s + 1ns -> offline
		wantShown string // collected_at as it must appear
	}{
		{
			name:      "utc half second",
			collected: "2026-10-01T11:59:00.5Z",
			receiveAt: "2026-10-01T11:59:00.5Z",
			atExact60: "2026-10-01T12:00:00.5Z",
			atOver1ns: "2026-10-01T12:00:00.500000001Z",
			wantShown: "2026-10-01T11:59:00.5Z",
		},
		{
			name:      "plus8 half second",
			collected: "2026-10-01T19:59:00.5+08:00",
			receiveAt: "2026-10-01T19:59:00.5+08:00",
			atExact60: "2026-10-01T20:00:00.5+08:00",
			atOver1ns: "2026-10-01T20:00:00.500000001+08:00",
			wantShown: "2026-10-01T19:59:00.5+08:00",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			submitBatchAt(t, dir, tc.receiveAt, `[
			  {"node":"n1","seq":1,"collected_at":"`+tc.collected+`","version":"1.26.0","height":100,"missed":0}
			]`)

			// Exactly 60s: online, fraction shown verbatim, command succeeds.
			out, errOut, code := healthAt(t, dir, tc.atExact60, "n1")
			if code != 0 {
				t.Fatalf("at exactly 60s must exit 0, stderr=%q", errOut)
			}
			lines := outputLines(out)
			if len(lines) != 1 {
				t.Fatalf("want one line, got %q", out)
			}
			want := "node=n1 status=online seq=1 collected_at=" + tc.wantShown +
				" version=1.26.0 height=100 missed=0 findings=[]"
			if lines[0] != want {
				t.Errorf("exact-60s line:\n got %q\nwant %q", lines[0], want)
			}

			// 60s + 1 nanosecond: offline, finding present, still a successful
			// query (exit 0, empty stderr) — not an input error.
			out, errOut, code = healthAt(t, dir, tc.atOver1ns, "n1")
			if code != 0 {
				t.Errorf("offline is a successful status, exit=%d stderr=%q", code, errOut)
			}
			if errOut != "" {
				t.Errorf("offline must not print to stderr, got %q", errOut)
			}
			lines = outputLines(out)
			if len(lines) != 1 {
				t.Fatalf("want one line, got %q", out)
			}
			want = "node=n1 status=offline seq=1 collected_at=" + tc.wantShown +
				" version=1.26.0 height=100 missed=0 findings=[offline]"
			if lines[0] != want {
				t.Errorf("60s+1ns line:\n got %q\nwant %q", lines[0], want)
			}

			// history keeps the fraction too.
			hist := historyLines(t, dir, "n1")
			if len(hist) != 1 || !strings.Contains(hist[0], "collected_at="+tc.wantShown) {
				t.Errorf("history must retain the fractional time %q, got %q", tc.wantShown, hist)
			}
		})
	}
}

// submitBatchAt is submitBatch with an explicit receive time.
func submitBatchAt(t *testing.T, dir, receiveAt, json string) {
	t.Helper()
	out, errOut, code := submitAt(t, dir, receiveAt, json)
	if code != 0 {
		t.Fatalf("submit exited %d: stderr=%q stdout=%q", code, errOut, out)
	}
}

// Whole-second timestamps must render exactly as before (no trailing dot), in
// both health and history output: the fractional-aware formatter only adds the
// fraction when one exists.
func TestCLIWholeSecondDisplayUnchanged(t *testing.T) {
	dir := t.TempDir()
	submitBatch(t, dir, `[
	  {"node":"n1","seq":1,"collected_at":"2026-10-01T11:59:00Z","version":"1.26.0","height":100,"missed":0}
	]`)
	out, _, code := healthAt(t, dir, cliQueryAt, "n1")
	if code != 0 {
		t.Fatalf("health must exit 0, stdout=%q", out)
	}
	wantHealth := "node=n1 status=online seq=1 collected_at=2026-10-01T11:59:00Z " +
		"version=1.26.0 height=100 missed=0 findings=[]"
	if lines := outputLines(out); len(lines) != 1 || lines[0] != wantHealth {
		t.Errorf("whole-second health line = %q, want %q", out, wantHealth)
	}
	wantHist := "node=n1 seq=1 collected_at=2026-10-01T11:59:00Z version=1.26.0 height=100 missed=0"
	if hist := historyLines(t, dir, "n1"); len(hist) != 1 || hist[0] != wantHist {
		t.Errorf("whole-second history line = %q, want %q", hist, wantHist)
	}
}

// A query time earlier than the greatest-seq record's collection time is a
// hard error for both the plain and the baseline query, even though the
// smaller seq 5 was already collected by then: no fallback health result,
// non-zero exit, reason on stderr, and the saved file is byte-for-byte
// unchanged.
func TestCLIQueryBeforeLatestFailsNonZeroWithoutFallback(t *testing.T) {
	dir := t.TempDir()
	submitBatch(t, dir, `[
	  {"node":"n1","seq":5,"collected_at":"2026-10-01T11:59:50Z","version":"1.26.0","height":999,"missed":2},
	  {"node":"n1","seq":8,"collected_at":"2026-10-01T11:59:58Z","version":"1.24.0","height":900,"missed":7}
	]`)
	storedBefore := readNodeFile(t, dir, "n1")

	// Query at :55 — after seq 5 was collected, but before the latest seq 8.
	const at = "2026-10-01T11:59:55Z"
	out, errOut, code := healthAt(t, dir, at, "n1")
	if code == 0 {
		t.Errorf("plain query before the latest record must exit non-zero; stdout=%q", out)
	}
	if out != "" {
		t.Errorf("no health result may be printed on the error, stdout=%q", out)
	}
	if !strings.Contains(errOut, "error:") || !strings.Contains(errOut, "earlier than collection time") {
		t.Errorf("stderr must explain query-before-collection, got %q", errOut)
	}
	// It must not silently answer with seq 5.
	if strings.Contains(errOut, "seq=5") || strings.Contains(out, "node=n1") {
		t.Errorf("must not fall back to the smaller seq-5 record: out=%q err=%q", out, errOut)
	}

	// A valid baseline naming seq 5 cannot rescue the query either: the
	// baseline changes only the missed-duty quantity, never the time rule.
	out, errOut, code = healthAt(t, dir, at, "n1", "--missed-since-seq", "5")
	if code == 0 {
		t.Errorf("baseline query before the latest record must exit non-zero; stdout=%q", out)
	}
	if out != "" {
		t.Errorf("baseline error must print no health result, stdout=%q", out)
	}
	if !strings.Contains(errOut, "earlier than collection time") {
		t.Errorf("baseline stderr must explain query-before-collection, got %q", errOut)
	}

	// Saved heartbeats are unchanged by either failed query.
	if got := readNodeFile(t, dir, "n1"); got != storedBefore {
		t.Errorf("failed queries rewrote the node file")
	}

	// The same node at a valid query instant (>= latest collection) still
	// answers normally, proving the error was about the time argument.
	out, _, code = healthAt(t, dir, cliQueryAt, "n1")
	if code != 0 || !strings.Contains(out, "status=online seq=8") {
		t.Errorf("node must query normally at/after latest collection: code=%d out=%q", code, out)
	}
}
