package main

// Command-line regression tests for the health query time (--at) validity at
// the real `edgefleet heartbeat health` entry point. A query instant drives
// the exact 60-second online edge, so --at is parsed with the same textual
// limits as a heartbeat collection time:
//
//   - an out-of-range numeric offset (+24:00, -24:00, +00:60, +23:60) fails
//     non-zero even though Go's own parser accepts/folds those strings;
//   - a fractional second (with "." or ",") past nine digits is refused when
//     any digit past the ninth is non-zero, never truncated or rounded — so a
//     query 60s plus a sub-nanosecond overshoot can never be judged as
//     exactly 60s and reported online;
//   - the --at check runs before any store access, so it fails identically
//     for a node without telemetry and for a query carrying a
//     --missed-since-seq baseline (valid or unknown): no health line or
//     baseline explanation is printed, no current time is substituted, and no
//     saved heartbeat changes;
//   - the legal boundaries keep working: Z, -00:00, ±23:59, up to nine
//     fraction digits, and longer fractions whose extra digits are all zero;
//     padded fractions and other timezone spellings of one instant agree,
//     exactly 60s stays online and one nanosecond more is offline;
//   - omitting --at still means the current time.

import (
	"os"
	"strings"
	"testing"
	"time"
)

// assertAtRejected is the shared contract for a malformed --at: non-zero
// exit, a reason on stderr naming --at, and neither a health line nor a
// missed-baseline explanation on stdout.
func assertAtRejected(t *testing.T, out, errOut string, code int) {
	t.Helper()
	if code == 0 {
		t.Errorf("malformed --at must exit non-zero; stdout=%q", out)
	}
	if !strings.Contains(errOut, "error:") || !strings.Contains(errOut, "--at") {
		t.Errorf("stderr must be an error naming --at, got %q", errOut)
	}
	if out != "" {
		t.Errorf("a rejected --at must print no health result or baseline, stdout=%q", out)
	}
}

func TestCLIHealthAtRejectsOutOfRangeOffsets(t *testing.T) {
	f := newFixture(t)

	cases := []struct {
		name   string
		at     string
		offset string
		reason string
	}{
		{"plus 24 hours", "2026-10-01T12:00:00+24:00", "+24:00", "00-23"},
		{"minus 24 hours", "2026-10-01T12:00:00-24:00", "-24:00", "00-23"},
		{"60 minutes folds to an hour", "2026-10-01T12:00:00+00:60", "+00:60", "00-59"},
		{"23 hours 60 minutes folds to 24 hours", "2026-10-01T12:00:00+23:60", "+23:60", "00-59"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, errOut, code := healthQueryAt(t, f.dir, tc.at,
				"--node", "mono", "--expected-version", "1.26.0", "--tolerated-misses", "0")
			assertAtRejected(t, out, errOut, code)
			if !strings.Contains(errOut, tc.offset) || !strings.Contains(errOut, tc.reason) {
				t.Errorf("stderr must name offset %q and bound %s, got %q", tc.offset, tc.reason, errOut)
			}
			// A folded offset must never reach node status as another zone.
			if strings.Contains(out, "status=") {
				t.Errorf("folded offset must not produce a health status: %q", out)
			}
		})
	}
}

func TestCLIHealthAtRejectsFractionBeyondNanoseconds(t *testing.T) {
	dir := t.TempDir()
	// Latest heartbeat collected exactly at 12:00:00Z: a truncated query at
	// 12:01:00.000000000 would land on the exact online edge.
	submitBatch(t, dir, `[{"node":"edge","seq":1,"collected_at":"2026-10-01T12:00:00Z","version":"1.0","height":1,"missed":0}]`)

	cases := []struct {
		name string
		at   string
		frac string
	}{
		{"sub-nanosecond overshoot past the edge, dot", "2026-10-01T12:01:00.0000000001Z", ".0000000001"},
		{"sub-nanosecond overshoot past the edge, comma", "2026-10-01T12:01:00,0000000001Z", ",0000000001"},
		{"non-zero eleventh digit", "2026-10-01T12:01:00.00000000001Z", ".00000000001"},
		{"non-zero far past the ninth", "2026-10-01T12:01:00.0000000000001Z", ".0000000000001"},
		{"sub-nanosecond overshoot with offset", "2026-10-01T20:01:00.0000000001+08:00", ".0000000001"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, errOut, code := healthQueryAt(t, dir, tc.at,
				"--node", "edge", "--expected-version", "1.0", "--tolerated-misses", "0")
			assertAtRejected(t, out, errOut, code)
			for _, want := range []string{tc.frac, "nanosecond"} {
				if !strings.Contains(errOut, want) {
					t.Errorf("stderr must quote %q, got %q", want, errOut)
				}
			}
			if strings.Contains(out, "online") {
				t.Errorf("the overshoot must not truncate into an online result: %q", out)
			}
		})
	}
}

// TestCLIHealthAtMalformedTextRejected covers input that is not an RFC3339
// instant with a timezone at all, including a seconds-precision offset.
func TestCLIHealthAtMalformedTextRejected(t *testing.T) {
	f := newFixture(t)
	for _, at := range []string{
		"yesterday",
		"2026-10-01T12:00:00",  // no timezone
		"2026-10-01 12:00:00Z", // wrong separator
		"2026-10-01T12:00:00+08:00:30",
	} {
		t.Run(at, func(t *testing.T) {
			out, errOut, code := healthQueryAt(t, f.dir, at,
				"--node", "mono", "--expected-version", "1.26.0", "--tolerated-misses", "0")
			assertAtRejected(t, out, errOut, code)
		})
	}
}

// TestCLIHealthAtCheckedBeforeTelemetryAndBaseline is the core precedence
// guarantee: an invalid --at is rejected regardless of whether the queried
// node has telemetry or whether a missed-since baseline was supplied. The
// store must never substitute the current time, answer 无遥测, or print the
// baseline explanation for a malformed query.
func TestCLIHealthAtCheckedBeforeTelemetryAndBaseline(t *testing.T) {
	f := newFixture(t)
	const badAt = "2026-10-01T12:01:00.0000000001Z" // would truncate onto the 60s edge

	cases := []struct {
		name string
		node string
		args []string
	}{
		{"node with telemetry, no baseline", "mono", nil},
		{"node with telemetry, valid baseline", "mono", []string{"--missed-since-seq", "1"}},
		{"node with telemetry, unknown baseline", "mono", []string{"--missed-since-seq", "7"}},
		{"node without telemetry, no baseline", "ghost", nil},
		{"node without telemetry, with baseline", "ghost", []string{"--missed-since-seq", "1"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			args := append([]string{"--node", tc.node, "--expected-version", "1.26.0",
				"--tolerated-misses", "0"}, tc.args...)
			out, errOut, code := healthQueryAt(t, f.dir, badAt, args...)
			assertAtRejected(t, out, errOut, code)
			if !strings.Contains(errOut, ".0000000001") || !strings.Contains(errOut, "nanosecond") {
				t.Errorf("stderr must explain the rejected fraction, got %q", errOut)
			}
			// Neither the notelemetry answer nor a baseline line may appear.
			if strings.Contains(out, "无遥测") || strings.Contains(out, "baseline_seq") {
				t.Errorf("malformed --at must not yield notelemetry or baseline output: %q", out)
			}
		})
	}

	// An out-of-range offset gets the same precedence on a node without
	// telemetry and with a baseline.
	for _, tc := range []struct {
		name string
		node string
	}{{"no telemetry", "ghost"}, {"has telemetry", "mono"}} {
		t.Run("offset error precedes "+tc.name, func(t *testing.T) {
			out, errOut, code := healthQueryAt(t, f.dir, "2026-10-01T12:00:00+00:60",
				"--node", tc.node, "--expected-version", "1.26.0",
				"--tolerated-misses", "0", "--missed-since-seq", "1")
			assertAtRejected(t, out, errOut, code)
			if !strings.Contains(errOut, "+00:60") {
				t.Errorf("stderr must name the folded offset, got %q", errOut)
			}
		})
	}

	// The rejected query must not create a file for the unknown node.
	if _, err := os.Stat(nodeFileHexPath(f.dir, "ghost")); !os.IsNotExist(err) {
		t.Errorf("a rejected --at query must not create a node file, stat err=%v", err)
	}
}

// TestCLIHealthAtRejectionLeavesSavedHeartbeatsIntact snapshots stored files
// and proves rejected --at queries (plain and with a baseline) never rewrite
// them.
func TestCLIHealthAtRejectionLeavesSavedHeartbeatsIntact(t *testing.T) {
	f := newFixture(t)
	before := map[string]string{}
	for _, node := range []string{"mono", "reset", "gappy"} {
		before[node] = readNodeFile(t, f.dir, node)
	}

	for _, at := range []string{
		"2026-10-01T12:00:00+00:60",
		"2026-10-01T12:01:00.0000000001Z",
		"2026-10-01T12:01:00,0000000001Z",
	} {
		if _, _, code := healthQueryAt(t, f.dir, at,
			"--node", "mono", "--expected-version", "1.26.0",
			"--tolerated-misses", "0", "--missed-since-seq", "1"); code == 0 {
			t.Errorf("query at %q must exit non-zero", at)
		}
	}
	for node, want := range before {
		if got := readNodeFile(t, f.dir, node); got != want {
			t.Errorf("saved file for %q changed during rejected --at queries", node)
		}
	}
}

// TestCLIHealthAtAcceptsBoundariesAndExactEdge pins the legal side through
// the real command: Z/-00:00/±23:59 offsets, padded fractions and the comma
// separator all work, a padded spelling of the exact 60s instant is online,
// one nanosecond more is offline, and timezone respellings agree.
func TestCLIHealthAtAcceptsBoundariesAndExactEdge(t *testing.T) {
	dir := t.TempDir()
	submitBatch(t, dir, `[{"node":"edge","seq":1,"collected_at":"2026-10-01T12:00:00Z","version":"1.0","height":1,"missed":0}]`)

	online := []struct {
		name string
		at   string
	}{
		{"zulu exactly 60s", "2026-10-01T12:01:00Z"},
		{"negative zero offset", "2026-10-01T12:01:00-00:00"},
		{"positive zero offset", "2026-10-01T12:01:00+00:00"},
		{"max positive offset spelling", "2026-10-02T12:00:00+23:59"},
		{"max negative offset spelling", "2026-09-30T12:02:00-23:59"},
		{"padded nanosecond fraction", "2026-10-01T12:01:00.0000000000Z"},
		{"padded comma fraction in another zone", "2026-10-01T20:01:00,0000000000+08:00"},
	}
	for _, tc := range online {
		t.Run("online/"+tc.name, func(t *testing.T) {
			out, errOut, code := healthQueryAt(t, dir, tc.at,
				"--node", "edge", "--expected-version", "1.0", "--tolerated-misses", "0")
			if code != 0 {
				t.Fatalf("legal boundary %q must succeed: %d %q", tc.at, code, errOut)
			}
			if !strings.Contains(out, "status=online") {
				t.Errorf("%q must be online: %q", tc.at, out)
			}
		})
	}

	offline := []struct {
		name string
		at   string
	}{
		{"one nanosecond past the edge", "2026-10-01T12:01:00.000000001Z"},
		{"same instant in another zone", "2026-10-01T08:01:00.000000001-04:00"},
	}
	for _, tc := range offline {
		t.Run("offline/"+tc.name, func(t *testing.T) {
			out, errOut, code := healthQueryAt(t, dir, tc.at,
				"--node", "edge", "--expected-version", "1.0", "--tolerated-misses", "0")
			if code != 0 {
				t.Fatalf("offline is a successful query, got %d: %q", code, errOut)
			}
			if !strings.Contains(out, "status=offline") {
				t.Errorf("%q must be offline: %q", tc.at, out)
			}
		})
	}
}

// TestCLIHealthAtPaddedFractionAndTimezoneAgreeWithBaseline shows the exact
// edge is respected with a missed-since baseline too: the padded 60s query is
// online with a baseline line, the one-nanosecond-overshoot query is offline,
// and a sub-nanosecond overshoot past nine digits is an input error instead
// of an online result.
func TestCLIHealthAtPaddedFractionAndTimezoneAgreeWithBaseline(t *testing.T) {
	dir := t.TempDir()
	submitBatch(t, dir, `[
	  {"node":"edge","seq":1,"collected_at":"2026-10-01T12:00:00Z","version":"1.0","height":1,"missed":2},
	  {"node":"edge","seq":2,"collected_at":"2026-10-01T12:00:00Z","version":"1.0","height":2,"missed":2}
	]`)

	out, _, code := healthQueryAt(t, dir, "2026-10-01T12:01:00.0000000000Z",
		"--node", "edge", "--expected-version", "1.0",
		"--tolerated-misses", "0", "--missed-since-seq", "1")
	if code != 0 || !strings.Contains(out, "status=online") || !strings.Contains(out, "baseline_seq=1") {
		t.Fatalf("padded exact edge with baseline: code=%d out=%q", code, out)
	}

	out, errOut, code := healthQueryAt(t, dir, "2026-10-01T12:01:00.0000000001Z",
		"--node", "edge", "--expected-version", "1.0",
		"--tolerated-misses", "0", "--missed-since-seq", "1")
	assertAtRejected(t, out, errOut, code)
}

// TestCLIHealthWithoutAtUsesCurrentTime pins omission of --at: the query runs
// against the current wall clock, and even a node without telemetry answers
// 无遥测 (no --at validation runs at all).
func TestCLIHealthWithoutAtUsesCurrentTime(t *testing.T) {
	dir := t.TempDir()
	now := time.Now().UTC()
	// Collected ~30 seconds ago, received now: recent enough to be online at
	// the current instant with a wide margin to the 60-second window.
	collected := now.Add(-30 * time.Second).Format(time.RFC3339Nano)
	receive := now.Format(time.RFC3339Nano)
	out, errOut, code := runCLI(t, dir, `[{"node":"live","seq":1,"collected_at":"`+collected+
		`","version":"1.0","height":1,"missed":0}]`,
		"heartbeat", "submit", "--data-dir", dir, "--receive-time", receive)
	if code != 0 {
		t.Fatalf("recent heartbeat submit failed: %d stdout=%q stderr=%q", code, out, errOut)
	}

	runWithoutAt := func(node string) (string, string, int) {
		return runCLI(t, dir, "",
			"heartbeat", "health", "--data-dir", dir,
			"--node", node, "--expected-version", "1.0", "--tolerated-misses", "0")
	}

	hout, herrOut, hcode := runWithoutAt("live")
	if hcode != 0 {
		t.Fatalf("health without --at must use current time and succeed: %d %q", hcode, herrOut)
	}
	if !strings.Contains(hout, "status=online") {
		t.Errorf("a heartbeat 30s old must be online at the current time: %q", hout)
	}

	// No telemetry and no --at: the notelemetry answer is unaffected.
	hout, _, hcode = runWithoutAt("ghost")
	if hcode != 0 || !strings.Contains(hout, "status=无遥测") {
		t.Fatalf("notelemetry without --at: code=%d out=%q", hcode, hout)
	}
}
