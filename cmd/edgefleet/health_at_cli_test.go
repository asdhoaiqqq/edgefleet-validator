package main

// Command-line regression tests for the `heartbeat health --at` query-time
// rules. The query instant is compared against saved collection times, so it
// must be the exact moment the user wrote — but plain time.Parse(RFC3339)
// folds an out-of-range numeric offset (+00:60 -> +01:00) and silently
// truncates a fractional second past nine digits, which would let an illegal
// or unrepresentable query time still produce a health result (even calling
// a just-expired node exactly-on-time). The command must instead hold --at
// to the same limits as a heartbeat's collected_at:
//
//   - a numeric offset needs hour 00-23 and minute 00-59 with no carry-over
//     (+00:60 and ±24:00 are rejected, not folded); Z, -00:00 and ±23:59
//     stay legal;
//   - a fractional second (with "." or ",") of at most nine digits is kept
//     whole; longer is accepted only when every digit past the ninth is
//     zero — any later non-zero digit is an error, never a truncation;
//   - a rejection exits non-zero, prints no health result or baseline
//     explanation on stdout, and names --at plus the offending offset or
//     fraction on stderr — even when the node has no telemetry or a
//     missed-duty baseline is given, and without touching saved heartbeats;
//   - legal query times keep the existing semantics: exactly 60 s is
//     online, one nanosecond more is offline, and the same instant written
//     in another timezone judges identically.

import (
	"strings"
	"testing"
)

// seedQueryNode stores one record collected exactly at cliReceiveAt, so the
// online boundary at query time 12:01:00Z is exactly 60 seconds.
func seedQueryNode(t *testing.T, dir string) {
	t.Helper()
	submitBatch(t, dir, `[{"node":"n1","seq":1,"collected_at":"2026-10-01T12:00:00Z","version":"1.0","height":1,"missed":0}]`)
}

// healthAtArgs are the required health flags for node n1.
var healthAtArgs = []string{"--node", "n1", "--expected-version", "1.0", "--tolerated-misses", "0"}

// TestCLIHealthRejectsOutOfRangeQueryOffset pins that an offset time.Parse
// would fold (+00:60 -> +01:00) or accept past the writable range (±24:00)
// is a command error naming --at and the offending offset, never a health
// result computed at a moment the user did not write.
func TestCLIHealthRejectsOutOfRangeQueryOffset(t *testing.T) {
	cases := []struct {
		name   string
		at     string
		offset string
	}{
		{"minute 60 not folded into the hour", "2026-10-01T12:00:30+00:60", "+00:60"},
		{"minute 99", "2026-10-01T12:00:30+05:99", "+05:99"},
		{"plus 24:00", "2026-10-01T12:00:30+24:00", "+24:00"},
		{"minus 24:00", "2026-09-30T12:00:30-24:00", "-24:00"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			seedQueryNode(t, dir)
			before := readNodeFile(t, dir, "n1")

			out, errOut, code := healthQueryAt(t, dir, tc.at, healthAtArgs...)
			if code == 0 {
				t.Errorf("offset %s must fail non-zero; stdout=%q", tc.offset, out)
			}
			if out != "" {
				t.Errorf("no health result may be printed on stdout, got %q", out)
			}
			for _, want := range []string{"--at", tc.offset} {
				if !strings.Contains(errOut, want) {
					t.Errorf("stderr must contain %q, got %q", want, errOut)
				}
			}
			if got := readNodeFile(t, dir, "n1"); got != before {
				t.Errorf("saved heartbeats changed after a rejected query")
			}
		})
	}
}

// TestCLIHealthRejectsUnrepresentableQueryFraction is the reported regression:
// the latest heartbeat was collected at 12:00:00Z, so a query at
// 12:01:00.0000000001Z is 60 s and a tenth of a nanosecond later — a moment
// nanosecond precision cannot write. The command must report the precision
// error, not truncate to exactly 60 s and report online. The comma decimal
// separator follows the same rule.
func TestCLIHealthRejectsUnrepresentableQueryFraction(t *testing.T) {
	cases := []struct {
		name string
		at   string
		frac string
	}{
		{"tenth digit non-zero", "2026-10-01T12:01:00.0000000001Z", ".0000000001"},
		{"non-zero far past the ninth", "2026-10-01T12:01:00.5000000000001Z", ".5000000000001"},
		{"comma separator, tenth digit non-zero", "2026-10-01T12:01:00,0000000001Z", ",0000000001"},
		{"non-zero tenth digit with offset", "2026-10-01T20:01:00.1234567891+08:00", ".1234567891"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			seedQueryNode(t, dir)
			before := readNodeFile(t, dir, "n1")

			out, errOut, code := healthQueryAt(t, dir, tc.at, healthAtArgs...)
			if code == 0 {
				t.Errorf("fraction %s must fail non-zero, not report online; stdout=%q", tc.frac, out)
			}
			if strings.Contains(out, "status=") {
				t.Errorf("no health result may be printed on stdout, got %q", out)
			}
			for _, want := range []string{"--at", tc.frac} {
				if !strings.Contains(errOut, want) {
					t.Errorf("stderr must contain %q, got %q", want, errOut)
				}
			}
			if got := readNodeFile(t, dir, "n1"); got != before {
				t.Errorf("saved heartbeats changed after a rejected query")
			}
		})
	}
}

// TestCLIHealthQueryTimeCheckedWithoutTelemetryOrBaseline pins the check
// ordering: an invalid --at is refused before any store access, so a node
// with no telemetry must not get its usual 无遥测 result, a query carrying a
// missed-duty baseline must not print the baseline explanation, and neither
// may fall back to the current time.
func TestCLIHealthQueryTimeCheckedWithoutTelemetryOrBaseline(t *testing.T) {
	dir := t.TempDir()
	seedQueryNode(t, dir)
	before := readNodeFile(t, dir, "n1")

	const badAt = "2026-10-01T12:01:00.0000000001Z"

	// No telemetry for this node at all: still a query-time error, not 无遥测.
	out, errOut, code := healthQueryAt(t, dir, badAt,
		"--node", "ghost", "--expected-version", "1.0", "--tolerated-misses", "0")
	if code == 0 {
		t.Errorf("bad --at on a node without telemetry must fail; stdout=%q", out)
	}
	if strings.Contains(out, "无遥测") || strings.Contains(out, "status=") {
		t.Errorf("no health result may be printed, got %q", out)
	}
	if !strings.Contains(errOut, "--at") || !strings.Contains(errOut, ".0000000001") {
		t.Errorf("stderr must name --at and the fraction, got %q", errOut)
	}

	// With a missed-duty baseline: the query time is still checked first, and
	// no baseline explanation line is printed either.
	out, errOut, code = healthQueryAt(t, dir, badAt,
		"--node", "n1", "--expected-version", "1.0", "--tolerated-misses", "0", "--missed-since-seq", "1")
	if code == 0 {
		t.Errorf("bad --at with a baseline must fail; stdout=%q", out)
	}
	if strings.Contains(out, "baseline_seq") || strings.Contains(out, "status=") {
		t.Errorf("no result or baseline explanation may be printed, got %q", out)
	}
	if !strings.Contains(errOut, "--at") {
		t.Errorf("stderr must name --at, got %q", errOut)
	}

	if got := readNodeFile(t, dir, "n1"); got != before {
		t.Errorf("saved heartbeats changed after rejected queries")
	}
}

// TestCLIHealthQueryTimeRequiresTimezone pins that an explicit --at without a
// timezone is rejected; only an omitted --at defaults to the current time.
func TestCLIHealthQueryTimeRequiresTimezone(t *testing.T) {
	dir := t.TempDir()
	seedQueryNode(t, dir)

	out, errOut, code := healthQueryAt(t, dir, "2026-10-01T12:01:00", healthAtArgs...)
	if code == 0 {
		t.Errorf("--at without a timezone must fail; stdout=%q", out)
	}
	if out != "" {
		t.Errorf("no health result may be printed, got %q", out)
	}
	if !strings.Contains(errOut, "--at") || !strings.Contains(errOut, "timezone") {
		t.Errorf("stderr must name --at and the timezone requirement, got %q", errOut)
	}
}

// TestCLIHealthAcceptsBoundaryQueryTimes pins the legal forms and the exact
// nanosecond semantics through the real command: exactly 60 s after the
// collection is online, one nanosecond more is offline, and the same instant
// respelled — trailing-zero fraction, comma separator, -00:00, ±23:59 or
// another timezone — judges identically.
func TestCLIHealthAcceptsBoundaryQueryTimes(t *testing.T) {
	dir := t.TempDir()
	seedQueryNode(t, dir)

	cases := []struct {
		name   string
		at     string
		status string
	}{
		{"exactly 60s is online", "2026-10-01T12:01:00Z", "online"},
		{"60s plus one nanosecond is offline", "2026-10-01T12:01:00.000000001Z", "offline"},
		{"trailing-zero long fraction keeps exactly 60s", "2026-10-01T12:01:00.0000000000Z", "online"},
		{"comma fraction 60.5s is offline", "2026-10-01T12:01:00,5Z", "offline"},
		{"nine-digit fraction 60.5s is offline", "2026-10-01T12:01:00.500000000Z", "offline"},
		{"minus zero offset is legal", "2026-10-01T12:01:00-00:00", "online"},
		{"same instant in +08:00", "2026-10-01T20:01:00+08:00", "online"},
		{"same instant in -04:00", "2026-10-01T08:01:00-04:00", "online"},
		{"plus 23:59 boundary, same instant", "2026-10-02T12:00:00+23:59", "online"},
		{"minus 23:59 boundary, same instant", "2026-09-30T12:02:00-23:59", "online"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, errOut, code := healthQueryAt(t, dir, tc.at, healthAtArgs...)
			if code != 0 {
				t.Fatalf("legal query time %q must succeed: exit=%d stderr=%q", tc.at, code, errOut)
			}
			if !strings.Contains(out, "status="+tc.status) {
				t.Errorf("query at %q: stdout=%q, want status=%s", tc.at, out, tc.status)
			}
		})
	}
}
