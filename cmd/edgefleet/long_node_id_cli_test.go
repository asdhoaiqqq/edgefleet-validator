package main

// End-to-end command-line tests for long node ids. The package tests cover the
// store mapping and ownership rules; these tests exercise what a user actually
// does through the real command: submitting, querying health and history,
// seeing "no telemetry" for a never-saved long id, and getting an explicit
// corruption refusal when another node's file occupies the slot.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// longIDJSON builds one heartbeat object for the given long node.
func longIDJSON(node string, seq, height, missed int, version, collected string) string {
	return `[{"node":` + quoteJSON(node) + `,"seq":` + itoa(seq) +
		`,"collected_at":"` + collected + `","version":"` + version +
		`","height":` + itoa(height) + `,"missed":` + itoa(missed) + `}]`
}

func quoteJSON(s string) string {
	b := strings.Builder{}
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte('"')
	return b.String()
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [24]byte
	i := len(buf)
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

func TestCLILongNodeIDSubmitHealthHistory(t *testing.T) {
	dir := t.TempDir()
	collected := "2026-10-01T11:59:59Z"

	cases := []struct {
		name string
		node string
	}{
		{"126 ascii", strings.Repeat("a", 126)},
		{"127 ascii", strings.Repeat("b", 127)},
		{"42 hanzi", strings.Repeat("汉", 42)},
		{"hanzi emoji spaces", strings.Repeat("节", 40) + "😀 " + strings.Repeat("x", 5)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			payload := longIDJSON(tc.node, 1, 12345, 0, "1.26.0", collected)
			out, errOut, code := runCLI(t, dir, payload,
				"heartbeat", "submit", "--data-dir", dir, "--receive-time", cliReceiveAt)
			if code != 0 || !strings.Contains(out, "new=1 duplicate=0") {
				t.Fatalf("submit long id failed: code=%d out=%q err=%q", code, out, errOut)
			}

			// Resubmitting the same record is a duplicate, not new.
			out, errOut, code = runCLI(t, dir, payload,
				"heartbeat", "submit", "--data-dir", dir, "--receive-time", cliReceiveAt)
			if code != 0 || !strings.Contains(out, "new=0 duplicate=1") {
				t.Fatalf("resubmit must be a duplicate: code=%d out=%q err=%q", code, out, errOut)
			}

			out, errOut, code = runCLI(t, dir, "",
				"heartbeat", "health", "--data-dir", dir, "--node", tc.node,
				"--expected-version", "1.26.0", "--tolerated-misses", "0", "--at", cliQueryAt)
			if code != 0 {
				t.Fatalf("health long id failed: code=%d err=%q", code, errOut)
			}
			for _, want := range []string{"status=online", "seq=1", "version=1.26.0", "height=12345", "missed=0"} {
				if !strings.Contains(out, want) {
					t.Errorf("health output %q missing %q", out, want)
				}
			}
			// The node is shown by its full original text (JSON-quoted when
			// required by the display rule), not by any hash or truncation.
			if !strings.Contains(out, "node=") {
				t.Errorf("health output missing node: %q", out)
			}

			out, errOut, code = runCLI(t, dir, "",
				"heartbeat", "history", "--data-dir", dir, "--node", tc.node)
			if code != 0 {
				t.Fatalf("history long id failed: code=%d err=%q", code, errOut)
			}
			if !strings.Contains(out, "seq=1") || !strings.Contains(out, "height=12345") {
				t.Errorf("history output wrong: %q", out)
			}
		})
	}

	// Exactly one slot file per long node exists, plus no nodes/ files for
	// them; the slot names are all short.
	slotEntries, err := os.ReadDir(filepath.Join(dir, "slots"))
	if err != nil {
		t.Fatal(err)
	}
	if len(slotEntries) != len(cases) {
		t.Fatalf("slots=%d, want %d", len(slotEntries), len(cases))
	}
	for _, e := range slotEntries {
		if len(e.Name()) > 255 {
			t.Errorf("slot name too long: %d bytes", len(e.Name()))
		}
	}
	nodeEntries, err := os.ReadDir(filepath.Join(dir, "nodes"))
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if len(nodeEntries) != 0 {
		t.Errorf("long ids must not create files under nodes/, found %d", len(nodeEntries))
	}
}

func TestCLILongNodeIDMultipleSeqsAscendingAndGreatestWins(t *testing.T) {
	dir := t.TempDir()
	node := strings.Repeat("very-long-node-id-", 8)
	submit := func(seq, height, missed int, collected string) {
		t.Helper()
		out, errOut, code := runCLI(t, dir,
			longIDJSON(node, seq, height, missed, "1.0", collected),
			"heartbeat", "submit", "--data-dir", dir, "--receive-time", cliReceiveAt)
		if code != 0 {
			t.Fatalf("submit seq %d failed: out=%q err=%q", seq, out, errOut)
		}
	}
	// Out of order; seq 5 has the earliest collection time but is greatest, so
	// its stale instant drives health offline at the query instant.
	submit(5, 500, 5, "2026-10-01T11:58:00Z")
	submit(1, 100, 1, "2026-10-01T11:59:57Z")
	submit(3, 300, 3, "2026-10-01T11:59:58Z")

	out, _, code := runCLI(t, dir, "",
		"heartbeat", "history", "--data-dir", dir, "--node", node)
	if code != 0 {
		t.Fatalf("history failed")
	}
	lines := strings.Split(strings.TrimRight(out, "\n"), "\n")
	if len(lines) != 3 {
		t.Fatalf("history lines=%d, want 3: %q", len(lines), out)
	}
	for i, h := range []string{"seq=1", "seq=3", "seq=5"} {
		if !strings.Contains(lines[i], h) {
			t.Errorf("history position %d = %q, want %s", i, lines[i], h)
		}
	}

	// Health takes every field from the greatest seq, including its stale
	// collection time: offline at the query instant.
	out, errOut, code := runCLI(t, dir, "",
		"heartbeat", "health", "--data-dir", dir, "--node", node,
		"--expected-version", "1.0", "--tolerated-misses", "4", "--at", cliQueryAt)
	if code != 0 {
		t.Fatalf("health failed: %q", errOut)
	}
	for _, want := range []string{"status=offline", "seq=5", "height=500", "missed=5", "missed duties above tolerance"} {
		if !strings.Contains(out, want) {
			t.Errorf("health output %q missing %q", out, want)
		}
	}
}

func TestCLILongNodeIDUnsavedQueries(t *testing.T) {
	dir := t.TempDir()
	node := strings.Repeat("幽灵节点-", 22)

	out, errOut, code := runCLI(t, dir, "",
		"heartbeat", "health", "--data-dir", dir, "--node", node,
		"--expected-version", "1.0", "--tolerated-misses", "0", "--at", cliQueryAt)
	if code != 0 {
		t.Fatalf("health on an unsaved long id must succeed: code=%d err=%q", code, errOut)
	}
	if !strings.Contains(out, "status=无遥测") || !strings.Contains(out, "findings=[无遥测]") {
		t.Errorf("want no-telemetry line, got %q", out)
	}

	out, errOut, code = runCLI(t, dir, "",
		"heartbeat", "history", "--data-dir", dir, "--node", node)
	if code != 0 {
		t.Fatalf("history on an unsaved long id must succeed: code=%d err=%q", code, errOut)
	}
	if !strings.Contains(out, "no heartbeats") {
		t.Errorf("want no-heartbeats line, got %q", out)
	}

	// Queries never create storage.
	entries, err := os.ReadDir(filepath.Join(dir, "slots"))
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Errorf("querying must not create slot files, found %d", len(entries))
	}
}

func TestCLILongNodeIDsSharingPrefixStayDistinct(t *testing.T) {
	dir := t.TempDir()
	prefix := strings.Repeat("fleet/region-edge-", 8) // 144 bytes
	nodeA, nodeB := prefix+"A", prefix+"B"

	submitBatch(t, dir, longIDJSON(nodeA, 7, 111, 1, "1.0", "2026-10-01T11:59:59Z"))
	submitBatch(t, dir, longIDJSON(nodeB, 7, 222, 2, "1.0", "2026-10-01T11:59:59Z"))

	for _, tc := range []struct {
		node   string
		height string
		missed string
	}{{nodeA, "height=111", "missed=1"}, {nodeB, "height=222", "missed=2"}} {
		out, errOut, code := runCLI(t, dir, "",
			"heartbeat", "health", "--data-dir", dir, "--node", tc.node,
			"--expected-version", "1.0", "--tolerated-misses", "9", "--at", cliQueryAt)
		if code != 0 {
			t.Fatalf("health failed: %q", errOut)
		}
		if !strings.Contains(out, "seq=7") || !strings.Contains(out, tc.height) || !strings.Contains(out, tc.missed) {
			t.Errorf("node read the other node's telemetry: %q", out)
		}
	}
}

func TestCLILongNodeIDEscapedSpellingIsSameNode(t *testing.T) {
	dir := t.TempDir()
	// 122 ASCII bytes then an emoji: literal form.
	node := strings.Repeat("a", 122) + "😀"
	literal := longIDJSON(node, 1, 1, 0, "1.0", "2026-10-01T11:59:59Z")
	out, _, code := runCLI(t, dir, literal,
		"heartbeat", "submit", "--data-dir", dir, "--receive-time", cliReceiveAt)
	if code != 0 || !strings.Contains(out, "new=1") {
		t.Fatalf("literal submit failed: %q", out)
	}
	// Same id with the emoji written as a paired \u escape.
	bs := "\\"
	escaped := `[{"node":"` + strings.Repeat("a", 122) + bs + `uD83D` + bs + `uDE00",` +
		`"seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"1.0","height":1,"missed":0}]`
	out, _, code = runCLI(t, dir, escaped,
		"heartbeat", "submit", "--data-dir", dir, "--receive-time", cliReceiveAt)
	if code != 0 || !strings.Contains(out, "new=0 duplicate=1") {
		t.Fatalf("escaped spelling must be the same node (duplicate): %q", out)
	}
}

func TestCLILongNodeIDForeignSlotFileIsRefused(t *testing.T) {
	dir := t.TempDir()
	victim := strings.Repeat("victim-node-", 11) // 132 bytes
	other := strings.Repeat("other-node--", 11) + "X"
	// Save the other long node: a complete, well-formed file with a valid
	// checksum.
	submitBatch(t, dir, longIDJSON(other, 1, 777, 7, "1.0", "2026-10-01T11:59:59Z"))
	// Plant that file at the victim's slot.
	data, err := os.ReadFile(nodeFileHexPath(dir, other))
	if err != nil {
		t.Fatal(err)
	}
	victimPath := nodeFileHexPath(dir, victim)
	if err := os.MkdirAll(filepath.Dir(victimPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(victimPath, data, 0o644); err != nil {
		t.Fatal(err)
	}

	// Every query refuses with an explicit corruption error, never "no
	// telemetry", naming the victim slot and both node ids.
	out, errOut, code := runCLI(t, dir, "",
		"heartbeat", "health", "--data-dir", dir, "--node", victim,
		"--expected-version", "1.0", "--tolerated-misses", "0", "--at", cliQueryAt)
	if code == 0 {
		t.Errorf("health must exit non-zero on a foreign slot file; stdout=%q", out)
	}
	if !strings.Contains(errOut, "data corruption") ||
		!strings.Contains(errOut, filepath.Base(victimPath)) ||
		!strings.Contains(errOut, "record 1") {
		t.Errorf("health stderr must name the corrupt slot and record, got %q", errOut)
	}
	if !strings.Contains(errOut, "victim") || !strings.Contains(errOut, "other") {
		t.Errorf("error must name both the file owner and the foreign node, got %q", errOut)
	}

	out, errOut, code = runCLI(t, dir, "",
		"heartbeat", "history", "--data-dir", dir, "--node", victim)
	if code == 0 || out != "" {
		t.Errorf("history must refuse and print nothing: code=%d out=%q", code, out)
	}
	if !strings.Contains(errOut, "data corruption") {
		t.Errorf("history stderr must report corruption, got %q", errOut)
	}

	// Submit refuses and leaves the planted file exactly in place.
	before, err := os.ReadFile(victimPath)
	if err != nil {
		t.Fatal(err)
	}
	out, errOut, code = runCLI(t, dir, longIDJSON(victim, 9, 9, 0, "1.0", "2026-10-01T11:59:59Z"),
		"heartbeat", "submit", "--data-dir", dir, "--receive-time", cliReceiveAt)
	if code == 0 || !strings.Contains(errOut, "data corruption") {
		t.Fatalf("submit must refuse the misowned slot: code=%d out=%q err=%q", code, out, errOut)
	}
	after, err := os.ReadFile(victimPath)
	if err != nil || string(after) != string(before) {
		t.Errorf("the refused submit must not alter the planted file")
	}

	// The real owner of the planted data is unaffected.
	out, _, code = runCLI(t, dir, "",
		"heartbeat", "health", "--data-dir", dir, "--node", other,
		"--expected-version", "1.0", "--tolerated-misses", "9", "--at", cliQueryAt)
	if code != 0 || !strings.Contains(out, "height=777") || !strings.Contains(out, "missed=7") {
		t.Errorf("the planted-from node must stay queryable: code=%d out=%q", code, out)
	}
}

func TestCLILegacyShortNodeStillAtHexPath(t *testing.T) {
	dir := t.TempDir()
	submitBatch(t, dir, `[{"node":"short-甲","seq":1,"collected_at":"2026-10-01T11:59:59Z","version":"1.0","height":42,"missed":0}]`)
	// Data is at the original nodes/hex(id).json location.
	if _, err := os.Stat(nodeFileHexPath(dir, "short-甲")); err != nil {
		t.Fatalf("short node data missing from legacy location: %v", err)
	}
	entries, err := os.ReadDir(filepath.Join(dir, "slots"))
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Errorf("short node must create no slot, found %d", len(entries))
	}
	out, _, code := runCLI(t, dir, "",
		"heartbeat", "health", "--data-dir", dir, "--node", "short-甲",
		"--expected-version", "1.0", "--tolerated-misses", "0", "--at", cliQueryAt)
	if code != 0 || !strings.Contains(out, "height=42") {
		t.Errorf("short node must keep working in place: code=%d out=%q", code, out)
	}
}
