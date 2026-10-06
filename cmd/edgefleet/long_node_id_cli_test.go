package main

// Command-line regression tests for node ids long enough that
// hex(nodeID)+".json" exceeds the local 255-byte file name limit. They run
// through the genuine submit/health/history commands and assert what a user
// actually observes: normal new-count results, complete-id queries, the
// existing no-telemetry / no-heartbeats messages for unknown long ids, and no
// merging of two ids that share a long prefix and differ only at the end.

import (
	"encoding/json"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/asdhoaiqqq/edgefleet-validator/edgefleet"
)

// hbJSON builds a one-element JSON array holding one heartbeat with the node
// id JSON-encoded by the standard library (so emoji, spaces and non-ASCII
// text stay exact).
func hbJSON(node string, seq int, collected, version string, height, missed int) string {
	b, _ := json.Marshal([]map[string]any{{
		"node": node, "seq": seq, "collected_at": collected,
		"version": version, "height": height, "missed": missed,
	}})
	return string(b)
}

func TestCLILongNodeIDSubmitHealthHistory(t *testing.T) {
	dir := t.TempDir()
	ids := map[string]struct {
		version string
		height  int
	}{
		strings.Repeat("a", 126):                   {"1.0", 126},
		strings.Repeat("汉", 43):                    {"v汉", 43},
		"😀 节点 " + strings.Repeat("x", 120) + " 尾部": {"2.0", 77},
	}
	collected := "2026-10-01T11:59:00Z"
	for node, meta := range ids {
		in := hbJSON(node, 1, collected, meta.version, meta.height, 0)
		out, errOut, code := runCLI(t, dir, in,
			"heartbeat", "submit", "--data-dir", dir, "--receive-time", cliReceiveAt)
		if code != 0 {
			t.Fatalf("submit long id exited %d: stderr=%q stdout=%q", code, errOut, out)
		}
		if strings.TrimSpace(out) != "submitted: new=1 duplicate=0" {
			t.Errorf("long id submit output = %q", out)
		}

		f := &fixture{t: t, dir: dir}
		hout, herr, hcode := health(t, f,
			"--node", node, "--expected-version", meta.version, "--tolerated-misses", "0")
		if hcode != 0 || herr != "" {
			t.Fatalf("health long id exit=%d stderr=%q", hcode, herr)
		}
		// The node value is the complete original text under the shared
		// one-line display rule (quoted when it contains spaces).
		wantPrefix := "node=" + edgefleet.DisplayText(node) + " "
		if !strings.HasPrefix(strings.TrimRight(hout, "\n"), wantPrefix) {
			t.Errorf("health line must start with the full node id %q: %q", wantPrefix, hout)
		}
		for _, want := range []string{
			"status=online", "seq=1", "collected_at=" + collected,
			"version=" + edgefleet.DisplayText(meta.version),
			"height=" + strconv.Itoa(meta.height), "missed=0",
		} {
			if !strings.Contains(hout, want) {
				t.Errorf("health output %q missing %q", hout, want)
			}
		}

		// History prints the node text raw and stays one line per record.
		lines := historyLines(t, dir, node)
		if len(lines) != 1 || !strings.HasPrefix(lines[0], "node="+node+" ") ||
			!strings.Contains(lines[0], "seq=1") ||
			!strings.Contains(lines[0], "height="+strconv.Itoa(meta.height)) {
			t.Errorf("history for long id wrong: %q", lines)
		}
	}

	// All three files must be present on disk in sharded form, and no path
	// component anywhere under nodes/ may exceed the local file name limit.
	files := 0
	sharded := 0
	nodesDir := filepath.Join(dir, "nodes")
	err := filepath.WalkDir(nodesDir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if len(filepath.Base(p)) > 255 {
			t.Errorf("path component over the 255-byte limit: %q", filepath.Base(p))
		}
		if d.IsDir() || !strings.HasSuffix(p, ".json") {
			return nil
		}
		files++
		if filepath.Dir(p) != nodesDir {
			sharded++
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if files != len(ids) {
		t.Errorf("files on disk = %d, want %d", files, len(ids))
	}
	if sharded != len(ids) {
		t.Errorf("sharded files = %d, want all %d long ids sharded", sharded, len(ids))
	}
}

func TestCLILongNodeIDUnknownNodeMessages(t *testing.T) {
	dir := t.TempDir()
	f := &fixture{t: t, dir: dir}
	ghost := strings.Repeat("g", 200)

	out, errOut, code := health(t, f,
		"--node", ghost, "--expected-version", "1.0", "--tolerated-misses", "0")
	if code != 0 {
		t.Fatalf("health of an unknown long id must exit 0, got %d stderr=%q", code, errOut)
	}
	if !strings.Contains(out, "status=无遥测") {
		t.Errorf("unknown long id health = %q, want 无遥测", out)
	}

	hout, herr, hcode := historyCLI(t, dir, ghost)
	if hcode != 0 || herr != "" {
		t.Fatalf("history of an unknown long id must exit 0, got %d stderr=%q", hcode, herr)
	}
	if strings.TrimSpace(hout) != "node="+ghost+" no heartbeats" {
		t.Errorf("unknown long id history = %q", hout)
	}
}

func TestCLILongNodeIDsWithSharedPrefixStaySeparate(t *testing.T) {
	dir := t.TempDir()
	prefix := strings.Repeat("shared-prefix-", 12) // 180 chars
	a, b := prefix+"A", prefix+"B"
	submitBatch(t, dir, hbJSON(a, 1, "2026-10-01T11:59:00Z", "1.0", 111, 0))
	submitBatch(t, dir, hbJSON(b, 1, "2026-10-01T11:59:00Z", "1.0", 222, 0))

	la := historyLines(t, dir, a)
	lb := historyLines(t, dir, b)
	if len(la) != 1 || !strings.HasPrefix(la[0], "node="+a+" ") || !strings.Contains(la[0], "height=111") {
		t.Errorf("node A history wrong: %q", la)
	}
	if len(lb) != 1 || !strings.HasPrefix(lb[0], "node="+b+" ") || !strings.Contains(lb[0], "height=222") {
		t.Errorf("node B history wrong: %q", lb)
	}

	// Same seq submitted again to each: duplicates of two distinct nodes, not
	// a conflict and not a merge.
	batch := "[" +
		strings.TrimSuffix(strings.TrimPrefix(hbJSON(a, 1, "2026-10-01T11:59:00Z", "1.0", 111, 0), "["), "]") + "," +
		strings.TrimSuffix(strings.TrimPrefix(hbJSON(b, 1, "2026-10-01T11:59:00Z", "1.0", 222, 0), "["), "]") + "]"
	out, errOut, code := runCLI(t, dir, batch,
		"heartbeat", "submit", "--data-dir", dir, "--receive-time", cliReceiveAt)
	if code != 0 {
		t.Fatalf("duplicate batch exited %d: stderr=%q stdout=%q", code, errOut, out)
	}
	if strings.TrimSpace(out) != "submitted: new=0 duplicate=2" {
		t.Errorf("duplicate batch output = %q", out)
	}
}
