package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestMain re-executes the test binary as independent worker processes so the
// flock, WAL and batch atomicity are exercised across real OS processes, not
// just goroutines sharing one address space.
func TestMain(m *testing.M) {
	switch mode := os.Getenv("EF_HELPER_MODE"); mode {
	case "receive", "query", "history":
		runHeartbeatReceiveCLI(mode, os.Args[2:])
	default:
		os.Exit(m.Run())
	}
	panic("unreachable")
}

// runHeartbeatReceiveCLI dispatches one CLI command inside the helper process.
// Command handlers call os.Exit(1) on failure (via fail), which terminates the
// helper with a non-zero status the parent can observe.
func runHeartbeatReceiveCLI(mode string, args []string) {
	switch mode {
	case "receive":
		runHeartbeatReceive(args)
		os.Exit(0)
	case "query":
		runHeartbeatQuery(args)
		os.Exit(0)
	case "history":
		runHeartbeatHistory(args)
		os.Exit(0)
	}
}

type helperResult struct {
	exitCode int
	stdout   bytes.Buffer
	stderr   bytes.Buffer
}

func runHelper(t *testing.T, mode, dir string, stdin string, extra ...string) helperResult {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	args := append([]string{"helper", "--data-dir", dir}, extra...)
	cmd := exec.Command(exe, args...)
	cmd.Env = append(os.Environ(), "EF_HELPER_MODE="+mode)
	cmd.Stdin = strings.NewReader(stdin)
	var r helperResult
	cmd.Stdout = &r.stdout
	cmd.Stderr = &r.stderr
	err = cmd.Run()
	if ee, ok := err.(*exec.ExitError); ok {
		r.exitCode = ee.ExitCode()
	} else if err != nil {
		t.Fatalf("helper %s failed to run: %v", mode, err)
	} else {
		r.exitCode = 0
	}
	return r
}

func receiveJSON(t *testing.T, r helperResult) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(r.stdout.Bytes()), &out); err != nil {
		t.Fatalf("bad helper JSON %q (stderr=%s): %v", r.stdout.String(), r.stderr.String(), err)
	}
	return out
}

const fixedAt = "2026-10-01T12:00:10+08:00"

func recordJSON(node string, seq, height, missed int64, collected string) string {
	return fmt.Sprintf(`{"node_id":%q,"seq":%d,"collected_at":%q,"version":"1.26.0","height":%d,"missed":%d}`,
		node, seq, collected, height, missed)
}

func TestCrossProcessIdenticalInsertedOnce(t *testing.T) {
	dir := t.TempDir()
	const n = 12
	payload := "[" + recordJSON("val-1", 300, 300, 0, "2026-10-01T12:00:00+08:00") + "]"

	start := make(chan struct{})
	results := make([]helperResult, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			results[i] = runHelper(t, "receive", dir, payload, "--at", fixedAt)
		}(i)
	}
	close(start)
	wg.Wait()

	var inserted, duplicated int
	for i, r := range results {
		if r.exitCode != 0 {
			t.Fatalf("worker %d failed: %s", i, r.stderr.String())
		}
		out := receiveJSON(t, r)
		inserted += int(out["inserted"].(float64))
		duplicated += int(out["duplicated"].(float64))
	}
	if inserted != 1 {
		t.Fatalf("cross-process identical: total inserted=%d, want 1", inserted)
	}
	if duplicated != n-1 {
		t.Fatalf("cross-process identical: duplicated=%d, want %d", duplicated, n-1)
	}
	r := runHelper(t, "history", dir, "", "--node", "val-1")
	out := receiveJSON(t, r)
	if int(out["count"].(float64)) != 1 {
		t.Fatalf("stored record count=%v, want 1", out["count"])
	}
}

func TestCrossProcessConflictSingleWinner(t *testing.T) {
	dir := t.TempDir()
	const n = 12

	start := make(chan struct{})
	results := make([]helperResult, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			// Same node+seq, different height per worker.
			payload := "[" + recordJSON("val-2", 200, int64(1000+i), 0, "2026-10-01T12:00:00+08:00") + "]"
			results[i] = runHelper(t, "receive", dir, payload, "--at", fixedAt)
		}(i)
	}
	close(start)
	wg.Wait()

	winners, conflicts := 0, 0
	for i, r := range results {
		switch r.exitCode {
		case 0:
			out := receiveJSON(t, r)
			if int(out["inserted"].(float64)) != 1 {
				t.Fatalf("worker %d winner did not insert 1: %s", i, r.stdout.String())
			}
			winners++
		case 1:
			if !strings.Contains(r.stderr.String(), "冲突") {
				t.Fatalf("worker %d unexpected failure: %s", i, r.stderr.String())
			}
			conflicts++
		default:
			t.Fatalf("worker %d unexpected exit %d: %s", i, r.exitCode, r.stderr.String())
		}
	}
	if winners != 1 || conflicts != n-1 {
		t.Fatalf("want exactly 1 winner / %d conflicts, got %d winners / %d conflicts",
			n-1, winners, conflicts)
	}
	r := runHelper(t, "history", dir, "", "--node", "val-2")
	out := receiveJSON(t, r)
	if int(out["count"].(float64)) != 1 {
		t.Fatalf("conflicting commits stored count=%v, want 1", out["count"])
	}
}

func TestCrossProcessNoHalfBatchVisible(t *testing.T) {
	dir := t.TempDir()

	// Inserter publishes a 2-record batch for node "main".
	batch := "[" +
		recordJSON("main", 100, 100, 0, "2026-10-01T12:00:00+08:00") + "," +
		recordJSON("main", 101, 101, 0, "2026-10-01T12:00:01+08:00") + "]"

	// Spoilers hammer a different key so they contend for the lock but can
	// never partially touch node "main". Their first identical insert lands
	// once; subsequent variants conflict and are rejected — all harmless noise
	// that maximises interleaving.
	stop := make(chan struct{})
	var spoilWG sync.WaitGroup
	for w := 0; w < 4; w++ {
		spoilWG.Add(1)
		go func(w int) {
			defer spoilWG.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				payload := "[" + recordJSON("spoiler", 1, int64(w*100000+i), 0,
					"2026-10-01T12:00:00+08:00") + "]"
				runHelper(t, "receive", dir, payload, "--at", fixedAt)
			}
		}(w)
	}

	// Query workers must only ever observe 0 or both records for node "main".
	var queryWG sync.WaitGroup
	bad := make(chan string, 1000)
	for w := 0; w < 4; w++ {
		queryWG.Add(1)
		go func() {
			defer queryWG.Done()
			deadline := time.Now().Add(800 * time.Millisecond)
			for time.Now().Before(deadline) {
				r := runHelper(t, "history", dir, "", "--node", "main")
				out := receiveJSON(t, r)
				cnt := int(out["count"].(float64))
				if cnt != 0 && cnt != 2 {
					bad <- fmt.Sprintf("observed half batch: count=%d", cnt)
					return
				}
			}
		}()
	}

	time.Sleep(150 * time.Millisecond)
	ins := runHelper(t, "receive", dir, batch, "--at", fixedAt)
	if ins.exitCode != 0 {
		t.Fatalf("insert batch failed: %s", ins.stderr.String())
	}

	queryWG.Wait()
	close(stop)
	spoilWG.Wait()
	close(bad)
	for msg := range bad {
		t.Error(msg)
	}

	final := runHelper(t, "history", dir, "", "--node", "main")
	if c := int(receiveJSON(t, final)["count"].(float64)); c != 2 {
		t.Fatalf("final main count=%d, want 2", c)
	}
}

func TestCrossProcessRestartConsistency(t *testing.T) {
	dir := t.TempDir()
	p1 := "[" + recordJSON("edge-9", 1, 1, 0, "2026-10-01T12:00:00+08:00") + "]"
	if r := runHelper(t, "receive", dir, p1, "--at", fixedAt); r.exitCode != 0 {
		t.Fatalf("initial receive: %s", r.stderr.String())
	}

	// A fresh process (simulated platform restart) sees history, dedup and
	// current telemetry consistently.
	r := runHelper(t, "history", dir, "", "--node", "edge-9")
	if c := int(receiveJSON(t, r)["count"].(float64)); c != 1 {
		t.Fatalf("after restart history count=%d", c)
	}
	if r := runHelper(t, "receive", dir, p1, "--at", fixedAt); r.exitCode != 0 {
		t.Fatalf("resubmit: %s", r.stderr.String())
	} else if d := int(receiveJSON(t, r)["duplicated"].(float64)); d != 1 {
		t.Fatalf("after restart duplicated=%d, want 1", d)
	}
	conflict := "[" + recordJSON("edge-9", 1, 2, 0, "2026-10-01T12:00:00+08:00") + "]"
	if r := runHelper(t, "receive", dir, conflict, "--at", fixedAt); r.exitCode == 0 {
		t.Fatal("conflict after restart must fail")
	}
}

func TestParseBatchAcceptsThreeShapes(t *testing.T) {
	rec := recordJSON("n", 1, 1, 0, "2026-10-01T12:00:00+08:00")
	for name, input := range map[string]string{
		"array":    "[" + rec + "]",
		"envelope": `{"heartbeats":[` + rec + `]}`,
		"single":   rec,
	} {
		t.Run(name, func(t *testing.T) {
			got, err := parseBatch([]byte(input))
			if err != nil || len(got) != 1 || got[0].NodeID != "n" {
				t.Fatalf("parse %s: got=%v err=%v", name, got, err)
			}
		})
	}
	if _, err := parseBatch(nil); err == nil {
		t.Fatal("empty input must error")
	}
}

func TestCLIEndToEndCorruptionRefused(t *testing.T) {
	dir := t.TempDir()
	p1 := "[" + recordJSON("n", 1, 1, 0, "2026-10-01T12:00:00+08:00") + "]"
	if r := runHelper(t, "receive", dir, p1, "--at", fixedAt); r.exitCode != 0 {
		t.Fatal(r.stderr.String())
	}
	logPath := filepath.Join(dir, "heartbeat.log")
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	// Damage a byte in the middle of the committed payload (frame header is
	// 9 bytes: 4 magic + 1 type + 4 length; then 8-byte batch id; +10 lands
	// inside the first heartbeat JSON, breaking its CRC).
	const frameHeaderSize = 9
	data[frameHeaderSize+8+10] ^= 0xFF
	if err := os.WriteFile(logPath, data, 0o644); err != nil {
		t.Fatal(err)
	}
	r := runHelper(t, "query", dir, "",
		"--node", "n", "--expected-version", "1.26.0", "--at", fixedAt)
	if r.exitCode == 0 || !strings.Contains(r.stderr.String(), "损坏") {
		t.Fatalf("corrupt data must be refused, exit=%d stderr=%s", r.exitCode, r.stderr.String())
	}
	if r := runHelper(t, "receive", dir, p1, "--at", fixedAt); r.exitCode == 0 {
		t.Fatal("must not keep writing to a corrupt directory")
	}
	// The damaged file must remain on disk, never silently reset.
	if info, err := os.Stat(logPath); err != nil || info.Size() == 0 {
		t.Fatalf("corrupt log vanished: info=%v err=%v", info, err)
	}
}
