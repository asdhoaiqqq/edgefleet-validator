package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// aggregateBinary is the real command built once in TestMain, so these tests
// exercise the full command-line contract: flag parsing, line-delimited JSON
// on standard input, one JSON object per standard-output line, diagnostics on
// standard error and the process exit code.
var aggregateBinary string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "edgefleet-cli-")
	if err != nil {
		fmt.Fprintln(os.Stderr, "create temp dir:", err)
		os.Exit(1)
	}
	aggregateBinary = filepath.Join(dir, "edgefleet")
	build := exec.Command("go", "build", "-o", aggregateBinary, ".")
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		fmt.Fprintln(os.Stderr, "build edgefleet command:", err)
		os.RemoveAll(dir)
		os.Exit(1)
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

// runAggregateCLI feeds input to "edgefleet aggregate" with the given flags
// and returns the captured standard output, standard error and exit code.
func runAggregateCLI(t *testing.T, input string, args ...string) (stdout, stderr string, exitCode int) {
	t.Helper()
	cmd := exec.Command(aggregateBinary, append([]string{"aggregate"}, args...)...)
	cmd.Stdin = strings.NewReader(input)
	var outBuf, errBuf bytes.Buffer
	cmd.Stdout = &outBuf
	cmd.Stderr = &errBuf
	err := cmd.Run()
	if err != nil {
		exitErr, ok := err.(*exec.ExitError)
		if !ok {
			t.Fatalf("run aggregate: %v", err)
		}
		exitCode = exitErr.ExitCode()
	}
	return outBuf.String(), errBuf.String(), exitCode
}

// The two closed-window results shared by the scenarios below: sliding length
// 1000 with interval 600 and two partitions. The event at time 1000 belongs
// only to [600,1600), so the first window holds only partition 0's event.
var cliSlidingResults = []string{
	`{"key":"k","start":0,"end":1000,"count":1,"sum":2}`,
	`{"key":"k","start":600,"end":1600,"count":2,"sum":5}`,
}

// assertResultShape verifies each line is one complete JSON object carrying
// exactly key, start, end, count, sum and never a partition field.
func assertResultShape(t *testing.T, stdout string, want []string) {
	t.Helper()
	text := strings.TrimSuffix(stdout, "\n")
	if text == "" && len(want) != 0 {
		t.Fatalf("stdout is empty, want %d result lines", len(want))
	}
	gotLines := strings.Split(text, "\n")
	if len(gotLines) != len(want) {
		t.Fatalf("stdout has %d lines, want %d: %q", len(gotLines), len(want), stdout)
	}
	for i, line := range gotLines {
		if line != want[i] {
			t.Errorf("stdout line %d = %q, want %q", i+1, line, want[i])
		}
		var obj map[string]json.RawMessage
		if err := json.Unmarshal([]byte(line), &obj); err != nil {
			t.Errorf("stdout line %d is not a complete JSON object: %v", i+1, err)
			continue
		}
		if _, ok := obj["partition"]; ok {
			t.Errorf("stdout line %d must not carry a partition field: %q", i+1, line)
		}
		var keys []string
		for k := range obj {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		if wantKeys := []string{"count", "end", "key", "start", "sum"}; !reflect.DeepEqual(keys, wantKeys) {
			t.Errorf("stdout line %d fields = %v, want %v", i+1, keys, wantKeys)
		}
	}
	if !strings.HasSuffix(stdout, "\n") {
		t.Errorf("stdout must end with a newline after the last result: %q", stdout)
	}
}

// Window 1000ms, slide 600ms, two partitions, driven entirely from standard
// input: the command must actually combine --window-ms, --slide-ms and
// --partitions. Partition 0 alone cannot close a window; with both partitions
// reporting the effective watermark is min(1600,600)=600 and nothing closes;
// partition 1's idle declaration raises the effective watermark to 1600 and
// both windows emit in order. A normal input ending after the idle record
// exits 0 with empty standard error.
func TestCLIAggregatePartitionedSlidingWindow(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":700,"value":2,"partition":0}`,
		`{"type":"event","key":"k","time":1000,"value":3,"partition":1}`,
		`{"type":"watermark","time":1600,"partition":0}`, // only partition 0: no effective watermark yet
		`{"type":"watermark","time":600,"partition":1}`,  // effective = 600: still below both window ends
		`{"type":"idle","partition":1}`,                  // effective = 1600: closes [0,1000) and [600,1600)
	}, "\n") + "\n"

	stdout, stderr, exitCode := runAggregateCLI(t, input,
		"--window-ms", "1000", "--slide-ms", "600", "--partitions", "2")

	if exitCode != 0 {
		t.Fatalf("exit code = %d, want 0; stderr = %q", exitCode, stderr)
	}
	if stderr != "" {
		t.Fatalf("stderr = %q, want empty", stderr)
	}
	assertResultShape(t, stdout, cliSlidingResults)
}

// Failure boundary: an event arriving while partition 1 stays idle is fatal
// with exit code 1, and the diagnostic must name the idle partition and the
// record's physical input line (the blank line 2 still counts). The event
// time 500 is also below the current effective watermark 1600, so this also
// pins that the idle error wins over the ordinary late-event notice (which
// would skip the record and exit 0). Results fully written before the error
// stay intact, no record after the error may produce further output, and the
// diagnostic must never appear on standard output.
func TestCLIAggregateEventForIdlePartitionFatal(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"k","time":700,"value":2,"partition":0}`, // line 1
		``, // line 2: blank, still counts
		`{"type":"event","key":"k","time":1000,"value":3,"partition":1}`,
		`{"type":"watermark","time":1600,"partition":0}`,
		`{"type":"watermark","time":600,"partition":1}`,
		`{"type":"idle","partition":1}`, // line 6: closes both windows
		// line 7: partition 1 is idle AND event time 500 is below the
		// effective watermark 1600; the idle error must take precedence.
		`{"type":"event","key":"k","time":500,"value":9,"partition":1}`,
		// Records after the failing line must never be read: if processing
		// wrongly continued, this event plus watermark would populate and
		// close a third window [1200,2200).
		`{"type":"event","key":"k","time":2000,"value":7,"partition":0}`,
		`{"type":"watermark","time":2200,"partition":0}`,
	}, "\n") + "\n"

	stdout, stderr, exitCode := runAggregateCLI(t, input,
		"--window-ms", "1000", "--slide-ms", "600", "--partitions", "2")

	if exitCode != 1 {
		t.Fatalf("exit code = %d, want 1", exitCode)
	}
	wantErr := "aggregate: line 7: event for idle partition 1 is not allowed; send a watermark to resume it first\n"
	if stderr != wantErr {
		t.Fatalf("stderr = %q, want %q", stderr, wantErr)
	}
	if strings.Contains(stderr, "late event") {
		t.Fatalf("idle-partition error must not be reported as an ordinary late-event notice: %q", stderr)
	}
	assertResultShape(t, stdout, cliSlidingResults)
	// Every stdout line is a window result; the diagnostic stays on stderr.
	if strings.Contains(stdout, "idle") || strings.Contains(stdout, "aggregate:") {
		t.Fatalf("standard output must contain window results only, got diagnostic text: %q", stdout)
	}
}
