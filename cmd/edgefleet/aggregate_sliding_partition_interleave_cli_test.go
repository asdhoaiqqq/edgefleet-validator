package main

import (
	"strings"
	"testing"
)

// Command-line regression coverage for interleaving invariance of the
// partitioned sliding-window aggregate: merging the same per-partition
// record sequences in different inter-partition orders must not change the
// final result. The library-level tests enumerate every interleaving; here
// two maximally different orders (partition 0 fully first, partition 1 fully
// first) plus one alternating order go through the real command line, so
// flag parsing, standard output/error separation and the exit code are
// exercised end to end. Window length 1000, slide 600, two partitions; each
// partition ends on the same watermark 2200, which closes every window that
// has events. See the edgefleet package's interleave regression tests for
// the fixture's full rationale.

var cliInterleavePart0 = []string{
	`{"type":"event","key":"k","time":700,"value":2,"partition":0}`,
	`{"type":"event","key":"k","time":1000,"value":3,"partition":0}`,
	`{"type":"event","key":"z","time":100,"value":1,"partition":0}`,
	`{"type":"watermark","time":2200,"partition":0}`,
	`{"type":"watermark","time":2200,"partition":0}`, // repeat: no new results
}

var cliInterleavePart1 = []string{
	`{"type":"event","key":"k","time":1300,"value":7,"partition":1}`,
	`{"type":"event","key":"k","time":100,"value":5,"partition":1}`,
	`{"type":"event","key":"a","time":200,"value":4,"partition":1}`,
	`{"type":"watermark","time":1000,"partition":1}`,
	`{"type":"event","key":"k","time":1000,"value":11,"partition":1}`,
	`{"type":"watermark","time":1600,"partition":1}`,
	`{"type":"watermark","time":2200,"partition":1}`,
}

// cliInterleaveWant is the single result every interleaving must print:
// rows ordered by window end and then by decoded key in UTF-8 byte order,
// with no partition field.
var cliInterleaveWant = []windowLine{
	{Key: "a", Start: 0, End: 1000, Count: 1, Sum: 4},
	{Key: "k", Start: 0, End: 1000, Count: 2, Sum: 7},
	{Key: "z", Start: 0, End: 1000, Count: 1, Sum: 1},
	{Key: "k", Start: 600, End: 1600, Count: 4, Sum: 23},
	{Key: "k", Start: 1200, End: 2200, Count: 1, Sum: 7},
}

func TestAggregateCLIPartitionedSlidingInterleaveInvariance(t *testing.T) {
	alternating := []string{
		cliInterleavePart1[0], cliInterleavePart0[0],
		cliInterleavePart1[1], cliInterleavePart0[1],
		cliInterleavePart1[2], cliInterleavePart0[2],
		cliInterleavePart0[3], // p0 watermark 2200 while p1 unreported: closes nothing
		cliInterleavePart1[3], // p1 first report: effective 1000
		cliInterleavePart0[4], // p0 repeat: no new results
		cliInterleavePart1[4],
		cliInterleavePart1[5],
		cliInterleavePart1[6],
	}
	interleavings := map[string]string{
		"partition 0 first": strings.Join(append(append([]string(nil), cliInterleavePart0...), cliInterleavePart1...), "\n") + "\n",
		"partition 1 first": strings.Join(append(append([]string(nil), cliInterleavePart1...), cliInterleavePart0...), "\n") + "\n",
		"alternating":       strings.Join(alternating, "\n") + "\n",
	}

	var firstStdout string
	for name, input := range interleavings {
		t.Run(name, func(t *testing.T) {
			code, stdout, stderr := runAggregateCLI(t, input, "--window-ms", "1000", "--slide-ms", "600", "--partitions", "2")
			if code != 0 {
				t.Fatalf("exit code = %d, want 0; stderr: %s", code, stderr)
			}
			if stderr != "" {
				t.Fatalf("stderr = %q, want empty (no late events in any interleaving)", stderr)
			}
			assertWindowLines(t, stdout, cliInterleaveWant)
			if firstStdout == "" {
				firstStdout = stdout
			} else if stdout != firstStdout {
				t.Fatalf("interleaving changed the output:\n got: %q\nwant: %q", stdout, firstStdout)
			}
		})
	}
}
