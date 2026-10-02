// Command edgefleet is the 验证者与边缘节点机群管理平台 entry point.
package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/asdhoaiqqq/edgefleet-validator/edgefleet"
)

func main() {
	command := "demo"
	if len(os.Args) > 1 {
		command = os.Args[1]
	}
	switch command {
	case "demo":
		runDemo()
	case "version":
		fmt.Println("edgefleet 0.1.0")
	case "aggregate":
		runAggregate(os.Args[2:])
	case "help", "-h", "--help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", command)
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Print(`usage: edgefleet [demo|version|aggregate|help]

commands:
  demo       run the fleet demo
  version    print version
  aggregate  aggregate fixed-length event-time windows from JSON records
  help       show this help

aggregate --window-ms <ms>
  Reads JSON records line by line from standard input and emits fixed-length
  windows driven by the input watermark. The wall clock is never used.
  <ms> must be a positive signed 64-bit integer.

  Event record:     {"type":"event","key":"sensor-a","time":1200,"value":5}
  Watermark record: {"type":"watermark","time":2000}

  time is a non-negative signed 64-bit integer millisecond timestamp; value
  is a signed 64-bit integer; key is a non-empty string.

  Windows start at time zero and are [start, end) intervals. Every valid
  event falling in a window for a key contributes to count and sum; windows
  with no events produce no output. Records may arrive out of order, but
  window assignment uses event time only.

  A watermark declares that events with time less than the watermark are
  late. Before the first watermark, all valid events are accepted. Once a
  watermark is received, only windows with end <= watermark are closed.
  Events with time equal to the current watermark are still valid; events
  with time below it are skipped (reported on stderr with line number, event
  time and current watermark) and processing continues.

  Each closed window is emitted as one JSON object with key, start, end,
  count, sum. When several windows close at once they are ordered by end
  ascending, then by key in UTF-8 byte order; each window is emitted once.
  Watermarks may jump or repeat, but must not regress. Reaching input end
  closes nothing: unclosed windows are not emitted. The same input with the
  same window length always produces identical output.
`)
}

func runAggregate(args []string) {
	windowMs, err := parseWindowMs(args)
	if err != nil {
		fmt.Fprintf(os.Stderr, "aggregate: %v\n", err)
		usage()
		os.Exit(2)
	}
	if err := edgefleet.RunAggregate(os.Stdin, os.Stdout, os.Stderr, windowMs); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}

// parseWindowMs accepts --window-ms <ms> and --window-ms=<ms> forms.
func parseWindowMs(args []string) (int64, error) {
	if len(args) == 0 {
		return 0, fmt.Errorf("missing --window-ms <ms>")
	}
	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch {
		case arg == "--window-ms":
			if i+1 >= len(args) {
				return 0, fmt.Errorf("--window-ms requires a value")
			}
			s := args[i+1]
			v, err := strconv.ParseInt(s, 10, 64)
			if err != nil || v <= 0 {
				return 0, fmt.Errorf("--window-ms must be a positive 64-bit integer, got %q", s)
			}
			return v, nil
		case strings.HasPrefix(arg, "--window-ms="):
			s := arg[len("--window-ms="):]
			v, err := strconv.ParseInt(s, 10, 64)
			if err != nil || v <= 0 {
				return 0, fmt.Errorf("--window-ms must be a positive 64-bit integer, got %q", s)
			}
			return v, nil
		default:
			return 0, fmt.Errorf("unknown argument %q", arg)
		}
	}
	return 0, fmt.Errorf("missing --window-ms <ms>")
}

func runDemo() {
	nodes := []edgefleet.Node{
		{ID: "val-eu-1", Region: "eu", Version: "1.26.0", Online: true},
		{ID: "val-us-1", Region: "us", Version: "1.25.4", Online: true, Missed: 3},
		{ID: "edge-ap-1", Region: "ap", Version: "1.26.0", Online: false},
	}
	unhealthy := 0
	for _, node := range nodes {
		health := edgefleet.Evaluate(node, "1.26.0", 1)
		if !health.Healthy {
			unhealthy++
		}
		fmt.Printf("node=%s healthy=%v findings=%v\n", health.Node, health.Healthy, health.Findings)
	}
	fmt.Println("rollout order:", edgefleet.Plan(nodes, "1.26.0"))
	fmt.Printf("summary: %d of %d nodes need attention\n", unhealthy, len(nodes))
}
