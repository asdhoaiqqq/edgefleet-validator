// Command edgefleet is the 验证者与边缘节点机群管理平台 entry point.
package main

import (
	"flag"
	"fmt"
	"os"

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
		os.Exit(runAggregate(os.Args[2:]))
	case "help", "-h", "--help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", command)
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Println("usage: edgefleet [demo|version|aggregate --window-ms <milliseconds> [--partitions <count>]|help]")
	fmt.Println()
	fmt.Println("commands:")
	fmt.Println("  demo       run a built-in fleet health demonstration")
	fmt.Println("  version    print the edgefleet version")
	fmt.Println("  aggregate  aggregate JSON events from standard input into fixed windows")
	fmt.Println("  help       show this help")
	fmt.Println()
	fmt.Println("aggregate:")
	fmt.Println("  edgefleet aggregate --window-ms <milliseconds> [--partitions <count>]")
	fmt.Println()
	fmt.Println("  --window-ms is a required positive signed 64-bit integer. Aggregation runs")
	fmt.Println("  fully offline and never uses the current time; all timing comes from input.")
	fmt.Println()
	fmt.Println("  standard input is line-delimited JSON with two record types:")
	fmt.Println(`    {"type":"event","key":"sensor-a","time":1200,"value":5}`)
	fmt.Println(`    {"type":"watermark","time":2000}`)
	fmt.Println("  time is a non-negative signed 64-bit millisecond timestamp, value is a")
	fmt.Println("  signed 64-bit integer, and key is a non-empty string. Blank lines are")
	fmt.Println("  ignored, but error line numbers count every physical input line.")
	fmt.Println()
	fmt.Println("  windows start at time zero, have the fixed length given by --window-ms")
	fmt.Println("  and are left-closed, right-open: [start,end). Every valid event for a key")
	fmt.Println("  in a window contributes to its count and sum; windows with no events do")
	fmt.Println("  not produce output. Records may arrive out of order; window membership")
	fmt.Println("  uses event time only.")
	fmt.Println()
	fmt.Println("  a watermark declares that events with time below it are late. Before the")
	fmt.Println("  first watermark every valid event is accepted. After a watermark, every")
	fmt.Println("  window whose end is less than or equal to the watermark is closed once")
	fmt.Println("  and emitted as one JSON line with key,start,end,count,sum; the sum is a")
	fmt.Println("  signed 64-bit integer. Closures are ordered by end ascending and then by")
	fmt.Println("  key in UTF-8 byte order. Watermarks may jump or repeat, and an event at")
	fmt.Println("  exactly the current watermark is still valid; events below the current")
	fmt.Println("  watermark are skipped with a note on standard error.")
	fmt.Println()
	fmt.Println("  --partitions merges several independent input sources of the same stream.")
	fmt.Println("  <count> must be a positive signed 64-bit integer. When it is given, every")
	fmt.Println("  event and watermark record must additionally carry an integer partition in")
	fmt.Println("  [0,count):")
	fmt.Println(`    {"type":"event","key":"sensor-a","time":1200,"value":5,"partition":0}`)
	fmt.Println(`    {"type":"watermark","time":2000,"partition":0}`)
	fmt.Println(`    {"type":"idle","partition":0}`)
	fmt.Println("  each partition advances its own watermark independently. No effective")
	fmt.Println("  watermark exists until every partition has reported at least once;")
	fmt.Println("  afterwards the effective watermark is the minimum of the per-partition")
	fmt.Println("  values, and window closure and late-event checks use that minimum, so one")
	fmt.Println("  partition's larger watermark can never close a window or drop an event")
	fmt.Println("  ahead of the others. A partition watermark may repeat or jump forward but")
	fmt.Println("  never move backwards. Events from different partitions for the same key")
	fmt.Println("  and window merge into one count and sum; output records never include a")
	fmt.Println("  partition field. Without --partitions the command keeps the single")
	fmt.Println("  watermark behavior and ignores extra fields.")
	fmt.Println()
	fmt.Println("  an idle record declares a partition temporarily without data. It is")
	fmt.Println("  declared by the input only and is never inferred from the current time")
	fmt.Println("  or read gaps. From that record on the partition is excluded from the")
	fmt.Println("  effective watermark, so it no longer holds the others back; the idle")
	fmt.Println("  record itself may close windows. A non-idle partition that has not")
	fmt.Println("  reported yet still keeps the effective watermark unknown; when every")
	fmt.Println("  partition is idle the last produced effective watermark is retained")
	fmt.Println("  (and stays unknown if none was produced). Idleness is not an infinite")
	fmt.Println("  watermark and does not drop pending events. Repeating an idle record")
	fmt.Println("  for the same partition succeeds without extra output. A partition")
	fmt.Println("  resumes on its next watermark record: its time must be at least both")
	fmt.Println("  its own previous watermark and the current effective watermark. An")
	fmt.Println("  event arriving while the partition is idle is fatal; it neither")
	fmt.Println("  resumes the partition nor is silently treated as late. In single")
	fmt.Println("  watermark mode an idle record is an unknown record type.")
	fmt.Println()
	fmt.Println("  at end of input the watermark is not advanced and still-open windows are")
	fmt.Println("  not emitted. Malformed JSON, missing or mistyped fields, out-of-range")
	fmt.Println("  integers, unknown record types, missing or out-of-range partitions,")
	fmt.Println("  partition watermark regression, an event for an idle partition, a resume")
	fmt.Println("  watermark below the partition's previous or the effective watermark, and")
	fmt.Println("  window-end or cumulative-sum overflow print the line number and reason")
	fmt.Println("  to standard error and exit non-zero; earlier output is retained.")
	fmt.Println("  Standard output contains window results only.")
}

func runAggregate(args []string) int {
	fs := flag.NewFlagSet("aggregate", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "usage: edgefleet aggregate --window-ms <milliseconds> [--partitions <count>]")
		fmt.Fprintln(fs.Output(), `run "edgefleet help" for the full record and window closure rules`)
	}
	windowMillis := fs.Int64("window-ms", 0, "fixed window length in milliseconds (required, positive signed 64-bit integer)")
	partitions := fs.Int64("partitions", 0, "number of input partitions (optional positive signed 64-bit integer); when set, every record must carry an integer partition in [0,count)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() != 0 {
		fmt.Fprintf(os.Stderr, "aggregate accepts no positional arguments, got %v\n", fs.Args())
		fs.Usage()
		return 2
	}
	windowSpecified := false
	partitionsSpecified := false
	fs.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "window-ms":
			windowSpecified = true
		case "partitions":
			partitionsSpecified = true
		}
	})
	if !windowSpecified {
		fmt.Fprintln(os.Stderr, "aggregate requires --window-ms <milliseconds>")
		fs.Usage()
		return 2
	}
	if *windowMillis <= 0 {
		fmt.Fprintf(os.Stderr, "aggregate --window-ms must be a positive signed 64-bit integer, got %d\n", *windowMillis)
		return 2
	}
	if partitionsSpecified && *partitions <= 0 {
		fmt.Fprintf(os.Stderr, "aggregate --partitions must be a positive signed 64-bit integer, got %d\n", *partitions)
		return 2
	}
	var err error
	if partitionsSpecified {
		err = edgefleet.RunAggregatePartitioned(os.Stdin, *windowMillis, *partitions, os.Stdout, os.Stderr)
	} else {
		err = edgefleet.RunAggregate(os.Stdin, *windowMillis, os.Stdout, os.Stderr)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "aggregate:", err)
		return 1
	}
	return 0
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
