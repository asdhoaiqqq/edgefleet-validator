// Command edgefleet is the 验证者与边缘节点机群管理平台 entry point.
package main

import (
	"errors"
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
	fmt.Println("usage: edgefleet [demo|version|aggregate --window-ms <milliseconds> [--slide-ms <milliseconds>] [--partitions <count>] [--max-open-windows <count>]|help]")
	fmt.Println()
	fmt.Println("commands:")
	fmt.Println("  demo       run a built-in fleet health demonstration")
	fmt.Println("  version    print the edgefleet version")
	fmt.Println("  aggregate  aggregate JSON events from standard input into fixed or sliding windows")
	fmt.Println("  help       show this help")
	fmt.Println()
	fmt.Println("aggregate:")
	fmt.Println("  edgefleet aggregate --window-ms <milliseconds> [--slide-ms <milliseconds>] [--partitions <count>] [--max-open-windows <count>]")
	fmt.Println()
	fmt.Println("  --window-ms is a required positive signed 64-bit integer. Aggregation runs")
	fmt.Println("  fully offline and never uses the current time; all timing comes from input.")
	fmt.Println()
	fmt.Println("  standard input is line-delimited JSON with two record types:")
	fmt.Println(`    {"type":"event","key":"sensor-a","time":1200,"value":5}`)
	fmt.Println(`    {"type":"watermark","time":2000}`)
	fmt.Println("  time is a non-negative signed 64-bit millisecond timestamp, value is a")
	fmt.Println("  signed 64-bit integer, and key is a non-empty string that an event")
	fmt.Println("  names at most once at its top level; a repeated key is a fatal record")
	fmt.Println("  error (judged by the decoded field name, equal values included). Blank")
	fmt.Println("  lines are ignored, but error line numbers count every physical input")
	fmt.Println("  line.")
	fmt.Println()
	fmt.Println("  windows start at time zero, have the fixed length given by --window-ms")
	fmt.Println("  and are left-closed, right-open: [start,end). Every valid event for a key")
	fmt.Println("  in a window contributes to its count and sum; windows with no events do")
	fmt.Println("  not produce output. Records may arrive out of order; window membership")
	fmt.Println("  uses event time only.")
	fmt.Println()
	fmt.Println("  --slide-ms makes the windows overlap: it gives the positive signed")
	fmt.Println("  64-bit integer distance between consecutive window starts (0, one")
	fmt.Println("  interval, two intervals, ...), must not exceed --window-ms and does not")
	fmt.Println("  have to divide it. Each valid event counts once in every window that")
	fmt.Println("  contains its event time, adding its value in full to each of them; for")
	fmt.Println("  example length 1000 with interval 600 produces [0,1000), [600,1600),")
	fmt.Println("  [1200,2200), ... and an event at time 1000 is outside the first. When")
	fmt.Println("  --slide-ms is omitted or equals --window-ms the behavior is identical")
	fmt.Println("  to fixed windows.")
	fmt.Println()
	fmt.Println("  --max-open-windows optionally caps how much aggregate state is kept")
	fmt.Println("  while the watermark stalls and the keys keep changing. <count> must be")
	fmt.Println("  a positive signed 64-bit integer; omitting the flag keeps the")
	fmt.Println("  unlimited behavior. One slot is held per distinct")
	fmt.Println("  pair of a decoded event key and one window interval [start,end) that")
	fmt.Println("  has not closed yet: slots are not counted per event record and not per")
	fmt.Println("  partition, so further events for a key and window that already exist")
	fmt.Println("  only grow that window's count and sum and take no new slot even when")
	fmt.Println("  the cap is full, and contributions to the same key and window from")
	fmt.Println("  different partitions merge into the one slot. With sliding windows a")
	fmt.Println("  single event can enter several overlapping windows: each (key,")
	fmt.Println("  window) pair it opens for the first time takes one slot, while pairs")
	fmt.Println("  that already exist take none. When a legal, non-late event's new")
	fmt.Println("  windows land exactly on the cap it is accepted; if they would exceed")
	fmt.Println("  it, the whole physical input line fails with exit code 1, the line")
	fmt.Println("  number, the event key, the slots currently open, the new slots the")
	fmt.Println("  event needs and the configured cap; the event enters none of its")
	fmt.Println("  windows, no later record is read, and results already written are")
	fmt.Println("  retained. Slots are released only when a window closes under the")
	fmt.Println("  watermark rules and its result line has been written in full, so a")
	fmt.Println("  later event can reuse them; declaring a partition idle frees nothing")
	fmt.Println("  by itself and only frees slots when it advances the effective")
	fmt.Println("  watermark enough to close windows. Late events take no slot and are")
	fmt.Println("  still skipped as late, never reported as the cap being exceeded.")
	fmt.Println()
	fmt.Println("  a watermark declares that events with time below it are late. Before the")
	fmt.Println("  first watermark every valid event is accepted. After a watermark, every")
	fmt.Println("  window whose end is less than or equal to the watermark is closed once")
	fmt.Println("  and emitted as one JSON line with key,start,end,count,sum; the sum is a")
	fmt.Println("  signed 64-bit integer. Closures are ordered by end ascending and then by")
	fmt.Println("  key in UTF-8 byte order. Watermarks may jump or repeat, and an event at")
	fmt.Println("  exactly the current watermark is still valid; events below the current")
	fmt.Println("  watermark are skipped with a note on standard error, even when they")
	fmt.Println("  would still fall inside an unclosed overlapping window. Still-open")
	fmt.Println("  windows are never emitted at end of input.")
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
	fmt.Println("  partition field. Sliding windows combine with partitions: the same key")
	fmt.Println("  from different partitions merges within each overlapping window, and the")
	fmt.Println("  effective watermark alone drives late-event checks and closure. Without")
	fmt.Println("  --partitions the command keeps the single watermark behavior and ignores")
	fmt.Println("  extra fields.")
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
	fmt.Println("  invalid command parameters -- a missing --window-ms, or a")
	fmt.Println("  non-positive --window-ms, --slide-ms or --max-open-windows, or a")
	fmt.Println("  --slide-ms larger than --window-ms, or a non-positive --partitions,")
	fmt.Println("  or a --max-open-windows that is not an integer or is outside the")
	fmt.Println("  signed 64-bit range -- print the reason to standard error and exit")
	fmt.Println("  with code 2 before any input is read. Malformed")
	fmt.Println("  JSON, missing or mistyped fields, out-of-range integers, unknown")
	fmt.Println("  record types, missing or out-of-range partitions, partition watermark")
	fmt.Println("  regression, an event for an idle partition, a resume watermark below")
	fmt.Println("  the partition's previous or the effective watermark, an event key")
	fmt.Println("  whose character encoding or Unicode escape is damaged (invalid UTF-8")
	fmt.Println("  bytes, or an unpaired or mispaired surrogate), an event record that")
	fmt.Println("  names the top-level key field more than once (the field is judged by")
	fmt.Println("  its decoded name, so an equivalent escape spelling still repeats it,")
	fmt.Println("  and equal values still count; this is rejected before the event's")
	fmt.Println("  fields, partition state and lateness), window-end,")
	fmt.Println("  event-count or cumulative-sum overflow, and a legal event that would")
	fmt.Println("  open more still-open key/window pairs than --max-open-windows allows")
	fmt.Println("  (the error names the line, the key, the current open count, the new")
	fmt.Println("  windows needed and the configured cap, and the whole event enters no")
	fmt.Println("  window) print the input line number and")
	fmt.Println("  reason to standard error and stop processing with exit code 1; no")
	fmt.Println("  further records are read and earlier output is retained. A window")
	fmt.Println("  result or late-event notice that cannot be written in full (the")
	fmt.Println("  output writer errors, even after accepting a prefix, or accepts too")
	fmt.Println("  few bytes without an error) is likewise fatal with the triggering")
	fmt.Println("  input line number, the affected key and window or the skipped event")
	fmt.Println("  time and watermark, and the writer's error; the failed content is not")
	fmt.Println("  resent, no further windows are emitted and content already written is")
	fmt.Println("  retained. Standard output contains window results only. A failure while")
	fmt.Println("  reading standard input is likewise fatal: records whose newline was")
	fmt.Println("  received, including those carried by the failing read, are processed in")
	fmt.Println("  order first, but the unterminated trailing bytes never become a record,")
	fmt.Println("  and the reader's error is reported here with a non-zero exit; clean end")
	fmt.Println("  of input is not a read failure, so a final record without a trailing")
	fmt.Println("  newline is still processed.")
}

func runAggregate(args []string) int {
	fs := flag.NewFlagSet("aggregate", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "usage: edgefleet aggregate --window-ms <milliseconds> [--slide-ms <milliseconds>] [--partitions <count>] [--max-open-windows <count>]")
		fmt.Fprintln(fs.Output(), `run "edgefleet help" for the full record and window closure rules`)
	}
	windowMillis := fs.Int64("window-ms", 0, "window length in milliseconds (required, positive signed 64-bit integer)")
	slideMillis := fs.Int64("slide-ms", 0, "distance between consecutive window starts in milliseconds (optional positive signed 64-bit integer, no greater than --window-ms); defaults to the window length for non-overlapping fixed windows")
	partitions := fs.Int64("partitions", 0, "number of input partitions (optional positive signed 64-bit integer); when set, every record must carry an integer partition in [0,count)")
	maxOpenWindows := fs.Int64("max-open-windows", 0, "maximum number of still-open key/window pairs kept at once (optional positive signed 64-bit integer); omitting the flag leaves the number unlimited")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() != 0 {
		fmt.Fprintf(os.Stderr, "aggregate accepts no positional arguments, got %v\n", fs.Args())
		fs.Usage()
		return 2
	}
	windowSpecified := false
	slideSpecified := false
	partitionsSpecified := false
	maxOpenWindowsSpecified := false
	fs.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "window-ms":
			windowSpecified = true
		case "slide-ms":
			slideSpecified = true
		case "partitions":
			partitionsSpecified = true
		case "max-open-windows":
			maxOpenWindowsSpecified = true
		}
	})
	if !windowSpecified {
		fmt.Fprintln(os.Stderr, "aggregate requires --window-ms <milliseconds>")
		fs.Usage()
		return 2
	}
	// An omitted --slide-ms defaults to the window length (fixed windows).
	// This is an omission policy specific to the command: an explicitly given
	// zero is left untouched, so it is still rejected below rather than being
	// silently treated as the default.
	if !slideSpecified {
		*slideMillis = *windowMillis
	}
	// The numeric limits are shared with the library entry points and live in
	// edgefleet.ValidateAggregateParams; only the flag-named wording and exit
	// code are command-specific. The shared report order (window, slide,
	// slide-vs-window, partitions) is the order this command already used.
	if err := edgefleet.ValidateAggregateParams(*windowMillis, *slideMillis, *partitions); err != nil {
		var paramErr *edgefleet.AggregateParamError
		if errors.As(err, &paramErr) {
			fmt.Fprint(os.Stderr, aggregateParamCLIError(paramErr))
		} else {
			fmt.Fprintln(os.Stderr, "aggregate:", err)
		}
		return 2
	}
	// The command rejects an explicitly supplied non-positive --partitions;
	// the library accepts zero as its legacy single-watermark mode selector.
	// The shared validator already rejected negative counts, so here this only
	// catches an explicit zero, kept as the command's own policy.
	if partitionsSpecified && *partitions <= 0 {
		fmt.Fprintf(os.Stderr, "aggregate --partitions must be a positive signed 64-bit integer, got %d\n", *partitions)
		return 2
	}
	// Omitted --max-open-windows keeps the unlimited behavior (the runner
	// reads zero as "no cap"). The command rejects an explicitly supplied
	// non-positive value; non-integer and out-of-range spellings never reach
	// here, the flag parser rejects them with exit 2 while parsing.
	if maxOpenWindowsSpecified && *maxOpenWindows <= 0 {
		fmt.Fprintf(os.Stderr, "aggregate --max-open-windows must be a positive signed 64-bit integer, got %d\n", *maxOpenWindows)
		return 2
	}
	var err error
	// A zero cap means unlimited, so the capped entry points reproduce the
	// uncapped ones exactly when the flag is omitted.
	if partitionsSpecified {
		err = edgefleet.RunAggregatePartitionedSlidingWithMaxOpenWindows(os.Stdin, *windowMillis, *slideMillis, *partitions, *maxOpenWindows, os.Stdout, os.Stderr)
	} else {
		err = edgefleet.RunAggregateSlidingWithMaxOpenWindows(os.Stdin, *windowMillis, *slideMillis, *maxOpenWindows, os.Stdout, os.Stderr)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "aggregate:", err)
		return 1
	}
	return 0
}

// aggregateParamCLIError renders a shared *edgefleet.AggregateParamError in
// the command's existing flag-named wording. The limit and its checking order
// come from edgefleet.ValidateAggregateParams; only this text and the exit
// code are command-specific.
func aggregateParamCLIError(e *edgefleet.AggregateParamError) string {
	switch e.Kind {
	case edgefleet.AggregateParamTooLarge:
		return fmt.Sprintf("aggregate --slide-ms %d must not exceed --window-ms %d\n", e.Value, e.Limit)
	case edgefleet.AggregateParamNotPositive:
		switch e.Field {
		case edgefleet.AggregateParamWindow:
			return fmt.Sprintf("aggregate --window-ms must be a positive signed 64-bit integer, got %d\n", e.Value)
		case edgefleet.AggregateParamSlide:
			return fmt.Sprintf("aggregate --slide-ms must be a positive signed 64-bit integer, got %d\n", e.Value)
		case edgefleet.AggregateParamPartitions:
			return fmt.Sprintf("aggregate --partitions must be a positive signed 64-bit integer, got %d\n", e.Value)
		}
	}
	return fmt.Sprintf("aggregate: invalid %s parameter %d\n", e.Field, e.Value)
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
