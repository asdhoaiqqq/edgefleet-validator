// Command edgefleet is the 验证者与边缘节点机群管理平台 entry point.
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"time"

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
	case "heartbeat":
		runHeartbeat(os.Args[2:])
	case "help", "-h", "--help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", command)
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Println("usage: edgefleet [demo|version|heartbeat|help]")
	fmt.Println()
	fmt.Println("commands:")
	fmt.Println("  demo       run the built-in evaluation demo")
	fmt.Println("  version    print version")
	fmt.Println("  heartbeat  receive, store and query real heartbeats")
	fmt.Println("  help       show this help")
	fmt.Println()
	fmt.Println("run 'edgefleet heartbeat --help' for heartbeat input format and storage location")
}

// defaultDataDir resolves the data directory: --data-dir flag, then the
// EDGEFLEET_DATA_DIR environment variable, then ~/.edgefleet.
func defaultDataDir() string {
	if d := os.Getenv("EDGEFLEET_DATA_DIR"); d != "" {
		return d
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ".edgefleet"
	}
	return home + "/.edgefleet"
}

// die prints an error to stderr and exits with status 1.
func die(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "error: "+format+"\n", args...)
	os.Exit(1)
}

// parseTime parses an RFC3339 time flag, defaulting to now when empty.
func parseTimeFlag(value, flagName string) time.Time {
	if value == "" {
		return time.Now()
	}
	t, err := time.Parse(time.RFC3339, value)
	if err != nil {
		die("invalid %s %q: must be RFC3339 with timezone (e.g. 2026-10-01T12:00:00+08:00)", flagName, value)
	}
	return t
}

func runHeartbeat(args []string) {
	if len(args) == 0 {
		heartbeatUsage()
		return
	}
	switch args[0] {
	case "submit":
		cmdSubmit(args[1:])
	case "health":
		cmdHealth(args[1:])
	case "history":
		cmdHistory(args[1:])
	case "help", "-h", "--help":
		heartbeatUsage()
	default:
		fmt.Fprintf(os.Stderr, "unknown heartbeat command %q\n", args[0])
		heartbeatUsage()
		os.Exit(2)
	}
}

func heartbeatUsage() {
	fmt.Println("usage: edgefleet heartbeat <submit|health|history> [flags]")
	fmt.Println()
	fmt.Println("commands:")
	fmt.Println("  submit   receive heartbeats from stdin as a JSON array")
	fmt.Println("  health   query node health from its latest telemetry")
	fmt.Println("  history  list all heartbeats for a node, ascending by seq")
	fmt.Println()
	fmt.Println("input format (submit):")
	fmt.Println("  A JSON array of heartbeat objects; every field is required.")
	fmt.Println("    node          string   node id (non-empty)")
	fmt.Println("    seq           integer  sequence number (> 0)")
	fmt.Println("    collected_at  string   RFC3339 with timezone, e.g. 2026-10-01T12:00:00+08:00")
	fmt.Println("                           must not be later than the receive time")
	fmt.Println("    version       string   version (non-empty)")
	fmt.Println("    height        integer  block height (>= 0)")
	fmt.Println("    missed        integer  cumulative missed duties (>= 0)")
	fmt.Println()
	fmt.Println("  example:")
	fmt.Println(`    [{"node":"val-eu-1","seq":7,"collected_at":"2026-10-01T11:59:00+08:00",`)
	fmt.Println(`      "version":"1.26.0","height":12345,"missed":0}]`)
	fmt.Println()
	fmt.Println("flags:")
	fmt.Println("  submit   --data-dir DIR        data directory (default: $EDGEFLEET_DATA_DIR or ~/.edgefleet)")
	fmt.Println("           --receive-time TIME   receive time, RFC3339 (default: now)")
	fmt.Println("  health   --node ID             node id (required)")
	fmt.Println("           --expected-version V  expected version (required)")
	fmt.Println("           --tolerated-misses N  tolerated missed duties, >= 0 (required)")
	fmt.Println("           --missed-since-seq S  seq of a saved heartbeat used as the")
	fmt.Println("                                missed-duty baseline (optional, > 0)")
	fmt.Println("           --at TIME             query time, RFC3339 (default: now)")
	fmt.Println("           --data-dir DIR        data directory")
	fmt.Println()
	fmt.Println("  Without --missed-since-seq the missed-duty alarm uses the cumulative")
	fmt.Println("  missed count of the latest heartbeat (missed in the output). With it,")
	fmt.Println("  the alarm counts only missed duties newly recorded between the baseline")
	fmt.Println("  heartbeat seq S and the latest one; a count strictly greater than")
	fmt.Println("  --tolerated-misses alarms, equality does not. The output then shows the")
	fmt.Println("  baseline seq, the baseline cumulative count and the new count. If the")
	fmt.Println("  cumulative counter decreased between the baseline and the latest record")
	fmt.Println("  (e.g. after a node reset), the new count is shown as 无法判断 and a")
	fmt.Println("  累计漏签数回退 finding is reported; the baseline must name an existing")
	fmt.Println("  saved seq, otherwise the command fails.")
	fmt.Println()
	fmt.Println("  history  --node ID             node id (required)")
	fmt.Println("           --data-dir DIR        data directory")
	fmt.Println()
	fmt.Println("data storage:")
	fmt.Println("  <data-dir>/nodes/<hex-node-id>.json, one file per node.")
	fmt.Println("  Writes are atomic and the directory is locked for concurrent access.")
	fmt.Println("  Corrupt data is refused, never silently cleared.")
}

func newFlagSet(name string) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.Usage = func() { heartbeatUsage() }
	return fs
}

func cmdSubmit(args []string) {
	fs := newFlagSet("heartbeat submit")
	dataDir := fs.String("data-dir", defaultDataDir(), "data directory")
	receiveStr := fs.String("receive-time", "", "receive time (RFC3339); default: now")
	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			os.Exit(0)
		}
		os.Exit(2)
	}
	if fs.NArg() != 0 {
		die("submit takes no positional arguments; heartbeats are read from stdin")
	}

	receiveTime := parseTimeFlag(*receiveStr, "--receive-time")

	data, err := io.ReadAll(os.Stdin)
	if err != nil {
		die("cannot read stdin: %v", err)
	}
	records, err := edgefleet.ParseHeartbeats(data, receiveTime)
	if err != nil {
		die("invalid input: %v", err)
	}

	store, err := edgefleet.OpenStore(*dataDir)
	if err != nil {
		die("%v", err)
	}
	newCount, dupCount, err := store.Submit(records, receiveTime)
	if err != nil {
		die("%v", err)
	}
	fmt.Printf("submitted: new=%d duplicate=%d\n", newCount, dupCount)
}

func cmdHealth(args []string) {
	fs := newFlagSet("heartbeat health")
	dataDir := fs.String("data-dir", defaultDataDir(), "data directory")
	node := fs.String("node", "", "node id (required)")
	atStr := fs.String("at", "", "query time (RFC3339); default: now")
	expectedVersion := fs.String("expected-version", "", "expected version (required)")
	toleratedMisses := fs.Int("tolerated-misses", -1, "tolerated missed duties, >= 0 (required)")
	missedSinceSeq := fs.Int64("missed-since-seq", 0, "seq of a saved heartbeat to use as the missed-duty baseline; must be > 0 and exist for the node")
	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			os.Exit(0)
		}
		os.Exit(2)
	}
	if *node == "" {
		die("--node is required")
	}
	if *expectedVersion == "" {
		die("--expected-version is required")
	}
	if *toleratedMisses < 0 {
		die("--tolerated-misses is required and must be >= 0")
	}
	if isFlagPassed(fs, "missed-since-seq") && *missedSinceSeq <= 0 {
		die("--missed-since-seq must be a positive integer")
	}

	at := parseTimeFlag(*atStr, "--at")

	store, err := edgefleet.OpenStore(*dataDir)
	if err != nil {
		die("%v", err)
	}

	var result edgefleet.HealthResult
	if isFlagPassed(fs, "missed-since-seq") {
		result, err = store.HealthSince(*node, at, *expectedVersion, *toleratedMisses, *missedSinceSeq)
	} else {
		result, err = store.Health(*node, at, *expectedVersion, *toleratedMisses)
	}
	if err != nil {
		die("%v", err)
	}

	if result.Status == "notelemetry" {
		fmt.Printf("node=%s status=无遥测 findings=[无遥测]\n", result.NodeID)
		return
	}
	// RFC3339Nano keeps the existing whole-second display (e.g.
	// 2026-10-01T11:59:00Z) but preserves sub-second fractions when the
	// heartbeat reported them; dropping them would hide the precision the
	// online/offline boundary is judged at (60s + 1ns).
	fmt.Printf("node=%s status=%s seq=%d collected_at=%s version=%s height=%d missed=%d findings=%v\n",
		result.NodeID, result.Status, result.Seq, result.CollectedAt.Format(time.RFC3339Nano),
		result.Version, result.Height, result.Missed, result.Findings)
	if isFlagPassed(fs, "missed-since-seq") {
		// missed above stays cumulative; this line states the alarm basis.
		if result.NewMissedKnown {
			fmt.Printf("baseline_seq=%d baseline_missed=%d new_missed=%d tolerated_misses=%d\n",
				result.BaselineSeq, result.BaselineMissed, result.NewMissed, *toleratedMisses)
		} else {
			fmt.Printf("baseline_seq=%d baseline_missed=%d new_missed=无法判断 tolerated_misses=%d\n",
				result.BaselineSeq, result.BaselineMissed, *toleratedMisses)
		}
	}
}

// isFlagPassed reports whether the named flag was explicitly set on the
// command line, so an optional flag with a zero default can be distinguished
// from its absence.
func isFlagPassed(fs *flag.FlagSet, name string) bool {
	passed := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == name {
			passed = true
		}
	})
	return passed
}

func cmdHistory(args []string) {
	fs := newFlagSet("heartbeat history")
	dataDir := fs.String("data-dir", defaultDataDir(), "data directory")
	node := fs.String("node", "", "node id (required)")
	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			os.Exit(0)
		}
		os.Exit(2)
	}
	if *node == "" {
		die("--node is required")
	}

	store, err := edgefleet.OpenStore(*dataDir)
	if err != nil {
		die("%v", err)
	}
	records, err := store.History(*node)
	if err != nil {
		die("%v", err)
	}
	if len(records) == 0 {
		fmt.Printf("node=%s no heartbeats\n", *node)
		return
	}
	for _, r := range records {
		fmt.Printf("node=%s seq=%d collected_at=%s version=%s height=%d missed=%d\n",
			r.NodeID, r.Seq, r.CollectedAt.Format(time.RFC3339Nano), r.Version, r.Height, r.Missed)
	}
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
