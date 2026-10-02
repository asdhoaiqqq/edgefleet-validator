// Package edgefleet implements validator and edge node fleet management.
package edgefleet

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
)

// AggregateError reports an invalid input record at a given original line.
type AggregateError struct {
	Line int
	Msg  string
}

func (e *AggregateError) Error() string {
	return fmt.Sprintf("aggregate: line %d: %s", e.Line, e.Msg)
}

type windowAgg struct {
	key   string
	start int64
	end   int64
	count int64
	sum   int64
}

type windowResult struct {
	Key   string `json:"key"`
	Start int64  `json:"start"`
	End   int64  `json:"end"`
	Count int64  `json:"count"`
	Sum   int64  `json:"sum"`
}

type aggregator struct {
	windowMs  int64
	windows   map[int64]map[string]*windowAgg
	watermark int64
	hasWM     bool
	stdout    io.Writer
	stderr    io.Writer
}

// RunAggregate reads event and watermark records line by line from r,
// aggregates fixed-length windows starting at time zero, and emits each
// closed window to w. Closing is driven solely by the input watermark; the
// wall clock is never consulted.
//
// Late events (event time below the current watermark) are reported to ew
// and skipped without stopping processing. Any malformed or invalid record
// returns an error; the offending record produces no window result and the
// process exits non-zero, while results already emitted to w are preserved.
func RunAggregate(r io.Reader, w io.Writer, ew io.Writer, windowMs int64) error {
	a := &aggregator{
		windowMs: windowMs,
		windows:  make(map[int64]map[string]*windowAgg),
		stdout:   w,
		stderr:   ew,
	}
	br := bufio.NewReader(r)
	lineNo := 0
	for {
		line, err := br.ReadString('\n')
		if len(line) > 0 {
			lineNo++
			if perr := a.processLine(lineNo, line); perr != nil {
				return perr
			}
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func (a *aggregator) processLine(lineNo int, line string) error {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" {
		return nil
	}
	var fields map[string]interface{}
	dec := json.NewDecoder(strings.NewReader(trimmed))
	dec.UseNumber()
	if err := dec.Decode(&fields); err != nil {
		return &AggregateError{Line: lineNo, Msg: "malformed JSON: " + err.Error()}
	}
	var extra json.RawMessage
	if err := dec.Decode(&extra); err != io.EOF {
		return &AggregateError{Line: lineNo, Msg: "malformed JSON: trailing data"}
	}
	typ, ok := fields["type"].(string)
	if !ok {
		return &AggregateError{Line: lineNo, Msg: "missing or invalid \"type\" field"}
	}
	switch typ {
	case "event":
		return a.processEvent(lineNo, fields)
	case "watermark":
		return a.processWatermark(lineNo, fields)
	default:
		return &AggregateError{Line: lineNo, Msg: fmt.Sprintf("unknown record type %q", typ)}
	}
}

func (a *aggregator) processEvent(lineNo int, fields map[string]interface{}) error {
	key, ok := fields["key"].(string)
	if !ok {
		return &AggregateError{Line: lineNo, Msg: "missing or invalid \"key\" field"}
	}
	if key == "" {
		return &AggregateError{Line: lineNo, Msg: "key must be a non-empty string"}
	}
	t, err := intField(fields, "time")
	if err != nil {
		return &AggregateError{Line: lineNo, Msg: err.Error()}
	}
	if t < 0 {
		return &AggregateError{Line: lineNo, Msg: "time must be non-negative"}
	}
	v, err := intField(fields, "value")
	if err != nil {
		return &AggregateError{Line: lineNo, Msg: err.Error()}
	}
	if a.hasWM && t < a.watermark {
		fmt.Fprintf(a.stderr, "aggregate: line %d: event time %d is before current watermark %d, skipping\n", lineNo, t, a.watermark)
		return nil
	}
	start := t - t%a.windowMs
	if start > math.MaxInt64-a.windowMs {
		return &AggregateError{Line: lineNo, Msg: "window end overflow"}
	}
	end := start + a.windowMs
	keyMap, ok := a.windows[start]
	if !ok {
		keyMap = make(map[string]*windowAgg)
		a.windows[start] = keyMap
	}
	agg, ok := keyMap[key]
	if !ok {
		agg = &windowAgg{key: key, start: start, end: end}
		keyMap[key] = agg
	}
	if agg.count == math.MaxInt64 {
		return &AggregateError{Line: lineNo, Msg: "count overflow"}
	}
	if v > 0 && agg.sum > math.MaxInt64-v {
		return &AggregateError{Line: lineNo, Msg: "sum overflow"}
	}
	if v < 0 && agg.sum < math.MinInt64-v {
		return &AggregateError{Line: lineNo, Msg: "sum overflow"}
	}
	agg.count++
	agg.sum += v
	return nil
}

func (a *aggregator) processWatermark(lineNo int, fields map[string]interface{}) error {
	t, err := intField(fields, "time")
	if err != nil {
		return &AggregateError{Line: lineNo, Msg: err.Error()}
	}
	if t < 0 {
		return &AggregateError{Line: lineNo, Msg: "watermark time must be non-negative"}
	}
	if a.hasWM && t < a.watermark {
		return &AggregateError{Line: lineNo, Msg: fmt.Sprintf("watermark regression: %d < %d", t, a.watermark)}
	}
	if a.hasWM && t == a.watermark {
		// Repeated watermark: no regression, no new output.
		return nil
	}
	a.watermark = t
	a.hasWM = true
	a.closeWindows()
	return nil
}

// closeWindows emits every window whose end is at or below the current
// watermark, ordered by end ascending then by key in UTF-8 byte order.
// Closed windows are removed so each is emitted exactly once.
func (a *aggregator) closeWindows() {
	var starts []int64
	for start := range a.windows {
		if start+a.windowMs <= a.watermark {
			starts = append(starts, start)
		}
	}
	sort.Slice(starts, func(i, j int) bool { return starts[i] < starts[j] })
	for _, start := range starts {
		keyMap := a.windows[start]
		keys := make([]string, 0, len(keyMap))
		for k := range keyMap {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			agg := keyMap[k]
			b, err := json.Marshal(windowResult{
				Key:   k,
				Start: agg.start,
				End:   agg.end,
				Count: agg.count,
				Sum:   agg.sum,
			})
			if err != nil {
				panic(err) // key is a string, numbers are int64: marshal cannot fail
			}
			fmt.Fprintf(a.stdout, "%s\n", b)
		}
		delete(a.windows, start)
	}
}

// intField reads a json.Number field and parses it as a signed 64-bit integer.
func intField(fields map[string]interface{}, name string) (int64, error) {
	n, ok := fields[name].(json.Number)
	if !ok {
		return 0, fmt.Errorf("missing or invalid %q field", name)
	}
	v, err := strconv.ParseInt(string(n), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%q is not a signed 64-bit integer: %v", name, err)
	}
	return v, nil
}
