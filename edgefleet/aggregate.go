package edgefleet

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
)

// AggregateResult is one closed window output line.
type AggregateResult struct {
	Key   string `json:"key"`
	Start int64  `json:"start"`
	End   int64  `json:"end"`
	Count int64  `json:"count"`
	Sum   int64  `json:"sum"`
}

// InputError reports a fatal problem with one physical input line.
type InputError struct {
	Line   int
	Reason string
}

func (e *InputError) Error() string {
	return fmt.Sprintf("line %d: %s", e.Line, e.Reason)
}

type windowID struct {
	start int64 // inclusive window start in milliseconds since time zero
	key   string
}

type windowState struct {
	end   int64 // exclusive window end in milliseconds
	count int64
	sum   int64
}

// RunAggregate reads line-delimited JSON records from r and writes closed
// window results, one JSON object per line, to out. windowMillis must be a
// positive signed 64-bit integer. Windows are fixed-length, start at time
// zero and use a left-closed right-open interval. Closure is driven solely by
// input watermarks: a window whose end is less than or equal to the current
// watermark is emitted once, ordered by end ascending and then by key in
// UTF-8 byte order. Late events (event time below the current watermark) are
// reported on lateLog with their physical line number and skipped. Fatal
// record problems return *InputError; results already written to out stay
// written, and open windows are not flushed at end of input.
//
// A single input watermark drives closure, and unrecognised record fields
// are ignored.
func RunAggregate(r io.Reader, windowMillis int64, out io.Writer, lateLog io.Writer) error {
	return runAggregateStream(r, windowMillis, 0, out, lateLog)
}

// RunAggregatePartitions behaves like RunAggregate but merges records coming
// from multiple independent input partitions. partitions must be a positive
// signed 64-bit integer and every event and watermark record must carry an
// integer "partition" field in [0,partitions).
//
// Each partition advances its own watermark independently. No effective
// watermark exists until every partition has reported at least one watermark;
// until then no window closes and no event is considered late. Afterwards the
// effective watermark is the minimum of the per-partition values, and both
// window closure and late-event detection use that minimum. A per-partition
// watermark may repeat or jump forward but may never move backwards. Output
// records carry no partition field: events with the same key and window are
// merged regardless of source partition.
func RunAggregatePartitions(r io.Reader, windowMillis int64, partitions int64, out io.Writer, lateLog io.Writer) error {
	if partitions <= 0 {
		return fmt.Errorf("partition count must be a positive signed 64-bit integer, got %d", partitions)
	}
	return runAggregateStream(r, windowMillis, partitions, out, lateLog)
}

// aggregator holds the processing state. partitions == 0 selects the legacy
// single-watermark behaviour; partitions >= 1 selects partition mode.
type aggregator struct {
	windowMillis int64
	partitions   int64
	windows      map[windowID]*windowState

	// Legacy mode only: the single input watermark.
	watermark *int64

	// Partition mode only: latest watermark seen per partition. The effective
	// watermark becomes defined once the map holds every partition.
	partitionWatermarks map[int64]int64
}

func runAggregateStream(r io.Reader, windowMillis int64, partitions int64, out io.Writer, lateLog io.Writer) error {
	if windowMillis <= 0 {
		return fmt.Errorf("window length must be a positive signed 64-bit integer, got %d", windowMillis)
	}

	a := &aggregator{
		windowMillis: windowMillis,
		partitions:   partitions,
		windows:      make(map[windowID]*windowState),
	}
	if partitions > 0 {
		a.partitionWatermarks = make(map[int64]int64)
	}

	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024*1024)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := strings.TrimSuffix(scanner.Text(), "\r")
		if strings.TrimSpace(line) == "" {
			continue
		}
		if err := a.processLine(line, lineNo, out, lateLog); err != nil {
			return err
		}
	}
	return scanner.Err()
}

// effectiveWatermark returns the current watermark or nil when no watermark
// is in effect yet (nothing seen, or not every partition has reported one).
func (a *aggregator) effectiveWatermark() *int64 {
	if a.partitions == 0 {
		return a.watermark
	}
	if int64(len(a.partitionWatermarks)) < a.partitions {
		return nil
	}
	min := int64(math.MaxInt64)
	for _, wm := range a.partitionWatermarks {
		if wm < min {
			min = wm
		}
	}
	return &min
}

func (a *aggregator) processLine(line string, lineNo int, out io.Writer, lateLog io.Writer) error {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal([]byte(line), &obj); err != nil {
		return &InputError{Line: lineNo, Reason: "invalid JSON record: " + err.Error()}
	}

	typeRaw, ok := obj["type"]
	if !ok {
		return &InputError{Line: lineNo, Reason: `missing required string field "type"`}
	}
	recordType, err := decodeString(typeRaw, "type")
	if err != nil {
		return &InputError{Line: lineNo, Reason: err.Error()}
	}

	switch recordType {
	case "event":
		return a.processEvent(obj, lineNo, lateLog)
	case "watermark":
		return a.processWatermark(obj, lineNo, out)
	default:
		return &InputError{Line: lineNo, Reason: fmt.Sprintf("unknown record type %q", recordType)}
	}
}

func (a *aggregator) processEvent(obj map[string]json.RawMessage, lineNo int, lateLog io.Writer) error {
	if a.partitions > 0 {
		if _, err := a.requiredPartition(obj, lineNo); err != nil {
			return err
		}
	}
	key, err := requiredString(obj, "key", lineNo)
	if err != nil {
		return err
	}
	if key == "" {
		return &InputError{Line: lineNo, Reason: `field "key" must be a non-empty string`}
	}
	eventTime, err := requiredNonNegInt64(obj, "time", lineNo)
	if err != nil {
		return err
	}
	value, err := requiredInt64(obj, "value", lineNo)
	if err != nil {
		return err
	}

	if watermark := a.effectiveWatermark(); watermark != nil && eventTime < *watermark {
		fmt.Fprintf(lateLog, "line %d: late event time=%d below current watermark %d, skipped\n", lineNo, eventTime, *watermark)
		return nil
	}

	start := eventTime - eventTime%a.windowMillis
	if start > math.MaxInt64-a.windowMillis {
		return &InputError{Line: lineNo, Reason: fmt.Sprintf("window end overflow for key %q window starting at %d with length %d", key, start, a.windowMillis)}
	}
	end := start + a.windowMillis

	id := windowID{start: start, key: key}
	st := a.windows[id]
	if st == nil {
		st = &windowState{end: end}
		a.windows[id] = st
	}
	if st.count == math.MaxInt64 {
		return &InputError{Line: lineNo, Reason: fmt.Sprintf("event count overflow for key %q window [%d,%d)", key, start, end)}
	}
	if value > 0 && st.sum > math.MaxInt64-value {
		return &InputError{Line: lineNo, Reason: fmt.Sprintf("cumulative sum overflow for key %q window [%d,%d): %d + %d", key, start, end, st.sum, value)}
	}
	if value < 0 && st.sum < math.MinInt64-value {
		return &InputError{Line: lineNo, Reason: fmt.Sprintf("cumulative sum overflow for key %q window [%d,%d): %d + %d", key, start, end, st.sum, value)}
	}
	st.count++
	st.sum += value
	return nil
}

func (a *aggregator) processWatermark(obj map[string]json.RawMessage, lineNo int, out io.Writer) error {
	var partition int64 = -1
	if a.partitions > 0 {
		p, err := a.requiredPartition(obj, lineNo)
		if err != nil {
			return err
		}
		partition = p
	}
	next, err := requiredNonNegInt64(obj, "time", lineNo)
	if err != nil {
		return err
	}

	if a.partitions == 0 {
		if a.watermark != nil && next < *a.watermark {
			return &InputError{Line: lineNo, Reason: fmt.Sprintf("watermark moved backwards from %d to %d", *a.watermark, next)}
		}
		a.watermark = &next
	} else {
		if prev, ok := a.partitionWatermarks[partition]; ok && next < prev {
			return &InputError{Line: lineNo, Reason: fmt.Sprintf("partition %d watermark moved backwards from %d to %d", partition, prev, next)}
		}
		a.partitionWatermarks[partition] = next
	}

	watermark := a.effectiveWatermark()
	if watermark == nil {
		return nil
	}
	return a.closeReady(*watermark, out)
}

// closeReady emits once, ordered by end ascending and then key in UTF-8 byte
// order, every window whose end is at or below watermark, and forgets it.
func (a *aggregator) closeReady(watermark int64, out io.Writer) error {
	type pending struct {
		id windowID
		st *windowState
	}
	var ready []pending
	for id, st := range a.windows {
		if st.end <= watermark {
			ready = append(ready, pending{id: id, st: st})
		}
	}
	sort.Slice(ready, func(i, j int) bool {
		if ready[i].st.end != ready[j].st.end {
			return ready[i].st.end < ready[j].st.end
		}
		return ready[i].id.key < ready[j].id.key
	})
	for _, p := range ready {
		encoded, err := json.Marshal(AggregateResult{
			Key:   p.id.key,
			Start: p.id.start,
			End:   p.st.end,
			Count: p.st.count,
			Sum:   p.st.sum,
		})
		if err != nil {
			return err
		}
		if _, err := out.Write(append(encoded, '\n')); err != nil {
			return err
		}
		delete(a.windows, p.id)
	}
	return nil
}

// requiredPartition reads the integer "partition" field and validates it
// against [0,partitions).
func (a *aggregator) requiredPartition(obj map[string]json.RawMessage, lineNo int) (int64, error) {
	partition, err := requiredInt64(obj, "partition", lineNo)
	if err != nil {
		return 0, err
	}
	if partition < 0 || partition >= a.partitions {
		return 0, &InputError{Line: lineNo, Reason: fmt.Sprintf("field %q must be an integer in [0,%d), got %d", "partition", a.partitions, partition)}
	}
	return partition, nil
}

func requiredString(obj map[string]json.RawMessage, field string, lineNo int) (string, error) {
	raw, ok := obj[field]
	if !ok {
		return "", &InputError{Line: lineNo, Reason: fmt.Sprintf("missing required string field %q", field)}
	}
	value, err := decodeString(raw, field)
	if err != nil {
		return "", &InputError{Line: lineNo, Reason: err.Error()}
	}
	return value, nil
}

func requiredInt64(obj map[string]json.RawMessage, field string, lineNo int) (int64, error) {
	raw, ok := obj[field]
	if !ok {
		return 0, &InputError{Line: lineNo, Reason: fmt.Sprintf("missing required integer field %q", field)}
	}
	value, err := decodeInt64(raw, field)
	if err != nil {
		return 0, &InputError{Line: lineNo, Reason: err.Error()}
	}
	return value, nil
}

func requiredNonNegInt64(obj map[string]json.RawMessage, field string, lineNo int) (int64, error) {
	value, err := requiredInt64(obj, field, lineNo)
	if err != nil {
		return 0, err
	}
	if value < 0 {
		return 0, &InputError{Line: lineNo, Reason: fmt.Sprintf("field %q must be a non-negative integer, got %d", field, value)}
	}
	return value, nil
}

func decodeString(raw json.RawMessage, field string) (string, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '"' {
		return "", fmt.Errorf("field %q must be a JSON string, got %s", field, string(trimmed))
	}
	var value string
	if err := json.Unmarshal(trimmed, &value); err != nil {
		return "", fmt.Errorf("field %q must be a JSON string: %v", field, err)
	}
	return value, nil
}

func decodeInt64(raw json.RawMessage, field string) (int64, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return 0, fmt.Errorf("field %q must be a signed 64-bit integer", field)
	}
	switch trimmed[0] {
	case '"', '{', '[', 't', 'f', 'n':
		return 0, fmt.Errorf("field %q must be a signed 64-bit integer, got %s", field, string(trimmed))
	}
	value, err := strconv.ParseInt(string(trimmed), 10, 64)
	if err != nil {
		if numErr, ok := err.(*strconv.NumError); ok && numErr.Err == strconv.ErrRange {
			return 0, fmt.Errorf("field %q integer out of signed 64-bit range: %s", field, string(trimmed))
		}
		return 0, fmt.Errorf("field %q must be a signed 64-bit integer, got %s", field, string(trimmed))
	}
	return value, nil
}
