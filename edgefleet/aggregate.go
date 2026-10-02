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
func RunAggregate(r io.Reader, windowMillis int64, out io.Writer, lateLog io.Writer) error {
	if windowMillis <= 0 {
		return fmt.Errorf("window length must be a positive signed 64-bit integer, got %d", windowMillis)
	}

	windows := make(map[windowID]*windowState)
	var watermark *int64

	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024*1024)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := strings.TrimSuffix(scanner.Text(), "\r")
		if strings.TrimSpace(line) == "" {
			continue
		}
		if err := processAggregateLine(line, lineNo, windowMillis, windows, &watermark, out, lateLog); err != nil {
			return err
		}
	}
	return scanner.Err()
}

func processAggregateLine(line string, lineNo int, windowMillis int64, windows map[windowID]*windowState, watermark **int64, out io.Writer, lateLog io.Writer) error {
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
		return processEvent(obj, lineNo, windowMillis, windows, *watermark, lateLog)
	case "watermark":
		return processWatermark(obj, lineNo, windows, watermark, out)
	default:
		return &InputError{Line: lineNo, Reason: fmt.Sprintf("unknown record type %q", recordType)}
	}
}

func processEvent(obj map[string]json.RawMessage, lineNo int, windowMillis int64, windows map[windowID]*windowState, watermark *int64, lateLog io.Writer) error {
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

	if watermark != nil && eventTime < *watermark {
		fmt.Fprintf(lateLog, "line %d: late event time=%d below current watermark %d, skipped\n", lineNo, eventTime, *watermark)
		return nil
	}

	start := eventTime - eventTime%windowMillis
	if start > math.MaxInt64-windowMillis {
		return &InputError{Line: lineNo, Reason: fmt.Sprintf("window end overflow for key %q window starting at %d with length %d", key, start, windowMillis)}
	}
	end := start + windowMillis

	id := windowID{start: start, key: key}
	st := windows[id]
	if st == nil {
		st = &windowState{end: end}
		windows[id] = st
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

func processWatermark(obj map[string]json.RawMessage, lineNo int, windows map[windowID]*windowState, watermark **int64, out io.Writer) error {
	next, err := requiredNonNegInt64(obj, "time", lineNo)
	if err != nil {
		return err
	}
	if *watermark != nil && next < **watermark {
		return &InputError{Line: lineNo, Reason: fmt.Sprintf("watermark moved backwards from %d to %d", **watermark, next)}
	}
	*watermark = &next

	type pending struct {
		id windowID
		st *windowState
	}
	var ready []pending
	for id, st := range windows {
		if st.end <= next {
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
		delete(windows, p.id)
	}
	return nil
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
