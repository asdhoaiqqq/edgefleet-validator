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
// This is the legacy single-watermark mode: records carry no partition field
// and any extra fields are ignored. Use RunAggregatePartitioned to merge
// several independent input sources of the same stream.
func RunAggregate(r io.Reader, windowMillis int64, out io.Writer, lateLog io.Writer) error {
	return RunAggregatePartitioned(r, windowMillis, 0, out, lateLog)
}

// RunAggregatePartitioned reads line-delimited JSON records from r and writes
// closed window results, one JSON object per line, to out. windowMillis must
// be a positive signed 64-bit integer. partitions selects the input mode:
//
//   - partitions == 0: legacy single-watermark mode. Records do not carry a
//     partition field; any extra fields are ignored.
//
//   - partitions > 0: partitioned mode, merging partitions independent input
//     sources of the same stream. Every event and watermark record must carry
//     an integer "partition" in [0,partitions). Each partition advances its
//     own watermark; no effective watermark exists until every partition has
//     reported at least once, afterwards the effective watermark is the
//     minimum of the per-partition values. Window closure and late-event
//     checks use this minimum, so one partition's larger watermark can never
//     close a window or drop an event ahead of the others. A partition
//     watermark may repeat or jump forward but never move backwards.
//
//     A partition may be declared idle with
//     {"type":"idle","partition":p}. An idle partition stops contributing to
//     the effective watermark: non-idle partitions that have not reported a
//     watermark yet still keep the effective watermark unknown, while a
//     partition that never reported may itself be declared idle. When every
//     remaining (non-idle) partition has reported, the effective watermark is
//     their minimum and the idle record itself can close windows. If all
//     partitions are idle, the last produced effective watermark is retained;
//     when none was ever produced it stays unknown. Idleness is declared by
//     the input only, never inferred from time; a repeated idle declaration
//     is a no-op. The next watermark record for that partition resumes it:
//     its time must be at least both the partition's previous watermark and
//     the current effective watermark. An event for an idle partition is
//     fatal; it never resumes implicitly.
//
// partitions < 0 is an error. Windows, ordering, late-event reporting,
// overflow checks and end-of-input behavior are otherwise identical to
// RunAggregate.
func RunAggregatePartitioned(r io.Reader, windowMillis, partitions int64, out io.Writer, lateLog io.Writer) error {
	if windowMillis <= 0 {
		return fmt.Errorf("window length must be a positive signed 64-bit integer, got %d", windowMillis)
	}
	if partitions < 0 {
		return fmt.Errorf("partition count must be a positive signed 64-bit integer, got %d", partitions)
	}
	s := &aggregateState{
		windowMillis:  windowMillis,
		windows:       make(map[windowID]*windowState),
		out:           out,
		lateLog:       lateLog,
		partitions:    partitions,
		partWatermark: make(map[int64]*int64),
		idle:          make(map[int64]bool),
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
		if err := s.processLine(line, lineNo); err != nil {
			return err
		}
	}
	return scanner.Err()
}

type aggregateState struct {
	windowMillis int64
	windows      map[windowID]*windowState
	out          io.Writer
	lateLog      io.Writer

	partitions    int64 // 0 = legacy single-watermark mode
	watermark     *int64
	partWatermark map[int64]*int64 // partition -> last watermark; partitioned mode only
	idle          map[int64]bool   // partitions declared idle in the input; partitioned mode only
}

func (s *aggregateState) processLine(line string, lineNo int) error {
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
		return s.processEvent(obj, lineNo)
	case "watermark":
		return s.processWatermark(obj, lineNo)
	case "idle":
		// Only partitioned mode knows idle declarations; legacy
		// single-watermark mode treats it as any unknown record type.
		if s.partitions == 0 {
			return &InputError{Line: lineNo, Reason: fmt.Sprintf("unknown record type %q", recordType)}
		}
		return s.processIdle(obj, lineNo)
	default:
		return &InputError{Line: lineNo, Reason: fmt.Sprintf("unknown record type %q", recordType)}
	}
}

func (s *aggregateState) processEvent(obj map[string]json.RawMessage, lineNo int) error {
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
	p, err := s.requiredPartition(obj, lineNo)
	if err != nil {
		return err
	}
	if s.partitions > 0 && s.idle[p] {
		return &InputError{Line: lineNo, Reason: fmt.Sprintf("event for idle partition %d is not allowed; send a watermark to resume it first", p)}
	}

	if s.watermark != nil && eventTime < *s.watermark {
		fmt.Fprintf(s.lateLog, "line %d: late event time=%d below current watermark %d, skipped\n", lineNo, eventTime, *s.watermark)
		return nil
	}

	start := eventTime - eventTime%s.windowMillis
	if start > math.MaxInt64-s.windowMillis {
		return &InputError{Line: lineNo, Reason: fmt.Sprintf("window end overflow for key %q window starting at %d with length %d", key, start, s.windowMillis)}
	}
	end := start + s.windowMillis

	id := windowID{start: start, key: key}
	st := s.windows[id]
	if st == nil {
		st = &windowState{end: end}
		s.windows[id] = st
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

func (s *aggregateState) processWatermark(obj map[string]json.RawMessage, lineNo int) error {
	next, err := requiredNonNegInt64(obj, "time", lineNo)
	if err != nil {
		return err
	}
	p, err := s.requiredPartition(obj, lineNo)
	if err != nil {
		return err
	}

	if s.partitions == 0 {
		if s.watermark != nil && next < *s.watermark {
			return &InputError{Line: lineNo, Reason: fmt.Sprintf("watermark moved backwards from %d to %d", *s.watermark, next)}
		}
		s.watermark = &next
	} else {
		resuming := s.idle[p]
		if resuming {
			// A resume watermark must be at least the partition's previous
			// watermark and the effective watermark produced while it was
			// idle; equality on either boundary resumes.
			if prev, ok := s.partWatermark[p]; ok && next < *prev {
				return &InputError{Line: lineNo, Reason: fmt.Sprintf("resume watermark %d for partition %d is below its previous watermark %d", next, p, *prev)}
			}
			if s.watermark != nil && next < *s.watermark {
				return &InputError{Line: lineNo, Reason: fmt.Sprintf("resume watermark %d for partition %d is below the current effective watermark %d", next, p, *s.watermark)}
			}
			delete(s.idle, p)
		} else if prev, ok := s.partWatermark[p]; ok && next < *prev {
			return &InputError{Line: lineNo, Reason: fmt.Sprintf("watermark for partition %d moved backwards from %d to %d", p, *prev, next)}
		}
		s.partWatermark[p] = &next
		if effective := s.effectiveWatermark(); effective != nil {
			s.watermark = effective
		}
	}

	if s.watermark == nil {
		return nil
	}
	return s.closeWindows(*s.watermark)
}

// processIdle declares a partition idle: it stops contributing to the
// effective watermark until its next watermark record. The declaration
// itself may advance the effective watermark and close windows.
func (s *aggregateState) processIdle(obj map[string]json.RawMessage, lineNo int) error {
	p, err := s.requiredPartition(obj, lineNo)
	if err != nil {
		return err
	}
	wasIdle := s.idle[p]
	s.idle[p] = true

	// Recompute the effective watermark. With the partition excluded, the
	// remaining non-idle partitions determine it; if every partition is idle
	// the last produced effective watermark is retained instead of clearing.
	// Repeated idle declarations do not change the state of the windows.
	if !wasIdle {
		if next := s.effectiveWatermark(); next != nil {
			s.watermark = next
		}
	}
	if s.watermark == nil {
		return nil
	}
	return s.closeWindows(*s.watermark)
}

// effectiveWatermark returns the minimum watermark over the non-idle
// partitions once each of them has reported at least once, or nil
// otherwise. When every partition is idle, nil is returned so the caller
// keeps the last produced effective watermark.
func (s *aggregateState) effectiveWatermark() *int64 {
	active := s.partitions - int64(len(s.idle))
	if active <= 0 {
		return nil
	}
	var min int64
	first := true
	for p := int64(0); p < s.partitions; p++ {
		if s.idle[p] {
			continue
		}
		w, ok := s.partWatermark[p]
		if !ok {
			return nil
		}
		if first || *w < min {
			min = *w
			first = false
		}
	}
	return &min
}

// requiredPartition returns the record's partition index. In legacy mode it
// returns 0 without inspecting the record, so extra fields stay ignored.
func (s *aggregateState) requiredPartition(obj map[string]json.RawMessage, lineNo int) (int64, error) {
	if s.partitions == 0 {
		return 0, nil
	}
	p, err := requiredInt64(obj, "partition", lineNo)
	if err != nil {
		return 0, err
	}
	if p < 0 || p >= s.partitions {
		return 0, &InputError{Line: lineNo, Reason: fmt.Sprintf("field %q must be an integer in range [0,%d), got %d", "partition", s.partitions, p)}
	}
	return p, nil
}

func (s *aggregateState) closeWindows(watermark int64) error {
	type pending struct {
		id windowID
		st *windowState
	}
	var ready []pending
	for id, st := range s.windows {
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
		if _, err := s.out.Write(append(encoded, '\n')); err != nil {
			return err
		}
		delete(s.windows, p.id)
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
