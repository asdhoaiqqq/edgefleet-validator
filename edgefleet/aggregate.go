package edgefleet

import (
	"bytes"
	"encoding/json"
	"errors"
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

// OutputError reports that output triggered by one physical input line could
// not be written in full. Line is that input line (blank lines count), and
// Kind is either "window result" or "late-event notice". Detail identifies
// the record: a window's key and [start,end), or the skipped event time and
// the watermark used to judge it. Err is the writer's own error, including
// when only a prefix was accepted before it failed; when the writer reported
// no error but accepted too few bytes, Err is io.ErrShortWrite. Use
// errors.Is to inspect it. Content the writer already accepted stays in
// place and fully written earlier results stay valid.
type OutputError struct {
	Line   int
	Kind   string
	Detail string
	Err    error
}

func (e *OutputError) Error() string {
	return fmt.Sprintf("line %d: %s not fully written (%s): %v", e.Line, e.Kind, e.Detail, e.Err)
}

func (e *OutputError) Unwrap() error {
	return e.Err
}

// writeFull writes all of p to w in one call. Any writer error is returned
// unchanged, even when the writer accepted a prefix first, so callers keep
// errors.Is access to the original error. A nil error with fewer than
// len(p) accepted bytes is reported as io.ErrShortWrite; writeFull never
// retries or resends the missing tail.
func writeFull(w io.Writer, p []byte) error {
	n, err := w.Write(p)
	if err != nil {
		return err
	}
	if n < len(p) {
		return io.ErrShortWrite
	}
	return nil
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
// fixed window results, one JSON object per line, to out. windowMillis must
// be a positive signed 64-bit integer. Windows are fixed-length, start at
// time zero and use a left-closed right-open interval. Closure is driven
// solely by input watermarks: a window whose end is less than or equal to the
// current watermark is emitted once, ordered by end ascending and then by key
// in UTF-8 byte order. Late events (event time below the current watermark)
// are reported on lateLog with their physical line number and skipped. Every
// output must land in full: a window result is its complete JSON object plus
// trailing newline and a late-event notice is its complete single line. If
// the corresponding writer returns an error after accepting zero bytes or
// only a prefix, or accepts too few bytes with no error, the run fails at
// once with *OutputError naming the triggering physical input line:
// window-result failures identify the key and window [start,end), and
// late-notice failures identify the skipped event time and the watermark
// used; a nil-error short write is io.ErrShortWrite and errors.Is exposes
// the writer's original error. After such a failure no further records are
// processed, the failed content is neither rewritten nor resent, and
// still-open windows produce no further results; content already accepted
// by the writers stays in place, including fully written earlier results.
// Fatal record problems return *InputError; results already written to out
// stay written, and open windows are not flushed at end of input.
//
// A read failure is distinct from reaching the end of input. At clean
// io.EOF a final record that arrived without a trailing newline is still
// processed (and a damaged one is an *InputError carrying its physical line
// number). When r returns any other error after delivering bytes, only
// records whose newline was already received are processed, in order,
// including records returned by the same Read that carried the error; the
// remaining unterminated bytes never become an event, watermark or idle
// declaration and can produce no late notice or window result, and the
// reader's own error is then returned unchanged so errors.Is exposes it. A
// record or output failure on one of those complete lines is reported
// instead of the read error discovered afterwards.
//
// This is the legacy single-watermark mode: records carry no partition field
// and any extra fields are ignored. Use RunAggregatePartitioned to merge
// several independent input sources of the same stream, or
// RunAggregateSliding to emit overlapping windows at a shorter interval.
func RunAggregate(r io.Reader, windowMillis int64, out io.Writer, lateLog io.Writer) error {
	return RunAggregatePartitionedSliding(r, windowMillis, windowMillis, 0, out, lateLog)
}

// RunAggregateSliding is RunAggregate with overlapping sliding windows. The
// window length is windowMillis and consecutive window starts are slideMillis
// apart, beginning at time zero: 0, slideMillis, 2*slideMillis, ... Each
// window is left-closed right-open and no window with a negative start is
// created. slideMillis must be a positive signed 64-bit integer no greater
// than windowMillis and does not have to divide it; slideMillis equal to
// windowMillis is exactly RunAggregate's fixed-window behavior. A valid event
// is counted once in every window that contains its event time, adding to
// that window's per-key count and the full value to its sum. Everything else
// (watermark-only closure, ordering, late reporting, overflow handling and
// end-of-input behavior) is identical to RunAggregate.
func RunAggregateSliding(r io.Reader, windowMillis, slideMillis int64, out io.Writer, lateLog io.Writer) error {
	return RunAggregatePartitionedSliding(r, windowMillis, slideMillis, 0, out, lateLog)
}

// RunAggregatePartitioned is RunAggregatePartitionedSliding with slideMillis
// equal to windowMillis (non-overlapping fixed windows).
func RunAggregatePartitioned(r io.Reader, windowMillis, partitions int64, out io.Writer, lateLog io.Writer) error {
	return RunAggregatePartitionedSliding(r, windowMillis, windowMillis, partitions, out, lateLog)
}

// RunAggregatePartitionedSliding combines sliding windows (see
// RunAggregateSliding) with partitioned inputs: events from different
// partitions merge into one count and sum per key and window, and both the
// late-event check and window closure use the effective (minimum) watermark.
// windowMillis and slideMillis are validated exactly as in
// RunAggregateSliding. partitions selects the input mode:
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
// RunAggregateSliding.
func RunAggregatePartitionedSliding(r io.Reader, windowMillis, slideMillis, partitions int64, out io.Writer, lateLog io.Writer) error {
	if windowMillis <= 0 {
		return fmt.Errorf("window length must be a positive signed 64-bit integer, got %d", windowMillis)
	}
	if slideMillis <= 0 {
		return fmt.Errorf("slide interval must be a positive signed 64-bit integer, got %d", slideMillis)
	}
	if slideMillis > windowMillis {
		return fmt.Errorf("slide interval %d must not exceed window length %d", slideMillis, windowMillis)
	}
	if partitions < 0 {
		return fmt.Errorf("partition count must be a positive signed 64-bit integer, got %d", partitions)
	}
	s := &aggregateState{
		windowMillis:  windowMillis,
		slideMillis:   slideMillis,
		windows:       make(map[windowID]*windowState),
		out:           out,
		lateLog:       lateLog,
		partitions:    partitions,
		partWatermark: make(map[int64]*int64),
		idle:          make(map[int64]bool),
	}

	// Read records with a line collector that keeps a read failure distinct
	// from end of input: only newline-terminated lines (plus, at a clean EOF,
	// one unterminated final line) ever become records.
	lineNo := 0
	err := readAggregateLines(r, func(line string, no int) error {
		lineNo = no
		if strings.TrimSpace(line) == "" {
			return nil
		}
		return s.processLine(line, lineNo)
	})
	return err
}

// maxAggregateLineBytes caps the size of a single physical input line, matching
// the scanner buffer limit this used to run with. A line this long is fatal
// before its record is parsed.
const maxAggregateLineBytes = 1024 * 1024 * 1024

// errAggregateLineTooLong reports a physical input line that never ends within
// maxAggregateLineBytes.
var errAggregateLineTooLong = errors.New("aggregate input line longer than 1 GiB")

// errAggregateBadReadCount reports an io.Reader that handed back a byte count
// outside the destination buffer, violating the io.Reader contract.
var errAggregateBadReadCount = errors.New("aggregate reader returned an impossible byte count")

// readAggregateLines feeds newline-delimited physical lines from r to handle,
// counting every line (a bare "\n" is a blank line too) and stripping a single
// trailing '\r' so CRLF input works as before. Clean end of input and a
// non-EOF read failure are handled differently: at clean io.EOF a final line
// missing its newline is still delivered, exactly like every other final
// record; after a non-EOF read error only lines whose newline was already
// received (including those delivered by the same Read that carried the
// error) are handled, in order, and then the reader's original error is
// returned. The unterminated tail never reaches handle, so a read failure
// mid-record can neither be misreported as an input error nor produce
// events, watermarks, idle declarations, late notices or window results. A
// non-nil error from handle stops reading immediately and replaces the read
// error, so an earlier record or output failure is not overwritten by a
// failure the reader only reports later.
func readAggregateLines(r io.Reader, handle func(line string, lineNo int) error) error {
	// buf[start:end] is the undelivered tail; buf[:start] is space freed by
	// already delivered lines. The buffer is compacted and grown like
	// bufio.Scanner's, so a long unterminated line is bounded by
	// maxAggregateLineBytes instead of growing without limit.
	buf := make([]byte, 64*1024)
	start, end := 0, 0
	lineNo := 0
	emptyReads := 0
	deliver := func(b []byte) error {
		lineNo++
		line := strings.TrimSuffix(string(b), "\r")
		return handle(line, lineNo)
	}
	for {
		// Move the pending tail to the front when its freed prefix is large
		// enough or when the whole buffer is pending.
		if start > 0 && (end == len(buf) || start > len(buf)/2) {
			copy(buf, buf[start:end])
			end -= start
			start = 0
		}
		if end == len(buf) {
			// No newline fit before the end: the physical line is too long.
			if len(buf) >= maxAggregateLineBytes {
				return errAggregateLineTooLong
			}
			newLen := len(buf) * 2
			if newLen > maxAggregateLineBytes || newLen < len(buf) {
				newLen = maxAggregateLineBytes
			}
			grown := make([]byte, newLen)
			copy(grown, buf[start:end])
			buf = grown
			end -= start
			start = 0
		}
		n, readErr := r.Read(buf[end:])
		if n < 0 || n > len(buf)-end {
			// A Reader must never report a byte count outside the buffer.
			return errAggregateBadReadCount
		}
		end += n
		for {
			i := bytes.IndexByte(buf[start:end], '\n')
			if i < 0 {
				break
			}
			if err := deliver(buf[start : start+i]); err != nil {
				return err
			}
			start += i + 1
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				// Clean end of input: a trailing line without a newline is a
				// complete final record; an empty tail (input ending exactly
				// on '\n') is nothing, not an extra blank line.
				if end > start {
					if err := deliver(buf[start:end]); err != nil {
						return err
					}
				}
				return nil
			}
			// Any other failure means the stream stopped mid-record: the
			// still-unterminated bytes are not a record. Surface the original
			// error unchanged so errors.Is keeps working for callers.
			return readErr
		}
		if n == 0 {
			// No data and no error: refuse to spin forever on a broken Reader.
			emptyReads++
			if emptyReads > 100 {
				return io.ErrNoProgress
			}
		} else {
			emptyReads = 0
		}
	}
}

type aggregateState struct {
	windowMillis int64
	slideMillis  int64 // distance between consecutive window starts; equals windowMillis for fixed windows
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

// floorDiv returns math.Floor(a/b) for b > 0; Go's / truncates toward zero,
// so negative numerators need explicit adjustment.
func floorDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && a < 0 {
		q--
	}
	return q
}

// containingWindowBounds returns the first and last (inclusive) window index
// whose window [k*slideMillis, k*slideMillis+windowMillis) contains
// eventTime. Windows start at 0, slideMillis, 2*slideMillis, ... and none
// with a negative start is created, so the first index is clamped to zero.
// The last index is eventTime/slideMillis, so an event at a window end is
// outside that window.
func (s *aggregateState) containingWindowBounds(eventTime int64) (first, last int64) {
	last = eventTime / s.slideMillis
	first = floorDiv(eventTime-s.windowMillis, s.slideMillis) + 1
	if first < 0 {
		first = 0
	}
	return first, last
}

// containingWindowCount returns how many windows contain eventTime.
func (s *aggregateState) containingWindowCount(eventTime int64) int64 {
	first, last := s.containingWindowBounds(eventTime)
	return last - first + 1
}

// containingStart returns the start of the containing window reached back by
// offset indices from the last one (offset 0 = last window).
func (s *aggregateState) containingStart(eventTime, offset int64) int64 {
	last := eventTime - eventTime%s.slideMillis
	return last - offset*s.slideMillis
}

// firstOverflowingStart reports the start of the earliest containing window
// whose end (start + windowMillis) leaves the signed 64-bit range, or -1 when
// every containing window fits. Containing starts are an ascending arithmetic
// sequence with step slideMillis, so overflow (if any) is a suffix; this
// computes its first member directly instead of walking the sequence.
func (s *aggregateState) firstOverflowingStart(eventTime int64) int64 {
	n := s.containingWindowCount(eventTime)
	lowest := s.containingStart(eventTime, n-1) // earliest containing start; never negative
	maxSafeStart := int64(math.MaxInt64 - s.windowMillis)
	if lowest > maxSafeStart {
		return lowest
	}
	safe := (maxSafeStart-lowest)/s.slideMillis + 1 // number of containing starts that fit
	if safe < n {
		// Reach back from last, which is at most eventTime, so the
		// subtraction cannot leave the signed 64-bit range.
		return s.containingStart(eventTime, n-1-safe)
	}
	return -1
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
		notice := fmt.Sprintf("line %d: late event time=%d below current watermark %d, skipped\n", lineNo, eventTime, *s.watermark)
		if err := writeFull(s.lateLog, []byte(notice)); err != nil {
			return &OutputError{
				Line:   lineNo,
				Kind:   "late-event notice",
				Detail: fmt.Sprintf("event time %d below current watermark %d", eventTime, *s.watermark),
				Err:    err,
			}
		}
		return nil
	}

	n := s.containingWindowCount(eventTime)

	// A window whose end leaves the signed 64-bit range is fatal for the
	// whole input line; check before updating any window state.
	if badStart := s.firstOverflowingStart(eventTime); badStart >= 0 {
		return &InputError{Line: lineNo, Reason: fmt.Sprintf("window end overflow for key %q window starting at %d with length %d", key, badStart, s.windowMillis)}
	}

	// Add the event once to each containing window, in ascending start
	// order. An overflow here stops processing immediately; already closed
	// (and thus already written) windows are unaffected.
	for i := n - 1; i >= 0; i-- {
		start := s.containingStart(eventTime, i)
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
	}
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
	return s.closeWindows(*s.watermark, lineNo)
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
	return s.closeWindows(*s.watermark, lineNo)
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

func (s *aggregateState) closeWindows(watermark int64, lineNo int) error {
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
		line := append(encoded, '\n')
		if err := writeFull(s.out, line); err != nil {
			// Stop immediately: the writer may hold zero bytes or only a
			// prefix of this record, but a truncated record is not a fully
			// output one, so the window stays open in the map. Do not retry
			// or resend it and do not emit the remaining ready windows;
			// content already accepted by the writer stays.
			return &OutputError{
				Line:   lineNo,
				Kind:   "window result",
				Detail: fmt.Sprintf("key %q window [%d,%d)", p.id.key, p.id.start, p.st.end),
				Err:    err,
			}
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
