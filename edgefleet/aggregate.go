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
	"unicode/utf8"
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

// AggregateParamName identifies which aggregate start parameter a
// *AggregateParamError reports.
type AggregateParamName string

const (
	// AggregateParamWindow is the window length (--window-ms).
	AggregateParamWindow AggregateParamName = "window"
	// AggregateParamSlide is the slide interval (--slide-ms).
	AggregateParamSlide AggregateParamName = "slide"
	// AggregateParamPartitions is the partition count (--partitions).
	AggregateParamPartitions AggregateParamName = "partitions"
)

// AggregateParamKind classifies the problem ValidateAggregateParams found with
// one parameter.
type AggregateParamKind string

const (
	// AggregateParamNotPositive means the value was zero or negative where a
	// positive signed 64-bit integer is required.
	AggregateParamNotPositive AggregateParamKind = "not_positive"
	// AggregateParamTooLarge means the slide interval exceeds the window
	// length. It is reported for AggregateParamSlide and carries the window
	// length in Limit.
	AggregateParamTooLarge AggregateParamKind = "too_large"
)

// AggregateParamError reports one invalid aggregate start parameter. It is the
// single source of the shared startup parameter limits: every aggregate entry
// point (the library runners and the command line) funnels its numeric
// parameter checks through ValidateAggregateParams, so the same restriction is
// stated in one place. Which parameter may be omitted, and what an omitted or
// zero value means, is entry-specific and decided by the caller; this reports
// only the limits common to every entry. Field identifies the parameter, Kind
// the violated limit, Value the rejected value and Limit the window length for
// AggregateParamTooLarge (zero otherwise). Error renders the library entry
// point's existing wording, which callers may rephrase for their own surface
// while keeping the field-driven decision identical.
type AggregateParamError struct {
	Field AggregateParamName
	Kind  AggregateParamKind
	Value int64
	Limit int64
}

func (e *AggregateParamError) Error() string {
	switch e.Kind {
	case AggregateParamTooLarge:
		return fmt.Sprintf("slide interval %d must not exceed window length %d", e.Value, e.Limit)
	case AggregateParamNotPositive:
		switch e.Field {
		case AggregateParamWindow:
			return fmt.Sprintf("window length must be a positive signed 64-bit integer, got %d", e.Value)
		case AggregateParamSlide:
			return fmt.Sprintf("slide interval must be a positive signed 64-bit integer, got %d", e.Value)
		case AggregateParamPartitions:
			return fmt.Sprintf("partition count must be a positive signed 64-bit integer, got %d", e.Value)
		}
	}
	return fmt.Sprintf("invalid aggregate parameter %s: %d", e.Field, e.Value)
}

// ValidateAggregateParams enforces the aggregate startup limits shared by every
// entry point, in the order they have always been reported: window length,
// then slide interval, then the slide-vs-window comparison, then the partition
// count. windowMillis must be a positive signed 64-bit integer. slideMillis
// must be a positive signed 64-bit integer no greater than windowMillis; it
// does not have to divide windowMillis, and equality is the fixed-window case.
// partitions must not be negative; zero itself is accepted here because the
// library entry points use it to select legacy single-watermark mode -- an
// entry that rejects an explicitly supplied zero (the command line) keeps that
// policy of its own before or around this call. The first violated limit is
// returned as *AggregateParamError; nil means the parameters are sound and the
// caller may start reading input, so no invalid configuration ever reaches the
// input reader.
func ValidateAggregateParams(windowMillis, slideMillis, partitions int64) error {
	if windowMillis <= 0 {
		return &AggregateParamError{Field: AggregateParamWindow, Kind: AggregateParamNotPositive, Value: windowMillis}
	}
	if slideMillis <= 0 {
		return &AggregateParamError{Field: AggregateParamSlide, Kind: AggregateParamNotPositive, Value: slideMillis}
	}
	if slideMillis > windowMillis {
		return &AggregateParamError{Field: AggregateParamSlide, Kind: AggregateParamTooLarge, Value: slideMillis, Limit: windowMillis}
	}
	if partitions < 0 {
		return &AggregateParamError{Field: AggregateParamPartitions, Kind: AggregateParamNotPositive, Value: partitions}
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
// stay written, and open windows are not flushed at end of input. An event
// key that would lose characters when decoded -- invalid UTF-8 bytes, or a
// \u escape whose surrogate half is unpaired or mispaired -- is such a fatal
// problem: it is never repaired to U+FFFD and counted, so distinct damaged
// keys can never merge or collide with a key that is genuinely U+FFFD. The
// check runs before the late-event check, so a damaged key is fatal even
// below the current watermark.
//
// A read failure is distinct from reaching the end of input. Only a reader
// returning the bare io.EOF sentinel is a clean end, where a final record that
// arrived without a trailing newline is still processed (and a damaged one is
// an *InputError carrying its physical line number). A wrapped io.EOF or a
// combined error whose chain merely contains io.EOF is still a read failure.
// When r returns any error other than the bare io.EOF after delivering bytes,
// only records whose newline was already received are processed, in order,
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
	if err := ValidateAggregateParams(windowMillis, slideMillis, partitions); err != nil {
		return err
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
// non-EOF read failure are handled differently: only the bare io.EOF
// sentinel is a clean end, and then a final line missing its newline is still
// delivered, exactly like every other final record; a wrapped io.EOF or a
// combined error whose chain contains io.EOF is a failure, not an end. After
// any error other than the bare io.EOF only lines whose newline was already
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
	//
	// searchFrom is the earliest absolute index not yet inspected for a '\n':
	// every byte before it has either been delivered or searched already. A
	// Read can only introduce a newline in the bytes it just appended, so the
	// pending tail of a long line is never rescanned; scanning, compaction and
	// growth together stay linear in the cumulative byte count even when each
	// Read brings a single byte. searchFrom >= start always holds, and
	// compaction/growth shift every absolute index by the same start they do.
	buf := make([]byte, 64*1024)
	start, end := 0, 0
	searchFrom := 0
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
			searchFrom -= start
			if searchFrom < 0 {
				searchFrom = 0
			}
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
			searchFrom -= start
			if searchFrom < 0 {
				searchFrom = 0
			}
			start = 0
		}
		n, readErr := r.Read(buf[end:])
		if n < 0 || n > len(buf)-end {
			// A Reader must never report a byte count outside the buffer.
			return errAggregateBadReadCount
		}
		end += n
		// Only this Read's newly appended bytes can still hold an unseen
		// newline; search from searchFrom rather than from start, so a long
		// line delivered a few bytes at a time costs one scan of each byte
		// instead of one scan per Read over the whole accumulated tail.
		for searchFrom < end {
			i := bytes.IndexByte(buf[searchFrom:end], '\n')
			if i < 0 {
				break
			}
			idx := searchFrom + i
			if err := deliver(buf[start:idx]); err != nil {
				return err
			}
			start = idx + 1
			searchFrom = start
		}
		searchFrom = end
		if readErr != nil {
			if readErr == io.EOF {
				// Clean end of input: a trailing line without a newline is a
				// complete final record; an empty tail (input ending exactly
				// on '\n') is nothing, not an extra blank line. Only the
				// bare io.EOF sentinel counts -- a wrapped EOF or a combined
				// error whose chain contains io.EOF is a read failure, never
				// a clean end.
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

// containingWindows describes every window that contains one event time, in
// one derivation shared by the normal counting path and the window-end
// overflow precheck: how many windows there are, where each starts, and which
// (if any) is the earliest whose end leaves the signed 64-bit range. Windows
// start at 0, slide, 2*slide, ... and none with a negative start is created,
// so the first index is clamped to zero. The last index is
// eventTime/slide, so an event at a window end is outside that window.
type containingWindows struct {
	count     int64 // how many windows contain the event time
	lastStart int64 // start of the last (highest) containing window
	slide     int64 // distance between consecutive window starts
	// firstOverflow is the start of the earliest containing window whose end
	// (start + windowMillis) leaves the signed 64-bit range, or -1 when every
	// containing window fits. Containing starts are an ascending arithmetic
	// sequence with step slide, so overflow (if any) is a suffix and this is
	// its first member.
	firstOverflow int64
}

// containingWindows derives the containing-window range for eventTime.
func (s *aggregateState) containingWindows(eventTime int64) containingWindows {
	last := eventTime / s.slideMillis
	first := floorDiv(eventTime-s.windowMillis, s.slideMillis) + 1
	if first < 0 {
		first = 0
	}
	w := containingWindows{
		count:         last - first + 1,
		lastStart:     eventTime - eventTime%s.slideMillis,
		slide:         s.slideMillis,
		firstOverflow: -1,
	}
	lowest := w.start(w.count - 1) // earliest containing start; never negative
	maxSafeStart := int64(math.MaxInt64 - s.windowMillis)
	switch {
	case lowest > maxSafeStart:
		w.firstOverflow = lowest
	default:
		safe := (maxSafeStart-lowest)/s.slideMillis + 1 // containing starts that fit
		if safe < w.count {
			// Reach back from lastStart, which is at most eventTime, so the
			// subtraction cannot leave the signed 64-bit range.
			w.firstOverflow = w.start(w.count - 1 - safe)
		}
	}
	return w
}

// start returns the start of the containing window reached back by offset
// indices from the last one (offset 0 = last window).
func (w containingWindows) start(offset int64) int64 {
	return w.lastStart - offset*w.slide
}

// eventRecord is one input event whose record content has already been
// interpreted and validated independently of aggregate state: key, time,
// value and (in partitioned mode) partition are final. Nothing about idle
// partitions, watermarks or windows has been inspected yet, so producing one
// never mutates -- or even reads -- window state.
type eventRecord struct {
	key       string
	time      int64
	value     int64
	partition int64 // partitioned mode only; always 0 in legacy mode
}

// parseEventRecord is the interpretation half of event intake. It reads and
// checks the record's own fields and never consults aggregate state, so field
// legality is fully decided before any idle, late or window rule runs. Fields
// are validated in the order they have always been rejected: key, then time,
// then value, then partition. The key is decoded strictly: an empty key,
// invalid UTF-8 bytes and an unpaired or mispaired surrogate half are record
// errors, never content repaired to U+FFFD that could merge keys. In legacy
// mode (partitioned == false) extra fields -- including any stray partition
// field -- stay ignored and partition is reported as zero.
func parseEventRecord(obj map[string]json.RawMessage, partitioned bool, partitionCount int64, lineNo int) (eventRecord, error) {
	key, err := requiredKey(obj, lineNo)
	if err != nil {
		return eventRecord{}, err
	}
	if key == "" {
		return eventRecord{}, &InputError{Line: lineNo, Reason: `field "key" must be a non-empty string`}
	}
	eventTime, err := requiredNonNegInt64(obj, "time", lineNo)
	if err != nil {
		return eventRecord{}, err
	}
	value, err := requiredInt64(obj, "value", lineNo)
	if err != nil {
		return eventRecord{}, err
	}
	var partition int64
	if partitioned {
		partition, err = requiredPartitionField(obj, partitionCount, lineNo)
		if err != nil {
			return eventRecord{}, err
		}
	}
	return eventRecord{
		key:       key,
		time:      eventTime,
		value:     value,
		partition: partition,
	}, nil
}

func (s *aggregateState) processEvent(obj map[string]json.RawMessage, lineNo int) error {
	// Stage 1: interpret and validate the record's own content without
	// touching aggregate state.
	ev, err := parseEventRecord(obj, s.partitions > 0, s.partitions, lineNo)
	if err != nil {
		return err
	}
	// Stage 2: apply the validated event to aggregate state.
	return s.acceptEvent(ev, lineNo)
}

// acceptEvent is the state half of event intake: it applies one record that
// parseEventRecord has already fully validated, in the same order as before
// the split -- idle partition first, then the global-watermark late rule, and
// only afterwards the window counts. It never re-reads or re-validates record
// fields, so a field problem can never be downgraded to an idle failure or a
// late notice here. Partitioned mode judges lateness against the overall
// effective watermark; an event on an idle partition stays fatal and never
// resumes the partition implicitly. A strictly late event is skipped with the
// existing notice; an event time equal to the watermark still enters its
// windows.
func (s *aggregateState) acceptEvent(ev eventRecord, lineNo int) error {
	if s.partitions > 0 && s.idle[ev.partition] {
		return &InputError{Line: lineNo, Reason: fmt.Sprintf("event for idle partition %d is not allowed; send a watermark to resume it first", ev.partition)}
	}

	if s.watermark != nil && ev.time < *s.watermark {
		notice := fmt.Sprintf("line %d: late event time=%d below current watermark %d, skipped\n", lineNo, ev.time, *s.watermark)
		if err := writeFull(s.lateLog, []byte(notice)); err != nil {
			return &OutputError{
				Line:   lineNo,
				Kind:   "late-event notice",
				Detail: fmt.Sprintf("event time %d below current watermark %d", ev.time, *s.watermark),
				Err:    err,
			}
		}
		return nil
	}

	return s.applyEventToWindows(ev, lineNo)
}

// applyEventToWindows counts one accepted event in every window that contains
// its event time, one count and the full value per window. It only mutates
// window state; the caller has already settled field validity, idleness and
// lateness. The containing-window range is derived once and shared by the
// window-end overflow precheck and the counting loop, so both judge the exact
// same set of windows. The precheck runs before any window is touched;
// per-window count and sum overflows then fail on this triggering event in
// ascending window-start order rather than at watermark closure.
func (s *aggregateState) applyEventToWindows(ev eventRecord, lineNo int) error {
	wins := s.containingWindows(ev.time)

	// A window whose end leaves the signed 64-bit range is fatal for the
	// whole input line; check before updating any window state.
	if wins.firstOverflow >= 0 {
		return &InputError{Line: lineNo, Reason: fmt.Sprintf("window end overflow for key %q window starting at %d with length %d", ev.key, wins.firstOverflow, s.windowMillis)}
	}

	// Add the event once to each containing window, in ascending start
	// order. An overflow here stops processing immediately; already closed
	// (and thus already written) windows are unaffected.
	for i := wins.count - 1; i >= 0; i-- {
		if err := s.addEventToWindow(ev, wins.start(i), lineNo); err != nil {
			return err
		}
	}
	return nil
}

// addEventToWindow folds a single accepted event into one containing window,
// creating the window first if needed, enforcing the signed 64-bit count and
// cumulative-sum boundaries before the mutation. On failure the window keeps
// its previous count and sum and the error names the physical line, the key
// and this window's [start,end) interval.
func (s *aggregateState) addEventToWindow(ev eventRecord, start int64, lineNo int) error {
	end := start + s.windowMillis
	id := windowID{start: start, key: ev.key}
	st := s.windows[id]
	if st == nil {
		st = &windowState{end: end}
		s.windows[id] = st
	}
	if st.count == math.MaxInt64 {
		return &InputError{Line: lineNo, Reason: fmt.Sprintf("event count overflow for key %q window [%d,%d)", ev.key, start, end)}
	}
	if ev.value > 0 && st.sum > math.MaxInt64-ev.value {
		return &InputError{Line: lineNo, Reason: fmt.Sprintf("cumulative sum overflow for key %q window [%d,%d): %d + %d", ev.key, start, end, st.sum, ev.value)}
	}
	if ev.value < 0 && st.sum < math.MinInt64-ev.value {
		return &InputError{Line: lineNo, Reason: fmt.Sprintf("cumulative sum overflow for key %q window [%d,%d): %d + %d", ev.key, start, end, st.sum, ev.value)}
	}
	st.count++
	st.sum += ev.value
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
	}

	// A watermark update and an idle declaration share one advance-and-close
	// path from here so both triggers follow the same rules.
	return s.advanceWatermark(lineNo)
}

// processIdle declares a partition idle: it stops contributing to the
// effective watermark until its next watermark record. The declaration
// itself may advance the effective watermark and close windows.
func (s *aggregateState) processIdle(obj map[string]json.RawMessage, lineNo int) error {
	p, err := s.requiredPartition(obj, lineNo)
	if err != nil {
		return err
	}
	if s.idle[p] {
		// A repeated idle declaration is a no-op: the partition already
		// stopped contributing, so neither the effective watermark nor the
		// set of closable windows can change.
		return nil
	}
	s.idle[p] = true
	// With the partition excluded, the remaining non-idle partitions
	// determine the effective watermark; the declaration itself can advance
	// it and close windows, exactly like a watermark update.
	return s.advanceWatermark(lineNo)
}

// advanceWatermark applies a partition state change -- a partition watermark
// update or an idle declaration -- and closes every window the resulting
// watermark allows. Both triggers funnel through here so they follow the
// same rules: in partitioned mode the effective watermark is recomputed from
// the current partition state (an active partition that has not reported yet
// keeps it unknown, and when every partition is idle the last produced value
// is retained instead of clearing); in legacy single-watermark mode the
// single watermark is used as is. While no effective watermark exists no
// window closes; otherwise closeWindows emits every window whose end is less
// than or equal to it.
func (s *aggregateState) advanceWatermark(lineNo int) error {
	if s.partitions > 0 {
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
// returns 0 without inspecting the record, so extra fields stay ignored. In
// partitioned mode the actual reading and range checking lives in the shared
// free function used by event interpretation as well.
func (s *aggregateState) requiredPartition(obj map[string]json.RawMessage, lineNo int) (int64, error) {
	if s.partitions == 0 {
		return 0, nil
	}
	return requiredPartitionField(obj, s.partitions, lineNo)
}

// requiredPartitionField reads and range-checks a partitioned-mode record's
// "partition" field. It is pure record interpretation: the partition count is
// passed in rather than read from aggregate state, so both the event intake
// split (parseEventRecord) and the watermark/idle paths validate the field by
// the exact same rule without touching window state.
func requiredPartitionField(obj map[string]json.RawMessage, partitionCount int64, lineNo int) (int64, error) {
	p, err := requiredInt64(obj, "partition", lineNo)
	if err != nil {
		return 0, err
	}
	if p < 0 || p >= partitionCount {
		return 0, &InputError{Line: lineNo, Reason: fmt.Sprintf("field %q must be an integer in range [0,%d), got %d", "partition", partitionCount, p)}
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

// requiredKey returns the event key decoded strictly. encoding/json silently
// repairs damaged string content with U+FFFD -- invalid UTF-8 bytes and \u
// escapes whose surrogate halves are unpaired or mispaired -- which would
// merge distinct damaged keys into one and collide with a key that is
// genuinely U+FFFD. Such damage is a fatal input error here instead, so a
// corrupt key never changes any window's count or sum. Valid characters
// (including a directly encoded or escaped U+FFFD and supplementary-plane
// characters, whether literal or written as a surrogate pair) decode to
// their exact value and aggregate as before.
func requiredKey(obj map[string]json.RawMessage, lineNo int) (string, error) {
	raw, ok := obj["key"]
	if !ok {
		return "", &InputError{Line: lineNo, Reason: `missing required string field "key"`}
	}
	key, err := decodeKeyString(raw)
	if err != nil {
		return "", &InputError{Line: lineNo, Reason: err.Error()}
	}
	return key, nil
}

// decodeKeyString decodes the event key's JSON string without the lossy
// repairs encoding/json applies: invalid UTF-8 byte sequences and \u escapes
// with unpaired or mispaired surrogate halves are errors instead of becoming
// U+FFFD. The record already parsed as JSON, so the string's escape syntax
// is valid; only its character content is checked here. An escaped backslash
// is decoded first, so "\\uD800" stays the literal text D800 and is not
// mistaken for an unpaired surrogate.
func decodeKeyString(raw json.RawMessage) (string, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) < 2 || trimmed[0] != '"' || trimmed[len(trimmed)-1] != '"' {
		return "", fmt.Errorf("field %q must be a JSON string, got %s", "key", string(trimmed))
	}
	s := string(trimmed[1 : len(trimmed)-1])
	var sb strings.Builder
	sb.Grow(len(s))
	for i := 0; i < len(s); {
		c := s[i]
		switch {
		case c == '\\':
			if i+1 >= len(s) {
				return "", fmt.Errorf("field %q ends in an incomplete escape", "key")
			}
			switch s[i+1] {
			case '"', '\\', '/':
				sb.WriteByte(s[i+1])
				i += 2
			case 'b':
				sb.WriteByte('\b')
				i += 2
			case 'f':
				sb.WriteByte('\f')
				i += 2
			case 'n':
				sb.WriteByte('\n')
				i += 2
			case 'r':
				sb.WriteByte('\r')
				i += 2
			case 't':
				sb.WriteByte('\t')
				i += 2
			case 'u':
				r, next, err := decodeUnicodeEscape(s, i)
				if err != nil {
					return "", err
				}
				sb.WriteRune(r)
				i = next
			default:
				return "", fmt.Errorf("field %q has an invalid escape \\%c", "key", s[i+1])
			}
		case c < utf8.RuneSelf:
			sb.WriteByte(c)
			i++
		default:
			r, size := utf8.DecodeRuneInString(s[i:])
			if r == utf8.RuneError && size == 1 {
				return "", fmt.Errorf("field %q contains invalid UTF-8 bytes in its character encoding", "key")
			}
			sb.WriteString(s[i : i+size])
			i += size
		}
	}
	return sb.String(), nil
}

// decodeUnicodeEscape decodes the \uXXXX escape at s[i] (where s[i] == '\\'
// and s[i+1] == 'u'), returning the decoded rune and the index just past the
// escape. A high surrogate must be immediately followed by a \u escape
// carrying the matching low surrogate; the pair combines into one
// supplementary-plane rune. An unpaired or mispaired half is an error rather
// than U+FFFD.
func decodeUnicodeEscape(s string, i int) (rune, int, error) {
	r, next, err := parseHexEscape(s, i)
	if err != nil {
		return 0, 0, err
	}
	switch {
	case r >= 0xD800 && r <= 0xDBFF:
		if next+1 >= len(s) || s[next] != '\\' || s[next+1] != 'u' {
			return 0, 0, fmt.Errorf("field %q has an unpaired high surrogate in a Unicode escape", "key")
		}
		lo, after, err := parseHexEscape(s, next)
		if err != nil {
			return 0, 0, err
		}
		if lo < 0xDC00 || lo > 0xDFFF {
			return 0, 0, fmt.Errorf("field %q has a high surrogate not followed by a low surrogate in a Unicode escape", "key")
		}
		return 0x10000 + (r-0xD800)<<10 + (lo - 0xDC00), after, nil
	case r >= 0xDC00 && r <= 0xDFFF:
		return 0, 0, fmt.Errorf("field %q has an unpaired low surrogate in a Unicode escape", "key")
	}
	return r, next, nil
}

// parseHexEscape reads the four hex digits of the \uXXXX escape at s[i]
// (where s[i] == '\\' and s[i+1] == 'u') and returns their value and the
// index just past them.
func parseHexEscape(s string, i int) (rune, int, error) {
	if i+6 > len(s) {
		return 0, 0, fmt.Errorf("field %q has a truncated Unicode escape", "key")
	}
	var v rune
	for _, c := range s[i+2 : i+6] {
		v <<= 4
		switch {
		case c >= '0' && c <= '9':
			v += c - '0'
		case c >= 'a' && c <= 'f':
			v += c - 'a' + 10
		case c >= 'A' && c <= 'F':
			v += c - 'A' + 10
		default:
			return 0, 0, fmt.Errorf("field %q has a non-hex digit in a Unicode escape", "key")
		}
	}
	return v, i + 6, nil
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
