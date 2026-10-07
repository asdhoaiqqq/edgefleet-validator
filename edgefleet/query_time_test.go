package edgefleet

import (
	"strings"
	"testing"
	"time"
)

// These tests pin the explicit health-query instant (--at) acceptance window:
// it is parsed with the same textual timezone-offset and fractional-second
// rules as a heartbeat collection time, because the query instant drives the
// exact 60-second online edge and a folded offset or truncated sub-nanosecond
// fraction would otherwise judge a moment the user never named.
//
// Unlike collected_at the query instant is never persisted, so the
// instant-level year/saved-offset rules do not apply here; the text rules
// (offset fields 00..23 / 00..59, fraction exact at nanosecond precision)
// do, and are the shared validateCollectedAtText/validateCollectedAtFraction
// functions themselves.

func TestParseQueryTimeRejectsOutOfRangeOffsets(t *testing.T) {
	cases := []struct {
		name    string
		stamp   string
		offset  string // the exact offending offset substring the message must name
		wantMsg string
	}{
		{"plus 24 hours", "2026-10-01T12:00:00+24:00", "+24:00", "00-23"},
		{"minus 24 hours", "2026-10-01T12:00:00-24:00", "-24:00", "00-23"},
		{"60 offset minutes folds to one hour", "2026-10-01T12:00:00+00:60", "+00:60", "00-59"},
		{"23 hours 60 minutes folds to 24 hours", "2026-10-01T12:00:00+23:60", "+23:60", "00-59"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseQueryTime(tc.stamp)
			if err == nil {
				t.Fatalf("offset %q must be rejected, got instant %v", tc.stamp, got)
			}
			msg := err.Error()
			for _, want := range []string{"--at", tc.offset, tc.wantMsg} {
				if !strings.Contains(msg, want) {
					t.Errorf("error %q must mention %q", msg, want)
				}
			}
			// It must fail as an offset error, not be folded into a legal zone.
			if strings.Contains(msg, "nanosecond") {
				t.Errorf("out-of-range offset must fail as an offset error: %v", err)
			}
		})
	}
}

func TestParseQueryTimeRejectsFractionBeyondNanoseconds(t *testing.T) {
	cases := []struct {
		name  string
		stamp string
		frac  string // the offending fraction text the error must quote
	}{
		{"tenth digit non-zero, dot", "2026-10-01T12:01:00.0000000001Z", ".0000000001"},
		{"tenth digit non-zero, comma", "2026-10-01T12:01:00,0000000001Z", ",0000000001"},
		{"non-zero later digit", "2026-10-01T12:01:00.00000000001Z", ".00000000001"},
		{"non-zero far past the ninth", "2026-10-01T12:01:00.0000000000001Z", ".0000000000001"},
		{"tenth digit non-zero with offset", "2026-10-01T20:01:00.0000000001+08:00", ".0000000001"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseQueryTime(tc.stamp)
			if err == nil {
				t.Fatalf("fraction %q must be rejected, not truncated, got %v", tc.frac, got)
			}
			msg := err.Error()
			for _, want := range []string{"--at", tc.frac, "nanosecond"} {
				if !strings.Contains(msg, want) {
					t.Errorf("error %q must mention %q", msg, want)
				}
			}
		})
	}
}

func TestParseQueryTimeAcceptsBoundaries(t *testing.T) {
	cases := []struct {
		name  string
		stamp string
	}{
		{"zulu", "2026-10-01T12:00:00Z"},
		{"negative zero offset", "2026-10-01T12:00:00-00:00"},
		{"positive zero offset", "2026-10-01T12:00:00+00:00"},
		{"max positive offset", "2026-10-02T11:59:00+23:59"},
		{"max negative offset", "2026-10-01T12:01:00-23:59"},
		{"no fraction", "2026-10-01T12:00:00+08:00"},
		{"nine-digit fraction", "2026-10-01T12:00:00.123456789Z"},
		{"nine-digit comma fraction", "2026-10-01T12:00:00,123456789Z"},
		{"trailing zero past the ninth", "2026-10-01T12:01:00.0000000000Z"},
		{"many trailing zeros", "2026-10-01T12:01:00.0000000000000Z"},
		{"trailing zero with comma and offset", "2026-10-01T20:01:00,5000000000+08:00"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseQueryTime(tc.stamp)
			if err != nil {
				t.Fatalf("boundary value %q must be accepted, got %v", tc.stamp, err)
			}
			want, perr := time.Parse(time.RFC3339, tc.stamp)
			if perr != nil {
				t.Fatal(perr)
			}
			if !got.Equal(want) {
				t.Errorf("decoded instant %v, want %v", got, want)
			}
		})
	}
}

func TestParseQueryTimeRejectsMalformedText(t *testing.T) {
	cases := []struct {
		name  string
		stamp string
	}{
		{"empty", ""},
		{"no timezone", "2026-10-01T12:00:00"},
		{"not a time", "yesterday"},
		{"seconds-precision offset", "2026-10-01T12:00:00+08:00:30"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseQueryTime(tc.stamp)
			if err == nil {
				t.Fatalf("malformed value %q must be rejected, got %v", tc.stamp, got)
			}
			if !strings.Contains(err.Error(), "--at") {
				t.Errorf("error must name --at, got %v", err)
			}
		})
	}
}

// TestParseQueryTimeEquivalentSpellingsEqual pins that a padded fraction and
// another timezone spelling of the same written moment decode to the same
// instant, so health judgements are identical regardless of spelling.
func TestParseQueryTimeEquivalentSpellingsEqual(t *testing.T) {
	candidates := []string{
		"2026-10-01T12:01:00.5Z",
		"2026-10-01T12:01:00.500000000Z",
		"2026-10-01T12:01:00.5000000000Z",
		"2026-10-01T12:01:00,5000000000Z",
		"2026-10-01T20:01:00.5+08:00",
		"2026-10-01T08:01:00.5-04:00",
	}
	var first time.Time
	for i, s := range candidates {
		got, err := ParseQueryTime(s)
		if err != nil {
			t.Fatalf("equivalent spelling %q rejected: %v", s, err)
		}
		if i == 0 {
			first = got
			continue
		}
		if !got.Equal(first) {
			t.Errorf("%q decoded to %v, want %v", s, got, first)
		}
	}
}

// TestParseQueryTimeFeedsExactOnlineEdge is the reported regression at the
// domain level: a heartbeat collected at 12:00:00Z must be online for a query
// exactly 60s later and offline one nanosecond later, while a query text that
// only differs from the 60s instant past the ninth fractional digit is
// rejected as an input error rather than truncated into an online result.
func TestParseQueryTimeFeedsExactOnlineEdge(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	collected := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	if _, _, err := store.Submit([]Heartbeat{hb("n1", 1, collected, "1.0", 1, 0)}, collected); err != nil {
		t.Fatal(err)
	}

	// 60.0 seconds: the tenth digit is a zero, so the padded text is accepted
	// and denotes exactly the boundary instant — still online.
	at, err := ParseQueryTime("2026-10-01T12:01:00.0000000000Z")
	if err != nil {
		t.Fatalf("padded exact boundary must be accepted: %v", err)
	}
	r, err := store.Health("n1", at, "1.0", 0)
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != "online" {
		t.Errorf("at exactly 60s want online, got %s", r.Status)
	}

	// 60 seconds plus one nanosecond, written exactly within nine digits:
	// accepted, and offline.
	at, err = ParseQueryTime("2026-10-01T12:01:00.000000001Z")
	if err != nil {
		t.Fatalf("legal one-nanosecond-overshoot query must be accepted: %v", err)
	}
	r, err = store.Health("n1", at, "1.0", 0)
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != "offline" {
		t.Errorf("at 60s+1ns want offline, got %s", r.Status)
	}

	// The same overshoot expressed only past the ninth digit must NOT be
	// truncated back to exactly 60s and judged online: it is an input error.
	if _, err := ParseQueryTime("2026-10-01T12:01:00.0000000001Z"); err == nil {
		t.Fatal("sub-nanosecond overshoot past nine digits must be rejected, not truncated to online")
	}
}
