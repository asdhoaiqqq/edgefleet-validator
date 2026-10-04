package edgefleet

import (
	"errors"
	"strings"
	"testing"
)

// ValidateAggregateParams is the one place the startup limits shared by the
// library entry points and the command line are stated. These tests pin both
// the accepted region (including the entry-shared boundary spellings the
// callers depend on) and the structured report for every rejected limit, in
// the order it has always surfaced.
func TestValidateAggregateParamsAccepts(t *testing.T) {
	cases := []struct {
		name                 string
		window, slide, parts int64
	}{
		{"fixed window", 1000, 1000, 0},
		{"sliding window", 1000, 600, 0},
		{"non-dividing interval", 1000, 300, 0},
		{"single partition", 1000, 1000, 1},
		{"many partitions with sliding", 1000, 600, 7},
		{"library zero partitions is single-watermark mode", 1000, 1000, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidateAggregateParams(tc.window, tc.slide, tc.parts); err != nil {
				t.Fatalf("ValidateAggregateParams(%d,%d,%d) = %v, want nil", tc.window, tc.slide, tc.parts, err)
			}
		})
	}
}

func TestValidateAggregateParamsRejects(t *testing.T) {
	cases := []struct {
		name                 string
		window, slide, parts int64
		field                AggregateParamName
		kind                 AggregateParamKind
		value, limit         int64
		wantInError          string
	}{
		{"zero window", 0, 0, 0, AggregateParamWindow, AggregateParamNotPositive, 0, 0, "window length"},
		{"negative window", -1, -1, 0, AggregateParamWindow, AggregateParamNotPositive, -1, 0, "window length"},
		{"min window", -1 << 63, 0, 0, AggregateParamWindow, AggregateParamNotPositive, -1 << 63, 0, "window length"},
		{"zero slide", 1000, 0, 0, AggregateParamSlide, AggregateParamNotPositive, 0, 0, "slide interval"},
		{"negative slide", 1000, -600, 0, AggregateParamSlide, AggregateParamNotPositive, -600, 0, "slide interval"},
		{"slide one over window", 1000, 1001, 0, AggregateParamSlide, AggregateParamTooLarge, 1001, 1000, "must not exceed"},
		{"negative partitions", 1000, 1000, -1, AggregateParamPartitions, AggregateParamNotPositive, -1, 0, "partition count"},
		{"min partitions", 1000, 600, -1 << 63, AggregateParamPartitions, AggregateParamNotPositive, -1 << 63, 0, "partition count"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateAggregateParams(tc.window, tc.slide, tc.parts)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			var pe *AggregateParamError
			if !errors.As(err, &pe) {
				t.Fatalf("error = %T, want *AggregateParamError: %v", err, err)
			}
			if pe.Field != tc.field || pe.Kind != tc.kind || pe.Value != tc.value || pe.Limit != tc.limit {
				t.Fatalf("error fields = (field=%s kind=%s value=%d limit=%d), want (%s %s %d %d)",
					pe.Field, pe.Kind, pe.Value, pe.Limit, tc.field, tc.kind, tc.value, tc.limit)
			}
			if !strings.Contains(pe.Error(), tc.wantInError) {
				t.Fatalf("error text = %q, want substring %q", pe.Error(), tc.wantInError)
			}
		})
	}
}

// The first violated limit is reported in the historical order: window length,
// then slide positivity, then slide-vs-window, then partitions. Several bad
// parameters at once must still name the first one only.
func TestValidateAggregateParamsReportOrder(t *testing.T) {
	cases := []struct {
		name                 string
		window, slide, parts int64
		field                AggregateParamName
	}{
		{"window beats slide and partitions", 0, 0, -1, AggregateParamWindow},
		{"slide positivity beats slide-too-large and partitions", 1000, -1, -1, AggregateParamSlide},
		{"slide too large beats partitions", 1000, 1001, -1, AggregateParamSlide},
		{"partitions is last", 1000, 1000, -5, AggregateParamPartitions},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateAggregateParams(tc.window, tc.slide, tc.parts)
			var pe *AggregateParamError
			if !errors.As(err, &pe) {
				t.Fatalf("error = %v, want *AggregateParamError", err)
			}
			if pe.Field != tc.field {
				t.Fatalf("reported field = %s, want %s", pe.Field, tc.field)
			}
		})
	}
}

// The structured error keeps rendering the library entry points' historical
// wording verbatim, so their callers observe the same parameter errors.
func TestAggregateParamErrorWording(t *testing.T) {
	cases := []struct {
		err  *AggregateParamError
		want string
	}{
		{&AggregateParamError{Field: AggregateParamWindow, Kind: AggregateParamNotPositive, Value: 0},
			"window length must be a positive signed 64-bit integer, got 0"},
		{&AggregateParamError{Field: AggregateParamSlide, Kind: AggregateParamNotPositive, Value: -600},
			"slide interval must be a positive signed 64-bit integer, got -600"},
		{&AggregateParamError{Field: AggregateParamSlide, Kind: AggregateParamTooLarge, Value: 1001, Limit: 1000},
			"slide interval 1001 must not exceed window length 1000"},
		{&AggregateParamError{Field: AggregateParamPartitions, Kind: AggregateParamNotPositive, Value: -1},
			"partition count must be a positive signed 64-bit integer, got -1"},
	}
	for _, tc := range cases {
		if got := tc.err.Error(); got != tc.want {
			t.Errorf("Error() = %q, want %q", got, tc.want)
		}
	}
}
