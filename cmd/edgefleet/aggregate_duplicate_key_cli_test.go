package main

import (
	"bytes"
	"strings"
	"testing"
)

// TestAggregateCLIDuplicateKeyIsFatal drives the command line with an event
// that names its top-level key twice. It must name the physical input line
// (blank lines still count) and the key field on standard error, exit with
// code 1, keep the already completed window on standard output, contribute no
// count or sum, read no later record, and never downgrade the record to a
// late-event notice -- even when the event time is below the current
// watermark and even though the two key strings are identical.
func TestAggregateCLIDuplicateKeyIsFatal(t *testing.T) {
	cases := []struct {
		name string
		line string
	}{
		{"different values", `{"type":"event","key":"dup","key":"other","time":500,"value":1}`},
		{"identical values", `{"type":"event","key":"dup","key":"dup","time":500,"value":1}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			input := strings.Join([]string{
				`{"type":"event","key":"ok","time":100,"value":2}`, // line 1
				`{"type":"watermark","time":1000}`,                 // line 2: closes [0,1000)
				``,                                                 // line 3: blank, still counted
				tc.line,                                            // line 4: duplicate, and below the watermark
				`{"type":"event","key":"later","time":1500,"value":9}`, // line 5: must never be read
				`{"type":"watermark","time":2000}`,                     // line 6: must never be read
			}, "\n") + "\n"

			cmd := aggregateCommand(t, input, "--window-ms", "1000")
			var stdout, stderr bytes.Buffer
			cmd.Stdout = &stdout
			cmd.Stderr = &stderr
			err := cmd.Run()
			if err == nil {
				t.Fatal("aggregate succeeded, want exit code 1 for a duplicate key")
			}
			if code := cmd.ProcessState.ExitCode(); code != 1 {
				t.Fatalf("exit code = %d, want 1", code)
			}

			errText := stderr.String()
			for _, want := range []string{"line 4", `duplicate field "key"`} {
				if !strings.Contains(errText, want) {
					t.Fatalf("stderr = %q, want it to mention %q", errText, want)
				}
			}
			if strings.Contains(errText, "late event") {
				t.Fatalf("a duplicate key must not be reported late: stderr = %q", errText)
			}

			// Only the window closed before the fatal record appears; line 5
			// never opens a window and line 6 never closes one.
			assertWindowLines(t, stdout.String(), []windowLine{
				{Key: "ok", Start: 0, End: 1000, Count: 1, Sum: 2},
			})
		})
	}
}

// TestAggregateCLIDuplicateKeyEscapedNameIsFatal is the JSON-name form: key
// written directly and key written with an escape for the letter k decode to
// the same field name, so the record is a duplicate regardless of order.
func TestAggregateCLIDuplicateKeyEscapedNameIsFatal(t *testing.T) {
	cases := []string{
		"{\"type\":\"event\",\"key\":\"a\",\"\\u006bey\":\"b\",\"time\":1,\"value\":1}",
		"{\"type\":\"event\",\"\\u006bey\":\"b\",\"key\":\"a\",\"time\":1,\"value\":1}",
	}
	for i, line := range cases {
		cmd := aggregateCommand(t, line+"\n", "--window-ms", "1000")
		var stdout, stderr bytes.Buffer
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		err := cmd.Run()
		if err == nil {
			t.Fatalf("case %d: aggregate succeeded, want exit code 1", i)
		}
		if code := cmd.ProcessState.ExitCode(); code != 1 {
			t.Fatalf("case %d: exit code = %d, want 1", i, code)
		}
		errText := stderr.String()
		for _, want := range []string{"line 1", `duplicate field "key"`} {
			if !strings.Contains(errText, want) {
				t.Fatalf("case %d: stderr = %q, want it to mention %q", i, errText, want)
			}
		}
		if stdout.Len() != 0 {
			t.Fatalf("case %d: the duplicate record produces no output, got %q", i, stdout.String())
		}
	}
}

// TestAggregateCLIDuplicateKeySlidingPartitioned covers the other two
// aggregate shapes: sliding windows and partitioned input both reject the
// duplicate, while a key nested in a nested object and a stray key on a
// watermark/idle record stay ignored.
func TestAggregateCLIDuplicateKeySlidingPartitioned(t *testing.T) {
	t.Run("sliding windows reject it", func(t *testing.T) {
		input := strings.Join([]string{
			`{"type":"event","key":"k","time":700,"value":2}`,
			`{"type":"watermark","time":1600}`, // closes [0,1000) and [600,1600)
			``,
			`{"type":"event","key":"a","key":"b","time":1700,"value":1}`, // line 4: fatal
			`{"type":"watermark","time":3000}`,
		}, "\n") + "\n"

		cmd := aggregateCommand(t, input, "--window-ms", "1000", "--slide-ms", "600")
		var stdout, stderr bytes.Buffer
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		if err := cmd.Run(); err == nil {
			t.Fatal("want exit code 1 for a duplicate key")
		} else if code := cmd.ProcessState.ExitCode(); code != 1 {
			t.Fatalf("exit code = %d, want 1", code)
		}
		if !strings.Contains(stderr.String(), "line 4") || !strings.Contains(stderr.String(), `duplicate field "key"`) {
			t.Fatalf("stderr = %q, want line 4 duplicate field key", stderr.String())
		}
		// The earlier event landed in both windows closed at line 2; the
		// duplicate contributed to neither of the still-open later windows.
		assertWindowLines(t, stdout.String(), []windowLine{
			{Key: "k", Start: 0, End: 1000, Count: 1, Sum: 2},
			{Key: "k", Start: 600, End: 1600, Count: 1, Sum: 2},
		})
	})

	t.Run("partitioned idle partition does not mask it", func(t *testing.T) {
		input := strings.Join([]string{
			`{"type":"event","key":"k","time":100,"value":2,"partition":0}`,
			`{"type":"watermark","time":1000,"partition":0}`,
			`{"type":"idle","partition":0}`,                                           // closes [0,1000), partition idle
			`{"type":"event","key":"a","key":"b","time":500,"value":1,"partition":0}`, // idle AND duplicate
			`{"type":"watermark","time":2000,"partition":0}`,
		}, "\n") + "\n"

		cmd := aggregateCommand(t, input, "--window-ms", "1000", "--partitions", "1")
		var stdout, stderr bytes.Buffer
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		if err := cmd.Run(); err == nil {
			t.Fatal("want exit code 1 for a duplicate key")
		} else if code := cmd.ProcessState.ExitCode(); code != 1 {
			t.Fatalf("exit code = %d, want 1", code)
		}
		errText := stderr.String()
		if !strings.Contains(errText, `duplicate field "key"`) {
			t.Fatalf("stderr = %q, want the duplicate field reason", errText)
		}
		if strings.Contains(errText, "idle partition") {
			t.Fatalf("the duplicate must beat the idle-partition rule: %q", errText)
		}
		if strings.Contains(errText, "late event") {
			t.Fatalf("the duplicate must not be reported late: %q", errText)
		}
		assertWindowLines(t, stdout.String(), []windowLine{
			{Key: "k", Start: 0, End: 1000, Count: 1, Sum: 2},
		})
	})

	t.Run("nested key and stray watermark key are not duplicates", func(t *testing.T) {
		input := strings.Join([]string{
			`{"type":"event","key":"k","time":100,"value":2,"meta":{"key":"x","key":"y"},"partition":0}`,
			`{"type":"watermark","time":1000,"partition":0,"key":"a","key":"b"}`,
		}, "\n") + "\n"

		cmd := aggregateCommand(t, input, "--window-ms", "1000", "--partitions", "1")
		var stdout, stderr bytes.Buffer
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		if err := cmd.Run(); err != nil {
			t.Fatalf("nested/non-event keys stay ignored: %v\nstderr: %s", err, stderr.String())
		}
		if code := cmd.ProcessState.ExitCode(); code != 0 {
			t.Fatalf("exit code = %d, want 0", code)
		}
		if stderr.Len() != 0 {
			t.Fatalf("stderr = %q, want empty", stderr.String())
		}
		assertWindowLines(t, stdout.String(), []windowLine{
			{Key: "k", Start: 0, End: 1000, Count: 1, Sum: 2},
		})
	})
}

// TestAggregateCLIDuplicateKeyMalformedJSONStillFormatError guards the
// boundary: a line that is not one complete JSON object keeps its format
// error rather than being called a duplicate key.
func TestAggregateCLIDuplicateKeyMalformedJSONStillFormatError(t *testing.T) {
	input := `{"type":"event","key":"a","key":}` + "\n"
	cmd := aggregateCommand(t, input, "--window-ms", "1000")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err == nil {
		t.Fatal("want a non-zero exit")
	} else if code := cmd.ProcessState.ExitCode(); code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	errText := stderr.String()
	if !strings.Contains(errText, "invalid JSON") {
		t.Fatalf("stderr = %q, want the invalid JSON format error", errText)
	}
	if strings.Contains(errText, "duplicate") {
		t.Fatalf("malformed JSON must not be reported as a duplicate: %q", errText)
	}
}
