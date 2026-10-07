package main

import (
	"bytes"
	"strings"
	"testing"
)

// Command-line end-to-end coverage for the new event intake rule: an event
// record may carry the top-level field "key" at most once. A repeated key
// drives the real aggregate child process to exit code 1 with the physical
// input line, the field and the duplicate reason on standard error; standard
// output keeps only window results already fully written; the offending
// event contributes no count or sum; and later records are never read. The
// field name is compared after JSON decoding, so a \uXXXX spelling of the
// same name repeats it; a "key" nested in an attached object and a stray key
// on a watermark record stay ignored extra fields.

// wireUnicode builds a JSON backslash-u escape at run time (wireUnicode of
// "006b" is the six JSON bytes spelling lowercase k), keeping this source
// free of literal escape text tooling might rewrite.
func wireUnicode(hex4 string) string {
	return string(rune(0x5c)) + "u" + hex4
}

// TestAggregateCLIDuplicateEventKeyIsFatal is the headline case:
//
//	line 1: a legal event closes nothing on its own
//	line 2: a watermark closes [0,1000) -> one stdout result
//	line 3: blank, still occupies a physical line number
//	line 4: the event repeats "key" (second spelling uses the U+006B escape)
//	        AND is below the current watermark -- duplicate must win
//	line 5: a later watermark that must never be read
func TestAggregateCLIDuplicateEventKeyIsFatal(t *testing.T) {
	dupLine := `{"type":"event","key":"a","` + wireUnicode("006b") + `ey":"b","time":999,"value":9}`
	input := strings.Join([]string{
		`{"type":"event","key":"ok","time":100,"value":2}`, // line 1
		`{"type":"watermark","time":1000}`,                 // line 2: closes [0,1000)
		``,                                                 // line 3: blank, still counted
		dupLine,                                            // line 4: duplicate + late
		`{"type":"watermark","time":2000}`,                 // line 5: never read
	}, "\n") + "\n"

	cmd := aggregateCommand(t, input, "--window-ms", "1000")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err == nil {
		t.Fatal("aggregate succeeded, want exit code 1 for a repeated event key")
	}
	if code := cmd.ProcessState.ExitCode(); code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}

	errText := stderr.String()
	for _, want := range []string{
		"line 4",                // physical input line, blank line 3 counted
		`field "key"`,           // the offending field
		"more than once",        // the duplicate reason
		"top level of an event", // scoped to event top level
	} {
		if !strings.Contains(errText, want) {
			t.Fatalf("stderr = %q, want it to contain %q", errText, want)
		}
	}
	// The duplicate must not be downgraded to a late-event skip.
	if strings.Contains(errText, "late event") {
		t.Fatalf("a duplicate key must not be reported as a late event: stderr = %q", errText)
	}
	if strings.Contains(stdout.String(), "aggregate:") {
		t.Fatalf("the explanation must stay on standard error: stdout = %q", stdout.String())
	}

	// Only the window fully written on line 2 survives; the line-4 event
	// added nothing and line 5 was never processed, so no second window.
	assertWindowLines(t, stdout.String(), []windowLine{
		{Key: "ok", Start: 0, End: 1000, Count: 1, Sum: 2},
	})
}

// TestAggregateCLIDuplicateEventKeyIdenticalValues still fails: repeating the
// field with the exact same string is a duplicate, not a harmless rewrite.
func TestAggregateCLIDuplicateEventKeyIdenticalValues(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event","key":"same","key":"same","time":100,"value":1}`,
		`{"type":"watermark","time":1000}`,
	}, "\n") + "\n"

	cmd := aggregateCommand(t, input, "--window-ms", "1000")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err == nil {
		t.Fatal("aggregate succeeded, want exit code 1 even when both key values are identical")
	}
	if code := cmd.ProcessState.ExitCode(); code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "line 1") || !strings.Contains(stderr.String(), "more than once") {
		t.Fatalf("stderr = %q, want line 1 and the duplicate reason", stderr.String())
	}
	if stdout.Len() != 0 {
		t.Fatalf("the rejected event must contribute nothing, stdout = %q", stdout.String())
	}
}

// TestAggregateCLINestedAndStrayKeysAreNotDuplicates fixes the scope: only
// the event's own top level counts, and only on event records. Each input
// exits 0 with empty standard error.
func TestAggregateCLINestedAndStrayKeysAreNotDuplicates(t *testing.T) {
	t.Run("key nested in an attached object is legal", func(t *testing.T) {
		input := strings.Join([]string{
			`{"type":"event","key":"a","time":100,"value":2,"extra":{"key":"b"}}`,
			`{"type":"watermark","time":1000}`,
		}, "\n") + "\n"
		cmd := aggregateCommand(t, input, "--window-ms", "1000")
		var stdout, stderr bytes.Buffer
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		if err := cmd.Run(); err != nil {
			t.Fatalf("a nested key is not a duplicate: %v\nstderr: %s", err, stderr.String())
		}
		if code := cmd.ProcessState.ExitCode(); code != 0 {
			t.Fatalf("exit code = %d, want 0", code)
		}
		if stderr.Len() != 0 {
			t.Fatalf("stderr = %q, want empty", stderr.String())
		}
		assertWindowLines(t, stdout.String(), []windowLine{
			{Key: "a", Start: 0, End: 1000, Count: 1, Sum: 2},
		})
	})

	t.Run("repeated stray key on a watermark stays ignored", func(t *testing.T) {
		input := strings.Join([]string{
			`{"type":"event","key":"a","time":100,"value":2}`,
			`{"type":"watermark","time":1000,"key":"x","key":"y"}`,
		}, "\n") + "\n"
		cmd := aggregateCommand(t, input, "--window-ms", "1000")
		var stdout, stderr bytes.Buffer
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		if err := cmd.Run(); err != nil {
			t.Fatalf("extra fields on a watermark stay ignored: %v\nstderr: %s", err, stderr.String())
		}
		if code := cmd.ProcessState.ExitCode(); code != 0 {
			t.Fatalf("exit code = %d, want 0", code)
		}
		if stderr.Len() != 0 {
			t.Fatalf("stderr = %q, want empty", stderr.String())
		}
		assertWindowLines(t, stdout.String(), []windowLine{
			{Key: "a", Start: 0, End: 1000, Count: 1, Sum: 2},
		})
	})

	t.Run("duplicate key on a partitioned event with sliding windows", func(t *testing.T) {
		input := strings.Join([]string{
			`{"type":"event","key":"k","time":700,"value":2,"partition":0}`,
			`{"type":"event","key":"k","key":"z","time":800,"value":3,"partition":1}`,
		}, "\n") + "\n"
		cmd := aggregateCommand(t, input, "--window-ms", "1000", "--slide-ms", "600", "--partitions", "2")
		var stdout, stderr bytes.Buffer
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		if err := cmd.Run(); err == nil {
			t.Fatal("aggregate succeeded, want exit code 1 for the partitioned duplicate key")
		}
		if code := cmd.ProcessState.ExitCode(); code != 1 {
			t.Fatalf("exit code = %d, want 1", code)
		}
		if !strings.Contains(stderr.String(), "line 2") || !strings.Contains(stderr.String(), "more than once") {
			t.Fatalf("stderr = %q, want line 2 and the duplicate reason", stderr.String())
		}
		if stdout.Len() != 0 {
			t.Fatalf("no watermark was given and the bad event counts for nothing: stdout = %q", stdout.String())
		}
	})
}
