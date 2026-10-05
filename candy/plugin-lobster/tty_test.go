package pluginlobster

// tty_test.go — the interactive-gate predicate.
//
// The gate's whole safety property is "prompt ONLY a real terminal": a gate that takes a
// non-terminal for a keyboard prompts nobody, reads EOF, and answers the run with an
// error INSTEAD of recording the resume handle a later `resume` needs. The tempting
// port of Node's `process.stdin.isTTY` is `fi.Mode()&os.ModeCharDevice != 0`, and
// /dev/null — which is exactly what a check runner hands a step — IS a character device,
// so that spelling gets this case wrong.

import (
	"os"
	"testing"
)

func TestIsTerminalRejectsDevNull(t *testing.T) {
	f, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatalf("open %s: %v", os.DevNull, err)
	}
	defer f.Close() //nolint:errcheck

	if isTerminal(f) {
		t.Fatalf("/dev/null was taken for a terminal — a gate would prompt a keyboard that is not there and lose the resume handle")
	}
}

func TestIsTerminalRejectsAPipe(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	defer r.Close() //nolint:errcheck
	defer w.Close() //nolint:errcheck

	if isTerminal(r) {
		t.Fatal("a pipe read end was taken for a terminal")
	}
}

func TestIsTerminalRejectsNil(t *testing.T) {
	if isTerminal(nil) {
		t.Fatal("a nil file was taken for a terminal")
	}
}
