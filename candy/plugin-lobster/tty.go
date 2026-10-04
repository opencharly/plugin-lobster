package pluginlobster

// tty.go — the terminal seam.
//
// The engine's INTERACTIVE path (a human running `charly lobster run` in a terminal)
// and its WIRE path (a plugin invoked over the provider channel, which has no TTY)
// differ only in whether a gate can be answered on the spot. Upstream keys this on
// `process.stdin.isTTY`, which is Node's isatty(0) — "is fd 0 attached to a terminal" —
// and so do we.

import (
	"bufio"
	"context"
	"os"
	"strings"

	"golang.org/x/term"
)

// stdinIsTTY reports whether stdin is a terminal. This is the ONE condition that turns
// a gate into an interactive prompt; everything else saves a resume state.
//
// The test is isatty(0), NOT "is fd 0 a character device". The character-device test is
// the tempting port of Node's `isTTY` and it is WRONG here: /dev/null is a character
// device, so a check runner (or any harness) that hands a step /dev/null would be taken
// for a human at a keyboard, the gate would prompt a terminal that is not there, read
// EOF and answer the run with an error — and the resume handle for the gate would never
// be written. A gate must prompt ONLY a real terminal.
func stdinIsTTY() bool {
	return isTerminal(os.Stdin)
}

// isTerminal is the predicate itself, on a named file, so a test can hold it to the
// cases that matter (/dev/null above all) without juggling the process's fd 0.
func isTerminal(f *os.File) bool {
	return f != nil && term.IsTerminal(int(f.Fd()))
}

// readStdinLine reads one line from stdin. It is used only on the interactive path,
// which is only reached when stdin is a terminal.
//
// The read is bounded by the context: a cancelled run must not leave a prompt hanging.
func readStdinLine(ctx context.Context) (string, error) {
	type line struct {
		s   string
		err error
	}
	ch := make(chan line, 1)
	go func() {
		r := bufio.NewReader(os.Stdin)
		s, err := r.ReadString('\n')
		ch <- line{s: s, err: err}
	}()
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case l := <-ch:
		if l.err != nil && strings.TrimSpace(l.s) == "" {
			return "", l.err
		}
		return strings.TrimRight(l.s, "\r\n"), nil
	}
}
