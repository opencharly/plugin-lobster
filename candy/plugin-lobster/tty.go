package pluginlobster

// tty.go — the terminal seam.
//
// The engine's INTERACTIVE path (a human running `charly lobster run` in a terminal)
// and its WIRE path (a plugin invoked over the provider channel, which has no TTY)
// differ only in whether a gate can be answered on the spot. Upstream keys this on
// `process.stdin.isTTY`, and so do we: a character device, not a heuristic.

import (
	"bufio"
	"context"
	"os"
	"strings"
)

// stdinIsTTY reports whether stdin is a terminal. This is the ONE condition that turns
// a gate into an interactive prompt; everything else saves a resume state.
func stdinIsTTY() bool {
	fi, err := os.Stdin.Stat()
	if err != nil {
		return false
	}
	return fi.Mode()&os.ModeCharDevice != 0
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
