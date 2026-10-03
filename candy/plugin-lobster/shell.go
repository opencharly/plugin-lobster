package pluginlobster

// shell.go — running ONE shell step, with upstream lobster's exact shell contract.
//
// This is the engine's own spec'd behaviour, not a generic "run a command" helper, so it
// lives here rather than in a shared kit: upstream resolves the shell itself
// (`src/shell.ts`), and the rules below are transcribed from it.
//
//   - default POSIX: `/bin/sh -lc <command>` — a LOGIN shell (`-l`), not `bash -c`.
//   - `$LOBSTER_SHELL` (trimmed, non-empty) overrides the BINARY; its FLAGS are inferred
//     by suffix: powershell/pwsh → `-NoProfile -Command`; ends-with `cmd`/`cmd.exe`, or
//     any Windows platform → `/d /s /c`; otherwise `-lc`.
//   - Windows without an override: `%ComSpec%` (else `cmd.exe`), `/d /s /c`.
//   - success is exit code 0 ONLY. A non-zero (or signal-killed, code -1) exit is an
//     error whose message is `workflow command failed (<code>): <stderr|stdout|command>`.
//   - a missing shell is its own error: "workflow shell not found; check LOBSTER_SHELL or
//     ComSpec" — the operator's actionable half of an ENOENT.
//   - the child is spawned in its OWN PROCESS GROUP, and a step timeout kills the whole
//     group with SIGKILL (upstream's killSignal thunk), so a step cannot outlive its
//     timeout through a backgrounded descendant.
//   - `stdin == nil` closes the child's stdin with no write and NO trailing newline; a
//     string is written verbatim.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"runtime"
	"strings"
	"syscall"
	"time"
)

// errShellNotFound is the upstream `notFoundMessage`.
var errShellNotFound = errors.New("workflow shell not found; check LOBSTER_SHELL or ComSpec")

// shellRunner runs one shell command. It is an interface so a test can substitute a
// failing/timing-out runner without a real process, and so a future host-executor arm
// (a remote venue) has ONE seam.
type shellRunner interface {
	Run(ctx context.Context, command string, stdin *string, env map[string]string, cwd string, timeout time.Duration) (stdout, stderr string, code int, err error)
}

// localShell runs the command on this host — the engine's only venue today.
type localShell struct{}

func (localShell) Run(ctx context.Context, command string, stdin *string, env map[string]string, cwd string, timeout time.Duration) (string, string, int, error) {
	bin, argv := resolveShell(command, env, runtime.GOOS)

	runCtx := ctx
	var cancel context.CancelFunc
	if timeout > 0 {
		runCtx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	cmd := exec.Command(bin, argv...)
	cmd.Dir = cwd
	cmd.Env = envList(env)
	if stdin != nil {
		cmd.Stdin = strings.NewReader(*stdin)
	}
	var out, errb bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errb
	// Own process group: the timeout path kills the GROUP, never just the leader.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := cmd.Start(); err != nil {
		if errors.Is(err, exec.ErrNotFound) {
			return "", "", -1, errShellNotFound
		}
		return "", "", -1, err
	}

	pgid := cmd.Process.Pid
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()

	select {
	case werr := <-done:
		code := 0
		if werr != nil {
			var ee *exec.ExitError
			if errors.As(werr, &ee) {
				code = ee.ExitCode() // -1 when killed by a signal
			} else {
				return out.String(), errb.String(), -1, werr
			}
		}
		return out.String(), errb.String(), code, nil
	case <-runCtx.Done():
		// SIGKILL the group (negative pid), then reap. Upstream's timeout kill is SIGKILL.
		_ = syscall.Kill(-pgid, syscall.SIGKILL)
		_ = syscall.Kill(pgid, syscall.SIGKILL)
		<-done
		if errors.Is(runCtx.Err(), context.DeadlineExceeded) {
			return out.String(), errb.String(), -1, fmt.Errorf("timed out after %s", timeout)
		}
		return out.String(), errb.String(), -1, runCtx.Err()
	}
}

// resolveShell is upstream's `resolveInlineShellCommand` + `buildShellArgs`.
func resolveShell(command string, env map[string]string, goos string) (string, []string) {
	override := ""
	if v, ok := env["LOBSTER_SHELL"]; ok {
		override = strings.TrimSpace(v)
	}
	isWindows := goos == "windows"

	if override != "" {
		return override, shellArgs(override, command, isWindows)
	}
	if isWindows {
		comspec := strings.TrimSpace(env["ComSpec"])
		if comspec == "" {
			comspec = strings.TrimSpace(env["COMSPEC"])
		}
		if comspec == "" {
			comspec = "cmd.exe"
		}
		return comspec, []string{"/d", "/s", "/c", command}
	}
	// "Keep default behavior deterministic and POSIX-compatible across environments."
	return "/bin/sh", []string{"-lc", command}
}

func shellArgs(shellCommand, command string, isWindows bool) []string {
	lowered := strings.ToLower(shellCommand)
	looksLikeCmd := strings.HasSuffix(lowered, "cmd") || strings.HasSuffix(lowered, "cmd.exe")
	looksLikePowerShell := strings.HasSuffix(lowered, "powershell") ||
		strings.HasSuffix(lowered, "powershell.exe") ||
		strings.HasSuffix(lowered, "pwsh") ||
		strings.HasSuffix(lowered, "pwsh.exe")

	if looksLikePowerShell {
		return []string{"-NoProfile", "-Command", command}
	}
	if looksLikeCmd || isWindows {
		return []string{"/d", "/s", "/c", command}
	}
	return []string{"-lc", command}
}

// envList renders a merged env map into the `KEY=VALUE` slice os/exec wants. Sorted for a
// deterministic child environment (a step that dumps `env` must not differ run to run).
func envList(env map[string]string) []string {
	out := make([]string, 0, len(env))
	for k, v := range env {
		out = append(out, k+"="+v)
	}
	// insertion sort on a small slice keeps the allocation count at one
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j] < out[j-1]; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}
