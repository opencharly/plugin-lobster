package pluginlobster

// cli_doctor_test.go — `charly lobster doctor`.
//
// doctor is an operator's first look at why a workflow step cannot run, so its contract
// is: report the resolved surface and ALWAYS exit 0. These tests drive it purely through
// the environment — `CHARLY_BIN` (what a charly step will exec) and `LOBSTER_STATE_DIR`
// (where a gate persists) — and pin the two branches of each, because those are the two
// answers an operator acts on.
//
// What is deliberately NOT asserted is the systemd line: it comes from a real
// `systemctl --user` query, so its value is a property of the host, not of this code.
// doctor already reports every outcome of that query without failing (absent / present /
// state-unavailable), and pinning one of them here would pin the test host instead.
// Nothing is stubbed to make it deterministic — the query is real, its result is simply
// not the contract.

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// captureStdout runs fn with os.Stdout redirected and returns what it printed.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stdout = w
	done := make(chan string, 1)
	go func() {
		var buf bytes.Buffer
		_, _ = io.Copy(&buf, r)
		done <- buf.String()
	}()
	fn()
	_ = w.Close()
	os.Stdout = old
	out := <-done
	_ = r.Close()
	return out
}

// TestCLIDoctorWarnsWhenCharlyIsNotResolvable drives the branch an operator is most
// likely to hit on a fresh host: CHARLY_BIN names something that is not on PATH, so every
// charly step in every workflow would fail at exec time. doctor must SAY so and still
// exit 0 — a diagnostic tool that exits non-zero on a diagnosis is a trap in a script.
func TestCLIDoctorWarnsWhenCharlyIsNotResolvable(t *testing.T) {
	const missing = "charly-definitely-not-on-path-xyz"
	t.Setenv("CHARLY_BIN", missing)

	stateDir := t.TempDir()
	t.Setenv("LOBSTER_STATE_DIR", stateDir)

	var rc int
	var err error
	out := captureStdout(t, func() { rc, err = cliDoctor(nil) })

	if err != nil {
		t.Fatalf("cliDoctor() err = %v; want nil (doctor reports, it does not fail)", err)
	}
	if rc != 0 {
		t.Fatalf("cliDoctor() rc = %d; want 0", rc)
	}
	if !strings.Contains(out, "charly binary:") || !strings.Contains(out, missing) {
		t.Errorf("output does not name the resolved charly binary %q:\n%s", missing, out)
	}
	if !strings.Contains(out, "WARNING") {
		t.Errorf("output carries no WARNING for an unresolvable CHARLY_BIN %q:\n%s", missing, out)
	}
	// A state dir that exists must read as present, not as "not created yet".
	if !strings.Contains(out, "state dir:") || !strings.Contains(out, stateDir) {
		t.Errorf("output does not name the state dir %q:\n%s", stateDir, out)
	}
	if !strings.Contains(out, "present") {
		t.Errorf("an EXISTING state dir (%s) was not reported present:\n%s", stateDir, out)
	}
	if strings.Contains(out, "not created yet") {
		t.Errorf("an existing state dir was reported as not created:\n%s", out)
	}
}

// TestCLIDoctorReportsTheEngineSurface is the other half of the contract: the lines that
// describe THIS build rather than the host. `pipeline stages:` must list exactly the
// stages the registry implements — that is the line telling an operator whether the
// `pipeline:` step in their workflow is supported — and the emit formats must be the ones
// this engine declared.
func TestCLIDoctorReportsTheEngineSurface(t *testing.T) {
	t.Setenv("LOBSTER_STATE_DIR", filepath.Join(t.TempDir(), "never-created"))

	var rc int
	var err error
	out := captureStdout(t, func() { rc, err = cliDoctor(nil) })

	if err != nil {
		t.Fatalf("cliDoctor() err = %v; want nil", err)
	}
	if rc != 0 {
		t.Fatalf("cliDoctor() rc = %d; want 0", rc)
	}

	// A state dir that does NOT exist must say so rather than claim one.
	if !strings.Contains(out, "not created yet") {
		t.Errorf("a missing state dir was not reported as not created yet:\n%s", out)
	}

	stages := newRegistry().names()
	if len(stages) == 0 {
		t.Fatal("newRegistry().names() is empty; the registry cannot be empty")
	}
	line := ""
	for _, l := range strings.Split(out, "\n") {
		if strings.HasPrefix(l, "pipeline stages:") {
			line = l
			break
		}
	}
	if line == "" {
		t.Fatalf("output has no `pipeline stages:` line:\n%s", out)
	}
	reported := strings.Fields(strings.TrimPrefix(line, "pipeline stages:"))
	if strings.Join(reported, " ") != strings.Join(stages, " ") {
		t.Errorf("doctor reports stages %v; the registry implements %v", reported, stages)
	}

	for _, want := range []string{"emit formats:", "lobster, charly-yml", "shell:", "user unit dir:"} {
		if !strings.Contains(out, want) {
			t.Errorf("output is missing %q:\n%s", want, out)
		}
	}
}
