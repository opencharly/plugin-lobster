package pluginlobster

// redo_test.go — the redo back-edge: the sentinel parse, the back-edge itself, the
// LOOP-GUARD bounds, and the budget's survival across a resume.
//
// Every back-edge case drives the REAL runSteps through the engine's own shell seam and a
// real child process that writes the sentinel to its own stderr — the exact channel a plan
// verb's `os.Stderr` reaches (the `charly task` process inherits the writer and the shell
// step captures it). Nothing here mocks the exec boundary: a fake would certify the
// imagined contract, not what a process actually does.

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRedoTriggerFromStderr(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"absent", "ordinary failure output\nnothing here\n", ""},
		{"plain", "LOOP-GUARD-TRIGGER: alpha\n", "alpha"},
		{"last wins", "LOOP-GUARD-TRIGGER: first\nnoise\nLOOP-GUARD-TRIGGER: second\n", "second"},
		{"whitespace tolerant", "   LOOP-GUARD-TRIGGER:   spaced   \n", "spaced"},
		{"empty name ignored", "LOOP-GUARD-TRIGGER:\nLOOP-GUARD-TRIGGER: real\n", "real"},
		{"empty name only", "LOOP-GUARD-TRIGGER:   \n", ""},
		{"mid line not a sentinel", "x LOOP-GUARD-TRIGGER: nope\n", ""},
		{"no trailing newline", "LOOP-GUARD-TRIGGER: tail", "tail"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := redoTriggerFromStderr(tc.in); got != tc.want {
				t.Fatalf("redoTriggerFromStderr(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestRedoBackEdgeReRunsTarget(t *testing.T) {
	eng, _, _ := testEngine(t, nil)
	marker := filepath.Join(t.TempDir(), "seedcount")
	path := writeWorkflow(t, `
steps:
  - id: seed
    run: 'n=$(cat `+marker+` 2>/dev/null || echo 0); n=$((n+1)); echo $n > `+marker+`'
  - id: flaky
    redo: {triggers: {needs-seed: seed}}
    run: 'n=$(cat `+marker+` 2>/dev/null || echo 0); if [ "$n" -lt 2 ]; then echo "LOOP-GUARD-TRIGGER: needs-seed" >&2; exit 1; fi; echo done'
`)
	res, err := eng.runFile(context.Background(), path, nil, nil)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if res.Status != "ok" {
		t.Fatalf("status = %q, want ok", res.Status)
	}
	if len(res.Output) != 1 || res.Output[0] != "done" {
		t.Fatalf("output = %#v, want [done]", res.Output)
	}
	// `seed` ran a SECOND time after the back-edge: the counter reaching 2 is the proof the
	// target was actually re-entered, not merely that the loop continued.
	body, err := os.ReadFile(marker)
	if err != nil {
		t.Fatalf("read marker: %v", err)
	}
	if strings.TrimSpace(string(body)) != "2" {
		t.Fatalf("seed runs = %q, want 2 (the back-edge must re-run the target)", strings.TrimSpace(string(body)))
	}
}

func TestRedoNoSentinelFailsHard(t *testing.T) {
	eng, _, _ := testEngine(t, nil)
	// The redo spec IS declared, but the failing step emits no sentinel: without a trigger
	// there is no back-edge, and the run ends — the trigger must cross the runtime channel,
	// it is not inferred from the spec.
	path := writeWorkflow(t, `
steps:
  - id: seed
    run: 'true'
  - id: flaky
    redo: {triggers: {needs-seed: seed}}
    run: 'echo boom >&2; exit 1'
`)
	_, err := eng.runFile(context.Background(), path, nil, nil)
	if err == nil {
		t.Fatal("expected a fail-hard error")
	}
	if !strings.Contains(err.Error(), "workflow command failed") {
		t.Fatalf("error = %q, want the shell failure", err)
	}
	if strings.Contains(err.Error(), "LOOP-GUARD") {
		t.Fatalf("error = %q, want no back-edge without a sentinel", err)
	}
}

func TestRedoEscalateGuard(t *testing.T) {
	eng, _, _ := testEngine(t, nil)
	path := writeWorkflow(t, `
steps:
  - id: seed
    run: 'true'
  - id: loop
    redo: {escalate_after: 2, triggers: {again: seed}}
    run: 'echo "LOOP-GUARD-TRIGGER: again" >&2; exit 1'
`)
	_, err := eng.runFile(context.Background(), path, nil, nil)
	if err == nil {
		t.Fatal("expected the escalate LOOP-GUARD")
	}
	if !strings.Contains(err.Error(), "LOOP-GUARD: seed re-entered 2 times (escalate)") {
		t.Fatalf("error = %q, want the escalate LOOP-GUARD text", err)
	}
}

func TestRedoMaxGuard(t *testing.T) {
	eng, _, _ := testEngine(t, nil)
	path := writeWorkflow(t, `
steps:
  - id: seed
    run: 'true'
  - id: loop
    redo: {max: 1, triggers: {again: seed}}
    run: 'echo "LOOP-GUARD-TRIGGER: again" >&2; exit 1'
`)
	_, err := eng.runFile(context.Background(), path, nil, nil)
	if err == nil {
		t.Fatal("expected the max LOOP-GUARD")
	}
	if !strings.Contains(err.Error(), "LOOP-GUARD: exceed redo max 1 for seed") {
		t.Fatalf("error = %q, want the max LOOP-GUARD text", err)
	}
}

// TestRedoPerStepMaxOverride is the regression test for the float64 defect class: a redo
// bound read out of an untyped map[string]any is a float64, and a bare `.(int)` assertion on
// it fails so the bound silently falls back to the default (2). Reading the GENERATED STRUCT
// field (int64) is what makes `max: 4` actually mean 4 — so the re-entry count must be 4, not 2.
func TestRedoPerStepMaxOverride(t *testing.T) {
	eng, _, _ := testEngine(t, nil)
	marker := filepath.Join(t.TempDir(), "seedcount")
	path := writeWorkflow(t, `
steps:
  - id: seed
    run: 'n=$(cat `+marker+` 2>/dev/null || echo 0); n=$((n+1)); echo $n > `+marker+`'
  - id: loop
    redo: {max: 4, escalate_after: 10, triggers: {again: seed}}
    run: 'echo "LOOP-GUARD-TRIGGER: again" >&2; exit 1'
`)
	_, err := eng.runFile(context.Background(), path, nil, nil)
	if err == nil {
		t.Fatal("expected the max LOOP-GUARD")
	}
	if !strings.Contains(err.Error(), "LOOP-GUARD: exceed redo max 4 for seed") {
		t.Fatalf("error = %q, want max 4 (a default-2 fallback is the float64 defect)", err)
	}
	body, rerr := os.ReadFile(marker)
	if rerr != nil {
		t.Fatalf("read marker: %v", rerr)
	}
	// 1 initial run + 4 re-entries = 5 executions of the target.
	if strings.TrimSpace(string(body)) != "5" {
		t.Fatalf("seed runs = %q, want 5 (1 initial + 4 re-entries)", strings.TrimSpace(string(body)))
	}
}

func TestRedoUnresolvableTarget(t *testing.T) {
	eng, _, _ := testEngine(t, nil)
	path := writeWorkflow(t, `
steps:
  - id: seed
    run: 'true'
  - id: loop
    redo: {triggers: {ghost: nowhere}}
    run: 'echo "LOOP-GUARD-TRIGGER: ghost" >&2; exit 1'
`)
	_, err := eng.runFile(context.Background(), path, nil, nil)
	if err == nil {
		t.Fatal("expected the unresolvable-target error")
	}
	if !strings.Contains(err.Error(), `redo trigger "ghost" maps to no step ("nowhere")`) {
		t.Fatalf("error = %q, want the named spec-defect message (never a silent swallow)", err)
	}
}

func TestRedoAbsentIsUnchanged(t *testing.T) {
	eng, _, _ := testEngine(t, nil)
	// No redo spec: a plain failure still ends the run.
	plain := writeWorkflow(t, `
steps:
  - id: boom
    run: 'exit 5'
`)
	if _, err := eng.runFile(context.Background(), plain, nil, nil); err == nil {
		t.Fatal("a failing step with no redo spec must still fail hard")
	} else if !strings.Contains(err.Error(), "workflow command failed") {
		t.Fatalf("error = %q, want the shell failure", err)
	}

	// A sentinel on a step that declares NO redo spec must NOT create a back-edge: the
	// on_error policy still governs, exactly as before this feature existed.
	withSentinel := writeWorkflow(t, `
steps:
  - id: boom
    on_error: continue
    run: 'echo "LOOP-GUARD-TRIGGER: x" >&2; exit 5'
  - id: after
    run: 'echo continued'
`)
	res, err := eng.runFile(context.Background(), withSentinel, nil, nil)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if res.Status != "ok" || len(res.Output) != 1 || res.Output[0] != "continued" {
		t.Fatalf("result = %#v, want on_error:continue to still govern (no back-edge)", res)
	}
}

func TestRedoCountSurvivesResume(t *testing.T) {
	eng, _, _ := testEngine(t, nil)

	// The store round-trip: a saved budget reloads intact.
	key, err := eng.store.save(context.Background(), &resumeState{
		FilePath:      "x",
		ResumeAtIndex: 1,
		RedoCount:     map[string]int{"seed": 2},
	})
	if err != nil {
		t.Fatalf("save: %v", err)
	}
	loaded, err := eng.store.load(key)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if loaded.RedoCount["seed"] != 2 {
		t.Fatalf("redoCount = %#v, want {seed:2} to survive the round trip", loaded.RedoCount)
	}

	// A state file that PREDATES the field must load as an empty budget, never an error.
	legacy := `{"filePath":"x","resumeAtIndex":1,"steps":{},"args":{},"createdAt":"2020-01-01T00:00:00Z"}`
	if err := os.WriteFile(filepath.Join(eng.store.dir, "workflow_resume_legacy.json"), []byte(legacy), 0o600); err != nil {
		t.Fatalf("write legacy state: %v", err)
	}
	legacyState, err := eng.store.load("workflow_resume_legacy")
	if err != nil {
		t.Fatalf("load legacy: %v", err)
	}
	if len(legacyState.RedoCount) != 0 {
		t.Fatalf("legacy redoCount = %#v, want empty", legacyState.RedoCount)
	}

	// The ENGINE proof: a resume carrying an already-spent budget (count 1, max 1) trips the
	// MAX guard on its FIRST re-entry, so the target step never re-runs. Were the budget reset
	// at resume the guard would not trip, the back-edge would fire, and the marker below would
	// exist. Its ABSENCE is therefore the evidence that the persisted count was restored.
	marker := filepath.Join(t.TempDir(), "seedran")
	path := writeWorkflow(t, `
steps:
  - id: seed
    run: 'touch `+marker+`'
  - id: loop
    redo: {max: 1, escalate_after: 10, triggers: {again: seed}}
    run: 'echo "LOOP-GUARD-TRIGGER: again" >&2; exit 1'
`)
	resume := &resumeState{
		FilePath:      path,
		ResumeAtIndex: 1,
		RedoCount:     map[string]int{"seed": 1},
	}
	_, err = eng.runFile(context.Background(), path, nil, resume)
	if err == nil {
		t.Fatal("expected the max LOOP-GUARD from the restored budget")
	}
	if !strings.Contains(err.Error(), "LOOP-GUARD: exceed redo max 1 for seed") {
		t.Fatalf("error = %q, want the restored budget to trip max on the first re-entry", err)
	}
	if _, serr := os.Stat(marker); serr == nil {
		t.Fatal("the target re-ran: the redo budget was reset by the resume instead of restored")
	}
}
