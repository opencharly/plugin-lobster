package pluginlobster

// engine_test.go — the engine's behaviour, exercised through the REAL shell and the REAL
// state store (redirected to a temp dir). Nothing here mocks the shell: the point of the
// engine is what it does to a real process, so a fake would certify the wrong thing.

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/opencharly/plugin-lobster/candy/plugin-lobster/params"
	"github.com/opencharly/spec/spec"
)

// testEngine builds an engine whose state lives in the test's own temp dir, so a test can
// never read or clobber a developer's real resume state.
func testEngine(t *testing.T, env map[string]string) (*engine, *bytes.Buffer, *bytes.Buffer) {
	t.Helper()
	base := map[string]string{
		"LOBSTER_STATE_DIR": t.TempDir(),
		"PATH":              os.Getenv("PATH"),
		"HOME":              os.Getenv("HOME"),
	}
	for k, v := range env {
		base[k] = v
	}
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	eng := newEngine(engineOptions{
		charlyBin: "charly",
		stdout:    stdout,
		stderr:    stderr,
		env:       base,
		cwd:       t.TempDir(),
	})
	eng.now = func() time.Time { return time.Unix(1700000000, 0).UTC() }
	return eng, stdout, stderr
}

func writeWorkflow(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "workflow.lobster")
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("write workflow: %v", err)
	}
	return path
}

// boolPtr takes the address of a bool literal — the test-side spelling of the
// wire tri-state: boolPtr(true) is an approval, boolPtr(false) a REJECTION, and
// leaving the field nil is the absence of any answer.
func boolPtr(v bool) *bool { return &v }

func TestRunShellStepAndRefs(t *testing.T) {
	eng, _, _ := testEngine(t, nil)
	path := writeWorkflow(t, `
steps:
  - id: produce
    run: 'echo "{\"name\":\"ada\",\"count\":3}"'
  - id: consume
    run: 'printf %s "$produce.json.name"'
  - id: skipme
    when: '$produce.json.count > 10'
    run: 'echo should-not-run'
`)
	res, err := eng.runFile(context.Background(), path, nil, nil)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if res.Status != "ok" {
		t.Fatalf("status = %q, want ok", res.Status)
	}
	// `skipme` is recorded as skipped so later refs can see it, but a skipped step is NOT
	// the run's last completed step — `consume` is, so its output is the run's output. That
	// the output is `ada` and not `should-not-run` is the proof the `when` gate held.
	if len(res.Output) != 1 || res.Output[0] != "ada" {
		t.Fatalf("output = %#v, want [ada] (the skipped step must not become the run output)", res.Output)
	}
}

func TestStepRefResolvesJSONPath(t *testing.T) {
	eng, _, _ := testEngine(t, nil)
	path := writeWorkflow(t, `
steps:
  - id: produce
    run: 'echo "{\"name\":\"ada\"}"'
  - id: consume
    run: 'printf "%s" "$produce.json.name"'
`)
	res, err := eng.runFile(context.Background(), path, nil, nil)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(res.Output) != 1 || res.Output[0] != "ada" {
		t.Fatalf("output = %#v, want [ada]", res.Output)
	}
}

func TestArgsTemplateAndEnv(t *testing.T) {
	eng, _, _ := testEngine(t, nil)
	path := writeWorkflow(t, `
args:
  who: {default: "world"}
steps:
  - id: greet
    run: 'printf "%s|%s" "${who}" "$LOBSTER_ARG_WHO"'
`)
	res, err := eng.runFile(context.Background(), path, map[string]any{"who": "atrawog"}, nil)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(res.Output) != 1 || res.Output[0] != "atrawog|atrawog" {
		t.Fatalf("output = %#v, want [atrawog|atrawog]", res.Output)
	}
}

func TestOnErrorContinueAndSkipRest(t *testing.T) {
	eng, _, _ := testEngine(t, nil)
	path := writeWorkflow(t, `
steps:
  - id: boom
    on_error: continue
    run: 'exit 3'
  - id: after
    run: 'echo continued'
`)
	res, err := eng.runFile(context.Background(), path, nil, nil)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(res.Output) != 1 || res.Output[0] != "continued" {
		t.Fatalf("output = %#v, want [continued]", res.Output)
	}

	path2 := writeWorkflow(t, `
steps:
  - id: boom
    on_error: skip_rest
    run: 'exit 3'
  - id: after
    run: 'echo should-not-run'
`)
	res2, err := eng.runFile(context.Background(), path2, nil, nil)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(res2.Output) != 0 {
		t.Fatalf("output = %#v, want empty (skip_rest stopped the run)", res2.Output)
	}
}

func TestRetrySucceedsOnThirdAttempt(t *testing.T) {
	eng, _, stderr := testEngine(t, nil)
	marker := filepath.Join(t.TempDir(), "attempts")
	path := writeWorkflow(t, `
steps:
  - id: flaky
    retry: {max: 3, delay_ms: 1}
    run: 'n=$(cat `+marker+` 2>/dev/null || echo 0); n=$((n+1)); echo $n > `+marker+`; [ $n -ge 3 ]'
`)
	res, err := eng.runFile(context.Background(), path, nil, nil)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if res.Status != "ok" {
		t.Fatalf("status = %q, want ok", res.Status)
	}
	if !strings.Contains(stderr.String(), "[RETRY]") {
		t.Fatalf("stderr = %q, want a RETRY line", stderr.String())
	}
	body, _ := os.ReadFile(marker)
	if strings.TrimSpace(string(body)) != "3" {
		t.Fatalf("attempts = %q, want 3", strings.TrimSpace(string(body)))
	}
}

func TestTimeoutKillsTheStep(t *testing.T) {
	eng, _, _ := testEngine(t, nil)
	path := writeWorkflow(t, `
steps:
  - id: slow
    timeout_ms: 200
    run: 'sleep 30'
`)
	start := time.Now()
	_, err := eng.runFile(context.Background(), path, nil, nil)
	if err == nil {
		t.Fatal("expected a timeout error")
	}
	if time.Since(start) > 10*time.Second {
		t.Fatalf("timeout did not fire promptly: %s", err)
	}
	if !strings.Contains(err.Error(), "slow") {
		t.Fatalf("error = %q, want it to name the step", err)
	}
}

func TestForEachProducesOrderedArray(t *testing.T) {
	eng, _, _ := testEngine(t, nil)
	path := writeWorkflow(t, `
steps:
  - id: seed
    run: 'echo "[1,2,3]"'
  - id: loop
    for_each: "$seed.json"
    batch_size: 2
    item_var: entry
    index_var: pos
    steps:
      - id: double
        run: 'echo $(( $entry.json * 2 ))'
  - id: prove
    when: '$loop.json.0 == 2 && $loop.json.2 == 6'
    run: 'echo ordered'
`)
	res, err := eng.runFile(context.Background(), path, nil, nil)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	// The loop's JSON array is the step's value; the run's output spreads it (a JSON array
	// yields its elements). The `prove` step only runs if the array really is ordered
	// [2,4,6] indexed through `$loop.json.N`, so `ordered` IS the ordering proof.
	if len(res.Output) != 1 || res.Output[0] != "ordered" {
		t.Fatalf("output = %#v, want [ordered]", res.Output)
	}
}

func TestForEachOutputArrayShape(t *testing.T) {
	eng, _, _ := testEngine(t, nil)
	path := writeWorkflow(t, `
steps:
  - id: seed
    run: 'echo "[10,20]"'
  - id: loop
    for_each: "$seed.json"
    steps:
      - id: echoitem
        run: 'printf "%s" "$item.json"'
`)
	res, err := eng.runFile(context.Background(), path, nil, nil)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(res.Output) != 2 || res.Output[0] != float64(10) || res.Output[1] != float64(20) {
		t.Fatalf("output = %#v, want [10 20] in order", res.Output)
	}
}

func TestParallelWaitAllPromotesBranchRefs(t *testing.T) {
	eng, _, _ := testEngine(t, nil)
	path := writeWorkflow(t, `
steps:
  - id: fan
    parallel:
      wait: all
      branches:
        - id: a
          run: 'echo "{\"v\":\"A\"}"'
        - id: b
          run: 'echo "{\"v\":\"B\"}"'
  - id: join
    run: 'printf "%s%s" "$a.json.v" "$b.json.v"'
`)
	res, err := eng.runFile(context.Background(), path, nil, nil)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(res.Output) != 1 || res.Output[0] != "AB" {
		t.Fatalf("output = %#v, want [AB]", res.Output)
	}
}

func TestApprovalGateResumeApproveAndDecline(t *testing.T) {
	eng, _, _ := testEngine(t, nil)
	path := writeWorkflow(t, `
steps:
  - id: prepare
    run: 'echo "ready"'
  - id: sign
    approval: "Ship it?"
    run: 'echo shipped'
  - id: finish
    run: 'echo done'
`)

	res, err := eng.runFile(context.Background(), path, nil, nil)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if res.Status != "needs_approval" {
		t.Fatalf("status = %q, want needs_approval", res.Status)
	}
	if res.RequiresApproval == nil || res.RequiresApproval.ResumeToken == "" {
		t.Fatal("needs_approval must carry a resume token")
	}
	if res.RequiresApproval.Prompt != "Ship it?" {
		t.Fatalf("prompt = %q", res.RequiresApproval.Prompt)
	}
	token := res.RequiresApproval.ResumeToken

	payload, err := decodeToken(token)
	if err != nil {
		t.Fatalf("token does not decode: %v", err)
	}
	st, err := eng.store.load(payload.StateKey)
	if err != nil {
		t.Fatalf("state not found: %v", err)
	}

	// Approve: the run continues from the step AFTER the gate.
	approved := *st
	approved.StateKey = payload.StateKey
	approved.Approved = boolPtr(true)
	done, err := eng.runFile(context.Background(), path, nil, &approved)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if done.Status != "ok" {
		t.Fatalf("resumed status = %q, want ok", done.Status)
	}
	if len(done.Output) != 1 || done.Output[0] != "done" {
		t.Fatalf("resumed output = %#v, want [done]", done.Output)
	}

	// Decline on a fresh token is a CANCELLATION, not a failure.
	res2, err := eng.runFile(context.Background(), path, nil, nil)
	if err != nil {
		t.Fatalf("second run: %v", err)
	}
	p2, _ := decodeToken(res2.RequiresApproval.ResumeToken)
	st2, err := eng.store.load(p2.StateKey)
	if err != nil {
		t.Fatalf("second state: %v", err)
	}
	declined := *st2
	declined.StateKey = p2.StateKey
	declined.Approved = boolPtr(false)
	cancelled, err := eng.runFile(context.Background(), path, nil, &declined)
	if err != nil {
		t.Fatalf("decline: %v", err)
	}
	if cancelled.Status != "cancelled" {
		t.Fatalf("declined status = %q, want cancelled", cancelled.Status)
	}
}

// TestApprovalGateResumeWithoutADecisionIsRefusedNotCancelled drives the REAL resume entry
// (`resumeWorkflow`, the same one the `workflow:lobster` op binds, reached host-side as
// OpWorkflowResume) through all three states of the wire tri-state `approve *bool`, plus the
// separate ABORT arm. The load-bearing case is the ABSENT one.
//
// The wire carries three DIFFERENT values, and the engine must not collapse them:
//
//	approve: true   → an approval      → the run proceeds past the gate
//	approve: false  → a REJECTION      → the run is cancelled
//	approve absent  → NO ANSWER at all → REFUSED BY NAME, and the gate is left answerable
//
// The third case is why the field became a pointer. On the OLD engine the request carried a
// plain `bool approve`, so a resume that named no decision arrived as the zero value `false`
// — indistinguishable from an explicit rejection — and `resumeWorkflow`'s catch-all
// `default:` arm assigned it straight to `state.Approved`. The gate was then read as a
// rejection: the pending state was DELETED and the run reported "cancelled". An absent answer
// silently destroyed a gate nobody had answered. After the change a decision-less request
// falls through the switch untouched, and `applyResume` refuses it by name BEFORE the state
// is touched, so the SAME token still carries a live gate for a later, real answer.
//
// Sub-assertion 3 also pins the survival: the refusal must not consume the token, so a
// follow-up `approve: true` on that same token must complete the run. A fix that refused by
// name but had already burned the state would pass the message check and still lose the gate.
func TestApprovalGateResumeWithoutADecisionIsRefusedNotCancelled(t *testing.T) {
	// `resumeWorkflow` builds its OWN engine from the PROCESS environment, so the state dir the
	// pause writes into must be visible to it — the engine helper alone is not enough.
	stateDir := t.TempDir()
	t.Setenv("LOBSTER_STATE_DIR", stateDir)
	eng, _, _ := testEngine(t, map[string]string{"LOBSTER_STATE_DIR": stateDir})
	path := writeWorkflow(t, `
steps:
  - id: prepare
    run: 'echo "ready"'
  - id: sign
    approval: "Ship it?"
    run: 'echo shipped'
  - id: finish
    run: 'echo done'
`)

	ctx := context.Background()

	// pause runs the workflow up to the gate and returns the resume token a caller would
	// answer. Each subtest gets its OWN pause: answering consumes the state, so a shared
	// token would make later subtests read a state an earlier one retired.
	pause := func(t *testing.T) string {
		t.Helper()
		res, err := eng.runFile(ctx, path, nil, nil)
		if err != nil {
			t.Fatalf("pause run: %v", err)
		}
		if res.Status != "needs_approval" {
			t.Fatalf("pause run status = %q, want needs_approval", res.Status)
		}
		if res.RequiresApproval == nil || res.RequiresApproval.ResumeToken == "" {
			t.Fatal("a paused gate must carry a resume token")
		}
		return res.RequiresApproval.ResumeToken
	}
	resume := func(t *testing.T, req spec.WorkflowResumeRequest) *spec.WorkflowRunReply {
		t.Helper()
		reply, err := resumeWorkflow(ctx, nil, req)
		if err != nil {
			t.Fatalf("resume %+v: transport error %v (the engine's own refusal must come back as a recorded reply, not an error)", req, err)
		}
		return reply
	}

	t.Run("approve=true proceeds past the gate", func(t *testing.T) {
		reply := resume(t, spec.WorkflowResumeRequest{Pipeline: "gate", Token: pause(t), Approve: boolPtr(true)})
		if reply.Status != "ok" {
			t.Fatalf("status = %q, want ok (an explicit approval must advance the run)", reply.Status)
		}
		if reply.Output != "done" {
			t.Fatalf("output = %q, want done (the step AFTER the gate must run)", reply.Output)
		}
	})

	t.Run("approve=false cancels", func(t *testing.T) {
		reply := resume(t, spec.WorkflowResumeRequest{Pipeline: "gate", Token: pause(t), Approve: boolPtr(false)})
		if reply.Status != "cancelled" {
			t.Fatalf("status = %q, want cancelled (an explicit REJECTION cancels the run)", reply.Status)
		}
	})

	t.Run("approve absent is refused by name and the gate survives", func(t *testing.T) {
		token := pause(t)
		reply := resume(t, spec.WorkflowResumeRequest{Pipeline: "gate", Token: token})
		const wantErr = "Workflow resume requires --approve yes|no for approval requests"
		if reply.Status != "error" {
			t.Fatalf("status = %q, want error — a request carrying NO decision was read as one (the old engine's catch-all default turned silence into a rejection)", reply.Status)
		}
		if !strings.Contains(reply.Error, wantErr) {
			t.Fatalf("error = %q, want it to name the refusal %q", reply.Error, wantErr)
		}

		// The refusal must return BEFORE the state is consumed: the same token must still gate.
		after := resume(t, spec.WorkflowResumeRequest{Pipeline: "gate", Token: token, Approve: boolPtr(true)})
		if after.Status != "ok" {
			t.Fatalf("follow-up approve=true status = %q (error %q), want ok — the decision-less resume must not have destroyed the pending gate", after.Status, after.Error)
		}
		if after.Output != "done" {
			t.Fatalf("follow-up output = %q, want done", after.Output)
		}
	})

	t.Run("cancel=true cancels", func(t *testing.T) {
		reply := resume(t, spec.WorkflowResumeRequest{Pipeline: "gate", Token: pause(t), Cancel: true})
		if reply.Status != "cancelled" {
			t.Fatalf("status = %q, want cancelled (an ABORT is a different arm from a rejection, and both cancel)", reply.Status)
		}
	})
}

func TestApprovalIdentityPolicy(t *testing.T) {
	eng, _, _ := testEngine(t, nil)
	path := writeWorkflow(t, `
steps:
  - id: sign
    approval:
      required_approver: "maintainer"
    run: 'echo ok'
`)
	res, err := eng.runFile(context.Background(), path, nil, nil)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	p, _ := decodeToken(res.RequiresApproval.ResumeToken)
	if _, err := eng.store.load(p.StateKey); err != nil {
		t.Fatalf("state was not persisted: %v", err)
	}

	// An approver who is NOT the required one is refused BEFORE the effect runs.
	engWrong, _, _ := testEngine(t, map[string]string{"LOBSTER_STATE_DIR": eng.store.dir})
	engWrong.env[approvalApprovedByEnv] = "someone-else"
	stWrong, err := engWrong.store.load(p.StateKey)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	wrong := *stWrong
	wrong.StateKey = p.StateKey
	wrong.Approved = boolPtr(true)
	if _, err := engWrong.runFile(context.Background(), path, nil, &wrong); err == nil {
		t.Fatal("expected the identity policy to refuse the wrong approver")
	} else if !strings.Contains(err.Error(), "requires approver 'maintainer', got 'someone-else'") {
		t.Fatalf("error = %q, want the required-approver message", err)
	}

	// A resume with NO approver identity at all is refused too, and names the env var that
	// supplies one.
	engAnon, _, _ := testEngine(t, map[string]string{"LOBSTER_STATE_DIR": eng.store.dir})
	stAnon, err := engAnon.store.load(p.StateKey)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	anon := *stAnon
	anon.StateKey = p.StateKey
	anon.Approved = boolPtr(true)
	if _, err := engAnon.runFile(context.Background(), path, nil, &anon); err == nil {
		t.Fatal("expected the identity policy to refuse an anonymous approver")
	} else if !strings.Contains(err.Error(), approvalApprovedByEnv) {
		t.Fatalf("error = %q, want it to name %s", err, approvalApprovedByEnv)
	}

	// The refused resumes must NOT have burned the token: the tombstone is only written
	// once the decision survives the policy check.
	if _, err := eng.store.load(p.StateKey); err != nil {
		t.Fatalf("state was destroyed by a refused resume: %v", err)
	}

	engRight, _, _ := testEngine(t, map[string]string{"LOBSTER_STATE_DIR": eng.store.dir})
	engRight.env[approvalApprovedByEnv] = "maintainer"
	st2, err := engRight.store.load(p.StateKey)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	right := *st2
	right.StateKey = p.StateKey
	right.Approved = boolPtr(true)
	good, err := engRight.runFile(context.Background(), path, nil, &right)
	if err != nil {
		t.Fatalf("approved resume: %v", err)
	}
	if good.Status != "ok" {
		t.Fatalf("status = %q, want ok", good.Status)
	}
}

func TestInputGateSchemaValidation(t *testing.T) {
	eng, _, _ := testEngine(t, nil)
	path := writeWorkflow(t, `
steps:
  - id: seed
    run: 'echo "{\"ticket\":42}"'
  - id: ask
    input:
      prompt: "Decide"
      responseSchema:
        type: object
        required: ["decision"]
        properties:
          decision: {type: string, enum: ["ship", "hold"]}
  - id: act
    run: 'printf "%s" "$ask.response.decision"'
`)
	res, err := eng.runFile(context.Background(), path, nil, nil)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if res.Status != "needs_input" {
		t.Fatalf("status = %q, want needs_input", res.Status)
	}
	if res.RequiresInput.StepID != "ask" {
		t.Fatalf("step = %q, want ask", res.RequiresInput.StepID)
	}
	// The subject came from the previous step's json.
	subject, ok := res.RequiresInput.Subject.(map[string]any)
	if !ok || subject["ticket"] != float64(42) {
		t.Fatalf("subject = %#v, want the previous step's json", res.RequiresInput.Subject)
	}

	p, _ := decodeToken(res.RequiresInput.ResumeToken)
	st, _ := eng.store.load(p.StateKey)

	// A response the schema refuses must be rejected.
	bad := *st
	bad.StateKey = p.StateKey
	bad.HasResponse = true
	bad.Response = map[string]any{"decision": "explode"}
	if _, err := eng.runFile(context.Background(), path, nil, &bad); err == nil {
		t.Fatal("expected the schema to reject the response")
	}

	good := *st
	good.StateKey = p.StateKey
	good.HasResponse = true
	good.Response = map[string]any{"decision": "ship"}
	done, err := eng.runFile(context.Background(), path, nil, &good)
	if err != nil {
		t.Fatalf("valid resume: %v", err)
	}
	if len(done.Output) != 1 || done.Output[0] != "ship" {
		t.Fatalf("output = %#v, want [ship]", done.Output)
	}
}

func TestPipelineStdlib(t *testing.T) {
	eng, _, _ := testEngine(t, nil)
	path := writeWorkflow(t, `
steps:
  - id: shape
    pipeline: 'exec echo ''[{"n":"b","v":2},{"n":"a","v":3},{"n":"c","v":1}]'' | where .v >= 2 | sort .n | pick n'
`)
	res, err := eng.runFile(context.Background(), path, nil, nil)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(res.Output) != 2 {
		t.Fatalf("output = %#v, want the two filtered items (a JSON array spreads into items)", res.Output)
	}
	first, _ := res.Output[0].(map[string]any)
	second, _ := res.Output[1].(map[string]any)
	if first["n"] != "a" || second["n"] != "b" {
		t.Fatalf("sorted/filtered = %#v, want a then b", res.Output)
	}
	if _, extra := first["v"]; extra {
		t.Fatalf("pick left other fields in place: %#v", first)
	}
}

func TestPipelineRefusesExternalStage(t *testing.T) {
	eng, _, _ := testEngine(t, nil)
	path := writeWorkflow(t, `
steps:
  - id: nope
    pipeline: "openclaw.invoke something"
`)
	_, err := eng.runFile(context.Background(), path, nil, nil)
	if err == nil {
		t.Fatal("expected the external stage to be refused")
	}
	if !strings.Contains(err.Error(), "not supported by the lobster engine") {
		t.Fatalf("error = %q, want the unsupported-stage message", err)
	}
}

func TestPipelineUnknownStageUsesUpstreamMessage(t *testing.T) {
	eng, _, _ := testEngine(t, nil)
	path := writeWorkflow(t, `
steps:
  - id: nope
    pipeline: "frobnicate"
`)
	_, err := eng.runFile(context.Background(), path, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "Unknown command: frobnicate") {
		t.Fatalf("error = %v, want upstream's Unknown command message", err)
	}
}

func TestWorkflowCompositionAndCycle(t *testing.T) {
	eng, _, _ := testEngine(t, nil)
	dir := t.TempDir()
	child := filepath.Join(dir, "child.lobster")
	parent := filepath.Join(dir, "parent.lobster")

	if err := os.WriteFile(child, []byte(`
steps:
  - id: greet
    run: 'printf "%s" "${who}"'
`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(parent, []byte(`
steps:
  - id: call
    workflow: child.lobster
    workflow_args:
      who: "composed"
`), 0o644); err != nil {
		t.Fatal(err)
	}

	res, err := eng.runFile(context.Background(), parent, nil, nil)
	if err != nil {
		t.Fatalf("compose: %v", err)
	}
	if len(res.Output) != 1 || res.Output[0] != "composed" {
		t.Fatalf("output = %#v, want [composed]", res.Output)
	}

	// A cycle must fail loudly, not recurse.
	if err := os.WriteFile(child, []byte(`
steps:
  - id: again
    workflow: parent.lobster
`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.runFile(context.Background(), parent, nil, nil); err == nil {
		t.Fatal("expected the composition cycle to be refused")
	} else if !strings.Contains(err.Error(), "cycle") {
		t.Fatalf("error = %q, want it to name the cycle", err)
	}
}

func TestTokenRoundTripAndRejection(t *testing.T) {
	token := encodeToken("workflow_resume_abc")
	p, err := decodeToken(token)
	if err != nil {
		t.Fatalf("round trip: %v", err)
	}
	if p.StateKey != "workflow_resume_abc" || p.Kind != tokenKind || p.ProtocolVersion != 1 || p.V != 1 {
		t.Fatalf("payload = %#v, want the upstream wire shape", p)
	}
	for _, bad := range []string{"", "not-base64!!", "e30"} { // "e30" is base64url for "{}"
		if _, err := decodeToken(bad); err == nil {
			t.Fatalf("token %q should have been rejected", bad)
		}
	}
}

func TestLoaderRejectsBrokenWorkflows(t *testing.T) {
	cases := map[string]string{
		"no steps":       "steps: []",
		"two exec arms":  "steps:\n  - id: a\n    run: 'echo x'\n    pipeline: 'json'\n",
		"duplicate id":   "steps:\n  - id: a\n    run: 'echo x'\n  - id: a\n    run: 'echo y'\n",
		"no exec arm":    "steps:\n  - id: a\n    timeout_ms: 100\n",
		"input with run": "steps:\n  - id: a\n    input: {prompt: 'p', responseSchema: {type: object}}\n    run: 'echo x'\n",
		"blank workflow": "steps:\n  - id: a\n    workflow: '  '\n",
		"bad on_error":   "steps:\n  - id: a\n    run: 'echo x'\n    on_error: explode\n",
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := loadWorkflowFile(writeWorkflow(t, body)); err == nil {
				t.Fatalf("%s should have been rejected", name)
			}
		})
	}
}

func TestLoaderAcceptsGateOnlyStep(t *testing.T) {
	file, err := loadWorkflowFile(writeWorkflow(t, `
steps:
  - id: gate
    approval: true
`))
	if err != nil {
		t.Fatalf("a gate-only step must load: %v", err)
	}
	if len(file.Steps) != 1 {
		t.Fatalf("steps = %d, want 1", len(file.Steps))
	}
}

func TestDryRunListsWithoutExecuting(t *testing.T) {
	eng, _, _ := testEngine(t, nil)
	eng.dryRun = true
	marker := filepath.Join(t.TempDir(), "ran")
	path := writeWorkflow(t, `
steps:
  - id: a
    run: 'touch `+marker+`'
  - id: b
    when: "false"
    run: 'touch `+marker+`'
`)
	res, err := eng.runFile(context.Background(), path, nil, nil)
	if err != nil {
		t.Fatalf("dry run: %v", err)
	}
	if _, err := os.Stat(marker); err == nil {
		t.Fatal("dry run executed a step")
	}
	text := renderTemplateValue(res.Output[0])
	if !strings.Contains(text, "run a shell") || !strings.Contains(text, "skip b") {
		t.Fatalf("listing = %q, want `run a` and `skip b`", text)
	}
}

func TestTimeoutMessageNamesTheStep(t *testing.T) {
	step := &params.LobsterStep{Id: "slow", Timeout_ms: 250}
	if got := timeoutMessage(step); got != "Step 'slow' timed out after 250ms" {
		t.Fatalf("timeoutMessage = %q", got)
	}
}

func TestStepRefForms(t *testing.T) {
	eng, _, _ := testEngine(t, nil)
	path := writeWorkflow(t, `
steps:
  - id: produce
    run: 'echo "{\"name\":\"ada\",\"items\":[{\"k\":\"x\"},{\"k\":\"y\"}]}"'
  - id: forms
    run: 'printf "%s|%s|%s|%s|%s" "$produce.json.name" "$produce.name" "$produce.json.items.1.k" "$produce.json.nope" "$produce.json.items.9"'
`)
	res, err := eng.runFile(context.Background(), path, nil, nil)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	want := "ada|ada|y||"
	if len(res.Output) != 1 || res.Output[0] != want {
		t.Fatalf("output = %#v, want [%s]", res.Output, want)
	}
}

func TestUnknownStepRefIsLeftForTheShell(t *testing.T) {
	eng, _, _ := testEngine(t, nil)
	path := writeWorkflow(t, `
steps:
  - id: show
    run: 'printf "%s|%s" "$nowhere.json.x" "$HOME"'
`)
	res, err := eng.runFile(context.Background(), path, nil, nil)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	// An UNKNOWN id's reference is left literally in place (upstream's non-strict pass), so
	// the SHELL sees `$nowhere.json.x` and expands the unset `$nowhere` away, leaving the
	// literal remainder. That is precisely what protects a real shell variable: `$HOME`
	// reaches the shell untouched instead of being eaten as a step ref.
	want := ".json.x|" + os.Getenv("HOME")
	if len(res.Output) != 1 || res.Output[0] != want {
		t.Fatalf("output = %#v, want [%s]", res.Output, want)
	}
}
