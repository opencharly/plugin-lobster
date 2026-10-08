package pluginlobster

// run.go — the op envelope surface: run, resume, schedule, emit.
//
// The engine is handed the LOWERED pair. It never lowers: `WorkflowRunRequest` carries a
// pipeline NAME and an optional `gen_dir`, and the front-end (plugin-pipeline) has
// already resolved the authored `kind:pipeline` entity and written
// `<gen_dir>/{workflow.lobster, charly.yml}` there. So a run loads the file that is
// already on disk, and `emit` re-materializes that same pair at an out_dir for
// inspection. This is the seam: lowering is the front-end's job, execution is this
// engine's, and neither guesses at the other's half.
//
// ── A NOTE ON THE ENGINE-WIRE PROJECTION ────────────────────────────────────
// `#WorkflowRunReply.requires_approval` is `#WorkflowApproval{message, timeout_ms}` and
// `requires_input` is `#WorkflowInputRequest{step, prompt, response_schema, defaults}`.
// The engine's own envelope is richer (lobster's `items`/`preview`/`approvalId` +
// the approver-identity policy), and those fields have NO home in the pinned engine wire.
// They
// are therefore dropped at this boundary — which is a LOSSY PROJECTION, not a silent
// drop: it is stated here, and the missing fields are recorded as an open coordination
// item against the spec leg. What IS carried is everything a resume needs: the token,
// the gate's step, its prompt, and (for input) its schema.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	pb "github.com/opencharly/spec/proto"
	"github.com/opencharly/spec/spec"
)

// runWorkflow executes a lowered workflow.
func runWorkflow(ctx context.Context, _ *pb.InvokeRequest, in spec.WorkflowRunRequest) (*spec.WorkflowRunReply, error) {
	if strings.TrimSpace(in.Pipeline) == "" {
		return nil, fmt.Errorf("workflow-run: pipeline is required")
	}
	genDir, err := resolveGenDir(in.Pipeline, in.GenDir)
	if err != nil {
		return nil, err
	}
	lobsterPath := filepath.Join(genDir, "workflow.lobster")
	if _, serr := os.Stat(lobsterPath); serr != nil {
		return nil, fmt.Errorf("workflow-run: %s is not lowered (%s); the front-end resolves and lowers the pipeline before dispatching here", lobsterPath, serr)
	}

	env := environMap()
	env["CHARLY_BIN"] = resolveCharlyBin(env)
	cwd, _ := os.Getwd()

	eng := newEngine(engineOptions{
		charlyBin:   env["CHARLY_BIN"],
		registry:    newRegistry(),
		stdout:      os.Stdout,
		stderr:      os.Stderr,
		interactive: in.Mode != "tool" && stdinIsTTY(),
		env:         env,
		cwd:         cwd,
		dryRun:      in.DryRun,
	})

	result, err := eng.runFile(ctx, lobsterPath, stringArgsToAny(in.Args), nil)
	marker := strings.TrimSpace(env[scheduleMarkerEnv])
	if err != nil {
		writeScheduleMarker(marker, in.Pipeline, "error", err)
		return &spec.WorkflowRunReply{Status: "error", Error: err.Error()}, nil
	}
	reply := runReply(result)
	writeScheduleMarker(marker, in.Pipeline, reply.Status, nil)
	return reply, nil
}

// resumeWorkflow answers a pending approval or input gate.
func resumeWorkflow(ctx context.Context, _ *pb.InvokeRequest, in spec.WorkflowResumeRequest) (*spec.WorkflowRunReply, error) {
	store := newStateStore(environMap())

	stateKey := ""
	switch {
	case strings.TrimSpace(in.Token) != "":
		payload, err := decodeToken(in.Token)
		if err != nil {
			return nil, err
		}
		stateKey = payload.StateKey
	case strings.TrimSpace(in.Id) != "":
		key, err := store.resolveApprovalID(in.Id)
		if err != nil {
			return nil, err
		}
		stateKey = key
	default:
		return nil, fmt.Errorf("workflow-resume: one of token or id is required")
	}

	state, err := store.load(stateKey)
	if err != nil {
		return nil, err
	}

	// The wire request is TRI-STATE, so the decision is read from the field that was
	// actually set: `response` is an input answer, `cancel` is an ABORT (it says nothing
	// about whether the proposal is acceptable), and an explicit `approve` — either
	// value — is the approval decision. `in.Approve` is a *bool, so `false` (a rejection)
	// and `nil` (no answer given) are DIFFERENT wire values.
	//
	// A request that sets NONE of the three carries no decision at all, and it is left
	// that way: `applyResume` refuses it by name ("requires --approve yes|no") instead of
	// letting the old catch-all default read a decision-less request as a rejection.
	switch {
	case in.Cancel:
		state.Cancel = true
	case in.Response != nil:
		state.HasResponse = true
		state.Response = in.Response
	case in.Approve != nil:
		state.Approved = in.Approve
	}

	env := environMap()
	env["CHARLY_BIN"] = resolveCharlyBin(env)
	cwd, _ := os.Getwd()

	eng := newEngine(engineOptions{
		charlyBin: env["CHARLY_BIN"],
		registry:  newRegistry(),
		stdout:    os.Stdout,
		stderr:    os.Stderr,
		env:       env,
		cwd:       cwd,
	})

	result, err := eng.runFile(ctx, state.FilePath, state.Args, state)
	if errors.Is(err, errCancelled) {
		result = &runResult{Status: "cancelled", Output: []any{}}
		err = nil
	}
	if err != nil {
		return &spec.WorkflowRunReply{Status: "error", Error: err.Error()}, nil
	}
	return runReply(result), nil
}

// emitWorkflow re-materializes the lowered pair for inspection.
func emitWorkflow(_ context.Context, in spec.WorkflowEmitRequest) (*spec.WorkflowEmitReply, error) {
	if strings.TrimSpace(in.Pipeline) == "" {
		return nil, fmt.Errorf("workflow-emit: pipeline is required")
	}
	if len(in.Format) == 0 {
		return nil, fmt.Errorf("workflow-emit: format is required")
	}
	genDir, err := resolveGenDir(in.Pipeline, "")
	if err != nil {
		return nil, err
	}
	outDir := strings.TrimSpace(in.OutDir)
	if outDir == "" {
		outDir = genDir
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return nil, fmt.Errorf("workflow-emit: create out_dir: %w", err)
	}

	files := map[string]string{}
	for _, format := range in.Format {
		switch format {
		case "lobster":
			src := filepath.Join(genDir, "workflow.lobster")
			dst := filepath.Join(outDir, "workflow.lobster")
			if err := copyFile(src, dst); err != nil {
				return nil, fmt.Errorf("workflow-emit: %w", err)
			}
			files["lobster"] = dst
		case "charly-yml":
			src := filepath.Join(genDir, "charly.yml")
			dst := filepath.Join(outDir, "charly.yml")
			if err := copyFile(src, dst); err != nil {
				return nil, fmt.Errorf("workflow-emit: %w", err)
			}
			files["charly-yml"] = dst
		case "github-actions":
			// The authored pipeline is designed to lower to GHA, but that consumer is NOT BUILT. Emitting
			// something that looked like it would be the silent-drop failure the engine wire's own
			// contract forbids, so this is a hard refusal with the reason.
			return nil, fmt.Errorf("workflow-emit: the github-actions consumer is designed but not built; supported formats are lobster, charly-yml")
		default:
			return nil, fmt.Errorf("workflow-emit: unknown format %q; supported formats are lobster, charly-yml", format)
		}
	}
	return &spec.WorkflowEmitReply{Files: files}, nil
}

// ---------------------------------------------------------------------------
// reply projection
// ---------------------------------------------------------------------------

// runReply maps the engine's envelope onto the engine wire's reply.
func runReply(result *runResult) *spec.WorkflowRunReply {
	if result == nil {
		return &spec.WorkflowRunReply{Status: "error", Error: "workflow produced no result"}
	}
	reply := &spec.WorkflowRunReply{Status: result.Status}
	if len(result.Output) > 0 {
		reply.Output = renderOutput(result.Output)
	}
	if result.Cost != nil {
		reply.Cost = result.Cost.EstimatedCostUSD
	}
	if a := result.RequiresApproval; a != nil {
		reply.ResumeToken = a.ResumeToken
		reply.RequiresApproval = spec.WorkflowApproval{
			Message:   a.Prompt,
			TimeoutMs: approvalTimeoutMs(),
		}
	}
	if i := result.RequiresInput; i != nil {
		reply.ResumeToken = i.ResumeToken
		reply.RequiresInput = spec.WorkflowInputRequest{
			Step:           i.StepID,
			Prompt:         i.Prompt,
			ResponseSchema: schemaMap(i.ResponseSchema),
			Defaults:       defaultsAsStrings(i.Defaults),
		}
	}
	return reply
}

// renderOutput is upstream's `toOutputItems` rendering for the one-string reply field:
// one item renders as itself, several render as a JSON array.
func renderOutput(items []any) string {
	if len(items) == 1 {
		return serializeValueForStdout(items[0])
	}
	b, err := json.Marshal(items)
	if err != nil {
		return ""
	}
	return string(b)
}

// approvalTimeoutMs reads the gate timeout the reply advertises, defaulting to 0
// (no advertised timeout) when unset.
func approvalTimeoutMs() int64 {
	raw := strings.TrimSpace(environMap()[approvalPromptTimeoutEnv])
	if raw == "" {
		return 0
	}
	var v any
	if err := json.Unmarshal([]byte(raw), &v); err != nil {
		return 0
	}
	n, ok := toInt(v)
	if !ok || n <= 0 {
		return 0
	}
	return n
}

// defaultsAsStrings projects the gate's defaults onto the reply's `map[string]string`.
func defaultsAsStrings(v any) map[string]string {
	obj, ok := v.(map[string]any)
	if !ok {
		return nil
	}
	out := make(map[string]string, len(obj))
	for k, val := range obj {
		out[k] = renderTemplateValue(val)
	}
	return out
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// resolveGenDir is the ONE place the generated directory is decided: an explicit
// gen_dir wins, else the documented project convention.
func resolveGenDir(pipeline, genDir string) (string, error) {
	if strings.TrimSpace(genDir) != "" {
		return genDir, nil
	}
	cwd, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("resolve project dir: %w", err)
	}
	return filepath.Join(cwd, ".opencharly", "pipelines", pipeline), nil
}

// resolveCharlyBin finds the charly binary the generated charly.yml invokes. The
// generated file references `$CHARLY_BIN`, so this value is the seam that makes a
// lowered pair runnable: without it, a charly step has nothing to exec.
func resolveCharlyBin(env map[string]string) string {
	if v := strings.TrimSpace(env["CHARLY_BIN"]); v != "" {
		return v
	}
	if p, err := exec.LookPath("charly"); err == nil {
		return p
	}
	// Not resolvable here: fall back to the bare name and let the shell's PATH decide at
	// exec time. Failing the whole run now would break a workflow that never uses a
	// charly step.
	return "charly"
}

func stringArgsToAny(in map[string]string) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func copyFile(src, dst string) error {
	b, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	if src == dst {
		return nil
	}
	return os.WriteFile(dst, b, 0o644)
}

// ---------------------------------------------------------------------------
// gate state helpers
// ---------------------------------------------------------------------------

// saveGateState persists a paused run and returns its state key.
func (st *runState) saveGateState(ctx context.Context, rs resumeState) (string, error) {
	rs.Steps = cloneResults(st.results)
	// Carry the redo budget across the pause: a resume that reset it would let the run
	// loop past the bound the budget exists to enforce.
	if len(st.redoCount) > 0 {
		rs.RedoCount = st.redoCount
	}
	return st.eng.store.save(ctx, &rs)
}

// cleanupOnError retires the consumed tombstone when a RESUMED run fails. The resumed
// effect did not commit, so the token that gated it must stay answerable — otherwise a
// single transient failure would burn the approval a human granted.
func (st *runState) cleanupOnError(ctx context.Context) {
	if st.consumedKey == "" || !st.consumed {
		return
	}
	if err := st.eng.store.restoreConsumed(st.consumedKey); err != nil {
		fmt.Fprintf(st.eng.stderr, "[WARN] could not restore the consumed resume marker for %s: %v\n", st.consumedKey, err)
	}
}
