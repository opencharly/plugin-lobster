package pluginlobster

// engine.go — the lobster execution loop.
//
// A transcription of upstream `src/workflows/file.ts`'s run loop: step dispatch, the
// retry policy, `on_error`, the approval/input gates, and the run result. The shape is
// upstream's, deliberately — the engine's whole value is that a `.lobster` file means
// the same thing here as it does there.
//
// Three divergences, each forced and each documented at its site:
//
//  1. There is no LLM-spend ledger or replay provenance. Those exist to bill and de-dupe
//     PROVIDER calls, and this engine serves only the deterministic stdlib: an LLM stage
//     is refused with an actionable error (see pipeline.go), so no step can produce
//     `usage`. Cost accounting reads the documented `_meta.cost` a step may emit.
//  2. `dry_run` reports the steps that would run; upstream's exact dry-run prose lives in
//     a module outside the transcribed source, so this is charly's own listing.
//  3. Sub-workflows resolve relative to the workflow file's directory, like upstream, but
//     the engine always executes the LOWERED pair — so a `workflow:` ref names another
//     lowered `.lobster` in the same gen dir.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/opencharly/plugin-lobster/candy/plugin-lobster/params"
)

// ---------------------------------------------------------------------------
// step dispatch
// ---------------------------------------------------------------------------

type stepKind int

const (
	kindNone stepKind = iota
	kindShell
	kindPipeline
	kindWorkflow
	kindParallel
)

func (k stepKind) String() string {
	switch k {
	case kindShell:
		return "shell"
	case kindPipeline:
		return "pipeline"
	case kindWorkflow:
		return "workflow"
	case kindParallel:
		return "parallel"
	default:
		return "none"
	}
}

// getStepExecution is upstream's precedence: parallel > workflow > pipeline > shell.
// `for_each` is not chosen here because upstream tests it first, in the run loop, and a
// for_each step may carry neither of these arms.
func getStepExecution(step *params.LobsterStep) (stepKind, string) {
	if len(step.Parallel.Branches) > 0 {
		return kindParallel, ""
	}
	if strings.TrimSpace(step.Workflow) != "" {
		return kindWorkflow, step.Workflow
	}
	if strings.TrimSpace(step.Pipeline) != "" {
		return kindPipeline, step.Pipeline
	}
	cmd := step.Run
	if strings.TrimSpace(cmd) == "" {
		cmd = step.Command
	}
	if strings.TrimSpace(cmd) != "" {
		return kindShell, cmd
	}
	return kindNone, ""
}

// ---------------------------------------------------------------------------
// redo — the charly-only conditional back-edge
// ---------------------------------------------------------------------------

// redoSentinelPrefix is the charly-only carriage for a redo trigger: a plan verb that fails
// redo-ably prints this line to ITS stderr, which the `charly task` process inherits and this
// engine captures in full (see attemptStep's kindShell arm). The trigger cannot be derived from
// the step's redo: spec alone — an ade bed ERROR and an ade fail_on verdict carry the same spec
// and diverge (one fails hard, one re-enters) — so it must cross the process boundary at runtime.
const redoSentinelPrefix = "LOOP-GUARD-TRIGGER: "

// redoDefaultMax / redoDefaultEscalate mirror the retired executor's defaults
// (executor.go:284-291: maxRedo<=0 -> 2, escalateAfter<=0 -> 3).
const (
	redoDefaultMax      = 2
	redoDefaultEscalate = 3
)

// redoTriggerFromStderr returns the trigger named by the LAST sentinel line in s, or "".
func redoTriggerFromStderr(s string) string {
	trigger := ""
	for _, line := range strings.Split(s, "\n") {
		if rest, ok := strings.CutPrefix(strings.TrimSpace(line), redoSentinelPrefix); ok {
			if name := strings.TrimSpace(rest); name != "" {
				trigger = name
			}
		}
	}
	return trigger
}

// redoTriggerError carries a redo trigger out of a failed shell step to runSteps, which acts on
// it. The step's stderr is in scope only inside attemptStep, so wrapping the trigger in the
// error is what reaches the loop without changing attemptStep's signature.
type redoTriggerError struct {
	trigger string
	err     error
}

func (e *redoTriggerError) Error() string { return e.err.Error() }
func (e *redoTriggerError) Unwrap() error { return e.err }

// ---------------------------------------------------------------------------
// engine
// ---------------------------------------------------------------------------

// engine holds everything a run needs that outlives one workflow file. A sub-workflow
// composition recurses through `runFile`, which is why the engine — not the run state —
// owns the cycle set and the shell seam.
type engine struct {
	charlyBin   string
	shell       shellRunner
	reg         *registry
	stdout      io.Writer
	stderr      io.Writer
	interactive bool
	env         map[string]string
	cwd         string
	dryRun      bool
	store       *stateStore
	active      map[string]bool
}

func newEngine(opts engineOptions) *engine {
	e := &engine{
		charlyBin:   opts.charlyBin,
		shell:       opts.shell,
		reg:         opts.registry,
		stdout:      opts.stdout,
		stderr:      opts.stderr,
		interactive: opts.interactive,
		env:         opts.env,
		cwd:         opts.cwd,
		dryRun:      opts.dryRun,
		store:       newStateStore(opts.env),
		active:      map[string]bool{},
	}
	if e.shell == nil {
		e.shell = localShell{}
	}
	if e.stdout == nil {
		e.stdout = os.Stdout
	}
	if e.stderr == nil {
		e.stderr = os.Stderr
	}
	if e.env == nil {
		e.env = environMap()
	}
	if e.cwd == "" {
		if wd, err := os.Getwd(); err == nil {
			e.cwd = wd
		}
	}
	if e.reg == nil {
		e.reg = newRegistry()
	}
	return e
}

type engineOptions struct {
	charlyBin   string
	shell       shellRunner
	registry    *registry
	stdout      io.Writer
	stderr      io.Writer
	interactive bool
	env         map[string]string
	cwd         string
	dryRun      bool
}

// ---------------------------------------------------------------------------
// run result
// ---------------------------------------------------------------------------

// runResult is upstream's WorkflowRunResult — the tool-mode envelope v1. run.go maps it
// onto spec.WorkflowRunReply.
type runResult struct {
	Status           string
	Output           []any
	RequiresApproval *approvalRequest
	RequiresInput    *inputRequest
	Cost             *costSummary
}

// cancellation is a resume that cancelled the run. It is returned as an error by the
// internals and turned into `status: "cancelled"` by run().
var errCancelled = errors.New("workflow cancelled")

// ---------------------------------------------------------------------------
// the run
// ---------------------------------------------------------------------------

// runState is one workflow file's execution state. Upstream carries the same values in a
// closure; naming them makes the resume path (which rewrites them) readable.
type runState struct {
	eng        *engine
	file       *params.LobsterFile
	filePath   string
	dir        string
	args       map[string]any
	results    results
	lastStepID string

	resume      *resumeState
	consumedKey string
	consumed    bool

	// redoCount is the per-target redo budget. It is persisted across a resume (see
	// resumeState.RedoCount): the LOOP-GUARD exists to bound re-entries, and a resume that
	// silently reset the budget would let a run loop forever across restarts.
	redoCount map[string]int

	costs *costTracker
}

// runFile loads and executes one workflow file, honouring the resume block handed to it.
func (e *engine) runFile(ctx context.Context, filePath string, args map[string]any, resume *resumeState) (*runResult, error) {
	canonical, err := filepath.Abs(filePath)
	if err != nil {
		return nil, fmt.Errorf("resolve workflow path: %w", err)
	}
	if resolved, rerr := filepath.EvalSymlinks(canonical); rerr == nil {
		canonical = resolved
	}
	if e.active[canonical] {
		return nil, fmt.Errorf("Workflow step creates a cycle: %s is already being executed", canonical)
	}
	e.active[canonical] = true
	defer delete(e.active, canonical)

	file, err := loadWorkflowFile(canonical)
	if err != nil {
		return nil, err
	}
	if resume == nil && len(args) == 0 {
		args = nil
	}
	return e.runLoaded(ctx, canonical, file, args, resume)
}

func (e *engine) runLoaded(ctx context.Context, filePath string, file *params.LobsterFile, args map[string]any, resume *resumeState) (*runResult, error) {
	st := &runState{
		eng:       e,
		file:      file,
		filePath:  filePath,
		dir:       filepath.Dir(filePath),
		args:      resolveWorkflowArgs(file, args, resume),
		results:   results{},
		redoCount: map[string]int{},
		costs:     newCostTracker(file.Cost_limit, e.stderr),
		resume:    resume,
	}
	if resume != nil {
		st.consumedKey = resume.StateKey
		if resume.Steps != nil {
			st.results = cloneResults(resume.Steps)
		}
		// A state file written before this field existed loads with a nil map; the guard
		// keeps the budget from resetting (see the field's comment).
		if resume.RedoCount != nil {
			st.redoCount = resume.RedoCount
		}
	}

	// The approval decision is applied BEFORE the loop: a resumed run must see the
	// approved step's result while evaluating the next step's `when`.
	startIndex := int64(0)
	if resume != nil {
		startIndex = resume.ResumeAtIndex
		if err := st.applyResume(ctx); err != nil {
			if errors.Is(err, errCancelled) {
				return &runResult{Status: "cancelled", Output: []any{}}, nil
			}
			return nil, err
		}
	}
	if startIndex < 0 {
		startIndex = 0
	}

	st.lastStepID = findLastCompletedStepID(file.Steps, st.results)
	if resume != nil && resume.InputStepID != "" {
		st.lastStepID = resume.InputStepID
	}

	if e.dryRun {
		return st.dryRun(startIndex)
	}

	result, err := st.runSteps(ctx, startIndex)
	if err != nil {
		st.cleanupOnError(ctx)
		return nil, err
	}

	// The consumed state is retired only after the run completes: deleting it earlier would
	// let a crash mid-run replay the very effect the token was issued to gate.
	if st.consumedKey != "" && st.consumed {
		if derr := e.store.delete(ctx, st.consumedKey); derr != nil {
			return nil, derr
		}
	}
	result.Cost = st.costs.summary()
	return result, nil
}

// runSteps is the loop. `startIndex` is where a resumed run picks up.
func (st *runState) runSteps(ctx context.Context, startIndex int64) (*runResult, error) {
	e := st.eng
	steps := st.file.Steps

	for idx := int(startIndex); idx < len(steps); idx++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		step := &steps[idx]

		if ok, err := evaluateWhen(step.When, st.results); err != nil {
			return nil, fmt.Errorf("Workflow step %s when: %w", step.Id, err)
		} else if !ok {
			st.results[step.Id] = &stepResult{ID: step.Id, Skipped: true}
			continue
		}

		// The `input` gate is handled here, before the exec arms, because a gate step has no
		// exec arm at all (the loader enforces that).
		if isTypedInputStep(step.Input) {
			gated, res, err := st.inputGate(ctx, step, idx)
			if err != nil {
				return nil, err
			}
			if gated {
				return res, nil
			}
			continue
		}

		env := mergeEnv(e.env, st.file.Env, step.Env, st.args, st.results)
		cwd := e.cwd
		if resolved := resolveCwd(step.Cwd, st.args); resolved != "" {
			cwd = resolved
		} else if resolved := resolveCwd(st.file.Cwd, st.args); resolved != "" {
			cwd = resolved
		}

		result, branchResults, err := st.executeStep(ctx, step, env, cwd)
		if err != nil {
			// `on_error` decides whether a failure ends the run. A cancelled run never gets here:
			// cancellation travels as a context error, checked above and re-checked by the caller.
			isTimeout := step.Timeout_ms > 0 && isTimeoutErr(err)
			errorMessage := err.Error()
			if isTimeout {
				errorMessage = timeoutMessage(step)
			}
			// The redo back-edge: a step that failed with a trigger re-enters an earlier step
			// instead of ending the run. `on_error` never sees it.
			var trgErr *redoTriggerError
			if errors.As(err, &trgErr) {
				target, taken, rerr := st.redoBackEdge(step, trgErr.trigger)
				if rerr != nil {
					return nil, rerr
				}
				if taken {
					idx = target - 1 // the loop's idx++ lands on target
					continue
				}
			}
			policy := step.On_error
			if policy == "" {
				policy = "stop"
			}
			if policy == "stop" {
				// A timeout is reported with the step's own message: the raw shell error says
				// only that something timed out, and in a workflow of many steps that is not a
				// diagnostic. Every other failure keeps its original error, wrap and all.
				if isTimeout {
					return nil, errors.New(errorMessage)
				}
				return nil, err
			}
			st.results[step.Id] = &stepResult{ID: step.Id, Error: true, ErrorMessage: errorMessage}
			st.costs.settle()
			if lerr := st.costs.checkLimit(); lerr != nil {
				return nil, lerr
			}
			if policy == "skip_rest" {
				break
			}
			continue
		}

		// Parallel branch results become top-level refs, exactly as upstream makes them.
		for branchID, br := range branchResults {
			st.results[branchID] = br
			st.costs.track(branchID, br)
		}
		st.results[step.Id] = result
		st.lastStepID = step.Id

		st.costs.track(step.Id, result)
		st.costs.settle()
		if lerr := st.costs.checkLimit(); lerr != nil {
			return nil, lerr
		}

		// The approval gate sits AFTER the step's result is recorded: the thing being approved
		// is what the step produced, and the approver sees it.
		if isApprovalStep(step.Approval) {
			gated, res, err := st.approvalGate(ctx, step, idx)
			if err != nil {
				return nil, err
			}
			if gated {
				return res, nil
			}
		}
	}

	var output []any
	if st.lastStepID != "" {
		output = toOutputItems(st.results[st.lastStepID])
	}
	return &runResult{Status: "ok", Output: output}, nil
}

// redoDeclared reports whether a step carries a redo spec at all. The generated params type
// carries `redo` as a VALUE (cue gengotypes emits an optional struct field as a value, not a
// pointer — the same shape as `retry`, `input` and `cost_limit` here), so absence is "every
// field at its zero value": the schema bounds `max`/`escalate_after` to `> 0`, so a set bound is
// non-zero, and an empty `triggers` map is nil.
func redoDeclared(r params.LobsterRedo) bool {
	return r.On_fail != nil || len(r.Triggers) > 0 || r.Max > 0 || r.Escalate_after > 0
}

// redoBackEdge applies the redo spec of a step that failed with `trigger`. It mirrors the retired
// executor's block in the SAME order (escalate guard FIRST, then max — executor.go:349 before :353),
// and returns the index of the step to re-enter. `taken` is false when the step declares no redo
// spec, so the caller falls through to the on_error policy unchanged.
//
// A trigger whose target resolves to no step is a HARD error: the retired engine silently swallowed
// the failure and ran on (it kept the raw trigger name as the target and matched nothing), which can
// mask a real failure — this port makes that a named spec defect instead.
//
// The bounds are read through the GENERATED STRUCT fields (typed int64), never a map[string]any: a
// JSON number there is a float64 and a bare `.(int)` assertion always fails — the exact defect that
// left the retired executor's per-stage override inert (executor.go:341/:344).
func (st *runState) redoBackEdge(step *params.LobsterStep, trigger string) (target int, taken bool, err error) {
	if !redoDeclared(step.Redo) {
		return 0, false, nil
	}
	name := trigger
	if t, ok := step.Redo.Triggers[trigger]; ok {
		name = t
	}
	j, ok := stepIndexByID(st.file.Steps, name)
	if !ok {
		return 0, true, fmt.Errorf("Workflow step %s: redo trigger %q maps to no step (%q); fix the redo.triggers map", step.Id, trigger, name)
	}
	if st.redoCount == nil {
		st.redoCount = map[string]int{}
	}
	st.redoCount[name]++
	maxRedo := redoDefaultMax
	if step.Redo.Max > 0 {
		maxRedo = int(step.Redo.Max)
	}
	escalateAfter := redoDefaultEscalate
	if step.Redo.Escalate_after > 0 {
		escalateAfter = int(step.Redo.Escalate_after)
	}
	if st.redoCount[name] >= escalateAfter {
		return 0, true, fmt.Errorf("LOOP-GUARD: %s re-entered %d times (escalate)", name, st.redoCount[name])
	}
	if st.redoCount[name] > maxRedo {
		return 0, true, fmt.Errorf("LOOP-GUARD: exceed redo max %d for %s", maxRedo, name)
	}
	return j, true, nil
}

// executeStep runs one step's exec arm with the retry policy applied. A for_each step is
// dispatched to runForEach and takes no exec arm of its own.
func (st *runState) executeStep(ctx context.Context, step *params.LobsterStep, env map[string]string, cwd string) (*stepResult, map[string]*stepResult, error) {
	if step.For_each != "" && len(step.Steps) > 0 {
		res, err := st.runForEach(ctx, step, env, cwd)
		return res, nil, err
	}

	kind, value := getStepExecution(step)
	cfg := resolveRetryConfig(step.Retry)

	var result *stepResult
	var branchResults map[string]*stepResult

	attempt := func() error {
		var err error
		result, branchResults, err = st.attemptStep(ctx, step, kind, value, env, cwd, st.results)
		return err
	}

	if cfg.Max > 1 {
		err := withRetry(ctx, cfg, st.eng.stderr, step.Id, attempt, func(err error) bool {
			return retryable(err, ctx)
		})
		if err != nil {
			return nil, nil, err
		}
	} else if err := attempt(); err != nil {
		return nil, nil, err
	}
	return result, branchResults, nil
}

// attemptStep is ONE attempt: the timeout envelope around one exec arm.
// scope is the ref scope THIS attempt resolves `$id.path` against. It is a parameter, not
// `st.results`, because a for_each body's steps see the LOOP's scope — `item`/`index` plus
// the outer results — and resolving against the run's own map would leave `$item` literal
// inside the shell, which is a silent wrong-answer rather than an error.
func (st *runState) attemptStep(ctx context.Context, step *params.LobsterStep, kind stepKind, value string, env map[string]string, cwd string, scope results) (*stepResult, map[string]*stepResult, error) {
	runCtx := ctx
	var cancel context.CancelFunc
	timeout := time.Duration(0)
	if step.Timeout_ms > 0 {
		timeout = time.Duration(step.Timeout_ms) * time.Millisecond
		runCtx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	switch kind {
	case kindShell:
		command := resolveTemplate(value, st.args, scope)
		stdinVal, err := resolveInputValue(step.Stdin, st.args, scope)
		if err != nil {
			return nil, nil, err
		}
		stdout, stderr, code, err := st.eng.shell.Run(runCtx, command, encodeShellInput(stdinVal), env, cwd, timeout)
		res := &stepResult{ID: step.Id, Stdout: stdout, Stderr: stderr, ExitCode: code}
		if err != nil {
			return nil, nil, err
		}
		if code != 0 {
			detail := strings.TrimSpace(stderr)
			if detail == "" {
				detail = strings.TrimSpace(stdout)
			}
			if detail == "" {
				detail = command
			}
			cause := fmt.Errorf("workflow command failed (%d): %s", code, truncateBytes(detail, 2000))
			// Scan the UNTRUNCATED stderr, never `detail` (which is capped at 2000 bytes and
			// may start mid-stream). A sentinel means this failure is REDO-ABLE: carry the
			// trigger to runSteps, which owns the back-edge.
			if trg := redoTriggerFromStderr(stderr); trg != "" {
				return nil, nil, &redoTriggerError{trigger: trg, err: cause}
			}
			return nil, nil, cause
		}
		res.JSON, res.HasJSON = parseJSON(stdout)
		return res, nil, nil

	case kindPipeline:
		if st.eng.reg == nil {
			return nil, nil, fmt.Errorf("Workflow step %s requires a command registry for pipeline execution", step.Id)
		}
		text := resolveTemplate(value, st.args, scope)
		inputValue, err := resolveInputValue(step.Stdin, st.args, scope)
		if err != nil {
			return nil, nil, err
		}
		items, rendered, err := runPipelineStep(runCtx, st.eng.reg, text, inputValue, env, cwd, st.eng.charlyBin, st.eng.stdout, st.eng.stderr)
		if err != nil {
			return nil, nil, err
		}
		res := &stepResult{ID: step.Id, Stdout: rendered}
		res.JSON, res.HasJSON = parseJSON(rendered)
		if !res.HasJSON && len(items) > 0 {
			res.JSON, res.HasJSON = items, true
			if len(items) == 1 {
				res.JSON = items[0]
			}
		}
		return res, nil, nil

	case kindWorkflow:
		ref := resolveTemplate(value, st.args, scope)
		childPath := ref
		if !filepath.IsAbs(childPath) {
			childPath = filepath.Join(st.dir, childPath)
		}
		subArgs := resolveWorkflowStepArgs(step.Workflow_args, st.args, scope)
		sub, err := st.eng.runFile(runCtx, childPath, subArgs, nil)
		if err != nil {
			return nil, nil, fmt.Errorf("Workflow step %s: %w", step.Id, err)
		}
		// A sub-workflow that halts for a gate cannot be composed: the parent has no way to
		// answer for it, and carrying the token back would need the child's own state — so this
		// is refused loudly rather than silently losing the gate.
		if sub.Status == "needs_approval" || sub.Status == "needs_input" {
			if sub.RequiresApproval != nil && sub.RequiresApproval.ResumeToken != "" {
				_ = st.eng.store.deleteByToken(context.WithoutCancel(runCtx), sub.RequiresApproval.ResumeToken)
			}
			if sub.RequiresInput != nil && sub.RequiresInput.ResumeToken != "" {
				_ = st.eng.store.deleteByToken(context.WithoutCancel(runCtx), sub.RequiresInput.ResumeToken)
			}
			what := "approval"
			if sub.Status == "needs_input" {
				what = "input"
			}
			return nil, nil, fmt.Errorf("Workflow step %s sub-workflow halted for %s. Sub-workflow approval/input gates are not supported in composition.", step.Id, what)
		}
		var jsonVal any
		if len(sub.Output) == 1 {
			jsonVal = sub.Output[0]
		} else {
			jsonVal = append([]any{}, sub.Output...)
		}
		res := &stepResult{ID: step.Id, JSON: jsonVal, HasJSON: true}
		if len(sub.Output) > 0 {
			res.Stdout = serializeValueForStdout(jsonVal)
		}
		return res, nil, nil

	case kindParallel:
		br, err := st.runParallel(runCtx, step, env, cwd)
		if err != nil {
			return nil, nil, err
		}
		primary := br[step.Id]
		delete(br, step.Id)
		return primary, br, nil

	default:
		// A gate-only step: no exec arm, so the result is the resolved `stdin`.
		inputValue, err := resolveInputValue(step.Stdin, st.args, scope)
		if err != nil {
			return nil, nil, err
		}
		return createSyntheticStepResult(step.Id, inputValue), nil, nil
	}
}

// ---------------------------------------------------------------------------
// resume
// ---------------------------------------------------------------------------

// applyResume folds a resume state's decision into the results map before the loop runs.
func (st *runState) applyResume(ctx context.Context) error {
	r := st.resume
	if r.ApprovalStepID != "" && r.InputStepID != "" {
		return fmt.Errorf("Invalid workflow resume state")
	}

	if r.ApprovalStepID != "" {
		if r.HasResponse {
			return fmt.Errorf("Workflow resume requires --approve yes|no for approval requests")
		}
		// A nil `Approved` is a request that carried NO decision at all. Refuse it by
		// name: the wire can now say "rejected" explicitly (`approve: false`), so an
		// absent answer is silence, and silence must never cancel a gate.
		if !r.Cancel && r.Approved == nil {
			return fmt.Errorf("Workflow resume requires --approve yes|no for approval requests")
		}
		if r.Cancel || !*r.Approved {
			if st.consumedKey != "" {
				if err := st.eng.store.delete(ctx, st.consumedKey); err != nil {
					return err
				}
			}
			return errCancelled
		}
		previous := st.results[r.ApprovalStepID]
		if previous == nil {
			previous = &stepResult{ID: r.ApprovalStepID}
		}
		approvedBy := strings.TrimSpace(st.eng.env["LOBSTER_APPROVAL_APPROVED_BY"])
		if err := enforceApprovalIdentity(r.ApprovalStepID, r.ApprovalIdentity, approvedBy); err != nil {
			return err
		}
		approved := true
		previous.Approved = &approved
		if approvedBy != "" {
			previous.ApprovedBy = approvedBy
		}
		st.results[r.ApprovalStepID] = previous
		if err := st.consume(ctx); err != nil {
			return err
		}
	}

	if r.InputStepID != "" {
		if r.Cancel {
			if st.consumedKey != "" {
				if err := st.eng.store.delete(ctx, st.consumedKey); err != nil {
					return err
				}
			}
			return errCancelled
		}
		if r.Approved != nil {
			return fmt.Errorf("Workflow resume requires --response-json for input requests")
		}
		if !r.HasResponse {
			return fmt.Errorf("Workflow resume requires --response-json for input requests")
		}
		idx, ok := stepIndexByID(st.file.Steps, r.InputStepID)
		if !ok {
			return fmt.Errorf("Invalid input step in resume state: %s", r.InputStepID)
		}
		// A resume is answered with `--approve no` on the wrong step
		// if the step's condition flipped; refuse rather than record a response for a
		// step that will not run.
		step := &st.file.Steps[idx]
		ok2, err := evaluateWhen(step.When, st.results)
		if err != nil {
			return err
		}
		if !ok2 {
			return fmt.Errorf("workflow input step condition changed since input request")
		}
		schema := r.InputSchema
		if schema == nil {
			schema = schemaMap(step.Input.ResponseSchema)
		}
		if err := validateInputResponse(schema, r.Response, step.Id); err != nil {
			return err
		}
		previous := st.results[r.InputStepID]
		if previous == nil {
			previous = &stepResult{ID: r.InputStepID}
		}
		previous.Subject = r.InputSubject
		previous.HasSubject = true
		previous.Response = r.Response
		previous.HasResp = true
		previous.Skipped = false
		st.results[r.InputStepID] = previous
		if err := st.consume(ctx); err != nil {
			return err
		}
	}
	return nil
}

// consume writes the non-replayable tombstone BEFORE the resumed effect runs. Ordering is
// the whole point: a crash between the tombstone and the effect leaves the effect gated,
// while the reverse order would leave it replayable.
func (st *runState) consume(ctx context.Context) error {
	if st.consumedKey == "" || st.consumed {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := st.eng.store.markConsumed(st.consumedKey); err != nil {
		return err
	}
	st.consumed = true
	return nil
}

func stepIndexByID(steps []params.LobsterStep, id string) (int, bool) {
	for i := range steps {
		if steps[i].Id == id {
			return i, true
		}
	}
	return 0, false
}

// ---------------------------------------------------------------------------
// dry run
// ---------------------------------------------------------------------------

// dryRun reports every step that would run from startIndex, without executing any of them.
func (st *runState) dryRun(startIndex int64) (*runResult, error) {
	var b strings.Builder
	for idx := int(startIndex); idx < len(st.file.Steps); idx++ {
		step := &st.file.Steps[idx]
		ok, err := evaluateWhen(step.When, st.results)
		if err != nil {
			return nil, fmt.Errorf("Workflow step %s when: %w", step.Id, err)
		}
		arm := describeArm(step)
		mark := "run"
		if !ok {
			mark = "skip"
		}
		fmt.Fprintf(&b, "%d %s %s %s\n", idx, mark, step.Id, arm)
		if isTypedInputStep(step.Input) || isApprovalStep(step.Approval) {
			fmt.Fprintf(&b, "%d gate %s\n", idx, step.Id)
		}
	}
	text := strings.TrimRight(b.String(), "\n")
	out := []any{}
	if text != "" {
		out = []any{text}
	}
	return &runResult{Status: "ok", Output: out}, nil
}

func describeArm(step *params.LobsterStep) string {
	switch {
	case step.For_each != "":
		return "for_each"
	case len(step.Parallel.Branches) > 0:
		return "parallel"
	case strings.TrimSpace(step.Workflow) != "":
		return "workflow"
	case strings.TrimSpace(step.Pipeline) != "":
		return "pipeline"
	case strings.TrimSpace(step.Run) != "" || strings.TrimSpace(step.Command) != "":
		return "shell"
	case isTypedInputStep(step.Input):
		return "input"
	case isApprovalStep(step.Approval):
		return "gate"
	default:
		return "none"
	}
}

// ---------------------------------------------------------------------------
// retry
// ---------------------------------------------------------------------------

// retryConfig is upstream's resolved retry policy. The DEFAULTS are upstream's:
// max 1 (total attempts), fixed backoff, 1000ms, capped at 30000ms, no jitter.
type retryConfig struct {
	Max        int64
	Backoff    string
	DelayMs    float64
	MaxDelayMs float64
	Jitter     bool
}

func resolveRetryConfig(r params.LobsterRetry) retryConfig {
	cfg := retryConfig{Max: 1, Backoff: "fixed", DelayMs: 1000, MaxDelayMs: 30000}
	if r.Max > 0 {
		cfg.Max = r.Max
	}
	if r.Backoff != "" {
		cfg.Backoff = r.Backoff
	}
	if v, ok := toFloat(r.Delay_ms); ok {
		cfg.DelayMs = v
	}
	if v, ok := toFloat(r.Max_delay_ms); ok {
		cfg.MaxDelayMs = v
	}
	cfg.Jitter = r.Jitter
	return cfg
}

// delay is the backoff for a 0-BASED attempt index: `min(delay * 2^attempt, max_delay)`
// for exponential, the flat delay for fixed, with an optional +/-10% jitter, re-clamped.
func (c retryConfig) delay(attempt int64) time.Duration {
	ms := c.DelayMs
	if c.Backoff == "exponential" {
		ms = c.DelayMs
		for i := int64(0); i < attempt; i++ {
			ms *= 2
			if ms >= c.MaxDelayMs {
				ms = c.MaxDelayMs
				break
			}
		}
	}
	if ms > c.MaxDelayMs {
		ms = c.MaxDelayMs
	}
	if c.Jitter {
		// ±10%, uniform, then re-clamped into the cap.
		delta := ms * 0.1
		ms += (rand.Float64()*2 - 1) * delta
		if ms < 0 {
			ms = 0
		}
		if ms > c.MaxDelayMs {
			ms = c.MaxDelayMs
		}
	}
	return time.Duration(ms * float64(time.Millisecond))
}

// withRetry runs `attempt` until it succeeds, is refused by `shouldRetry`, or the attempt
// budget (`cfg.Max`, which is a TOTAL) is spent.
func withRetry(ctx context.Context, cfg retryConfig, stderr io.Writer, stepID string, attempt func() error, shouldRetry func(error) bool) error {
	var last error
	for i := int64(0); i < cfg.Max; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		last = attempt()
		if last == nil {
			return nil
		}
		if ctx.Err() != nil || !shouldRetry(last) {
			return last
		}
		if i+1 >= cfg.Max {
			break
		}
		d := cfg.delay(i)
		fmt.Fprintf(stderr, "[RETRY] Step '%s' failed (attempt %d/%d): %s. Retrying in %dms...\n", stepID, i+1, cfg.Max, last.Error(), d.Milliseconds())
		if err := sleepCtx(ctx, d); err != nil {
			return err
		}
	}
	return last
}

// retryable is upstream's non-retryable list minus the LLM-spend and pipeline-suspension
// errors, which this engine cannot produce: an external abort and a gate halt are the
// two that remain.
func retryable(err error, ctx context.Context) bool {
	if ctx.Err() != nil {
		return false
	}
	if errors.Is(err, errCancelled) {
		return false
	}
	if strings.Contains(err.Error(), "halted for approval") || strings.Contains(err.Error(), "sub-workflow halted for") {
		return false
	}
	return true
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// ---------------------------------------------------------------------------
// small helpers
// ---------------------------------------------------------------------------

// isTimeoutErr reports whether a step error came from the per-step timeout rather than
// from the command itself. The shell runner wraps a deadline in `timed out after …`, so
// that (plus a deadline-carrying context) is the signal.
func isTimeoutErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	return strings.Contains(err.Error(), "timed out after")
}

func timeoutMessage(step *params.LobsterStep) string {
	return fmt.Sprintf("Step '%s' timed out after %dms", step.Id, step.Timeout_ms)
}

// parseJSON is upstream's `parseJson`: a JSON document, or absent (never an error).
func parseJSON(stdout string) (any, bool) {
	trimmed := strings.TrimSpace(stdout)
	if trimmed == "" {
		return nil, false
	}
	var v any
	if err := json.Unmarshal([]byte(trimmed), &v); err != nil {
		return nil, false
	}
	return v, true
}

// toOutputItems is upstream's `toOutputItems`: json, else response, else stdout.
func toOutputItems(r *stepResult) []any {
	if r == nil {
		return nil
	}
	if r.HasJSON && r.JSON != nil {
		if arr, ok := r.JSON.([]any); ok {
			return arr
		}
		return []any{r.JSON}
	}
	if r.HasResp && r.Response != nil {
		if arr, ok := r.Response.([]any); ok {
			return arr
		}
		return []any{r.Response}
	}
	// Only the output ITEMS are trimmed: the run's output is the value a caller reads, and
	// a shell's trailing line ending is not part of it. `$id.stdout` keeps the bytes the
	// process wrote, verbatim, because that is the machine surface.
	if out := strings.TrimRight(r.Stdout, "\r\n"); out != "" {
		return []any{out}
	}
	return nil
}

func cloneResults(in results) results {
	out := make(results, len(in))
	for k, v := range in {
		cp := *v
		out[k] = &cp
	}
	return out
}

// createSyntheticStepResult is upstream's synthetic result for a step with no exec arm.
func createSyntheticStepResult(stepID string, value any) *stepResult {
	res := &stepResult{ID: stepID}
	if value == nil {
		return res
	}
	if s, ok := value.(string); ok {
		res.Stdout = s
		res.JSON, res.HasJSON = parseJSON(s)
		return res
	}
	res.Stdout = serializeValueForStdout(value)
	res.JSON, res.HasJSON = value, true
	return res
}

// serializeValueForStdout is upstream's serializer: strings raw, everything else JSON.
func serializeValueForStdout(value any) string {
	if value == nil {
		return ""
	}
	if s, ok := value.(string); ok {
		return s
	}
	b, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprintf("%v", value)
	}
	return string(b)
}

// findLastCompletedStepID walks back to the last step with a recorded result.
func findLastCompletedStepID(steps []params.LobsterStep, rs results) string {
	for i := len(steps) - 1; i >= 0; i-- {
		if _, ok := rs[steps[i].Id]; ok {
			return steps[i].Id
		}
	}
	return ""
}

// resolveWorkflowArgs is upstream's `resolveWorkflowArgs`: declared defaults first, then
// the caller's values override.
func resolveWorkflowArgs(file *params.LobsterFile, provided map[string]any, resume *resumeState) map[string]any {
	out := map[string]any{}
	for key, def := range file.Args {
		if def.Default != nil {
			out[key] = def.Default
		}
	}
	source := provided
	if len(source) == 0 && resume != nil && len(resume.Args) > 0 {
		source = resume.Args
	}
	for key, value := range source {
		out[key] = value
	}
	return out
}

// resolveWorkflowStepArgs is upstream's `resolveWorkflowStepArgs`: string values are
// templated against the PARENT's args and results; everything else passes through.
func resolveWorkflowStepArgs(stepArgs map[string]any, parentArgs map[string]any, rs results) map[string]any {
	out := map[string]any{}
	for key, value := range stepArgs {
		if s, ok := value.(string); ok {
			out[key] = resolveTemplate(s, parentArgs, rs)
			continue
		}
		out[key] = value
	}
	return out
}

// resolveCwd is upstream's `resolveCwd`: `${arg}` templating only, never a step ref.
func resolveCwd(cwd string, args map[string]any) string {
	if cwd == "" {
		return ""
	}
	return resolveArgsTemplate(cwd, args)
}

// environMap renders the process environment as the engine's base env.
func environMap() map[string]string {
	out := map[string]string{}
	for _, kv := range os.Environ() {
		if i := strings.IndexByte(kv, '='); i > 0 {
			out[kv[:i]] = kv[i+1:]
		}
	}
	return out
}

// normalizeArgEnvKey is upstream's `normalizeArgEnvKey`: uppercase, every run of
// non-[A-Z0-9] to `_`, then trim the `_` runs at both ends. An arg that normalizes to
// nothing exports no variable at all.
func normalizeArgEnvKey(key string) string {
	trimmed := strings.TrimSpace(key)
	if trimmed == "" {
		return ""
	}
	var b strings.Builder
	prevUnderscore := false
	for _, r := range strings.ToUpper(trimmed) {
		if (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			prevUnderscore = false
			continue
		}
		if !prevUnderscore {
			b.WriteByte('_')
			prevUnderscore = true
		}
	}
	return strings.Trim(b.String(), "_")
}

// mergeEnv is upstream's `mergeEnv`: the base env, then the injected
// LOBSTER_ARGS_JSON / LOBSTER_ARG_* defaults, then the workflow env block, then the step
// env block — each value template-resolved against the run's args and results.
func mergeEnv(base map[string]string, workflowEnv, stepEnv map[string]string, args map[string]any, rs results) map[string]string {
	env := make(map[string]string, len(base)+len(workflowEnv)+len(stepEnv)+8)
	for k, v := range base {
		env[k] = v
	}

	argsJSON, err := json.Marshal(args)
	if err != nil {
		argsJSON = []byte("{}")
	}
	env["LOBSTER_ARGS_JSON"] = string(argsJSON)
	for key, value := range args {
		normalized := normalizeArgEnvKey(key)
		if normalized == "" {
			continue
		}
		env["LOBSTER_ARG_"+normalized] = argEnvValue(value)
	}

	apply := func(source map[string]string) {
		for key, value := range source {
			env[key] = resolveTemplate(value, args, rs)
		}
	}
	apply(workflowEnv)
	apply(stepEnv)
	return env
}

// argEnvValue is JS `String(value)` for the arg env vars, except that a structured value
// is JSON-rendered rather than "[object Object]" — an object arg exported as
// "[object Object]" is a value no script can use, so the useful rendering wins here.
func argEnvValue(v any) string {
	if v == nil {
		return ""
	}
	return renderTemplateValue(v)
}
