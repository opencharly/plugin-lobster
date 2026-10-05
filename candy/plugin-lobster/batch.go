package pluginlobster

// batch.go — the two fan-out arms: `parallel` and `for_each`.
//
// These are ports of upstream `file.ts`'s parallel/for_each handling. The parts worth
// stating, because they are the ones a re-implementation gets wrong:
//
//   - a parallel step's BRANCH results are promoted to TOP-LEVEL refs (`$branch.field`),
//     and the step's OWN result is the aggregate of the branches;
//   - `wait: any` returns the FIRST branch to complete and cancels the rest — it does not
//     wait for the fastest SUCCESS;
//   - a for_each loop's `item`/`index` are REFS (synthetic step results), never env
//     vars, so `${item}` does not exist and `$item.json` does;
//   - the loop's output is the array of each iteration's LAST sub-step output, in
//     iteration order, whatever the batch size was.

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/opencharly/plugin-lobster/candy/plugin-lobster/params"
)

// ---------------------------------------------------------------------------
// parallel
// ---------------------------------------------------------------------------

// runParallel runs a step's branches concurrently. The returned map carries an entry per
// branch id PLUS an entry under the step's own id holding the aggregate — the caller
// promotes the branches and keeps the aggregate as the step result.
func (st *runState) runParallel(ctx context.Context, step *params.LobsterStep, env map[string]string, cwd string) (map[string]*stepResult, error) {
	par := step.Parallel
	branches := par.Branches
	if len(branches) == 0 {
		return nil, fmt.Errorf("Workflow step %s parallel requires a non-empty branches array", step.Id)
	}
	wait := strings.TrimSpace(par.Wait)
	if wait == "" {
		wait = "all"
	}

	runCtx := ctx
	var cancel context.CancelFunc
	if par.Timeout_ms > 0 {
		runCtx, cancel = context.WithTimeout(ctx, time.Duration(par.Timeout_ms)*time.Millisecond)
		defer cancel()
	}

	// `wait: any` cancels the losers the moment the winner lands, so the cancel has to be
	// reachable from the collector as well as from the timeout above.
	inner, innerCancel := context.WithCancel(runCtx)
	defer innerCancel()

	type outcome struct {
		id  string
		res *stepResult
		err error
	}
	done := make(chan outcome, len(branches))
	var wg sync.WaitGroup

	for _, b := range branches {
		branch := b
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := st.runBranch(inner, step, branch)
			select {
			case done <- outcome{id: branch.Id, res: res, err: err}:
			case <-inner.Done():
			}
		}()
	}

	go func() {
		wg.Wait()
		close(done)
	}()

	collected := map[string]*stepResult{}
	var firstErr error
	count := 0

	for o := range done {
		count++
		if o.err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("Workflow step %s branch %s: %w", step.Id, o.id, o.err)
			}
		} else {
			collected[o.id] = o.res
		}
		if wait == "any" {
			// The FIRST branch to complete decides, success or failure.
			innerCancel()
			break
		}
	}
	if wait == "any" && count == 1 {
		// Drain the rest so the cancelled goroutines do not block on the send.
		go func() {
			for range done {
			}
		}()
	}

	if err := runCtx.Err(); err != nil && firstErr == nil {
		firstErr = err
	}
	if wait == "all" {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if count < len(branches) && firstErr == nil {
			firstErr = fmt.Errorf("Workflow step %s parallel did not complete every branch", step.Id)
		}
	}
	if firstErr != nil {
		return nil, firstErr
	}

	aggregate := map[string]any{}
	for _, b := range branches {
		if res, ok := collected[b.Id]; ok {
			aggregate[b.Id] = res.Json
		}
	}
	collected[step.Id] = &stepResult{LobsterStepResult: params.LobsterStepResult{Id: step.Id, Json: aggregate}, HasJSON: true}
	return collected, nil
}

// runBranch runs one parallel branch as if it were a step of its own.
func (st *runState) runBranch(ctx context.Context, step *params.LobsterStep, b params.LobsterBranch) (*stepResult, error) {
	bs := &params.LobsterStep{
		Id:       b.Id,
		Run:      b.Run,
		Command:  b.Command,
		Pipeline: b.Pipeline,
		Env:      b.Env,
		Cwd:      b.Cwd,
		Stdin:    b.Stdin,
	}
	kind, value := getStepExecution(bs)
	if kind == kindNone {
		return nil, fmt.Errorf("Workflow step %s parallel branch %s has no exec arm", step.Id, b.Id)
	}
	env := mergeEnv(st.eng.env, st.file.Env, bs.Env, st.args, st.results)
	cwd := st.eng.cwd
	if resolved := resolveCwd(bs.Cwd, st.args); resolved != "" {
		cwd = resolved
	} else if resolved := resolveCwd(st.file.Cwd, st.args); resolved != "" {
		cwd = resolved
	}
	res, _, err := st.attemptStep(ctx, bs, kind, value, env, cwd, st.results)
	return res, err
}

// ---------------------------------------------------------------------------
// for_each
// ---------------------------------------------------------------------------

// runForEach runs a step's sub-steps once per item of the resolved collection.
func (st *runState) runForEach(ctx context.Context, step *params.LobsterStep, env map[string]string, cwd string) (*stepResult, error) {
	items, err := resolveForEachItems(step, st.args, st.results)
	if err != nil {
		return nil, err
	}

	itemVar := strings.TrimSpace(step.Item_var)
	if itemVar == "" {
		itemVar = "item"
	}
	indexVar := strings.TrimSpace(step.Index_var)
	if indexVar == "" {
		indexVar = "index"
	}
	pause := time.Duration(0)
	if f, ok := toFloat(step.Pause_ms); ok && f > 0 {
		pause = time.Duration(f) * time.Millisecond
	}

	outputs := make([]any, 0, len(items))
	for i, item := range items {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if i > 0 && pause > 0 {
			if err := sleepCtx(ctx, pause); err != nil {
				return nil, err
			}
		}

		scope := st.results.scoped(map[string]*stepResult{
			itemVar:  createSyntheticStepResult(itemVar, item),
			indexVar: createSyntheticStepResult(indexVar, float64(i)),
		})
		out, err := st.runSubSteps(ctx, step, step.Steps, scope, env, cwd)
		if err != nil {
			return nil, err
		}
		outputs = append(outputs, out)
	}

	// `batch_size` is a scheduling hint (how many iterations a caller may run at once). This
	// engine runs the items in order, which yields the identical observable result with a
	// stronger ordering guarantee, so the hint is accepted and validated — the loader
	// rejects a `batch_size` below 1 (`lobster.go`) — and deliberately not used to pace the
	// loop. The steps assert the ordering, not the hint.

	return &stepResult{LobsterStepResult: params.LobsterStepResult{Id: step.Id, Json: outputs}, HasJSON: true}, nil
}

// runSubSteps runs a loop body's sub-steps in order against a scoped result set,
// returning the LAST completed sub-step's value — upstream's loop output.
func (st *runState) runSubSteps(ctx context.Context, loop *params.LobsterStep, subs []*params.LobsterStep, scope results, env map[string]string, cwd string) (any, error) {
	var last any
	for _, sub := range subs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		ok, err := evaluateWhen(sub, scope)
		if err != nil {
			return nil, fmt.Errorf("Workflow step %s sub-step %s when: %w", loop.Id, sub.Id, err)
		}
		if !ok {
			scope[sub.Id] = &stepResult{LobsterStepResult: params.LobsterStepResult{Id: sub.Id, Skipped: true}}
			continue
		}

		subEnv := mergeEnv(env, nil, sub.Env, st.args, scope)
		subCwd := cwd
		if resolved := resolveCwd(sub.Cwd, st.args); resolved != "" {
			subCwd = resolved
		}

		kind, value := getStepExecution(sub)
		res, _, err := st.attemptStep(ctx, sub, kind, value, subEnv, subCwd, scope)
		if err != nil {
			// A sub-step's `on_error` is the loop's to honour; upstream fails the loop.
			// Anything else would silently discard a loop body's failure.
			return nil, fmt.Errorf("Workflow step %s sub-step %s: %w", loop.Id, sub.Id, err)
		}
		scope[sub.Id] = res
		if res.HasJSON {
			last = res.Json
		} else {
			last = res.Stdout
		}
	}
	return last, nil
}

// resolveForEachItems resolves the loop operand. A whole-string step reference yields
// that step's value directly; anything else is templated and parsed as JSON, because a
// loop must iterate a COLLECTION and a bare string is not one.
func resolveForEachItems(step *params.LobsterStep, args map[string]any, rs results) ([]any, error) {
	value, err := resolveInputValue(step.For_each, args, rs)
	if err != nil {
		return nil, err
	}
	switch v := value.(type) {
	case []any:
		return v, nil
	case nil:
		return nil, fmt.Errorf("Workflow step %s for_each resolved to nothing", step.Id)
	case string:
		var parsed any
		if err := json.Unmarshal([]byte(strings.TrimSpace(v)), &parsed); err == nil {
			if arr, ok := parsed.([]any); ok {
				return arr, nil
			}
		}
		return nil, fmt.Errorf("Workflow step %s for_each must resolve to an array", step.Id)
	default:
		return nil, fmt.Errorf("Workflow step %s for_each must resolve to an array", step.Id)
	}
}
