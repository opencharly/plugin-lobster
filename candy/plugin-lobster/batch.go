package pluginlobster

// batch.go — the three fan-out arms: `parallel`, `for_each`, and `lanes`.
//
// `parallel` and `for_each` are ports of upstream `file.ts`'s handling. The parts worth
// stating, because they are the ones a re-implementation gets wrong:
//
//   - a parallel step's BRANCH results are promoted to TOP-LEVEL refs (`$branch.field`),
//     and the step's OWN result is the aggregate of the branches;
//   - `wait: any` returns the FIRST branch to complete and cancels the rest — it does not
//     wait for the fastest SUCCESS;
//   - a loop's `item`/`index` are REFS (synthetic step results), never env vars, so
//     `${item}` does not exist and `$item.json` does;
//   - a loop's output is the array of each iteration's LAST sub-step output, in ITERATION
//     order, whatever the width was.
//
// `lanes` is the charly-only third arm: a POOLED fan-out over a runtime collection, which
// is what neither of the other two can express (`parallel` branches are static in the
// file; `for_each` runs a runtime collection in order by upstream's own semantics). It
// shares for_each's loop driver — runLoop/runLoopItem — so the two arms cannot drift, and
// differs only in the WIDTH it passes; the properties it must carry are stated on
// runLanes.

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

// runForEach runs a step's sub-steps once per item of the resolved collection, IN ORDER —
// upstream lobster's own semantics, which is why this arm runs the collection one item at
// a time (see the `batch_size` note in runLoop).
func (st *runState) runForEach(ctx context.Context, step *params.LobsterStep, env map[string]string, cwd string) (*stepResult, error) {
	items, err := resolveLoopItems(step, step.For_each, "for_each", st.args, st.results)
	if err != nil {
		return nil, err
	}
	return st.runLoop(ctx, step, items, env, cwd, 1)
}

// ---------------------------------------------------------------------------
// lanes — the POOLED fan-out arm
// ---------------------------------------------------------------------------

// runLanes runs a step's sub-steps once per item of the resolved collection through a POOL
// of `batch_size` lanes. The arm itself is documented on the schema (`lanes:` on
// #LobsterStep / #LobsterLanes in schema/lobster.cue), which is where an author looks.
//
// Four properties this arm exists to carry, each of which a re-implementation can lose:
//
//  1. CANCELLATION REACHES THE CHILDREN. Every lane's step runs under the run's context,
//     and the shell arm's `exec.CommandContext` turns that into a killed process group.
//     So a cancelled run (Ctrl-C on the engine, a killed plugin, a deadline) kills the
//     lane's children instead of orphaning them — the old batch drive let a child keep a
//     per-bed flock while the engine that owned it was gone, and every later lane died on
//     "bed already running". A first failing lane also cancels the rest, so a failure is
//     not paid for by running the remaining items.
//  2. PER-LANE BINDING IS A SCOPE, NEVER PROCESS ENV. `item`/`index` are installed as
//     synthetic step RESULTS in a per-lane COPY of the result set (`results.scoped`), and
//     the lane's env is a per-lane map. Nothing calls os.Setenv: process env is shared by
//     concurrent lanes, and concurrent lanes sharing process env is the bug that was
//     fixed (RCA 2026.252.2210).
//  3. THE STEP'S GATE IS THE AGGREGATE. ANY failed lane fails the step, so a run whose
//     every lane failed cannot read as success. The predecessor returned 0
//     unconditionally, and that was live-caught.
//  4. WIDTH IS A POOL, NOT A SPAWN PER ITEM. `batch_size` workers pull indices from one
//     channel, so item N starts only when a lane frees up — width is an authored knob
//     (`params.LobsterStep.Batch_size`), not a constant, and the loader refuses a lanes
//     step without it (lobster.go) instead of inventing a default.
//
// The loop's value is the ARRAY of per-item results in ITEM order, so a consumer's
// `$id.json.N` does not depend on which lane finished first.
func (st *runState) runLanes(ctx context.Context, step *params.LobsterStep, env map[string]string, cwd string) (*stepResult, error) {
	width := int(step.Batch_size)
	if width < 1 {
		return nil, fmt.Errorf("Workflow step %s lanes requires batch_size >= 1 (the pool width)", step.Id)
	}
	items, err := resolveLoopItems(step, step.Lanes, "lanes", st.args, st.results)
	if err != nil {
		return nil, err
	}
	return st.runLoop(ctx, step, items, env, cwd, width)
}

// runLoop is the ONE loop driver both fan-out arms share (R2): `width` 1 is the
// sequential upstream-faithful loop, `width` > 1 is the lane pool. `item`/`index` binding,
// the body, and the ordered output array are identical in both, so the arms cannot drift.
//
// `batch_size` is NOT consulted here for for_each: upstream treats it as a scheduling hint
// that only decides when `pause_ms` fires, so this engine runs those items in order —
// which yields the identical observable result with a stronger ordering guarantee — and
// deliberately does not let the hint pace the loop. The caller passes the width.
func (st *runState) runLoop(ctx context.Context, step *params.LobsterStep, items []any, env map[string]string, cwd string, width int) (*stepResult, error) {
	itemVar := strings.TrimSpace(step.Item_var)
	if itemVar == "" {
		itemVar = "item"
	}
	indexVar := strings.TrimSpace(step.Index_var)
	if indexVar == "" {
		indexVar = "index"
	}
	var outputs []any
	var err error
	if width <= 1 {
		outputs, err = st.runLoopInOrder(ctx, step, items, itemVar, indexVar, env, cwd)
	} else {
		outputs, err = st.runLoopPooled(ctx, step, items, itemVar, indexVar, env, cwd, width)
	}
	if err != nil {
		return nil, err
	}
	return &stepResult{LobsterStepResult: params.LobsterStepResult{Id: step.Id, Json: outputs}, HasJSON: true}, nil
}

// runLoopInOrder is the sequential loop: one item at a time, `pause_ms` between
// iterations, fail-fast.
func (st *runState) runLoopInOrder(ctx context.Context, step *params.LobsterStep, items []any, itemVar, indexVar string, env map[string]string, cwd string) ([]any, error) {
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
		out, err := st.runLoopItem(ctx, step, item, i, itemVar, indexVar, env, cwd)
		if err != nil {
			return nil, err
		}
		outputs = append(outputs, out)
	}
	return outputs, nil
}

// runLoopPooled is the lane pool: EXACTLY `width` workers (never one goroutine per item),
// each pulling the next index and writing its own slot. A failure cancels the pool, and
// the LOWEST failing index is the one reported, so the message does not depend on the
// order the lanes happened to finish in.
func (st *runState) runLoopPooled(ctx context.Context, step *params.LobsterStep, items []any, itemVar, indexVar string, env map[string]string, cwd string, width int) ([]any, error) {
	if width > len(items) {
		// A pool can never be wider than the work: an idle worker would be a goroutine
		// that can only exit, and `width` is a bound, not a target.
		width = len(items)
	}
	outputs := make([]any, len(items))
	failures := make([]error, len(items))

	poolCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan int)
	var wg sync.WaitGroup
	for w := 0; w < width; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				if poolCtx.Err() != nil {
					return
				}
				out, err := st.runLoopItem(poolCtx, step, items[i], i, itemVar, indexVar, env, cwd)
				if err != nil {
					// Only a failure that is NOT the pool's own cancellation is this lane's
					// to report: a sibling's failure cancels the rest, and those lanes must
					// not overwrite the reason with "context canceled".
					if poolCtx.Err() == nil {
						failures[i] = err
					}
					cancel()
					return
				}
				outputs[i] = out
			}
		}()
	}

feed:
	for i := range items {
		select {
		case jobs <- i:
		case <-poolCtx.Done():
			break feed
		}
	}
	close(jobs)
	wg.Wait()

	// The aggregate gate: ANY failed lane fails the step.
	for i, err := range failures {
		if err != nil {
			return nil, fmt.Errorf("Workflow step %s lane %d: %w", step.Id, i, err)
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(items) == 0 {
		return []any{}, nil
	}
	return outputs, nil
}

// runLoopItem runs one iteration of either loop arm: the body against a SCOPE carrying this
// item's `item`/`index` refs. The scope is a per-item copy, which is what makes the pooled
// arm safe: nothing an iteration writes can reach another lane.
func (st *runState) runLoopItem(ctx context.Context, step *params.LobsterStep, item any, i int, itemVar, indexVar string, env map[string]string, cwd string) (any, error) {
	scope := st.results.scoped(map[string]*stepResult{
		itemVar:  createSyntheticStepResult(itemVar, item),
		indexVar: createSyntheticStepResult(indexVar, float64(i)),
	})
	return st.runSubSteps(ctx, step, step.Steps, scope, env, cwd)
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

// resolveLoopItems resolves a loop arm's operand (`for_each:` or `lanes:`). A whole-string
// step reference yields that step's value directly; anything else is templated and parsed
// as JSON, because a loop must iterate a COLLECTION and a bare string is not one.
func resolveLoopItems(step *params.LobsterStep, expr, arm string, args map[string]any, rs results) ([]any, error) {
	value, err := resolveInputValue(expr, args, rs)
	if err != nil {
		return nil, err
	}
	switch v := value.(type) {
	case []any:
		return v, nil
	case nil:
		return nil, fmt.Errorf("Workflow step %s %s resolved to nothing", step.Id, arm)
	case string:
		var parsed any
		if err := json.Unmarshal([]byte(strings.TrimSpace(v)), &parsed); err == nil {
			if arr, ok := parsed.([]any); ok {
				return arr, nil
			}
		}
		return nil, fmt.Errorf("Workflow step %s %s must resolve to an array", step.Id, arm)
	default:
		return nil, fmt.Errorf("Workflow step %s %s must resolve to an array", step.Id, arm)
	}
}
