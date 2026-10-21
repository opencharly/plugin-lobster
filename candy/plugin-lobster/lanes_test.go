package pluginlobster

// lanes_test.go — the POOLED fan-out arm, proved one property at a time.
//
// Every test here is written so that REMOVING the property makes it FAIL, and that is the
// only reason it exists. The four properties are the ones the arm was ruled to carry (the
// lane model's live-caught defects), plus the width, which is the arm's whole point:
//
//	1. a stop request cancels the run and KILLS its children
//	   -> TestLanesStopSignalKillsInFlightChildren
//	2. per-lane binding is a SCOPE, never process env
//	   -> TestLanesBindPerItemWithoutProcessEnv
//	3. ANY failed lane fails the step (the aggregate gate)
//	   -> TestLanesAnyFailedLaneFailsTheStep
//	4. the width is a POOL of `batch_size` lanes, not a spawn per item
//	   -> TestLanesPoolWidthBoundsConcurrency (width 1 AND width 3: the first fails a
//	      spawn-per-item implementation, the second fails a sequential one)
//
// Nothing here mocks the shell: the point of the arm is what it does to real concurrent
// processes, so the bodies are real commands and the assertions read what those commands
// left behind. `-race` on this file is the minimum proof (see the PR body).

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/opencharly/plugin-lobster/candy/plugin-lobster/params"
)

// fakeStopSignal stands in for an operator's Ctrl-C on the channel a test injects. It is
// deliberately NOT a syscall.Signal: stopSignalContext re-raises a REAL signal once the
// run returns (so the stop request is still answered), and a test must never take that
// path — re-raising SIGTERM would kill the test binary instead of failing the test.
type fakeStopSignal struct{}

func (fakeStopSignal) String() string { return "fake-stop" }
func (fakeStopSignal) Signal()        {}

// waitFor polls cond until it holds or the deadline passes.
func waitFor(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", d, what)
}

// appendCount counts the bytes a body appended to <dir>/<name>.
func appendCount(dir, name string) int {
	b, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		return 0
	}
	return len(b)
}

// lines reads <dir>/<name> as trimmed non-empty lines.
func lines(t *testing.T, dir, name string) []string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		return nil
	}
	var out []string
	for _, l := range strings.Split(string(b), "\n") {
		if l = strings.TrimSpace(l); l != "" {
			out = append(out, l)
		}
	}
	return out
}

// runAsync starts a run in the background and returns its result channel.
type runOutcome struct {
	res *runResult
	err error
}

func runAsync(eng *engine, path string, ctx context.Context) chan runOutcome {
	ch := make(chan runOutcome, 1)
	go func() {
		res, err := eng.runFile(ctx, path, nil, nil)
		ch <- runOutcome{res: res, err: err}
	}()
	return ch
}

// blockingLaneWorkflow is a fan-out whose lane bodies BLOCK until the test drops a
// `release` file. Reaching a given number of concurrent bodies is therefore observable
// directly, with no timing threshold: a lane that starts is a lane whose body is inside
// the wait loop.
func blockingLaneWorkflow(width int) string {
	return fmt.Sprintf(`
steps:
  - id: seed
    run: 'echo "[1,2,3,4]"'
  - id: fan
    lanes: $seed.json
    batch_size: %d
    steps:
      - id: body
        run: 'printf x >> "$LOB_DIR/started"; while [ ! -f "$LOB_DIR/release" ]; do sleep 0.02; done; printf "%%s\n" "$item.json" | tee -a "$LOB_DIR/finished"'
`, width)
}

// TestLanesPoolWidthBoundsConcurrency proves property 4: the width is a POOL, not a spawn
// per item, and not a sequential loop.
//
// The body blocks, so the number of bodies inside the wait loop IS the number of running
// lanes. Width 1 must show exactly 1 (a spawn-per-item implementation shows 4
// immediately); width 3 must show exactly 3 (a sequential implementation never exceeds 1
// and the wait times out). The second half of each case proves the released run produces
// every item, in ITEM order, whatever order the lanes finished in.
func TestLanesPoolWidthBoundsConcurrency(t *testing.T) {
	for _, width := range []int{1, 3} {
		t.Run(fmt.Sprintf("batch_size=%d", width), func(t *testing.T) {
			dir := t.TempDir()
			eng, _, _ := testEngine(t, map[string]string{"LOB_DIR": dir})
			path := writeWorkflow(t, blockingLaneWorkflow(width))

			done := runAsync(eng, path, context.Background())
			waitFor(t, 10*time.Second, fmt.Sprintf("%d concurrent lanes", width), func() bool {
				return appendCount(dir, "started") >= width
			})
			// The pool is full and every lane is blocked, so nothing else may start.
			time.Sleep(300 * time.Millisecond)
			if got := appendCount(dir, "started"); got != width {
				t.Fatalf("concurrent lanes = %d, want exactly %d: %d items must not become %d processes (the width is the pool)",
					got, width, 4, got)
			}

			if err := os.WriteFile(filepath.Join(dir, "release"), nil, 0o644); err != nil {
				t.Fatalf("release: %v", err)
			}
			select {
			case out := <-done:
				if out.err != nil {
					t.Fatalf("run: %v", out.err)
				}
				// The step's value is the per-item results in ITEM order — not completion
				// order — so a consumer's $fan.json.N is stable.
				want := []any{float64(1), float64(2), float64(3), float64(4)}
				if len(out.res.Output) != len(want) {
					t.Fatalf("output = %#v, want %#v", out.res.Output, want)
				}
				for i := range want {
					if out.res.Output[i] != want[i] {
						t.Fatalf("output[%d] = %#v, want %#v (item order, not completion order): %#v",
							i, out.res.Output[i], want[i], out.res.Output)
					}
				}
			case <-time.After(30 * time.Second):
				t.Fatal("run did not finish after every lane was released")
			}
			finished := lines(t, dir, "finished")
			sort.Strings(finished)
			if strings.Join(finished, ",") != "1,2,3,4" {
				t.Fatalf("finished items = %v, want 1,2,3,4 (every item must run exactly once)", finished)
			}
		})
	}
}

// TestLanesBindPerItemWithoutProcessEnv proves property 2: per-lane binding is a SCOPE,
// and concurrent lanes never share process env.
//
// Each lane's body is TWO sub-steps and the first one only sleeps: the second step's refs
// are resolved AFTER the lanes have all started, so a SHARED binding (one result set, or
// os.Setenv — the bug that was fixed) has every lane read whichever value was written
// last, and each lane writes the value it read keyed by its own index. The second
// assertion closes the other half: `pr` must not appear in the CHILD's environment at all,
// which is what makes the binding a per-lane scope rather than process state.
func TestLanesBindPerItemWithoutProcessEnv(t *testing.T) {
	dir := t.TempDir()
	eng, _, engErr := testEngine(t, map[string]string{"LOB_DIR": dir})
	path := writeWorkflow(t, `
steps:
  - id: seed
    run: 'echo "[10,20,30,40]"'
  - id: fan
    lanes: $seed.json
    batch_size: 4
    item_var: pr
    index_var: slot
    steps:
      - id: overlap
        run: 'sleep 0.2'
      - id: body
        run: 'printf "%s" "$pr.json" > "$LOB_DIR/seen.$slot.json"; printf "%s\n" "$(env | grep -c "^pr=")" >> "$LOB_DIR/envhits"'
`)
	out := runAsync(eng, path, context.Background())
	select {
	case r := <-out:
		if r.err != nil {
			t.Fatalf("run: %v", r.err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("run did not finish")
	}
	want := map[string]string{"0": "10", "1": "20", "2": "30", "3": "40"}
	for slot, item := range want {
		b, err := os.ReadFile(filepath.Join(dir, "seen."+slot))
		if err != nil {
			t.Fatalf("lane %s wrote nothing: %v (engine stderr: %s)", slot, err, engErr.String())
		}
		if got := strings.TrimSpace(string(b)); got != item {
			t.Fatalf("lane %s saw item %q, want %q: a concurrent lane read another lane's binding", slot, got, item)
		}
	}
	for _, n := range lines(t, dir, "envhits") {
		if n != "0" {
			t.Fatalf("a lane's child environment carried %s variable(s) named pr; the item binding must be a SCOPED REF, never process env", n)
		}
	}
	if len(lines(t, dir, "envhits")) != 4 {
		t.Fatalf("env probe ran %d times, want 4", len(lines(t, dir, "envhits")))
	}
}

// TestLanesAnyFailedLaneFailsTheStep proves property 3: the step's gate is the AGGREGATE,
// so one failed lane fails the step and the run stops. A pool that collects results but
// swallows a lane's error would return success here and run the next step.
func TestLanesAnyFailedLaneFailsTheStep(t *testing.T) {
	dir := t.TempDir()
	eng, _, _ := testEngine(t, map[string]string{"LOB_DIR": dir})
	path := writeWorkflow(t, `
steps:
  - id: seed
    run: 'echo "[1,2,3,4]"'
  - id: fan
    lanes: $seed.json
    batch_size: 2
    steps:
      - id: body
        run: 'n=$item.json; if [ "$n" = "2" ]; then echo boom >&2; exit 3; fi; printf "%s" "$n"'
  - id: after
    run: 'printf RAN > "$LOB_DIR/after.marker"'
`)
	_, err := eng.runFile(context.Background(), path, nil, nil)
	if err == nil {
		t.Fatal("run succeeded although lane 1 failed: a failed lane must fail the step (ANY failed lane)")
	}
	// The failing lane is named by INDEX, and the LOWEST failing index wins so the message
	// does not depend on which lane happened to finish first.
	if !strings.Contains(err.Error(), "lane 1") {
		t.Fatalf("error = %q, want it to name lane 1 (the failing item's index)", err.Error())
	}
	if _, statErr := os.Stat(filepath.Join(dir, "after.marker")); statErr == nil {
		t.Fatal("the step after the failed fan-out ran: a failed lane must stop the run")
	}
}

// TestLanesStopSignalKillsInFlightChildren proves property 1: a stop request for the
// engine reaches the run, and the run KILLS its children rather than leaving them behind.
//
// The children record their own PIDs and then sleep far longer than the test. After the
// signal, the run must return an error promptly AND every recorded PID must be gone —
// being merely abandoned would leave the sleep running and the PID alive, which is exactly
// the orphan that holds a per-bed flock and makes the next run die on "already running".
func TestLanesStopSignalKillsInFlightChildren(t *testing.T) {
	dir := t.TempDir()
	eng, _, _ := testEngine(t, map[string]string{"LOB_DIR": dir})
	path := writeWorkflow(t, `
steps:
  - id: seed
    run: 'echo "[1,2]"'
  - id: fan
    lanes: $seed.json
    batch_size: 2
    steps:
      - id: body
        run: 'printf "%s\n" $$ >> "$LOB_DIR/pids"; exec sleep 120'
`)

	sigs := make(chan os.Signal, 1)
	ctx, release := stopSignalContext(context.Background(), sigs)
	defer release()

	done := runAsync(eng, path, ctx)
	waitFor(t, 10*time.Second, "both lanes to be in their shell", func() bool {
		return len(lines(t, dir, "pids")) == 2
	})

	sigs <- fakeStopSignal{}
	select {
	case out := <-done:
		if out.err == nil {
			t.Fatal("run returned success after a stop signal")
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the run did not return after a stop signal: the cancellation never reached the lanes")
	}

	pids := lines(t, dir, "pids")
	if len(pids) != 2 {
		t.Fatalf("recorded %d lane pids, want 2", len(pids))
	}
	for _, pid := range pids {
		var n int
		if _, err := fmt.Sscanf(pid, "%d", &n); err != nil {
			t.Fatalf("pid %q: %v", pid, err)
		}
		waitFor(t, 10*time.Second, fmt.Sprintf("child pid %d to be killed", n), func() bool {
			return syscall.Kill(n, 0) == syscall.ESRCH
		})
	}
}

// TestLoaderRejectsBrokenLanesSteps proves the loader refuses the shapes that would make
// the arm lie: a missing width (a default would be a number the author never wrote, and an
// unbounded pool is the spawn-per-item the arm replaces), a sequential-loop pacing knob,
// two arms at once, an empty body, and a nested fan-out inside a lane.
func TestLoaderRejectsBrokenLanesSteps(t *testing.T) {
	cases := []struct {
		name string
		body string
		want string
	}{
		{
			name: "missing width",
			body: `
steps:
  - id: fan
    lanes: $seed.json
    steps:
      - id: body
        run: 'printf ok'
`,
			want: "lanes requires batch_size",
		},
		{
			name: "pause is not a lane knob",
			body: `
steps:
  - id: fan
    lanes: $seed.json
    batch_size: 2
    pause_ms: 10
    steps:
      - id: body
        run: 'printf ok'
`,
			want: "lanes cannot define pause_ms",
		},
		{
			name: "two arms",
			body: `
steps:
  - id: fan
    lanes: $seed.json
    for_each: $seed.json
    batch_size: 2
    steps:
      - id: body
        run: 'printf ok'
`,
			want: "can only define one of",
		},
		{
			name: "empty body",
			body: `
steps:
  - id: fan
    lanes: $seed.json
    batch_size: 2
`,
			want: "lanes requires a non-empty steps array",
		},
		{
			name: "nested fan-out",
			body: `
steps:
  - id: fan
    lanes: $seed.json
    batch_size: 2
    steps:
      - id: inner
        for_each: $seed.json
        steps:
          - id: body
            run: 'printf ok'
`,
			want: "cannot define workflow, parallel, for_each, or lanes",
		},
		{
			name: "operand must be an expression",
			body: `
steps:
  - id: fan
    lanes: 4
    batch_size: 2
    steps:
      - id: body
        run: 'printf ok'
`,
			want: "lanes must be a string",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := writeWorkflow(t, "name: broken\n"+tc.body)
			if _, err := loadWorkflowFile(path); err == nil {
				t.Fatalf("loader accepted a broken lanes step; want %q", tc.want)
			} else if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %q, want it to contain %q", err.Error(), tc.want)
			}
		})
	}
}

// TestDryRunNamesTheLanesArm proves the arm is visible to `--dry-run`: a plan that lists a
// fan-out step as something else is a plan that cannot be read.
func TestDryRunNamesTheLanesArm(t *testing.T) {
	if got := describeArm(&params.LobsterStep{Lanes: "$seed.json"}); got != "lanes" {
		t.Fatalf("describeArm(lanes) = %q, want lanes", got)
	}
	// The two arms must stay distinguishable: a lanes step must never be described as the
	// sequential arm it shares a loop driver with.
	if got := describeArm(&params.LobsterStep{For_each: "$seed.json"}); got != "for_each" {
		t.Fatalf("describeArm(for_each) = %q, want for_each", got)
	}
}

// TestStopSignalContextCancelsOnce pins the bridge's contract: the first signal cancels
// the run context, and release is idempotent (a double release must not panic on the
// closed channel or re-raise twice).
func TestStopSignalContextCancelsOnce(t *testing.T) {
	sigs := make(chan os.Signal, 1)
	ctx, release := stopSignalContext(context.Background(), sigs)
	sigs <- fakeStopSignal{}
	waitFor(t, 5*time.Second, "the run context to be cancelled", func() bool { return ctx.Err() != nil })
	release()
	release()

	// A run that finishes on its own must still be released cleanly, with no goroutine
	// left waiting on a signal that will never come.
	ctx2, release2 := stopSignalContext(context.Background(), make(chan os.Signal))
	if err := ctx2.Err(); err != nil {
		t.Fatalf("a fresh run context is already cancelled: %v", err)
	}
	release2()
	if ctx2.Err() == nil {
		t.Fatal("release must cancel the released context (nothing may use it afterwards)")
	}
}

// TestLanesPoolUnderLoad drives the pool at a width above one worker per CPU with a body
// long enough that the lanes genuinely overlap, and asserts the invariants that a race or
// a shared slot would break: every item runs EXACTLY once, the value each lane read is its
// own, and the step's array is in item order. It is the test to run under `-race` with
// `-count` (see the PR body's evidence).
func TestLanesPoolUnderLoad(t *testing.T) {
	const items = 32
	const width = 8
	dir := t.TempDir()
	eng, _, _ := testEngine(t, map[string]string{"LOB_DIR": dir})
	path := writeWorkflow(t, fmt.Sprintf(`
steps:
  - id: seed
    run: 'seq -s, 1 %d | sed "s/^/[/;s/$/]/"' 
  - id: fan
    lanes: $seed.json
    batch_size: %d
    steps:
      - id: body
        run: 'printf "%%s\n" "$item.json" >> "$LOB_DIR/ran"; for i in 1 2 3 4 5; do :; done; printf "%%s" "$item.json"'
`, items, width))
	res, err := eng.runFile(context.Background(), path, nil, nil)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(res.Output) != items {
		t.Fatalf("output has %d items, want %d: %#v", len(res.Output), items, res.Output)
	}
	for i := 0; i < items; i++ {
		if got := res.Output[i]; got != float64(i+1) {
			t.Fatalf("output[%d] = %#v, want %d (item order)", i, got, i+1)
		}
	}
	ran := lines(t, dir, "ran")
	if len(ran) != items {
		t.Fatalf("%d bodies ran, want exactly %d (each item once)", len(ran), items)
	}
	seen := map[string]int{}
	for _, n := range ran {
		seen[n]++
	}
	for i := 1; i <= items; i++ {
		key := fmt.Sprintf("%d", i)
		if seen[key] != 1 {
			t.Fatalf("item %s ran %d times, want exactly once (a shared slot loses or duplicates items)", key, seen[key])
		}
	}
}
