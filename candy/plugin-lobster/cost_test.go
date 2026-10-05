package pluginlobster

// cost_test.go — the `cost_limit` ceiling and the ledger behind it.
//
// This engine spends nothing of its own: the `pipeline:` stdlib refuses the LLM stages
// (`externalStages` maps them to `agent:`/`input:` steps, and `runPipelineStep` returns
// "not supported by the lobster engine" for each). What it does still do is READ the
// `_meta.cost` block a step emits — a charly step's own accounting, produced by whatever
// plugin ran it — and enforce the workflow's `cost_limit` on the total. That read and that
// ceiling are the whole of cost.go's behaviour, so they are what these tests pin: what is
// recorded, what is deliberately ignored, and what each `action` does AT the boundary.
//
// Every case is in-process. No external service is crossed, so nothing here is skipped,
// stubbed or faked — the tracker's inputs are the decoded JSON a real run hands it.

import (
	"bytes"
	"strings"
	"testing"

	"github.com/opencharly/plugin-lobster/candy/plugin-lobster/params"
)

// costStep builds the stepResult a completed step returns when it emitted `_meta.cost`.
func costStep(t *testing.T, body map[string]any) *stepResult {
	t.Helper()
	return &stepResult{LobsterStepResult: params.LobsterStepResult{Id: "s", Json: body}, HasJSON: true}
}

// TestCostTrackerRecordsWhatTheStepReported pins the happy path: model, both token
// counts and the dollar figure are read off `_meta.cost`, the entry is kept per step, and
// the run total is their sum.
func TestCostTrackerRecordsWhatTheStepReported(t *testing.T) {
	var stderr bytes.Buffer
	ct := newCostTracker(params.LobsterCostLimit{}, &stderr)

	ct.track("first", costStep(t, map[string]any{"_meta": map[string]any{"cost": map[string]any{
		"totalInputTokens":  float64(1200),
		"totalOutputTokens": float64(300),
		"estimatedCostUsd":  0.25,
		"byStep": []any{map[string]any{
			"stepId":       "first",
			"model":        "some-model",
			"inputTokens":  float64(1200),
			"outputTokens": float64(300),
			"costUsd":      0.25,
		}},
	}}}))
	ct.track("second", costStep(t, map[string]any{"_meta": map[string]any{"cost": map[string]any{
		"estimatedCostUsd": 0.5,
	}}}))

	s := ct.summary()
	if s == nil {
		t.Fatal("summary() = nil after two tracked steps; want a ledger")
	}
	if len(s.ByStep) != 2 {
		t.Fatalf("ByStep has %d entries; want 2", len(s.ByStep))
	}
	if s.ByStep[0].StepID != "first" || s.ByStep[1].StepID != "second" {
		t.Fatalf("ByStep ids = %q, %q; want first, second (insertion order)", s.ByStep[0].StepID, s.ByStep[1].StepID)
	}
	if s.ByStep[0].Model != "some-model" {
		t.Errorf("ByStep[0].Model = %q; want some-model", s.ByStep[0].Model)
	}
	if s.ByStep[0].InputTokens != 1200 || s.ByStep[0].OutputTokens != 300 {
		t.Errorf("ByStep[0] tokens = %d/%d; want 1200/300", s.ByStep[0].InputTokens, s.ByStep[0].OutputTokens)
	}
	if s.EstimatedCostUSD != 0.75 {
		t.Errorf("EstimatedCostUSD = %v; want 0.75", s.EstimatedCostUSD)
	}
	if s.TotalInputTokens != 1200 || s.TotalOutputTokens != 300 {
		t.Errorf("totals = %d/%d; want 1200/300", s.TotalInputTokens, s.TotalOutputTokens)
	}
	if stderr.Len() != 0 {
		t.Errorf("stderr = %q; want empty (no limit was set)", stderr.String())
	}
}

// TestCostTrackerIgnoresAStepWithoutACostBlock is the negative control for the read: a
// step that reported nothing, reported non-object JSON, or reported a `_meta` without a
// `cost` must contribute NO entry and NO dollars. Without this, the ledger would be an
// estimate rather than a record, and a ceiling computed from it would be fiction.
func TestCostTrackerIgnoresAStepWithoutACostBlock(t *testing.T) {
	cases := []struct {
		name string
		r    *stepResult
	}{
		{"nil result", nil},
		{"no JSON at all", &stepResult{LobsterStepResult: params.LobsterStepResult{Id: "s"}}},
		{"JSON that is a list, not an object", &stepResult{LobsterStepResult: params.LobsterStepResult{Id: "s", Json: []any{1, 2}}, HasJSON: true}},
		{"object with no _meta", costStep(t, map[string]any{"ok": true})},
		{"_meta that is not an object", &stepResult{LobsterStepResult: params.LobsterStepResult{Id: "s", Json: map[string]any{"_meta": "nope"}}, HasJSON: true}},
		{"_meta with no cost", costStep(t, map[string]any{"_meta": map[string]any{"other": 1}})},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ct := newCostTracker(params.LobsterCostLimit{}, nil)
			ct.track("s", tc.r)
			if len(ct.byStep) != 0 {
				t.Errorf("byStep has %d entries; want 0", len(ct.byStep))
			}
			if ct.totalUSD != 0 {
				t.Errorf("totalUSD = %v; want 0", ct.totalUSD)
			}
			if s := ct.summary(); s != nil {
				t.Errorf("summary() = %+v; want nil — nothing was accounted", s)
			}
		})
	}
}

// TestCostCheckLimitActions pins the ceiling itself. The action is upstream's: `stop`
// fails the run with BOTH figures named, `warn` prints once and continues, and an absent
// `action` defaults to `warn`. The comparison is on the total against `max_usd`, and a
// total exactly AT the cap is not over it.
func TestCostCheckLimitActions(t *testing.T) {
	cases := []struct {
		name      string
		maxUSD    any
		action    string
		spend     float64
		wantErr   bool
		wantWarn  bool
		errSubstr []string
	}{
		{
			name: "no max_usd is no limit", maxUSD: nil, action: "stop", spend: 99,
		},
		{
			name: "under the cap passes", maxUSD: 1.0, action: "stop", spend: 0.5,
		},
		{
			name: "exactly at the cap is not over it", maxUSD: 1.0, action: "stop", spend: 1.0,
		},
		{
			name: "over the cap with stop fails", maxUSD: 1.0, action: "stop", spend: 1.25,
			wantErr: true, errSubstr: []string{"$1.2500", "$1.0000", "cost limit exceeded"},
		},
		{
			name: "over the cap with warn prints and continues", maxUSD: 1.0, action: "warn", spend: 1.5,
			wantWarn: true,
		},
		{
			name: "an empty action defaults to warn", maxUSD: 1.0, action: "", spend: 1.5,
			wantWarn: true,
		},
		{
			name: "max_usd as a float64 off the wire", maxUSD: float64(2), action: "stop", spend: 2.5,
			wantErr: true, errSubstr: []string{"$2.5000", "$2.0000"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var stderr bytes.Buffer
			ct := newCostTracker(params.LobsterCostLimit{Max_usd: tc.maxUSD, Action: tc.action}, &stderr)
			ct.totalUSD = tc.spend

			err := ct.checkLimit()
			if tc.wantErr {
				if err == nil {
					t.Fatalf("checkLimit() = nil; want an error (spend %v > max %v, action %q)", tc.spend, tc.maxUSD, tc.action)
				}
				for _, sub := range tc.errSubstr {
					if !strings.Contains(err.Error(), sub) {
						t.Errorf("error %q does not name %q", err.Error(), sub)
					}
				}
			} else if err != nil {
				t.Fatalf("checkLimit() = %v; want nil", err)
			}

			if tc.wantWarn {
				if !strings.Contains(stderr.String(), "[COST]") {
					t.Errorf("stderr = %q; want a [COST] line", stderr.String())
				}
			} else if stderr.Len() != 0 {
				t.Errorf("stderr = %q; want empty", stderr.String())
			}
		})
	}
}

// TestCostWarnIsLatchedAndStopIsNot is the one piece of state in the tracker. `warn` is
// a notice, so it must print ONCE however many steps push the total higher — otherwise a
// long workflow prints the same line per step and the notice becomes noise. `stop` is an
// error return, so it is deliberately NOT latched: every over-cap check keeps failing
// rather than succeeding on the second call.
func TestCostWarnIsLatchedAndStopIsNot(t *testing.T) {
	var stderr bytes.Buffer
	ct := newCostTracker(params.LobsterCostLimit{Max_usd: 1.0, Action: "warn"}, &stderr)
	ct.totalUSD = 2.0

	for i := 0; i < 4; i++ {
		if err := ct.checkLimit(); err != nil {
			t.Fatalf("call %d: checkLimit() = %v; want nil for warn", i, err)
		}
	}
	if got := strings.Count(stderr.String(), "[COST]"); got != 1 {
		t.Errorf("warn printed %d [COST] lines over 4 over-cap checks; want exactly 1", got)
	}

	stop := newCostTracker(params.LobsterCostLimit{Max_usd: 1.0, Action: "stop"}, nil)
	stop.totalUSD = 2.0
	for i := 0; i < 3; i++ {
		if err := stop.checkLimit(); err == nil {
			t.Fatalf("call %d: checkLimit() = nil; want an error for stop, every time", i)
		}
	}
}
