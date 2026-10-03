package pluginlobster

// cost.go — the cost ledger and `cost_limit` enforcement.
//
// DIVERGENCE (documented, not silent): upstream lobster maintains a full LLM SPEND
// ledger with replay provenance, because its steps may call a model. This engine's
// stdlib is deterministic by construction — it refuses the LLM pipeline stages — so
// there is nothing for this engine to spend. What it still does, faithfully, is READ
// the `_meta.cost` block a step emits (a charly step's own accounting, produced by
// whatever plugin ran it) and enforce the workflow's `cost_limit` on the total, so a
// workflow that composes an expensive step still cannot run away without the author's
// ceiling firing.
//
// `settle` is upstream's reservation release. There are no in-flight reservations here
// (every step is synchronous and its cost is known when it returns), so it is a no-op
// kept as the named point where the reservation lifecycle WOULD go.

import (
	"fmt"
	"io"
	"strings"

	"github.com/opencharly/plugin-lobster/candy/plugin-lobster/params"
)

// stepCost is one step's recorded spend, mirroring upstream's by-step entry.
type stepCost struct {
	StepID       string
	Model        string
	InputTokens  int64
	OutputTokens int64
	CostUSD      float64
}

// costSummary is what a run reports back.
type costSummary struct {
	TotalInputTokens  int64
	TotalOutputTokens int64
	EstimatedCostUSD  float64
	ByStep            []stepCost
}

// costTracker accumulates the ledger for one run and enforces the ceiling.
type costTracker struct {
	limit    params.LobsterCostLimit
	stderr   io.Writer
	byStep   []stepCost
	totalUSD float64
	warned   bool
}

func newCostTracker(limit params.LobsterCostLimit, stderr io.Writer) *costTracker {
	if stderr == nil {
		stderr = io.Discard
	}
	return &costTracker{limit: limit, stderr: stderr}
}

// track records whatever `_meta.cost` a completed step emitted. A step that reports no
// cost contributes nothing — this is a ledger, not an estimate.
func (c *costTracker) track(stepID string, r *stepResult) {
	if r == nil || !r.HasJSON {
		return
	}
	obj, ok := r.JSON.(map[string]any)
	if !ok {
		return
	}
	meta, ok := obj["_meta"].(map[string]any)
	if !ok {
		return
	}
	cost, ok := meta["cost"].(map[string]any)
	if !ok {
		return
	}

	entry := stepCost{StepID: stepID}
	if s, ok := cost["model"].(string); ok {
		entry.Model = s
	}
	if n, ok := toInt(cost["totalInputTokens"]); ok {
		entry.InputTokens = n
	}
	if n, ok := toInt(cost["totalOutputTokens"]); ok {
		entry.OutputTokens = n
	}
	if f, ok := toFloat(cost["estimatedCostUsd"]); ok {
		entry.CostUSD = f
	}

	c.byStep = append(c.byStep, entry)
	c.totalUSD += entry.CostUSD
}

// settle is the reservation-release point. See the file header: this engine holds no
// in-flight reservations, so there is nothing to release.
func (c *costTracker) settle() {}

// checkLimit enforces `cost_limit`. The action is upstream's: `stop` fails the run,
// `warn` prints once and continues. An absent limit is no limit.
func (c *costTracker) checkLimit() error {
	maxUSD, ok := toFloat(c.limit.Max_usd)
	if !ok {
		return nil
	}
	if c.totalUSD <= maxUSD {
		return nil
	}
	action := strings.TrimSpace(c.limit.Action)
	if action == "" {
		action = "warn"
	}
	if action == "stop" {
		return fmt.Errorf("Workflow cost limit exceeded: $%.4f > $%.4f", c.totalUSD, maxUSD)
	}
	if !c.warned {
		c.warned = true
		fmt.Fprintf(c.stderr, "[COST] Workflow cost limit exceeded: $%.4f > $%.4f\n", c.totalUSD, maxUSD)
	}
	return nil
}

// summary renders the ledger. A run that spent nothing reports nil rather than a
// zero-valued block, so a caller can tell "no accounting" from "accounted, zero".
func (c *costTracker) summary() *costSummary {
	if len(c.byStep) == 0 {
		return nil
	}
	s := &costSummary{
		EstimatedCostUSD: c.totalUSD,
		ByStep:           append([]stepCost{}, c.byStep...),
	}
	for _, e := range c.byStep {
		s.TotalInputTokens += e.InputTokens
		s.TotalOutputTokens += e.OutputTokens
	}
	return s
}
