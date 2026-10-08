// lobster.cue — the `workflow:lobster` engine's own schema (SDD single source).
//
// Self-contained: no package clause, no base references — it compiles standalone and
// splices onto the host base (the `plugin-pipeline/schema/pipeline.cue` precedent).
// cue:gen (wrapped with package params + @go(params)) emits params/cue_types_gen.go, which
// is the Go view of these shapes. The engine REACHES that view one of two ways: by DIRECT
// use of the generated type, or by EMBEDDING it in a small engine-only wrapper (the
// documented `json:"-"` exception) when the engine needs a field upstream does not carry.
// It is never re-declared by hand.
//
// WHAT THIS IS
// ------------
// #LobsterFile and its satellites are a faithful CUE transcription of UPSTREAM lobster's
// authored-file contract — the workflow file format this engine consumes and produces:
//
//   upstream  openclaw/lobster @ 74db2679fe34c6b04b067eebea943eaabd8b789f
//   package   @clawdbot/lobster 2026.9.16   (MIT)
//   sources   src/workflows/types.ts        (WorkflowFile and every step/result type)
//             src/core/cost_tracker.ts      (CostLimit / CostSummary / StepCost)
//             src/workflows/load.ts         (the cross-field rules, encoded as CUE bounds
//                                            where CUE can express them)
//
// It is NOT invented and it is NOT a re-design: every field name, every alias, every
// enum member and every numeric bound below is transcribed from those files. Two
// deliberate transcription rules:
//
//  1. OPEN, like upstream. load.ts type-checks the keys it knows and IGNORES unknown
//     ones; it never rejects a file for carrying an extra key. A closed CUE def would
//     reject real upstream files, so every struct here is open. The rules that are NOT
//     expressible as a CUE type (exactly one exec arm per step; unique ids; the
//     approval/input exclusion inside for_each) live in the Go cross-field validator,
//     ported rule-for-rule from load.ts.
//  2. ALIASES ARE KEPT (run/command, initiated_by/initiatedBy, required_approver/
//     requiredApprover, require_different_approver/requireDifferentApprover). Upstream
//     accepts both spellings, so the importer must too. This is the ONE surface where
//     an alias form is legal — the authored `kind:pipeline` form has no aliases
//     (cutover policy); upstream files are ingested here and migrated on import.
//
// WHERE EACH DEF IS USED
// ----------------------
// Every line names a consumer that exists in this repo today (grep the type name to find
// it); a def with no consumer says so explicitly rather than naming a dead one.
//
//  #LobsterFile              loadWorkflowFile (lobster.go) decodes it; the engine's run
//                            path and `charly lobster import` consume it, `export` marshals it.
//  #LobsterStep              the step the engine walks (engine.go, batch.go, gate.go).
//  #LobsterApprovalRequest   extractApprovalRequest (gate.go) returns it directly, and
//                            runResult.RequiresApproval (engine.go) carries it.
//  #LobsterInputRequest      EMBEDDED by inputRequest (gate.go), which adds only StepID;
//                            runResult.RequiresInput (engine.go) carries the wrapper.
//  #LobsterApprovalIdentity  approvalIdentityFromConfig / approvalIdentityFromRequest
//                            (gate.go) build it, and resumeState.ApprovalIdentity (state.go)
//                            persists it.
//  #LobsterApprovalObject    normalizeApprovalConfig (gate.go) decodes the `approval`
//                            object form THROUGH it instead of hand-reading keys.
//  #LobsterStepResult        EMBEDDED by stepResult (refs.go), one run-ledger row; the
//                            custom Marshal/Unmarshal in state.go is the PERSISTED codec.
//  #LobsterCostMeta          costTracker.track (cost.go) decodes a step's `_meta` through
//                            it.
//  #LobsterCostSummary /     the `cost` payload of #LobsterCostMeta, projected onto the
//  #LobsterStepCost          engine's typed costSummary / stepCost so `checkLimit` can do
//                            the arithmetic (int64/float64 — the generated fields are `any`).
//  #LobsterCostLimit         the costTracker's configured ceiling (cost.go), used directly.
//  #LobsterRunResult         ZERO references: the wire reply the engine answers is
//                            spec.WorkflowRunReply (run.go), never this authored shape. It
//                            is kept as part of the upstream contract's transcription, not
//                            because a Go consumer reads it.
//
// The remaining step satellites (#LobsterParallel, #LobsterBranch, #LobsterInput,
// #LobsterRetry, #LobsterApproval, …) are used DIRECTLY as the engine's field types.
//
// THREE engine types sit BESIDE the generated defs rather than being them, each for a
// reason the generated shape cannot express, and each documented at its definition:
//
//   runResult (engine.go)              its FIELD types ARE the generated ones; only the
//                                      POINTERS diverge (an absent gate must be
//                                      distinguishable from a zero value), and it is never
//                                      marshalled — runReply projects it onto the engine wire's reply.
//   stepResult (refs.go)               EMBEDS #LobsterStepResult; adds only the presence
//                                      flags plus exit code / stderr the persisted state
//                                      codec needs.
//   costSummary / stepCost (cost.go)   typed int64/float64 for `checkLimit`'s arithmetic.
//
// The ENGINE'S WIRE contract is NOT here: `workflow-run|resume|schedule|emit` decode
// `spec.WorkflowRunRequest` / `spec.WorkflowResumeRequest` / `spec.WorkflowScheduleRequest`
// / `spec.WorkflowEmitRequest` and answer the matching `spec.Workflow*Reply` — the
// engine-agnostic IR envelopes from spec/schema/workflow.cue. This file is the
// lobster-SPECIFIC authored form, i.e. the one thing the engine wire deliberately does not carry.

// ── the workflow file ───────────────────────────────────────────────────────
// #LobsterFile is upstream's WorkflowFile. `steps` is non-empty (load.ts: "Workflow
// file requires a non-empty steps array").
#LobsterFile: {
	name?:        string
	description?: string
	// args: name -> {default, description}. Upstream's arg spec is a strict subset of
	// charly's #TaskParamSpec, which is what the engine wire carries.
	args?: {[string]: {default?: _, description?: string}}
	env?:  {[string]: string}
	cwd?:  string
	steps: [#LobsterStep, ...#LobsterStep]
	cost_limit?: #LobsterCostLimit
}

// ── one step ────────────────────────────────────────────────────────────────
// Exactly one of run | pipeline | workflow | parallel | for_each, OR an approval |
// input gate. load.ts enforces the exclusivity and the counts; the CUE bounds below
// carry the per-field ones (types, enums, numeric ranges) it also enforces.
#LobsterStep: {
	id: string

	// the shell arm. `command` is upstream's deprecated ALIAS of `run` (load.ts:
	// `typeof step.run === "string" ? step.run : step.command`) — accepted on import.
	run?:     string
	command?: string
	// the deterministic stdlib arm: a lobster pipeline expression, parsed and run
	// natively (never handed to a shell).
	pipeline?: string
	// the sub-workflow arm: a file path to another .lobster file.
	workflow?:      string
	workflow_args?: {[string]: _}

	parallel?: #LobsterParallel
	for_each?: string
	item_var?: string
	index_var?: string
	batch_size?: int & >=1
	pause_ms?:   number & >=0
	steps?:      [...#LobsterStep]

	env?:    {[string]: string}
	cwd?:    string
	stdin?:  _
	approval?: #LobsterApproval
	input?:    #LobsterInput
	// condition/when: upstream carries both; `when` is the one the runner evaluates
	// (an expression over $id.* refs) and `condition` is the legacy spelling.
	condition?: _
	when?:      _
	timeout_ms?: int & >=1 & <=2147483647
	on_error?:   "stop" | "continue" | "skip_rest"
	retry?:      #LobsterRetry
	// redo: the charly-only conditional BACK-EDGE (see #LobsterRedo). It rides a STEP key
	// because that is the only shape upstream's open loader passes through unchanged: an
	// unknown step key is ignored by upstream, so an exported workflow.lobster stays
	// runnable there (it simply runs every step once), while charly's native engine honours
	// it. This is the operator-ruled route — the retired plugin-pipeline executor carried the
	// same spec in its own IR.
	redo?: #LobsterRedo
}

// ── redo ─────────────────────────────────────────────────────────────────────
// #LobsterRedo: the charly-only conditional BACK-EDGE. A failing step emits a trigger on
// its stderr ("LOOP-GUARD-TRIGGER: <name>"); `triggers` maps that name to the id of an
// EARLIER step to re-enter. Upstream lobster has no such key and ignores it, so an exported
// workflow.lobster stays runnable by upstream (it simply runs every step once).
#LobsterRedo: {
	on_fail?:        [...string] | string
	triggers?:       {[string]: string}
	max?:            int & >0
	escalate_after?: int & >0
}

// ── retry ───────────────────────────────────────────────────────────────────
// #LobsterRetry: the per-step retry policy (load.ts validates each field's type and
// range; `max` is a positive integer, `backoff` the closed two-member enum).
#LobsterRetry: {
	max?:         int & >=1
	backoff?:     "fixed" | "exponential"
	delay_ms?:    number & >=0
	max_delay_ms?: number & >=0
	jitter?:      bool
}

// ── parallel ────────────────────────────────────────────────────────────────
#LobsterParallel: {
	wait?:       "all" | "any"
	timeout_ms?: int & >=1 & <=2147483647
	branches: [#LobsterBranch, ...#LobsterBranch]
}

// #LobsterBranch: one parallel branch. Exactly one of run | command | pipeline
// (load.ts: "can only define one of run, command, or pipeline"). A branch has no
// nested steps, no approval and no input — upstream's shape, not a simplification.
#LobsterBranch: {
	id: string
	run?:     string
	command?: string
	pipeline?: string
	env?:  {[string]: string}
	cwd?:  string
	stdin?: _
}

// ── gates ───────────────────────────────────────────────────────────────────
// #LobsterApproval is upstream's union: `true`, the literal "required", a free-form
// prompt string, or the object form. `false` is transcribed as a legal bool because
// upstream's declared type is `boolean` — its runtime predicate isApprovalStep(false)
// is false, so a false gate is simply not a gate; that predicate lives in Go, not here.
#LobsterApproval: bool | string | #LobsterApprovalObject

// The object form. The snake_case and camelCase spellings are BOTH upstream-legal
// (load.ts validates each independently), hence both are carried.
#LobsterApprovalObject: {
	prompt?:  string
	items?:   [..._]
	preview?: string
	initiated_by?:   string
	initiatedBy?:    string
	required_approver?:  string
	requiredApprover?:   string
	require_different_approver?: bool
	requireDifferentApprover?:   bool
}

// #LobsterApprovalIdentity: the identity triple a completed approval records — the
// same three fields, camelCase only (upstream's WorkflowApprovalIdentity).
#LobsterApprovalIdentity: {
	initiatedBy?:              string
	requiredApprover?:         string
	requireDifferentApprover?: bool
}

// #LobsterInput: the `input` gate. `responseSchema` is a JSON Schema; upstream compiles
// it (compileCached) and rejects an invalid one, which the Go side does with
// cuelang.org/go/encoding/jsonschema.
#LobsterInput: {
	prompt: string
	responseSchema: _
	defaults?:      _
}

// ── results ─────────────────────────────────────────────────────────────────
// #LobsterStepResult: one step's recorded outcome.
#LobsterStepResult: {
	id: string
	stdout?:  string
	json?:    _
	approved?: bool
	initiatedBy?: string
	approvedBy?:  string
	subject?:     _
	response?:    _
	skipped?:     bool
	error?:       bool
	errorMessage?: string
}

// #LobsterRunResult: the tool-mode envelope. `status` is the full upstream enum; the
// engine's own wire reply (spec.WorkflowRunReply) maps onto it one-for-one.
#LobsterRunResult: {
	status: "ok" | "needs_approval" | "needs_input" | "cancelled"
	output: [..._]
	requiresApproval?: #LobsterApprovalRequest
	requiresInput?:    #LobsterInputRequest
	// "_meta" is QUOTED deliberately: an unquoted _meta would be a CUE hidden field
	// (not emitted, not generated), which is not what upstream's JSON carries. The
	// @go(Meta) rename is required for the same reason — without it gengotypes derives
	// the UNEXPORTED field name `meta`, which encoding/json cannot populate.
	"_meta"?: #LobsterCostMeta @go(Meta)
}

// #LobsterCostMeta: the `_meta` block (upstream: `{cost?: CostSummary}`).
#LobsterCostMeta: {
	cost?: #LobsterCostSummary
}

// #LobsterApprovalRequest: the pending-approval block a needs_approval reply carries.
#LobsterApprovalRequest: {
	type: "approval_request"
	prompt: string
	items:  [..._]
	preview?: string
	initiatedBy?:              string
	requiredApprover?:         string
	requireDifferentApprover?: bool
	resumeToken?: string
	approvalId?:  string
}

// #LobsterInputRequest: the pending-input block a needs_input reply carries.
#LobsterInputRequest: {
	type: "input_request"
	prompt:         string
	responseSchema: _
	defaults?:      _
	subject?:       _
	resumeToken?:   string
}

// ── cost ────────────────────────────────────────────────────────────────────
// #LobsterCostLimit: the workflow-level `cost_limit` (cost_tracker.ts). max_usd is a
// non-negative number and action is the closed {warn, stop} enum.
#LobsterCostLimit: {
	max_usd: number & >=0
	action?: "warn" | "stop"
}

// #LobsterCostSummary / #LobsterStepCost: the `_meta.cost` payload.
#LobsterCostSummary: {
	totalInputTokens:  number
	totalOutputTokens: number
	estimatedCostUsd:  number
	byStep: [...#LobsterStepCost]
}

#LobsterStepCost: {
	stepId:       string
	model:        string | null
	inputTokens:  number
	outputTokens: number
	costUsd:      number
}
