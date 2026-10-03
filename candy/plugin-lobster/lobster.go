package pluginlobster

// lobster.go — loading and validating a `.lobster` workflow file.
//
// A port of upstream `src/workflows/load.ts`. It validates the RAW parsed tree (a
// YAML/JSON object), NOT the typed struct — that is deliberate and load-bearing:
// upstream's rules are about KEY PRESENCE (`parallel` present-but-empty is an exec arm
// and a load error; absent is not an arm at all), and a typed Go struct cannot tell
// those two apart. So the raw tree is the validation representation and
// `params.LobsterFile` is the execution representation, decoded only after the tree
// validates. Validating the struct instead would silently ACCEPT a file whose
// `parallel:` block the engine would then ignore — exactly the "silently drop" the IR
// contract forbids.
//
// The error strings are upstream's, verbatim: they are the operator's only diagnostic,
// and a reworded one is a different product.

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/opencharly/plugin-lobster/candy/plugin-lobster/params"
)

// parseWorkflowTree reads a workflow file into the normalized raw tree. This is the ONE
// reader: the loader validates it and the `charly lobster import` CLI re-shapes it, and
// neither may disagree about what is in the file.
func parseWorkflowTree(filePath string) (any, error) {
	raw, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("read workflow file: %w", err)
	}
	var parsed any
	if strings.EqualFold(filepath.Ext(filePath), ".json") {
		if err := json.Unmarshal(raw, &parsed); err != nil {
			return nil, fmt.Errorf("parse workflow file: %w", err)
		}
	} else {
		if err := yaml.Unmarshal(raw, &parsed); err != nil {
			return nil, fmt.Errorf("parse workflow file: %w", err)
		}
	}
	return normalizeYAML(parsed), nil
}

// loadWorkflowFile reads and validates a workflow file, returning the typed file the
// engine executes.
func loadWorkflowFile(filePath string) (*params.LobsterFile, error) {
	parsed, err := parseWorkflowTree(filePath)
	if err != nil {
		return nil, err
	}

	if err := validateWorkflowTree(parsed); err != nil {
		return nil, err
	}

	// Re-encode the validated tree for the typed decode, so the YAML/JSON spelling of a
	// key never reaches the typed struct through a different path than the one validated.
	encoded, err := json.Marshal(parsed)
	if err != nil {
		return nil, fmt.Errorf("re-encode workflow file: %w", err)
	}
	var file params.LobsterFile
	if err := json.Unmarshal(encoded, &file); err != nil {
		return nil, fmt.Errorf("decode workflow file: %w", err)
	}
	return &file, nil
}

// normalizeYAML converts the map[any]any that yaml.v3 can produce at a nested level into
// map[string]any, so every accessor below sees one map type.
func normalizeYAML(v any) any {
	switch t := v.(type) {
	case map[string]any:
		for k, val := range t {
			t[k] = normalizeYAML(val)
		}
		return t
	case map[any]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			out[fmt.Sprint(k)] = normalizeYAML(val)
		}
		return out
	case []any:
		for i, val := range t {
			t[i] = normalizeYAML(val)
		}
		return t
	default:
		return v
	}
}

// ---------------------------------------------------------------------------
// raw-tree accessors
// ---------------------------------------------------------------------------

func asMap(v any) (map[string]any, bool) {
	m, ok := v.(map[string]any)
	return m, ok
}

func asString(v any) (string, bool) {
	s, ok := v.(string)
	return s, ok
}

func present(m map[string]any, key string) bool {
	_, ok := m[key]
	return ok
}

// firstString is upstream's `typeof a === "string" ? a : b` — the FIRST value that is a
// string, used for the `run`/`command` alias pair.
func firstString(vs ...any) (string, bool) {
	for _, v := range vs {
		if s, ok := asString(v); ok {
			return s, true
		}
	}
	return "", false
}

// isApprovalStep is upstream's predicate: `true`, a non-blank string, or a plain object.
// `false` is NOT an approval step.
func isApprovalStep(v any) bool {
	if b, ok := v.(bool); ok {
		return b
	}
	if s, ok := v.(string); ok {
		return strings.TrimSpace(s) != ""
	}
	if _, ok := asMap(v); ok {
		return true
	}
	return false
}

// isInputStep is upstream's predicate: a plain object.
func isInputStep(v any) bool {
	_, ok := asMap(v)
	return ok
}

// isTypedInputStep is `isInputStep` applied to the UNMARSHALLED step the engine runs. The
// raw predicate reads the JSON object; the engine holds a `params.LobsterInput`, where the
// key's presence is not recoverable. `responseSchema` IS recoverable: the loader requires
// it on every `input` step and rejects it anywhere else, so its presence is exactly "this
// step declares an input gate" — including a `prompt: ""` gate, which a prompt-based test
// would silently miss.
func isTypedInputStep(in params.LobsterInput) bool {
	return in.ResponseSchema != nil
}

// ---------------------------------------------------------------------------
// validation
// ---------------------------------------------------------------------------

func validateWorkflowTree(parsed any) error {
	root, ok := asMap(parsed)
	if !ok {
		return fmt.Errorf("Workflow file must be a JSON/YAML object")
	}
	stepsRaw, ok := root["steps"].([]any)
	if !ok || len(stepsRaw) == 0 {
		return fmt.Errorf("Workflow file requires a non-empty steps array")
	}

	if clRaw, has := root["cost_limit"]; has {
		if err := validateCostLimit(clRaw); err != nil {
			return err
		}
	}

	seen := map[string]bool{}
	for _, stepRaw := range stepsRaw {
		step, ok := asMap(stepRaw)
		if !ok {
			return fmt.Errorf("Workflow step must be an object")
		}
		if err := validateStep(step, seen); err != nil {
			return err
		}
	}
	return nil
}

func validateCostLimit(v any) error {
	cl, ok := asMap(v)
	if !ok {
		return fmt.Errorf("Workflow cost_limit must be an object")
	}
	maxUSD, ok := toFloat(cl["max_usd"])
	if !ok || maxUSD < 0 {
		return fmt.Errorf("Workflow cost_limit.max_usd must be a non-negative number")
	}
	if action, has := cl["action"]; has {
		s, _ := asString(action)
		if s != "warn" && s != "stop" {
			return fmt.Errorf(`Workflow cost_limit.action must be "warn" or "stop"`)
		}
	}
	return nil
}

func validateStep(step map[string]any, seen map[string]bool) error {
	id, _ := asString(step["id"])
	if id == "" {
		return fmt.Errorf("Workflow step requires an id")
	}

	if wf, has := step["workflow"]; has {
		s, ok := asString(wf)
		if !ok {
			return fmt.Errorf("Workflow step %s workflow must be a string (file path)", id)
		}
		if strings.TrimSpace(s) == "" {
			return fmt.Errorf("Workflow step %s workflow path cannot be blank", id)
		}
	}
	if wa, has := step["workflow_args"]; has {
		if _, ok := asMap(wa); !ok {
			return fmt.Errorf("Workflow step %s workflow_args must be a plain object", id)
		}
	}

	// parallel
	if parRaw, has := step["parallel"]; has {
		par, ok := asMap(parRaw)
		if !ok {
			return fmt.Errorf("Workflow step %s parallel must be an object", id)
		}
		if err := validateParallel(id, par, seen); err != nil {
			return err
		}
	}
	isParallel := func() bool {
		_, ok := asMap(step["parallel"])
		return ok
	}()

	// for_each
	if fe, has := step["for_each"]; has {
		if _, ok := asString(fe); !ok {
			return fmt.Errorf("Workflow step %s for_each must be a string (step reference expression)", id)
		}
	}
	isForEach := func() bool {
		_, ok := asString(step["for_each"])
		return ok
	}()
	if isForEach {
		if err := validateForEach(id, step); err != nil {
			return err
		}
	}

	shellCommand, _ := firstString(step["run"], step["command"])
	pipeline, _ := asString(step["pipeline"])
	workflowRef, _ := asString(step["workflow"])

	executionCount := 0
	for _, s := range []string{shellCommand, pipeline, workflowRef} {
		if s != "" {
			executionCount++
		}
	}
	if isParallel {
		executionCount++
	}
	if isForEach {
		executionCount++
	}

	if executionCount == 0 && !isApprovalStep(step["approval"]) && !isInputStep(step["input"]) {
		return fmt.Errorf("Workflow step %s requires run, command, pipeline, workflow, parallel, for_each, approval, or input", id)
	}
	if executionCount > 1 {
		return fmt.Errorf("Workflow step %s can only define one of run, command, pipeline, workflow, parallel, or for_each", id)
	}
	if executionCount > 0 && isInputStep(step["input"]) {
		return fmt.Errorf("Workflow step %s input steps cannot define run, command, pipeline, workflow, parallel, or for_each", id)
	}
	if isApprovalStep(step["approval"]) && isInputStep(step["input"]) {
		return fmt.Errorf("Workflow step %s cannot define both approval and input", id)
	}

	if v, has := step["run"]; has {
		if _, ok := asString(v); !ok {
			return fmt.Errorf("Workflow step %s run must be a string", id)
		}
	}
	if v, has := step["command"]; has {
		if _, ok := asString(v); !ok {
			return fmt.Errorf("Workflow step %s command must be a string", id)
		}
	}
	if v, has := step["pipeline"]; has {
		if _, ok := asString(v); !ok {
			return fmt.Errorf("Workflow step %s pipeline must be a string", id)
		}
	}
	if v, has := step["input"]; has && !isInputStep(v) {
		return fmt.Errorf("Workflow step %s input must be an object", id)
	}
	if in, ok := asMap(step["input"]); ok {
		if _, ok := asString(in["prompt"]); !ok {
			return fmt.Errorf("Workflow step %s input.prompt must be a string", id)
		}
		schema, has := in["responseSchema"]
		if !has {
			return fmt.Errorf("Workflow step %s input.responseSchema must be an object", id)
		}
		obj, ok := asMap(schema)
		if !ok {
			// A JSON Schema may legitimately be `true`/`false` (a boolean schema), which
			// upstream's `typeof !== "object"` check also rejects — kept for fidelity.
			return fmt.Errorf("Workflow step %s input.responseSchema must be an object", id)
		}
		if err := validateJSONSchema(obj); err != nil {
			return fmt.Errorf("Workflow step %s input.responseSchema is invalid: %v", id, err)
		}
	}
	if ap, ok := asMap(step["approval"]); ok {
		if err := validateApprovalObject(id, ap); err != nil {
			return err
		}
	}

	if v, has := step["timeout_ms"]; has {
		if n, ok := toInt(v); !ok || n < 1 || n > 2_147_483_647 {
			return fmt.Errorf("Workflow step %s timeout_ms must be a positive integer between 1 and 2147483647", id)
		}
	}
	if v, has := step["on_error"]; has {
		s, _ := asString(v)
		if s != "stop" && s != "continue" && s != "skip_rest" {
			return fmt.Errorf(`Workflow step %s on_error must be "stop", "continue", or "skip_rest"`, id)
		}
	}
	if v, has := step["retry"]; has {
		if err := validateRetry(id, v); err != nil {
			return err
		}
	}

	// The id set is filled LAST, exactly as upstream does it: a step is checked against the
	// ids of the steps and branches BEFORE it, then contributes its own id — and, when it is
	// a parallel step, its branch ids too, so a LATER step carrying a branch id is caught.
	if seen[id] {
		return fmt.Errorf("Duplicate workflow step id: %s", id)
	}
	if par, ok := asMap(step["parallel"]); ok {
		if branches, ok := par["branches"].([]any); ok {
			for _, bRaw := range branches {
				if branch, ok := asMap(bRaw); ok {
					if bid, ok := asString(branch["id"]); ok {
						seen[bid] = true
					}
				}
			}
		}
	}
	seen[id] = true
	return nil
}

func validateParallel(stepID string, par map[string]any, seen map[string]bool) error {
	branches, ok := par["branches"].([]any)
	if !ok || len(branches) == 0 {
		return fmt.Errorf("Workflow step %s parallel requires a non-empty branches array", stepID)
	}
	if v, has := par["wait"]; has {
		s, _ := asString(v)
		if s != "all" && s != "any" {
			return fmt.Errorf(`Workflow step %s parallel wait must be "all" or "any"`, stepID)
		}
	}
	if v, has := par["timeout_ms"]; has {
		if n, ok := toInt(v); !ok || n < 1 || n > 2_147_483_647 {
			return fmt.Errorf("Workflow step %s parallel timeout_ms must be a positive integer between 1 and 2147483647", stepID)
		}
	}

	branchIDs := map[string]bool{}
	for _, bRaw := range branches {
		branch, ok := asMap(bRaw)
		if !ok {
			return fmt.Errorf("Workflow step %s parallel branches must be objects", stepID)
		}
		bid, _ := asString(branch["id"])
		if bid == "" {
			return fmt.Errorf("Workflow step %s parallel branch requires an id", stepID)
		}
		if bid == stepID {
			return fmt.Errorf("Workflow step %s parallel branch id cannot match the step id", stepID)
		}
		if branchIDs[bid] {
			return fmt.Errorf("Workflow step %s duplicate parallel branch id: %s", stepID, bid)
		}
		if seen[bid] {
			return fmt.Errorf("Duplicate workflow id across steps/parallel branches: %s", bid)
		}
		branchIDs[bid] = true

		branchShell, _ := firstString(branch["run"], branch["command"])
		branchPipeline, _ := asString(branch["pipeline"])
		execCount := 0
		if branchShell != "" {
			execCount++
		}
		if branchPipeline != "" {
			execCount++
		}
		if execCount == 0 {
			return fmt.Errorf("Workflow step %s parallel branch %s requires run, command, or pipeline", stepID, bid)
		}
		if execCount > 1 {
			return fmt.Errorf("Workflow step %s parallel branch %s can only define one of run, command, or pipeline", stepID, bid)
		}
		if v, has := branch["run"]; has {
			if _, ok := asString(v); !ok {
				return fmt.Errorf("Workflow step %s parallel branch %s run must be a string", stepID, bid)
			}
		}
		if v, has := branch["command"]; has {
			if _, ok := asString(v); !ok {
				return fmt.Errorf("Workflow step %s parallel branch %s command must be a string", stepID, bid)
			}
		}
		if v, has := branch["pipeline"]; has {
			if _, ok := asString(v); !ok {
				return fmt.Errorf("Workflow step %s parallel branch %s pipeline must be a string", stepID, bid)
			}
		}
	}
	return nil
}

func validateForEach(stepID string, step map[string]any) error {
	subsRaw, ok := step["steps"].([]any)
	if !ok || len(subsRaw) == 0 {
		return fmt.Errorf("Workflow step %s for_each requires a non-empty steps array", stepID)
	}
	if v, has := step["batch_size"]; has {
		if n, ok := toInt(v); !ok || n < 1 {
			return fmt.Errorf("Workflow step %s batch_size must be a positive integer", stepID)
		}
	}
	if v, has := step["pause_ms"]; has {
		if f, ok := toFloat(v); !ok || f < 0 {
			return fmt.Errorf("Workflow step %s pause_ms must be a finite non-negative number", stepID)
		}
	}
	if isApprovalStep(step["approval"]) {
		return fmt.Errorf("Workflow step %s for_each steps cannot define approval (use a separate step after the loop)", stepID)
	}
	if isInputStep(step["input"]) {
		return fmt.Errorf("Workflow step %s for_each steps cannot define input (use a separate step after the loop)", stepID)
	}
	if v, has := step["stdin"]; has && v != nil {
		return fmt.Errorf("Workflow step %s for_each steps cannot define stdin (loop input comes from the for_each expression)", stepID)
	}
	loopShell, _ := firstString(step["run"], step["command"])
	loopPipeline, _ := asString(step["pipeline"])
	if loopShell != "" || loopPipeline != "" || present(step, "workflow") || present(step, "parallel") {
		return fmt.Errorf("Workflow step %s for_each cannot also define run, command, pipeline, workflow, or parallel", stepID)
	}
	if v, has := step["item_var"]; has {
		if _, ok := asString(v); !ok {
			return fmt.Errorf("Workflow step %s item_var must be a string", stepID)
		}
	}
	if v, has := step["index_var"]; has {
		if _, ok := asString(v); !ok {
			return fmt.Errorf("Workflow step %s index_var must be a string", stepID)
		}
	}
	itemVar, _ := asString(step["item_var"])
	if itemVar == "" {
		itemVar = "item"
	}
	indexVar, _ := asString(step["index_var"])
	if indexVar == "" {
		indexVar = "index"
	}
	if itemVar == indexVar {
		return fmt.Errorf("Workflow step %s item_var and index_var cannot be the same", stepID)
	}

	subIDs := map[string]bool{}
	for _, sRaw := range subsRaw {
		sub, ok := asMap(sRaw)
		if !ok {
			return fmt.Errorf("Workflow step %s for_each sub-step requires an id", stepID)
		}
		sid, _ := asString(sub["id"])
		if sid == "" {
			return fmt.Errorf("Workflow step %s for_each sub-step requires an id", stepID)
		}
		if sid == itemVar || sid == indexVar {
			return fmt.Errorf("Workflow step %s for_each sub-step id '%s' conflicts with loop variable", stepID, sid)
		}
		if subIDs[sid] {
			return fmt.Errorf("Workflow step %s duplicate for_each sub-step id: %s", stepID, sid)
		}
		subIDs[sid] = true

		if isApprovalStep(sub["approval"]) || isInputStep(sub["input"]) {
			return fmt.Errorf("Workflow step %s for_each sub-steps cannot contain approval or input steps", stepID)
		}
		if v, has := sub["run"]; has {
			if _, ok := asString(v); !ok {
				return fmt.Errorf("Workflow step %s for_each sub-step %s run must be a string", stepID, sid)
			}
		}
		if v, has := sub["command"]; has {
			if _, ok := asString(v); !ok {
				return fmt.Errorf("Workflow step %s for_each sub-step %s command must be a string", stepID, sid)
			}
		}
		if v, has := sub["pipeline"]; has {
			if _, ok := asString(v); !ok {
				return fmt.Errorf("Workflow step %s for_each sub-step %s pipeline must be a string", stepID, sid)
			}
		}
		if present(sub, "workflow") || present(sub, "parallel") || present(sub, "for_each") {
			return fmt.Errorf("Workflow step %s for_each sub-step %s cannot define workflow, parallel, or for_each", stepID, sid)
		}
		subShell, _ := firstString(sub["run"], sub["command"])
		subPipeline, _ := asString(sub["pipeline"])
		if subShell == "" && subPipeline == "" {
			return fmt.Errorf("Workflow step %s for_each sub-step %s requires run, command, or pipeline", stepID, sid)
		}
		count := 0
		if subShell != "" {
			count++
		}
		if subPipeline != "" {
			count++
		}
		if count > 1 {
			return fmt.Errorf("Workflow step %s for_each sub-step %s can only define one of run, command, or pipeline", stepID, sid)
		}
		// A for_each sub-step id is scoped to its loop (upstream checks it against the
		// loop's own set only), so it does NOT join the global id set.
	}
	return nil
}

func validateApprovalObject(stepID string, ap map[string]any) error {
	for _, key := range []string{"initiated_by", "initiatedBy", "required_approver", "requiredApprover"} {
		if v, has := ap[key]; has {
			if _, ok := asString(v); !ok {
				return fmt.Errorf("Workflow step %s approval.%s must be a string", stepID, key)
			}
		}
	}
	for _, key := range []string{"require_different_approver", "requireDifferentApprover"} {
		if v, has := ap[key]; has {
			if _, ok := v.(bool); !ok {
				return fmt.Errorf("Workflow step %s approval.%s must be a boolean", stepID, key)
			}
		}
	}
	return nil
}

func validateRetry(stepID string, v any) error {
	r, ok := asMap(v)
	if !ok {
		return fmt.Errorf("Workflow step %s retry must be an object", stepID)
	}
	if mv, has := r["max"]; has {
		if n, ok := toInt(mv); !ok || n < 1 {
			return fmt.Errorf("Workflow step %s retry.max must be a positive integer", stepID)
		}
	}
	if bv, has := r["backoff"]; has {
		s, _ := asString(bv)
		if s != "fixed" && s != "exponential" {
			return fmt.Errorf(`Workflow step %s retry.backoff must be "fixed" or "exponential"`, stepID)
		}
	}
	for _, key := range []string{"delay_ms", "max_delay_ms"} {
		if dv, has := r[key]; has {
			if f, ok := toFloat(dv); !ok || f < 0 {
				return fmt.Errorf("Workflow step %s retry.%s must be a finite non-negative number", stepID, key)
			}
		}
	}
	if jv, has := r["jitter"]; has {
		if _, ok := jv.(bool); !ok {
			return fmt.Errorf("Workflow step %s retry.jitter must be a boolean", stepID)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// numeric coercion (YAML gives int, JSON gives float64)
// ---------------------------------------------------------------------------

func toFloat(v any) (float64, bool) {
	switch t := v.(type) {
	case float64:
		return t, true
	case float32:
		return float64(t), true
	case int:
		return float64(t), true
	case int64:
		return float64(t), true
	case json.Number:
		f, err := t.Float64()
		return f, err == nil
	default:
		return 0, false
	}
}

func toInt(v any) (int64, bool) {
	switch t := v.(type) {
	case int:
		return int64(t), true
	case int64:
		return t, true
	case float64:
		if t != float64(int64(t)) {
			return 0, false
		}
		return int64(t), true
	case json.Number:
		n, err := t.Int64()
		return n, err == nil
	default:
		return 0, false
	}
}
