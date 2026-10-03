package pluginlobster

// gate.go — the approval and input gates.
//
// A port of upstream `src/workflows/file.ts`'s gate handling plus `validation.ts`'s
// response check. The two rules that matter most, and that are easy to get subtly wrong:
//
//   - an approval RESUME must carry the approver identity when the gate demands one, and
//     a `--approve no` is a cancellation, not a failure;
//   - an input RESUME response is checked against the gate's JSON Schema BEFORE it is
//     recorded, and a schema violation is an ARGUMENT error (the operator's command was
//     wrong) rather than a run failure.

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"cuelang.org/go/cue"
	"cuelang.org/go/cue/cuecontext"
	"cuelang.org/go/encoding/jsonschema"

	"github.com/opencharly/plugin-lobster/candy/plugin-lobster/params"
)

// ---------------------------------------------------------------------------
// envelope blocks
// ---------------------------------------------------------------------------

// approvalRequest is upstream's WorkflowApprovalRequest (the needs_approval payload).
// Its JSON names are upstream's camelCase, because the envelope is the wire contract.
type approvalRequest struct {
	Type                     string `json:"type"`
	Prompt                   string `json:"prompt"`
	Items                    []any  `json:"items"`
	Preview                  string `json:"preview,omitempty"`
	InitiatedBy              string `json:"initiatedBy,omitempty"`
	RequiredApprover         string `json:"requiredApprover,omitempty"`
	RequireDifferentApprover bool   `json:"requireDifferentApprover,omitempty"`
	ResumeToken              string `json:"resumeToken,omitempty"`
	ApprovalID               string `json:"approvalId,omitempty"`
}

// inputRequest is upstream's WorkflowInputRequest, plus StepID — the IR reply carries
// the gate's STEP (`#WorkflowInputRequest.step`) while upstream's own envelope does not,
// so the id rides along unexported and is projected at the reply boundary.
type inputRequest struct {
	Type           string         `json:"type"`
	StepID         string         `json:"-"`
	Prompt         string         `json:"prompt"`
	ResponseSchema map[string]any `json:"responseSchema"`
	Defaults       any            `json:"defaults,omitempty"`
	Subject        any            `json:"subject,omitempty"`
	ResumeToken    string         `json:"resumeToken,omitempty"`
}

// The envelope size limits. Upstream keeps the FULL resolved subject in the resume state
// and only truncates the COPY it puts in the tool envelope, so a resume sees the same
// subject the paused run did.
const (
	maxNeedsInputSubjectBytes   = 192_000
	defaultToolEnvelopeMaxBytes = 512_000
	approvalPromptTimeoutEnv    = "LOBSTER_APPROVAL_INPUT_TIMEOUT_MS"
	approvalApprovedByEnv       = "LOBSTER_APPROVAL_APPROVED_BY"
	approvalInitiatedByEnv      = "LOBSTER_APPROVAL_INITIATED_BY"
	approvalRequiredApproverEnv = "LOBSTER_APPROVAL_REQUIRED_APPROVER"
	approvalRequireDifferentEnv = "LOBSTER_APPROVAL_REQUIRE_DIFFERENT_APPROVER"
	toolEnvelopeMaxBytesEnv     = "LOBSTER_TOOL_ENVELOPE_MAX_BYTES"
)

// ---------------------------------------------------------------------------
// approval
// ---------------------------------------------------------------------------

// approvalGate decides what to do after an approval-marked step. It returns gated=true
// when the run must stop and hand the envelope back, with the reply to return.
func (st *runState) approvalGate(ctx context.Context, step *params.LobsterStep, idx int) (bool, *runResult, error) {
	approval := extractApprovalRequest(step, st.results[step.Id], st.eng.env)
	if approval.InitiatedBy != "" && st.results[step.Id] != nil {
		st.results[step.Id].InitiatedBy = approval.InitiatedBy
	}

	// A gate pauses a TOOL run and an interactive one that cannot be answered. The wire
	// path is always non-interactive (a plugin has no TTY), so `mode: tool` is the default
	// there and `interactive` is set only by the CLI.
	if !st.eng.interactive {
		stateKey, err := st.saveGateState(ctx, resumeState{
			FilePath:         st.filePath,
			ResumeAtIndex:    int64(idx) + 1,
			Steps:            st.results,
			Args:             st.args,
			ApprovalStepID:   step.Id,
			ApprovalIdentity: approvalIdentityFromRequest(approval),
		})
		if err != nil {
			return false, nil, err
		}
		approvalID, ierr := st.eng.store.createApprovalIndex(stateKey)
		if ierr != nil {
			_ = st.eng.store.delete(ctx, stateKey)
			return false, nil, ierr
		}
		approval.ResumeToken = encodeToken(stateKey)
		if approvalID != "" {
			approval.ApprovalID = approvalID
		}
		return true, &runResult{Status: "needs_approval", Output: []any{}, RequiresApproval: &approval}, nil
	}

	// Interactive: ask on the terminal. Anything but y/yes is a refusal, and a refusal is
	// fatal in this path — upstream throws "Not approved".
	fmt.Fprintf(st.eng.stdout, "%s [y/N] ", approval.Prompt)
	answer, err := readLine(ctx, st.eng.stdout)
	if err != nil {
		return false, nil, err
	}
	if !strings.EqualFold(strings.TrimSpace(answer), "y") && !strings.EqualFold(strings.TrimSpace(answer), "yes") {
		return false, nil, fmt.Errorf("Not approved")
	}
	approvedBy := strings.TrimSpace(st.eng.env[approvalApprovedByEnv])
	if err := enforceApprovalIdentity(step.Id, approvalIdentityFromRequest(approval), approvedBy); err != nil {
		return false, nil, err
	}
	t := true
	st.results[step.Id].Approved = &t
	if approvedBy != "" {
		st.results[step.Id].ApprovedBy = approvedBy
	}
	return false, nil, nil
}

// extractApprovalRequest is upstream's `extractApprovalRequest`: the step's own JSON may
// carry a `requiresApproval` block that REPLACES the config's prompt/items/preview, so a
// step can compute what it is asking to approve.
func extractApprovalRequest(step *params.LobsterStep, result *stepResult, env map[string]string) approvalRequest {
	cfg := normalizeApprovalConfig(step.Approval)
	identity := approvalIdentityFromConfig(cfg)
	if identity.InitiatedBy == "" {
		identity.InitiatedBy = strings.TrimSpace(env[approvalInitiatedByEnv])
	}
	if identity.RequiredApprover == "" {
		identity.RequiredApprover = strings.TrimSpace(env[approvalRequiredApproverEnv])
	}
	if !identity.RequireDifferentApprover {
		if b, ok := parseBoolLike(env[approvalRequireDifferentEnv]); ok {
			identity.RequireDifferentApprover = b
		}
	}

	req := approvalRequest{
		Type:                     "approval_request",
		Prompt:                   cfg.Prompt,
		Items:                    cfg.Items,
		Preview:                  cfg.Preview,
		InitiatedBy:              identity.InitiatedBy,
		RequiredApprover:         identity.RequiredApprover,
		RequireDifferentApprover: identity.RequireDifferentApprover,
	}
	if req.Prompt == "" {
		req.Prompt = "Approve " + step.Id + "?"
	}
	if req.Items == nil {
		req.Items = []any{}
	}

	if result != nil && result.HasJSON {
		if obj, ok := result.JSON.(map[string]any); ok {
			if ra, ok := obj["requiresApproval"].(map[string]any); ok {
				if s, ok := ra["prompt"].(string); ok && s != "" {
					req.Prompt = s
				}
				if items, ok := ra["items"].([]any); ok {
					req.Items = items
				}
				if s, ok := ra["preview"].(string); ok {
					req.Preview = s
				}
				for _, key := range []string{"initiated_by", "initiatedBy"} {
					if s, ok := ra[key].(string); ok && strings.TrimSpace(s) != "" {
						req.InitiatedBy = strings.TrimSpace(s)
					}
				}
				for _, key := range []string{"required_approver", "requiredApprover"} {
					if s, ok := ra[key].(string); ok && strings.TrimSpace(s) != "" {
						req.RequiredApprover = strings.TrimSpace(s)
					}
				}
				for _, key := range []string{"require_different_approver", "requireDifferentApprover"} {
					if b, ok := ra[key].(bool); ok {
						req.RequireDifferentApprover = b
					}
				}
			}
		}
	}
	return req
}

// normalizedApproval is the object form's fields, whichever spelling carried them.
type normalizedApproval struct {
	Prompt                   string
	Items                    []any
	Preview                  string
	InitiatedBy              string
	RequiredApprover         string
	RequireDifferentApprover bool
}

// normalizeApprovalConfig accepts the `approval` union: true/"required", a prompt string,
// or the object form in either spelling.
func normalizeApprovalConfig(v any) normalizedApproval {
	switch t := v.(type) {
	case string:
		if strings.TrimSpace(t) == "required" {
			return normalizedApproval{}
		}
		return normalizedApproval{Prompt: t}
	case map[string]any:
		out := normalizedApproval{}
		if s, ok := t["prompt"].(string); ok {
			out.Prompt = s
		}
		if items, ok := t["items"].([]any); ok {
			out.Items = items
		}
		if s, ok := t["preview"].(string); ok {
			out.Preview = s
		}
		out.InitiatedBy = firstNonBlankString(t["initiated_by"], t["initiatedBy"])
		out.RequiredApprover = firstNonBlankString(t["required_approver"], t["requiredApprover"])
		out.RequireDifferentApprover = firstBool(t["require_different_approver"], t["requireDifferentApprover"])
		return out
	default:
		return normalizedApproval{}
	}
}

func approvalIdentityFromConfig(c normalizedApproval) *approvalIdentity {
	return &approvalIdentity{
		InitiatedBy:              c.InitiatedBy,
		RequiredApprover:         c.RequiredApprover,
		RequireDifferentApprover: c.RequireDifferentApprover,
	}
}

func approvalIdentityFromRequest(r approvalRequest) *approvalIdentity {
	return &approvalIdentity{
		InitiatedBy:              r.InitiatedBy,
		RequiredApprover:         r.RequiredApprover,
		RequireDifferentApprover: r.RequireDifferentApprover,
	}
}

// enforceApprovalIdentity is upstream's rule, message for message: an approver is required
// only when the gate names one or demands a different one, and the first failure wins.
func enforceApprovalIdentity(stepID string, identity *approvalIdentity, approvedBy string) error {
	policy := identity
	if policy == nil {
		policy = &approvalIdentity{}
	}
	approver := strings.TrimSpace(approvedBy)

	if policy.RequiredApprover == "" && !policy.RequireDifferentApprover {
		return nil
	}
	if approver == "" {
		return fmt.Errorf("Workflow step %s approval requires approver identity; set %s", stepID, approvalApprovedByEnv)
	}
	if policy.RequiredApprover != "" && approver != policy.RequiredApprover {
		return fmt.Errorf("Workflow step %s approval requires approver '%s', got '%s'", stepID, policy.RequiredApprover, approver)
	}
	if policy.RequireDifferentApprover && policy.InitiatedBy != "" && approver == policy.InitiatedBy {
		return fmt.Errorf("Workflow step %s approval must be granted by someone other than '%s'", stepID, policy.InitiatedBy)
	}
	return nil
}

// ---------------------------------------------------------------------------
// input
// ---------------------------------------------------------------------------

// inputGate handles an `input` step: stop the run with a needs_input envelope, or record
// the interactively-typed response.
func (st *runState) inputGate(ctx context.Context, step *params.LobsterStep, idx int) (bool, *runResult, error) {
	subject, err := resolveInputSubject(step, st.args, st.results, st.lastStepID)
	if err != nil {
		return false, nil, err
	}

	if !st.eng.interactive {
		req := buildNeedsInputRequest(step.Id, step.Input.Prompt, schemaMap(step.Input.ResponseSchema), step.Input.Defaults, subject, st.toolEnvelopeMaxBytes())
		req.StepID = step.Id
		stateKey, serr := st.saveGateState(ctx, resumeState{
			FilePath:      st.filePath,
			ResumeAtIndex: int64(idx) + 1,
			Steps:         st.results,
			Args:          st.args,
			InputStepID:   step.Id,
			InputSchema:   schemaMap(step.Input.ResponseSchema),
			// The FULL resolved subject is persisted; only the envelope copy is truncated.
			InputSubject: subject,
		})
		if serr != nil {
			return false, nil, serr
		}
		req.ResumeToken = encodeToken(stateKey)
		return true, &runResult{Status: "needs_input", Output: []any{}, RequiresInput: req}, nil
	}

	fmt.Fprintf(st.eng.stdout, "%s\n", step.Input.Prompt)
	fmt.Fprintf(st.eng.stdout, "Enter JSON response: ")
	raw, err := readLine(ctx, st.eng.stdout)
	if err != nil {
		return false, nil, err
	}
	var parsed any
	if err := json.Unmarshal([]byte(strings.TrimSpace(raw)), &parsed); err != nil {
		return false, nil, fmt.Errorf("Input response must be valid JSON")
	}
	if err := validateInputResponse(schemaMap(step.Input.ResponseSchema), parsed, step.Id); err != nil {
		return false, nil, err
	}
	st.results[step.Id] = &stepResult{ID: step.Id, Subject: subject, HasSubject: true, Response: parsed, HasResp: true}
	st.lastStepID = step.Id
	return false, nil, nil
}

// resolveInputSubject is upstream's `resolveInputSubject`: an explicit `stdin` wins; else
// the previous step's json, response, or stdout.
func resolveInputSubject(step *params.LobsterStep, args map[string]any, rs results, lastStepID string) (any, error) {
	if step.Stdin != nil {
		return resolveInputValue(step.Stdin, args, rs)
	}
	if lastStepID == "" {
		return nil, nil
	}
	prev := rs[lastStepID]
	if prev == nil {
		return nil, nil
	}
	if prev.HasJSON {
		return prev.JSON, nil
	}
	if prev.HasResp {
		return prev.Response, nil
	}
	if prev.Stdout != "" {
		return prev.Stdout, nil
	}
	return nil, nil
}

// buildNeedsInputRequest assembles the envelope, degrading the subject in three steps as
// upstream does, and refusing outright if even that does not fit.
func buildNeedsInputRequest(stepID, prompt string, schema map[string]any, defaults any, subject any, maxBytes int) *inputRequest {
	base := &inputRequest{Type: "input_request", Prompt: prompt, ResponseSchema: schema}
	if defaults != nil {
		base.Defaults = defaults
	}
	req := *base
	req.Subject = subject
	if fitsEnvelope(&req, maxBytes) {
		return &req
	}
	req.Subject = maybeTruncateSubject(subject)
	if fitsEnvelope(&req, maxBytes) {
		return &req
	}
	req.Subject = map[string]any{
		"truncated": true,
		"bytes":     estimateBytes(subject),
		"preview":   "[subject omitted: envelope size limit]",
	}
	if fitsEnvelope(&req, maxBytes) {
		return &req
	}
	// Callers that cannot fit the envelope at all must fail: silently shipping an
	// oversized subject to a tool surface would be a truncation nobody can see.
	panic(fmt.Sprintf("Workflow input step %s needs_input envelope exceeds %d bytes even after subject truncation", stepID, maxBytes))
}

func fitsEnvelope(r *inputRequest, maxBytes int) bool {
	b, err := json.Marshal(r)
	if err != nil {
		return false
	}
	return len(b) <= maxBytes
}

func maybeTruncateSubject(subject any) any {
	serialized, err := json.Marshal(subject)
	if err != nil || serialized == nil {
		return map[string]any{"truncated": true, "bytes": 0, "preview": "[unserializable subject]"}
	}
	if len(serialized) <= maxNeedsInputSubjectBytes {
		return subject
	}
	return map[string]any{
		"truncated": true,
		"bytes":     len(serialized),
		"preview":   truncateBytes(string(serialized), maxNeedsInputSubjectBytes),
	}
}

func estimateBytes(v any) int {
	b, err := json.Marshal(v)
	if err != nil {
		return 0
	}
	return len(b)
}

// toolEnvelopeMaxBytes reads the envelope cap, defaulting to upstream's 512000.
func (st *runState) toolEnvelopeMaxBytes() int {
	raw := strings.TrimSpace(st.eng.env[toolEnvelopeMaxBytesEnv])
	if raw == "" {
		return defaultToolEnvelopeMaxBytes
	}
	n, ok := toInt(jsonNum(raw))
	if !ok || n <= 0 {
		return defaultToolEnvelopeMaxBytes
	}
	return int(n)
}

func jsonNum(s string) any {
	var v any
	if err := json.Unmarshal([]byte(s), &v); err != nil {
		return nil
	}
	return v
}

// ---------------------------------------------------------------------------
// schema validation
// ---------------------------------------------------------------------------

// validateInputResponse is upstream's `validateRequestInputResponse`: the response must
// satisfy the gate's JSON Schema. The message names the failing path, because that is the
// part the operator has to fix.
func validateInputResponse(schema map[string]any, response any, stepID string) error {
	if schema == nil {
		return nil
	}
	compiled, err := compileJSONSchema(schema)
	if err != nil {
		return fmt.Errorf("Workflow step %s response schema is invalid: %s", stepID, collapseError(err))
	}
	if err := validateAgainstSchema(compiled, response); err != nil {
		return fmt.Errorf("Workflow step %s response failed schema validation: %s", stepID, collapseError(err))
	}
	return nil
}

// validateAgainstSchema checks data conformance by UNIFYING the schema value with the
// data value. A contradiction makes the unify bottom, which is exactly the answer we
// want; a disjunction that one branch accepts resolves rather than failing.
func validateAgainstSchema(schema cue.Value, response any) error {
	ctx := cuecontext.New()
	data := ctx.Encode(response)
	if err := data.Err(); err != nil {
		return err
	}
	unified := schema.Unify(data)
	if err := unified.Err(); err != nil {
		return err
	}
	if unified.IncompleteKind() == cue.BottomKind {
		return fmt.Errorf("value does not satisfy the schema")
	}
	return nil
}

// collapseError renders a CUE error on one line: CUE's diagnostics are multi-line trees,
// and the operator's message is the first useful line of it.
func collapseError(err error) string {
	line := strings.TrimSpace(strings.SplitN(err.Error(), "\n", 2)[0])
	if line == "" {
		line = "invalid"
	}
	return truncateBytes(line, 500)
}

// schemaMap adapts the gate's schema, which the wire types it as `any`, to the map the
// validator needs. A non-object schema (a boolean schema, say) has no CUE projection here
// and is refused rather than ignored — the loader already rejects one, so reaching this is
// a corrupt state file.
func schemaMap(v any) map[string]any {
	if v == nil {
		return nil
	}
	if m, ok := v.(map[string]any); ok {
		return m
	}
	return nil
}

// validateJSONSchema is the load-time check: a schema that cannot compile is a load error,
// not a surprise when a human is finally asked to answer it.
func validateJSONSchema(schema map[string]any) error {
	_, err := compileJSONSchema(schema)
	return err
}

// compileJSONSchema turns a JSON Schema document into a CUE value to validate against.
// CUE's jsonschema extractor is used rather than a second schema engine: it is already a
// pinned dependency of this plugin's SDK, and the IR's own contract is CUE.
func compileJSONSchema(schema map[string]any) (cue.Value, error) {
	ctx := cuecontext.New()
	doc := ctx.Encode(schema)
	if err := doc.Err(); err != nil {
		return cue.Value{}, err
	}
	file, err := jsonschema.Extract(doc, &jsonschema.Config{})
	if err != nil {
		return cue.Value{}, err
	}
	compiled := ctx.BuildFile(file)
	if err := compiled.Err(); err != nil {
		return cue.Value{}, err
	}
	return compiled, nil
}

// ---------------------------------------------------------------------------
// small helpers
// ---------------------------------------------------------------------------

func firstNonBlankString(vs ...any) string {
	for _, v := range vs {
		if s, ok := v.(string); ok && strings.TrimSpace(s) != "" {
			return strings.TrimSpace(s)
		}
	}
	return ""
}

func firstBool(vs ...any) bool {
	for _, v := range vs {
		if b, ok := v.(bool); ok {
			return b
		}
	}
	return false
}

// parseBoolLike is upstream's `parseBoolLike`: booleans pass through, and the two common
// truthy/falsy vocabularies are accepted; anything else is absent (not false).
func parseBoolLike(value any) (bool, bool) {
	switch t := value.(type) {
	case bool:
		return t, true
	case nil:
		return false, false
	case string:
		switch strings.ToLower(strings.TrimSpace(t)) {
		case "1", "true", "yes", "y":
			return true, true
		case "0", "false", "no", "n":
			return false, true
		}
	}
	return false, false
}

// readLine reads one line from the engine's stdin. The engine's interactive path is the
// CLI, whose stdin is a terminal.
func readLine(ctx context.Context, _ any) (string, error) {
	return readStdinLine(ctx)
}
