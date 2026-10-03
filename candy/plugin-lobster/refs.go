package pluginlobster

// refs.go — upstream lobster's ref/value semantics: `${arg}` templating, `$id.path` step
// references, the per-step value walk, and `when` evaluation.
//
// Transcribed from upstream `src/workflows/expressions.ts` + `src/workflows/values.ts`.
// The LOOKUP ROOT for a reference is the step's RESULT object (its `stdout` / `json` /
// `response` / `approved` / `skipped` / … fields), NOT the raw output — so `$s.json.a` and
// `$s.stdout` are the two documented ways in, and `$s.approved` is the gate's.
//
// `${arg}` is resolved FIRST, then `$id.path`; `${…}` reads ARGS ONLY and never a step
// result. A `$id.path` whose id is UNKNOWN is left literally in place (upstream); a known id
// with a missing path renders "" (or "false" for `approved`/`skipped`).
//
// `when` evaluation is delegated to `workflowkit.Eval` — the ONE lobster expression
// grammar (expr-lang + the grammar-enforcing AST patcher) shared with the front-end and
// the migration hook, rather than a second evaluator here (R3).

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/opencharly/sdk/workflowkit"
)

var (
	argTemplateRe = regexp.MustCompile(`\$\{([A-Za-z0-9_-]+)\}`)
	stepRefInline = regexp.MustCompile(`\$([A-Za-z0-9_-]+)\.([A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+)*)`)
	stepRefWhole  = regexp.MustCompile(`^\$([A-Za-z0-9_-]+)\.([A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+)*)$`)
)

// stepResult is one step's recorded outcome — the ref root. Fields mirror upstream's
// WorkflowStepResult, plus the fields charly records (exit code, attempt count).
type stepResult struct {
	ID           string
	Stdout       string
	JSON         any
	HasJSON      bool
	Response     any
	HasResp      bool
	Subject      any
	HasSubject   bool
	Approved     *bool
	ApprovedBy   string
	InitiatedBy  string
	Skipped      bool
	Error        bool
	ErrorMessage string
	ExitCode     int
	Stderr       string
	Attempts     int64
	DurationMs   int64
}

// envelope renders the step's ref root as the JSON document `workflowkit.Eval` walks with
// gjson. Only present fields are emitted, so `$s.json` on a step that produced no JSON
// stays absent rather than becoming an explicit null (upstream's `undefined`).
func (r *stepResult) envelope() string {
	m := map[string]any{"id": r.ID}
	if r.Stdout != "" {
		m["stdout"] = r.Stdout
	}
	if r.HasJSON && r.JSON != nil {
		m["json"] = r.JSON
	}
	if r.HasResp {
		m["response"] = r.Response
	}
	if r.HasSubject {
		m["subject"] = r.Subject
	}
	if r.Approved != nil {
		m["approved"] = *r.Approved
	}
	if r.ApprovedBy != "" {
		m["approvedBy"] = r.ApprovedBy
	}
	if r.InitiatedBy != "" {
		m["initiatedBy"] = r.InitiatedBy
	}
	if r.Skipped {
		m["skipped"] = true
	}
	if r.Error {
		m["error"] = true
		m["errorMessage"] = r.ErrorMessage
	}
	b, err := json.Marshal(m)
	if err != nil {
		return "{}"
	}
	return string(b)
}

// results is the ref scope: step id → result.
type results map[string]*stepResult

// scope builds the Eval scope (id → the JSON envelope).
func (rs results) scope() workflowkit.Scope {
	s := make(workflowkit.Scope, len(rs))
	for id, r := range rs {
		s[id] = r.envelope()
	}
	return s
}

// scoped is a shallow COPY of the results with extra synthetic entries (a for_each loop's
// `item`/`index`). Upstream does exactly this: the loop variables are refs, not env vars.
func (rs results) scoped(extra map[string]*stepResult) results {
	out := make(results, len(rs)+len(extra))
	for k, v := range rs {
		out[k] = v
	}
	for k, v := range extra {
		out[k] = v
	}
	return out
}

// resolveArgsTemplate is upstream's `resolveArgsTemplate`: `${key}` → the arg's string
// form, and a key that is NOT an arg is left literally in place.
func resolveArgsTemplate(input string, args map[string]any) string {
	return argTemplateRe.ReplaceAllStringFunc(input, func(m string) string {
		key := m[2 : len(m)-1]
		if v, ok := args[key]; ok {
			return renderTemplateValue(v)
		}
		return m
	})
}

// resolveStepRefs is upstream's `resolveStepRefs` (the INLINE, non-strict pass).
func resolveStepRefs(input string, rs results) string {
	return stepRefInline.ReplaceAllStringFunc(input, func(m string) string {
		sub := stepRefInline.FindStringSubmatch(m)
		id, path := sub[1], sub[2]
		r, ok := rs[id]
		if !ok {
			return m // unknown id: the literal text survives
		}
		v := getValueByPath(r, path)
		if v == nil {
			if path == "approved" || path == "skipped" {
				return "false"
			}
			return ""
		}
		return renderTemplateValue(v)
	})
}

// resolveTemplate = `${arg}` then `$id.path` — upstream's order, and the ONE entry point
// every templated field uses.
func resolveTemplate(input string, args map[string]any, rs results) string {
	return resolveStepRefs(resolveArgsTemplate(input, args), rs)
}

// parseStepRef is upstream's `parseStepRef`: the WHOLE trimmed string must be `$id.path`.
func parseStepRef(value string) (id, path string, ok bool) {
	m := stepRefWhole.FindStringSubmatch(strings.TrimSpace(value))
	if m == nil {
		return "", "", false
	}
	return m[1], m[2], true
}

// resolveInputValue resolves a `stdin`/`for_each` operand: a whole-string ref is resolved
// STRICTLY (an unknown id is an error, not a literal), any other string is templated, and
// a non-string value passes through untouched.
func resolveInputValue(raw any, args map[string]any, rs results) (any, error) {
	if raw == nil {
		return nil, nil
	}
	s, ok := raw.(string)
	if !ok {
		return raw, nil
	}
	if id, path, isRef := parseStepRef(s); isRef {
		r, ok := rs[id]
		if !ok {
			return nil, fmt.Errorf("Unknown step reference: %s.%s", id, path)
		}
		return getValueByPath(r, path), nil
	}
	return resolveTemplate(s, args, rs), nil
}

// encodeShellInput is upstream's `encodeShellInput`: nil → no write, string → verbatim,
// anything else → JSON.
func encodeShellInput(v any) *string {
	if v == nil {
		return nil
	}
	if s, ok := v.(string); ok {
		return &s
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	s := string(b)
	return &s
}

// getValueByPath walks a dotted path where the FIRST segment names one of the result
// object's fields (`stdout`, `json`, `response`, `subject`, `approved`, …) and the REST is
// a walk INSIDE that field's value, indexing arrays with a numeric segment. A first
// segment that is none of those names is a shorthand for `json`, so the whole path walks
// the parsed stdout. A missing/null/non-object hop yields nil, exactly like upstream's
// `getValueByPath`.
func getValueByPath(r *stepResult, path string) any {
	segments := strings.Split(path, ".")
	rest := segments[1:]
	var cur any
	switch head := segments[0]; head {
	case "stdout":
		cur = r.Stdout
	case "stderr":
		cur = r.Stderr
	case "id":
		cur = r.ID
	case "json":
		if !r.HasJSON {
			return nil
		}
		cur = r.JSON
	case "response":
		if !r.HasResp {
			return nil
		}
		cur = r.Response
	case "subject":
		if !r.HasSubject {
			return nil
		}
		cur = r.Subject
	case "approved":
		if r.Approved == nil {
			return nil
		}
		cur = *r.Approved
	case "approvedBy":
		if r.ApprovedBy == "" {
			return nil
		}
		cur = r.ApprovedBy
	case "initiatedBy":
		if r.InitiatedBy == "" {
			return nil
		}
		cur = r.InitiatedBy
	case "skipped":
		if !r.Skipped {
			return nil
		}
		cur = true
	case "error":
		if !r.Error {
			return nil
		}
		cur = true
	case "errorMessage":
		if !r.Error {
			return nil
		}
		cur = r.ErrorMessage
	case "exit_code":
		cur = r.ExitCode
	default:
		// Not a result-object field: the whole path is a walk inside the parsed stdout, so
		// `$s.name` is the documented shorthand for `$s.json.name`.
		if !r.HasJSON {
			return nil
		}
		cur = r.JSON
		rest = segments
	}
	for _, field := range rest {
		if cur == nil {
			return nil
		}
		switch node := cur.(type) {
		case []any:
			idx, ok := parseIndex(field)
			if !ok || idx < 0 || idx >= len(node) {
				return nil
			}
			cur = node[idx]
		case map[string]any:
			cur = node[field]
		default:
			return nil
		}
	}
	return cur
}

// getPathValue is the plain walk used by the pipeline stdlib and by `map`, from an
// arbitrary root (no result-object special casing).
func getPathValue(root any, path string) any {
	cur := root
	for _, field := range strings.Split(path, ".") {
		if field == "" {
			continue
		}
		if cur == nil {
			return nil
		}
		switch node := cur.(type) {
		case []any:
			idx, ok := parseIndex(field)
			if !ok || idx < 0 || idx >= len(node) {
				return nil
			}
			cur = node[idx]
		case map[string]any:
			cur = node[field]
		default:
			return nil
		}
	}
	return cur
}

func parseIndex(s string) (int, bool) {
	if s == "" {
		return 0, false
	}
	n := 0
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, false
		}
		n = n*10 + int(s[i]-'0')
	}
	return n, true
}

// renderTemplateValue is upstream's `renderTemplateValue`: strings raw, scalars String()ed,
// objects/arrays JSON.
func renderTemplateValue(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case bool:
		if t {
			return "true"
		}
		return "false"
	case float64:
		return trimFloat(t)
	case json.Number:
		return t.String()
	case nil:
		return ""
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return fmt.Sprintf("%v", v)
		}
		return string(b)
	}
}

// trimFloat renders a JSON number the way JS `String()` does: an integral float has no
// ".0" suffix (upstream's values come from JSON.parse, so 3 is 3 and 3.5 is 3.5).
func trimFloat(f float64) string {
	if f == float64(int64(f)) {
		return fmt.Sprintf("%d", int64(f))
	}
	return fmt.Sprintf("%g", f)
}

// evaluateWhen is `when ?? condition`: nil → true, a bool → itself, "true"/"false"
// literally, else the workflowkit expression grammar against the step envelope scope.
func evaluateWhen(condition any, rs results) (bool, error) {
	if condition == nil {
		return true, nil
	}
	switch c := condition.(type) {
	case bool:
		return c, nil
	case string:
		trimmed := strings.TrimSpace(c)
		if trimmed == "" {
			return true, nil
		}
		if trimmed == "true" {
			return true, nil
		}
		if trimmed == "false" {
			return false, nil
		}
		return workflowkit.Eval(trimmed, rs.scope())
	default:
		return false, fmt.Errorf("Unsupported condition type %T", condition)
	}
}

// truncateBytes caps a string to n bytes on a rune boundary (upstream's `.slice(0, 2000)`
// for previews; Go must not split a rune).
func truncateBytes(s string, n int) string {
	if len(s) <= n {
		return s
	}
	cut := n
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut]
}
