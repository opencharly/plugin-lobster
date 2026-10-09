package pluginlobster

// pipeline.go — the `pipeline:` expression language (lobster's deterministic stdlib).
//
// A pipeline is a `|`-separated chain of stages over a stream of ITEMS. Each stage takes
// the previous stage's items and yields the next. Upstream ships 27 registered stages;
// this engine implements the DETERMINISTIC ones — filtering, shaping, rendering, and its
// own key/value state — and REFUSES the ones that need a model or an external service.
//
// The refusal is deliberate and is the engine wire's CLOSED doctrine applied at stage level: a
// stage that silently did nothing would run a workflow whose author believed a model had
// triaged their mail. So an external stage fails with an error that NAMES the charly
// alternative (`agent:`/`task:` steps, which go through the normal plugin dispatch), and
// a stage that is not a stage at all fails with upstream's own `Unknown command: <name>`.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/opencharly/sdk/kit"
)

// registry holds the shared state ONE engine's pipeline stages need: the shell that
// `exec` uses and the key/value store `state.get`/`state.set`/`diff.last` share.
type registry struct {
	shell shellRunner
	state *pipelineKV
}

func newRegistry() *registry {
	return &registry{shell: localShell{}, state: newPipelineKV(environMap())}
}

// externalStages are stages that exist upstream but need something this engine does not
// have: a model provider, or an OpenClaw/gog/email service. Each maps to the charly
// feature that DOES provide it, because "unsupported" without a next step is not a
// diagnostic.
var externalStages = map[string]string{
	"openclaw.invoke":  "use a workflow `task:`/plugin-verb step (charly dispatches it over the normal plugin path)",
	"clawd.invoke":     "use a workflow `agent:` step (charly's agent-runtime provider)",
	"openclaw.agent":   "use a workflow `agent:` step (charly's agent-runtime provider)",
	"llm.invoke":       "use a workflow `agent:` step (charly's agent-runtime provider)",
	"llm_task.invoke":  "use a workflow `agent:` step (charly's agent-runtime provider)",
	"gog.gmail.search": "use a workflow `charly:` command step against a mail plugin",
	"gog.gmail.send":   "use a workflow `charly:` command step against a mail plugin",
	"email.triage":     "use a workflow `agent:` step, or express the rules with `where`/`pick`",
	"workflows.run":    "use a workflow `workflow:` step (composition is a step, not a pipe stage)",
	"approve":          "use an `approval:` step, which persists a resumable token",
	"ask":              "use an `input:` step, which validates the answer against a JSON Schema",
}

// pipelineCommandNames is what `commands.list` reports: the stages this engine serves.
func (r *registry) names() []string {
	names := make([]string, 0, len(internalStages))
	for name := range internalStages {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// internalStages is the set this engine implements. Membership is the dispatch table.
var internalStages = map[string]bool{
	"exec": true, "json": true, "table": true, "where": true, "pick": true, "head": true,
	"tail": true, "sort": true, "dedupe": true, "map": true, "groupBy": true,
	"template": true, "state.get": true, "state.set": true, "diff.last": true,
	"commands.list": true, "workflows.list": true,
}

// runPipelineStep evaluates one `pipeline:` expression. It returns the final items and
// the rendered text (the stage output a caller would print, which a shell step treats as
// its stdout and therefore its `$id.json` when it parses).
func runPipelineStep(ctx context.Context, reg *registry, expr string, input any, env map[string]string, cwd, charlyBin string, stdout, stderr io.Writer) ([]any, string, error) {
	if reg == nil {
		return nil, "", fmt.Errorf("pipeline: no command registry")
	}
	stages := splitPipeline(expr)
	if len(stages) == 0 {
		return nil, "", fmt.Errorf("pipeline: empty expression")
	}

	items := toItems(input)
	scope := pipelineScope{reg: reg, env: env, cwd: cwd, charlyBin: charlyBin, stdout: stdout, stderr: stderr}
	var rendered []string
	lastText := ""

	for _, stageText := range stages {
		name, args, rawArgs, err := parseStage(stageText)
		if err != nil {
			return nil, "", err
		}
		if externalStages[name] != "" {
			return nil, "", fmt.Errorf("pipeline: command %q is not supported by the lobster engine: %s", name, externalStages[name])
		}
		if !internalStages[name] {
			// Upstream's own message, verbatim: the operator's next move is the same.
			return nil, "", fmt.Errorf("Unknown command: %s", name)
		}
		var text string
		items, text, err = runStage(ctx, scope, name, args, rawArgs, items)
		if err != nil {
			return nil, "", err
		}
		if text != "" {
			rendered = append(rendered, text)
		}
		lastText = text
	}

	out := strings.Join(rendered, "\n")
	// The LAST stage decides what the expression means. A stage that renders nothing of its
	// own (`where`, `sort`, `pick`, `head`, …) leaves the text of an EARLIER stage behind,
	// which is the wrong answer for `$id.stdout`: the pipeline reshaped the items after that
	// text was produced, so the final items are what this step's value is.
	if lastText == "" {
		out = renderItemsJSON(items)
	}
	return items, out, nil
}

// pipelineScope carries everything a stage may need beyond its items.
type pipelineScope struct {
	reg       *registry
	env       map[string]string
	cwd       string
	charlyBin string
	stdout    io.Writer
	stderr    io.Writer
}

// ---------------------------------------------------------------------------
// stage dispatch
// ---------------------------------------------------------------------------

// raw is the stage's text after the command word, with its ORIGINAL quoting intact. Only
// the stages that hand text to another interpreter (`exec`) use it: rebuilding a command
// from the unquoted field list would strip the very quotes that keep a JSON argument one
// word, and a shell would brace-expand it into many.
func runStage(ctx context.Context, s pipelineScope, name string, args []string, raw string, items []any) ([]any, string, error) {
	switch name {
	case "exec":
		return stageExec(ctx, s, raw, items)
	case "json":
		return items, renderItemsJSON(items), nil
	case "table":
		return items, renderTable(items), nil
	case "head":
		return stageHead(args, items, false)
	case "tail":
		return stageHead(args, items, true)
	case "where":
		return stageWhere(args, items)
	case "pick":
		return stagePick(args, items)
	case "sort":
		return stageSort(args, items)
	case "dedupe":
		return stageDedupe(args, items)
	case "map":
		return stageMap(args, items)
	case "groupBy":
		return stageGroupBy(args, items)
	case "template":
		return stageTemplate(args, items)
	case "state.get":
		return stageStateGet(s, args, items)
	case "state.set":
		return stageStateSet(s, args, items)
	case "diff.last":
		return stageDiffLast(s, items)
	case "commands.list":
		list := s.reg.names()
		out := make([]any, 0, len(list))
		for _, n := range list {
			out = append(out, n)
		}
		return out, renderItemsJSON(out), nil
	case "workflows.list":
		return stageWorkflowsList(s, items)
	}
	return nil, "", fmt.Errorf("Unknown command: %s", name)
}

// ---------------------------------------------------------------------------
// stages
// ---------------------------------------------------------------------------

// stageExec runs a shell command with the current items as JSON on stdin and parses its
// stdout: a JSON array becomes the items, a JSON object becomes one item, a bare scalar
// becomes one item, and anything else becomes one item per line.
func stageExec(ctx context.Context, s pipelineScope, command string, items []any) ([]any, string, error) {
	command = strings.TrimSpace(command)
	if command == "" {
		return nil, "", fmt.Errorf("pipeline: exec requires a command")
	}

	var stdin *string
	if len(items) > 0 {
		b, err := json.Marshal(items)
		if err != nil {
			return nil, "", fmt.Errorf("pipeline: encode exec stdin: %w", err)
		}
		v := string(b)
		stdin = &v
	}

	stdout, stderr, code, err := s.reg.shell.Run(ctx, command, stdin, s.env, s.cwd, 0)
	if err != nil {
		return nil, "", err
	}
	if code != 0 {
		detail := strings.TrimSpace(stderr)
		if detail == "" {
			detail = strings.TrimSpace(stdout)
		}
		return nil, "", fmt.Errorf("pipeline exec failed (%d): %s", code, truncateBytes(detail, 2000))
	}

	parsed, ok := parseJSON(stdout)
	if ok {
		return toItems(parsed), stdout, nil
	}
	trimmed := strings.TrimSpace(stdout)
	if trimmed == "" {
		return []any{}, stdout, nil
	}
	lines := strings.Split(trimmed, "\n")
	out := make([]any, 0, len(lines))
	for _, l := range lines {
		out = append(out, l)
	}
	return out, stdout, nil
}

func stageHead(args []string, items []any, fromTail bool) ([]any, string, error) {
	n := 10
	if len(args) > 0 {
		parsed, err := strconv.Atoi(strings.TrimSpace(args[0]))
		if err != nil || parsed < 0 {
			// A negative count is upstream's `tail -n` idiom; a non-number is an error.
			if strings.HasPrefix(strings.TrimSpace(args[0]), "-") {
				parsed, err = strconv.Atoi(strings.TrimPrefix(strings.TrimSpace(args[0]), "-"))
				if err == nil {
					fromTail = !fromTail
				}
			}
			if err != nil || parsed < 0 {
				return nil, "", fmt.Errorf("pipeline: head/tail requires a non-negative count, got %q", args[0])
			}
		}
		n = parsed
	}
	if n > len(items) {
		n = len(items)
	}
	if fromTail {
		return items[len(items)-n:], "", nil
	}
	return items[:n], "", nil
}

func stageWhere(args []string, items []any) ([]any, string, error) {
	expr := strings.TrimSpace(strings.Join(args, " "))
	if expr == "" {
		return nil, "", fmt.Errorf("pipeline: where requires a predicate")
	}
	out := make([]any, 0, len(items))
	for _, it := range items {
		ok, err := evalPredicate(expr, it)
		if err != nil {
			return nil, "", err
		}
		if ok {
			out = append(out, it)
		}
	}
	return out, "", nil
}

func stagePick(args []string, items []any) ([]any, string, error) {
	if len(args) == 0 {
		return nil, "", fmt.Errorf("pipeline: pick requires at least one field")
	}
	fields := strings.Split(strings.Join(args, " "), ",")
	for i := range fields {
		fields[i] = strings.TrimSpace(fields[i])
	}
	out := make([]any, 0, len(items))
	for _, it := range items {
		if obj, ok := it.(map[string]any); ok {
			kept := map[string]any{}
			for _, f := range fields {
				if f == "" {
					continue
				}
				if v, ok := obj[f]; ok {
					kept[f] = v
				}
			}
			out = append(out, kept)
			continue
		}
		out = append(out, it)
	}
	return out, "", nil
}

func stageSort(args []string, items []any) ([]any, string, error) {
	path := ""
	desc := false
	for _, a := range args {
		a = strings.TrimSpace(a)
		switch a {
		case "desc", "-r", "--reverse":
			desc = true
		case "asc":
			desc = false
		case "":
		default:
			path = strings.TrimPrefix(a, ".")
		}
	}
	out := append([]any{}, items...)
	sort.SliceStable(out, func(i, j int) bool {
		a, b := sortKey(out[i], path), sortKey(out[j], path)
		less := compareValues(a, b) < 0
		if desc {
			return !less && compareValues(a, b) != 0
		}
		return less
	})
	return out, "", nil
}

func stageDedupe(args []string, items []any) ([]any, string, error) {
	path := ""
	if len(args) > 0 {
		path = strings.TrimPrefix(strings.TrimSpace(args[0]), ".")
	}
	seen := map[string]bool{}
	out := make([]any, 0, len(items))
	for _, it := range items {
		var key string
		if path == "" {
			b, err := json.Marshal(it)
			if err != nil {
				return nil, "", err
			}
			key = string(b)
		} else {
			key = renderTemplateValue(getPathValue(it, path))
		}
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, it)
	}
	return out, "", nil
}

func stageMap(args []string, items []any) ([]any, string, error) {
	path := strings.TrimSpace(strings.Join(args, " "))
	if path == "" {
		return nil, "", fmt.Errorf("pipeline: map requires a path")
	}
	path = strings.TrimPrefix(path, ".")
	out := make([]any, 0, len(items))
	for _, it := range items {
		out = append(out, getPathValue(it, path))
	}
	return out, "", nil
}

func stageGroupBy(args []string, items []any) ([]any, string, error) {
	if len(args) == 0 {
		return nil, "", fmt.Errorf("pipeline: groupBy requires a path")
	}
	path := strings.TrimPrefix(strings.TrimSpace(args[0]), ".")
	groups := map[string][]any{}
	for _, it := range items {
		key := renderTemplateValue(getPathValue(it, path))
		groups[key] = append(groups[key], it)
	}
	keys := make([]string, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	grouped := map[string]any{}
	for _, k := range keys {
		grouped[k] = groups[k]
	}
	return []any{grouped}, "", nil
}

func stageTemplate(args []string, items []any) ([]any, string, error) {
	text := strings.Join(args, " ")
	if text == "" {
		return nil, "", fmt.Errorf("pipeline: template requires text")
	}
	out := make([]any, 0, len(items))
	for _, it := range items {
		out = append(out, renderMustache(text, it))
	}
	return out, "", nil
}

func stageStateGet(s pipelineScope, args []string, items []any) ([]any, string, error) {
	if len(args) == 0 {
		return nil, "", fmt.Errorf("pipeline: state.get requires a key")
	}
	v, ok, err := s.reg.state.get(ctxOf(s), args[0])
	if err != nil {
		return nil, "", err
	}
	if !ok {
		return []any{}, "", nil
	}
	if arr, isArr := v.([]any); isArr {
		return arr, "", nil
	}
	return []any{v}, "", nil
}

func stageStateSet(s pipelineScope, args []string, items []any) ([]any, string, error) {
	if len(args) == 0 {
		return nil, "", fmt.Errorf("pipeline: state.set requires a key")
	}
	if err := s.reg.state.set(ctxOf(s), args[0], items); err != nil {
		return nil, "", err
	}
	return items, "", nil
}

// stageDiffLast yields the items whose content differs from the previous run.
func stageDiffLast(s pipelineScope, items []any) ([]any, string, error) {
	b, err := json.Marshal(items)
	if err != nil {
		return nil, "", err
	}
	sum := sha256.Sum256(b)
	digest := hex.EncodeToString(sum[:])
	prev, ok, err := s.reg.state.get(ctxOf(s), "diff.last")
	if err != nil {
		return nil, "", err
	}
	if err := s.reg.state.set(ctxOf(s), "diff.last", digest); err != nil {
		return nil, "", err
	}
	if ok && prev == digest {
		return []any{}, "", nil
	}
	return items, "", nil
}

// stageWorkflowsList lists the lowered pipelines in the project's generated state.
func stageWorkflowsList(s pipelineScope, items []any) ([]any, string, error) {
	dir := filepath.Join(s.cwd, ".opencharly", "pipelines")
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return []any{}, "", nil
		}
		return nil, "", err
	}
	out := make([]any, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			out = append(out, e.Name())
		}
	}
	sort.Slice(out, func(i, j int) bool { return renderTemplateValue(out[i]) < renderTemplateValue(out[j]) })
	return out, renderItemsJSON(out), nil
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// ctxOf gives a background context to the store calls that have no cancellation to
// honour — a key/value read is not interruptible work.
func ctxOf(_ pipelineScope) context.Context { return context.Background() }

// splitPipeline splits an expression on top-level `|`, respecting quotes.
func splitPipeline(expr string) []string {
	var out []string
	var b strings.Builder
	var quote rune
	for _, r := range expr {
		switch {
		case quote != 0:
			b.WriteRune(r)
			if r == quote {
				quote = 0
			}
		case r == '\'' || r == '"':
			quote = r
			b.WriteRune(r)
		case r == '|':
			out = append(out, b.String())
			b.Reset()
		default:
			b.WriteRune(r)
		}
	}
	out = append(out, b.String())
	trimmed := make([]string, 0, len(out))
	for _, s := range out {
		if strings.TrimSpace(s) != "" {
			trimmed = append(trimmed, strings.TrimSpace(s))
		}
	}
	return trimmed
}

// parseStage splits one stage into its command name, its unquoted arguments, and the RAW
// remainder after the command name.
func parseStage(stage string) (string, []string, string, error) {
	fields, err := splitFields(stage)
	if err != nil {
		return "", nil, "", err
	}
	if len(fields) == 0 {
		return "", nil, "", fmt.Errorf("pipeline: empty stage")
	}
	return fields[0], fields[1:], stageRemainder(stage), nil
}

// stageRemainder is everything after the command word, quoted text included.
func stageRemainder(stage string) string {
	var quote rune
	for i, r := range stage {
		switch {
		case quote != 0:
			if r == quote {
				quote = 0
			}
		case r == '\'' || r == '"':
			quote = r
		case r == ' ' || r == '\t' || r == '\n':
			return strings.TrimSpace(stage[i:])
		}
	}
	return ""
}

// splitFields splits on whitespace, honouring single and double quotes.
func splitFields(s string) ([]string, error) {
	var out []string
	var b strings.Builder
	var quote rune
	inField := false
	for _, r := range s {
		switch {
		case quote != 0:
			if r == quote {
				quote = 0
				continue
			}
			b.WriteRune(r)
		case r == '\'' || r == '"':
			quote = r
			inField = true
		case r == ' ' || r == '\t' || r == '\n':
			if inField || b.Len() > 0 {
				out = append(out, b.String())
				b.Reset()
				inField = false
			}
		default:
			b.WriteRune(r)
			inField = true
		}
	}
	if quote != 0 {
		return nil, fmt.Errorf("pipeline: unterminated quote in %q", s)
	}
	if inField || b.Len() > 0 {
		out = append(out, b.String())
	}
	return out, nil
}

// toItems normalizes a stage input into a stream: an array spreads, anything else is one
// item, and nil is empty.
func toItems(v any) []any {
	switch t := v.(type) {
	case nil:
		return []any{}
	case []any:
		return t
	default:
		return []any{t}
	}
}

func renderItemsJSON(items []any) string {
	b, err := json.MarshalIndent(items, "", "  ")
	if err != nil {
		return ""
	}
	return string(b)
}

func renderTable(items []any) string {
	if len(items) == 0 {
		return ""
	}
	rows := make([][]string, 0, len(items))
	for _, it := range items {
		obj, ok := it.(map[string]any)
		if !ok {
			rows = append(rows, []string{renderTemplateValue(it)})
			continue
		}
		keys := make([]string, 0, len(obj))
		for k := range obj {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		row := make([]string, 0, len(keys))
		for _, k := range keys {
			row = append(row, renderTemplateValue(obj[k]))
		}
		rows = append(rows, row)
	}
	var b strings.Builder
	for i, r := range rows {
		if i > 0 {
			b.WriteString("\n")
		}
		b.WriteString(strings.Join(r, "\t"))
	}
	return b.String()
}

// sortKey extracts a comparable value for ordering.
func sortKey(item any, path string) any {
	if path == "" {
		return item
	}
	return getPathValue(item, path)
}

// compareValues orders two values: numbers numerically, everything else as text.
func compareValues(a, b any) int {
	af, aok := toFloat(a)
	bf, bok := toFloat(b)
	if aok && bok {
		switch {
		case af < bf:
			return -1
		case af > bf:
			return 1
		default:
			return 0
		}
	}
	as, bs := renderTemplateValue(a), renderTemplateValue(b)
	switch {
	case as < bs:
		return -1
	case as > bs:
		return 1
	default:
		return 0
	}
}

// evalPredicate evaluates a `where` predicate: `<path> <op> <value>`, or a bare path
// tested for truthiness.
func evalPredicate(expr string, item any) (bool, error) {
	fields, err := splitFields(expr)
	if err != nil {
		return false, err
	}
	if len(fields) == 0 {
		return false, fmt.Errorf("pipeline: empty predicate")
	}
	left := getPathValue(item, strings.TrimPrefix(fields[0], "."))
	if len(fields) == 1 {
		return truthy(left), nil
	}
	if len(fields) < 3 {
		return false, fmt.Errorf("pipeline: predicate %q needs <path> <op> <value>", expr)
	}
	op := fields[1]
	raw := strings.Join(fields[2:], " ")

	var want any = raw
	if parsed, ok := parseJSON(raw); ok {
		want = parsed
	}

	switch op {
	case "==", "=":
		return compareValues(left, want) == 0, nil
	case "!=":
		return compareValues(left, want) != 0, nil
	case ">", ">=", "<", "<=":
		c := compareValues(left, want)
		switch op {
		case ">":
			return c > 0, nil
		case ">=":
			return c >= 0, nil
		case "<":
			return c < 0, nil
		default:
			return c <= 0, nil
		}
	case "contains":
		return strings.Contains(renderTemplateValue(left), renderTemplateValue(want)), nil
	case "startswith":
		return strings.HasPrefix(renderTemplateValue(left), renderTemplateValue(want)), nil
	case "endswith":
		return strings.HasSuffix(renderTemplateValue(left), renderTemplateValue(want)), nil
	case "matches":
		return strings.Contains(renderTemplateValue(left), renderTemplateValue(want)), nil
	}
	return false, fmt.Errorf("pipeline: unknown predicate operator %q", op)
}

func truthy(v any) bool {
	switch t := v.(type) {
	case nil:
		return false
	case bool:
		return t
	case string:
		return strings.TrimSpace(t) != ""
	case float64:
		return t != 0
	case []any:
		return len(t) > 0
	case map[string]any:
		return len(t) > 0
	default:
		return true
	}
}

// renderMustache substitutes `{{path}}` from an item.
func renderMustache(text string, item any) string {
	var b strings.Builder
	for {
		i := strings.Index(text, "{{")
		if i < 0 {
			b.WriteString(text)
			break
		}
		j := strings.Index(text[i:], "}}")
		if j < 0 {
			b.WriteString(text)
			break
		}
		b.WriteString(text[:i])
		path := strings.TrimSpace(text[i+2 : i+j])
		b.WriteString(renderTemplateValue(getPathValue(item, strings.TrimPrefix(path, "."))))
		text = text[i+j+2:]
	}
	return b.String()
}

// ---------------------------------------------------------------------------
// the key/value store
// ---------------------------------------------------------------------------

// pipelineKV is the tiny JSON-file key/value store `state.get`/`state.set`/`diff.last`
// share. It lives in the engine's state dir, so a pipeline's state sits with the run
// state it belongs to rather than in an unrelated location.
type pipelineKV struct {
	path string
}

func newPipelineKV(env map[string]string) *pipelineKV {
	return &pipelineKV{path: filepath.Join(defaultStateDir(env), "pipeline-state.json")}
}

func (kv *pipelineKV) read() (map[string]any, error) {
	b, err := os.ReadFile(kv.path)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]any{}, nil
		}
		return nil, err
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("pipeline state: %w", err)
	}
	return m, nil
}

func (kv *pipelineKV) get(_ context.Context, key string) (any, bool, error) {
	m, err := kv.read()
	if err != nil {
		return nil, false, err
	}
	v, ok := m[key]
	return v, ok, nil
}

func (kv *pipelineKV) set(_ context.Context, key string, value any) error {
	if err := os.MkdirAll(filepath.Dir(kv.path), 0o700); err != nil {
		return err
	}
	m, err := kv.read()
	if err != nil {
		return err
	}
	m[key] = value
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return kit.AtomicWriteFile(kv.path, b, 0o600)
}
