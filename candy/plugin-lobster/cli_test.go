package pluginlobster

// cli_test.go — the `command:lobster` import face. The conversion is a RE-SHAPING of the
// workflow level (lobster's `name` becomes the pipeline `description`, the engine is
// pinned, every step and every other key is carried through), so these tests pin exactly
// that: nothing invented, nothing dropped.

import (
	"bytes"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// importEntity runs cliImport against a written workflow and decodes the emitted YAML.
func importEntity(t *testing.T, body string) map[string]any {
	t.Helper()
	out := &bytes.Buffer{}
	if code, err := cliImport([]string{writeWorkflow(t, body)}, out); err != nil || code != 0 {
		t.Fatalf("cliImport = (%d, %v), want (0, nil)", code, err)
	}
	if out.Len() == 0 {
		t.Fatal("cliImport wrote nothing")
	}
	var entity map[string]any
	if err := yaml.Unmarshal(out.Bytes(), &entity); err != nil {
		t.Fatalf("emitted YAML does not decode: %v\n%s", err, out.String())
	}
	return entity
}

func TestCLIImportCarriesTheStepsThrough(t *testing.T) {
	entity := importEntity(t, `
name: nightly
args:
  who: {default: world}
steps:
  - id: produce
    run: 'echo hi'
  - id: gate
    approval: required
`)
	if entity["engine"] != "lobster" {
		t.Fatalf("engine = %v, want lobster", entity["engine"])
	}
	// lobster's `name` is not an authored pipeline key; it becomes the description.
	if entity["description"] != "nightly" {
		t.Fatalf("description = %v, want the lobster name", entity["description"])
	}
	if _, carried := entity["name"]; carried {
		t.Fatalf("the lobster `name` key survived: %v", entity)
	}
	steps, ok := entity["steps"].([]any)
	if !ok || len(steps) != 2 {
		t.Fatalf("steps = %#v, want the two authored steps", entity["steps"])
	}
	first, _ := steps[0].(map[string]any)
	if first["id"] != "produce" || first["run"] != "echo hi" {
		t.Fatalf("first step = %#v", steps[0])
	}
	second, _ := steps[1].(map[string]any)
	if second["id"] != "gate" || second["approval"] != "required" {
		t.Fatalf("second step = %#v", steps[1])
	}
	// `args` is authored pipeline grammar too, so it is carried verbatim.
	args, ok := entity["args"].(map[string]any)
	if !ok {
		t.Fatalf("args = %#v, want the declared args carried through", entity["args"])
	}
	if _, ok := args["who"]; !ok {
		t.Fatalf("args lost its `who` entry: %#v", args)
	}
}

func TestCLIImportDescriptionWins(t *testing.T) {
	// An explicit description is the authored field, so it is used as-is.
	entity := importEntity(t, "name: nightly\ndescription: Nightly CI\nsteps:\n  - id: a\n    run: 'true'\n")
	if entity["description"] != "Nightly CI" {
		t.Fatalf("description = %v, want the explicit one", entity["description"])
	}
}

func TestCLIImportFallsBackToTheFilename(t *testing.T) {
	// Neither `description` nor `name`: the origin is named, so an operator can tell where
	// an imported entity came from.
	entity := importEntity(t, "steps:\n  - id: a\n    run: 'true'\n")
	desc, _ := entity["description"].(string)
	if !strings.HasPrefix(desc, "Imported from ") || !strings.Contains(desc, "workflow.lobster") {
		t.Fatalf("description = %q, want the file it came from", desc)
	}
}

func TestCLIImportRejectsABrokenWorkflow(t *testing.T) {
	// The loader runs BEFORE anything is emitted, so a workflow the engine cannot run is
	// never written out as an entity.
	out := &bytes.Buffer{}
	code, err := cliImport([]string{writeWorkflow(t, "steps:\n  - id: a\n    run: 'true'\n    pipeline: 'exec'\n")}, out)
	if err != nil {
		t.Fatalf("cliImport returned a Go error: %v", err)
	}
	if code == 0 {
		t.Fatalf("a two-exec-arm workflow was imported (exit 0):\n%s", out.String())
	}
	if out.Len() != 0 {
		t.Fatalf("a rejected workflow still wrote an entity:\n%s", out.String())
	}
}

func TestCLIImportRequiresExactlyOneArgument(t *testing.T) {
	for _, args := range [][]string{{}, {"a.lobster", "b.lobster"}} {
		if code, _ := cliImport(args, &bytes.Buffer{}); code != 2 {
			t.Fatalf("cliImport(%v) = %d, want usage exit 2", args, code)
		}
	}
}

func TestCLIUnknownCommandIsAUsageError(t *testing.T) {
	if code, err := runCLI([]string{"nope"}, nil); code != 2 || err != nil {
		t.Fatalf("runCLI = (%d, %v), want (2, nil)", code, err)
	}
	if code, err := runCLI(nil, nil); code != 2 || err != nil {
		t.Fatalf("runCLI(nil) = (%d, %v), want (2, nil)", code, err)
	}
}
