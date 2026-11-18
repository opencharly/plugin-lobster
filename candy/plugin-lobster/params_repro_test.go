package pluginlobster

// params_repro_test.go — the committed `params/cue_types_gen.go` IS the artifact of the schema, and
// this is the gate that keeps it one (P03: a generated file that nothing re-checks goes stale
// silently, and a stale params file ships a wire contract that no longer matches the CUE source).
//
// The generator is `spec/schemaparams.Generate` — the SAME function `charly candy params` calls — so
// the gate judges the committed file against the canonical pipeline rather than a second
// re-implementation. Two properties, one test: the committed file is CURRENT, and a second run is
// byte-identical.
import (
	"bytes"
	"os"
	"testing"

	"github.com/opencharly/spec/schemaparams"
)

func TestGeneratedParamsAreCurrentAndReproducible(t *testing.T) {
	committed, err := os.ReadFile("params/cue_types_gen.go")
	if err != nil {
		t.Fatalf("reading the committed params: %v", err)
	}
	first, err := schemaparams.Generate("schema", "params")
	if err != nil {
		t.Fatalf("regenerating params from schema/: %v", err)
	}
	if !bytes.Equal(first, committed) {
		t.Fatalf("params/cue_types_gen.go is STALE: regenerate it with `charly candy params plugin-lobster` (schema/ is the source; the file is generated, never hand-edited)")
	}
	second, err := schemaparams.Generate("schema", "params")
	if err != nil {
		t.Fatalf("second regeneration: %v", err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("the schema→params pipeline is NOT reproducible: two runs of spec/schemaparams.Generate differ")
	}
}
