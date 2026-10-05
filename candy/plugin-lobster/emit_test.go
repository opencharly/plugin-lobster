package pluginlobster

// emit_test.go — the `workflow-emit` op (`ops.OpWorkflowEmit` → `emitWorkflow`). The
// engine advertises four ops and this is the one that re-materializes the lowered pair
// for inspection, so it is driven here through the REAL provider dispatch
// (`NewProvider().Invoke`) rather than by calling `emitWorkflow` directly: one test then
// covers both the dispatch arm in plugin.go and the copy/refusal logic in run.go, which
// is exactly the pair the block named as having zero coverage.
//
// What is pinned: the happy path for every advertised format (an entry per format, byte-
// identical to the lowered source), the `out_dir` default, and each refusal — a refusal
// that silently wrote nothing, or that wrote a file for a format it cannot actually
// produce, is the silent-drop failure the IR's own contract forbids.

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/opencharly/spec/ops"
	pb "github.com/opencharly/spec/proto"
	"github.com/opencharly/spec/spec"
)

// genPair writes the lowered pair for `pipeline` into the gen dir the emit op reads from
// (resolveGenDir with an empty gen_dir: <cwd>/.opencharly/pipelines/<pipeline>) and
// returns that absolute path. An empty argument writes no file, so a test can leave one
// half of the pair missing on purpose.
func genPair(t *testing.T, pipeline, lobster, charlyYML string) string {
	t.Helper()
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	gen := filepath.Join(cwd, ".opencharly", "pipelines", pipeline)
	if err := os.MkdirAll(gen, 0o755); err != nil {
		t.Fatal(err)
	}
	if lobster != "" {
		if err := os.WriteFile(filepath.Join(gen, "workflow.lobster"), []byte(lobster), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if charlyYML != "" {
		if err := os.WriteFile(filepath.Join(gen, "charly.yml"), []byte(charlyYML), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return gen
}

// invokeEmit drives the real provider dispatch with a workflow-emit envelope, so the test
// fails if either the plugin.go arm or emitWorkflow regresses. A non-nil error is the op
// refusing; a nil error returns the decoded reply.
func invokeEmit(t *testing.T, in spec.WorkflowEmitRequest) (*spec.WorkflowEmitReply, error) {
	t.Helper()
	body, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	reply, err := NewProvider().Invoke(context.Background(), &pb.InvokeRequest{
		Op:         ops.OpWorkflowEmit,
		Reserved:   "lobster",
		Class:      "workflow",
		ParamsJson: body,
	})
	if err != nil {
		return nil, err
	}
	var out spec.WorkflowEmitReply
	if err := json.Unmarshal(reply.GetResultJson(), &out); err != nil {
		t.Fatalf("decode WorkflowEmitReply: %v (result_json %s)", err, reply.GetResultJson())
	}
	return &out, nil
}

// TestWorkflowEmitWritesTheAdvertisedFormats is the happy path: every format the engine
// advertises lands in out_dir under its own name, byte-identical to the lowered source.
func TestWorkflowEmitWritesTheAdvertisedFormats(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	const (
		loweredLobster = "name: nightly\nsteps:\n  - id: a\n    run: echo hi\n"
		loweredCharly  = "pipeline:\n  nightly: {}\n"
	)
	genPair(t, "nightly", loweredLobster, loweredCharly)
	out := t.TempDir()

	reply, err := invokeEmit(t, spec.WorkflowEmitRequest{
		Pipeline: "nightly",
		Format:   []string{"lobster", "charly-yml"},
		OutDir:   out,
	})
	if err != nil {
		t.Fatalf("workflow-emit returned %v", err)
	}
	if len(reply.Files) != 2 {
		t.Fatalf("Files = %v, want one entry per requested format", reply.Files)
	}
	for format, pair := range map[string]struct{ name, want string }{
		"lobster":    {"workflow.lobster", loweredLobster},
		"charly-yml": {"charly.yml", loweredCharly},
	} {
		dst := filepath.Join(out, pair.name)
		if got := reply.Files[format]; got != dst {
			t.Errorf("Files[%q] = %q, want %q", format, got, dst)
		}
		got, rerr := os.ReadFile(dst)
		if rerr != nil {
			t.Fatalf("emitted %s is not on disk: %v", pair.name, rerr)
		}
		if string(got) != pair.want {
			t.Errorf("emitted %s = %q, want the lowered source %q", pair.name, got, pair.want)
		}
	}
}

// TestWorkflowEmitDefaultsOutDirToTheGenDir covers the empty-out_dir branch: with no
// out_dir the pair is re-materialized in place, so the reply still answers with a path.
func TestWorkflowEmitDefaultsOutDirToTheGenDir(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	gen := genPair(t, "nightly", "name: nightly\nsteps: []\n", "")

	reply, err := invokeEmit(t, spec.WorkflowEmitRequest{Pipeline: "nightly", Format: []string{"lobster"}})
	if err != nil {
		t.Fatalf("workflow-emit returned %v", err)
	}
	want := filepath.Join(gen, "workflow.lobster")
	if got := reply.Files["lobster"]; got != want {
		t.Fatalf("Files[lobster] = %q, want the gen dir %q", got, want)
	}
}

// TestWorkflowEmitRefuses covers every way the op must refuse. Each case names the
// substring the refusal has to carry, so a refusal that degrades to a bare error — or to
// a silent empty reply — fails here.
func TestWorkflowEmitRefuses(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   spec.WorkflowEmitRequest
		want string
	}{
		{
			name: "no pipeline",
			in:   spec.WorkflowEmitRequest{Format: []string{"lobster"}},
			want: "pipeline is required",
		},
		{
			name: "no format",
			in:   spec.WorkflowEmitRequest{Pipeline: "nightly"},
			want: "format is required",
		},
		{
			name: "github-actions is designed but not built",
			in:   spec.WorkflowEmitRequest{Pipeline: "nightly", Format: []string{"github-actions"}},
			want: "github-actions consumer is designed but not built",
		},
		{
			name: "unknown format",
			in:   spec.WorkflowEmitRequest{Pipeline: "nightly", Format: []string{"mermaid"}},
			want: `unknown format "mermaid"`,
		},
		{
			// genPair writes only workflow.lobster, so the charly-yml half is absent:
			// a format naming a file the lowering never produced must fail, not answer
			// with a path to nothing.
			name: "requested file is not in the gen dir",
			in:   spec.WorkflowEmitRequest{Pipeline: "nightly", Format: []string{"charly-yml"}},
			want: "charly.yml",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			t.Chdir(dir)
			genPair(t, "nightly", "name: nightly\nsteps: []\n", "")
			reply, err := invokeEmit(t, tc.in)
			if err == nil {
				t.Fatalf("workflow-emit accepted %+v and answered %v", tc.in, reply)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %q, want it to contain %q", err, tc.want)
			}
		})
	}
}

// TestWorkflowEmitFormatsAreTheAdvertisedCapability ties the op to the static fact the
// front-end reads. EmitFormats is how a caller refuses an unsupported format before it
// dispatches, so a format accepted here but not advertised (or the reverse) is a
// contract split; this pins the two together.
func TestWorkflowEmitFormatsAreTheAdvertisedCapability(t *testing.T) {
	capability := engineCapability()
	if got, want := strings.Join(capability.EmitFormats, ","), "lobster,charly-yml"; got != want {
		t.Fatalf("EmitFormats = %q, want %q", got, want)
	}

	dir := t.TempDir()
	t.Chdir(dir)
	genPair(t, "nightly", "name: nightly\nsteps: []\n", "pipeline: {}\n")
	out := t.TempDir()

	// Every advertised format is accepted...
	for _, format := range capability.EmitFormats {
		if _, err := invokeEmit(t, spec.WorkflowEmitRequest{
			Pipeline: "nightly", Format: []string{format}, OutDir: out,
		}); err != nil {
			t.Errorf("advertised format %q was refused: %v", format, err)
		}
	}
	// ...and the refusal for anything else names the whole supported set.
	_, err := invokeEmit(t, spec.WorkflowEmitRequest{
		Pipeline: "nightly", Format: []string{"github-actions"}, OutDir: out,
	})
	if err == nil || !strings.Contains(err.Error(), "lobster, charly-yml") {
		t.Fatalf("the github-actions refusal = %v, want it to name the supported set", err)
	}
}
