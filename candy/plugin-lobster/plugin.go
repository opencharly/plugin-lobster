// Package pluginlobster — the LOBSTER workflow engine for charly.
//
// It provides two capabilities:
//
//   - workflow:lobster — the ENGINE. It is handed the LOWERED pair
//     (`<gen_dir>/workflow.lobster` + `<gen_dir>/charly.yml`) by the front-end
//     (`plugin-pipeline`, which resolved and validated the authored `kind:pipeline`
//     entity and lowered it with `sdk/workflowkit.Lower`), and it executes it.
//   - command:lobster — the `charly lobster import|export|doctor` CLI.
//
// Four ops, dispatched on `req.GetOp()`, each decoding the matching
// `spec.Workflow*Request` from `ParamsJson` and answering the matching
// `spec.Workflow*Reply` in `InvokeReply.ResultJson`:
//
//	workflow-run      execute the lowered pair            → spec.WorkflowRunReply
//	workflow-resume   answer a pending approval/input     → spec.WorkflowRunReply
//	workflow-schedule apply|list|remove|run-now the timers→ spec.WorkflowScheduleReply
//	workflow-emit     re-emit the pair to out_dir         → spec.WorkflowEmitReply
//
// The engine is the ONLY thing that knows lobster's step semantics; everything
// charly-specific happens through the generated `charly.yml`, because a charly step is
// lowered to an ordinary `run: <charly> -C <gen_dir> task _pipeline-…` invocation and
// therefore reaches every plugin of every provider class through the NORMAL
// core→plugin dispatch. Nothing here special-cases a word.
//
// SDD: schema/lobster.cue is the single source for params/cue_types_gen.go (the
// upstream file contract, generated — never hand-edited).
package pluginlobster

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"

	"github.com/opencharly/sdk"
	"github.com/opencharly/spec/ops"
	pb "github.com/opencharly/spec/proto"
	"github.com/opencharly/spec/spec"
)

//go:embed schema/*.cue
var schemaFS embed.FS

// calver is this plugin's own version, advertised over Describe.
const calver = "2026.276.1000"

// NewProvider returns the engine provider (both capabilities).
func NewProvider() pb.ProviderServer { return &provider{} }

// NewMeta advertises the two declared capabilities plus the self-contained CUE schema.
// Neither capability carries an InputDef: the `workflow` class takes the base
// `spec.Workflow*Request` envelopes (they are the wire contract, and a self-contained
// plugin schema cannot reference them), and `command` takes raw argv — exactly the
// plugin-pipeline precedent.
func NewMeta() pb.PluginMetaServer {
	return sdk.NewMeta(calver, []sdk.ProvidedCapability{
		{Class: "workflow", Word: "lobster"},
		{Class: "command", Word: "lobster"},
	}, schemaFS)
}

type provider struct{ pb.UnimplementedProviderServer }

func (provider) Invoke(ctx context.Context, req *pb.InvokeRequest) (*pb.InvokeReply, error) {
	switch req.GetOp() {
	case ops.OpWorkflowRun:
		var in spec.WorkflowRunRequest
		if err := decodeParams(req, &in); err != nil {
			return nil, err
		}
		reply, err := runWorkflow(ctx, req, in)
		return replyResult(reply, err)

	case ops.OpWorkflowResume:
		var in spec.WorkflowResumeRequest
		if err := decodeParams(req, &in); err != nil {
			return nil, err
		}
		reply, err := resumeWorkflow(ctx, req, in)
		return replyResult(reply, err)

	case ops.OpWorkflowSchedule:
		var in spec.WorkflowScheduleRequest
		if err := decodeParams(req, &in); err != nil {
			return nil, err
		}
		reply, err := scheduleWorkflow(ctx, in)
		return replyResult(reply, err)

	case ops.OpWorkflowEmit:
		var in spec.WorkflowEmitRequest
		if err := decodeParams(req, &in); err != nil {
			return nil, err
		}
		reply, err := emitWorkflow(ctx, in)
		return replyResult(reply, err)

	case sdk.OpRun: // command:lobster — the CLI
		var in struct {
			Args []string `json:"args"`
		}
		if len(req.GetParamsJson()) > 0 {
			_ = json.Unmarshal(req.GetParamsJson(), &in)
		}
		ex, xerr := sdk.ExecutorForInvoke(ctx, req.GetExecutorBrokerId())
		if xerr != nil {
			return nil, xerr
		}
		code, err := runCLI(in.Args, ex)
		if err != nil {
			return nil, err
		}
		if code != 0 {
			return nil, fmt.Errorf("lobster: exit %d", code)
		}
		return &pb.InvokeReply{}, nil
	}
	// Every other selector is not this provider's (the host only routes declared
	// capabilities here, so reaching this is a host bug, not a silent no-op).
	return nil, fmt.Errorf("lobster: unsupported op %q (this engine serves %s, %s, %s, %s and OpRun for the CLI)",
		req.GetOp(), ops.OpWorkflowRun, ops.OpWorkflowResume, ops.OpWorkflowSchedule, ops.OpWorkflowEmit)
}

// decodeParams unmarshals the op envelope out of the InvokeRequest. A missing or
// malformed envelope is an error, never a silent zero value: every one of these
// requests has a required field (pipeline / op), so an empty decode would otherwise
// surface much later as a confusing "pipeline \"\" not found".
func decodeParams(req *pb.InvokeRequest, out any) error {
	if len(req.GetParamsJson()) == 0 {
		return fmt.Errorf("lobster: %s carries no params_json", req.GetOp())
	}
	if err := json.Unmarshal(req.GetParamsJson(), out); err != nil {
		return fmt.Errorf("lobster: decode %s params: %w", req.GetOp(), err)
	}
	return nil
}

// replyResult marshals an op reply into InvokeReply.ResultJson. A nil reply with a
// non-nil err is returned as the error; a nil reply with a nil err is a bug.
func replyResult(reply any, err error) (*pb.InvokeReply, error) {
	if err != nil {
		return nil, err
	}
	if reply == nil {
		return nil, fmt.Errorf("lobster: internal error: nil reply")
	}
	b, merr := json.Marshal(reply)
	if merr != nil {
		return nil, fmt.Errorf("lobster: marshal reply: %w", merr)
	}
	return &pb.InvokeReply{ResultJson: b}, nil
}

// engineCapability is the static fact block a caller can read to refuse an
// unsupported feature with a clear message instead of silently dropping it.
func engineCapability() spec.WorkflowEngineCapability {
	return spec.WorkflowEngineCapability{
		Name:        "lobster",
		Execute:     true,
		Resume:      true,
		Approvals:   true,
		Schedule:    true,
		EmitFormats: []string{"lobster", "charly-yml"},
	}
}
