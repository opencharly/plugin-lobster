# AGENTS.md — opencharly/plugin-lobster

The **`workflow:lobster` engine**: the one plugin that EXECUTES a charly workflow.
It is an engine in the `workflow` provider class, reached host-side as
`InvokeProvider("workflow", "lobster", workflow-<op>)`; its `command:lobster` face
is the `charly lobster` CLI.

Canonical files:

- `candy/plugin-lobster/plugin.go` — `NewProvider` / `NewMeta` (the two declared
  capabilities) and the op dispatch.
- `candy/plugin-lobster/executor.go` — the native lobster executor (the moved
  ledger + ref resolution + the step loop).
- `candy/plugin-lobster/batch.go` — `parallel` / `for_each` batching.
- `candy/plugin-lobster/cli.go` — `charly lobster import|export|doctor`.
- `candy/plugin-lobster/schema/lobster.cue` — the engine doc schema + `#LobsterFile`.
- `candy/plugin-lobster/params/cue_types_gen.go` — GENERATED (never hand-edit).
- `charly.yml` — the root project manifest (`discover:` only), so the candy is
  scanned and the CUE schema gate applies.

## Load these skills first (R0)

- `/charly-internals:plugin` — the per-plugin CUE-schema contract, provider model,
  and the `plugin:` block.
- `/charly-pipeline:pipeline` — the front-end engine and `kind:pipeline`.
- `/charly-internals:go` — the schema→generated-code pipeline (`cue-gen`).
- `/charly-internals:git-workflow` — the branch/PR/landing discipline.

## The contract this plugin implements

The authored form is `kind:pipeline` (`spec.Pipeline`, schema/pipeline.cue). The
front-end resolves and validates it, lowers it with `sdk/workflowkit.Lower` to the
`(workflow.lobster, charly.yml)` pair in the run's gen dir, and dispatches
`workflow-run` with a `spec.WorkflowRunRequest{pipeline, args, mode, dry_run, gen_dir}`
in `params_json`. **This plugin decodes that request and consumes the lowered pair** —
`workflow.lobster` carries the flow, the generated `charly.yml` carries the
charly-side data (entities, triggers, config) that lobster itself has no syntax for.

Charly steps inside a workflow are ordinary `charly task` invocations, so a workflow
reaches every plugin of every provider class through the normal core→plugin dispatch.
Nothing here special-cases a word.

## Build / validate / test

- `go build ./... && go vet ./... && go test ./...` from `candy/plugin-lobster`
  (workspace-off: this module resolves sdk/spec from the proxy at the pinned requires).
- The R10 beds are `check-lobster-local` (engine semantics) and
  `check-lobster-schedule-local` (the systemd-timer scheduler) — see
  `/charly-check:check`. There is **no R10 class exemption**: a library or schema
  change runs the full assembled `disposable: true` bed.
- The merge gate is the **org-wide** `charly/pr-validator` (required check
  `validate / validate`); its PASS arms native auto-merge (squash), and
  `tag-on-merge` writes `CHANGELOG/<CalVer>.md`.

## Landing

- PR-only. Every change lands through a pull request; direct pushes to `main` are
  blocked. No force-push, no `--no-verify`.
- History lives in `CHANGELOG/`; the PR body IS the changelog.
- The authoritative rulebook is the umbrella `AGENTS.md` in
  `opencharly/opencharly` and `charly/AGENTS.md` in the charly repo. Do not restate
  its rules here.
