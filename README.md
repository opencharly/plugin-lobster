# plugin-lobster

The **LOBSTER workflow engine** for [charly](https://github.com/opencharly/charly).

It is the plugin that *executes* a workflow. A pipeline is authored in the
`kind:pipeline` syntax — lobster's own step grammar (`run` / `pipeline` /
`workflow` / `parallel` / `for_each` / `input`, with `when` / `stdin` / `retry` /
`on_error` / `timeout_ms` / `approval`) **extended with charly's full step
grammar**, so a workflow can use any charly plugin of any provider class.

## What it provides

| capability | what it is |
|---|---|
| `workflow:lobster` | the engine — `workflow-run` · `workflow-resume` · `workflow-schedule` · `workflow-emit` |
| `command:lobster` | the `charly lobster import\|export\|doctor` CLI |

## How a run works

```
charly.yml  kind:pipeline            lobster syntax + charly plan: steps
      │  front-end (plugin-pipeline) resolves + validates
      ▼  sdk/workflowkit.Lower  ──►  .opencharly/pipelines/<name>/
      │                                 workflow.lobster   (the flow)
      │                                 charly.yml         (entities/triggers/config)
      ▼  InvokeProvider("workflow", "lobster", workflow-run)
   plugin-lobster executes the lowered pair
      └─►  charly steps run as plain `charly task …`
              └─► core prescan → registry → compiled-in / gRPC plugin
```

Because every charly step is an ordinary `charly` invocation, the exported
`(workflow.lobster, charly.yml)` pair is portable to **upstream lobster** — nothing in it
needs this engine — and a future consumer (GitHub Actions) lowers from the same IR. That
portability is the *interface* property the lowering is designed for; it is **not asserted
by this repo's beds**, which have no upstream lobster to invoke (openclaw/lobster is not
vendored here, and the engine bed deliberately carries no green no-op step standing in for
the comparison).

## Scheduling

A `schedule:` trigger becomes a **systemd user timer**
(`~/.config/systemd/user/charly-pipeline-<name>.{service,timer}`), generated from
the same `sdk/deploykit` unit helpers the deploy code uses. `cron` expressions are
converted to `OnCalendar` and validated with `systemd-analyze calendar`.

## Build

```sh
cd candy/plugin-lobster
go build ./... && go vet ./... && go test ./...
```

## License

MIT — see [LICENSE](LICENSE).
