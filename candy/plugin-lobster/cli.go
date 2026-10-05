package pluginlobster

// cli.go — the `command:lobster` face: `charly lobster import|export|doctor`.
//
// This is a SEPARATE face of the same plugin, never the dispatch that runs a workflow
// (that is `workflow:lobster`). What it adds is the round trip an operator needs to move
// a workflow between the two grammars:
//
//   - `import <file.lobster>` turns a lobster workflow into an authored `kind: pipeline`
//     entity, so a hand-written or upstream `.lobster` file becomes a charly pipeline;
//   - `export <name>` writes the LOWERED pair where upstream lobster (or a human) can
//     read it — which works because a charly step lowers to a plain `charly … task …`
//     invocation, so the exported file is real lobster, not a dialect;
//   - `doctor` reports what the engine can and cannot do on this host, so a missing
//     systemd user manager or an unresolvable charly binary is a message rather than a
//     mystery.

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"

	"github.com/opencharly/sdk"
	"gopkg.in/yaml.v3"
)

func runCLI(args []string, _ *sdk.Executor) (int, error) {
	if len(args) == 0 {
		cliUsage(os.Stderr)
		return 2, nil
	}
	switch args[0] {
	case "import":
		return cliImport(args[1:], os.Stdout)
	case "export":
		return cliExport(args[1:])
	case "doctor":
		return cliDoctor(args[1:])
	case "help", "-h", "--help":
		cliUsage(os.Stdout)
		return 0, nil
	default:
		fmt.Fprintf(os.Stderr, "lobster: unknown command %q\n", args[0])
		cliUsage(os.Stderr)
		return 2, nil
	}
}

// CliMain is the `command:lobster` entrypoint, and it is REQUIRED, not optional.
//
// A plugin is reached by one of exactly two routes, and both name this function
// (or its absence breaks them):
//
//   - EXTERNAL (the default for a new plugin) — charly host-builds ./cmd/serve,
//     and that shim passes this function as the third argument of sdk.Main.
//     Without a cmd/serve main the plugin cannot be connected at all.
//   - COMPILED IN — `sdk/cmd/charly-lib-gen` emits
//     `{Provider: X.NewProvider, Meta: X.NewMeta, CLI: X.CliMain}` for every
//     listed plugin, so a missing exported CliMain is a COMPILE ERROR there.
//
// It forwards to the SAME runCLI that Invoke(OpRun) drives, so the two routes
// cannot drift. Unlike plugin-task's CliMain this one runs anywhere: lobster's
// CLI is self-contained (it reads a file / writes a directory) and needs no host
// project, so dropping the executor is safe in both placements.
func CliMain(args []string) int {
	code, err := runCLI(args, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "lobster: %v\n", err)
		return 1
	}
	return code
}

func cliUsage(w io.Writer) {
	fmt.Fprint(w, `usage: charly lobster <command> [args]

  import <file.lobster>   convert a lobster workflow into a kind: pipeline entity (stdout)
  export <pipeline> [dir] write the lowered workflow.lobster + charly.yml for inspection
  doctor                  report this host's lobster-engine capabilities
`)
}

// ---------------------------------------------------------------------------
// import
// ---------------------------------------------------------------------------

// cliImport converts a `.lobster` workflow into the authored `kind: pipeline` grammar.
//
// The step grammar is SHARED between the two forms (the authored form is lobster's plus
// charly's arms), so the conversion is a re-shaping of the workflow level: lobster's
// `name` becomes the pipeline `description`, `engine: lobster` is pinned, and every other
// key and every step is carried through unchanged. That is why the round trip is stable:
// nothing is invented.
func cliImport(args []string, out io.Writer) (int, error) {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "lobster import: exactly one <file.lobster> argument is required")
		return 2, nil
	}
	path := args[0]
	raw, err := parseWorkflowTree(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "lobster import: %v\n", err)
		return 1, nil
	}
	root, ok := asMap(raw)
	if !ok {
		fmt.Fprintln(os.Stderr, "lobster import: workflow file must be a JSON/YAML object")
		return 1, nil
	}
	if _, verr := loadWorkflowFile(path); verr != nil {
		fmt.Fprintf(os.Stderr, "lobster import: %v\n", verr)
		return 1, nil
	}

	entity := map[string]any{"engine": "lobster"}
	description, _ := asString(root["description"])
	if description == "" {
		if name, ok := asString(root["name"]); ok && name != "" {
			description = name
		} else {
			description = "Imported from " + filepath.Base(path)
		}
	}
	entity["description"] = description

	keys := make([]string, 0, len(root))
	for k := range root {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		switch k {
		case "name", "description", "engine":
			continue
		}
		entity[k] = root[k]
	}

	b, err := yaml.Marshal(entity)
	if err != nil {
		fmt.Fprintf(os.Stderr, "lobster import: %v\n", err)
		return 1, nil
	}
	if _, err := out.Write(b); err != nil {
		return 1, nil
	}
	return 0, nil
}

// ---------------------------------------------------------------------------
// export
// ---------------------------------------------------------------------------

// cliExport writes the lowered pair to a directory, so it can be read, diffed, or run by
// upstream lobster with `CHARLY_BIN` set.
func cliExport(args []string) (int, error) {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "lobster export: a <pipeline> argument is required")
		return 2, nil
	}
	pipeline := args[0]
	dir := "."
	if len(args) > 1 {
		dir = args[1]
	}
	genDir, err := resolveGenDir(pipeline, "")
	if err != nil {
		fmt.Fprintf(os.Stderr, "lobster export: %v\n", err)
		return 1, nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "lobster export: %v\n", err)
		return 1, nil
	}
	// The pair is written by `pipeline run`'s lowering, so a pipeline that was never run
	// has NOTHING to export. That case must be a NAMED refusal, never a silent success: a
	// caller who exports a typo'd name and gets exit 0 with no output believes it exported
	// something, and the absence is discovered later, somewhere else. The per-file
	// `continue` below therefore counts what it actually copied, and the count is what
	// decides the exit status — a pipeline that lowered only its workflow.lobster still
	// exports exactly what exists.
	exported := 0
	for _, f := range []string{"workflow.lobster", "charly.yml", "schedule.json"} {
		src := filepath.Join(genDir, f)
		if _, serr := os.Stat(src); serr != nil {
			continue
		}
		dst := filepath.Join(dir, f)
		if err := copyFile(src, dst); err != nil {
			fmt.Fprintf(os.Stderr, "lobster export: %v\n", err)
			return 1, nil
		}
		fmt.Println(dst)
		exported++
	}
	if exported == 0 {
		fmt.Fprintf(os.Stderr, "lobster export: no lowered files for %q in %s — the front-end writes them when the pipeline runs; run `charly pipeline run %s` first\n", pipeline, genDir, pipeline)
		return 1, nil
	}
	return 0, nil
}

// ---------------------------------------------------------------------------
// doctor
// ---------------------------------------------------------------------------

// cliDoctor reports the engine's host capabilities. Everything it prints is a real probe,
// so a "no" is actionable rather than a guess.
func cliDoctor(_ []string) (int, error) {
	env := environMap()
	charlyBin := resolveCharlyBin(env)
	fmt.Printf("charly binary:      %s\n", charlyBin)
	if _, err := exec.LookPath(charlyBin); err != nil && !filepath.IsAbs(charlyBin) {
		fmt.Printf("  WARNING: %q is not on PATH; charly steps will fail unless CHARLY_BIN is set\n", charlyBin)
	}

	stateDir := defaultStateDir(env)
	fmt.Printf("state dir:          %s\n", stateDir)
	if _, err := os.Stat(stateDir); err == nil {
		fmt.Println("  present")
	} else {
		fmt.Println("  not created yet (created on the first gated run)")
	}

	if dir, err := userUnitDir(); err == nil {
		fmt.Printf("user unit dir:      %s\n", dir)
	} else {
		fmt.Printf("user unit dir:      unavailable (%v)\n", err)
	}
	if _, err := exec.LookPath("systemctl"); err != nil {
		fmt.Println("systemd:            NOT available — `pipeline schedule` will refuse")
	} else if err := systemctlUserQuiet(context.Background(), "is-system-running"); err != nil {
		fmt.Println("systemd user manager: present (state query unavailable, which is normal for --user)")
	} else {
		fmt.Println("systemd:            available")
	}
	shellPath, shellArgv := resolveShell("", env, runtime.GOOS)
	fmt.Printf("shell:              %s %s\n", shellPath, strings.Join(shellArgv, " "))
	fmt.Printf("emit formats:       %s\n", strings.Join(engineCapability().EmitFormats, ", "))
	fmt.Println("pipeline stages:    " + strings.Join(newRegistry().names(), " "))
	return 0, nil
}
