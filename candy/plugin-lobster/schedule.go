package pluginlobster

// schedule.go — the systemd USER TIMER scheduler.
//
// lobster has no scheduler; this is the capability charly adds. A pipeline's
// `triggers:` become user timers that activate a run-to-completion oneshot service
// (`charly -C <project> pipeline run <name> --mode tool`), so a scheduled workflow runs
// through exactly the same engine path as a hand-run one — there is no second runner.
//
// WHERE THE CRON COMES FROM. The lowered pair deliberately does NOT carry `triggers`:
// `lobsterDrop` in workflowkit hands them to the ENGINE, and they are IR-level data the
// front-end owns. So the front-end writes them where the engine can read them, as
// `<gen-dir>/schedule.json` — the contract's OWN `#WorkflowTrigger` list (schema/workflow.cue), so this is not a new
// contract. `apply` without that file is a hard error naming it, never a silently
// absent timer.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/opencharly/sdk/deploykit"
	"github.com/opencharly/sdk/kit"
	"github.com/opencharly/sdk/workflowkit"
	"github.com/opencharly/spec/spec"
	"github.com/robfig/cron/v3"
)

// scheduleDescriptor is what the front-end writes for the scheduler. It is the contract's own
// trigger list, wrapped with the pipeline name so the file is self-describing.
type scheduleDescriptor struct {
	Pipeline string                 `json:"pipeline"`
	Triggers []spec.WorkflowTrigger `json:"triggers"`
}

// scheduleMarkerEnv names the file a completed scheduled run touches. The generated
// service sets it, so BOTH a timer-fired run and `run-now` (which starts the same
// service) record that they ran, without this engine having to reach into systemd's
// journal.
const scheduleMarkerEnv = "LOBSTER_SCHEDULE_MARKER"

// schedulePrefix is the unit basename prefix every scheduled pipeline owns, and
// therefore the exact set `list` and `remove` walk.
const schedulePrefix = "charly-pipeline-"

func scheduleWorkflow(ctx context.Context, in spec.WorkflowScheduleRequest) (*spec.WorkflowScheduleReply, error) {
	switch in.Op {
	case "apply":
		return scheduleApply(ctx, in.Pipeline)
	case "list":
		return scheduleList(ctx)
	case "remove":
		return scheduleRemove(ctx, in.Pipeline)
	case "run-now":
		return scheduleRunNow(ctx, in.Pipeline)
	}
	return nil, fmt.Errorf("workflow-schedule: unknown op %q (apply|list|remove|run-now)", in.Op)
}

// ---------------------------------------------------------------------------
// apply
// ---------------------------------------------------------------------------

func scheduleApply(ctx context.Context, pipeline string) (*spec.WorkflowScheduleReply, error) {
	if strings.TrimSpace(pipeline) == "" {
		return nil, fmt.Errorf("workflow-schedule apply: pipeline is required")
	}
	genDir, err := resolveGenDir(pipeline, "")
	if err != nil {
		return nil, err
	}
	descPath := filepath.Join(genDir, "schedule.json")
	raw, err := os.ReadFile(descPath)
	if err != nil {
		return nil, fmt.Errorf("workflow-schedule apply: %s has no %s; the front-end writes the pipeline's triggers there when it lowers (%w)", pipeline, descPath, err)
	}
	var desc scheduleDescriptor
	if err := json.Unmarshal(raw, &desc); err != nil {
		return nil, fmt.Errorf("workflow-schedule apply: parse %s: %w", descPath, err)
	}

	type schedule struct {
		cron       string
		timezone   string
		args       map[string]string
		persistent bool
	}
	var schedules []schedule
	for _, t := range desc.Triggers {
		// `#WorkflowTrigger.schedule` is optional, so its absence is the zero value —
		// an empty cron, not a nil pointer.
		if t.Schedule.Cron == "" {
			continue
		}
		schedules = append(schedules, schedule{
			cron:       t.Schedule.Cron,
			timezone:   t.Schedule.Timezone,
			args:       t.Schedule.Args,
			persistent: t.Schedule.Persistent,
		})
	}
	if len(schedules) == 0 {
		return nil, fmt.Errorf("workflow-schedule apply: pipeline %s declares no `triggers.schedule`", pipeline)
	}

	projectDir, err := os.Getwd()
	if err != nil {
		return nil, fmt.Errorf("workflow-schedule apply: resolve project dir: %w", err)
	}
	charlyBin := resolveCharlyBin(environMap())
	stateDir := defaultStateDir(environMap())

	var units []string
	for i, s := range schedules {
		name := scheduleUnitName(pipeline, i)
		onCalendar, cerr := workflowkit.CronToOnCalendar(s.cron)
		if cerr != nil {
			return nil, fmt.Errorf("workflow-schedule apply: pipeline %s trigger %d: %w", pipeline, i, cerr)
		}
		if s.timezone != "" {
			onCalendar = onCalendar + " " + s.timezone
		}
		if verr := validateOnCalendar(ctx, onCalendar); verr != nil {
			return nil, verr
		}

		argsJSON := "{}"
		if len(s.args) > 0 {
			b, merr := json.Marshal(s.args)
			if merr != nil {
				return nil, merr
			}
			argsJSON = string(b)
		}

		marker := filepath.Join(stateDir, "schedule", sanitizeUnitSegment(pipeline)+".last")
		cfg := deploykit.TimerConfig{
			Name:        name,
			Description: fmt.Sprintf("charly pipeline %s (scheduled)", pipeline),
			OnCalendar:  onCalendar,
			Persistent:  s.persistent,
			Service: deploykit.SystemdUnitConfig{
				Name:        name,
				Description: fmt.Sprintf("charly pipeline %s (run to completion)", pipeline),
				StartArgv:   []string{charlyBin, "-C", projectDir, "pipeline", "run", pipeline, "--mode", "tool", "--args-json", argsJSON},
				Environment: map[string]string{scheduleMarkerEnv: marker},
			},
		}
		timerText := deploykit.GenerateSystemdTimer(cfg)
		serviceText := deploykit.GenerateTimerService(cfg)
		if timerText == "" || serviceText == "" {
			return nil, fmt.Errorf("workflow-schedule apply: could not render units for pipeline %s trigger %d", pipeline, i)
		}

		dir, derr := userUnitDir()
		if derr != nil {
			return nil, derr
		}
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("workflow-schedule apply: create user unit dir: %w", err)
		}
		// Idempotent by CONTENT: a second apply rewrites nothing, so unit mtimes stay put
		// and systemd sees no change.
		if err := writeIfChanged(filepath.Join(dir, deploykit.TimerFilename(name)), timerText); err != nil {
			return nil, err
		}
		if err := writeIfChanged(filepath.Join(dir, deploykit.SystemdUnitFilename(name)), serviceText); err != nil {
			return nil, err
		}

		if err := runSystemctlUser(ctx, "daemon-reload"); err != nil {
			return nil, err
		}
		if err := runSystemctlUser(ctx, "enable", "--now", deploykit.TimerFilename(name)); err != nil {
			return nil, err
		}
		units = append(units, deploykit.TimerFilename(name), deploykit.SystemdUnitFilename(name))
	}
	return &spec.WorkflowScheduleReply{Units: units}, nil
}

// ---------------------------------------------------------------------------
// list
// ---------------------------------------------------------------------------

func scheduleList(ctx context.Context) (*spec.WorkflowScheduleReply, error) {
	dir, err := userUnitDir()
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return &spec.WorkflowScheduleReply{}, nil
		}
		return nil, err
	}

	var out []spec.WorkflowScheduleEntry
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, schedulePrefix) || !strings.HasSuffix(name, ".timer") {
			continue
		}
		base := strings.TrimSuffix(name, ".timer")
		onCalendar := readOnCalendar(filepath.Join(dir, name))
		active := systemctlUserQuiet(ctx, "is-active", name) == nil

		entry := spec.WorkflowScheduleEntry{
			Pipeline:   strings.TrimSuffix(strings.TrimPrefix(base, schedulePrefix), "-0"),
			Timer:      name,
			OnCalendar: onCalendar,
			Active:     active,
		}
		// The next fire time is computed from the CRON, not read from systemd: it is the
		// engine's own answer, and it stays available when the user manager is not.
		if cron, ok := descriptorCronFor(entry.Pipeline); ok {
			if next, ok := nextCronRun(cron, time.Now()); ok {
				entry.NextRun = next.UTC().Format(time.RFC3339)
			}
		}
		out = append(out, entry)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Timer < out[j].Timer })
	return &spec.WorkflowScheduleReply{Entries: out}, nil
}

// ---------------------------------------------------------------------------
// remove
// ---------------------------------------------------------------------------

func scheduleRemove(ctx context.Context, pipeline string) (*spec.WorkflowScheduleReply, error) {
	if strings.TrimSpace(pipeline) == "" {
		return nil, fmt.Errorf("workflow-schedule remove: pipeline is required")
	}
	dir, err := userUnitDir()
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return &spec.WorkflowScheduleReply{}, nil
		}
		return nil, err
	}

	prefix := schedulePrefix + sanitizeUnitSegment(pipeline)
	var units []string
	removed := false
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		if !strings.HasSuffix(name, ".timer") && !strings.HasSuffix(name, ".service") {
			continue
		}
		if strings.HasSuffix(name, ".timer") {
			// `disable --now` is best-effort: a unit whose user manager is gone still has
			// files to delete, and refusing to remove it would strand them.
			_ = runSystemctlUser(ctx, "disable", "--now", name)
		}
		if err := os.Remove(filepath.Join(dir, name)); err != nil && !os.IsNotExist(err) {
			return nil, err
		}
		units = append(units, name)
		removed = true
	}
	if removed {
		if err := runSystemctlUser(ctx, "daemon-reload"); err != nil {
			return nil, err
		}
	}
	return &spec.WorkflowScheduleReply{Units: units}, nil
}

// ---------------------------------------------------------------------------
// run-now
// ---------------------------------------------------------------------------

func scheduleRunNow(ctx context.Context, pipeline string) (*spec.WorkflowScheduleReply, error) {
	if strings.TrimSpace(pipeline) == "" {
		return nil, fmt.Errorf("workflow-schedule run-now: pipeline is required")
	}
	dir, err := userUnitDir()
	if err != nil {
		return nil, err
	}
	name := deploykit.TimerFilename(scheduleUnitName(pipeline, 0))
	if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
		return nil, fmt.Errorf("workflow-schedule run-now: %s is not installed (apply first)", name)
	}
	if err := runSystemctlUser(ctx, "start", deploykit.SystemdUnitFilename(scheduleUnitName(pipeline, 0))); err != nil {
		return nil, err
	}
	return &spec.WorkflowScheduleReply{Units: []string{deploykit.SystemdUnitFilename(scheduleUnitName(pipeline, 0))}}, nil
}

// ---------------------------------------------------------------------------
// unit helpers
// ---------------------------------------------------------------------------

// scheduleUnitName is the systemd basename for a pipeline's nth schedule.
func scheduleUnitName(pipeline string, n int) string {
	return fmt.Sprintf("%s%s-%d", schedulePrefix, sanitizeUnitSegment(pipeline), n)
}

// sanitizeUnitSegment reduces a pipeline name to systemd's unit-name alphabet.
func sanitizeUnitSegment(s string) string {
	var b strings.Builder
	prevDash := false
	for _, r := range strings.ToLower(s) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
			prevDash = false
		case r == '.' || r == '_' || r == '-':
			b.WriteRune(r)
			prevDash = false
		default:
			if !prevDash {
				b.WriteByte('-')
				prevDash = true
			}
		}
	}
	out := strings.Trim(b.String(), "-.")
	if out == "" {
		out = "pipeline"
	}
	return out
}

// userUnitDir is systemd's user unit directory, honouring XDG_CONFIG_HOME.
func userUnitDir() (string, error) {
	if v := strings.TrimSpace(os.Getenv("XDG_CONFIG_HOME")); v != "" {
		return filepath.Join(v, "systemd", "user"), nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve home dir: %w", err)
	}
	return filepath.Join(home, ".config", "systemd", "user"), nil
}

// writeIfChanged writes a unit only when its content differs, so an idempotent apply
// leaves the file's mtime alone.
func writeIfChanged(path, content string) error {
	if existing, err := os.ReadFile(path); err == nil && string(existing) == content {
		return nil
	}
	return kit.AtomicWriteFile(path, []byte(content), 0o644)
}

// readOnCalendar extracts OnCalendar= from a timer unit.
func readOnCalendar(path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(b), "\n") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(line), "OnCalendar="); ok {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

// descriptorCronFor finds the cron a pipeline's schedule descriptor declares, for the
// engine's own next-run computation.
func descriptorCronFor(pipeline string) (string, bool) {
	genDir, err := resolveGenDir(pipeline, "")
	if err != nil {
		return "", false
	}
	raw, err := os.ReadFile(filepath.Join(genDir, "schedule.json"))
	if err != nil {
		return "", false
	}
	var desc scheduleDescriptor
	if err := json.Unmarshal(raw, &desc); err != nil {
		return "", false
	}
	for _, t := range desc.Triggers {
		if t.Schedule.Cron != "" {
			return t.Schedule.Cron, true
		}
	}
	return "", false
}

// validateOnCalendar asks systemd to verify the expression. It SKIPS cleanly when
// `systemd-analyze` is absent: a real systemd check is the only honest one, and a
// hand-rolled parser would certify a grammar systemd may not accept.
func validateOnCalendar(ctx context.Context, onCalendar string) error {
	if !strings.Contains(onCalendar, ":") && !strings.Contains(onCalendar, "*") {
		return fmt.Errorf("workflow-schedule: %q is not a calendar expression", onCalendar)
	}
	bin, err := exec.LookPath("systemd-analyze")
	if err != nil {
		return nil // no systemd-analyze to ask; the timer install is the real check
	}
	cmd := exec.CommandContext(ctx, bin, "calendar", onCalendar)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("workflow-schedule: systemd-analyze rejected %q: %s", onCalendar, strings.TrimSpace(string(out)))
	}
	return nil
}

// runSystemctlUser runs one `systemctl --user` command, surfacing its output on failure.
func runSystemctlUser(ctx context.Context, args ...string) error {
	bin, err := exec.LookPath("systemctl")
	if err != nil {
		return fmt.Errorf("workflow-schedule: systemctl is not available: %w", err)
	}
	full := append([]string{"--user"}, args...)
	out, err := exec.CommandContext(ctx, bin, full...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("workflow-schedule: systemctl --user %s: %s", strings.Join(args, " "), strings.TrimSpace(string(out)))
	}
	return nil
}

// systemctlUserQuiet runs a systemctl query whose failure is an expected answer.
func systemctlUserQuiet(ctx context.Context, args ...string) error {
	bin, err := exec.LookPath("systemctl")
	if err != nil {
		return err
	}
	return exec.CommandContext(ctx, bin, append([]string{"--user"}, args...)...).Run()
}

// ---------------------------------------------------------------------------
// next-run computation
// ---------------------------------------------------------------------------

// nextCronRun computes a cron's next fire time with the SAME library that produced the
// OnCalendar expression, so the reported time and the installed timer agree.
func nextCronRun(expr string, from time.Time) (time.Time, bool) {
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	sched, err := parser.Parse(strings.TrimSpace(expr))
	if err != nil {
		return time.Time{}, false
	}
	return sched.Next(from), true
}

// writeScheduleMarker records that a scheduled run finished. The generated service sets
// LOBSTER_SCHEDULE_MARKER, so a timer-fired run and `run-now` (which starts the same
// service) both leave the same evidence, and an operator can see a schedule really fired
// without reading the journal.
func writeScheduleMarker(marker, pipeline, status string, runErr error) {
	body := map[string]any{
		"pipeline":   pipeline,
		"status":     status,
		"finishedAt": time.Now().UTC().Format(time.RFC3339Nano),
	}
	if runErr != nil {
		body["error"] = runErr.Error()
	}
	b, err := json.MarshalIndent(body, "", "  ")
	if err != nil {
		return
	}
	b = append(b, '\n')
	if err := os.MkdirAll(filepath.Dir(marker), 0o700); err != nil {
		return
	}
	_ = kit.AtomicWriteFile(marker, b, 0o600)
}
