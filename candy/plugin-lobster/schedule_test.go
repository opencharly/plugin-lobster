package pluginlobster

// schedule_test.go — the scheduler's DECIDABLE half. `apply`'s systemd side is a live
// boundary and is proven by the bed; what is unit-testable is the name/unit mapping, the
// cron conversion, the idempotent writer, and the descriptors `apply` reads. Those are the
// parts where a mistake would install the WRONG timer rather than fail loudly.

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/opencharly/spec/spec"
)

func TestSanitizeUnitSegment(t *testing.T) {
	cases := map[string]string{
		"check-omarchy-accept-matrix": "check-omarchy-accept-matrix",
		"My Pipeline":                 "my-pipeline",
		"a/b:c":                       "a-b-c",
		"  spaced  ":                  "spaced",
		"...":                         "pipeline",
		"UPPER_case.1":                "upper_case.1",
	}
	for in, want := range cases {
		if got := sanitizeUnitSegment(in); got != want {
			t.Fatalf("sanitizeUnitSegment(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestScheduleUnitName(t *testing.T) {
	if got := scheduleUnitName("nightly", 0); got != "charly-pipeline-nightly-0" {
		t.Fatalf("unit name = %q", got)
	}
	if got := scheduleUnitName("a b", 2); got != "charly-pipeline-a-b-2" {
		t.Fatalf("unit name = %q", got)
	}
}

func TestNextCronRun(t *testing.T) {
	from := time.Date(2026, 10, 3, 12, 30, 0, 0, time.UTC)
	next, ok := nextCronRun("0 * * * *", from)
	if !ok {
		t.Fatal("hourly cron did not parse")
	}
	if got := next.UTC().Format(time.RFC3339); got != "2026-10-03T13:00:00Z" {
		t.Fatalf("next run = %s", got)
	}
	if _, ok := nextCronRun("not a cron", from); ok {
		t.Fatal("a non-cron was accepted")
	}
}

func TestWriteIfChangedIsContentIdempotent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "unit.timer")
	if err := writeIfChanged(path, "body\n"); err != nil {
		t.Fatalf("first write: %v", err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	first := fi.ModTime()
	// A second write of the SAME content must not touch the file: an idempotent apply has to
	// leave unit mtimes alone or systemd sees churn.
	time.Sleep(10 * time.Millisecond)
	if err := writeIfChanged(path, "body\n"); err != nil {
		t.Fatalf("second write: %v", err)
	}
	if fi2, err := os.Stat(path); err != nil || !fi2.ModTime().Equal(first) {
		t.Fatalf("an identical write rewrote the file (mtime %v -> %v)", first, fi2.ModTime())
	}
	if err := writeIfChanged(path, "other\n"); err != nil {
		t.Fatalf("changed write: %v", err)
	}
	if body, _ := os.ReadFile(path); string(body) != "other\n" {
		t.Fatalf("content = %q, want the new body", body)
	}
}

func TestReadOnCalendar(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "x.timer")
	if err := os.WriteFile(path, []byte("[Timer]\nOnCalendar=*-*-* 01:00:00 UTC\nPersistent=true\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := readOnCalendar(path); got != "*-*-* 01:00:00 UTC" {
		t.Fatalf("OnCalendar = %q", got)
	}
	if got := readOnCalendar(filepath.Join(dir, "missing.timer")); got != "" {
		t.Fatalf("missing timer = %q, want empty", got)
	}
}

func TestDescriptorCronForReadsTheFrontEndFile(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	gen := filepath.Join(dir, ".opencharly", "pipelines", "nightly")
	if err := os.MkdirAll(gen, 0o755); err != nil {
		t.Fatal(err)
	}
	desc := scheduleDescriptor{Pipeline: "nightly", Triggers: []spec.WorkflowTrigger{{Schedule: spec.WorkflowSchedule{Cron: "*/5 * * * *"}}}}
	body, _ := json.Marshal(desc)
	if err := os.WriteFile(filepath.Join(gen, "schedule.json"), body, 0o644); err != nil {
		t.Fatal(err)
	}
	cron, ok := descriptorCronFor("nightly")
	if !ok || cron != "*/5 * * * *" {
		t.Fatalf("descriptorCronFor = (%q,%v)", cron, ok)
	}
	if _, ok := descriptorCronFor("absent"); ok {
		t.Fatal("a pipeline with no descriptor reported a cron")
	}
}

func TestScheduleApplyRefusesWithoutADescriptor(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	_, err := scheduleApply(context.Background(), "nightly")
	if err == nil {
		t.Fatal("apply must refuse when the front-end wrote no descriptor")
	}
	if want := "schedule.json"; !strings.Contains(err.Error(), want) {
		t.Fatalf("error = %q, want it to name %s", err, want)
	}
}

func TestScheduleApplyRefusesWithNoTriggers(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	gen := filepath.Join(dir, ".opencharly", "pipelines", "nightly")
	if err := os.MkdirAll(gen, 0o755); err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(scheduleDescriptor{Pipeline: "nightly", Triggers: []spec.WorkflowTrigger{{}}})
	if err := os.WriteFile(filepath.Join(gen, "schedule.json"), body, 0o644); err != nil {
		t.Fatal(err)
	}
	// A trigger with no `schedule` is legal (a manual trigger); it must not install a
	// timer, and apply must say so rather than write a unit with an empty OnCalendar.
	_, err := scheduleApply(context.Background(), "nightly")
	if err == nil || !strings.Contains(err.Error(), "triggers.schedule") {
		t.Fatalf("error = %v, want the no-triggers message", err)
	}
}

func TestScheduleApplyRefusesAnUnparseableCron(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	gen := filepath.Join(dir, ".opencharly", "pipelines", "nightly")
	if err := os.MkdirAll(gen, 0o755); err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(scheduleDescriptor{Pipeline: "nightly", Triggers: []spec.WorkflowTrigger{{Schedule: spec.WorkflowSchedule{Cron: "every so often"}}}})
	if err := os.WriteFile(filepath.Join(gen, "schedule.json"), body, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := scheduleApply(context.Background(), "nightly"); err == nil {
		t.Fatal("apply must refuse a cron it cannot convert")
	}
}

func TestScheduleWorkflowUnknownOp(t *testing.T) {
	_, err := scheduleWorkflow(context.Background(), spec.WorkflowScheduleRequest{Op: "install"})
	if err == nil || !strings.Contains(err.Error(), "unknown op") {
		t.Fatalf("error = %v, want the unknown-op message", err)
	}
}

func TestScheduleMarkerRecordsTheRun(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "nested", "nightly.last")
	writeScheduleMarker(marker, "nightly", "ok", nil)
	body, err := os.ReadFile(marker)
	if err != nil {
		t.Fatalf("marker not written: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatalf("marker is not JSON: %v", err)
	}
	if m["pipeline"] != "nightly" || m["status"] != "ok" {
		t.Fatalf("marker = %v", m)
	}
	if _, hasErr := m["error"]; hasErr {
		t.Fatalf("a successful run recorded an error: %v", m)
	}

	// A failure records the message, so a marker left by a broken run is self-explaining.
	writeScheduleMarker(marker, "nightly", "error", errSentinel{})
	if body, err := os.ReadFile(marker); err != nil || !strings.Contains(string(body), errSentinel{}.Error()) {
		t.Fatalf("failed-run marker = %q (%v)", body, err)
	}
}

// errSentinel is a stand-in for a run error; the marker stores Error()'s text.
type errSentinel struct{}

func (errSentinel) Error() string { return "sentinel failure" }
