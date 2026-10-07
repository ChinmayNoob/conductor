// Package schedule parses cron expressions and decides when schedules fire,
// including what to do about runs missed while the coordinator was down.
package schedule

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/ChinmayNoob/conductor/pkg/task"
	"github.com/robfig/cron/v3"
)

// Misfire policies: what to do when a schedule's run time passed while
// nothing could fire it (e.g. the coordinator was down).
const (
	Skip    = "skip"     // drop missed runs; wait for the next one
	RunOnce = "run_once" // fire once now, then resume the normal cadence
	CatchUp = "catch_up" // fire every missed run, one per tick
)

// Grace is how late a run may fire and still count as on time.
const Grace = time.Minute

// Spec is a parsed cron expression in a time zone.
type Spec struct {
	sched cron.Schedule
	loc   *time.Location
}

// Parse accepts standard 5-field cron ("*/5 * * * *") and descriptors
// ("@hourly", "@every 30s").
func Parse(expr, timezone string) (Spec, error) {
	if timezone == "" {
		timezone = "UTC"
	}
	loc, err := time.LoadLocation(timezone)
	if err != nil {
		return Spec{}, fmt.Errorf("unknown time zone %q", timezone)
	}
	s, err := cron.ParseStandard(expr)
	if err != nil {
		return Spec{}, fmt.Errorf("invalid cron expression %q: %w", expr, err)
	}
	return Spec{sched: s, loc: loc}, nil
}

// Next returns the first run time strictly after t.
func (s Spec) Next(t time.Time) time.Time {
	return s.sched.Next(t.In(s.loc)).UTC()
}

// Decision says whether a due schedule fires now and when it is due next.
type Decision struct {
	Fire bool
	// FireAt is the run time being fired (used to make firing idempotent).
	FireAt time.Time
	Next   time.Time
}

// Decide handles a schedule whose run at `due` has come up. A run that is
// late by more than Grace is a misfire, handled per policy.
func Decide(s Spec, policy string, due, now time.Time) Decision {
	if now.Sub(due) <= Grace {
		next := s.Next(due)
		if !next.After(now) {
			next = s.Next(now)
		}
		return Decision{Fire: true, FireAt: due, Next: next}
	}
	switch policy {
	case RunOnce:
		return Decision{Fire: true, FireAt: due, Next: s.Next(now)}
	case CatchUp:
		// Fire this missed run; the next one is probably due already and
		// will fire on the following tick.
		return Decision{Fire: true, FireAt: due, Next: s.Next(due)}
	default:
		return Decision{Fire: false, Next: s.Next(now)}
	}
}

// ValidPolicy reports whether p is a known misfire policy.
func ValidPolicy(p string) bool {
	return p == Skip || p == RunOnce || p == CatchUp
}

// Target is what a schedule starts: a task or a workflow run.
type Target struct {
	Task     *task.Template  `json:"task,omitempty"`
	Workflow *WorkflowTarget `json:"workflow,omitempty"`
}

type WorkflowTarget struct {
	Name    string          `json:"name"`
	Version int             `json:"version,omitempty"` // 0 = latest at fire time
	Input   json.RawMessage `json:"input,omitempty"`
}

func (t *Target) Validate() error {
	switch {
	case (t.Task == nil) == (t.Workflow == nil):
		return errors.New("a schedule needs exactly one of task or workflow")
	case t.Task != nil:
		return t.Task.Validate()
	case t.Workflow.Name == "":
		return errors.New("workflow.name is required")
	}
	return nil
}
