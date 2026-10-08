package workflow

import "strings"

// Step statuses.
const (
	StepPending            = "PENDING"
	StepRunning            = "RUNNING"
	StepCompleted          = "COMPLETED"
	StepFailed             = "FAILED"
	StepCancelled          = "CANCELLED"
	StepSkipped            = "SKIPPED" // never ran because the workflow failed first
	StepCompensating       = "COMPENSATING"
	StepCompensated        = "COMPENSATED"
	StepCompensationFailed = "COMPENSATION_FAILED"
)

// Workflow statuses.
const (
	StatusRunning      = "RUNNING"
	StatusCompensating = "COMPENSATING"
	StatusCompleted    = "COMPLETED"
	StatusFailed       = "FAILED"
	StatusCancelled    = "CANCELLED"
)

// Task statuses, as reported by the task queue.
const (
	taskCompleted = "COMPLETED"
	taskFailed    = "FAILED"
	taskCancelled = "CANCELLED"
)

// StepState is what the reconciler knows about one step.
type StepState struct {
	Status string
	// TaskStatus is the status of the step's task ("" if none yet).
	TaskStatus string
	// CompensationStatus is the status of its compensation task, if any.
	CompensationStatus string
	// Error is the step task's error message, if it failed.
	Error string
}

// RunState is the current state of a workflow run.
type RunState struct {
	Status          string
	CancelRequested bool
	// Abort, if set, fails a running workflow for this reason (e.g. its
	// budget is spent): running steps are cancelled and completed ones
	// compensated, as for a failed step.
	Abort string
	Steps map[string]StepState
}

// Plan is what should happen next. The caller applies it, reloads the state,
// and reconciles again until the plan is empty.
type Plan struct {
	// SetStep gives steps a new status.
	SetStep map[string]string
	// Start, Compensate and Cancel list steps whose task (or compensation
	// task) should be created, or whose running task should be cancelled.
	Start      []string
	Compensate []string
	Cancel     []string
	// SetStatus, if not empty, is the workflow's new status.
	SetStatus string
	Error     string
}

func (p *Plan) Empty() bool {
	return len(p.SetStep) == 0 && len(p.Start) == 0 && len(p.Compensate) == 0 &&
		len(p.Cancel) == 0 && p.SetStatus == ""
}

// Reconcile decides the next actions for a run. It is a pure function of the
// definition and the state, which makes the engine easy to test and safe to
// re-run: applying the same state twice yields the same plan.
//
// The rules:
//   - A step's status follows its task: COMPLETED, FAILED or CANCELLED.
//   - While RUNNING, every PENDING step whose dependencies all COMPLETED
//     starts. Independent steps therefore run in parallel.
//   - When a step fails or is cancelled, or cancellation is requested, the
//     run switches to COMPENSATING: running steps are cancelled, pending ones
//     are skipped, and completed steps are compensated in reverse dependency
//     order. A step is compensated only after every step that depends on it
//     has been compensated (or never ran).
//   - The run ends COMPLETED, FAILED, or CANCELLED (if a user asked).
func Reconcile(d *Definition, r RunState) Plan {
	p := Plan{SetStep: make(map[string]string)}
	steps := make(map[string]StepState, len(r.Steps))
	for k, v := range r.Steps {
		steps[k] = v
	}
	set := func(name, status string) {
		s := steps[name]
		s.Status = status
		steps[name] = s
		p.SetStep[name] = status
	}

	// 1. Follow task results.
	var failure string
	for _, def := range d.Steps {
		s := steps[def.Name]
		switch s.Status {
		case StepRunning:
			switch s.TaskStatus {
			case taskCompleted:
				set(def.Name, StepCompleted)
			case taskFailed:
				set(def.Name, StepFailed)
			case taskCancelled:
				set(def.Name, StepCancelled)
			}
		case StepCompensating:
			switch s.CompensationStatus {
			case taskCompleted:
				set(def.Name, StepCompensated)
			case taskFailed, taskCancelled:
				set(def.Name, StepCompensationFailed)
			}
		}
		if st := steps[def.Name].Status; (st == StepFailed || st == StepCancelled) && failure == "" {
			failure = def.Name
		}
	}

	status := r.Status
	if status == StatusRunning && (r.CancelRequested || failure != "" || r.Abort != "") {
		status = StatusCompensating
		p.SetStatus = status
		switch {
		case r.CancelRequested:
			p.Error = "cancelled"
		case r.Abort != "":
			p.Error = r.Abort
		case steps[failure].Error != "":
			p.Error = "step " + failure + " failed: " + steps[failure].Error
		default:
			p.Error = "step " + failure + " " + strings.ToLower(steps[failure].Status)
		}
	}

	switch status {
	case StatusRunning:
		allDone := true
		for _, def := range d.Steps {
			s := steps[def.Name]
			if s.Status == StepPending && depsCompleted(def, steps) {
				p.Start = append(p.Start, def.Name)
				s.Status = StepRunning
				steps[def.Name] = s
			}
			if steps[def.Name].Status != StepCompleted {
				allDone = false
			}
		}
		if allDone {
			p.SetStatus = StatusCompleted
		}

	case StatusCompensating:
		running := false
		for _, def := range d.Steps {
			switch steps[def.Name].Status {
			case StepRunning:
				p.Cancel = append(p.Cancel, def.Name)
				running = true
			case StepPending:
				set(def.Name, StepSkipped)
			}
		}
		if running {
			// Wait for cancelled steps to stop: one may still complete and
			// need compensating.
			break
		}

		// Compensate in reverse dependency order. Steps without a
		// compensation are settled immediately, which can unblock their own
		// dependencies, so repeat until nothing changes.
		for changed := true; changed; {
			changed = false
			for i := len(d.Steps) - 1; i >= 0; i-- {
				def := d.Steps[i]
				if steps[def.Name].Status != StepCompleted || !dependentsSettled(d, def.Name, steps) {
					continue
				}
				if d.HasCompensation(def.Name) {
					p.Compensate = append(p.Compensate, def.Name)
					s := steps[def.Name]
					s.Status = StepCompensating
					steps[def.Name] = s
				} else {
					set(def.Name, StepCompensated)
				}
				changed = true
			}
		}

		busy := false
		for _, s := range steps {
			if s.Status == StepCompleted || s.Status == StepCompensating || s.Status == StepRunning {
				busy = true
			}
		}
		if !busy {
			if r.CancelRequested {
				p.SetStatus = StatusCancelled
			} else {
				p.SetStatus = StatusFailed
			}
		}
	}

	// Don't report a status change that isn't one.
	if p.SetStatus == r.Status {
		p.SetStatus = ""
	}
	return p
}

func depsCompleted(s Step, steps map[string]StepState) bool {
	for _, dep := range s.DependsOn {
		if steps[dep].Status != StepCompleted {
			return false
		}
	}
	return true
}

// dependentsSettled reports whether nothing that depends on name still has
// effects that must be undone first.
func dependentsSettled(d *Definition, name string, steps map[string]StepState) bool {
	for _, dep := range d.Dependents(name) {
		switch steps[dep].Status {
		case StepCompleted, StepCompensating, StepRunning:
			return false
		}
	}
	return true
}
