package workflow

import (
	"fmt"
	"slices"
	"strings"
	"testing"
)

// sim plays the coordinator: it applies plans and finishes tasks according
// to a script, recording what happened in which round.
type sim struct {
	t      *testing.T
	d      *Definition
	run    RunState
	result map[string]string // step -> task result (COMPLETED/FAILED/hang)
	comp   map[string]string // step -> compensation result (default COMPLETED)
	events []string          // "round:verb:step"
	round  int
}

func newSim(t *testing.T, yaml string, result map[string]string) *sim {
	t.Helper()
	d, err := Parse([]byte(yaml))
	if err != nil {
		t.Fatal(err)
	}
	s := &sim{t: t, d: d, result: result, comp: map[string]string{},
		run: RunState{Status: StatusRunning, Steps: map[string]StepState{}}}
	for _, st := range d.Steps {
		s.run.Steps[st.Name] = StepState{Status: StepPending}
	}
	return s
}

// reconcile applies plans until there is nothing more to do right now.
func (s *sim) reconcile() {
	for i := 0; ; i++ {
		if i > 50 {
			s.t.Fatal("reconcile did not converge")
		}
		p := Reconcile(s.d, s.run)
		if p.Empty() {
			return
		}
		for name, st := range p.SetStep {
			s.update(name, func(x *StepState) { x.Status = st })
		}
		for _, name := range p.Start {
			s.events = append(s.events, fmt.Sprintf("%d:start:%s", s.round, name))
			s.update(name, func(x *StepState) { x.Status, x.TaskStatus = StepRunning, "STARTED" })
		}
		for _, name := range p.Cancel {
			if s.run.Steps[name].TaskStatus == "STARTED" {
				s.events = append(s.events, fmt.Sprintf("%d:cancel:%s", s.round, name))
				s.update(name, func(x *StepState) { x.TaskStatus = taskCancelled })
			}
		}
		for _, name := range p.Compensate {
			s.events = append(s.events, fmt.Sprintf("%d:compensate:%s", s.round, name))
			s.update(name, func(x *StepState) { x.Status, x.CompensationStatus = StepCompensating, "STARTED" })
		}
		if p.SetStatus != "" {
			s.run.Status = p.SetStatus
		}
	}
}

// tick finishes every running task and compensation, as scripted.
func (s *sim) tick() {
	s.round++
	for name, st := range s.run.Steps {
		if st.TaskStatus == "STARTED" && s.result[name] != "hang" {
			r := s.result[name]
			if r == "" {
				r = taskCompleted
			}
			s.update(name, func(x *StepState) { x.TaskStatus = r; x.Error = "exit 1" })
		}
		if st.CompensationStatus == "STARTED" {
			r := s.comp[name]
			if r == "" {
				r = taskCompleted
			}
			s.update(name, func(x *StepState) { x.CompensationStatus = r })
		}
	}
}

func (s *sim) update(name string, fn func(*StepState)) {
	st := s.run.Steps[name]
	fn(&st)
	s.run.Steps[name] = st
}

func (s *sim) runToEnd() {
	for range 20 {
		s.reconcile()
		if s.run.Status != StatusRunning && s.run.Status != StatusCompensating {
			return
		}
		s.tick()
	}
	s.t.Fatalf("workflow never finished: %+v", s.run)
}

func (s *sim) statusOf(name string) string { return s.run.Steps[name].Status }

func (s *sim) roundOf(verb, name string) int {
	for _, e := range s.events {
		var r int
		var v, n string
		parts := strings.SplitN(e, ":", 3)
		fmt.Sscan(parts[0], &r)
		v, n = parts[1], parts[2]
		if v == verb && n == name {
			return r
		}
	}
	return -1
}

const diamond = `
name: diamond
steps:
  - name: a
    run: echo a
    compensate: undo a
  - name: b
    depends_on: [a]
    run: echo b
    compensate: undo b
  - name: c
    depends_on: [a]
    run: echo c
  - name: d
    depends_on: [b, c]
    run: echo d
    compensate: undo d
`

func TestDiamondRunsBranchesInParallel(t *testing.T) {
	s := newSim(t, diamond, nil)
	s.runToEnd()

	if s.run.Status != StatusCompleted {
		t.Fatalf("status %s, want COMPLETED", s.run.Status)
	}
	if a, b, c, d := s.roundOf("start", "a"), s.roundOf("start", "b"), s.roundOf("start", "c"), s.roundOf("start", "d"); !(a < b && b == c && c < d) {
		t.Fatalf("start rounds a=%d b=%d c=%d d=%d; want a < b == c < d. events: %v", a, b, c, d, s.events)
	}
}

func TestFailureCompensatesInReverseDependencyOrder(t *testing.T) {
	// c fails while b is still running; b finishes anyway (it is not
	// cancellable in this script), so b and a must both be undone, b first.
	s := newSim(t, diamond, map[string]string{"c": taskFailed})
	s.runToEnd()

	if s.run.Status != StatusFailed {
		t.Fatalf("status %s, want FAILED", s.run.Status)
	}
	want := map[string]string{"a": StepCompensated, "b": StepCompensated, "c": StepFailed, "d": StepSkipped}
	for name, st := range want {
		if got := s.statusOf(name); got != st {
			t.Errorf("step %s = %s, want %s", name, got, st)
		}
	}
	if b, a := s.roundOf("compensate", "b"), s.roundOf("compensate", "a"); !(b >= 0 && a > b) {
		t.Fatalf("compensate rounds b=%d a=%d; want b before a. events: %v", b, a, s.events)
	}
}

func TestFailureCancelsRunningSiblings(t *testing.T) {
	s := newSim(t, diamond, map[string]string{"b": "hang", "c": taskFailed})
	s.runToEnd()

	if s.roundOf("cancel", "b") < 0 {
		t.Fatalf("the hanging sibling was not cancelled: %v", s.events)
	}
	want := map[string]string{"a": StepCompensated, "b": StepCancelled, "c": StepFailed, "d": StepSkipped}
	for name, st := range want {
		if got := s.statusOf(name); got != st {
			t.Errorf("step %s = %s, want %s", name, got, st)
		}
	}
	if s.run.Status != StatusFailed {
		t.Fatalf("status %s, want FAILED", s.run.Status)
	}
}

func TestCancelRequestedEndsCancelled(t *testing.T) {
	s := newSim(t, diamond, map[string]string{"b": "hang", "c": "hang"})
	s.reconcile()
	s.tick() // a completes; b and c start and hang
	s.reconcile()
	s.run.CancelRequested = true
	s.runToEnd()

	if s.run.Status != StatusCancelled {
		t.Fatalf("status %s, want CANCELLED", s.run.Status)
	}
	if s.statusOf("a") != StepCompensated || s.statusOf("d") != StepSkipped {
		t.Fatalf("steps: %+v", s.run.Steps)
	}
}

func TestChainCompensatesStrictlyBackwards(t *testing.T) {
	s := newSim(t, `
name: chain
steps:
  - {name: one, run: "1", compensate: "undo 1"}
  - {name: two, run: "2", compensate: "undo 2", depends_on: [one]}
  - {name: three, run: "3", compensate: "undo 3", depends_on: [two]}
  - {name: four, run: "4", depends_on: [three]}
`, map[string]string{"four": taskFailed})
	s.runToEnd()

	r3, r2, r1 := s.roundOf("compensate", "three"), s.roundOf("compensate", "two"), s.roundOf("compensate", "one")
	if !(r3 < r2 && r2 < r1) {
		t.Fatalf("compensation rounds three=%d two=%d one=%d; want strictly backwards. events: %v", r3, r2, r1, s.events)
	}
}

func TestStepsWithoutCompensationSettleImmediately(t *testing.T) {
	s := newSim(t, `
name: plain
steps:
  - {name: a, run: "a"}
  - {name: b, run: "b", depends_on: [a]}
`, map[string]string{"b": taskFailed})
	s.runToEnd()

	if s.run.Status != StatusFailed || s.statusOf("a") != StepCompensated {
		t.Fatalf("status %s, a=%s", s.run.Status, s.statusOf("a"))
	}
	if slices.ContainsFunc(s.events, func(e string) bool { return strings.Contains(e, "compensate") }) {
		t.Fatalf("a compensation task was created for a step without one: %v", s.events)
	}
}

func TestCompensationFailureContinues(t *testing.T) {
	s := newSim(t, diamond, map[string]string{"d": taskFailed})
	s.comp["b"] = taskFailed
	s.runToEnd()

	if s.statusOf("b") != StepCompensationFailed || s.statusOf("a") != StepCompensated {
		t.Fatalf("b=%s a=%s; want COMPENSATION_FAILED then a still COMPENSATED", s.statusOf("b"), s.statusOf("a"))
	}
	if s.run.Status != StatusFailed {
		t.Fatalf("status %s, want FAILED", s.run.Status)
	}
}

func TestFailureMessageNamesTheStep(t *testing.T) {
	s := newSim(t, diamond, map[string]string{"a": taskFailed})
	s.reconcile()
	s.tick()
	p := Reconcile(s.d, s.run)
	if !strings.Contains(p.Error, "step a failed") {
		t.Fatalf("error %q should name the failed step", p.Error)
	}
}
