//go:build e2e

package e2e

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// TestAgentSurvivesWorkerCrash is Phase 5's exit criterion: a multi-step
// agent survives killing its worker mid-run and resumes without repeating
// completed tool calls. (Named zz_ to run with the other disruptive tests.)
func TestAgentSurvivesWorkerCrash(t *testing.T) {
	needMockLLM(t)
	c := newClient(t)
	marker := uniqueName("crash-agent")
	name := uniqueName("durable-agent")
	applyDefinition(t, c, fmt.Sprintf(`
name: %s
defaults: {retries: 3, retry_delay: 1s}
steps:
  - name: work
    type: agent
    agent:
      prompt: |
        Gather the facts, then the slow report, then answer.
        [[mock: {"tool_calls": [[{"name": "facts", "arguments": {"n": 1}}, {"name": "facts", "arguments": {"n": 2}}],
                                [{"name": "slow_report"}],
                                [{"name": "facts", "arguments": {"n": 3}}]],
                 "answer": "all done"}]]
      tools:
        - {name: facts, run: 'echo "fact $ARG_N"'}
        - {name: slow_report, run: 'sleep 12; echo "%s report ready"', timeout: 2m}
`, name, marker))
	wf := startWorkflow(t, c, name, nil)

	// Kill the worker while turn 2's slow tool call runs; turn 1's two
	// calls have finished by then.
	victim := workerRunning(t, marker)
	restartLater(t, victim)
	docker(t, "kill", victim)

	wf = waitWorkflow(t, c, wf.ID, 3*time.Minute)
	if wf.Status != "COMPLETED" || wf.Step("work").Outputs["answer"] != "all done" {
		t.Fatalf("workflow %s (%s), answer %q", wf.Status, wf.ErrorMessage, wf.Step("work").Outputs["answer"])
	}
	run, err := c.GetAgentRun(ctxTimeout(t, 10*time.Second), wf.Step("work").AgentRunID)
	if err != nil {
		t.Fatal(err)
	}

	// One task per call, ever: nothing was re-created...
	seen := map[string]int{}
	for _, task := range run.Tasks {
		seen[fmt.Sprintf("%d/%s/%s", task.Turn, task.Role, task.ToolCallID)]++
	}
	for call, n := range seen {
		if n != 1 {
			t.Errorf("call %s has %d tasks", call, n)
		}
	}
	// Turns: 1 (facts x2), 2 (slow_report), 3 (facts), 4 (the answer).
	if run.Turn != 4 || run.ToolCalls != 4 || len(run.Tasks) != 8 {
		t.Errorf("turn %d, %d tool calls, %d tasks; want 4 model calls and 4 tool calls", run.Turn, run.ToolCalls, len(run.Tasks))
	}
	// ...and only the call that was in flight ran again.
	for _, task := range run.Tasks {
		inFlight := strings.Contains(task.Command, marker)
		switch {
		case inFlight && task.Attempt != 2:
			t.Errorf("the interrupted slow_report ran %d times, want 2 (once interrupted, once retried)", task.Attempt)
		case !inFlight && task.Attempt != 1:
			t.Errorf("turn %d %s call %s was repeated (attempt %d)", task.Turn, task.Role, task.ToolCallID, task.Attempt)
		}
	}
	t.Logf("agent finished after %d turns and %d tool calls; only the interrupted call was retried", run.Turn, run.ToolCalls)
}
