//go:build e2e

package e2e

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// The agent tests script the mock model: the directive in the prompt names
// the tool calls of each turn and the final answer.

func TestAgentCallsToolsAndAnswers(t *testing.T) {
	needMockLLM(t)
	c := newClient(t)
	name := uniqueName("support-agent")
	applyDefinition(t, c, fmt.Sprintf(`
name: %s
steps:
  - name: investigate
    type: agent
    agent:
      system: You are an on-call assistant.
      prompt: |
        Why is order A7 late?
        [[mock: {"tool_calls": [[{"name": "lookup_order", "arguments": {"id": "A7"}}],
                                [{"name": "notify", "arguments": {"to": "ops", "note": "A7 shipped"}}]],
                 "answer": "Order A7 shipped yesterday; ops has been told."}]]
      tools:
        - name: lookup_order
          description: Look up an order by ID
          parameters: {type: object, properties: {id: {type: string}}, required: [id]}
          run: 'echo "order $ARG_ID: shipped yesterday"'
        - name: notify
          parameters: {type: object, properties: {to: {type: string}, note: {type: string}}}
          run: 'echo "told $ARG_TO: $ARG_NOTE"'
  - name: report
    depends_on: [investigate]
    run: 'echo "agent says: $ANSWER"'
    env:
      ANSWER: "${{ steps.investigate.outputs.answer }}"
`, name))
	wf := waitWorkflow(t, c, startWorkflow(t, c, name, nil).ID, 2*time.Minute)
	if wf.Status != "COMPLETED" {
		t.Fatalf("workflow %s: %s", wf.Status, wf.ErrorMessage)
	}
	report := waitTask(t, c, wf.Step("report").TaskID, time.Minute)
	if !strings.Contains(report.Output, "agent says: Order A7 shipped yesterday; ops has been told.") {
		t.Fatalf("report %q", report.Output)
	}

	run, err := c.GetAgentRun(ctxTimeout(t, 10*time.Second), wf.Step("investigate").AgentRunID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != "COMPLETED" || run.Turn != 3 || run.ToolCalls != 2 || len(run.Tasks) != 5 {
		t.Fatalf("run %s, turn %d, %d tool calls, %d tasks; want 3 model turns and 2 tool calls",
			run.Status, run.Turn, run.ToolCalls, len(run.Tasks))
	}
	// The tools' outputs went back to the model; the arguments came in as
	// environment variables.
	transcript := string(run.Messages)
	for _, want := range []string{"order A7: shipped yesterday", "told ops: A7 shipped"} {
		if !strings.Contains(transcript, want) {
			t.Errorf("the conversation lacks the tool result %q", want)
		}
	}
	if run.LLMSpend.Tokens == 0 {
		t.Error("the agent's spend was not counted")
	}
}

func TestAgentLimits(t *testing.T) {
	needMockLLM(t)
	c := newClient(t)
	name := uniqueName("looping-agent")
	// The model keeps calling tools; max_turns stops it.
	applyDefinition(t, c, fmt.Sprintf(`
name: %s
steps:
  - name: loop
    type: agent
    agent:
      prompt: '[[mock: {"tool_calls": [[{"name": "ping"}], [{"name": "ping"}], [{"name": "ping"}], [{"name": "ping"}]]}]]'
      max_turns: 2
      tools:
        - {name: ping, run: echo pong}
    compensate: echo "cleaning up"
`, name))
	wf := waitWorkflow(t, c, startWorkflow(t, c, name, nil).ID, 2*time.Minute)
	if wf.Status != "FAILED" || !strings.Contains(wf.ErrorMessage, "max_turns (2)") {
		t.Fatalf("workflow %s: %q; want FAILED at max_turns", wf.Status, wf.ErrorMessage)
	}
}
