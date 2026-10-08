//go:build e2e

package e2e

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ChinmayNoob/conductor/pkg/client"
)

// The llm tests script the mock model server ([[mock: ...]] directives), so
// they need the stack pointed at it:
//
//	CONDUCTOR_LLM_BASE_URL=http://mock-llm:8090/v1 docker compose --profile mock-llm ... up
//	E2E_LLM=mock go test -tags e2e ./test/e2e/
func needMockLLM(t *testing.T) {
	t.Helper()
	if env("E2E_LLM", "") != "mock" {
		t.Skip("set E2E_LLM=mock and run the stack with the mock-llm profile")
	}
}

func TestLLMTaskStructuredOutput(t *testing.T) {
	needMockLLM(t)
	c := newClient(t)
	marker := uniqueName("llm")
	task := submit(t, c, client.TaskRequest{Type: "llm", LLM: &client.LLMSpec{
		Model:  "mock-model",
		Prompt: fmt.Sprintf(`[[mock: {"json": {"severity": "high", "marker": %q}}]] Classify: the site is down`, marker),
		Schema: map[string]any{"type": "object", "properties": map[string]any{"severity": map[string]any{"type": "string"}}},
	}})
	task = waitTask(t, c, task.ID, time.Minute)
	if task.Status != "COMPLETED" || task.Outputs["severity"] != "high" || task.Outputs["marker"] != marker {
		t.Fatalf("task %s, outputs %v, error %q", task.Status, task.Outputs, task.ErrorMessage)
	}
	if u := task.LLMUsage; u == nil || u.Model != "mock-model" || u.InputTokens == 0 || u.OutputTokens == 0 {
		t.Fatalf("usage = %+v", task.LLMUsage)
	}
}

func TestLLMStepOutputsFeedTheNextStep(t *testing.T) {
	needMockLLM(t)
	c := newClient(t)
	name := uniqueName("triage")
	applyDefinition(t, c, fmt.Sprintf(`
name: %s
steps:
  - name: classify
    type: llm
    llm:
      prompt: '[[mock: {"json": {"severity": "high"}}]] Classify the ticket.'
      schema: {type: object, properties: {severity: {type: string}}}
  - name: page
    depends_on: [classify]
    run: echo "paging on-call for a $SEVERITY ticket"
    env:
      SEVERITY: "${{ steps.classify.outputs.severity }}"
`, name))
	wf := waitWorkflow(t, c, startWorkflow(t, c, name, nil).ID, time.Minute)
	if wf.Status != "COMPLETED" {
		t.Fatalf("workflow %s: %s", wf.Status, wf.ErrorMessage)
	}
	page := waitTask(t, c, wf.Step("page").TaskID, time.Minute)
	if !strings.Contains(page.Output, "paging on-call for a high ticket") {
		t.Fatalf("page output %q", page.Output)
	}
	if wf.LLMSpend == nil || wf.LLMSpend.Tokens == 0 {
		t.Fatalf("workflow spend = %+v", wf.LLMSpend)
	}
}

func TestLLMRateLimitWaitsForRetryAfter(t *testing.T) {
	needMockLLM(t)
	c := newClient(t)
	marker := uniqueName("ratelimit")
	task := submit(t, c, client.TaskRequest{Type: "llm", MaxRetries: client.Retries(2), RetryDelaySeconds: 1,
		LLM: &client.LLMSpec{Prompt: fmt.Sprintf(`[[mock: {"fail": 429, "retry_after": 4, "answer": %q}]]`, marker)}})
	task = waitTask(t, c, task.ID, time.Minute)
	if task.Status != "COMPLETED" || task.Output != marker {
		t.Fatalf("task %s, output %q, error %q", task.Status, task.Output, task.ErrorMessage)
	}
	attempts, err := c.TaskAttempts(ctxTimeout(t, 10*time.Second), task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(attempts) != 2 || !strings.Contains(attempts[0].Error, "rate limited") {
		t.Fatalf("attempts = %+v", attempts)
	}
	// The retry waited for the provider's 4s, not the task's 1s back-off.
	if gap := attempts[1].StartedAt.Sub(*attempts[0].FinishedAt); gap < 3500*time.Millisecond {
		t.Fatalf("retried after %v, want the 4s Retry-After", gap)
	}
}

func TestLLMPermanentErrorSkipsRetries(t *testing.T) {
	needMockLLM(t)
	c := newClient(t)
	task := submit(t, c, client.TaskRequest{Type: "llm", MaxRetries: client.Retries(3), RetryDelaySeconds: 1,
		LLM: &client.LLMSpec{Prompt: fmt.Sprintf(`[[mock: {"fail": 400, "fail_times": 9}]] %s`, uniqueName("bad"))}})
	task = waitTask(t, c, task.ID, time.Minute)
	if task.Status != "FAILED" || task.RetryCount != 0 || !strings.Contains(task.ErrorMessage, "400") {
		t.Fatalf("task %s after %d retries: %s; want FAILED at once", task.Status, task.RetryCount, task.ErrorMessage)
	}
}

func TestWorkflowBudgetCompensates(t *testing.T) {
	needMockLLM(t)
	c := newClient(t)
	name := uniqueName("budget")
	// Each llm step's long prompt costs well over the 60-token budget.
	long := strings.Repeat("Consider every detail of the order history carefully. ", 8)
	applyDefinition(t, c, fmt.Sprintf(`
name: %s
budget: {max_tokens: 60}
steps:
  - name: draft
    type: llm
    llm: {prompt: %q}
    compensate: echo "discarding the draft"
  - name: review
    depends_on: [draft]
    type: llm
    llm: {prompt: %q}
`, name, long, long))
	wf := waitWorkflow(t, c, startWorkflow(t, c, name, nil).ID, time.Minute)
	if wf.Status != "FAILED" || !strings.Contains(wf.ErrorMessage, "budget exceeded") {
		t.Fatalf("workflow %s: %q; want FAILED over budget", wf.Status, wf.ErrorMessage)
	}
	wantSteps(t, wf, map[string]string{"draft": "COMPENSATED", "review": "SKIPPED"})
}

func TestNamespaceDailyModelBudget(t *testing.T) {
	needMockLLM(t)
	admin := newClient(t)
	ns := uniqueName("frugal")
	limit := int64(1)
	if _, err := admin.CreateNamespace(ctxTimeout(t, 10*time.Second), client.Namespace{Name: ns, MaxLLMTokensPerDay: &limit}); err != nil {
		t.Fatal(err)
	}
	c := admin.WithNamespace(ns)
	req := client.TaskRequest{Type: "llm", LLM: &client.LLMSpec{Prompt: "hello"}}

	// The first task is allowed (nothing spent yet) and spends the budget.
	first := submit(t, c, req)
	if first = waitTask(t, c, first.ID, time.Minute); first.Status != "COMPLETED" {
		t.Fatalf("first task %s: %s", first.Status, first.ErrorMessage)
	}
	_, err := c.SubmitTask(ctxTimeout(t, 10*time.Second), req)
	if client.StatusCode(err) != 429 || !strings.Contains(err.Error(), "daily model budget") {
		t.Fatalf("second submission: %v; want 429 for the spent daily budget", err)
	}
	// Other task types are unaffected.
	submit(t, c, client.TaskRequest{Command: "echo still fine"})
}
