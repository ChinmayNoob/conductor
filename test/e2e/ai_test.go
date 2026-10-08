//go:build e2e

package e2e

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ChinmayNoob/conductor/pkg/client"
)

// The operations assistant tests use the mock model too (see llm_test.go).

// assistedNamespace creates a namespace with ai_assist on and returns a client
// acting in it.
func assistedNamespace(t *testing.T, aiAssist bool) *client.Client {
	t.Helper()
	admin := newClient(t)
	ns := uniqueName("ops")
	if _, err := admin.CreateNamespace(ctxTimeout(t, 10*time.Second), client.Namespace{Name: ns, AIAssist: aiAssist}); err != nil {
		t.Fatal(err)
	}
	return admin.WithNamespace(ns)
}

func TestAIAssistIsOptIn(t *testing.T) {
	needMockLLM(t)
	c := assistedNamespace(t, false)
	task := submit(t, c, client.TaskRequest{Command: "exit 3", MaxRetries: client.Retries(0)})
	waitTask(t, c, task.ID, time.Minute)

	for name, call := range map[string]func() error{
		"explain":  func() error { _, err := c.ExplainTask(ctxTimeout(t, 10*time.Second), task.ID); return err },
		"ask":      func() error { _, err := c.Ask(ctxTimeout(t, 10*time.Second), "what failed?"); return err },
		"workflow": func() error { _, err := c.DraftWorkflow(ctxTimeout(t, 10*time.Second), "say hi"); return err },
	} {
		if err := call(); client.StatusCode(err) != 403 {
			t.Errorf("%s without ai_assist: %v; want 403", name, err)
		}
	}
}

func TestAIExplainsFailedTask(t *testing.T) {
	needMockLLM(t)
	c := assistedNamespace(t, true)
	marker := uniqueName("explain")
	// The failure's own output scripts the mock model's explanation. The key
	// in the output must never reach the model, or the stored text.
	task := submit(t, c, client.TaskRequest{
		Command:    fmt.Sprintf(`echo 'order %s is already shipped; api_key=sk-test0123456789abcdefghij [[mock: {"json": {"class": "permanent", "confidence": 0.8, "cause": "The order shipped.", "fix": "Start a return instead."}}]]'; exit 1`, marker),
		MaxRetries: client.Retries(0),
	})
	if task = waitTask(t, c, task.ID, time.Minute); task.Status != "FAILED" {
		t.Fatalf("task %s", task.Status)
	}
	// The assistant explains new failures on its own...
	var e *client.Explanation
	deadline := time.Now().Add(time.Minute)
	for time.Now().Before(deadline) {
		var err error
		if e, err = c.GetExplanation(ctxTimeout(t, 10*time.Second), task.ID); err == nil {
			break
		}
		time.Sleep(time.Second)
	}
	if e == nil || e.TaskID != task.ID {
		t.Fatal("the failure was not explained within a minute")
	}
	if e.Class != "permanent" || e.Source != "model" || e.Cause != "The order shipped." || e.Fix == "" || e.InputTokens == 0 {
		t.Fatalf("explanation %+v", e)
	}
	// ...and on request. A transient failure is classified by rules.
	flaky := submit(t, c, client.TaskRequest{Command: "echo 'connection refused'; exit 1", MaxRetries: client.Retries(0)})
	waitTask(t, c, flaky.ID, time.Minute)
	e, err := c.ExplainTask(ctxTimeout(t, 30*time.Second), flaky.ID)
	if err != nil {
		t.Fatal(err)
	}
	if e.Class != "transient" || e.Source != "rules" {
		t.Fatalf("explanation %+v", e)
	}
	// Only failed tasks are explained.
	ok := submit(t, c, client.TaskRequest{Command: "true"})
	waitTask(t, c, ok.ID, time.Minute)
	if _, err := c.ExplainTask(ctxTimeout(t, 10*time.Second), ok.ID); client.StatusCode(err) != 409 {
		t.Fatalf("explaining a completed task: %v; want 409", err)
	}
}

func TestAIDraftsAWorkflowForApproval(t *testing.T) {
	needMockLLM(t)
	c := assistedNamespace(t, true)
	name := uniqueName("drafted")
	yaml := fmt.Sprintf(`name: %s\nsteps:\n  - name: build\n    run: echo build\n  - name: test\n    depends_on: [build]\n    run: echo test`, name)
	d, err := c.DraftWorkflow(ctxTimeout(t, 30*time.Second),
		`Build, then test. [[mock: {"answer": "`+yaml+`"}]]`)
	if err != nil {
		t.Fatal(err)
	}
	if !d.Valid || d.Name != name || len(d.Plan) != 2 {
		t.Fatalf("draft %+v", d)
	}
	// Drafting saves nothing: the definition appears only once a person
	// applies it.
	if defs, err := c.ListDefinitions(ctxTimeout(t, 10*time.Second)); err != nil || len(defs) != 0 {
		t.Fatalf("definitions after drafting: %v, %v", defs, err)
	}
	if _, err := c.ApplyDefinition(ctxTimeout(t, 10*time.Second), []byte(d.YAML)); err != nil {
		t.Fatal(err)
	}
}

func TestAIAsksTheCluster(t *testing.T) {
	needMockLLM(t)
	c := assistedNamespace(t, true)
	marker := uniqueName("asked")
	task := submit(t, c, client.TaskRequest{Command: "echo " + marker + "; exit 2", MaxRetries: client.Retries(0)})
	waitTask(t, c, task.ID, time.Minute)

	// The model looks the failure up (read-only) and answers.
	ans, err := c.Ask(ctxTimeout(t, 60*time.Second),
		`What failed? [[mock: {"tool_calls": [[{"name": "task_counts"}, {"name": "list_tasks", "arguments": {"status": "FAILED"}}]], "answer": "One task failed."}]]`)
	if err != nil {
		t.Fatal(err)
	}
	if ans.Answer != "One task failed." || len(ans.Lookups) != 2 || !strings.HasPrefix(ans.Lookups[0], "task_counts") {
		t.Fatalf("answer %+v", ans)
	}
}
