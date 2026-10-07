//go:build e2e

package e2e

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ChinmayNoob/conductor/pkg/client"
)

// Core behaviour from Phases 0 and 1. Tests that stop workers live in
// zz_disruptive_test.go so they run last.

func TestWorkersRegisterWithUniqueIDs(t *testing.T) {
	c := newClient(t)
	ws, err := c.ListWorkers(ctxTimeout(t, 10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	healthy := 0
	byAddr := make(map[string]int64)
	for _, w := range ws {
		if w.Status != "healthy" {
			continue
		}
		healthy++
		if other, dup := byAddr[w.Address]; dup {
			t.Fatalf("workers %d and %d share address %s", w.ID, other, w.Address)
		}
		byAddr[w.Address] = w.ID
	}
	if healthy < workers {
		t.Fatalf("%d healthy workers, want at least %d: %+v", healthy, workers, ws)
	}
}

func TestAuth(t *testing.T) {
	c := newClient(t)
	ctx := ctxTimeout(t, 30*time.Second)

	wantStatus := func(err error, code int) {
		t.Helper()
		if client.StatusCode(err) != code {
			t.Fatalf("got %v, want HTTP %d", err, code)
		}
	}

	_, err := client.New(apiURL, "").ListTasks(ctx, client.TaskFilter{Limit: 1})
	wantStatus(err, http.StatusUnauthorized)
	_, err = client.New(apiURL, "cnd_not_a_real_key").ListTasks(ctx, client.TaskFilter{Limit: 1})
	wantStatus(err, http.StatusUnauthorized)

	key, err := c.CreateAPIKey(ctx, uniqueName("e2e-key"), "", false)
	if err != nil {
		t.Fatal(err)
	}
	scoped := client.New(apiURL, key.Key)
	if _, err := scoped.ListTasks(ctx, client.TaskFilter{Limit: 1}); err != nil {
		t.Fatalf("new key rejected: %v", err)
	}
	_, err = scoped.ListAPIKeys(ctx)
	wantStatus(err, http.StatusForbidden)

	if err := c.RevokeAPIKey(ctx, key.ID); err != nil {
		t.Fatal(err)
	}
	_, err = scoped.ListTasks(ctx, client.TaskFilter{Limit: 1})
	wantStatus(err, http.StatusUnauthorized)
}

func TestTaskLifecycle(t *testing.T) {
	c := newClient(t)

	task := submit(t, c, client.TaskRequest{Command: "echo hello e2e && echo to-stderr >&2"})
	task = waitTask(t, c, task.ID, time.Minute)
	if task.Status != "COMPLETED" {
		t.Fatalf("status %s (%s), want COMPLETED", task.Status, task.ErrorMessage)
	}
	if !strings.Contains(task.Output, "hello e2e") || !strings.Contains(task.Output, "to-stderr") {
		t.Fatalf("output %q is missing stdout or stderr", task.Output)
	}
	if task.StartedAt == nil || task.CompletedAt == nil || task.WorkerID == nil {
		t.Fatal("started_at, completed_at or worker_id not recorded")
	}

	failing := submit(t, c, client.TaskRequest{Command: "echo about to fail; exit 3", MaxRetries: client.Retries(1), RetryDelaySeconds: 1})
	failing = waitTask(t, c, failing.ID, time.Minute)
	if failing.Status != "FAILED" || failing.RetryCount != 1 || !strings.Contains(failing.Output, "about to fail") {
		t.Fatalf("failing task = %s, retries %d, output %q", failing.Status, failing.RetryCount, failing.Output)
	}
}

func TestDelayedTask(t *testing.T) {
	c := newClient(t)
	submitted := time.Now()
	task := submit(t, c, client.TaskRequest{Command: "echo later", DelaySeconds: 3})
	task = waitTask(t, c, task.ID, time.Minute)
	if task.Status != "COMPLETED" {
		t.Fatalf("status %s, want COMPLETED", task.Status)
	}
	if early := task.StartedAt.Sub(submitted); early < 2*time.Second {
		t.Fatalf("delayed task started after %v, want about 3s", early)
	}
}

func TestInputValidation(t *testing.T) {
	c := newClient(t)
	ctx := ctxTimeout(t, 30*time.Second)
	for _, req := range []client.TaskRequest{
		{},
		{Command: "echo", Priority: 11},
		{Command: "echo", DelaySeconds: -1},
		{Type: "lambda", Command: "x"},
		{Type: "http"},
		{Command: "echo", Queue: "Bad Queue"},
	} {
		if _, err := c.SubmitTask(ctx, req); client.StatusCode(err) != http.StatusBadRequest {
			t.Errorf("invalid task %+v: got %v, want HTTP 400", req, err)
		}
	}
}

func TestThroughputBurst(t *testing.T) {
	c := newClient(t)
	ctx := ctxTimeout(t, 2*time.Minute)
	slots := capacity(t, c)
	n := 5 * slots // five rounds of one-second tasks
	marker := uniqueName("burst")

	start := time.Now()
	ids := make([]string, n)
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			task, err := c.SubmitTask(ctx, client.TaskRequest{Command: fmt.Sprintf("sleep 1 && echo %s-%d", marker, i)})
			if err != nil {
				errs <- err
				return
			}
			ids[i] = task.ID
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	perWorker := make(map[int64]int)
	for _, id := range ids {
		task := waitTask(t, c, id, 2*time.Minute)
		if task.Status != "COMPLETED" {
			t.Fatalf("task %s: %s", id, task.Status)
		}
		perWorker[*task.WorkerID]++
	}
	elapsed := time.Since(start)
	ideal := 5 * time.Second
	t.Logf("%d one-second tasks on %d slots took %v (ideal %v); per worker: %v",
		n, slots, elapsed.Round(100*time.Millisecond), ideal, perWorker)
	if elapsed > 2*ideal+5*time.Second {
		t.Fatalf("burst took %v, more than twice the ideal %v", elapsed, ideal)
	}
	if len(perWorker) < workers {
		t.Fatalf("only %d workers got tasks, want at least %d", len(perWorker), workers)
	}
}

func TestWorkflowSucceeds(t *testing.T) {
	c := newClient(t)
	wf := startWorkflow(t, c, "trip_booking", map[string]any{"user_id": 123, "amount": 5000})
	wf = waitWorkflow(t, c, wf.ID, time.Minute)
	if wf.Status != "COMPLETED" {
		t.Fatalf("workflow %s: %s", wf.Status, wf.ErrorMessage)
	}
	wantSteps(t, wf, map[string]string{"book_flight": "COMPLETED", "book_hotel": "COMPLETED", "charge_payment": "COMPLETED"})
}

func TestWorkflowFailureCompensates(t *testing.T) {
	c := newClient(t)
	wf := startWorkflow(t, c, "trip_booking_fail", map[string]any{"user_id": 123, "amount": 5000})
	wf = waitWorkflow(t, c, wf.ID, 2*time.Minute)

	if wf.Status != "FAILED" || !strings.Contains(wf.ErrorMessage, "book_hotel") {
		t.Fatalf("workflow %s (%q), want FAILED naming book_hotel", wf.Status, wf.ErrorMessage)
	}
	wantSteps(t, wf, map[string]string{"book_flight": "COMPENSATED", "book_hotel": "FAILED", "charge_payment": "SKIPPED"})

	comp := waitTask(t, c, wf.Step("book_flight").CompensationTaskID, time.Minute)
	if !strings.Contains(comp.Output, "cancelling flight for user=123") {
		t.Fatalf("compensation output = %q", comp.Output)
	}
}

func TestUnknownWorkflow(t *testing.T) {
	c := newClient(t)
	_, err := c.StartWorkflow(ctxTimeout(t, 30*time.Second), client.WorkflowRequest{Workflow: "no_such_workflow"})
	if !client.IsNotFound(err) {
		t.Fatalf("got %v, want 404", err)
	}
	_, err = c.StartWorkflow(ctxTimeout(t, 30*time.Second), client.WorkflowRequest{Workflow: "trip_booking", Input: map[string]any{"user_id": 1}})
	if client.StatusCode(err) != http.StatusBadRequest {
		t.Fatalf("missing required input: got %v, want 400", err)
	}
}

func TestCancelQueuedTask(t *testing.T) {
	c := newClient(t)
	ctx := ctxTimeout(t, 30*time.Second)

	task := submit(t, c, client.TaskRequest{Command: "echo never", DelaySeconds: 300})
	task, err := c.CancelTask(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if task.Status != "CANCELLED" {
		t.Fatalf("status %s, want CANCELLED", task.Status)
	}
	if _, err := c.CancelTask(ctx, task.ID); client.StatusCode(err) != http.StatusConflict {
		t.Fatalf("second cancel: got %v, want HTTP 409", err)
	}
}

func TestCancelRunningTaskKillsProcess(t *testing.T) {
	c := newClient(t)
	marker := uniqueName("cancel-me")

	task := submit(t, c, client.TaskRequest{Command: "sleep 60; echo " + marker})
	workerRunning(t, marker)

	task, err := c.CancelTask(ctxTimeout(t, 30*time.Second), task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if task.Status != "CANCELLED" {
		t.Fatalf("status %s, want CANCELLED", task.Status)
	}

	deadline := time.Now().Add(10 * time.Second)
	for containerRunning(t, marker) != "" {
		if time.Now().After(deadline) {
			t.Fatal("the cancelled task's process is still running")
		}
		time.Sleep(300 * time.Millisecond)
	}
	if task, _ = c.GetTask(ctxTimeout(t, 10*time.Second), task.ID); task.Status != "CANCELLED" {
		t.Fatalf("status changed to %s after the worker reported", task.Status)
	}
}

func TestCancelWorkflowCompensates(t *testing.T) {
	c := newClient(t)
	ctx := ctxTimeout(t, 90*time.Second)

	wf := startWorkflow(t, c, "trip_booking_slow", map[string]any{"user_id": 42, "amount": 10})
	for {
		var err error
		if wf, err = c.GetWorkflow(ctx, wf.ID); err != nil {
			t.Fatal(err)
		}
		if s := wf.Step("book_hotel"); s != nil && s.TaskStatus == "STARTED" {
			break
		}
		time.Sleep(300 * time.Millisecond)
	}

	if _, err := c.CancelWorkflow(ctx, wf.ID); err != nil {
		t.Fatal(err)
	}
	wf = waitWorkflow(t, c, wf.ID, 90*time.Second)
	if wf.Status != "CANCELLED" {
		t.Fatalf("workflow %s, want CANCELLED", wf.Status)
	}
	wantSteps(t, wf, map[string]string{"book_flight": "COMPENSATED", "book_hotel": "CANCELLED", "charge_payment": "SKIPPED"})
}
