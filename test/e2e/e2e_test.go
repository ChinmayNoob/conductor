//go:build e2e

package e2e

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ChinmayNoob/conductor/pkg/client"
)

// Tests run in file order. The ones that stop workers come last.

func TestWorkersRegisterWithUniqueIDs(t *testing.T) {
	newClient(t)
	ids := registeredWorkers(t)
	if len(ids) < workers {
		t.Fatalf("%d workers registered, want at least %d: %v", len(ids), workers, ids)
	}
	byAddr := make(map[string]string)
	for id, addr := range ids {
		if other, dup := byAddr[addr]; dup {
			t.Fatalf("workers %s and %s share address %s", id, other, addr)
		}
		byAddr[addr] = id
	}
}

func TestAuth(t *testing.T) {
	c := newClient(t)
	ctx := ctxTimeout(t, 30*time.Second)

	wantStatus := func(err error, code int) {
		t.Helper()
		var apiErr *client.APIError
		if !errors.As(err, &apiErr) || apiErr.StatusCode != code {
			t.Fatalf("got %v, want HTTP %d", err, code)
		}
	}

	_, err := client.New(apiURL, "").ListTasks(ctx, "", 1)
	wantStatus(err, http.StatusUnauthorized)
	_, err = client.New(apiURL, "cnd_not_a_real_key").ListTasks(ctx, "", 1)
	wantStatus(err, http.StatusUnauthorized)

	key, err := c.CreateAPIKey(ctx, "e2e-"+uniqueMarker("key"), false)
	if err != nil {
		t.Fatal(err)
	}
	scoped := client.New(apiURL, key.Key)
	if _, err := scoped.ListTasks(ctx, "", 1); err != nil {
		t.Fatalf("new key rejected: %v", err)
	}
	_, err = scoped.ListAPIKeys(ctx)
	wantStatus(err, http.StatusForbidden)

	if err := c.RevokeAPIKey(ctx, key.ID); err != nil {
		t.Fatal(err)
	}
	_, err = scoped.ListTasks(ctx, "", 1)
	wantStatus(err, http.StatusUnauthorized)
}

func TestTaskLifecycle(t *testing.T) {
	c := newClient(t)
	ctx := ctxTimeout(t, 60*time.Second)

	task, err := c.SubmitTask(ctx, client.TaskRequest{Data: "echo hello e2e && echo to-stderr >&2"})
	if err != nil {
		t.Fatal(err)
	}
	task, err = c.WaitForTask(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if task.Status != "COMPLETED" {
		t.Fatalf("status %s (%s), want COMPLETED", task.Status, task.ErrorMessage)
	}
	if !strings.Contains(task.Output, "hello e2e") || !strings.Contains(task.Output, "to-stderr") {
		t.Fatalf("output %q is missing stdout or stderr", task.Output)
	}
	if task.StartedAt == nil || task.CompletedAt == nil {
		t.Fatal("started_at or completed_at not recorded")
	}

	failing, err := c.SubmitTask(ctx, client.TaskRequest{Data: "echo about to fail; exit 3", MaxRetries: 1, RetryDelaySeconds: 1})
	if err != nil {
		t.Fatal(err)
	}
	failing, err = c.WaitForTask(ctx, failing.ID)
	if err != nil {
		t.Fatal(err)
	}
	if failing.Status != "FAILED" || failing.RetryCount != 1 || !strings.Contains(failing.Output, "about to fail") {
		t.Fatalf("failing task = %s, retries %d, output %q", failing.Status, failing.RetryCount, failing.Output)
	}
}

func TestDelayedTask(t *testing.T) {
	c := newClient(t)
	ctx := ctxTimeout(t, 60*time.Second)

	submitted := time.Now()
	task, err := c.SubmitTask(ctx, client.TaskRequest{Data: "echo later", DelaySeconds: 3})
	if err != nil {
		t.Fatal(err)
	}
	task, err = c.WaitForTask(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
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
		{Data: "echo", Priority: 11},
		{Data: "echo", DelaySeconds: -1},
	} {
		if _, err := c.SubmitTask(ctx, req); err == nil {
			t.Errorf("invalid task %+v was accepted", req)
		}
	}
}

func TestThroughputBurst(t *testing.T) {
	c := newClient(t)
	ctx := ctxTimeout(t, 2*time.Minute)
	const n = 30
	marker := uniqueMarker("burst")

	start := time.Now()
	ids := make([]string, n)
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			task, err := c.SubmitTask(ctx, client.TaskRequest{Data: fmt.Sprintf("sleep 1 && echo %s-%d", marker, i)})
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

	for _, id := range ids {
		task, err := c.WaitForTask(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if task.Status != "COMPLETED" {
			t.Fatalf("task %s: %s", id, task.Status)
		}
	}
	elapsed := time.Since(start)
	ideal := time.Duration(n/workers) * time.Second
	t.Logf("%d one-second tasks on %d workers took %v (ideal %v)", n, workers, elapsed.Round(100*time.Millisecond), ideal)
	if elapsed > 2*ideal+5*time.Second {
		t.Fatalf("burst took %v, more than twice the ideal %v", elapsed, ideal)
	}

	perWorker := make(map[string]int)
	dispatched := dispatchedTo(t)
	for _, id := range ids {
		perWorker[dispatched[id]]++
	}
	t.Logf("tasks per worker: %v", perWorker)
	if len(perWorker) < workers {
		t.Fatalf("only %d of %d workers got tasks", len(perWorker), workers)
	}
}

func TestWorkflowSucceeds(t *testing.T) {
	c := newClient(t)
	ctx := ctxTimeout(t, 60*time.Second)

	wf, err := c.StartWorkflow(ctx, "trip_booking", map[string]any{"user_id": 123, "amount": 5000})
	if err != nil {
		t.Fatal(err)
	}
	wf, err = c.WaitForWorkflow(ctx, wf.ID)
	if err != nil {
		t.Fatal(err)
	}
	if wf.Status != "COMPLETED" {
		t.Fatalf("workflow %s: %s", wf.Status, wf.ErrorMessage)
	}
	for _, s := range wf.Steps {
		if s.Status != "COMPLETED" {
			t.Errorf("step %s is %s, want COMPLETED", s.Name, s.Status)
		}
	}
}

func TestWorkflowFailureCompensates(t *testing.T) {
	c := newClient(t)
	ctx := ctxTimeout(t, 2*time.Minute)

	wf, err := c.StartWorkflow(ctx, "trip_booking_fail", map[string]any{"user_id": 123, "amount": 5000})
	if err != nil {
		t.Fatal(err)
	}
	wf, err = c.WaitForWorkflow(ctx, wf.ID)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"Book Flight": "COMPENSATED", "Book Hotel": "FAILED", "Charge Payment": "PENDING"}
	if wf.Status != "FAILED" {
		t.Fatalf("workflow %s, want FAILED", wf.Status)
	}
	for _, s := range wf.Steps {
		if s.Status != want[s.Name] {
			t.Errorf("step %s is %s, want %s", s.Name, s.Status, want[s.Name])
		}
	}

	comp, err := c.GetTask(ctx, wf.Steps[0].CompensationTaskID)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(comp.Output, "cancelling flight for user=123") {
		t.Fatalf("compensation output = %q", comp.Output)
	}
}

func TestWorkflowRejectsShellInjection(t *testing.T) {
	c := newClient(t)
	ctx := ctxTimeout(t, 30*time.Second)

	_, err := c.StartWorkflow(ctx, "trip_booking", map[string]any{"user_id": "1; touch /tmp/pwned", "amount": 5})
	var apiErr *client.APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusBadRequest {
		t.Fatalf("got %v, want HTTP 400", err)
	}
	if _, err := c.StartWorkflow(ctx, "no_such_workflow", nil); !client.IsNotFound(err) {
		t.Fatalf("unknown workflow type: got %v, want 404", err)
	}
}

func TestCancelQueuedTask(t *testing.T) {
	c := newClient(t)
	ctx := ctxTimeout(t, 30*time.Second)

	task, err := c.SubmitTask(ctx, client.TaskRequest{Data: "echo never", DelaySeconds: 300})
	if err != nil {
		t.Fatal(err)
	}
	task, err = c.CancelTask(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if task.Status != "CANCELLED" {
		t.Fatalf("status %s, want CANCELLED", task.Status)
	}
	var apiErr *client.APIError
	if _, err := c.CancelTask(ctx, task.ID); !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusConflict {
		t.Fatalf("second cancel: got %v, want HTTP 409", err)
	}
}

func TestCancelRunningTaskKillsProcess(t *testing.T) {
	c := newClient(t)
	ctx := ctxTimeout(t, 60*time.Second)
	marker := uniqueMarker("cancel-me")

	task, err := c.SubmitTask(ctx, client.TaskRequest{Data: "sleep 60; echo " + marker})
	if err != nil {
		t.Fatal(err)
	}
	workerRunning(t, marker)

	if task, err = c.CancelTask(ctx, task.ID); err != nil {
		t.Fatal(err)
	}
	if task.Status != "CANCELLED" {
		t.Fatalf("status %s, want CANCELLED", task.Status)
	}

	deadline := time.Now().Add(10 * time.Second)
	for processRunning(t, marker) {
		if time.Now().After(deadline) {
			t.Fatal("the cancelled task's process is still running")
		}
		time.Sleep(300 * time.Millisecond)
	}
	if task, _ = c.GetTask(ctx, task.ID); task.Status != "CANCELLED" {
		t.Fatalf("status changed to %s after the worker reported", task.Status)
	}
}

func TestCancelWorkflowCompensates(t *testing.T) {
	c := newClient(t)
	ctx := ctxTimeout(t, 90*time.Second)

	wf, err := c.StartWorkflow(ctx, "trip_booking_slow", map[string]any{"user_id": 42, "amount": 10})
	if err != nil {
		t.Fatal(err)
	}
	// Wait for the slow hotel step to be running.
	for {
		wf, err = c.GetWorkflow(ctx, wf.ID)
		if err != nil {
			t.Fatal(err)
		}
		if len(wf.Steps) > 1 && wf.Steps[1].Status == "RUNNING" {
			break
		}
		time.Sleep(300 * time.Millisecond)
	}

	if _, err := c.CancelWorkflow(ctx, wf.ID); err != nil {
		t.Fatal(err)
	}
	wf, err = c.WaitForWorkflow(ctx, wf.ID)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"Book Flight": "COMPENSATED", "Book Hotel": "CANCELLED", "Charge Payment": "PENDING"}
	if wf.Status != "CANCELLED" {
		t.Fatalf("workflow %s, want CANCELLED", wf.Status)
	}
	for _, s := range wf.Steps {
		if s.Status != want[s.Name] {
			t.Errorf("step %s is %s, want %s", s.Name, s.Status, want[s.Name])
		}
	}
}

func TestGracefulDrainFinishesRunningTask(t *testing.T) {
	c := newClient(t)
	ctx := ctxTimeout(t, 2*time.Minute)
	marker := uniqueMarker("drain")

	task, err := c.SubmitTask(ctx, client.TaskRequest{Data: "sleep 6; echo " + marker})
	if err != nil {
		t.Fatal(err)
	}
	container := workerRunning(t, marker)
	t.Cleanup(func() { restoreWorkers(t) })

	docker(t, "stop", "-t", "30", container) // SIGTERM, then wait for the drain

	task, err = c.WaitForTask(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if task.Status != "COMPLETED" || task.RetryCount != 0 || !strings.Contains(task.Output, marker) {
		t.Fatalf("task = %s with %d retries and output %q; want COMPLETED on the first attempt",
			task.Status, task.RetryCount, task.Output)
	}
}

func TestWorkerCrashIsRecovered(t *testing.T) {
	c := newClient(t)
	ctx := ctxTimeout(t, 3*time.Minute)
	marker := uniqueMarker("crash")

	task, err := c.SubmitTask(ctx, client.TaskRequest{
		Data:              "sleep 15; echo " + marker,
		RetryDelaySeconds: 1,
		TimeoutSeconds:    60,
	})
	if err != nil {
		t.Fatal(err)
	}
	container := workerRunning(t, marker)
	t.Cleanup(func() { restoreWorkers(t) })

	docker(t, "kill", container) // no chance to drain

	task, err = c.WaitForTask(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if task.Status != "COMPLETED" || task.RetryCount != 1 {
		t.Fatalf("task = %s with %d retries; want COMPLETED after 1 retry", task.Status, task.RetryCount)
	}
}
