//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ChinmayNoob/conductor/pkg/client"
)

// Phase 2: workflow definitions and DAGs, schedules, queues, labels, task
// types, namespaces, idempotency and the dead-letter queue.

func TestDefinitionVersioning(t *testing.T) {
	c := newClient(t)
	name := uniqueName("versioned")
	v1 := fmt.Sprintf("name: %s\nsteps:\n  - name: hello\n    run: echo one\n", name)
	v2 := strings.Replace(v1, "echo one", "echo two", 1)

	if d := applyDefinition(t, c, v1); !d.Created || d.Version != 1 {
		t.Fatalf("first apply: %+v", d)
	}
	if d := applyDefinition(t, c, v1); d.Created || d.Version != 1 {
		t.Fatalf("re-applying the same file: %+v; want unchanged version 1", d)
	}
	if d := applyDefinition(t, c, v2); !d.Created || d.Version != 2 {
		t.Fatalf("changed file: %+v; want version 2", d)
	}

	y, err := c.GetDefinitionYAML(ctxTimeout(t, 10*time.Second), name, 1)
	if err != nil || !strings.Contains(string(y), "echo one") {
		t.Fatalf("version 1 as YAML = %q, %v", y, err)
	}

	_, err = c.ApplyDefinition(ctxTimeout(t, 10*time.Second), []byte("name: bad\nsteps:\n  - {name: a, run: a, depends_on: [a]}\n"))
	if client.StatusCode(err) != http.StatusBadRequest || !strings.Contains(err.Error(), "depends on itself") {
		t.Fatalf("invalid definition: got %v, want a 400 explaining the problem", err)
	}
}

func TestDAGRunsBranchesInParallelAndPassesOutputs(t *testing.T) {
	c := newClient(t)
	wf := startWorkflow(t, c, "order_pipeline", map[string]any{"order_id": "E2E1", "amount": 7})
	wf = waitWorkflow(t, c, wf.ID, 2*time.Minute)
	if wf.Status != "COMPLETED" {
		t.Fatalf("workflow %s: %s", wf.Status, wf.ErrorMessage)
	}

	reserve := waitTask(t, c, wf.Step("reserve_stock").TaskID, time.Minute)
	charge := waitTask(t, c, wf.Step("charge_card").TaskID, time.Minute)
	gap := reserve.StartedAt.Sub(*charge.StartedAt).Abs()
	t.Logf("parallel branches started %v apart", gap)
	if gap > time.Second {
		t.Fatalf("reserve_stock and charge_card started %v apart; they should run in parallel", gap)
	}
	if !charge.StartedAt.Before(*reserve.CompletedAt) {
		t.Fatal("charge_card only started after reserve_stock finished")
	}

	if wf.Step("validate").Outputs["customer"] != "cus_E2E1" {
		t.Fatalf("validate outputs = %v", wf.Step("validate").Outputs)
	}
	ship := waitTask(t, c, wf.Step("ship").TaskID, time.Minute)
	if !strings.Contains(ship.Output, "shipping res_E2E1, paid with ch_E2E1") {
		t.Fatalf("ship output = %q; outputs of both branches should reach it", ship.Output)
	}
}

func TestDAGFailureCompensatesParallelBranches(t *testing.T) {
	c := newClient(t)
	wf := startWorkflow(t, c, "order_pipeline", map[string]any{"order_id": "E2E2", "fail_shipping": true})
	wf = waitWorkflow(t, c, wf.ID, 2*time.Minute)

	if wf.Status != "FAILED" {
		t.Fatalf("workflow %s, want FAILED", wf.Status)
	}
	wantSteps(t, wf, map[string]string{
		"validate": "COMPENSATED", "reserve_stock": "COMPENSATED", "charge_card": "COMPENSATED", "ship": "FAILED",
	})
	refund := waitTask(t, c, wf.Step("charge_card").CompensationTaskID, time.Minute)
	if !strings.Contains(refund.Output, "refunding charge ch_E2E2") {
		t.Fatalf("refund output = %q; compensation should see its step's outputs", refund.Output)
	}
}

func TestWorkflowInputsCannotInjectShell(t *testing.T) {
	c := newClient(t)
	name := uniqueName("injection")
	applyDefinition(t, c, fmt.Sprintf(`
name: %s
inputs:
  user_id: {required: true}
steps:
  - name: greet
    run: echo "user=$INPUT_USER_ID"
`, name))

	evil := "1; touch /tmp/pwned-" + name + " #"
	wf := startWorkflow(t, c, name, map[string]any{"user_id": evil})
	wf = waitWorkflow(t, c, wf.ID, time.Minute)
	if wf.Status != "COMPLETED" {
		t.Fatalf("workflow %s: %s", wf.Status, wf.ErrorMessage)
	}
	task := waitTask(t, c, wf.Step("greet").TaskID, time.Minute)
	if !strings.Contains(task.Output, "user="+evil) {
		t.Fatalf("output %q should contain the input verbatim", task.Output)
	}
	for _, id := range workerContainers(t) {
		if out, _ := exec.Command("docker", "exec", id, "ls", "/tmp").CombinedOutput(); strings.Contains(string(out), "pwned-"+name) {
			t.Fatalf("the injected command ran on worker %s", id)
		}
	}
}

func TestTasksCannotSeeClusterSecrets(t *testing.T) {
	c := newClient(t)
	task := submit(t, c, client.TaskRequest{Command: "env"})
	task = waitTask(t, c, task.ID, time.Minute)
	for _, secret := range []string{"CONDUCTOR_CLUSTER_TOKEN", "POSTGRES_PASSWORD", "CONDUCTOR_API_KEY"} {
		if strings.Contains(task.Output, secret) {
			t.Fatalf("%s is visible to tasks:\n%s", secret, task.Output)
		}
	}
	if !strings.Contains(task.Output, "CONDUCTOR_TASK_ID="+task.ID) {
		t.Fatal("tasks should see their own CONDUCTOR_TASK_ID")
	}
}

func TestHTTPTask(t *testing.T) {
	c := newClient(t)
	task := submit(t, c, client.TaskRequest{HTTP: &client.HTTPSpec{URL: "http://api:8081/health"}, Type: "http"})
	task = waitTask(t, c, task.ID, time.Minute)
	if task.Status != "COMPLETED" || task.Outputs["status"] != "200" || !strings.Contains(task.Outputs["body"], `"ok"`) {
		t.Fatalf("http task = %s, outputs %v, error %q", task.Status, task.Outputs, task.ErrorMessage)
	}

	notFound := submit(t, c, client.TaskRequest{Type: "http", HTTP: &client.HTTPSpec{URL: "http://api:8081/nope"}, MaxRetries: client.Retries(0)})
	if notFound = waitTask(t, c, notFound.ID, time.Minute); notFound.Status != "FAILED" {
		t.Fatalf("a 404 should fail the task, got %s", notFound.Status)
	}
}

func TestContainerTask(t *testing.T) {
	c := newClient(t)
	if !hasContainerWorker(t, c) {
		t.Skip("no container worker; start the stack with --profile containers")
	}
	task := submit(t, c, client.TaskRequest{
		Type:           "container",
		Container:      &client.ContainerSpec{Image: "alpine:3.20", Command: []string{"cat", "/etc/alpine-release"}, Network: "none"},
		Env:            map[string]string{"UNUSED": "x"},
		MaxRetries:     client.Retries(0),
		TimeoutSeconds: 120,
	})
	task = waitTask(t, c, task.ID, 3*time.Minute)
	if task.Status != "COMPLETED" || !strings.HasPrefix(strings.TrimSpace(task.Output), "3.20") {
		t.Fatalf("container task = %s, output %q, error %q", task.Status, task.Output, task.ErrorMessage)
	}

	failing := submit(t, c, client.TaskRequest{
		Type: "container", Container: &client.ContainerSpec{Image: "alpine:3.20", Command: []string{"false"}},
		MaxRetries: client.Retries(0),
	})
	if failing = waitTask(t, c, failing.ID, 3*time.Minute); failing.Status != "FAILED" || !strings.Contains(failing.ErrorMessage, "status 1") {
		t.Fatalf("failing container = %s %q", failing.Status, failing.ErrorMessage)
	}
}

func TestLabelsRouteTasks(t *testing.T) {
	c := newClient(t)

	// Nobody has this label, so the task must wait rather than run anywhere.
	stuck := submit(t, c, client.TaskRequest{Command: "echo never", Labels: map[string]string{"gpu": "h100"}})
	time.Sleep(3 * time.Second)
	if got, _ := c.GetTask(ctxTimeout(t, 10*time.Second), stuck.ID); got.Status != "QUEUED" || got.PickedAt != nil {
		t.Fatalf("task needing a missing label was dispatched: %+v", got)
	}
	if _, err := c.CancelTask(ctxTimeout(t, 10*time.Second), stuck.ID); err != nil {
		t.Fatal(err)
	}

	if !hasContainerWorker(t, c) {
		return
	}
	ws, _ := c.ListWorkers(ctxTimeout(t, 10*time.Second))
	containerWorkers := map[int64]bool{}
	for _, w := range ws {
		if w.Labels["type.container"] == "true" {
			containerWorkers[w.ID] = true
		}
	}
	// Requiring the container label pins even a shell task to that worker.
	for range 3 {
		task := submit(t, c, client.TaskRequest{Command: "echo pinned", Labels: map[string]string{"type.container": "true"}})
		task = waitTask(t, c, task.ID, time.Minute)
		if task.WorkerID == nil || !containerWorkers[*task.WorkerID] {
			t.Fatalf("task ran on worker %v, want one of %v", task.WorkerID, containerWorkers)
		}
	}
}

func TestQueueConcurrencyLimit(t *testing.T) {
	c := newClient(t)
	ctx := ctxTimeout(t, 2*time.Minute)
	queue := uniqueName("serial")
	one := 1
	if _, err := c.PutQueue(ctx, queue, client.QueueSettings{ConcurrencyLimit: &one}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.DeleteQueue(context.Background(), queue) })

	var tasks []*client.Task
	for range 3 {
		tasks = append(tasks, submit(t, c, client.TaskRequest{Command: "sleep 1", Queue: queue}))
	}
	for i, task := range tasks {
		tasks[i] = waitTask(t, c, task.ID, time.Minute)
	}
	// With a limit of 1, no two runs may overlap.
	for i := range tasks {
		for j := i + 1; j < len(tasks); j++ {
			a, b := tasks[i], tasks[j]
			if a.StartedAt.Before(*b.CompletedAt) && b.StartedAt.Before(*a.CompletedAt) {
				t.Fatalf("tasks %d and %d ran at the same time on a queue limited to 1", i, j)
			}
		}
	}

	// Pausing a queue holds its tasks.
	if _, err := c.PutQueue(ctx, queue, client.QueueSettings{Paused: true}); err != nil {
		t.Fatal(err)
	}
	held := submit(t, c, client.TaskRequest{Command: "echo held", Queue: queue})
	time.Sleep(3 * time.Second)
	if got, _ := c.GetTask(ctx, held.ID); got.Status != "QUEUED" {
		t.Fatalf("task on a paused queue is %s", got.Status)
	}
	if _, err := c.PutQueue(ctx, queue, client.QueueSettings{}); err != nil {
		t.Fatal(err)
	}
	if got := waitTask(t, c, held.ID, time.Minute); got.Status != "COMPLETED" {
		t.Fatalf("task did not run after the queue resumed: %s", got.Status)
	}
}

func TestPriorityOrder(t *testing.T) {
	c := newClient(t)
	ctx := ctxTimeout(t, 2*time.Minute)
	queue := uniqueName("prio")
	one := 1
	if _, err := c.PutQueue(ctx, queue, client.QueueSettings{ConcurrencyLimit: &one, Paused: true}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.DeleteQueue(context.Background(), queue) })

	low := submit(t, c, client.TaskRequest{Command: "echo low", Queue: queue, Priority: 9})
	high := submit(t, c, client.TaskRequest{Command: "echo high", Queue: queue, Priority: 1})
	if _, err := c.PutQueue(ctx, queue, client.QueueSettings{ConcurrencyLimit: &one}); err != nil {
		t.Fatal(err)
	}
	low, high = waitTask(t, c, low.ID, time.Minute), waitTask(t, c, high.ID, time.Minute)
	if !high.StartedAt.Before(*low.StartedAt) {
		t.Fatal("the priority-1 task should start before the priority-9 one")
	}
}

func TestIdempotencyKeys(t *testing.T) {
	c := newClient(t)
	key := uniqueName("idem")
	first := submit(t, c, client.TaskRequest{Command: "echo once", IdempotencyKey: key})
	second := submit(t, c, client.TaskRequest{Command: "echo once", IdempotencyKey: key})
	if !first.Created || second.Created || second.ID != first.ID {
		t.Fatalf("first created=%v, second created=%v, same id=%v", first.Created, second.Created, first.ID == second.ID)
	}

	wf1, err := c.StartWorkflow(ctxTimeout(t, 10*time.Second), client.WorkflowRequest{Workflow: "trip_booking", Input: map[string]any{"user_id": 1, "amount": 1}, IdempotencyKey: key})
	if err != nil {
		t.Fatal(err)
	}
	wf2, err := c.StartWorkflow(ctxTimeout(t, 10*time.Second), client.WorkflowRequest{Workflow: "trip_booking", Input: map[string]any{"user_id": 1, "amount": 1}, IdempotencyKey: key})
	if err != nil || wf2.Created || wf2.ID != wf1.ID {
		t.Fatalf("second workflow start: created=%v same=%v err=%v", wf2.Created, wf1.ID == wf2.ID, err)
	}
}

func TestDeadLetterRequeue(t *testing.T) {
	c := newClient(t)
	ctx := ctxTimeout(t, time.Minute)
	marker := uniqueName("dlq")
	task := submit(t, c, client.TaskRequest{Command: "echo " + marker + "; exit 1", MaxRetries: client.Retries(0)})
	task = waitTask(t, c, task.ID, time.Minute)

	dead, err := c.DeadLetter(ctx, 200)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, d := range dead {
		found = found || d.ID == task.ID
	}
	if !found {
		t.Fatal("failed task is not in the dead-letter queue")
	}

	requeued, err := c.RequeueTask(ctx, task.ID)
	if err != nil || requeued.Status != "QUEUED" {
		t.Fatalf("requeue: %+v %v", requeued, err)
	}
	if again := waitTask(t, c, task.ID, time.Minute); again.Status != "FAILED" || again.RetryCount != 0 {
		t.Fatalf("requeued task = %s with %d retries; want it to run again and fail", again.Status, again.RetryCount)
	}
	if _, err := c.RequeueTask(ctx, submit(t, c, client.TaskRequest{Command: "true"}).ID); client.StatusCode(err) != http.StatusConflict {
		t.Fatalf("requeueing a non-failed task: got %v, want 409", err)
	}
}

func TestSchedules(t *testing.T) {
	c := newClient(t)
	ctx := ctxTimeout(t, 2*time.Minute)
	name := uniqueName("tick")

	s, err := c.CreateSchedule(ctx, client.ScheduleRequest{
		Name: name, Cron: "@every 2s", Task: &client.TaskRequest{Command: "echo " + name},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.DeleteSchedule(context.Background(), name) })
	if len(s.Upcoming) != 3 {
		t.Fatalf("upcoming = %v", s.Upcoming)
	}

	countRuns := func() int {
		tasks, err := c.ListTasks(ctx, client.TaskFilter{Limit: 500})
		if err != nil {
			t.Fatal(err)
		}
		n := 0
		for _, task := range tasks {
			if task.Command == "echo "+name {
				n++
			}
		}
		return n
	}

	time.Sleep(7 * time.Second)
	if n := countRuns(); n < 2 || n > 5 {
		t.Fatalf("an @every 2s schedule fired %d times in 7s", n)
	}

	if _, err := c.ScheduleAction(ctx, name, "pause"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)
	paused := countRuns()
	time.Sleep(4 * time.Second)
	if n := countRuns(); n != paused {
		t.Fatalf("a paused schedule fired (%d -> %d runs)", paused, n)
	}

	// A workflow schedule, fired on demand.
	wfName := uniqueName("nightly")
	if _, err := c.CreateSchedule(ctx, client.ScheduleRequest{
		Name: wfName, Cron: "0 3 * * *", Timezone: "Asia/Kolkata",
		Workflow: &client.ScheduledWorkflow{Name: "trip_booking", Input: map[string]any{"user_id": 9, "amount": 1}},
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.DeleteSchedule(context.Background(), wfName) })
	if _, err := c.ScheduleAction(ctx, wfName, "trigger"); err != nil {
		t.Fatal(err)
	}
	var sched *client.Schedule
	for sched == nil || sched.LastRunID == "" {
		time.Sleep(500 * time.Millisecond)
		if sched, err = c.GetSchedule(ctx, wfName); err != nil {
			t.Fatal(err)
		}
	}
	if wf := waitWorkflow(t, c, sched.LastRunID, time.Minute); wf.Status != "COMPLETED" {
		t.Fatalf("scheduled workflow %s: %s", wf.Status, wf.ErrorMessage)
	}

	if _, err := c.CreateSchedule(ctx, client.ScheduleRequest{Name: uniqueName("bad"), Cron: "not cron", Task: &client.TaskRequest{Command: "x"}}); client.StatusCode(err) != http.StatusBadRequest {
		t.Fatalf("invalid cron: got %v, want 400", err)
	}
}

func TestNamespacesIsolateAndLimit(t *testing.T) {
	c := newClient(t)
	ctx := ctxTimeout(t, time.Minute)
	ns := uniqueName("team")
	two := 2
	if _, err := c.CreateNamespace(ctx, client.Namespace{Name: ns, MaxPendingTasks: &two}); err != nil {
		t.Fatal(err)
	}
	key, err := c.CreateAPIKey(ctx, ns+"-bot", ns, false)
	if err != nil {
		t.Fatal(err)
	}
	team := client.New(apiURL, key.Key)

	var mine []string
	for range 2 {
		task, err := team.SubmitTask(ctx, client.TaskRequest{Command: "echo later", DelaySeconds: 600})
		if err != nil {
			t.Fatal(err)
		}
		mine = append(mine, task.ID)
	}
	if _, err := team.SubmitTask(ctx, client.TaskRequest{Command: "echo third", DelaySeconds: 600}); client.StatusCode(err) != http.StatusTooManyRequests {
		t.Fatalf("third pending task: got %v, want HTTP 429", err)
	}

	if _, err := c.GetTask(ctx, mine[0]); !client.IsNotFound(err) {
		t.Fatalf("the default namespace can see another namespace's task: %v", err)
	}
	if _, err := c.WithNamespace(ns).GetTask(ctx, mine[0]); err != nil {
		t.Fatalf("an admin key should reach the task through the namespace header: %v", err)
	}
	if _, err := team.WithNamespace("default").ListTasks(ctx, client.TaskFilter{}); client.StatusCode(err) != http.StatusForbidden {
		t.Fatalf("a namespaced key switched namespaces: %v", err)
	}
	for _, id := range mine {
		if _, err := team.CancelTask(ctx, id); err != nil {
			t.Fatal(err)
		}
	}
}

func TestDevMode(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs a second Conductor")
	}
	root := repoRoot(t)
	bin := filepath.Join(t.TempDir(), "conductor")
	if runtime.GOOS == "windows" {
		bin += ".exe"
	}
	if out, err := exec.Command("go", "build", "-o", bin, filepath.Join(root, "cmd", "conductor")).CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}

	// Its own database, so it doesn't compete with the compose cluster.
	db := strings.ReplaceAll(uniqueName("dev_e2e"), "-", "_")
	compose(t, "exec", "-T", "postgres", "psql", "-U", "postgres", "-c", "CREATE DATABASE "+db)
	t.Cleanup(func() {
		compose(t, "exec", "-T", "postgres", "psql", "-U", "postgres", "-c", "DROP DATABASE IF EXISTS "+db+" WITH (FORCE)")
	})

	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, bin, "dev")
	cmd.Env = append(os.Environ(),
		"POSTGRES_HOST=localhost", "POSTGRES_PORT=5433", "POSTGRES_DB="+db,
		"CONDUCTOR_COORDINATOR_LISTEN=127.0.0.1:18080",
		"CONDUCTOR_API_LISTEN=127.0.0.1:18081",
		"CONDUCTOR_WORKER_LISTEN=127.0.0.1:19000",
		"CONDUCTOR_API_KEY=dev-e2e-key",
	)
	var logs strings.Builder
	var mu sync.Mutex
	cmd.Stdout, cmd.Stderr = lockedWriter{&mu, &logs}, lockedWriter{&mu, &logs}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancel()
		_ = cmd.Wait()
		if t.Failed() {
			mu.Lock()
			t.Logf("dev mode logs:\n%s", logs.String())
			mu.Unlock()
		}
	})

	dev := client.New("http://127.0.0.1:18081", "dev-e2e-key")
	deadline := time.Now().Add(60 * time.Second)
	for dev.Health(context.Background()) != nil {
		if time.Now().After(deadline) {
			t.Fatal("dev mode API never became healthy")
		}
		time.Sleep(500 * time.Millisecond)
	}
	task, err := dev.SubmitTask(context.Background(), client.TaskRequest{Command: "echo dev-ok"})
	if err != nil {
		t.Fatal(err)
	}
	task, err = dev.WaitForTask(ctxTimeout(t, 30*time.Second), task.ID)
	if err != nil || task.Status != "COMPLETED" || !strings.Contains(task.Output, "dev-ok") {
		t.Fatalf("dev mode task = %+v, %v", task, err)
	}
	wf, err := dev.StartWorkflow(context.Background(), client.WorkflowRequest{Workflow: "trip_booking", Input: map[string]any{"user_id": 1, "amount": 2}})
	if err != nil {
		t.Fatal(err)
	}
	if wf, err = dev.WaitForWorkflow(ctxTimeout(t, time.Minute), wf.ID); err != nil || wf.Status != "COMPLETED" {
		t.Fatalf("dev mode workflow = %+v, %v", wf, err)
	}
}

type lockedWriter struct {
	mu *sync.Mutex
	b  *strings.Builder
}

func (w lockedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.Write(p)
}
