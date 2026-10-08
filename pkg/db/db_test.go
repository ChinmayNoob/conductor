package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

// testDB returns a migrated, empty database that is dropped after the test.
// Tests are skipped unless CONDUCTOR_TEST_DATABASE_URL points at a Postgres
// server where the user may create databases, e.g.
//
//	CONDUCTOR_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5433/postgres?sslmode=disable
func testDB(t *testing.T) *DB {
	t.Helper()
	db, err := Open(context.Background(), testDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if err := db.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	return db
}

// testDSN creates an empty database that is dropped after the test, and
// returns its connection string.
func testDSN(t *testing.T) string {
	t.Helper()
	base := os.Getenv("CONDUCTOR_TEST_DATABASE_URL")
	if base == "" {
		t.Skip("CONDUCTOR_TEST_DATABASE_URL not set")
	}
	ctx := context.Background()

	admin, err := sql.Open("postgres", base)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { admin.Close() })

	name := fmt.Sprintf("conductor_test_%d", rand.Uint32())
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		admin.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
	})

	u, err := url.Parse(base)
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/" + name
	return u.String()
}

func must[T any](t *testing.T) func(T, error) T {
	return func(v T, err error) T {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
}

// shellWorkers is the label set of a plain worker.
var shellWorkers = PickOptions{WorkerLabels: []map[string]string{{"type.shell": "true"}}}

// create inserts a shell task, letting the caller adjust it first.
func create(t *testing.T, db *DB, data string, adjust ...func(*NewTask)) *Task {
	t.Helper()
	n := DefaultTask(data)
	n.Requirements = StringMap{"type.shell": "true"}
	for _, f := range adjust {
		f(&n)
	}
	task, _, err := db.CreateTask(context.Background(), n)
	if err != nil {
		t.Fatal(err)
	}
	return task
}

func pick(t *testing.T, db *DB, opts ...PickOptions) *Task {
	t.Helper()
	o := shellWorkers
	if len(opts) > 0 {
		o = opts[0]
	}
	task, err := db.PickNextTask(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	return task
}

// att returns a task's current attempt, which status reports must quote.
func att(t *testing.T, db *DB, id uuid.UUID) int {
	t.Helper()
	task, err := db.GetTask(context.Background(), id)
	if err != nil || task == nil {
		t.Fatalf("GetTask(%s): %v", id, err)
	}
	return task.Attempt
}

func exec(t *testing.T, db *DB, query string, args ...any) {
	t.Helper()
	if _, err := db.q.ExecContext(context.Background(), query, args...); err != nil {
		t.Fatal(err)
	}
}

func TestMigrateIsIdempotent(t *testing.T) {
	db := testDB(t)
	// Concurrent components all migrate on startup.
	var wg sync.WaitGroup
	errs := make(chan error, 3)
	for range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- db.Migrate(context.Background())
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("second migration run failed: %v", err)
		}
	}
}

func TestTaskLifecycle(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	task := create(t, db, "echo hi")
	if task.Status != StatusQueued || task.Priority != 5 || task.MaxRetries != 3 || task.Namespace != "default" {
		t.Fatalf("unexpected defaults: %+v", task)
	}

	picked := pick(t, db)
	if picked == nil || picked.ID != task.ID || picked.PickedAt == nil {
		t.Fatalf("PickNextTask = %+v, want task %s", picked, task.ID)
	}
	if pick(t, db) != nil {
		t.Fatal("a picked task was picked twice")
	}

	if !must[bool](t)(db.MarkTaskStarted(ctx, task.ID, att(t, db, task.ID), 42)) {
		t.Fatal("MarkTaskStarted did not update")
	}
	if r := must[TaskResult](t)(db.MarkTaskCompleted(ctx, task.ID, att(t, db, task.ID), 0, "hi\n", StringMap{"k": "v"})); !r.Updated {
		t.Fatal("MarkTaskCompleted did not update")
	}

	got := must[*Task](t)(db.GetTask(ctx, task.ID))
	if got.Status != StatusCompleted || got.Output != "hi\n" || got.Outputs["k"] != "v" ||
		got.StartedAt == nil || got.CompletedAt == nil || got.WorkerID == nil || *got.WorkerID != 42 {
		t.Fatalf("completed task = %+v", got)
	}

	// Late or duplicate reports must not change a finished task.
	if must[TaskResult](t)(db.MarkTaskFailed(ctx, task.ID, att(t, db, task.ID), 0, "", "late failure")).Updated {
		t.Fatal("a late failure report changed a completed task")
	}
	if must[bool](t)(db.RetryTask(ctx, task.ID, att(t, db, task.ID), "", "late failure")) {
		t.Fatal("a late failure report retried a completed task")
	}
}

func TestPickOrderAndSchedule(t *testing.T) {
	db := testDB(t)

	create(t, db, "future", func(n *NewTask) { n.ScheduledAt = time.Now().Add(time.Hour) })
	low := create(t, db, "low", func(n *NewTask) { n.Priority = 9 })
	high := create(t, db, "high", func(n *NewTask) { n.Priority = 1 })

	for _, want := range []*Task{high, low} {
		if got := pick(t, db); got == nil || got.ID != want.ID {
			t.Fatalf("picked %v, want %q", got, want.Data)
		}
	}
	if got := pick(t, db); got != nil {
		t.Fatalf("picked %q before its scheduled time", got.Data)
	}
}

func TestPriorityAging(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	if err := db.SetPriorityAging(ctx, time.Minute); err != nil {
		t.Fatal(err)
	}
	// A priority-9 task that has waited 10 minutes beats a fresh priority-5
	// one when aging is one level per minute.
	old := create(t, db, "old", func(n *NewTask) { n.Priority = 9; n.ScheduledAt = time.Now().Add(-10 * time.Minute) })
	create(t, db, "fresh", func(n *NewTask) { n.Priority = 5 })
	if got := pick(t, db); got == nil || got.ID != old.ID {
		t.Fatalf("picked %v, want the aged task", got)
	}

	// With aging off, priority is strict however long a task waits.
	if err := db.SetPriorityAging(ctx, 0); err != nil {
		t.Fatal(err)
	}
	ancient := create(t, db, "ancient", func(n *NewTask) { n.Priority = 9; n.ScheduledAt = time.Now().Add(-24 * time.Hour) })
	if got := pick(t, db); got == nil || got.Data != "fresh" {
		t.Fatalf("picked %v, want the priority-5 task", got)
	}
	if got := pick(t, db); got == nil || got.ID != ancient.ID {
		t.Fatalf("picked %v, want the ancient task last", got)
	}
}

func TestBatchPickRespectsLimits(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	two := 2
	must[*Queue](t)(db.UpsertQueue(ctx, Queue{Namespace: "default", Name: "limited", ConcurrencyLimit: &two}))
	for range 5 {
		create(t, db, "limited", func(n *NewTask) { n.Queue = "limited" })
	}
	for range 5 {
		create(t, db, "free")
	}

	batch := must[[]*Task](t)(db.PickTasks(ctx, shellWorkers, 10))
	perQueue := map[string]int{}
	for _, task := range batch {
		perQueue[task.Queue]++
	}
	if perQueue["limited"] != 2 || perQueue["default"] != 5 {
		t.Fatalf("batch per queue = %v, want limited=2 (its limit) and default=5", perQueue)
	}
	if more := must[[]*Task](t)(db.PickTasks(ctx, shellWorkers, 10)); len(more) != 0 {
		t.Fatalf("picked %d more tasks with the limited queue full and the rest drained", len(more))
	}
}

func TestPickMatchesWorkerLabels(t *testing.T) {
	db := testDB(t)
	gpu := create(t, db, "train", func(n *NewTask) { n.Requirements = StringMap{"type.shell": "true", "gpu": "true"} })
	plain := create(t, db, "plain")

	if got := pick(t, db); got == nil || got.ID != plain.ID {
		t.Fatalf("a plain worker got %v, want the plain task", got)
	}
	if got := pick(t, db); got != nil {
		t.Fatalf("a plain worker got the GPU task")
	}
	withGPU := PickOptions{WorkerLabels: []map[string]string{{"type.shell": "true"}, {"type.shell": "true", "gpu": "true"}}}
	if got := pick(t, db, withGPU); got == nil || got.ID != gpu.ID {
		t.Fatalf("a GPU worker got %v, want the GPU task", got)
	}
}

func TestQueueLimits(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	onQueue := func(q string) func(*NewTask) { return func(n *NewTask) { n.Queue = q } }

	for range 3 {
		create(t, db, "limited", onQueue("limited"))
	}
	two := 2
	must[*Queue](t)(db.UpsertQueue(ctx, Queue{Namespace: "default", Name: "limited", ConcurrencyLimit: &two}))

	for range 2 {
		if pick(t, db) == nil {
			t.Fatal("the first two tasks should be picked")
		}
	}
	if got := pick(t, db); got != nil {
		t.Fatal("a third task was picked past the concurrency limit of 2")
	}

	// Pausing stops a queue entirely.
	create(t, db, "paused", onQueue("paused"))
	must[*Queue](t)(db.UpsertQueue(ctx, Queue{Namespace: "default", Name: "paused", Paused: true}))
	if got := pick(t, db); got != nil {
		t.Fatalf("picked %q from a paused queue", got.Data)
	}

	// Rate limit: one dispatch per hour.
	one := 1
	must[*Queue](t)(db.UpsertQueue(ctx, Queue{Namespace: "default", Name: "rated", RateLimit: &one, RatePeriodSeconds: 3600}))
	for range 2 {
		create(t, db, "rated", onQueue("rated"))
	}
	if pick(t, db) == nil {
		t.Fatal("the first rate-limited task should be picked")
	}
	if got := pick(t, db); got != nil {
		t.Fatal("a second task was picked within the rate limit window")
	}

	queues := must[[]*Queue](t)(db.ListQueues(ctx, "default"))
	counts := map[string][2]int{}
	for _, q := range queues {
		counts[q.Name] = [2]int{q.Queued, q.Running}
	}
	if counts["limited"] != [2]int{1, 2} || counts["rated"] != [2]int{1, 1} {
		t.Fatalf("queue counts = %v", counts)
	}
}

func TestNamespaceConcurrency(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	one := 1
	must[*Namespace](t)(db.CreateNamespace(ctx, Namespace{Name: "team-a", MaxConcurrency: &one}))
	for range 2 {
		create(t, db, "a", func(n *NewTask) { n.Namespace = "team-a" })
	}

	if pick(t, db) == nil {
		t.Fatal("first task in team-a should be picked")
	}
	if pick(t, db) != nil {
		t.Fatal("team-a ran two tasks with max_concurrency 1")
	}
	if _, err := db.CreateNamespace(ctx, Namespace{Name: "team-a"}); !errors.Is(err, ErrExists) {
		t.Fatalf("duplicate namespace: got %v, want ErrExists", err)
	}
}

func TestIdempotencyKeys(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	n := DefaultTask("charge")
	n.IdempotencyKey = "order-42"

	first, created, err := db.CreateTask(ctx, n)
	if err != nil || !created {
		t.Fatalf("first create: created=%v err=%v", created, err)
	}
	second, created, err := db.CreateTask(ctx, n)
	if err != nil || created || second.ID != first.ID {
		t.Fatalf("second create: created=%v id=%v err=%v; want the first task back", created, second.ID, err)
	}

	// Keys are scoped to a namespace.
	must[*Namespace](t)(db.CreateNamespace(ctx, Namespace{Name: "other"}))
	n.Namespace = "other"
	if _, created, _ := db.CreateTask(ctx, n); !created {
		t.Fatal("the same key in another namespace should create a new task")
	}
}

func TestConcurrentPickersNeverShareATask(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	const n = 50
	for i := range n {
		create(t, db, fmt.Sprintf("task %d", i))
	}

	var mu sync.Mutex
	seen := make(map[uuid.UUID]int)
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				task, err := db.PickNextTask(ctx, shellWorkers)
				if err != nil {
					t.Error(err)
					return
				}
				if task == nil {
					return
				}
				mu.Lock()
				seen[task.ID]++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if len(seen) != n {
		t.Fatalf("picked %d distinct tasks, want %d", len(seen), n)
	}
	for id, count := range seen {
		if count != 1 {
			t.Fatalf("task %s picked %d times", id, count)
		}
	}
}

func TestRetryBackoffIsExponential(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	task := create(t, db, "flaky", func(n *NewTask) { n.MaxRetries = 3; n.RetryDelaySeconds = 10 })

	var delays []time.Duration
	for range 3 {
		exec(t, db, `UPDATE tasks SET scheduled_at = NOW() WHERE id = $1`, task.ID)
		pick(t, db)
		before := time.Now()
		if !must[bool](t)(db.RetryTask(ctx, task.ID, att(t, db, task.ID), "", "boom")) {
			t.Fatal("RetryTask returned false with retries left")
		}
		got := must[*Task](t)(db.GetTask(ctx, task.ID))
		delays = append(delays, got.ScheduledAt.Sub(before).Round(time.Second))
	}
	want := []time.Duration{10 * time.Second, 20 * time.Second, 40 * time.Second}
	for i := range want {
		if delays[i] != want[i] {
			t.Fatalf("retry delays = %v, want %v", delays, want)
		}
	}

	exec(t, db, `UPDATE tasks SET scheduled_at = NOW() WHERE id = $1`, task.ID)
	pick(t, db)
	if must[bool](t)(db.RetryTask(ctx, task.ID, att(t, db, task.ID), "", "boom")) {
		t.Fatal("RetryTask returned true with no retries left")
	}
}

func TestCancelTask(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	queued := create(t, db, "queued")
	before := must[*Task](t)(db.CancelTask(ctx, queued.ID))
	if before == nil || before.Status != StatusQueued || before.PickedAt != nil {
		t.Fatalf("CancelTask returned %+v, want the queued, undispatched task", before)
	}
	if got := must[*Task](t)(db.GetTask(ctx, queued.ID)); got.Status != StatusCancelled || got.CancelledAt == nil {
		t.Fatalf("task after cancel = %+v", got)
	}
	if again := must[*Task](t)(db.CancelTask(ctx, queued.ID)); again != nil {
		t.Fatal("cancelling twice succeeded")
	}
	if pick(t, db) != nil {
		t.Fatal("a cancelled task was picked")
	}

	running := create(t, db, "running")
	pick(t, db)
	must[bool](t)(db.MarkTaskStarted(ctx, running.ID, att(t, db, running.ID), 1))
	before = must[*Task](t)(db.CancelTask(ctx, running.ID))
	if before == nil || before.Status != StatusStarted || before.PickedAt == nil {
		t.Fatalf("CancelTask returned %+v, want the started task", before)
	}
	// The killed process's failure report must not resurrect the task.
	if must[bool](t)(db.RetryTask(ctx, running.ID, att(t, db, running.ID), "", "killed")) || must[TaskResult](t)(db.MarkTaskFailed(ctx, running.ID, att(t, db, running.ID), 0, "", "killed")).Updated {
		t.Fatal("a report from the killed process changed the cancelled task")
	}
}

func TestDeadLetterAndRequeue(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	task := create(t, db, "doomed", func(n *NewTask) { n.MaxRetries = 0 })
	pick(t, db)
	must[TaskResult](t)(db.MarkTaskFailed(ctx, task.ID, att(t, db, task.ID), 0, "", "boom"))

	dead := must[[]*Task](t)(db.ListTasks(ctx, TaskFilter{Namespace: "default", DeadLetter: true, Limit: 10}))
	if len(dead) != 1 || dead[0].ID != task.ID {
		t.Fatalf("dead letter = %v", dead)
	}
	requeued := must[*Task](t)(db.RequeueFailedTask(ctx, task.ID))
	if requeued == nil || requeued.Status != StatusQueued || requeued.RetryCount != 0 {
		t.Fatalf("requeued = %+v", requeued)
	}
	if got := pick(t, db); got == nil || got.ID != task.ID {
		t.Fatal("requeued task was not picked")
	}
}

func TestOverdueAndStaleTasks(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	stuck := create(t, db, "stuck", func(n *NewTask) { n.TimeoutSeconds = 1 })
	pick(t, db)
	must[bool](t)(db.MarkTaskStarted(ctx, stuck.ID, att(t, db, stuck.ID), 1))
	exec(t, db, `UPDATE tasks SET started_at = NOW() - INTERVAL '1 minute' WHERE id = $1`, stuck.ID)
	overdue := must[[]TaskAttempt](t)(db.ListOverdueTasks(ctx, 30*time.Second))
	if len(overdue) != 1 || overdue[0].ID != stuck.ID || overdue[0].Attempt != 1 {
		t.Fatalf("overdue = %v, want [%s]", overdue, stuck.ID)
	}

	lost := create(t, db, "lost dispatch")
	pick(t, db)
	exec(t, db, `UPDATE tasks SET picked_at = NOW() - INTERVAL '10 minutes' WHERE id = $1`, lost.ID)
	if n := must[int64](t)(db.ResetStaleTasks(ctx, 5*time.Minute)); n != 1 {
		t.Fatalf("ResetStaleTasks reset %d tasks, want 1", n)
	}
	if got := pick(t, db); got == nil || got.ID != lost.ID {
		t.Fatal("stale task was not requeued")
	}
}

func TestWithTxRollsBack(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	var id uuid.UUID
	err := db.WithTx(ctx, func(tx *DB) error {
		task, _, err := tx.CreateTask(ctx, DefaultTask("rolled back"))
		if err != nil {
			return err
		}
		id = task.ID
		return fmt.Errorf("abort")
	})
	if err == nil {
		t.Fatal("WithTx swallowed the error")
	}
	if got := must[*Task](t)(db.GetTask(ctx, id)); got != nil {
		t.Fatal("task from a rolled-back transaction exists")
	}
}

func TestDefinitionVersions(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	v1 := json.RawMessage(`{"name": "x", "steps": [{"name": "a", "run": "a"}]}`)
	v2 := json.RawMessage(`{"name": "x", "steps": [{"name": "a", "run": "b"}]}`)

	d, created, err := db.SaveDefinition(ctx, "default", "x", v1)
	if err != nil || !created || d.Version != 1 {
		t.Fatalf("first save: %+v created=%v err=%v", d, created, err)
	}
	if d, created, _ = db.SaveDefinition(ctx, "default", "x", v1); created || d.Version != 1 {
		t.Fatalf("re-saving the same spec made version %d (created=%v)", d.Version, created)
	}
	if d, created, _ = db.SaveDefinition(ctx, "default", "x", v2); !created || d.Version != 2 {
		t.Fatalf("changed spec: version %d created=%v, want 2", d.Version, created)
	}
	if got := must[*Definition](t)(db.GetDefinition(ctx, "default", "x", 1)); got == nil || got.Version != 1 {
		t.Fatal("version 1 lost")
	}
	if got := must[*Definition](t)(db.GetDefinition(ctx, "default", "x", 0)); got.Version != 2 {
		t.Fatalf("latest = %d, want 2", got.Version)
	}
}

func TestWorkflowRunAndStepStates(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	wf, created, err := db.CreateWorkflow(ctx, NewWorkflow{
		Namespace: "default", Name: "x", DefinitionVersion: 1, Definition: json.RawMessage(`{}`),
		Input: json.RawMessage(`{"a":"1"}`), Steps: []string{"one", "two"}, IdempotencyKey: "run-1",
	})
	if err != nil || !created {
		t.Fatalf("create: %v %v", created, err)
	}
	again, created, _ := db.CreateWorkflow(ctx, NewWorkflow{Namespace: "default", Name: "x", Definition: json.RawMessage(`{}`), IdempotencyKey: "run-1"})
	if created || again.ID != wf.ID {
		t.Fatal("idempotency key did not return the existing run")
	}

	states := must[[]*StepState](t)(db.GetStepStates(ctx, wf.ID))
	if len(states) != 2 || states[0].Name != "one" || states[0].Status != "PENDING" || states[0].TaskStatus != "" {
		t.Fatalf("initial states = %+v", states)
	}

	task := create(t, db, "step one", func(n *NewTask) { n.WorkflowID = &wf.ID })
	if err := db.StartStep(ctx, states[0].ID, task.ID); err != nil {
		t.Fatal(err)
	}
	pick(t, db)
	r := must[TaskResult](t)(db.MarkTaskCompleted(ctx, task.ID, att(t, db, task.ID), 0, "", StringMap{"out": "x"}))
	if r.WorkflowID == nil || *r.WorkflowID != wf.ID {
		t.Fatal("MarkTaskCompleted did not report the task's workflow")
	}
	states = must[[]*StepState](t)(db.GetStepStates(ctx, wf.ID))
	if states[0].Status != "RUNNING" || states[0].TaskStatus != "COMPLETED" || states[0].Outputs["out"] != "x" {
		t.Fatalf("step one = %+v", states[0])
	}
}

func TestSchedules(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	target := json.RawMessage(`{"task": {"command": "echo"}}`)

	must[*Schedule](t)(db.CreateSchedule(ctx, Schedule{Namespace: "default", Name: "due", Cron: "@every 1m",
		Timezone: "UTC", MisfirePolicy: "skip", Target: target, Enabled: true, NextRunAt: time.Now().Add(-time.Second)}))
	must[*Schedule](t)(db.CreateSchedule(ctx, Schedule{Namespace: "default", Name: "later", Cron: "@every 1m",
		Timezone: "UTC", MisfirePolicy: "skip", Target: target, Enabled: true, NextRunAt: time.Now().Add(time.Hour)}))
	if _, err := db.CreateSchedule(ctx, Schedule{Namespace: "default", Name: "due", Cron: "x", MisfirePolicy: "skip", Target: target, NextRunAt: time.Now()}); !errors.Is(err, ErrExists) {
		t.Fatalf("duplicate schedule: got %v, want ErrExists", err)
	}

	err := db.WithTx(ctx, func(tx *DB) error {
		due, err := tx.ClaimDueSchedules(ctx, 10)
		if err != nil {
			return err
		}
		if len(due) != 1 || due[0].Name != "due" {
			return fmt.Errorf("due = %v", due)
		}
		// A second coordinator must not claim it while we hold it.
		other, err := db.ClaimDueSchedules(ctx, 10)
		if err != nil {
			return err
		}
		if len(other) != 0 {
			return fmt.Errorf("a locked schedule was claimed twice")
		}
		id := uuid.New()
		return tx.RecordScheduleRun(ctx, due[0].ID, time.Now().Add(time.Minute), true, &id, "")
	})
	if err != nil {
		t.Fatal(err)
	}
	s := must[*Schedule](t)(db.GetSchedule(ctx, "default", "due"))
	if s.LastRunAt == nil || s.LastRunID == nil || !s.NextRunAt.After(time.Now()) {
		t.Fatalf("schedule after run = %+v", s)
	}
}

func TestAPIKeys(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	hash := []byte("0123456789abcdef0123456789abcdef")

	if err := db.EnsureAPIKey(ctx, "bootstrap", hash, "cnd_0123", true); err != nil {
		t.Fatal(err)
	}
	if err := db.EnsureAPIKey(ctx, "bootstrap", hash, "cnd_0123", true); err != nil {
		t.Fatalf("EnsureAPIKey is not idempotent: %v", err)
	}
	if keys := must[[]*APIKey](t)(db.ListAPIKeys(ctx)); len(keys) != 1 {
		t.Fatalf("got %d keys, want 1", len(keys))
	}

	k := must[*APIKey](t)(db.LookupAPIKey(ctx, hash))
	if k == nil || !k.IsAdmin || k.Namespace != "default" {
		t.Fatalf("LookupAPIKey = %+v", k)
	}
	if !must[bool](t)(db.RevokeAPIKey(ctx, k.ID)) {
		t.Fatal("RevokeAPIKey did not revoke")
	}
	if must[*APIKey](t)(db.LookupAPIKey(ctx, hash)) != nil {
		t.Fatal("a revoked key still authenticates")
	}
}

func TestQueueDepths(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	must[*Queue](t)(db.UpsertQueue(ctx, Queue{Namespace: "default", Name: "held", Paused: true}))
	create(t, db, "ready")
	create(t, db, "ready too")
	create(t, db, "later", func(n *NewTask) { n.ScheduledAt = time.Now().Add(time.Hour) })
	create(t, db, "held", func(n *NewTask) { n.Queue = "held" })
	pick(t, db) // one ready task starts running

	got := map[string]QueueDepth{}
	for _, d := range must[[]QueueDepth](t)(db.QueueDepths(ctx)) {
		got[d.Queue] = d
	}
	def, held := got["default"], got["held"]
	if def.Ready != 1 || def.Delayed != 1 || def.Running != 1 || def.Paused != 0 || def.OldestReady == nil {
		t.Errorf("default queue = %+v, want 1 ready (with an age), 1 delayed, 1 running", def)
	}
	if held.Paused != 1 || held.Ready != 0 || held.OldestReady != nil {
		t.Errorf("paused queue = %+v, want its task counted as paused, not ready", held)
	}
}

func TestAttemptHistory(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	task := create(t, db, "flaky", func(n *NewTask) { n.MaxRetries = 1; n.RetryDelaySeconds = 0 })

	// Attempt 1 fails and is retried: it moves to the history.
	pick(t, db)
	must[bool](t)(db.MarkTaskStarted(ctx, task.ID, 1, 7))
	if !must[bool](t)(db.RetryTask(ctx, task.ID, 1, "out 1", "boom 1")) {
		t.Fatal("not retried")
	}
	// Attempt 2 fails for good: it stays on the task row.
	exec(t, db, `UPDATE tasks SET scheduled_at = NOW() - INTERVAL '1 second' WHERE id = $1`, task.ID)
	pick(t, db)
	if must[bool](t)(db.RetryTask(ctx, task.ID, 2, "out 2", "boom 2")) {
		t.Fatal("retried past max_retries")
	}
	must[TaskResult](t)(db.MarkTaskFailed(ctx, task.ID, 2, 8, "out 2", "boom 2"))

	got := must[[]AttemptRecord](t)(db.ListAttempts(ctx, task.ID))
	if len(got) != 1 || got[0].Attempt != 1 || got[0].ErrorMessage != "boom 1" || got[0].Output != "out 1" ||
		got[0].WorkerID == nil || *got[0].WorkerID != 7 || got[0].StartedAt == nil {
		t.Fatalf("history after retry = %+v", got)
	}

	// Requeueing the failed task keeps attempt 2 before resetting the row.
	must[*Task](t)(db.RequeueFailedTask(ctx, task.ID))
	got = must[[]AttemptRecord](t)(db.ListAttempts(ctx, task.ID))
	if len(got) != 2 || got[1].Attempt != 2 || got[1].ErrorMessage != "boom 2" {
		t.Fatalf("history after requeue = %+v", got)
	}
}
