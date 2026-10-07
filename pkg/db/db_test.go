package db

import (
	"context"
	"database/sql"
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
	db, err := Open(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if err := db.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	return db
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

	task := must[*Task](t)(db.CreateTask(ctx, "echo hi", TaskOptions{}))
	if task.Status != StatusQueued || task.Priority != 5 || task.MaxRetries != 3 {
		t.Fatalf("unexpected defaults: %+v", task)
	}

	picked := must[*Task](t)(db.PickNextTask(ctx))
	if picked == nil || picked.ID != task.ID || picked.PickedAt == nil {
		t.Fatalf("PickNextTask = %+v, want task %s", picked, task.ID)
	}
	if again := must[*Task](t)(db.PickNextTask(ctx)); again != nil {
		t.Fatal("a picked task was picked twice")
	}

	if !must[bool](t)(db.MarkTaskStarted(ctx, task.ID)) {
		t.Fatal("MarkTaskStarted did not update")
	}
	if !must[bool](t)(db.MarkTaskCompleted(ctx, task.ID, "hi\n")) {
		t.Fatal("MarkTaskCompleted did not update")
	}

	got := must[*Task](t)(db.GetTask(ctx, task.ID))
	if got.Status != StatusCompleted || got.Output != "hi\n" || got.StartedAt == nil || got.CompletedAt == nil {
		t.Fatalf("completed task = %+v", got)
	}

	// Late or duplicate reports must not change a finished task.
	if must[bool](t)(db.MarkTaskFailed(ctx, task.ID, "", "late failure")) {
		t.Fatal("a late failure report changed a completed task")
	}
	if must[bool](t)(db.RetryTask(ctx, task.ID, "late failure")) {
		t.Fatal("a late failure report retried a completed task")
	}
}

func TestPickOrderAndSchedule(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	must[*Task](t)(db.CreateTask(ctx, "future", TaskOptions{ScheduledAt: time.Now().Add(time.Hour)}))
	low := must[*Task](t)(db.CreateTask(ctx, "low", TaskOptions{Priority: 9}))
	high := must[*Task](t)(db.CreateTask(ctx, "high", TaskOptions{Priority: 1}))

	for _, want := range []*Task{high, low} {
		got := must[*Task](t)(db.PickNextTask(ctx))
		if got == nil || got.ID != want.ID {
			t.Fatalf("picked %v, want %q", got, want.Data)
		}
	}
	if got := must[*Task](t)(db.PickNextTask(ctx)); got != nil {
		t.Fatalf("picked %q before its scheduled time", got.Data)
	}
}

func TestConcurrentPickersNeverShareATask(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	const n = 50
	for i := range n {
		must[*Task](t)(db.CreateTask(ctx, fmt.Sprintf("task %d", i), TaskOptions{}))
	}

	var mu sync.Mutex
	seen := make(map[uuid.UUID]int)
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				task, err := db.PickNextTask(ctx)
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
	task := must[*Task](t)(db.CreateTask(ctx, "flaky", TaskOptions{MaxRetries: 3, RetryDelaySeconds: 10}))

	var delays []time.Duration
	for range 3 {
		// Make the task runnable now, then claim and fail it.
		if _, err := db.q.ExecContext(ctx, `UPDATE tasks SET scheduled_at = NOW() WHERE id = $1`, task.ID); err != nil {
			t.Fatal(err)
		}
		must[*Task](t)(db.PickNextTask(ctx))
		before := time.Now()
		if !must[bool](t)(db.RetryTask(ctx, task.ID, "boom")) {
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

	must[*Task](t)(db.PickNextTask(ctx)) // not yet due; nothing picked
	if _, err := db.q.ExecContext(ctx, `UPDATE tasks SET scheduled_at = NOW() WHERE id = $1`, task.ID); err != nil {
		t.Fatal(err)
	}
	must[*Task](t)(db.PickNextTask(ctx))
	if must[bool](t)(db.RetryTask(ctx, task.ID, "boom")) {
		t.Fatal("RetryTask returned true with no retries left")
	}
}

func TestCancelTask(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	queued := must[*Task](t)(db.CreateTask(ctx, "queued", TaskOptions{}))
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
	if must[*Task](t)(db.PickNextTask(ctx)) != nil {
		t.Fatal("a cancelled task was picked")
	}

	running := must[*Task](t)(db.CreateTask(ctx, "running", TaskOptions{}))
	must[*Task](t)(db.PickNextTask(ctx))
	must[bool](t)(db.MarkTaskStarted(ctx, running.ID))
	before = must[*Task](t)(db.CancelTask(ctx, running.ID))
	if before == nil || before.Status != StatusStarted || before.PickedAt == nil {
		t.Fatalf("CancelTask returned %+v, want the started task", before)
	}
	// The killed process's failure report must not resurrect the task.
	if must[bool](t)(db.RetryTask(ctx, running.ID, "killed")) || must[bool](t)(db.MarkTaskFailed(ctx, running.ID, "", "killed")) {
		t.Fatal("a report from the killed process changed the cancelled task")
	}
}

func TestOverdueAndStaleTasks(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	stuck := must[*Task](t)(db.CreateTask(ctx, "stuck", TaskOptions{TimeoutSeconds: 1}))
	must[*Task](t)(db.PickNextTask(ctx))
	must[bool](t)(db.MarkTaskStarted(ctx, stuck.ID))
	if _, err := db.q.ExecContext(ctx, `UPDATE tasks SET started_at = NOW() - INTERVAL '1 minute' WHERE id = $1`, stuck.ID); err != nil {
		t.Fatal(err)
	}
	overdue := must[[]uuid.UUID](t)(db.ListOverdueTasks(ctx, 30*time.Second))
	if len(overdue) != 1 || overdue[0] != stuck.ID {
		t.Fatalf("overdue = %v, want [%s]", overdue, stuck.ID)
	}

	lost := must[*Task](t)(db.CreateTask(ctx, "lost dispatch", TaskOptions{}))
	must[*Task](t)(db.PickNextTask(ctx))
	if _, err := db.q.ExecContext(ctx, `UPDATE tasks SET picked_at = NOW() - INTERVAL '10 minutes' WHERE id = $1`, lost.ID); err != nil {
		t.Fatal(err)
	}
	if n := must[int64](t)(db.ResetStaleTasks(ctx, 5*time.Minute)); n != 1 {
		t.Fatalf("ResetStaleTasks reset %d tasks, want 1", n)
	}
	if got := must[*Task](t)(db.PickNextTask(ctx)); got == nil || got.ID != lost.ID {
		t.Fatal("stale task was not requeued")
	}
}

func TestWithTxRollsBack(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	var id uuid.UUID
	err := db.WithTx(ctx, func(tx *DB) error {
		task, err := tx.CreateTask(ctx, "rolled back", TaskOptions{})
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
	if k == nil || !k.IsAdmin {
		t.Fatalf("LookupAPIKey = %+v", k)
	}
	if !must[bool](t)(db.RevokeAPIKey(ctx, k.ID)) {
		t.Fatal("RevokeAPIKey did not revoke")
	}
	if must[*APIKey](t)(db.LookupAPIKey(ctx, hash)) != nil {
		t.Fatal("a revoked key still authenticates")
	}
}
