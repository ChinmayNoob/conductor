package db

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
)

func TestAttemptFencing(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	task := create(t, db, "flaky dispatch")
	first := pick(t, db)
	if first.Attempt != 1 {
		t.Fatalf("first dispatch has attempt %d, want 1", first.Attempt)
	}

	// The dispatch is lost (e.g. the worker was partitioned away) and the
	// task is requeued and dispatched again.
	exec(t, db, `UPDATE tasks SET picked_at = NOW() - INTERVAL '10 minutes' WHERE id = $1`, task.ID)
	must[int64](t)(db.ResetStaleTasks(ctx, 5*time.Minute))
	second := pick(t, db)
	if second.Attempt != 2 {
		t.Fatalf("second dispatch has attempt %d, want 2", second.Attempt)
	}

	// The first worker comes back and reports. Nothing it says may count.
	if must[bool](t)(db.MarkTaskStarted(ctx, task.ID, 1, 1)) {
		t.Fatal("a STARTED report from the old attempt was accepted")
	}
	if must[TaskResult](t)(db.MarkTaskCompleted(ctx, task.ID, 1, 0, "stale", nil)).Updated {
		t.Fatal("a COMPLETE report from the old attempt was accepted")
	}
	if must[bool](t)(db.RetryTask(ctx, task.ID, 1, "", "stale failure")) {
		t.Fatal("a FAILED report from the old attempt caused a retry")
	}

	// The current attempt's reports do count.
	if !must[bool](t)(db.MarkTaskStarted(ctx, task.ID, 2, 2)) {
		t.Fatal("the current attempt's STARTED report was rejected")
	}
	if !must[TaskResult](t)(db.MarkTaskCompleted(ctx, task.ID, 2, 0, "ok", nil)).Updated {
		t.Fatal("the current attempt's COMPLETE report was rejected")
	}
	if got := must[*Task](t)(db.GetTask(ctx, task.ID)); got.Output != "ok" {
		t.Fatalf("output = %q, want the current attempt's", got.Output)
	}
}

func TestLeaderElectionIsExclusive(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	a := must[*LeaderSession](t)(db.NewLeaderSession(ctx))
	b := must[*LeaderSession](t)(db.NewLeaderSession(ctx))
	defer b.Release()

	if !must[bool](t)(a.TryAcquire(ctx)) {
		t.Fatal("the first campaigner did not get the lock")
	}
	if must[bool](t)(b.TryAcquire(ctx)) {
		t.Fatal("two coordinators hold the leader lock at once")
	}

	// When the leader's session ends, a standby can take over.
	a.Release()
	if !must[bool](t)(b.TryAcquire(ctx)) {
		t.Fatal("the lock was not released when the leader stepped down")
	}
}

func TestEpochFencing(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	old := must[int64](t)(db.BumpEpoch(ctx, "a", "a:8080"))
	if err := db.WithFencedTx(ctx, old, func(tx *DB) error { return nil }); err != nil {
		t.Fatalf("the current leader was fenced: %v", err)
	}

	// A new leader is elected; the old one hasn't noticed yet.
	if _, err := db.BumpEpoch(ctx, "b", "b:8080"); err != nil {
		t.Fatal(err)
	}
	wrote := false
	err := db.WithFencedTx(ctx, old, func(tx *DB) error {
		wrote = true
		return nil
	})
	if !errors.Is(err, ErrFenced) || wrote {
		t.Fatalf("a deposed leader wrote (err=%v, ran=%v)", err, wrote)
	}

	l := must[*Leader](t)(db.GetLeader(ctx))
	if l.CoordinatorID != "b" || l.Epoch != old+1 {
		t.Fatalf("leader = %+v", l)
	}
}

func TestBumpEpochWaitsForFencedTransactions(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	old := must[int64](t)(db.BumpEpoch(ctx, "a", "a:8080"))

	// The old leader is in the middle of a fenced write when the new leader
	// is elected. The election must wait for the write to finish, so the
	// two leaders' writes can never interleave.
	inTx := make(chan struct{})
	release := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = db.WithFencedTx(ctx, old, func(tx *DB) error {
			close(inTx)
			<-release
			return nil
		})
	}()
	<-inTx

	bumped := make(chan time.Time, 1)
	go func() {
		must[int64](t)(db.BumpEpoch(ctx, "b", "b:8080"))
		bumped <- time.Now()
	}()

	time.Sleep(300 * time.Millisecond)
	select {
	case <-bumped:
		t.Fatal("the epoch changed while the old leader's fenced write was in progress")
	default:
	}
	finished := time.Now()
	close(release)
	wg.Wait()
	if at := <-bumped; at.Before(finished) {
		t.Fatal("the epoch bump did not wait for the fenced write")
	}
}

func TestRebuildQueries(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	running := create(t, db, "running", func(n *NewTask) { n.TimeoutSeconds = 42 })
	pick(t, db)
	must[bool](t)(db.MarkTaskStarted(ctx, running.ID, 1, 7))
	create(t, db, "later", func(n *NewTask) { n.ScheduledAt = time.Now().Add(time.Hour) })

	dispatched := must[[]DispatchedTask](t)(db.ListDispatchedTasks(ctx))
	if len(dispatched) != 1 || dispatched[0].ID != running.ID || dispatched[0].WorkerID == nil ||
		*dispatched[0].WorkerID != 7 || dispatched[0].Attempt != 1 || dispatched[0].TimeoutSeconds != 42 {
		t.Fatalf("dispatched = %+v", dispatched)
	}

	next := must[*time.Time](t)(db.NextDueAt(ctx))
	if next == nil || time.Until(*next) < 59*time.Minute || time.Until(*next) > 61*time.Minute {
		t.Fatalf("next due = %v, want about an hour from now", next)
	}
}

func TestStatementFencing(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	old := db.Fenced(must[int64](t)(db.BumpEpoch(ctx, "a", "a:8080")))

	// While leading, the hot transitions work as usual.
	task := create(t, db, "running")
	picked := must[[]*Task](t)(old.PickTasks(ctx, shellWorkers, 10))
	if len(picked) != 1 || !must[bool](t)(old.MarkTaskStarted(ctx, task.ID, 1, 1)) {
		t.Fatalf("the leader could not dispatch (picked %d)", len(picked))
	}

	// A new leader is elected; the old one hasn't noticed yet.
	current := db.Fenced(must[int64](t)(db.BumpEpoch(ctx, "b", "b:8080")))
	queued := create(t, db, "queued")
	if _, err := old.PickTasks(ctx, shellWorkers, 10); !errors.Is(err, ErrFenced) {
		t.Fatalf("a deposed leader's pick returned %v, want ErrFenced", err)
	}
	if _, err := old.MarkTaskCompleted(ctx, task.ID, 1, 0, "late", nil); !errors.Is(err, ErrFenced) {
		t.Fatalf("a deposed leader's completion returned %v, want ErrFenced", err)
	}
	if err := old.WithTx(ctx, func(*DB) error { return nil }); !errors.Is(err, ErrFenced) {
		t.Fatalf("a deposed leader's transaction returned %v, want ErrFenced", err)
	}
	if got := must[*Task](t)(db.GetTask(ctx, task.ID)); got.Status != "STARTED" {
		t.Fatalf("a deposed leader changed a task to %s", got.Status)
	}
	if got := must[*Task](t)(db.GetTask(ctx, queued.ID)); got.Attempt != 0 {
		t.Fatal("a deposed leader claimed a task")
	}

	// The new leader carries on. A completion for the wrong attempt is
	// simply not applied, not mistaken for fencing.
	if r, err := current.MarkTaskCompleted(ctx, task.ID, 2, 0, "", nil); err != nil || r.Updated {
		t.Fatalf("stale attempt: updated=%v err=%v", r.Updated, err)
	}
	if r := must[TaskResult](t)(current.MarkTaskCompleted(ctx, task.ID, 1, 0, "ok", nil)); !r.Updated {
		t.Fatal("the current leader's completion was rejected")
	}
	if got := must[[]*Task](t)(current.PickTasks(ctx, shellWorkers, 10)); len(got) != 1 {
		t.Fatalf("the current leader picked %d tasks, want 1", len(got))
	}
}

func TestBumpEpochWaitsForFencedStatements(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	old := must[int64](t)(db.BumpEpoch(ctx, "a", "a:8080"))
	task := create(t, db, "running")
	pick(t, db)

	// A fenced statement holds its share lock on the leader row until its
	// transaction ends; run one in a transaction to hold it open.
	inTx := make(chan struct{})
	release := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = db.WithTx(ctx, func(tx *DB) error {
			if _, err := tx.Fenced(old).MarkTaskStarted(ctx, task.ID, 1, 1); err != nil {
				return err
			}
			close(inTx)
			<-release
			return nil
		})
	}()
	<-inTx

	bumped := make(chan struct{})
	go func() {
		must[int64](t)(db.BumpEpoch(ctx, "b", "b:8080"))
		close(bumped)
	}()
	select {
	case <-bumped:
		t.Fatal("the epoch changed while a fenced statement was in progress")
	case <-time.After(300 * time.Millisecond):
	}
	close(release)
	wg.Wait()
	<-bumped
}

func TestLocalWakeSkipsNotify(t *testing.T) {
	dsn := testDSN(t)
	ctx := context.Background()
	db := must[*DB](t)(Open(ctx, dsn))
	defer db.Close()
	if err := db.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	leader := must[*DB](t)(Open(ctx, WithLocalWake(dsn)))
	defer leader.Close()

	l := pq.NewListener(WithUTC(dsn), time.Second, time.Second, nil)
	defer l.Close()
	if err := l.Listen("conductor_tasks"); err != nil {
		t.Fatal(err)
	}
	notified := func() bool {
		select {
		case n := <-l.Notify:
			return n != nil
		case <-time.After(500 * time.Millisecond):
			return false
		}
	}

	// The leader wakes its own dispatcher, so its inserts don't notify.
	create(t, leader, "from the leader")
	if notified() {
		t.Fatal("the leader's insert sent a notification")
	}
	// Anyone else's do.
	create(t, db, "from elsewhere")
	if !notified() {
		t.Fatal("an insert from another session sent no notification")
	}
}

// Worker IDs use the full uint32 range, so they must survive the round trip
// through the terminal transitions, which record the worker when the task
// finished before it was marked started.
func TestFinishRecordsLargeWorkerID(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	const worker = 3_000_000_000

	done := create(t, db, "done")
	pick(t, db)
	if r := must[TaskResult](t)(db.MarkTaskCompleted(ctx, done.ID, 1, worker, "", nil)); !r.Updated {
		t.Fatal("completion not applied")
	}
	failed := create(t, db, "failed", func(n *NewTask) { n.MaxRetries = 0 })
	pick(t, db)
	if r := must[TaskResult](t)(db.MarkTaskFailed(ctx, failed.ID, 1, worker, "", "boom")); !r.Updated {
		t.Fatal("failure not applied")
	}
	for _, id := range []uuid.UUID{done.ID, failed.ID} {
		got := must[*Task](t)(db.GetTask(ctx, id))
		if got.WorkerID == nil || *got.WorkerID != worker || got.StartedAt == nil {
			t.Fatalf("task %s: worker_id=%v started_at=%v, want %d and set", id, got.WorkerID, got.StartedAt, worker)
		}
	}
}

func TestLeaderHeartbeat(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	old := must[int64](t)(db.BumpEpoch(ctx, "a", "a:8080"))
	if l := must[*Leader](t)(db.GetLeader(ctx)); l.HeartbeatAt == nil {
		t.Fatal("a new leader has no heartbeat")
	}

	// A heartbeat must not wait for the share locks held by fenced writes:
	// that stalled the hot path and, under latency, timed out.
	inTx := make(chan struct{})
	release := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = db.WithFencedTx(ctx, old, func(*DB) error {
			close(inTx)
			<-release
			return nil
		})
	}()
	<-inTx
	beatCtx, cancel := context.WithTimeout(ctx, time.Second)
	err := db.LeaderHeartbeat(beatCtx, old)
	cancel()
	close(release)
	wg.Wait()
	if err != nil {
		t.Fatalf("the heartbeat waited for a fenced write: %v", err)
	}

	// After an election, the old leader's heartbeat doesn't count.
	current := must[int64](t)(db.BumpEpoch(ctx, "b", "b:8080"))
	exec(t, db, `UPDATE leader_heartbeat SET heartbeat_at = NOW() - INTERVAL '1 hour'`)
	if err := db.LeaderHeartbeat(ctx, old); err != nil {
		t.Fatal(err)
	}
	if l := must[*Leader](t)(db.GetLeader(ctx)); l.Epoch != current || time.Since(*l.HeartbeatAt) < time.Minute {
		t.Fatalf("a deposed leader's heartbeat refreshed the new leader (%+v)", l)
	}
}
