package db

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
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
	if must[TaskResult](t)(db.MarkTaskCompleted(ctx, task.ID, 1, "stale", nil)).Updated {
		t.Fatal("a COMPLETE report from the old attempt was accepted")
	}
	if must[bool](t)(db.RetryTask(ctx, task.ID, 1, "", "stale failure")) {
		t.Fatal("a FAILED report from the old attempt caused a retry")
	}

	// The current attempt's reports do count.
	if !must[bool](t)(db.MarkTaskStarted(ctx, task.ID, 2, 2)) {
		t.Fatal("the current attempt's STARTED report was rejected")
	}
	if !must[TaskResult](t)(db.MarkTaskCompleted(ctx, task.ID, 2, "ok", nil)).Updated {
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
