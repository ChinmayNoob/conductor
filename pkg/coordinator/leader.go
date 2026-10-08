package coordinator

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/ChinmayNoob/conductor/pkg/coordclient"
	"github.com/ChinmayNoob/conductor/pkg/db"
	"github.com/ChinmayNoob/conductor/pkg/grpcapi"
	"github.com/ChinmayNoob/conductor/pkg/metrics"
	"github.com/google/uuid"
	"github.com/lib/pq"
	"google.golang.org/grpc"
)

// Leader election.
//
// Every coordinator campaigns by trying to take a Postgres advisory lock on a
// dedicated session. The holder bumps the epoch in coordinator_leader and
// runs the dispatch, recovery, workflow and schedule loops; the others are
// hot standbys that redirect callers to it. Postgres releases the lock when
// the leader's session dies, so a standby takes over within about a second.
//
// Every write the leader makes is fenced: it re-checks the epoch under a
// share lock on the leader row, in the same transaction (fenced) or the same
// statement (fencedStmt, for the hot task transitions). A new leader's epoch
// bump waits for those locks, and once it commits the old leader's writes
// fail with db.ErrFenced: a deposed leader can't change anything, even if it
// hasn't noticed yet.

const (
	campaignInterval = time.Second
	sessionCheck     = time.Second
	// A leader whose heartbeat is older than this isn't advertised to clients.
	leaderStale = 5 * time.Second
)

var errNotLeader = errors.New("not the leader")

// campaign runs until ctx ends, leading whenever this coordinator holds the
// lock.
func (s *Server) campaign(ctx context.Context) {
	for ctx.Err() == nil {
		sess, err := s.db.NewLeaderSession(ctx)
		if err != nil {
			s.log.Warn("Cannot campaign for leadership", "error", err)
			sleep(ctx, campaignInterval)
			continue
		}
		ok, err := sess.TryAcquire(ctx)
		if err != nil || !ok {
			sess.Release()
			sleep(ctx, campaignInterval)
			continue
		}

		epoch, err := s.db.BumpEpoch(ctx, s.opts.ID, s.opts.Address)
		if err != nil {
			s.log.Error("Won the lock but failed to record leadership", "error", err)
			sess.Release()
			sleep(ctx, campaignInterval)
			continue
		}
		s.log.Info("Became leader", "epoch", epoch)
		metrics.LeaderElections.Inc()
		s.lead(ctx, sess, epoch)
		sess.Release()
		s.log.Warn("Stepped down", "epoch", epoch)
	}
}

// lead runs the leader's loops until leadership is lost or ctx ends.
func (s *Server) lead(ctx context.Context, sess *db.LeaderSession, epoch int64) {
	leadCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	s.mu.Lock()
	s.epoch = epoch
	s.fencedOut = make(chan struct{})
	fencedOut := s.fencedOut
	s.mu.Unlock()

	if err := s.db.SetPriorityAging(leadCtx, s.opts.PriorityAging); err != nil {
		s.log.Error("Failed to apply priority aging", "error", err)
	}
	s.rebuild(leadCtx)
	if err := s.registerExamples(leadCtx); err != nil {
		s.log.Error("Failed to register example workflows", "error", err)
	}

	loops := []func(context.Context){
		s.dispatchLoop, s.recoveryLoop, s.workflowSweepLoop, s.scheduleLoop, s.listen,
	}
	var wg sync.WaitGroup
	for _, loop := range loops {
		wg.Add(1)
		go func() {
			defer wg.Done()
			loop(leadCtx)
		}()
	}

	// Watch the lock's session: if it dies, Postgres has released the lock
	// and another coordinator may already lead.
	ticker := time.NewTicker(sessionCheck)
watch:
	for {
		select {
		case <-ctx.Done():
			break watch
		case <-fencedOut:
			s.log.Error("Fenced: another coordinator became leader")
			break watch
		case <-ticker.C:
			// Only the session decides leadership: if it is gone, Postgres
			// has released the lock. A slow database is not a lost session,
			// so allow the ping plenty of time; fencing covers the gap.
			pingCtx, pingCancel := context.WithTimeout(ctx, 5*time.Second)
			err := sess.Ping(pingCtx)
			pingCancel()
			if err != nil {
				s.log.Error("Lost the leader session", "error", err)
				break watch
			}
			// The heartbeat only tells clients the leader is alive.
			beatCtx, beatCancel := context.WithTimeout(ctx, 2*time.Second)
			if err := s.db.LeaderHeartbeat(beatCtx, epoch); err != nil && ctx.Err() == nil {
				s.log.Warn("Failed to record leader heartbeat", "error", err)
			}
			beatCancel()
		}
	}
	ticker.Stop()

	s.mu.Lock()
	s.epoch = 0
	s.mu.Unlock()
	cancel()
	wg.Wait()
	s.resetState()
}

// fenced runs fn in a transaction that only commits while this coordinator
// is the leader that was elected.
func (s *Server) fenced(ctx context.Context, fn func(tx *db.DB) error) error {
	return s.fencedStmt(ctx, func(q *db.DB) error { return q.WithTx(ctx, fn) })
}

// fencedStmt runs fn with a handle fenced to this leader's epoch, outside a
// transaction. Only db methods that fence their own statement may be used.
func (s *Server) fencedStmt(_ context.Context, fn func(q *db.DB) error) error {
	s.mu.RLock()
	epoch, fencedOut := s.epoch, s.fencedOut
	s.mu.RUnlock()
	if epoch == 0 {
		return errNotLeader
	}
	err := fn(s.db.Fenced(epoch))
	if errors.Is(err, db.ErrFenced) {
		s.mu.Lock()
		if s.fencedOut == fencedOut && fencedOut != nil {
			close(fencedOut)
			s.fencedOut = nil
		}
		s.mu.Unlock()
	}
	return err
}

func (s *Server) isLeader() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.epoch != 0
}

// Epoch returns the current leadership epoch, or 0 when not leading.
func (s *Server) Epoch() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.epoch
}

// LeaderOnly is a gRPC interceptor that turns callers away from a standby,
// telling them where the leader is.
func (s *Server) LeaderOnly(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if !s.isLeader() {
		s.mu.RLock()
		leader := s.leaderAddr
		s.mu.RUnlock()
		return nil, coordclient.NotLeaderError(leader)
	}
	return handler(ctx, req)
}

// presenceLoop records this coordinator and caches the leader's address for
// redirects, until ctx ends.
func (s *Server) presenceLoop(ctx context.Context) {
	for {
		if err := s.db.RegisterCoordinator(ctx, s.opts.ID, s.opts.Address); err != nil && ctx.Err() == nil {
			s.log.Warn("Failed to record coordinator", "error", err)
		}
		if l, err := s.db.GetLeader(ctx); err == nil {
			addr := ""
			if l.HeartbeatAt != nil && time.Since(*l.HeartbeatAt) < leaderStale {
				addr = l.Address
			}
			s.mu.Lock()
			s.leaderAddr = addr
			s.mu.Unlock()
		}
		if !sleep(ctx, 2*time.Second) {
			return
		}
	}
}

// rebuild restores the leader's in-memory view from the database: workers
// recently seen (by whichever coordinator led before) and the tasks they are
// running. Without it, a new leader would treat every worker as unknown and
// fail all running tasks.
func (s *Server) rebuild(ctx context.Context) {
	records, err := s.db.ListWorkers(ctx, workerTimeout)
	if err != nil {
		s.log.Error("Failed to load workers", "error", err)
	}
	s.mu.Lock()
	for _, r := range records {
		if r.Status == "unhealthy" {
			continue
		}
		conn, err := grpc.NewClient(r.Address, s.opts.DialOptions...)
		if err != nil {
			continue
		}
		s.workers[uint32(r.ID)] = &Worker{
			ID: uint32(r.ID), Address: r.Address, LastSeen: r.LastSeen, IsHealthy: true,
			Draining: r.Status == "draining", Slots: max(r.Slots, 1), Labels: r.Labels,
			client: grpcapi.NewWorkerServiceClient(conn), conn: conn,
		}
	}
	s.mu.Unlock()

	tasks, err := s.db.ListDispatchedTasks(ctx)
	if err != nil {
		s.log.Error("Failed to load dispatched tasks", "error", err)
	}
	restored := 0
	s.mu.Lock()
	for _, t := range tasks {
		if t.WorkerID == nil {
			continue // picked but not started; the stale-task reset covers it
		}
		w, ok := s.workers[uint32(*t.WorkerID)]
		if !ok {
			continue // its worker is gone; overdue detection covers it
		}
		start := time.Now()
		if t.StartedAt != nil {
			start = *t.StartedAt
		}
		s.inFlight[t.ID] = dispatchedTask{
			workerID:  w.ID,
			attempt:   t.Attempt,
			deadline:  start.Add(time.Duration(t.TimeoutSeconds)*time.Second + lostTaskGrace),
			namespace: t.Namespace,
			queue:     t.Queue,
		}
		w.inFlight++
		restored++
	}
	s.mu.Unlock()
	s.log.Info("Restored state", "workers", len(records), "running_tasks", restored)
}

// resetState forgets everything a leader keeps in memory.
func (s *Server) resetState() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, w := range s.workers {
		if w.conn != nil {
			w.conn.Close()
		}
	}
	s.workers = make(map[uint32]*Worker)
	s.inFlight = make(map[uuid.UUID]dispatchedTask)
}

// listen wakes the dispatcher and scheduler on Postgres notifications, so
// work created elsewhere (the API, retries coming due) starts immediately.
func (s *Server) listen(ctx context.Context) {
	if s.opts.DSN == "" {
		return
	}
	l := pq.NewListener(s.opts.DSN, time.Second, 10*time.Second, func(ev pq.ListenerEventType, err error) {
		if err != nil {
			s.log.Warn("Notification listener", "event", ev, "error", err)
		}
	})
	defer l.Close()
	for _, ch := range []string{"conductor_tasks", "conductor_schedules"} {
		if err := l.Listen(ch); err != nil {
			s.log.Warn("Cannot listen for notifications; falling back to polling", "error", err)
			return
		}
	}
	for {
		select {
		case <-ctx.Done():
			return
		case n := <-l.Notify:
			switch {
			case n == nil: // reconnected; something may have been missed
				s.wakeDispatcher()
				s.wakeScheduler()
			case n.Channel == "conductor_schedules":
				s.wakeScheduler()
			default:
				s.wakeDispatcher()
			}
		}
	}
}

// sleep waits for d or until ctx ends, reporting whether it slept fully.
func sleep(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}
