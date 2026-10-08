// Package coordinator is the brain of the cluster: it tracks workers through
// heartbeats, dispatches queued tasks to them, handles retries, drives
// workflow DAGs, and fires cron schedules. Several coordinators may run; one
// is elected leader and does all of this, the rest stand by (see leader.go).
package coordinator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/ChinmayNoob/conductor/pkg/db"
	"github.com/ChinmayNoob/conductor/pkg/grpcapi"
	"github.com/ChinmayNoob/conductor/pkg/metrics"
	"github.com/ChinmayNoob/conductor/pkg/task"
	"github.com/ChinmayNoob/conductor/pkg/tracing"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// A worker that misses heartbeats for this long is considered dead.
	workerTimeout = 30 * time.Second

	// Extra time on top of a task's own timeout before a task with no result
	// is considered lost.
	lostTaskGrace = 30 * time.Second

	// A task picked for dispatch but never started within this long is
	// requeued (e.g. the coordinator crashed mid-dispatch).
	staleTaskThreshold = 5 * time.Minute

	// Notifications wake the dispatcher; this is only a safety net.
	maxDispatchSleep = 5 * time.Second
)

type Worker struct {
	ID        uint32
	Address   string
	LastSeen  time.Time
	IsHealthy bool
	Draining  bool
	Slots     int
	Labels    map[string]string
	inFlight  int
	client    grpcapi.WorkerServiceClient
	conn      *grpc.ClientConn
}

func (w *Worker) available() bool {
	return w.IsHealthy && !w.Draining && w.client != nil && w.inFlight < w.Slots
}

// hasLabels reports whether the worker has every required label.
func (w *Worker) hasLabels(req map[string]string) bool {
	for k, v := range req {
		if w.Labels[k] != v {
			return false
		}
	}
	return true
}

func (w *Worker) status() string {
	switch {
	case !w.IsHealthy:
		return "unhealthy"
	case w.Draining:
		return "draining"
	}
	return "healthy"
}

// dispatchedTask records which worker runs which attempt of a task, so the
// worker's slot can be freed and its tasks recovered if it dies.
type dispatchedTask struct {
	workerID uint32
	attempt  int
	deadline time.Time
	// For metrics.
	namespace, queue string
	dispatchedAt     time.Time
}

type Options struct {
	// ID and Address identify this coordinator; Address is where callers
	// are redirected when it leads.
	ID      string
	Address string
	// DSN is used to LISTEN for notifications. Empty disables them.
	DSN string
	// DialOptions are used to connect to workers.
	DialOptions []grpc.DialOption
	// PriorityAging raises a waiting task's priority one level per interval.
	PriorityAging time.Duration
}

type Server struct {
	grpcapi.UnimplementedCoordinatorServiceServer

	db   *db.DB
	opts Options
	log  *slog.Logger

	mu           sync.RWMutex
	epoch        int64         // 0 when not leading
	fencedOut    chan struct{} // closed when a fenced write finds a newer leader
	leaderAddr   string        // the current leader, for redirects
	workers      map[uint32]*Worker
	inFlight     map[uuid.UUID]dispatchedTask
	lastWorkerID uint32

	wake      chan struct{}
	wakeSched chan struct{}
}

// NewServer creates a coordinator. Call Run to campaign and, once elected,
// dispatch.
func NewServer(database *db.DB, opts Options) *Server {
	return &Server{
		db:        database,
		opts:      opts,
		log:       slog.Default(),
		workers:   make(map[uint32]*Worker),
		inFlight:  make(map[uuid.UUID]dispatchedTask),
		wake:      make(chan struct{}, 1),
		wakeSched: make(chan struct{}, 1),
	}
}

// Run campaigns for leadership and leads whenever elected, until ctx ends.
func (s *Server) Run(ctx context.Context) {
	go s.presenceLoop(ctx)
	s.campaign(ctx)
	s.log.Info("Coordinator stopped")
}

func (s *Server) recoveryLoop(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.recoverLostTasks(ctx)
			var n int64
			err := s.fenced(ctx, func(tx *db.DB) error {
				var err error
				n, err = tx.ResetStaleTasks(ctx, staleTaskThreshold)
				return err
			})
			if err != nil && ctx.Err() == nil {
				s.log.Error("Failed to reset stale tasks", "error", err)
			} else if n > 0 {
				s.log.Warn("Requeued stale tasks", "count", n)
				s.wakeDispatcher()
			}
		}
	}
}

// recoverLostTasks marks workers that stopped heartbeating as unhealthy and
// fails the tasks they were running, along with any task that ran far past its
// timeout without a result. Failing them sends them through the normal retry
// (or workflow compensation) path instead of leaving them stuck forever.
func (s *Server) recoverLostTasks(ctx context.Context) {
	type lostTask struct {
		attempt int
		reason  string
	}
	now := time.Now()
	lost := make(map[uuid.UUID]lostTask)
	var died []uint32

	s.mu.Lock()
	for id, worker := range s.workers {
		if worker.IsHealthy && now.Sub(worker.LastSeen) > workerTimeout {
			s.log.Warn("Worker missed heartbeats, marking unhealthy", "worker_id", id)
			worker.IsHealthy = false
			died = append(died, id)
		}
	}
	for taskID, t := range s.inFlight {
		if w, ok := s.workers[t.workerID]; !ok || !w.IsHealthy {
			lost[taskID] = lostTask{t.attempt, fmt.Sprintf("worker %d became unhealthy", t.workerID)}
		} else if now.After(t.deadline) {
			// The task itself is recovered from the database below; here we
			// only free the worker's slot.
			s.releaseTaskLocked(taskID)
		}
	}
	s.mu.Unlock()

	for _, id := range died {
		if err := s.db.SetWorkerStatus(ctx, int64(id), "unhealthy"); err != nil {
			s.log.Warn("Failed to record worker status", "worker_id", id, "error", err)
		}
	}

	// Catches tasks the in-memory bookkeeping doesn't know about, e.g. ones
	// running on a worker that died while another coordinator led.
	overdue, err := s.db.ListOverdueTasks(ctx, lostTaskGrace)
	if err != nil && ctx.Err() == nil {
		s.log.Error("Failed to list overdue tasks", "error", err)
	}
	for _, t := range overdue {
		if _, ok := lost[t.ID]; !ok {
			lost[t.ID] = lostTask{t.Attempt, "no result reported within the task timeout"}
		}
	}

	for taskID, l := range lost {
		s.log.Warn("Task lost", "task_id", taskID, "attempt", l.attempt, "reason", l.reason)
		d := s.releaseTask(taskID)
		metrics.TasksLost.Inc()
		if _, err := s.failTask(ctx, taskID, l.attempt, d, "", "task lost: "+l.reason); err != nil {
			s.log.Error("Failed to recover lost task", "task_id", taskID, "error", err)
		}
	}
	if len(lost) > 0 {
		s.wakeDispatcher()
	}
}

// dispatchLoop hands out tasks whenever something may have become runnable:
// a notification, a worker freeing up, or the next delayed task coming due.
func (s *Server) dispatchLoop(ctx context.Context) {
	timer := time.NewTimer(0)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		case <-s.wake:
		}
		s.dispatchTasks(ctx)

		// Sleep until the next delayed task (or retry) is due.
		wait := maxDispatchSleep
		if next, err := s.db.NextDueAt(ctx); err == nil && next != nil {
			wait = min(max(time.Until(*next), 10*time.Millisecond), maxDispatchSleep)
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(wait)
	}
}

// wakeDispatcher runs a dispatch round now.
func (s *Server) wakeDispatcher() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// wakeScheduler checks schedules now.
func (s *Server) wakeScheduler() {
	select {
	case s.wakeSched <- struct{}{}:
	default:
	}
}

// maxBatch caps how many tasks one pick query claims.
const maxBatch = 64

// dispatchTasks hands out queued tasks until the queue is empty or no free
// worker can run what's left. Each round claims a batch sized to the free
// slots in one query and sends the tasks to their workers in the background:
// slots are reserved before sending, so the next round can start at once.
func (s *Server) dispatchTasks(ctx context.Context) {
	for ctx.Err() == nil {
		// Only claim tasks some free worker can run, so tasks aren't picked
		// just to be put back.
		labels, free := s.freeCapacity()
		if free == 0 {
			return
		}
		want := min(free, maxBatch)
		var tasks []*db.Task
		err := s.fencedStmt(ctx, func(q *db.DB) error {
			var err error
			tasks, err = q.PickTasks(ctx, db.PickOptions{WorkerLabels: labels}, want)
			return err
		})
		if err != nil {
			if ctx.Err() == nil && !errors.Is(err, errNotLeader) {
				s.log.Error("Failed to pick tasks", "error", err)
			}
			return
		}

		for _, t := range tasks {
			if t.PickedAt != nil {
				metrics.DispatchLatency.Observe(t.PickedAt.Sub(latest(t.CreatedAt, t.ScheduledAt)).Seconds())
			}
			worker := s.chooseWorker(t.Requirements)
			if worker == nil {
				// No free worker has its labels (another task in the batch
				// took the last matching slot).
				s.requeueTask(ctx, t.ID)
				continue
			}
			// Reserve the slot now so the next task sees it taken.
			s.trackTask(t, worker)
			go s.sendTask(ctx, worker, t)
		}
		if len(tasks) < want {
			return
		}
	}
}

// freeCapacity returns the distinct label sets of workers with a free slot,
// and the total number of free slots.
func (s *Server) freeCapacity() ([]map[string]string, int) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	seen := make(map[string]bool)
	var labels []map[string]string
	free := 0
	for _, w := range s.workers {
		if !w.available() {
			continue
		}
		free += w.Slots - w.inFlight
		key := labelKey(w.Labels)
		if !seen[key] {
			seen[key] = true
			labels = append(labels, w.Labels)
		}
	}
	return labels, free
}

func labelKey(labels map[string]string) string {
	keys := slices.Sorted(maps.Keys(labels))
	var b strings.Builder
	for _, k := range keys {
		b.WriteString(k + "=" + labels[k] + "\x00")
	}
	return b.String()
}

// chooseWorker picks the next free worker that has the required labels,
// going round-robin in worker ID order.
func (s *Server) chooseWorker(req map[string]string) *Worker {
	s.mu.Lock()
	defer s.mu.Unlock()

	var ids []uint32
	for id, w := range s.workers {
		if w.available() && w.hasLabels(req) {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return nil
	}
	slices.Sort(ids)

	next := ids[0]
	for _, id := range ids {
		if id > s.lastWorkerID {
			next = id
			break
		}
	}
	s.lastWorkerID = next
	return s.workers[next]
}

// sendTask hands a claimed task to its worker. The caller has already
// reserved the worker's slot with trackTask; on failure the slot is freed and
// the task requeued.
func (s *Server) sendTask(ctx context.Context, worker *Worker, t *db.Task) {
	log := s.log.With("task_id", t.ID, "worker_id", worker.ID, "attempt", t.Attempt)
	log.Debug("Dispatching task", "priority", t.Priority)

	// Continue the submitter's trace; the worker's run span follows.
	ctx, span := tracing.Start(tracing.WithTraceParent(ctx, t.TraceParent), "dispatch",
		trace.WithAttributes(attribute.String("conductor.task_id", t.ID.String()),
			attribute.Int("conductor.attempt", t.Attempt), attribute.Int64("conductor.worker_id", int64(worker.ID)),
			attribute.String("conductor.queue", t.Queue)))
	defer span.End()

	rpcCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	resp, err := worker.client.SubmitTask(rpcCtx, &grpcapi.TaskRequest{
		TaskId:         t.ID.String(),
		Type:           t.Type,
		Data:           t.Data,
		SpecJson:       t.Spec,
		Env:            t.Env,
		TimeoutSeconds: int32(t.TimeoutSeconds),
		RetryCount:     int32(t.RetryCount),
		Attempt:        int32(t.Attempt),
	})

	if err != nil {
		log.Warn("Failed to dispatch task, marking worker unhealthy", "error", err)
		metrics.DispatchRejected.Inc()
		s.releaseTask(t.ID)
		s.requeueTask(ctx, t.ID)
		s.mu.Lock()
		worker.IsHealthy = false
		s.mu.Unlock()
		s.wakeDispatcher()
		return
	}
	if !resp.Success {
		log.Info("Worker rejected task", "reason", resp.Message)
		metrics.DispatchRejected.Inc()
		s.releaseTask(t.ID)
		s.requeueTask(ctx, t.ID)
		s.wakeDispatcher()
		return
	}

	// The worker has the task: record it as started. A result may already
	// have arrived for a fast task, in which case this changes nothing.
	err = s.fencedStmt(ctx, func(q *db.DB) error {
		_, err := q.MarkTaskStarted(context.WithoutCancel(ctx), t.ID, t.Attempt, int64(worker.ID))
		return err
	})
	if err != nil && !errors.Is(err, errNotLeader) && !errors.Is(err, db.ErrFenced) {
		log.Warn("Failed to mark task started", "error", err)
	}
	metrics.TasksDispatched.WithLabelValues(t.Namespace, t.Queue).Inc()
	log.Info("Task dispatched")
}

func (s *Server) requeueTask(ctx context.Context, taskID uuid.UUID) {
	err := s.fenced(context.WithoutCancel(ctx), func(tx *db.DB) error { return tx.RequeueTask(ctx, taskID) })
	if err != nil {
		s.log.Error("Failed to requeue task", "task_id", taskID, "error", err)
	}
}

func (s *Server) trackTask(t *db.Task, worker *Worker) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	s.inFlight[t.ID] = dispatchedTask{
		workerID:     worker.ID,
		attempt:      t.Attempt,
		deadline:     now.Add(time.Duration(t.TimeoutSeconds)*time.Second + lostTaskGrace),
		namespace:    t.Namespace,
		queue:        t.Queue,
		dispatchedAt: now,
	}
	worker.inFlight++
}

func latest(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

// releaseTask frees the worker slot held by a task and returns what was
// tracked about it. It is a no-op (returning the zero value) for tasks that
// aren't tracked.
func (s *Server) releaseTask(taskID uuid.UUID) dispatchedTask {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.releaseTaskLocked(taskID)
}

func (s *Server) releaseTaskLocked(taskID uuid.UUID) dispatchedTask {
	t, ok := s.inFlight[taskID]
	if !ok {
		return dispatchedTask{}
	}
	delete(s.inFlight, taskID)
	if w, ok := s.workers[t.workerID]; ok && w.inFlight > 0 {
		w.inFlight--
	}
	return t
}

// observeResult records a task attempt's outcome.
func observeResult(d dispatchedTask, result string) {
	metrics.TaskResults.WithLabelValues(d.namespace, d.queue, result).Inc()
	if !d.dispatchedAt.IsZero() {
		metrics.TaskRunTime.Observe(time.Since(d.dispatchedAt).Seconds())
	}
}

// --- Task submission and cancellation ---

func namespaceOf(ns string) string {
	if ns == "" {
		return "default"
	}
	return ns
}

// checkNamespace returns the namespace, or a gRPC error if it doesn't exist
// or is over its pending-task quota.
func (s *Server) checkNamespace(ctx context.Context, name string, adding bool) (*db.Namespace, error) {
	ns, err := s.db.GetNamespace(ctx, name)
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to load namespace")
	}
	if ns == nil {
		return nil, status.Errorf(codes.NotFound, "namespace %q does not exist", name)
	}
	if adding && ns.MaxPendingTasks != nil {
		n, err := s.db.CountPendingTasks(ctx, name)
		if err != nil {
			return nil, status.Error(codes.Internal, "failed to count pending tasks")
		}
		if n >= *ns.MaxPendingTasks {
			return nil, status.Errorf(codes.ResourceExhausted,
				"namespace %q has %d pending tasks, its limit", name, n)
		}
	}
	return ns, nil
}

func (s *Server) SubmitTask(ctx context.Context, req *grpcapi.ClientTaskRequest) (*grpcapi.ClientTaskResponse, error) {
	ns := namespaceOf(req.Namespace)
	if _, err := s.checkNamespace(ctx, ns, true); err != nil {
		return nil, err
	}

	var tmpl task.Template
	if err := json.Unmarshal(req.TemplateJson, &tmpl); err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid task: "+err.Error())
	}
	n, err := tmpl.NewTask(ns)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if req.ScheduledAt > 0 {
		n.ScheduledAt = time.Unix(req.ScheduledAt, 0).UTC()
	}
	n.IdempotencyKey = req.IdempotencyKey
	n.TraceParent = tracing.TraceParent(ctx)

	t, created, err := s.db.CreateTask(ctx, n)
	if err != nil {
		s.log.Error("Failed to create task", "error", err)
		return nil, status.Error(codes.Internal, "failed to create task")
	}
	if created {
		s.log.Info("Task created", "task_id", t.ID, "namespace", ns, "queue", t.Queue, "type", t.Type)
		s.wakeDispatcher()
	}
	return &grpcapi.ClientTaskResponse{TaskId: t.ID.String(), Created: created}, nil
}

// CancelTask cancels a task. A queued task is simply never dispatched; a
// running one is killed on its worker.
func (s *Server) CancelTask(ctx context.Context, req *grpcapi.CancelTaskRequest) (*grpcapi.CancelTaskResponse, error) {
	taskID, err := uuid.Parse(req.TaskId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid task ID")
	}
	t, err := s.db.GetTask(ctx, taskID)
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to look up task")
	}
	if t == nil || t.Namespace != namespaceOf(req.Namespace) {
		return nil, status.Error(codes.NotFound, "task not found")
	}

	var before *db.Task
	err = s.fenced(ctx, func(tx *db.DB) error {
		var err error
		before, err = tx.CancelTask(ctx, taskID)
		return err
	})
	if err != nil {
		s.log.Error("Failed to cancel task", "task_id", taskID, "error", err)
		return nil, status.Error(codes.Internal, "failed to cancel task")
	}
	if before == nil {
		return &grpcapi.CancelTaskResponse{Cancelled: false}, nil // already finished
	}

	s.log.Info("Task cancelled", "task_id", taskID, "was", before.Status)
	if before.PickedAt != nil {
		s.killOnWorker(ctx, taskID)
	}
	if before.WorkflowID != nil {
		s.reconcile(context.WithoutCancel(ctx), *before.WorkflowID)
	}
	return &grpcapi.CancelTaskResponse{Cancelled: true}, nil
}

// killOnWorker asks the worker running a task to kill it. Its slot is freed
// when the worker reports the (now ignored) result.
func (s *Server) killOnWorker(ctx context.Context, taskID uuid.UUID) {
	s.mu.RLock()
	t, ok := s.inFlight[taskID]
	var w *Worker
	if ok {
		w = s.workers[t.workerID]
	}
	s.mu.RUnlock()
	if w == nil || w.client == nil {
		return
	}

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if _, err := w.client.CancelTask(ctx, &grpcapi.CancelTaskRequest{TaskId: taskID.String()}); err != nil {
		s.log.Warn("Failed to cancel task on worker", "task_id", taskID, "worker_id", w.ID, "error", err)
	}
}

// --- Worker-facing RPCs ---

func (s *Server) SendHeartbeat(ctx context.Context, req *grpcapi.HeartbeatRequest) (*grpcapi.HeartbeatResponse, error) {
	s.mu.Lock()
	worker, exists := s.workers[req.WorkerId]
	if !exists || worker.Address != req.Address {
		conn, err := grpc.NewClient(req.Address, s.opts.DialOptions...)
		if err != nil {
			s.mu.Unlock()
			s.log.Error("Failed to connect to worker", "worker_id", req.WorkerId, "address", req.Address, "error", err)
			return nil, status.Error(codes.InvalidArgument, "invalid worker address")
		}
		if exists {
			worker.conn.Close()
			worker.conn, worker.client, worker.Address = conn, grpcapi.NewWorkerServiceClient(conn), req.Address
		} else {
			worker = &Worker{
				ID:      req.WorkerId,
				Address: req.Address,
				client:  grpcapi.NewWorkerServiceClient(conn),
				conn:    conn,
			}
			s.workers[req.WorkerId] = worker
		}
		s.log.Info("Worker registered", "worker_id", req.WorkerId, "address", req.Address,
			"slots", req.Slots, "labels", req.Labels)
	}

	if req.Draining && !worker.Draining {
		s.log.Info("Worker is draining", "worker_id", req.WorkerId)
	}
	becameAvailable := !worker.available()
	worker.LastSeen = time.Now()
	worker.IsHealthy = true
	worker.Draining = req.Draining
	worker.Slots = max(int(req.Slots), 1)
	worker.Labels = req.Labels
	becameAvailable = becameAvailable && worker.available()
	record := db.WorkerRecord{
		ID: int64(worker.ID), Address: worker.Address, Labels: worker.Labels,
		Slots: worker.Slots, Running: int(req.Running), Status: worker.status(),
	}
	s.mu.Unlock()

	if err := s.db.UpsertWorker(ctx, record); err != nil {
		s.log.Warn("Failed to record worker", "worker_id", req.WorkerId, "error", err)
	}
	if becameAvailable {
		s.wakeDispatcher()
	}
	return &grpcapi.HeartbeatResponse{Acknowledged: true}, nil
}

// UpdateTaskStatus handles task status updates from workers. Reports for
// any attempt other than the task's current one are ignored.
func (s *Server) UpdateTaskStatus(ctx context.Context, req *grpcapi.UpdateTaskStatusRequest) (*grpcapi.UpdateTaskStatusResponse, error) {
	taskID, err := uuid.Parse(req.TaskId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid task ID")
	}
	// Finish state transitions even if the worker's RPC deadline passes.
	ctx = context.WithoutCancel(ctx)
	attempt := int(req.Attempt)
	log := s.log.With("task_id", taskID, "attempt", attempt)

	shouldRetry := false
	switch req.Status {
	case grpcapi.TaskStatus_STARTED:
		// The coordinator marks tasks started when a worker accepts them;
		// workers before Phase 3 also report it. Nothing to do.

	case grpcapi.TaskStatus_COMPLETE:
		d := s.releaseTask(taskID)
		var r db.TaskResult
		err = s.fencedStmt(ctx, func(q *db.DB) error {
			var err error
			r, err = q.MarkTaskCompleted(ctx, taskID, attempt, int64(d.workerID), req.Output, req.Outputs)
			return err
		})
		if err == nil {
			if r.Updated {
				observeResult(d, "completed")
			} else {
				observeResult(d, "stale")
			}
			if r.Updated {
				log.Info("Task completed")
				if r.WorkflowID != nil {
					s.reconcile(ctx, *r.WorkflowID)
				}
			} else {
				log.Info("Ignoring COMPLETE report: not the current attempt")
			}
		}
		s.wakeDispatcher()

	case grpcapi.TaskStatus_FAILED:
		d := s.releaseTask(taskID)
		shouldRetry, err = s.failTask(ctx, taskID, attempt, d, req.Output, req.ErrorMessage)
		s.wakeDispatcher()

	default:
		return nil, status.Errorf(codes.InvalidArgument, "unexpected status %v", req.Status)
	}

	if err != nil {
		log.Error("Failed to update task status", "error", err)
		return nil, status.Error(codes.Unavailable, "failed to update task status; retry")
	}
	return &grpcapi.UpdateTaskStatusResponse{ShouldRetry: shouldRetry}, nil
}

// failTask records a failed attempt. The task is requeued if it has retries
// left; otherwise it is marked FAILED and its workflow, if any, advances
// (which starts compensation). It returns whether the task will be retried.
func (s *Server) failTask(ctx context.Context, taskID uuid.UUID, attempt int, d dispatchedTask, output, errMsg string) (bool, error) {
	log := s.log.With("task_id", taskID, "attempt", attempt)

	var retrying bool
	var r db.TaskResult
	err := s.fenced(ctx, func(tx *db.DB) error {
		var err error
		if retrying, err = tx.RetryTask(ctx, taskID, attempt, output, errMsg); err != nil || retrying {
			return err
		}
		r, err = tx.MarkTaskFailed(ctx, taskID, attempt, int64(d.workerID), output, errMsg)
		return err
	})
	if err != nil {
		return false, err
	}
	if retrying {
		observeResult(d, "retried")
		log.Warn("Task failed, will retry", "error", errMsg)
		return true, nil
	}
	if !r.Updated {
		observeResult(d, "stale")
		log.Info("Ignoring failure: not the current attempt")
		return false, nil
	}
	observeResult(d, "failed")
	log.Error("Task failed permanently", "error", errMsg)
	if r.WorkflowID != nil {
		s.reconcile(ctx, *r.WorkflowID)
	}
	return false, nil
}
