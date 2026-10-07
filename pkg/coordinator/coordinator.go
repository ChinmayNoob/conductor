// Package coordinator is the brain of the cluster: it tracks workers through
// heartbeats, dispatches queued tasks to them, handles retries, drives
// workflow DAGs, and fires cron schedules.
package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/ChinmayNoob/conductor/pkg/db"
	"github.com/ChinmayNoob/conductor/pkg/grpcapi"
	"github.com/ChinmayNoob/conductor/pkg/task"
	"github.com/google/uuid"
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

// dispatchedTask records which worker a task was handed to, so the worker's
// slot can be freed and its tasks recovered if it dies.
type dispatchedTask struct {
	workerID uint32
	deadline time.Time
}

type Options struct {
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
	workers      map[uint32]*Worker
	inFlight     map[uuid.UUID]dispatchedTask
	lastWorkerID uint32
	wake         chan struct{}
}

// NewServer creates a coordinator. Call Run to start dispatching.
func NewServer(database *db.DB, opts Options) *Server {
	return &Server{
		db:       database,
		opts:     opts,
		log:      slog.Default(),
		workers:  make(map[uint32]*Worker),
		inFlight: make(map[uuid.UUID]dispatchedTask),
		wake:     make(chan struct{}, 1),
	}
}

// Run dispatches tasks, recovers lost ones, advances workflows and fires
// schedules until ctx is cancelled, then closes worker connections.
func (s *Server) Run(ctx context.Context) {
	if err := s.registerExamples(ctx); err != nil {
		s.log.Error("Failed to register example workflows", "error", err)
	}

	loops := []func(context.Context){s.dispatchLoop, s.recoveryLoop, s.workflowSweepLoop, s.scheduleLoop}
	var wg sync.WaitGroup
	for _, loop := range loops {
		wg.Add(1)
		go func() {
			defer wg.Done()
			loop(ctx)
		}()
	}
	wg.Wait()

	s.mu.Lock()
	defer s.mu.Unlock()
	for _, w := range s.workers {
		if w.conn != nil {
			w.conn.Close()
		}
	}
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
			if n, err := s.db.ResetStaleTasks(ctx, staleTaskThreshold); err != nil {
				s.log.Error("Failed to reset stale tasks", "error", err)
			} else if n > 0 {
				s.log.Warn("Requeued stale tasks", "count", n)
			}
		}
	}
}

// recoverLostTasks marks workers that stopped heartbeating as unhealthy and
// fails the tasks they were running, along with any task that ran far past its
// timeout without a result. Failing them sends them through the normal retry
// (or workflow compensation) path instead of leaving them stuck forever.
func (s *Server) recoverLostTasks(ctx context.Context) {
	now := time.Now()
	lost := make(map[uuid.UUID]string)
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
			lost[taskID] = fmt.Sprintf("worker %d became unhealthy", t.workerID)
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
	// that were running when the coordinator restarted.
	overdue, err := s.db.ListOverdueTasks(ctx, lostTaskGrace)
	if err != nil {
		s.log.Error("Failed to list overdue tasks", "error", err)
	}
	for _, taskID := range overdue {
		if _, ok := lost[taskID]; !ok {
			lost[taskID] = "no result reported within the task timeout"
		}
	}

	for taskID, reason := range lost {
		s.log.Warn("Task lost", "task_id", taskID, "reason", reason)
		s.releaseTask(taskID)
		if _, err := s.failTask(ctx, taskID, "", "task lost: "+reason); err != nil {
			s.log.Error("Failed to recover lost task", "task_id", taskID, "error", err)
		}
	}
	if len(lost) > 0 {
		s.wakeDispatcher()
	}
}

func (s *Server) dispatchLoop(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-s.wake:
		}
		s.dispatchTasks(ctx)
	}
}

// wakeDispatcher runs a dispatch round now instead of waiting for the next
// tick, e.g. because a worker just freed up.
func (s *Server) wakeDispatcher() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// dispatchTasks hands out queued tasks until the queue is empty or no free
// worker can run what's left.
func (s *Server) dispatchTasks(ctx context.Context) {
	for ctx.Err() == nil {
		// Only claim tasks some free worker can run, so tasks aren't picked
		// just to be put back.
		labels := s.freeWorkerLabels()
		if len(labels) == 0 {
			return
		}
		task, err := s.db.PickNextTask(ctx, db.PickOptions{
			WorkerLabels:  labels,
			AgingInterval: s.opts.PriorityAging,
		})
		if err != nil {
			s.log.Error("Failed to pick next task", "error", err)
			return
		}
		if task == nil {
			return
		}

		worker := s.chooseWorker(task.Requirements)
		if worker == nil {
			// The worker filled up or left since we looked.
			s.requeueTask(task.ID)
			return
		}
		if !s.dispatchTask(ctx, worker, task) {
			return
		}
	}
}

// freeWorkerLabels returns the distinct label sets of workers with a free slot.
func (s *Server) freeWorkerLabels() []map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	seen := make(map[string]bool)
	var out []map[string]string
	for _, w := range s.workers {
		if !w.available() {
			continue
		}
		key := labelKey(w.Labels)
		if !seen[key] {
			seen[key] = true
			out = append(out, w.Labels)
		}
	}
	return out
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

func (s *Server) dispatchTask(ctx context.Context, worker *Worker, t *db.Task) bool {
	log := s.log.With("task_id", t.ID, "worker_id", worker.ID)
	log.Debug("Dispatching task", "priority", t.Priority, "attempt", t.RetryCount)

	// Track the task before sending it: the worker may report back before the
	// RPC below returns.
	s.trackTask(t.ID, worker, time.Duration(t.TimeoutSeconds)*time.Second)

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	resp, err := worker.client.SubmitTask(ctx, &grpcapi.TaskRequest{
		TaskId:         t.ID.String(),
		Type:           t.Type,
		Data:           t.Data,
		SpecJson:       t.Spec,
		Env:            t.Env,
		TimeoutSeconds: int32(t.TimeoutSeconds),
		RetryCount:     int32(t.RetryCount),
	})

	if err != nil {
		log.Warn("Failed to dispatch task, marking worker unhealthy", "error", err)
		s.releaseTask(t.ID)
		s.requeueTask(t.ID)
		s.mu.Lock()
		worker.IsHealthy = false
		s.mu.Unlock()
		return false
	}
	if !resp.Success {
		log.Info("Worker rejected task", "reason", resp.Message)
		s.releaseTask(t.ID)
		s.requeueTask(t.ID)
		return false
	}

	log.Info("Task dispatched")
	return true
}

func (s *Server) requeueTask(taskID uuid.UUID) {
	if err := s.db.RequeueTask(context.Background(), taskID); err != nil {
		s.log.Error("Failed to requeue task", "task_id", taskID, "error", err)
	}
}

func (s *Server) trackTask(taskID uuid.UUID, worker *Worker, timeout time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.inFlight[taskID] = dispatchedTask{
		workerID: worker.ID,
		deadline: time.Now().Add(timeout + lostTaskGrace),
	}
	worker.inFlight++
}

// releaseTask frees the worker slot held by a task. It is a no-op for tasks
// that aren't tracked.
func (s *Server) releaseTask(taskID uuid.UUID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.releaseTaskLocked(taskID)
}

func (s *Server) releaseTaskLocked(taskID uuid.UUID) {
	t, ok := s.inFlight[taskID]
	if !ok {
		return
	}
	delete(s.inFlight, taskID)
	if w, ok := s.workers[t.workerID]; ok && w.inFlight > 0 {
		w.inFlight--
	}
}

// workerFor returns the ID of the worker a task was dispatched to, or 0.
func (s *Server) workerFor(taskID uuid.UUID) uint32 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.inFlight[taskID].workerID
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

	before, err := s.db.CancelTask(ctx, taskID)
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

// UpdateTaskStatus handles task status updates from workers.
func (s *Server) UpdateTaskStatus(ctx context.Context, req *grpcapi.UpdateTaskStatusRequest) (*grpcapi.UpdateTaskStatusResponse, error) {
	taskID, err := uuid.Parse(req.TaskId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid task ID")
	}
	// Finish state transitions even if the worker's RPC deadline passes.
	ctx = context.WithoutCancel(ctx)
	log := s.log.With("task_id", taskID)

	shouldRetry := false
	switch req.Status {
	case grpcapi.TaskStatus_STARTED:
		var updated bool
		updated, err = s.db.MarkTaskStarted(ctx, taskID, int64(s.workerFor(taskID)))
		if err == nil && !updated {
			log.Info("Ignoring STARTED report: task is no longer dispatched")
		}

	case grpcapi.TaskStatus_COMPLETE:
		s.releaseTask(taskID)
		var r db.TaskResult
		r, err = s.db.MarkTaskCompleted(ctx, taskID, req.Output, req.Outputs)
		if err == nil {
			if r.Updated {
				log.Info("Task completed")
				if r.WorkflowID != nil {
					s.reconcile(ctx, *r.WorkflowID)
				}
			} else {
				log.Info("Ignoring COMPLETE report: task is no longer dispatched")
			}
		}
		s.wakeDispatcher()

	case grpcapi.TaskStatus_FAILED:
		s.releaseTask(taskID)
		shouldRetry, err = s.failTask(ctx, taskID, req.Output, req.ErrorMessage)
		s.wakeDispatcher()

	default:
		return nil, status.Errorf(codes.InvalidArgument, "unexpected status %v", req.Status)
	}

	if err != nil {
		log.Error("Failed to update task status", "error", err)
		return nil, status.Error(codes.Internal, "failed to update task status")
	}
	return &grpcapi.UpdateTaskStatusResponse{ShouldRetry: shouldRetry}, nil
}

// failTask records a failed attempt. The task is requeued if it has retries
// left; otherwise it is marked FAILED and its workflow, if any, advances
// (which starts compensation). It returns whether the task will be retried.
func (s *Server) failTask(ctx context.Context, taskID uuid.UUID, output, errMsg string) (bool, error) {
	log := s.log.With("task_id", taskID)

	retrying, err := s.db.RetryTask(ctx, taskID, output, errMsg)
	if err != nil {
		return false, err
	}
	if retrying {
		log.Warn("Task failed, will retry", "error", errMsg)
		return true, nil
	}

	r, err := s.db.MarkTaskFailed(ctx, taskID, output, errMsg)
	if err != nil {
		return false, err
	}
	if !r.Updated {
		log.Info("Ignoring failure: task is no longer dispatched")
		return false, nil
	}
	log.Error("Task failed permanently", "error", errMsg)
	if r.WorkflowID != nil {
		s.reconcile(ctx, *r.WorkflowID)
	}
	return false, nil
}
