// Package coordinator is the brain of the cluster: it tracks workers through
// heartbeats, dispatches queued tasks to them, handles retries, and drives
// saga workflows.
package coordinator

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/ChinmayNoob/conductor/pkg/db"
	"github.com/ChinmayNoob/conductor/pkg/grpcapi"
	"github.com/ChinmayNoob/conductor/pkg/workflow"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// Workers run one task at a time, so only hand each worker one task at a
	// time. Extra tasks would wait in the worker's local queue, where the
	// stale-task cleanup could mistake them for lost tasks and run them twice.
	maxTasksPerWorker = 1

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
	inFlight  int
	client    grpcapi.WorkerServiceClient
	conn      *grpc.ClientConn
}

// dispatchedTask records which worker a task was handed to, so the worker's
// slot can be freed and its tasks recovered if it dies.
type dispatchedTask struct {
	workerID uint32
	deadline time.Time
}

type Server struct {
	grpcapi.UnimplementedCoordinatorServiceServer

	db       *db.DB
	registry *workflow.Registry
	dialOpts []grpc.DialOption
	log      *slog.Logger

	mu           sync.RWMutex
	workers      map[uint32]*Worker
	inFlight     map[uuid.UUID]dispatchedTask
	lastWorkerID uint32
	wake         chan struct{}
}

// NewServer creates a coordinator. dialOpts are used to connect to workers.
// Call Run to start dispatching.
func NewServer(database *db.DB, dialOpts []grpc.DialOption) *Server {
	return &Server{
		db:       database,
		registry: workflow.NewRegistry(),
		dialOpts: dialOpts,
		log:      slog.Default(),
		workers:  make(map[uint32]*Worker),
		inFlight: make(map[uuid.UUID]dispatchedTask),
		wake:     make(chan struct{}, 1),
	}
}

// Run dispatches tasks and recovers lost ones until ctx is cancelled, then
// closes worker connections.
func (s *Server) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, loop := range []func(context.Context){s.dispatchLoop, s.recoveryLoop} {
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

	s.mu.Lock()
	for id, worker := range s.workers {
		if worker.IsHealthy && now.Sub(worker.LastSeen) > workerTimeout {
			s.log.Warn("Worker missed heartbeats, marking unhealthy", "worker_id", id)
			worker.IsHealthy = false
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

// dispatchTasks hands out queued tasks until the queue is empty or every
// worker is busy.
func (s *Server) dispatchTasks(ctx context.Context) {
	for ctx.Err() == nil {
		// Find a free worker first so tasks aren't picked only to be put back.
		worker := s.getNextAvailableWorker()
		if worker == nil {
			return
		}

		task, err := s.db.PickNextTask(ctx)
		if err != nil {
			s.log.Error("Failed to pick next task", "error", err)
			return
		}
		if task == nil {
			return
		}

		if !s.dispatchTask(ctx, worker, task) {
			return
		}
	}
}

func (s *Server) dispatchTask(ctx context.Context, worker *Worker, task *db.Task) bool {
	log := s.log.With("task_id", task.ID, "worker_id", worker.ID)
	log.Debug("Dispatching task", "priority", task.Priority, "attempt", task.RetryCount)

	// Track the task before sending it: the worker may report back before the
	// RPC below returns.
	s.trackTask(task.ID, worker, time.Duration(task.TimeoutSeconds)*time.Second)

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	resp, err := worker.client.SubmitTask(ctx, &grpcapi.TaskRequest{
		TaskId:         task.ID.String(),
		Data:           task.Data,
		TimeoutSeconds: int32(task.TimeoutSeconds),
		RetryCount:     int32(task.RetryCount),
	})

	if err != nil {
		log.Warn("Failed to dispatch task, marking worker unhealthy", "error", err)
		s.releaseTask(task.ID)
		s.requeueTask(task.ID)
		s.mu.Lock()
		worker.IsHealthy = false
		s.mu.Unlock()
		return false
	}
	if !resp.Success {
		log.Info("Worker rejected task", "reason", resp.Message)
		s.releaseTask(task.ID)
		s.requeueTask(task.ID)
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

// getNextAvailableWorker returns the next healthy worker with a free slot,
// going round-robin in worker ID order.
func (s *Server) getNextAvailableWorker() *Worker {
	s.mu.Lock()
	defer s.mu.Unlock()

	var ids []uint32
	for id, w := range s.workers {
		if w.IsHealthy && !w.Draining && w.client != nil && w.inFlight < maxTasksPerWorker {
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

func (s *Server) SubmitTask(ctx context.Context, req *grpcapi.ClientTaskRequest) (*grpcapi.ClientTaskResponse, error) {
	opts := db.DefaultTaskOptions()
	if req.Priority > 0 && req.Priority <= 10 {
		opts.Priority = int(req.Priority)
	}
	if req.MaxRetries > 0 {
		opts.MaxRetries = int(req.MaxRetries)
	}
	if req.RetryDelaySeconds > 0 {
		opts.RetryDelaySeconds = int(req.RetryDelaySeconds)
	}
	if req.TimeoutSeconds > 0 {
		opts.TimeoutSeconds = int(req.TimeoutSeconds)
	}
	if req.ScheduledAt > 0 {
		opts.ScheduledAt = time.Unix(req.ScheduledAt, 0).UTC()
	}

	task, err := s.db.CreateTask(ctx, req.Data, opts)
	if err != nil {
		s.log.Error("Failed to create task", "error", err)
		return nil, status.Error(codes.Internal, "failed to create task")
	}
	s.log.Info("Task created", "task_id", task.ID, "priority", task.Priority, "scheduled_at", task.ScheduledAt)
	s.wakeDispatcher()

	return &grpcapi.ClientTaskResponse{TaskId: task.ID.String()}, nil
}

// CancelTask cancels a task. A queued task is simply never dispatched; a
// running one is killed on its worker.
func (s *Server) CancelTask(ctx context.Context, req *grpcapi.CancelTaskRequest) (*grpcapi.CancelTaskResponse, error) {
	taskID, err := uuid.Parse(req.TaskId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid task ID")
	}
	cancelled, err := s.cancelTask(ctx, taskID)
	if err != nil {
		return nil, err
	}
	return &grpcapi.CancelTaskResponse{Cancelled: cancelled}, nil
}

func (s *Server) cancelTask(ctx context.Context, taskID uuid.UUID) (bool, error) {
	before, err := s.db.CancelTask(ctx, taskID)
	if err != nil {
		s.log.Error("Failed to cancel task", "task_id", taskID, "error", err)
		return false, status.Error(codes.Internal, "failed to cancel task")
	}
	if before == nil {
		t, err := s.db.GetTask(ctx, taskID)
		if err != nil {
			return false, status.Error(codes.Internal, "failed to look up task")
		}
		if t == nil {
			return false, status.Error(codes.NotFound, "task not found")
		}
		return false, nil // already finished
	}

	s.log.Info("Task cancelled", "task_id", taskID, "was", before.Status)
	if before.PickedAt != nil {
		s.killOnWorker(ctx, taskID)
	}
	s.handleWorkflowTaskFailed(context.WithoutCancel(ctx), taskID, "cancelled")
	return true, nil
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

func (s *Server) SendHeartbeat(ctx context.Context, req *grpcapi.HeartbeatRequest) (*grpcapi.HeartbeatResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	worker, exists := s.workers[req.WorkerId]
	if !exists || worker.Address != req.Address {
		conn, err := grpc.NewClient(req.Address, s.dialOpts...)
		if err != nil {
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
		s.log.Info("Worker registered", "worker_id", req.WorkerId, "address", req.Address)
	}

	if req.Draining && !worker.Draining {
		s.log.Info("Worker is draining", "worker_id", req.WorkerId)
	}
	worker.LastSeen = time.Now()
	worker.IsHealthy = true
	worker.Draining = req.Draining

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
		updated, err = s.db.MarkTaskStarted(ctx, taskID)
		if err == nil && !updated {
			log.Info("Ignoring STARTED report: task is no longer dispatched")
		}

	case grpcapi.TaskStatus_COMPLETE:
		s.releaseTask(taskID)
		var updated bool
		updated, err = s.db.MarkTaskCompleted(ctx, taskID, req.Output)
		if err == nil {
			if updated {
				log.Info("Task completed")
				s.handleWorkflowTaskComplete(ctx, taskID)
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
// left; otherwise it is marked FAILED and its workflow, if any, is compensated.
// It returns whether the task will be retried.
func (s *Server) failTask(ctx context.Context, taskID uuid.UUID, output, errMsg string) (bool, error) {
	log := s.log.With("task_id", taskID)

	retrying, err := s.db.RetryTask(ctx, taskID, errMsg)
	if err != nil {
		return false, err
	}
	if retrying {
		log.Warn("Task failed, will retry", "error", errMsg)
		return true, nil
	}

	failed, err := s.db.MarkTaskFailed(ctx, taskID, output, errMsg)
	if err != nil {
		return false, err
	}
	if !failed {
		log.Info("Ignoring failure: task is no longer dispatched")
		return false, nil
	}
	log.Error("Task failed permanently", "error", errMsg)
	s.handleWorkflowTaskFailed(ctx, taskID, errMsg)
	return false, nil
}
