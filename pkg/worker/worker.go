// Package worker executes tasks dispatched by the coordinator and reports
// their results back.
package worker

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/ChinmayNoob/conductor/pkg/grpcapi"
)

var (
	errCancelled = errors.New("task cancelled")
	errShutdown  = errors.New("worker shutting down")
)

const heartbeatInterval = 10 * time.Second

type Server struct {
	grpcapi.UnimplementedWorkerServiceServer

	id          uint32
	address     string
	slots       int
	maxOutput   int
	coordinator grpcapi.CoordinatorServiceClient
	log         *slog.Logger

	// tasksCtx is the parent of every task's context; cancelling it kills all
	// running tasks when a graceful shutdown runs out of time.
	tasksCtx  context.Context
	killTasks context.CancelCauseFunc

	mu       sync.Mutex
	running  map[string]context.CancelCauseFunc
	draining bool
	wg       sync.WaitGroup
}

type Options struct {
	ID          uint32
	Address     string // host:port the coordinator dials
	Slots       int    // tasks run concurrently
	MaxOutput   int    // bytes of output kept per task
	Coordinator grpcapi.CoordinatorServiceClient
}

func NewServer(opts Options) *Server {
	ctx, kill := context.WithCancelCause(context.Background())
	return &Server{
		id:          opts.ID,
		address:     opts.Address,
		slots:       max(opts.Slots, 1),
		maxOutput:   opts.MaxOutput,
		coordinator: opts.Coordinator,
		log:         slog.Default(),
		tasksCtx:    ctx,
		killTasks:   kill,
		running:     make(map[string]context.CancelCauseFunc),
	}
}

// ID derives a stable worker ID from the advertised address, so replicas of
// the same service get distinct IDs without configuration.
func ID(address string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(address))
	return max(h.Sum32(), 1)
}

// LocalIP returns the IP this machine uses to reach the coordinator. Replicas
// share one service name, so each advertises its own IP instead.
func LocalIP(coordinatorAddr string) string {
	// Dialing UDP sends nothing; it only picks the outgoing interface.
	conn, err := net.Dial("udp", coordinatorAddr)
	if err != nil {
		slog.Warn("Could not determine local IP, using localhost", "error", err)
		return "localhost"
	}
	defer conn.Close()
	return conn.LocalAddr().(*net.UDPAddr).IP.String()
}

func (s *Server) SubmitTask(_ context.Context, req *grpcapi.TaskRequest) (*grpcapi.TaskResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	reject := func(msg string) (*grpcapi.TaskResponse, error) {
		return &grpcapi.TaskResponse{TaskId: req.TaskId, Message: msg}, nil
	}
	if s.draining {
		return reject("worker is draining")
	}
	if len(s.running) >= s.slots {
		return reject("no free slot")
	}
	if _, dup := s.running[req.TaskId]; dup {
		return reject("task is already running here")
	}

	ctx, cancel := context.WithCancelCause(s.tasksCtx)
	s.running[req.TaskId] = cancel
	s.wg.Add(1)
	go s.run(ctx, req)

	return &grpcapi.TaskResponse{TaskId: req.TaskId, Message: "accepted", Success: true}, nil
}

func (s *Server) CancelTask(_ context.Context, req *grpcapi.CancelTaskRequest) (*grpcapi.CancelTaskResponse, error) {
	s.mu.Lock()
	cancel, ok := s.running[req.TaskId]
	s.mu.Unlock()
	if ok {
		s.log.Info("Cancelling task", "task_id", req.TaskId)
		cancel(errCancelled)
	}
	return &grpcapi.CancelTaskResponse{Cancelled: ok}, nil
}

func (s *Server) run(ctx context.Context, task *grpcapi.TaskRequest) {
	defer s.wg.Done()
	defer func() {
		s.mu.Lock()
		delete(s.running, task.TaskId)
		s.mu.Unlock()
	}()

	log := s.log.With("task_id", task.TaskId, "attempt", task.RetryCount)
	log.Info("Task started")
	s.report(task.TaskId, grpcapi.TaskStatus_STARTED, "", "")

	timeout := time.Duration(task.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 5 * time.Minute
	}
	start := time.Now()
	output, err := runShell(ctx, task.Data, timeout, s.maxOutput)

	if err != nil {
		if cause := context.Cause(ctx); cause != nil {
			err = cause
		}
		log.Warn("Task failed", "error", err, "duration", time.Since(start))
		s.report(task.TaskId, grpcapi.TaskStatus_FAILED, output, err.Error())
		return
	}
	log.Info("Task completed", "duration", time.Since(start))
	s.report(task.TaskId, grpcapi.TaskStatus_COMPLETE, output, "")
}

func (s *Server) report(taskID string, status grpcapi.TaskStatus, output, errMsg string) {
	// Use a fresh context: results must be reported even while shutting down.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := s.coordinator.UpdateTaskStatus(ctx, &grpcapi.UpdateTaskStatusRequest{
		TaskId:       taskID,
		Status:       status,
		Output:       output,
		ErrorMessage: errMsg,
	})
	if err != nil {
		s.log.Error("Failed to report task status", "task_id", taskID, "status", status, "error", err)
		return
	}
	if resp.ShouldRetry {
		s.log.Info("Coordinator will retry task", "task_id", taskID)
	}
}

// RunHeartbeats sends heartbeats until ctx is cancelled.
func (s *Server) RunHeartbeats(ctx context.Context) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	s.sendHeartbeat(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.sendHeartbeat(ctx)
		}
	}
}

func (s *Server) sendHeartbeat(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	s.mu.Lock()
	draining := s.draining
	s.mu.Unlock()

	_, err := s.coordinator.SendHeartbeat(ctx, &grpcapi.HeartbeatRequest{
		WorkerId: s.id,
		Address:  s.address,
		Draining: draining,
	})
	if err != nil {
		s.log.Warn("Heartbeat failed", "error", err)
	}
}

// Drain stops accepting tasks and waits for running ones to finish. If ctx
// ends first, the remaining tasks are killed and reported as failed so the
// coordinator retries them elsewhere.
func (s *Server) Drain(ctx context.Context) {
	s.mu.Lock()
	s.draining = true
	n := len(s.running)
	s.mu.Unlock()

	s.log.Info("Draining", "running_tasks", n)
	// Tell the coordinator right away instead of at the next heartbeat.
	s.sendHeartbeat(context.Background())

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		s.log.Info("All tasks finished")
	case <-ctx.Done():
		s.log.Warn("Shutdown timeout reached, killing remaining tasks")
		s.killTasks(errShutdown)
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			s.log.Error("Tasks did not exit after being killed")
		}
	}
}

func (s *Server) String() string {
	return fmt.Sprintf("worker %d (%s)", s.id, s.address)
}
