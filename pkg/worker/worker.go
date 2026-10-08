// Package worker executes tasks dispatched by the coordinator and reports
// their results back.
package worker

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"maps"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/ChinmayNoob/conductor/pkg/grpcapi"
	"github.com/ChinmayNoob/conductor/pkg/metrics"
	"github.com/ChinmayNoob/conductor/pkg/tracing"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

var (
	errCancelled = errors.New("task cancelled")
	errShutdown  = errors.New("worker shutting down")
)

const (
	heartbeatInterval = 10 * time.Second
	// How long a worker keeps trying to report a result, e.g. while a new
	// coordinator is being elected.
	reportTimeout = 30 * time.Second
)

// Label keys a worker sets automatically to advertise the task types it can
// run. Tasks require the label for their type.
const (
	LabelShell     = "type.shell"
	LabelHTTP      = "type.http"
	LabelContainer = "type.container"
)

type Server struct {
	grpcapi.UnimplementedWorkerServiceServer

	id          uint32
	address     string
	slots       int
	labels      map[string]string
	maxOutput   int
	env         []string // base environment for tasks
	docker      *docker  // nil if containers aren't available
	coordinator grpcapi.CoordinatorServiceClient
	log         *slog.Logger

	// tasksCtx is the parent of every task's context; cancelling it kills all
	// running tasks when a graceful shutdown runs out of time.
	tasksCtx  context.Context
	killTasks context.CancelCauseFunc

	mu       sync.Mutex
	running  map[string]*runningTask
	draining bool
	wg       sync.WaitGroup
}

type Options struct {
	ID        uint32
	Address   string            // host:port the coordinator dials
	Slots     int               // tasks run concurrently
	Labels    map[string]string // user labels, e.g. region=eu
	MaxOutput int               // bytes of output kept per task
	// PassEnv names worker environment variables that tasks may see. Nothing
	// else from the worker's environment is passed on.
	PassEnv []string
	// DockerSocket enables the container executor if the daemon answers.
	DockerSocket string
	Coordinator  grpcapi.CoordinatorServiceClient
}

func NewServer(opts Options) *Server {
	ctx, kill := context.WithCancelCause(context.Background())
	s := &Server{
		id:          opts.ID,
		address:     opts.Address,
		slots:       max(opts.Slots, 1),
		maxOutput:   opts.MaxOutput,
		env:         baseEnv(opts.PassEnv),
		coordinator: opts.Coordinator,
		log:         slog.Default(),
		tasksCtx:    ctx,
		killTasks:   kill,
		running:     make(map[string]*runningTask),
	}

	s.labels = maps.Clone(opts.Labels)
	if s.labels == nil {
		s.labels = make(map[string]string)
	}
	s.labels[LabelShell] = "true"
	s.labels[LabelHTTP] = "true"
	if opts.DockerSocket != "" {
		d := newDocker(opts.DockerSocket)
		if err := d.Ping(context.Background()); err != nil {
			s.log.Info("Container tasks disabled: Docker is not reachable", "socket", opts.DockerSocket, "error", err)
		} else {
			s.docker = d
			s.labels[LabelContainer] = "true"
		}
	}
	return s
}

// Labels returns the labels the worker advertises.
func (s *Server) Labels() map[string]string { return maps.Clone(s.labels) }

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

func (s *Server) SubmitTask(rpcCtx context.Context, req *grpcapi.TaskRequest) (*grpcapi.TaskResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	reject := func(msg string) (*grpcapi.TaskResponse, error) {
		return &grpcapi.TaskResponse{TaskId: req.TaskId, Message: msg}, nil
	}
	switch {
	case s.draining:
		return reject("worker is draining")
	case len(s.running) >= s.slots:
		return reject("no free slot")
	case s.running[req.TaskId] != nil:
		return reject("task is already running here")
	case req.Type == "container" && s.docker == nil:
		return reject("container tasks are not available on this worker")
	}

	ctx, cancel := context.WithCancelCause(s.tasksCtx)
	s.running[req.TaskId] = &runningTask{cancel: cancel, attempt: req.Attempt, out: newCappedBuffer(s.maxOutput)}
	s.wg.Add(1)
	// The task outlives this call; keep its trace.
	go s.run(trace.ContextWithSpanContext(ctx, trace.SpanContextFromContext(rpcCtx)), req)

	return &grpcapi.TaskResponse{TaskId: req.TaskId, Message: "accepted", Success: true}, nil
}

func (s *Server) CancelTask(_ context.Context, req *grpcapi.CancelTaskRequest) (*grpcapi.CancelTaskResponse, error) {
	s.mu.Lock()
	rt, ok := s.running[req.TaskId]
	s.mu.Unlock()
	if ok {
		s.log.Info("Cancelling task", "task_id", req.TaskId)
		rt.cancel(errCancelled)
	}
	return &grpcapi.CancelTaskResponse{Cancelled: ok}, nil
}

// runningTask is a task this worker is running.
type runningTask struct {
	cancel  context.CancelCauseFunc
	attempt int32
	out     *cappedBuffer // shell output, readable while it runs
}

// GetTaskOutput returns a running task's output so far. Only shell tasks
// produce output while running; the others report it when they finish.
func (s *Server) GetTaskOutput(_ context.Context, req *grpcapi.TaskOutputRequest) (*grpcapi.TaskOutputResponse, error) {
	s.mu.Lock()
	rt, ok := s.running[req.TaskId]
	s.mu.Unlock()
	if !ok || rt.attempt != req.Attempt {
		return &grpcapi.TaskOutputResponse{Running: false, NextOffset: req.Offset}, nil
	}
	data, next := rt.out.ReadFrom(req.Offset)
	return &grpcapi.TaskOutputResponse{Running: true, Data: data, NextOffset: next}, nil
}

func (s *Server) run(ctx context.Context, task *grpcapi.TaskRequest) {
	defer s.wg.Done()
	// Free the slot before reporting the result: the coordinator counts the
	// slot free as soon as the report arrives and may send the next task
	// right away.
	release := func() {
		s.mu.Lock()
		delete(s.running, task.TaskId)
		s.mu.Unlock()
	}
	defer release()

	ctx, span := tracing.Start(ctx, "run "+taskType(task.Type), trace.WithAttributes(
		attribute.String("conductor.task_id", task.TaskId), attribute.Int("conductor.attempt", int(task.Attempt))))
	defer span.End()

	log := s.log.With("task_id", task.TaskId, "type", task.Type, "attempt", task.Attempt)
	// No STARTED report: the coordinator records the start when this worker
	// accepts the task, which keeps a round trip off every task's path.
	log.Info("Task started")

	timeout := time.Duration(task.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 5 * time.Minute
	}
	env := maps.Clone(task.Env)
	if env == nil {
		env = make(map[string]string)
	}
	env["CONDUCTOR_TASK_ID"] = task.TaskId
	// A unique number per dispatch: use TASK_ID + ATTEMPT to de-duplicate
	// side effects, since a task can run more than once.
	env["CONDUCTOR_ATTEMPT"] = strconv.Itoa(int(task.Attempt))
	env["CONDUCTOR_RETRY"] = strconv.Itoa(int(task.RetryCount))
	// User code can continue the trace (W3C Trace Context).
	if tp := tracing.TraceParent(ctx); tp != "" {
		env["TRACEPARENT"] = tp
	}

	start := time.Now()
	var (
		output  string
		outputs map[string]string
		err     error
	)
	switch task.Type {
	case "", "shell":
		output, outputs, err = runShell(ctx, task.Data, taskEnv(s.env, env), timeout, s.outputOf(task.TaskId))
	case "http":
		output, outputs, err = runHTTP(ctx, task.SpecJson, timeout, s.maxOutput)
	case "container":
		output, err = s.docker.run(ctx, task.TaskId, task.SpecJson, taskEnv(nil, env), timeout, s.maxOutput)
	default:
		err = fmt.Errorf("unknown task type %q", task.Type)
	}

	if err != nil {
		if cause := context.Cause(ctx); cause != nil {
			err = cause
		}
		log.Warn("Task failed", "error", err, "duration", time.Since(start))
		metrics.WorkerTaskDuration.WithLabelValues(taskType(task.Type), "failed").Observe(time.Since(start).Seconds())
		span.SetStatus(codes.Error, err.Error())
		release()
		s.report(ctx, task, grpcapi.TaskStatus_FAILED, output, err.Error(), nil)
		return
	}
	log.Info("Task completed", "duration", time.Since(start))
	metrics.WorkerTaskDuration.WithLabelValues(taskType(task.Type), "completed").Observe(time.Since(start).Seconds())
	release()
	s.report(ctx, task, grpcapi.TaskStatus_COMPLETE, output, "", outputs)
}

func (s *Server) report(ctx context.Context, task *grpcapi.TaskRequest, status grpcapi.TaskStatus, output, errMsg string, outputs map[string]string) {
	taskID := task.TaskId
	// Results must be reported even if the task was cancelled or the worker
	// is shutting down, so keep only ctx's trace. Allow long enough to ride
	// out a coordinator failover.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), reportTimeout)
	defer cancel()

	resp, err := s.coordinator.UpdateTaskStatus(ctx, &grpcapi.UpdateTaskStatusRequest{
		TaskId:       taskID,
		Status:       status,
		Output:       output,
		ErrorMessage: errMsg,
		Outputs:      outputs,
		Attempt:      task.Attempt,
	})
	if err != nil {
		s.log.Error("Failed to report task status", "task_id", taskID, "status", status, "error", err)
		metrics.WorkerReportFailures.Inc()
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

func taskType(t string) string {
	if t == "" {
		return "shell"
	}
	return t
}

// outputOf returns the live output buffer of a running task.
func (s *Server) outputOf(taskID string) *cappedBuffer {
	s.mu.Lock()
	defer s.mu.Unlock()
	if rt, ok := s.running[taskID]; ok {
		return rt.out
	}
	return newCappedBuffer(s.maxOutput)
}

// Collector reports this worker's slots and running tasks when scraped.
func (s *Server) Collector() prometheus.Collector { return workerCollector{s} }

type workerCollector struct{ s *Server }

var (
	workerSlotsDesc   = prometheus.NewDesc("conductor_worker_slots", "Tasks this worker can run at once.", nil, nil)
	workerRunningDesc = prometheus.NewDesc("conductor_worker_tasks_running", "Tasks running on this worker.", nil, nil)
)

func (c workerCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- workerSlotsDesc
	ch <- workerRunningDesc
}

func (c workerCollector) Collect(ch chan<- prometheus.Metric) {
	c.s.mu.Lock()
	running := len(c.s.running)
	c.s.mu.Unlock()
	ch <- prometheus.MustNewConstMetric(workerSlotsDesc, prometheus.GaugeValue, float64(c.s.slots))
	ch <- prometheus.MustNewConstMetric(workerRunningDesc, prometheus.GaugeValue, float64(running))
}

func (s *Server) sendHeartbeat(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	s.mu.Lock()
	draining, running := s.draining, len(s.running)
	s.mu.Unlock()

	_, err := s.coordinator.SendHeartbeat(ctx, &grpcapi.HeartbeatRequest{
		WorkerId: s.id,
		Address:  s.address,
		Draining: draining,
		Slots:    int32(s.slots),
		Running:  int32(running),
		Labels:   s.labels,
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
