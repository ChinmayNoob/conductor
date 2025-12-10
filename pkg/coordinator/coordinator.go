package coordinator

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/ChinmayNoob/conductor/pkg/db"
	"github.com/ChinmayNoob/conductor/pkg/grpcapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"github.com/google/uuid"
)

type Worker struct {
	ID        uint32
	Address   string
	LastSeen  time.Time
	IsHealthy bool
	IsBusy    bool
	client    grpcapi.WorkerServiceClient
	conn      *grpc.ClientConn
}

type Server struct {
	grpcapi.UnimplementedCoordinatorServiceServer
	db              *db.DB
	workers         map[uint32]*Worker
	mu              sync.RWMutex
	nextWorkerIndex int
	stopDispatcher  chan struct{}
}

func NewServer(database *db.DB) *Server {
	s := &Server{
		db:             database,
		workers:        make(map[uint32]*Worker),
		stopDispatcher: make(chan struct{}),
	}

	go s.checkWorkerHealth()

	go s.dispatchTasksLoop()

	return s
}

func (s *Server) Stop() {
	close(s.stopDispatcher)

	// Close all worker connections
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, w := range s.workers {
		if w.conn != nil {
			w.conn.Close()
		}
	}
}


func (s *Server) checkWorkerHealth() {
	ticker := time.NewTicker(10 * time.Second)

	defer ticker.Stop()

	for {
		select {
		case <-s.stopDispatcher:
			return
		case <-ticker.C:
			s.mu.Lock()
			for id, worker := range s.workers {
				if time.Since(worker.LastSeen) > 30*time.Second {
					if worker.IsHealthy {
						log.Printf("Worker %d is unhealthy, marking as unhealthy", id)
						worker.IsHealthy = false
					}
				}
			}
			s.mu.Unlock()
		}
	}
}

func (s *Server) dispatchTasksLoop() {
	ticker := time.NewTicker(1 * time.Second)

	defer ticker.Stop()

	for {
		select {
		case <-s.stopDispatcher:
			log.Println("Task dispatcher stopped")
			return
		case <-ticker.C:
			s.dispatchTasks()
		}
	}
}

func (s *Server) dispatchTasks() {
	task, err := s.db.PickNextTask()

	if err != nil {
		log.Printf("Error picking next task: %v", err)
		return
	}
	if task == nil {
		return
	}

	worker := s.getNextAvailableWorker()

	if worker == nil {
		log.Printf("No workers available for task %s (priority=%d), will retry",
			task.ID.String(), task.Priority)
		// Reset the task to QUEUED state
		s.db.UpdateTaskStatus(task.ID, db.StatusQueued, nil, nil, nil)
		return
	}

	log.Printf("Dispatching task %s to worker %d (priority=%d, timeout=%ds, retry=%d/%d)",
		task.ID.String(), worker.ID, task.Priority, task.TimeoutSeconds, task.RetryCount, task.MaxRetries)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()


	resp, err := worker.client.SubmitTask(ctx, &grpcapi.TaskRequest{
		TaskId:         task.ID.String(),
		Data:           task.Data,
		TimeoutSeconds: int32(task.TimeoutSeconds),
		RetryCount:     int32(task.RetryCount),
	})

	if err != nil {
		log.Printf("Failed to dispatch task %s to worker %d: %v", task.ID.String(), worker.ID, err)
		// Reset task to QUEUED
		s.db.UpdateTaskStatus(task.ID, db.StatusQueued, nil, nil, nil)
		// Mark worker as potentially unhealthy
		s.mu.Lock()
		worker.IsHealthy = false
		s.mu.Unlock()
		return
	}
	if !resp.Success {
		log.Printf("Worker %d rejected task %s: %s", worker.ID, task.ID.String(), resp.Message)
		// Reset task to QUEUED
		s.db.UpdateTaskStatus(task.ID, db.StatusQueued, nil, nil, nil)
		return
	}

	log.Printf("Task %s accepted by worker %d", task.ID.String(), worker.ID)

}

func (s *Server) getNextAvailableWorker() *Worker {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.workers) == 0 {
		return nil
	}

	var healthyWorkers []*Worker
	for _, w := range s.workers {
		if w.IsHealthy && w.client != nil {
			healthyWorkers = append(healthyWorkers, w)
		}
	}
	if len(healthyWorkers) == 0 {
		return nil
	}
	s.nextWorkerIndex = (s.nextWorkerIndex + 1) % len(healthyWorkers)
	return healthyWorkers[s.nextWorkerIndex]
}

func (s *Server) GetHealthyWorkers() []*Worker {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var healthy []*Worker
	for _, w := range s.workers {
		if w.IsHealthy {
			healthy = append(healthy, w)
		}
	}
	return healthy
}


func (s *Server) SubmitTask(ctx context.Context, req *grpcapi.ClientTaskRequest) (*grpcapi.ClientTaskResponse, error) {
	log.Printf("Received task submission: %s (priority=%d, retries=%d, timeout=%ds)",
		req.Data, req.Priority, req.MaxRetries, req.TimeoutSeconds)

	// Build task options from request
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

	// Create task in database
	task, err := s.db.CreateTaskWithOptions(req.Data, opts)
	if err != nil {
		log.Printf("Failed to create task: %v", err)
		return &grpcapi.ClientTaskResponse{
			Message: "Failed to create task: " + err.Error(),
			TaskId:  "",
		}, nil
	}

	log.Printf("Task created: ID=%s, Priority=%d, MaxRetries=%d, Timeout=%ds",
		task.ID.String(), task.Priority, task.MaxRetries, task.TimeoutSeconds)

	return &grpcapi.ClientTaskResponse{
		Message: "Task queued successfully",
		TaskId:  task.ID.String(),
	}, nil
}


func (s *Server) SendHeartbeat(ctx context.Context, req *grpcapi.HeartbeatRequest) (*grpcapi.HeartbeatResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	worker, exists := s.workers[req.WorkerId]
	if !exists {
		// New worker registration - create gRPC client connection
		conn, err := grpc.NewClient(req.Address,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			log.Printf("Failed to connect to worker %d at %s: %v", req.WorkerId, req.Address, err)
			return &grpcapi.HeartbeatResponse{Acknowledged: false}, nil
		}

		worker = &Worker{
			ID:      req.WorkerId,
			Address: req.Address,
			client:  grpcapi.NewWorkerServiceClient(conn),
			conn:    conn,
		}
		s.workers[req.WorkerId] = worker
		log.Printf("New worker registered: ID=%d, Address=%s", req.WorkerId, req.Address)
	}

	worker.LastSeen = time.Now()
	worker.IsHealthy = true

	// Update address if changed
	if worker.Address != req.Address {
		if worker.conn != nil {
			worker.conn.Close()
		}
		conn, err := grpc.NewClient(req.Address,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			log.Printf("Failed to reconnect to worker %d at %s: %v", req.WorkerId, req.Address, err)
		} else {
			worker.Address = req.Address
			worker.client = grpcapi.NewWorkerServiceClient(conn)
			worker.conn = conn
		}
	}

	return &grpcapi.HeartbeatResponse{
		Acknowledged: true,
	}, nil
}

// UpdateTaskStatus handles task status updates from workers (with retry logic)
func (s *Server) UpdateTaskStatus(ctx context.Context, req *grpcapi.UpdateTaskStatusRequest) (*grpcapi.UpdateTaskStatusResponse, error) {
	log.Printf("Task status update: ID=%s, Status=%v", req.TaskId, req.Status)

	taskID, err := uuid.Parse(req.TaskId)
	if err != nil {
		log.Printf("Invalid task ID: %v", err)
		return &grpcapi.UpdateTaskStatusResponse{Success: false}, nil
	}

	// Convert proto timestamps to time.Time pointers
	var startedAt *time.Time

	if req.StartedAt > 0 {
		t := time.Unix(req.StartedAt, 0)
		startedAt = &t
	}

	shouldRetry := false

	// Handle based on status
	switch req.Status {
	case grpcapi.TaskStatus_STARTED:
		err = s.db.UpdateTaskStatus(taskID, db.StatusStarted, startedAt, nil, nil)

	case grpcapi.TaskStatus_COMPLETE:
		err = s.db.MarkTaskCompleted(taskID, req.Output)
		log.Printf("Task %s completed successfully", req.TaskId)

	case grpcapi.TaskStatus_FAILED:
		// Check if we should retry
		canRetry, retryErr := s.db.IncrementRetryCount(taskID)
		if retryErr != nil {
			log.Printf("Error checking retry: %v", retryErr)
		}

		if canRetry {
			log.Printf("Task %s failed, will retry (error: %s)", req.TaskId, req.ErrorMessage)
			shouldRetry = true
			// Task is already rescheduled by IncrementRetryCount
		} else {
			// No more retries - mark as permanently failed
			err = s.db.MarkTaskFailed(taskID, req.ErrorMessage)
			log.Printf("Task %s failed permanently after max retries (error: %s)", req.TaskId, req.ErrorMessage)
		}

	default:
		err = s.db.UpdateTaskStatus(taskID, db.StatusQueued, nil, nil, nil)
	}

	if err != nil {
		log.Printf("Failed to update task status: %v", err)
		return &grpcapi.UpdateTaskStatusResponse{Success: false}, nil
	}

	return &grpcapi.UpdateTaskStatusResponse{
		Success:     true,
		ShouldRetry: shouldRetry,
	}, nil
}
