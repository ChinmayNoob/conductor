package coordinator

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/ChinmayNoob/conductor/pkg/db"
	"github.com/ChinmayNoob/conductor/pkg/grpcapi"
	"github.com/ChinmayNoob/conductor/pkg/workflow"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
	registry        *workflow.Registry
}

func NewServer(database *db.DB) *Server {
	s := &Server{
		db:             database,
		workers:        make(map[uint32]*Worker),
		stopDispatcher: make(chan struct{}),
		registry:       workflow.NewRegistry(),
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
		s.handleWorkflowTaskComplete(taskID)

	case grpcapi.TaskStatus_FAILED:
		canRetry, retryErr := s.db.IncrementRetryCount(taskID)
		if retryErr != nil {
			log.Printf("Error checking retry: %v", retryErr)
		}

		if canRetry {
			log.Printf("Task %s failed, will retry (error: %s)", req.TaskId, req.ErrorMessage)
			shouldRetry = true
		} else {
			err = s.db.MarkTaskFailed(taskID, req.ErrorMessage)
			log.Printf("Task %s failed permanently after max retries (error: %s)", req.TaskId, req.ErrorMessage)
			s.handleWorkflowTaskFailed(taskID, req.ErrorMessage)
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

// --- Saga / Workflow methods ---

func (s *Server) SubmitWorkflow(ctx context.Context, req *grpcapi.WorkflowRequest) (*grpcapi.WorkflowResponse, error) {
	log.Printf("Received workflow submission: type=%s", req.WorkflowType)

	def, err := s.registry.Get(req.WorkflowType)
	if err != nil {
		return &grpcapi.WorkflowResponse{Message: err.Error(), Success: false}, nil
	}

	inputJSON := json.RawMessage(req.InputJson)
	if req.InputJson == "" {
		inputJSON = json.RawMessage("{}")
	}

	wf, err := s.db.CreateWorkflow(req.WorkflowType, inputJSON, len(def.Steps))
	if err != nil {
		log.Printf("Failed to create workflow: %v", err)
		return &grpcapi.WorkflowResponse{Message: "Failed to create workflow", Success: false}, nil
	}

	for i, stepDef := range def.Steps {
		_, err := s.db.CreateWorkflowStep(wf.ID, i, stepDef.Name)
		if err != nil {
			log.Printf("Failed to create workflow step %d: %v", i, err)
			s.db.UpdateWorkflowStatus(wf.ID, db.WorkflowFailed, "failed to create steps")
			return &grpcapi.WorkflowResponse{Message: "Failed to create workflow steps", Success: false}, nil
		}
	}

	if err := s.startWorkflowStep(wf.ID, 0, inputJSON); err != nil {
		log.Printf("Failed to start first workflow step: %v", err)
		s.db.UpdateWorkflowStatus(wf.ID, db.WorkflowFailed, err.Error())
		return &grpcapi.WorkflowResponse{Message: "Failed to start workflow", Success: false}, nil
	}

	log.Printf("Workflow created: ID=%s, Type=%s, Steps=%d", wf.ID, req.WorkflowType, len(def.Steps))
	return &grpcapi.WorkflowResponse{
		WorkflowId: wf.ID.String(),
		Message:    "Workflow started successfully",
		Success:    true,
	}, nil
}

func (s *Server) GetWorkflowStatus(ctx context.Context, req *grpcapi.WorkflowStatusRequest) (*grpcapi.WorkflowStatusResponse, error) {
	wfID, err := uuid.Parse(req.WorkflowId)
	if err != nil {
		return nil, err
	}

	wf, err := s.db.GetWorkflow(wfID)
	if err != nil {
		return nil, err
	}
	if wf == nil {
		return &grpcapi.WorkflowStatusResponse{}, nil
	}

	steps, err := s.db.GetWorkflowSteps(wfID)
	if err != nil {
		return nil, err
	}

	var stepInfos []*grpcapi.WorkflowStepInfo
	for _, step := range steps {
		info := &grpcapi.WorkflowStepInfo{
			StepNumber: int32(step.StepNumber),
			Name:       step.Name,
			Status:     string(step.Status),
		}
		if step.TaskID != nil {
			info.TaskId = step.TaskID.String()
		}
		if step.CompensationTaskID != nil {
			info.CompensationTaskId = step.CompensationTaskID.String()
		}
		stepInfos = append(stepInfos, info)
	}

	return &grpcapi.WorkflowStatusResponse{
		WorkflowId:   wf.ID.String(),
		WorkflowType: wf.Type,
		Status:       string(wf.Status),
		CurrentStep:  int32(wf.CurrentStep),
		Context:      string(wf.Context),
		ErrorMessage: wf.ErrorMessage,
		Steps:        stepInfos,
		CreatedAt:    wf.CreatedAt.Format("2006-01-02T15:04:05Z07:00"),
		UpdatedAt:    wf.UpdatedAt.Format("2006-01-02T15:04:05Z07:00"),
	}, nil
}

func (s *Server) startWorkflowStep(workflowID uuid.UUID, stepNumber int, ctx json.RawMessage) error {
	wf, err := s.db.GetWorkflow(workflowID)
	if err != nil {
		return err
	}

	def, err := s.registry.Get(wf.Type)
	if err != nil {
		return err
	}
	if stepNumber >= len(def.Steps) {
		return nil
	}

	steps, err := s.db.GetWorkflowSteps(workflowID)
	if err != nil {
		return err
	}

	var step *db.WorkflowStep
	for _, s := range steps {
		if s.StepNumber == stepNumber {
			step = s
			break
		}
	}
	if step == nil {
		return nil
	}

	command := workflow.ExpandCommand(def.Steps[stepNumber].CommandTemplate, ctx)

	opts := db.DefaultTaskOptions()
	opts.MaxRetries = 2
	opts.RetryDelaySeconds = 5
	opts.TimeoutSeconds = 60

	task, err := s.db.CreateTaskWithOptions(command, opts)
	if err != nil {
		return err
	}

	if err := s.db.LinkTaskToStep(step.ID, task.ID); err != nil {
		return err
	}
	if err := s.db.UpdateStepStatus(step.ID, db.StepRunning); err != nil {
		return err
	}
	if err := s.db.AdvanceWorkflowStep(workflowID, stepNumber); err != nil {
		return err
	}

	log.Printf("Workflow %s: started step %d (%s) -> task %s",
		workflowID, stepNumber, def.Steps[stepNumber].Name, task.ID)
	return nil
}

func (s *Server) handleWorkflowTaskComplete(taskID uuid.UUID) {
	step, err := s.db.GetStepByTaskID(taskID)
	if err != nil || step == nil {
		return
	}

	wf, err := s.db.GetWorkflow(step.WorkflowID)
	if err != nil || wf == nil {
		return
	}

	if step.CompensationTaskID != nil && *step.CompensationTaskID == taskID {
		s.handleCompensationComplete(wf, step)
		return
	}

	s.db.UpdateStepStatus(step.ID, db.StepCompleted)

	def, err := s.registry.Get(wf.Type)
	if err != nil {
		return
	}

	nextStep := step.StepNumber + 1
	if nextStep >= len(def.Steps) {
		s.db.UpdateWorkflowStatus(wf.ID, db.WorkflowCompleted, "")
		log.Printf("Workflow %s COMPLETED (all %d steps done)", wf.ID, len(def.Steps))
		return
	}

	if err := s.startWorkflowStep(wf.ID, nextStep, wf.Context); err != nil {
		log.Printf("Workflow %s: failed to start step %d: %v", wf.ID, nextStep, err)
		s.db.UpdateWorkflowStatus(wf.ID, db.WorkflowFailed, err.Error())
	}
}

func (s *Server) handleWorkflowTaskFailed(taskID uuid.UUID, errMsg string) {
	step, err := s.db.GetStepByTaskID(taskID)
	if err != nil || step == nil {
		return
	}

	wf, err := s.db.GetWorkflow(step.WorkflowID)
	if err != nil || wf == nil {
		return
	}

	if step.CompensationTaskID != nil && *step.CompensationTaskID == taskID {
		s.db.UpdateStepStatus(step.ID, db.StepFailed)
		log.Printf("Workflow %s: compensation for step %d FAILED (best-effort)", wf.ID, step.StepNumber)
		s.continueCompensation(wf)
		return
	}

	s.db.UpdateStepStatus(step.ID, db.StepFailed)
	s.db.UpdateWorkflowStatus(wf.ID, db.WorkflowCompensating, errMsg)
	log.Printf("Workflow %s: step %d FAILED, starting compensation", wf.ID, step.StepNumber)
	s.startCompensation(wf, step.StepNumber)
}

func (s *Server) startCompensation(wf *db.Workflow, failedStep int) {
	steps, err := s.db.GetWorkflowSteps(wf.ID)
	if err != nil {
		return
	}

	for i := failedStep - 1; i >= 0; i-- {
		if steps[i].Status == db.StepCompleted {
			s.runCompensationTask(wf, steps[i])
			return
		}
	}

	s.db.UpdateWorkflowStatus(wf.ID, db.WorkflowFailed, wf.ErrorMessage)
	log.Printf("Workflow %s: compensation complete, workflow FAILED", wf.ID)
}

func (s *Server) handleCompensationComplete(wf *db.Workflow, step *db.WorkflowStep) {
	s.db.UpdateStepStatus(step.ID, db.StepCompensated)
	log.Printf("Workflow %s: step %d (%s) COMPENSATED", wf.ID, step.StepNumber, step.Name)
	s.continueCompensation(wf)
}

func (s *Server) continueCompensation(wf *db.Workflow) {
	steps, err := s.db.GetWorkflowSteps(wf.ID)
	if err != nil {
		return
	}

	for i := len(steps) - 1; i >= 0; i-- {
		if steps[i].Status == db.StepCompleted {
			s.runCompensationTask(wf, steps[i])
			return
		}
	}

	s.db.UpdateWorkflowStatus(wf.ID, db.WorkflowFailed, wf.ErrorMessage)
	log.Printf("Workflow %s: all compensations done, workflow FAILED", wf.ID)
}

func (s *Server) runCompensationTask(wf *db.Workflow, step *db.WorkflowStep) {
	def, err := s.registry.Get(wf.Type)
	if err != nil || step.StepNumber >= len(def.Steps) {
		return
	}

	compensateCmd := workflow.ExpandCommand(def.Steps[step.StepNumber].CompensateTemplate, wf.Context)

	opts := db.DefaultTaskOptions()
	opts.MaxRetries = 1
	opts.RetryDelaySeconds = 3
	opts.TimeoutSeconds = 60

	task, err := s.db.CreateTaskWithOptions(compensateCmd, opts)
	if err != nil {
		log.Printf("Workflow %s: failed to create compensation task for step %d: %v", wf.ID, step.StepNumber, err)
		return
	}

	s.db.LinkCompensationTaskToStep(step.ID, task.ID)
	s.db.UpdateStepStatus(step.ID, db.StepCompensating)

	log.Printf("Workflow %s: compensating step %d (%s) -> task %s",
		wf.ID, step.StepNumber, step.Name, task.ID)
}
