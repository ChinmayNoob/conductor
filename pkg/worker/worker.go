package worker

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/ChinmayNoob/conductor/pkg/grpcapi"
)

type Server struct {
	grpcapi.UnimplementedWorkerServiceServer
	workerID          uint32
	address           string
	coordinatorClient grpcapi.CoordinatorServiceClient
	taskQueue         chan *grpcapi.TaskRequest
	mu                sync.Mutex
	isProcessing      bool
}

func NewServer(workerID uint32, address string, coordinatorClient grpcapi.CoordinatorServiceClient) *Server {
	s := &Server{
		workerID:          workerID,
		address:           address,
		coordinatorClient: coordinatorClient,
		taskQueue:         make(chan *grpcapi.TaskRequest, 100),
	}

	go s.processTasksLoop()

	go s.sendHeartbeatLoop()

	return s
}

func (s *Server) SubmitTask(ctx context.Context, req *grpcapi.TaskRequest) (*grpcapi.TaskResponse, error) {
	log.Printf("Received task: ID=%s, Data=%s, Timeout=%ds, Retry=%d",
		req.TaskId, req.Data, req.TimeoutSeconds, req.RetryCount)

	// Queue the task for processing
	select {
	case s.taskQueue <- req:
		return &grpcapi.TaskResponse{
			TaskId:  req.TaskId,
			Message: "Task accepted",
			Success: true,
		}, nil
	default:
		return &grpcapi.TaskResponse{
			TaskId:  req.TaskId,
			Message: "Worker queue is full",
			Success: false,
		}, nil
	}
}

func (s *Server) processTasksLoop() {
	for task := range s.taskQueue {
		s.processTask(task)
	}
}

func (s *Server) processTask(task *grpcapi.TaskRequest) {
	log.Printf("Processing task: ID=%s (retry=%d)", task.TaskId, task.RetryCount)

	// Update status to STARTED
	s.updateTaskStatus(task.TaskId, grpcapi.TaskStatus_STARTED, time.Now().Unix(), 0, 0, "", "")

	// Determine timeout (use provided or default to 5 minutes)
	timeout := time.Duration(task.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 5 * time.Minute
	}

	// Execute the command with timeout
	output, err := s.executeCommandWithTimeout(task.Data, timeout)

	if err != nil {
		errorMsg := err.Error()
		log.Printf("Task %s failed: %v", task.TaskId, err)
		s.updateTaskStatus(task.TaskId, grpcapi.TaskStatus_FAILED, 0, 0, time.Now().Unix(), output, errorMsg)
	} else {
		log.Printf("Task %s completed successfully", task.TaskId)
		s.updateTaskStatus(task.TaskId, grpcapi.TaskStatus_COMPLETE, 0, time.Now().Unix(), 0, output, "")
	}
}

func (s *Server) executeCommandWithTimeout(command string, timeout time.Duration) (string, error) {
	log.Printf("Executing command (timeout=%v): %s", timeout, command)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Use cmd.exe on Windows, sh on Unix
	var cmd *exec.Cmd
	if isWindows() {
		cmd = exec.CommandContext(ctx, "cmd", "/C", command)
	} else {
		cmd = exec.CommandContext(ctx, "sh", "-c", command)
	}

	// Capture output
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	// Also print to console for debugging
	cmd.Stdout = &combinedWriter{&stdout, os.Stdout}
	cmd.Stderr = &combinedWriter{&stderr, os.Stderr}

	err := cmd.Run()

	// Combine stdout and stderr
	output := stdout.String()
	if stderr.Len() > 0 {
		output += "\n[stderr]: " + stderr.String()
	}

	// Check for timeout
	if ctx.Err() == context.DeadlineExceeded {
		return output, fmt.Errorf("task timed out after %v", timeout)
	}

	return output, err
}

type combinedWriter struct {
	buffer  *bytes.Buffer
	console *os.File
}

func (w *combinedWriter) Write(p []byte) (n int, err error) {
	w.buffer.Write(p)
	return w.console.Write(p)
}

func (s *Server) updateTaskStatus(taskID string, status grpcapi.TaskStatus, startedAt, completedAt, failedAt int64, output, errorMessage string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &grpcapi.UpdateTaskStatusRequest{
		TaskId:       taskID,
		Status:       status,
		StartedAt:    startedAt,
		CompletedAt:  completedAt,
		FailedAt:     failedAt,
		Output:       output,
		ErrorMessage: errorMessage,
	}

	resp, err := s.coordinatorClient.UpdateTaskStatus(ctx, req)
	if err != nil {
		log.Printf("Failed to update task status: %v", err)
		return
	}

	if !resp.Success {
		log.Printf("Coordinator rejected status update for task %s", taskID)
	}

	// Log retry information
	if status == grpcapi.TaskStatus_FAILED && resp.ShouldRetry {
		log.Printf("Task %s will be retried by coordinator", taskID)
	}
}

func (s *Server) sendHeartbeatLoop() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	// Send initial heartbeat immediately
	s.sendHeartbeat()

	for range ticker.C {
		s.sendHeartbeat()
	}
}

func (s *Server) sendHeartbeat() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &grpcapi.HeartbeatRequest{
		WorkerId: s.workerID,
		Address:  s.address,
	}

	resp, err := s.coordinatorClient.SendHeartbeat(ctx, req)
	if err != nil {
		log.Printf("Failed to send heartbeat: %v", err)
		return
	}

	if resp.Acknowledged {
		log.Printf("Heartbeat acknowledged by coordinator")
	}
}

func isWindows() bool {
	return os.PathSeparator == '\\' && os.PathListSeparator == ';'
}
