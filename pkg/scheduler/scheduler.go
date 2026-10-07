package scheduler

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ChinmayNoob/conductor/pkg/db"
	"github.com/ChinmayNoob/conductor/pkg/grpcapi"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Server struct {
	db                *db.DB
	coordinatorClient grpcapi.CoordinatorServiceClient
	coordinatorConn   *grpc.ClientConn
	stopCleanup       chan struct{}
}

func NewServer(database *db.DB, coordinatorAddr string) (*Server, error) {
	s := &Server{
		db:          database,
		stopCleanup: make(chan struct{}),
	}

	// Optionally connect to coordinator for task submission
	if coordinatorAddr != "" {
		conn, err := grpc.NewClient(coordinatorAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			log.Printf("Warning: Could not connect to coordinator: %v", err)
		} else {
			s.coordinatorClient = grpcapi.NewCoordinatorServiceClient(conn)
			s.coordinatorConn = conn
			log.Printf("Connected to coordinator at %s", coordinatorAddr)
		}
	}

	// Start background cleanup of stale tasks
	go s.cleanupLoop()

	return s, nil
}

func (s *Server) Close() {
	close(s.stopCleanup)
	if s.coordinatorConn != nil {
		s.coordinatorConn.Close()
	}
}

func (s *Server) ScheduleTask(data string) (string, error) {
	return s.ScheduleTaskWithOptions(data, db.DefaultTaskOptions())
}

func (s *Server) ScheduleTaskWithOptions(data string, opts db.TaskOptions) (string, error) {
	// If connected to coordinator, submit via gRPC
	if s.coordinatorClient != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		var scheduledAt int64
		if !opts.ScheduledAt.IsZero() {
			scheduledAt = opts.ScheduledAt.Unix()
		}

		resp, err := s.coordinatorClient.SubmitTask(ctx, &grpcapi.ClientTaskRequest{
			Data:              data,
			Priority:          int32(opts.Priority),
			MaxRetries:        int32(opts.MaxRetries),
			RetryDelaySeconds: int32(opts.RetryDelaySeconds),
			TimeoutSeconds:    int32(opts.TimeoutSeconds),
			ScheduledAt:       scheduledAt,
		})
		if err != nil {
			return "", err
		}
		return resp.TaskId, nil
	}

	// Otherwise, submit directly to database
	task, err := s.db.CreateTaskWithOptions(data, opts)
	if err != nil {
		return "", err
	}
	return task.ID.String(), nil
}

func (s *Server) ScheduleDelayedTask(data string, delay time.Duration) (string, error) {
	opts := db.DefaultTaskOptions()
	opts.ScheduledAt = time.Now().UTC().Add(delay)
	return s.ScheduleTaskWithOptions(data, opts)
}

func (s *Server) ScheduleTaskAt(data string, scheduledAt time.Time) (string, error) {
	opts := db.DefaultTaskOptions()
	opts.ScheduledAt = scheduledAt.UTC()
	return s.ScheduleTaskWithOptions(data, opts)
}

func (s *Server) GetTaskStatus(taskID string) (*db.Task, error) {
	id, err := uuid.Parse(taskID)
	if err != nil {
		return nil, err
	}
	return s.db.GetTask(id)
}

func (s *Server) ListPendingTasks(limit int) ([]*db.Task, error) {
	return s.db.ListTasksByStatus(db.StatusQueued, limit)
}

func (s *Server) ListAllTasks(status db.TaskStatus, limit int) ([]*db.Task, error) {
	return s.db.ListTasksByStatus(status, limit)
}

func (s *Server) SubmitWorkflow(wfType, inputJSON string) (string, error) {
	if s.coordinatorClient == nil {
		return "", fmt.Errorf("coordinator not connected")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := s.coordinatorClient.SubmitWorkflow(ctx, &grpcapi.WorkflowRequest{
		WorkflowType: wfType,
		InputJson:    inputJSON,
	})
	if err != nil {
		return "", err
	}
	if !resp.Success {
		return "", fmt.Errorf("%s", resp.Message)
	}
	return resp.WorkflowId, nil
}

func (s *Server) GetWorkflowStatus(workflowID string) (*grpcapi.WorkflowStatusResponse, error) {
	if s.coordinatorClient == nil {
		return nil, fmt.Errorf("coordinator not connected")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	return s.coordinatorClient.GetWorkflowStatus(ctx, &grpcapi.WorkflowStatusRequest{
		WorkflowId: workflowID,
	})
}

func (s *Server) cleanupLoop() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCleanup:
			return
		case <-ticker.C:
			// Reset tasks that were picked but never processed (stale for > 5 minutes)
			count, err := s.db.ResetStaleTasks(5 * time.Minute)
			if err != nil {
				log.Printf("Error resetting stale tasks: %v", err)
			} else if count > 0 {
				log.Printf("Reset %d stale tasks", count)
			}
		}
	}
}
