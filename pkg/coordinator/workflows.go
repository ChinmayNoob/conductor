package coordinator

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/ChinmayNoob/conductor/pkg/db"
	"github.com/ChinmayNoob/conductor/pkg/grpcapi"
	"github.com/ChinmayNoob/conductor/pkg/workflow"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) SubmitWorkflow(ctx context.Context, req *grpcapi.WorkflowRequest) (*grpcapi.WorkflowResponse, error) {
	def, err := s.registry.Get(req.WorkflowType)
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}

	input := json.RawMessage(req.InputJson)
	if req.InputJson == "" {
		input = json.RawMessage("{}")
	}
	// Input values end up in shell commands, so reject unsafe ones up front.
	if _, err := workflow.ParseInput(input); err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid workflow input: "+err.Error())
	}

	var wfID uuid.UUID
	err = s.db.WithTx(ctx, func(tx *db.DB) error {
		wf, err := tx.CreateWorkflow(ctx, req.WorkflowType, input)
		if err != nil {
			return err
		}
		wfID = wf.ID
		for i, step := range def.Steps {
			if _, err := tx.CreateWorkflowStep(ctx, wf.ID, i, step.Name); err != nil {
				return err
			}
		}
		return s.startWorkflowStep(ctx, tx, wf.ID, def, 0, input)
	})
	if err != nil {
		s.log.Error("Failed to create workflow", "type", req.WorkflowType, "error", err)
		return nil, status.Error(codes.Internal, "failed to create workflow")
	}

	s.log.Info("Workflow started", "workflow_id", wfID, "type", req.WorkflowType, "steps", len(def.Steps))
	s.wakeDispatcher()
	return &grpcapi.WorkflowResponse{WorkflowId: wfID.String()}, nil
}

// CancelWorkflow stops a workflow: the running step is cancelled and every
// completed step is compensated, after which the workflow is CANCELLED.
func (s *Server) CancelWorkflow(ctx context.Context, req *grpcapi.CancelWorkflowRequest) (*grpcapi.CancelWorkflowResponse, error) {
	wfID, err := uuid.Parse(req.WorkflowId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid workflow ID")
	}

	ok, err := s.db.RequestWorkflowCancel(ctx, wfID)
	if err != nil {
		s.log.Error("Failed to cancel workflow", "workflow_id", wfID, "error", err)
		return nil, status.Error(codes.Internal, "failed to cancel workflow")
	}
	if !ok {
		wf, err := s.db.GetWorkflow(ctx, wfID)
		if err != nil {
			return nil, status.Error(codes.Internal, "failed to look up workflow")
		}
		if wf == nil {
			return nil, status.Error(codes.NotFound, "workflow not found")
		}
		return &grpcapi.CancelWorkflowResponse{Cancelled: false}, nil // already finished
	}
	s.log.Info("Workflow cancellation requested", "workflow_id", wfID)

	// Cancelling the running step triggers compensation. If the step finishes
	// first, the completion handler sees the request and compensates instead
	// of starting the next step.
	steps, err := s.db.GetWorkflowSteps(ctx, wfID)
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to load workflow steps")
	}
	for _, step := range steps {
		if step.Status == db.StepRunning && step.TaskID != nil {
			if _, err := s.cancelTask(ctx, *step.TaskID); err != nil {
				s.log.Error("Failed to cancel workflow step", "workflow_id", wfID, "step", step.Name, "error", err)
			}
		}
	}
	return &grpcapi.CancelWorkflowResponse{Cancelled: true}, nil
}

func (s *Server) startWorkflowStep(ctx context.Context, tx *db.DB, wfID uuid.UUID, def *workflow.WorkflowDefinition,
	stepNumber int, input json.RawMessage) error {
	steps, err := tx.GetWorkflowSteps(ctx, wfID)
	if err != nil {
		return err
	}
	if stepNumber >= len(steps) {
		return errors.New("step out of range")
	}
	step := steps[stepNumber]

	command, err := workflow.ExpandCommand(def.Steps[stepNumber].CommandTemplate, input)
	if err != nil {
		return err
	}

	opts := db.DefaultTaskOptions()
	opts.MaxRetries = 2
	opts.RetryDelaySeconds = 5
	opts.TimeoutSeconds = 60

	task, err := tx.CreateTask(ctx, command, opts)
	if err != nil {
		return err
	}
	if err := tx.LinkTaskToStep(ctx, step.ID, task.ID); err != nil {
		return err
	}
	if err := tx.UpdateStepStatus(ctx, step.ID, db.StepRunning); err != nil {
		return err
	}
	if err := tx.AdvanceWorkflowStep(ctx, wfID, stepNumber); err != nil {
		return err
	}

	s.log.Info("Workflow step started", "workflow_id", wfID, "step", step.Name, "task_id", task.ID)
	return nil
}

// workflowFor loads the step a task belongs to and its workflow. It returns
// nils for tasks that aren't part of a workflow.
func (s *Server) workflowFor(ctx context.Context, taskID uuid.UUID) (*db.Workflow, *db.WorkflowStep) {
	step, err := s.db.GetStepByTaskID(ctx, taskID)
	if err != nil {
		s.log.Error("Failed to look up workflow step", "task_id", taskID, "error", err)
		return nil, nil
	}
	if step == nil {
		return nil, nil
	}
	wf, err := s.db.GetWorkflow(ctx, step.WorkflowID)
	if err != nil || wf == nil {
		s.log.Error("Failed to load workflow", "workflow_id", step.WorkflowID, "error", err)
		return nil, nil
	}
	return wf, step
}

func isCompensation(step *db.WorkflowStep, taskID uuid.UUID) bool {
	return step.CompensationTaskID != nil && *step.CompensationTaskID == taskID
}

func (s *Server) handleWorkflowTaskComplete(ctx context.Context, taskID uuid.UUID) {
	wf, step := s.workflowFor(ctx, taskID)
	if wf == nil {
		return
	}
	log := s.log.With("workflow_id", wf.ID, "step", step.Name)

	if isCompensation(step, taskID) {
		s.setStepStatus(ctx, step, db.StepCompensated)
		log.Info("Workflow step compensated")
		s.continueCompensation(ctx, wf)
		return
	}

	s.setStepStatus(ctx, step, db.StepCompleted)

	if wf.CancelRequested {
		log.Info("Workflow cancelled, compensating instead of continuing")
		s.setWorkflowStatus(ctx, wf.ID, db.WorkflowCompensating, "cancelled")
		s.continueCompensation(ctx, wf)
		return
	}

	def, err := s.registry.Get(wf.Type)
	if err != nil {
		log.Error("Unknown workflow type", "type", wf.Type)
		return
	}
	next := step.StepNumber + 1
	if next >= len(def.Steps) {
		s.setWorkflowStatus(ctx, wf.ID, db.WorkflowCompleted, "")
		log.Info("Workflow completed", "steps", len(def.Steps))
		return
	}

	err = s.db.WithTx(ctx, func(tx *db.DB) error {
		return s.startWorkflowStep(ctx, tx, wf.ID, def, next, wf.Context)
	})
	if err != nil {
		log.Error("Failed to start next workflow step", "error", err)
		s.setWorkflowStatus(ctx, wf.ID, db.WorkflowCompensating, "failed to start next step: "+err.Error())
		s.continueCompensation(ctx, wf)
		return
	}
	s.wakeDispatcher()
}

func (s *Server) handleWorkflowTaskFailed(ctx context.Context, taskID uuid.UUID, errMsg string) {
	wf, step := s.workflowFor(ctx, taskID)
	if wf == nil {
		return
	}
	log := s.log.With("workflow_id", wf.ID, "step", step.Name)

	if isCompensation(step, taskID) {
		s.setStepStatus(ctx, step, db.StepFailed)
		log.Error("Workflow compensation failed; continuing with the remaining steps")
		s.continueCompensation(ctx, wf)
		return
	}
	if step.Status != db.StepRunning {
		return // already handled
	}

	if wf.CancelRequested {
		s.setStepStatus(ctx, step, db.StepCancelled)
		log.Info("Workflow step cancelled, compensating")
	} else {
		s.setStepStatus(ctx, step, db.StepFailed)
		log.Warn("Workflow step failed, compensating", "error", errMsg)
	}
	s.setWorkflowStatus(ctx, wf.ID, db.WorkflowCompensating, errMsg)
	s.continueCompensation(ctx, wf)
}

// continueCompensation compensates the latest completed step, or finishes the
// workflow if none are left.
func (s *Server) continueCompensation(ctx context.Context, wf *db.Workflow) {
	steps, err := s.db.GetWorkflowSteps(ctx, wf.ID)
	if err != nil {
		s.log.Error("Failed to load workflow steps", "workflow_id", wf.ID, "error", err)
		return
	}
	for i := len(steps) - 1; i >= 0; i-- {
		if steps[i].Status == db.StepCompleted {
			s.runCompensationTask(ctx, wf, steps[i])
			return
		}
	}
	s.finishCompensation(ctx, wf.ID)
}

// finishCompensation marks a compensated workflow CANCELLED if the user asked
// for it, or FAILED otherwise.
func (s *Server) finishCompensation(ctx context.Context, wfID uuid.UUID) {
	wf, err := s.db.GetWorkflow(ctx, wfID)
	if err != nil || wf == nil {
		s.log.Error("Failed to load workflow", "workflow_id", wfID, "error", err)
		return
	}
	final := db.WorkflowFailed
	if wf.CancelRequested {
		final = db.WorkflowCancelled
	}
	s.setWorkflowStatus(ctx, wf.ID, final, wf.ErrorMessage)
	s.log.Info("Workflow compensation finished", "workflow_id", wf.ID, "status", final)
}

func (s *Server) runCompensationTask(ctx context.Context, wf *db.Workflow, step *db.WorkflowStep) {
	log := s.log.With("workflow_id", wf.ID, "step", step.Name)

	def, err := s.registry.Get(wf.Type)
	if err != nil || step.StepNumber >= len(def.Steps) {
		log.Error("Cannot compensate step: unknown workflow definition")
		return
	}

	command, err := workflow.ExpandCommand(def.Steps[step.StepNumber].CompensateTemplate, wf.Context)
	if err == nil {
		opts := db.DefaultTaskOptions()
		opts.MaxRetries = 1
		opts.RetryDelaySeconds = 3
		opts.TimeoutSeconds = 60

		err = s.db.WithTx(ctx, func(tx *db.DB) error {
			task, err := tx.CreateTask(ctx, command, opts)
			if err != nil {
				return err
			}
			if err := tx.LinkCompensationTaskToStep(ctx, step.ID, task.ID); err != nil {
				return err
			}
			return tx.UpdateStepStatus(ctx, step.ID, db.StepCompensating)
		})
	}
	if err != nil {
		// Give up on this step rather than leaving the workflow stuck.
		log.Error("Failed to start compensation, skipping step", "error", err)
		s.setStepStatus(ctx, step, db.StepFailed)
		s.continueCompensation(ctx, wf)
		return
	}

	log.Info("Compensating workflow step")
	s.wakeDispatcher()
}

func (s *Server) setStepStatus(ctx context.Context, step *db.WorkflowStep, st db.StepStatus) {
	if err := s.db.UpdateStepStatus(ctx, step.ID, st); err != nil {
		s.log.Error("Failed to update step status", "step_id", step.ID, "status", st, "error", err)
	}
	step.Status = st
}

func (s *Server) setWorkflowStatus(ctx context.Context, id uuid.UUID, st db.WorkflowStatus, errMsg string) {
	if err := s.db.UpdateWorkflowStatus(ctx, id, st, errMsg); err != nil {
		s.log.Error("Failed to update workflow status", "workflow_id", id, "status", st, "error", err)
	}
}
