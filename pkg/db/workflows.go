package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type WorkflowStatus string
type StepStatus string

const (
	WorkflowRunning      WorkflowStatus = "RUNNING"
	WorkflowCompensating WorkflowStatus = "COMPENSATING"
	WorkflowCompleted    WorkflowStatus = "COMPLETED"
	WorkflowFailed       WorkflowStatus = "FAILED"
	WorkflowCancelled    WorkflowStatus = "CANCELLED"
)

const (
	StepPending      StepStatus = "PENDING"
	StepRunning      StepStatus = "RUNNING"
	StepCompleted    StepStatus = "COMPLETED"
	StepFailed       StepStatus = "FAILED"
	StepCompensating StepStatus = "COMPENSATING"
	StepCompensated  StepStatus = "COMPENSATED"
	StepCancelled    StepStatus = "CANCELLED"
)

type Workflow struct {
	ID              uuid.UUID
	Type            string
	Status          WorkflowStatus
	CurrentStep     int
	Context         json.RawMessage
	ErrorMessage    string
	CancelRequested bool
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

type WorkflowStep struct {
	ID                 uuid.UUID
	WorkflowID         uuid.UUID
	StepNumber         int
	Name               string
	TaskID             *uuid.UUID
	CompensationTaskID *uuid.UUID
	Status             StepStatus
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

const workflowColumns = `id, type, status, current_step, context, error_message, cancel_requested, created_at, updated_at`

func scanWorkflow(row scanner) (*Workflow, error) {
	wf := &Workflow{}
	var errMsg sql.NullString
	err := row.Scan(&wf.ID, &wf.Type, &wf.Status, &wf.CurrentStep, &wf.Context, &errMsg,
		&wf.CancelRequested, &wf.CreatedAt, &wf.UpdatedAt)
	if err != nil {
		return nil, err
	}
	wf.ErrorMessage = errMsg.String
	return wf, nil
}

func (db *DB) CreateWorkflow(ctx context.Context, wfType string, input json.RawMessage) (*Workflow, error) {
	if input == nil {
		input = json.RawMessage("{}")
	}
	wf, err := scanWorkflow(db.q.QueryRowContext(ctx,
		`INSERT INTO workflows (type, status, current_step, context)
		 VALUES ($1, 'RUNNING', 0, $2)
		 RETURNING `+workflowColumns,
		wfType, input,
	))
	if err != nil {
		return nil, fmt.Errorf("failed to create workflow: %w", err)
	}
	return wf, nil
}

// GetWorkflow returns the workflow, or nil if it doesn't exist.
func (db *DB) GetWorkflow(ctx context.Context, id uuid.UUID) (*Workflow, error) {
	wf, err := scanWorkflow(db.q.QueryRowContext(ctx,
		`SELECT `+workflowColumns+` FROM workflows WHERE id = $1`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get workflow: %w", err)
	}
	return wf, nil
}

func (db *DB) UpdateWorkflowStatus(ctx context.Context, id uuid.UUID, status WorkflowStatus, errMsg string) error {
	_, err := db.q.ExecContext(ctx,
		`UPDATE workflows SET status = $2, error_message = $3, updated_at = NOW() WHERE id = $1`,
		id, status, errMsg,
	)
	if err != nil {
		return fmt.Errorf("failed to update workflow status: %w", err)
	}
	return nil
}

func (db *DB) AdvanceWorkflowStep(ctx context.Context, id uuid.UUID, step int) error {
	_, err := db.q.ExecContext(ctx,
		`UPDATE workflows SET current_step = $2, updated_at = NOW() WHERE id = $1`, id, step)
	if err != nil {
		return fmt.Errorf("failed to advance workflow: %w", err)
	}
	return nil
}

// RequestWorkflowCancel flags a running workflow for cancellation. It returns
// false if the workflow doesn't exist or has already finished.
func (db *DB) RequestWorkflowCancel(ctx context.Context, id uuid.UUID) (bool, error) {
	result, err := db.q.ExecContext(ctx,
		`UPDATE workflows SET cancel_requested = true, updated_at = NOW()
		 WHERE id = $1 AND status IN ('RUNNING', 'COMPENSATING')`, id)
	if err != nil {
		return false, fmt.Errorf("failed to cancel workflow: %w", err)
	}
	return rowsChanged(result)
}

func (db *DB) ListWorkflows(ctx context.Context, limit int) ([]*Workflow, error) {
	rows, err := db.q.QueryContext(ctx,
		`SELECT `+workflowColumns+` FROM workflows ORDER BY created_at DESC LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to list workflows: %w", err)
	}
	defer rows.Close()

	var workflows []*Workflow
	for rows.Next() {
		wf, err := scanWorkflow(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan workflow: %w", err)
		}
		workflows = append(workflows, wf)
	}
	return workflows, rows.Err()
}

// --- Workflow steps ---

const stepColumns = `id, workflow_id, step_number, name, task_id, compensation_task_id, status, created_at, updated_at`

func scanStep(row scanner) (*WorkflowStep, error) {
	s := &WorkflowStep{}
	err := row.Scan(&s.ID, &s.WorkflowID, &s.StepNumber, &s.Name, &s.TaskID,
		&s.CompensationTaskID, &s.Status, &s.CreatedAt, &s.UpdatedAt)
	return s, err
}

func (db *DB) CreateWorkflowStep(ctx context.Context, workflowID uuid.UUID, stepNumber int, name string) (*WorkflowStep, error) {
	s, err := scanStep(db.q.QueryRowContext(ctx,
		`INSERT INTO workflow_steps (workflow_id, step_number, name, status)
		 VALUES ($1, $2, $3, 'PENDING')
		 RETURNING `+stepColumns,
		workflowID, stepNumber, name,
	))
	if err != nil {
		return nil, fmt.Errorf("failed to create workflow step: %w", err)
	}
	return s, nil
}

func (db *DB) GetWorkflowSteps(ctx context.Context, workflowID uuid.UUID) ([]*WorkflowStep, error) {
	rows, err := db.q.QueryContext(ctx,
		`SELECT `+stepColumns+` FROM workflow_steps WHERE workflow_id = $1 ORDER BY step_number ASC`,
		workflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to get workflow steps: %w", err)
	}
	defer rows.Close()

	var steps []*WorkflowStep
	for rows.Next() {
		s, err := scanStep(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan workflow step: %w", err)
		}
		steps = append(steps, s)
	}
	return steps, rows.Err()
}

func (db *DB) UpdateStepStatus(ctx context.Context, stepID uuid.UUID, status StepStatus) error {
	_, err := db.q.ExecContext(ctx,
		`UPDATE workflow_steps SET status = $2, updated_at = NOW() WHERE id = $1`, stepID, status)
	if err != nil {
		return fmt.Errorf("failed to update step status: %w", err)
	}
	return nil
}

func (db *DB) LinkTaskToStep(ctx context.Context, stepID, taskID uuid.UUID) error {
	_, err := db.q.ExecContext(ctx,
		`UPDATE workflow_steps SET task_id = $2, updated_at = NOW() WHERE id = $1`, stepID, taskID)
	if err != nil {
		return fmt.Errorf("failed to link task to step: %w", err)
	}
	return nil
}

func (db *DB) LinkCompensationTaskToStep(ctx context.Context, stepID, taskID uuid.UUID) error {
	_, err := db.q.ExecContext(ctx,
		`UPDATE workflow_steps SET compensation_task_id = $2, updated_at = NOW() WHERE id = $1`, stepID, taskID)
	if err != nil {
		return fmt.Errorf("failed to link compensation task to step: %w", err)
	}
	return nil
}

// GetStepByTaskID returns the step that a task (or compensation task) belongs
// to, or nil if it isn't part of a workflow.
func (db *DB) GetStepByTaskID(ctx context.Context, taskID uuid.UUID) (*WorkflowStep, error) {
	s, err := scanStep(db.q.QueryRowContext(ctx,
		`SELECT `+stepColumns+` FROM workflow_steps WHERE task_id = $1 OR compensation_task_id = $1`, taskID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get step by task: %w", err)
	}
	return s, nil
}
