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

func (s WorkflowStatus) Terminal() bool {
	return s == WorkflowCompleted || s == WorkflowFailed || s == WorkflowCancelled
}

type Workflow struct {
	ID                uuid.UUID
	Namespace         string
	Type              string // the definition's name
	DefinitionVersion *int
	Definition        json.RawMessage // snapshot taken when the run started
	Status            WorkflowStatus
	Input             json.RawMessage
	ErrorMessage      string
	CancelRequested   bool
	IdempotencyKey    string
	TraceParent       string
	CreatedAt         time.Time
	UpdatedAt         time.Time
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

const workflowColumns = `id, namespace, type, definition_version, definition, status, context,
	error_message, cancel_requested, idempotency_key, trace_parent, created_at, updated_at`

func scanWorkflow(row scanner) (*Workflow, error) {
	wf := &Workflow{}
	var def nullJSON
	var errMsg, idemKey, traceParent sql.NullString
	err := row.Scan(&wf.ID, &wf.Namespace, &wf.Type, &wf.DefinitionVersion, &def, &wf.Status, &wf.Input,
		&errMsg, &wf.CancelRequested, &idemKey, &traceParent, &wf.CreatedAt, &wf.UpdatedAt)
	if err != nil {
		return nil, err
	}
	wf.Definition = def.RawMessage
	wf.ErrorMessage = errMsg.String
	wf.IdempotencyKey = idemKey.String
	wf.TraceParent = traceParent.String
	return wf, nil
}

type NewWorkflow struct {
	Namespace         string
	Name              string
	DefinitionVersion int
	Definition        json.RawMessage
	Input             json.RawMessage
	Steps             []string // step names, in definition order
	IdempotencyKey    string
	TraceParent       string
}

// CreateWorkflow inserts a run and its PENDING steps. If the idempotency key
// was already used in the namespace, it returns the existing run and
// created=false.
func (db *DB) CreateWorkflow(ctx context.Context, n NewWorkflow) (wf *Workflow, created bool, err error) {
	if n.Input == nil {
		n.Input = json.RawMessage("{}")
	}
	var idemKey any
	if n.IdempotencyKey != "" {
		idemKey = n.IdempotencyKey
	}

	err = db.WithTx(ctx, func(tx *DB) error {
		var err error
		wf, err = scanWorkflow(tx.q.QueryRowContext(ctx,
			`INSERT INTO workflows (namespace, type, definition_version, definition, status, current_step, context, idempotency_key, trace_parent)
			 VALUES ($1, $2, $3, $4, 'RUNNING', 0, $5, $6, NULLIF($7, ''))
			 ON CONFLICT (namespace, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
			 RETURNING `+workflowColumns,
			n.Namespace, n.Name, n.DefinitionVersion, []byte(n.Definition), []byte(n.Input), idemKey, n.TraceParent,
		))
		if errors.Is(err, sql.ErrNoRows) && n.IdempotencyKey != "" {
			wf, err = scanWorkflow(tx.q.QueryRowContext(ctx,
				`SELECT `+workflowColumns+` FROM workflows WHERE namespace = $1 AND idempotency_key = $2`,
				n.Namespace, n.IdempotencyKey))
			return err
		}
		if err != nil {
			return err
		}
		created = true
		for i, name := range n.Steps {
			if _, err := tx.q.ExecContext(ctx,
				`INSERT INTO workflow_steps (workflow_id, step_number, name, status) VALUES ($1, $2, $3, 'PENDING')`,
				wf.ID, i, name); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, false, fmt.Errorf("failed to create workflow: %w", err)
	}
	return wf, created, nil
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

// LockWorkflow loads a workflow and locks its row until the transaction ends,
// serializing everything that advances the run. Use it inside WithTx.
func (db *DB) LockWorkflow(ctx context.Context, id uuid.UUID) (*Workflow, error) {
	wf, err := scanWorkflow(db.q.QueryRowContext(ctx,
		`SELECT `+workflowColumns+` FROM workflows WHERE id = $1 FOR UPDATE`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to lock workflow: %w", err)
	}
	return wf, nil
}

func (db *DB) UpdateWorkflowStatus(ctx context.Context, id uuid.UUID, status WorkflowStatus, errMsg string) error {
	_, err := db.q.ExecContext(ctx,
		`UPDATE workflows SET status = $2, error_message = NULLIF($3, ''), updated_at = NOW() WHERE id = $1`,
		id, status, errMsg,
	)
	if err != nil {
		return fmt.Errorf("failed to update workflow status: %w", err)
	}
	return nil
}

// TouchWorkflow bumps updated_at, e.g. after its steps changed.
func (db *DB) TouchWorkflow(ctx context.Context, id uuid.UUID) error {
	_, err := db.q.ExecContext(ctx, `UPDATE workflows SET updated_at = NOW() WHERE id = $1`, id)
	return err
}

// RequestWorkflowCancel flags a running workflow for cancellation. It returns
// false if the workflow has already finished.
func (db *DB) RequestWorkflowCancel(ctx context.Context, id uuid.UUID) (bool, error) {
	result, err := db.q.ExecContext(ctx,
		`UPDATE workflows SET cancel_requested = true, updated_at = NOW()
		 WHERE id = $1 AND status IN ('RUNNING', 'COMPENSATING')`, id)
	if err != nil {
		return false, fmt.Errorf("failed to cancel workflow: %w", err)
	}
	return rowsChanged(result)
}

type WorkflowFilter struct {
	Namespace string
	Status    WorkflowStatus // optional
	Name      string         // optional
	Limit     int
}

func (db *DB) ListWorkflows(ctx context.Context, f WorkflowFilter) ([]*Workflow, error) {
	rows, err := db.q.QueryContext(ctx,
		`SELECT `+workflowColumns+` FROM workflows
		 WHERE namespace = $1 AND ($2 = '' OR status::text = $2) AND ($3 = '' OR type = $3)
		 ORDER BY created_at DESC LIMIT $4`,
		f.Namespace, string(f.Status), f.Name, f.Limit)
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

// ListActiveWorkflowIDs returns runs that are still RUNNING or COMPENSATING,
// least recently updated first.
func (db *DB) ListActiveWorkflowIDs(ctx context.Context, limit int) ([]uuid.UUID, error) {
	rows, err := db.q.QueryContext(ctx,
		`SELECT id FROM workflows WHERE status IN ('RUNNING', 'COMPENSATING')
		 ORDER BY updated_at LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to list active workflows: %w", err)
	}
	defer rows.Close()

	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// --- Workflow steps ---

const stepColumns = `id, workflow_id, step_number, name, task_id, compensation_task_id, status, created_at, updated_at`

func scanStep(row scanner) (*WorkflowStep, error) {
	s := &WorkflowStep{}
	err := row.Scan(&s.ID, &s.WorkflowID, &s.StepNumber, &s.Name, &s.TaskID,
		&s.CompensationTaskID, &s.Status, &s.CreatedAt, &s.UpdatedAt)
	return s, err
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

// StepState is a step joined with the state of its task and compensation
// task: everything the workflow engine needs to decide what's next.
type StepState struct {
	WorkflowStep
	TaskStatus         string
	TaskError          string
	Outputs            StringMap
	CompensationStatus string
	// Waiting steps (approval, signal): what they wait for and, once
	// decided, who decided. Their decision is reported as TaskStatus.
	WaitKind     string
	WaitMessage  string
	WaitDeadline *time.Time
	DecidedBy    string
	// Agent steps: the run (its status is reported as TaskStatus).
	AgentRunID *uuid.UUID
}

func (db *DB) GetStepStates(ctx context.Context, workflowID uuid.UUID) ([]*StepState, error) {
	rows, err := db.q.QueryContext(ctx,
		`SELECT s.id, s.workflow_id, s.step_number, s.name, s.task_id, s.compensation_task_id, s.status,
		        s.created_at, s.updated_at,
		        CASE WHEN s.wait_kind IS NOT NULL THEN COALESCE(s.decision, '')
		             WHEN a.id IS NOT NULL THEN CASE WHEN a.status = 'RUNNING' THEN 'STARTED' ELSE a.status END
		             ELSE COALESCE(t.status::text, '') END,
		        COALESCE(t.error_message, s.decision_error, a.error_message, ''),
		        COALESCE(t.outputs, s.step_outputs, CASE WHEN a.answer IS NOT NULL THEN jsonb_build_object('answer', a.answer) END, '{}'),
		        COALESCE(c.status::text, ''),
		        COALESCE(s.wait_kind, ''), COALESCE(s.wait_message, s.wait_signal, ''), s.wait_deadline,
		        COALESCE(s.decided_by, ''), s.agent_run_id
		 FROM workflow_steps s
		 LEFT JOIN tasks t ON t.id = s.task_id
		 LEFT JOIN tasks c ON c.id = s.compensation_task_id
		 LEFT JOIN agent_runs a ON a.id = s.agent_run_id
		 WHERE s.workflow_id = $1
		 ORDER BY s.step_number`, workflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to load step states: %w", err)
	}
	defer rows.Close()

	var out []*StepState
	for rows.Next() {
		s := &StepState{}
		if err := rows.Scan(&s.ID, &s.WorkflowID, &s.StepNumber, &s.Name, &s.TaskID, &s.CompensationTaskID,
			&s.Status, &s.CreatedAt, &s.UpdatedAt, &s.TaskStatus, &s.TaskError, &s.Outputs,
			&s.CompensationStatus, &s.WaitKind, &s.WaitMessage, &s.WaitDeadline, &s.DecidedBy, &s.AgentRunID); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

func (db *DB) UpdateStepStatus(ctx context.Context, stepID uuid.UUID, status StepStatus) error {
	_, err := db.q.ExecContext(ctx,
		`UPDATE workflow_steps SET status = $2, updated_at = NOW() WHERE id = $1`, stepID, status)
	if err != nil {
		return fmt.Errorf("failed to update step status: %w", err)
	}
	return nil
}

// StartStep links a step to its new task and marks it RUNNING.
func (db *DB) StartStep(ctx context.Context, stepID, taskID uuid.UUID) error {
	_, err := db.q.ExecContext(ctx,
		`UPDATE workflow_steps SET task_id = $2, status = 'RUNNING', updated_at = NOW() WHERE id = $1`,
		stepID, taskID)
	if err != nil {
		return fmt.Errorf("failed to start step: %w", err)
	}
	return nil
}

// StartCompensation links a step to its compensation task and marks it
// COMPENSATING.
func (db *DB) StartCompensation(ctx context.Context, stepID, taskID uuid.UUID) error {
	_, err := db.q.ExecContext(ctx,
		`UPDATE workflow_steps SET compensation_task_id = $2, status = 'COMPENSATING', updated_at = NOW()
		 WHERE id = $1`, stepID, taskID)
	if err != nil {
		return fmt.Errorf("failed to start compensation: %w", err)
	}
	return nil
}
