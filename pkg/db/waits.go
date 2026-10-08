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

// Wait kinds: steps that wait instead of running a task.
const (
	WaitApproval = "approval"
	WaitSignal   = "signal"
)

// Wait describes what a waiting step waits for.
type Wait struct {
	Kind      string
	Signal    string // signal steps: the signal's name
	Message   string // approval steps: what a person is asked
	Deadline  *time.Time
	OnTimeout string
}

// StartWait marks a step RUNNING and waiting.
func (db *DB) StartWait(ctx context.Context, stepID uuid.UUID, w Wait) error {
	_, err := db.q.ExecContext(ctx,
		`UPDATE workflow_steps
		 SET status = 'RUNNING', wait_kind = $2, wait_signal = NULLIF($3, ''), wait_message = NULLIF($4, ''),
		     wait_deadline = $5, wait_on_timeout = NULLIF($6, ''), updated_at = NOW()
		 WHERE id = $1`, stepID, w.Kind, w.Signal, w.Message, w.Deadline, w.OnTimeout)
	if err != nil {
		return fmt.Errorf("failed to start waiting: %w", err)
	}
	return nil
}

// Decision resolves a waiting step.
type Decision struct {
	Status  string // COMPLETED, FAILED or CANCELLED
	Error   string
	By      string
	Outputs StringMap
}

// Decide resolves a step that is still waiting. It returns false if the step
// isn't waiting (never started, or already decided).
func (db *DB) Decide(ctx context.Context, stepID uuid.UUID, d Decision) (bool, error) {
	outputs, err := json.Marshal(d.Outputs)
	if err != nil {
		return false, err
	}
	result, err := db.q.ExecContext(ctx,
		`UPDATE workflow_steps
		 SET decision = $2, decision_error = NULLIF($3, ''), decided_by = NULLIF($4, ''), decided_at = NOW(),
		     step_outputs = $5, updated_at = NOW()
		 WHERE id = $1 AND wait_kind IS NOT NULL AND decision IS NULL AND status = 'RUNNING'`,
		stepID, d.Status, d.Error, d.By, outputs)
	if err != nil {
		return false, fmt.Errorf("failed to record decision: %w", err)
	}
	return rowsChanged(result)
}

// WaitingStep is a step waiting for a decision.
type WaitingStep struct {
	StepID     uuid.UUID
	WorkflowID uuid.UUID
	Namespace  string
	Workflow   string // the definition's name
	Step       string
	Wait
	Since time.Time
}

const waitingColumns = `s.id, s.workflow_id, w.namespace, w.type, s.name, s.wait_kind, COALESCE(s.wait_signal, ''),
	COALESCE(s.wait_message, ''), s.wait_deadline, COALESCE(s.wait_on_timeout, ''), s.updated_at`

func scanWaiting(row scanner) (*WaitingStep, error) {
	w := &WaitingStep{}
	err := row.Scan(&w.StepID, &w.WorkflowID, &w.Namespace, &w.Workflow, &w.Step, &w.Kind, &w.Signal, &w.Message,
		&w.Deadline, &w.OnTimeout, &w.Since)
	return w, err
}

// GetWaitingStep returns a run's step by name if it is waiting, else nil.
func (db *DB) GetWaitingStep(ctx context.Context, workflowID uuid.UUID, step string) (*WaitingStep, error) {
	w, err := scanWaiting(db.q.QueryRowContext(ctx,
		`SELECT `+waitingColumns+` FROM workflow_steps s JOIN workflows w ON w.id = s.workflow_id
		 WHERE s.workflow_id = $1 AND s.name = $2 AND s.wait_kind IS NOT NULL AND s.decision IS NULL
		   AND s.status = 'RUNNING'`, workflowID, step))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get waiting step: %w", err)
	}
	return w, nil
}

// ListWaiting returns steps waiting in a namespace (optionally of one kind),
// oldest first.
func (db *DB) ListWaiting(ctx context.Context, namespace, kind string) ([]*WaitingStep, error) {
	return db.listWaiting(ctx,
		`WHERE w.namespace = $1 AND ($2 = '' OR s.wait_kind = $2) AND s.wait_kind IS NOT NULL
		   AND s.decision IS NULL AND s.status = 'RUNNING' ORDER BY s.updated_at`, namespace, kind)
}

// ListWaitingInRun returns a run's waiting steps.
func (db *DB) ListWaitingInRun(ctx context.Context, workflowID uuid.UUID) ([]*WaitingStep, error) {
	return db.listWaiting(ctx,
		`WHERE s.workflow_id = $1 AND s.wait_kind IS NOT NULL AND s.decision IS NULL AND s.status = 'RUNNING'
		 ORDER BY s.step_number`, workflowID)
}

// ExpiredWaits returns waiting steps past their deadline, across namespaces.
func (db *DB) ExpiredWaits(ctx context.Context, limit int) ([]*WaitingStep, error) {
	return db.listWaiting(ctx,
		`WHERE s.wait_kind IS NOT NULL AND s.decision IS NULL AND s.wait_deadline <= NOW()
		   AND s.status = 'RUNNING' ORDER BY s.wait_deadline LIMIT $1`, limit)
}

func (db *DB) listWaiting(ctx context.Context, where string, args ...any) ([]*WaitingStep, error) {
	rows, err := db.q.QueryContext(ctx,
		`SELECT `+waitingColumns+` FROM workflow_steps s JOIN workflows w ON w.id = s.workflow_id `+where, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list waiting steps: %w", err)
	}
	defer rows.Close()
	var out []*WaitingStep
	for rows.Next() {
		w, err := scanWaiting(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

// Signal is data sent to a workflow run.
type Signal struct {
	ID      int64
	Name    string
	Payload json.RawMessage
	SentBy  string
}

// AddSignal stores a signal sent to a run.
func (db *DB) AddSignal(ctx context.Context, workflowID uuid.UUID, name string, payload json.RawMessage, by string) (int64, error) {
	if len(payload) == 0 {
		payload = json.RawMessage("{}")
	}
	var id int64
	err := db.q.QueryRowContext(ctx,
		`INSERT INTO workflow_signals (workflow_id, name, payload, sent_by) VALUES ($1, $2, $3, NULLIF($4, ''))
		 RETURNING id`, workflowID, name, []byte(payload), by).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("failed to store signal: %w", err)
	}
	return id, nil
}

// TakeSignal claims the oldest unconsumed signal of a name for a step, or
// returns nil if none has arrived.
func (db *DB) TakeSignal(ctx context.Context, workflowID uuid.UUID, name string, stepID uuid.UUID) (*Signal, error) {
	sig := &Signal{Name: name}
	var by sql.NullString
	err := db.q.QueryRowContext(ctx,
		`UPDATE workflow_signals SET consumed_by = $3
		 WHERE id = (SELECT id FROM workflow_signals
		             WHERE workflow_id = $1 AND name = $2 AND consumed_by IS NULL
		             ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED)
		 RETURNING id, payload, sent_by`, workflowID, name, stepID).Scan(&sig.ID, &sig.Payload, &by)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to take signal: %w", err)
	}
	sig.SentBy = by.String
	return sig, nil
}
