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

// Agent run phases and roles of its tasks.
const (
	AgentThink = "think" // waiting for the model's next turn
	AgentAct   = "act"   // waiting for the turn's tool calls

	AgentRoleLLM  = "llm"
	AgentRoleTool = "tool"
)

// AgentRun is a durable agent: its conversation and progress.
type AgentRun struct {
	ID         uuid.UUID
	Namespace  string
	WorkflowID uuid.UUID
	StepID     uuid.UUID
	Spec       json.RawMessage
	Messages   json.RawMessage
	Turn       int
	Phase      string
	ToolCalls  int
	Status     string // RUNNING, COMPLETED, FAILED, CANCELLED
	Answer     string
	Error      string
	Deadline   *time.Time
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

const agentColumns = `id, namespace, workflow_id, step_id, spec, messages, turn, phase, tool_calls, status,
	COALESCE(answer, ''), COALESCE(error_message, ''), deadline, created_at, updated_at`

func scanAgentRun(row scanner) (*AgentRun, error) {
	r := &AgentRun{}
	err := row.Scan(&r.ID, &r.Namespace, &r.WorkflowID, &r.StepID, &r.Spec, &r.Messages, &r.Turn, &r.Phase,
		&r.ToolCalls, &r.Status, &r.Answer, &r.Error, &r.Deadline, &r.CreatedAt, &r.UpdatedAt)
	return r, err
}

// CreateAgentRun stores a new run and marks its step RUNNING.
func (db *DB) CreateAgentRun(ctx context.Context, r AgentRun) (*AgentRun, error) {
	out, err := scanAgentRun(db.q.QueryRowContext(ctx,
		`INSERT INTO agent_runs (namespace, workflow_id, step_id, spec, messages, deadline)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 RETURNING `+agentColumns,
		r.Namespace, r.WorkflowID, r.StepID, []byte(r.Spec), []byte(r.Messages), r.Deadline))
	if err != nil {
		return nil, fmt.Errorf("failed to create agent run: %w", err)
	}
	if _, err := db.q.ExecContext(ctx,
		`UPDATE workflow_steps SET agent_run_id = $2, status = 'RUNNING', updated_at = NOW() WHERE id = $1`,
		r.StepID, out.ID); err != nil {
		return nil, fmt.Errorf("failed to start agent step: %w", err)
	}
	return out, nil
}

// GetAgentRun returns a run, or nil. LockAgentRun also locks it until the
// transaction ends, serializing everything that advances it.
func (db *DB) GetAgentRun(ctx context.Context, id uuid.UUID) (*AgentRun, error) {
	return db.getAgentRun(ctx, id, "")
}

func (db *DB) LockAgentRun(ctx context.Context, id uuid.UUID) (*AgentRun, error) {
	return db.getAgentRun(ctx, id, " FOR UPDATE")
}

func (db *DB) getAgentRun(ctx context.Context, id uuid.UUID, lock string) (*AgentRun, error) {
	r, err := scanAgentRun(db.q.QueryRowContext(ctx, `SELECT `+agentColumns+` FROM agent_runs WHERE id = $1`+lock, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get agent run: %w", err)
	}
	return r, nil
}

// SaveAgentRun writes a run's progress.
func (db *DB) SaveAgentRun(ctx context.Context, r *AgentRun) error {
	_, err := db.q.ExecContext(ctx,
		`UPDATE agent_runs SET messages = $2, turn = $3, phase = $4, tool_calls = $5, status = $6,
		     answer = NULLIF($7, ''), error_message = NULLIF($8, ''), updated_at = NOW()
		 WHERE id = $1`,
		r.ID, []byte(r.Messages), r.Turn, r.Phase, r.ToolCalls, r.Status, r.Answer, r.Error)
	if err != nil {
		return fmt.Errorf("failed to save agent run: %w", err)
	}
	return nil
}

// AgentTasks returns a run's tasks for one turn (or every turn, with turn
// 0), oldest first.
func (db *DB) AgentTasks(ctx context.Context, runID uuid.UUID, turn int) ([]*Task, error) {
	rows, err := db.q.QueryContext(ctx,
		`SELECT `+taskColumns+` FROM tasks WHERE agent_run_id = $1 AND ($2 = 0 OR agent_turn = $2)
		 ORDER BY agent_turn, created_at, id`, runID, turn)
	if err != nil {
		return nil, fmt.Errorf("failed to list agent tasks: %w", err)
	}
	defer rows.Close()
	var out []*Task
	for rows.Next() {
		t, err := scanTask(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// AgentRunSpend sums a run's model usage.
func (db *DB) AgentRunSpend(ctx context.Context, runID uuid.UUID) (Spend, error) {
	var s Spend
	err := db.q.QueryRowContext(ctx,
		`SELECT COALESCE(sum(input_tokens + output_tokens), 0), COALESCE(sum(cost_usd), 0)
		 FROM tasks WHERE agent_run_id = $1`, runID).Scan(&s.Tokens, &s.CostUSD)
	if err != nil {
		return s, fmt.Errorf("failed to sum agent spend: %w", err)
	}
	return s, nil
}

// RunningAgentRuns lists unfinished runs, least recently advanced first.
func (db *DB) RunningAgentRuns(ctx context.Context, limit int) ([]uuid.UUID, error) {
	rows, err := db.q.QueryContext(ctx,
		`SELECT id FROM agent_runs WHERE status = 'RUNNING' ORDER BY updated_at LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to list agent runs: %w", err)
	}
	defer rows.Close()
	var out []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}
