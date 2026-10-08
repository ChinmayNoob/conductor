package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Explanation is the operations assistant's reading of a failed task.
type Explanation struct {
	TaskID       uuid.UUID
	Attempt      int
	Class        string
	Confidence   float64
	Source       string
	Cause        string
	Fix          string
	Model        string
	InputTokens  int64
	OutputTokens int64
	CostUSD      float64
	CreatedAt    time.Time
}

const explanationColumns = `task_id, attempt, class, confidence, source, cause, fix, model,
	input_tokens, output_tokens, cost_usd, created_at`

func scanExplanation(row scanner) (*Explanation, error) {
	var e Explanation
	err := row.Scan(&e.TaskID, &e.Attempt, &e.Class, &e.Confidence, &e.Source, &e.Cause, &e.Fix, &e.Model,
		&e.InputTokens, &e.OutputTokens, &e.CostUSD, &e.CreatedAt)
	return &e, err
}

// SaveExplanation stores a task's explanation, replacing an older one.
func (db *DB) SaveExplanation(ctx context.Context, e Explanation) error {
	_, err := db.q.ExecContext(ctx,
		`INSERT INTO task_explanations (`+explanationColumns+`)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NOW())
		 ON CONFLICT (task_id) DO UPDATE SET attempt = $2, class = $3, confidence = $4, source = $5,
		   cause = $6, fix = $7, model = $8, input_tokens = $9, output_tokens = $10, cost_usd = $11,
		   created_at = NOW()`,
		e.TaskID, e.Attempt, e.Class, e.Confidence, e.Source, e.Cause, e.Fix, e.Model,
		e.InputTokens, e.OutputTokens, e.CostUSD)
	if err != nil {
		return fmt.Errorf("failed to save explanation: %w", err)
	}
	return nil
}

// GetExplanation returns a task's explanation, or nil.
func (db *DB) GetExplanation(ctx context.Context, taskID uuid.UUID) (*Explanation, error) {
	e, err := scanExplanation(db.q.QueryRowContext(ctx,
		`SELECT `+explanationColumns+` FROM task_explanations WHERE task_id = $1`, taskID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get explanation: %w", err)
	}
	return e, nil
}

// UnexplainedFailures returns recently failed tasks, in namespaces that opted
// in to the assistant, that have no explanation for their latest attempt.
func (db *DB) UnexplainedFailures(ctx context.Context, since time.Duration, limit int) ([]*Task, error) {
	rows, err := db.q.QueryContext(ctx,
		`SELECT `+prefixColumns("t", taskColumns)+` FROM tasks t
		 JOIN namespaces n ON n.name = t.namespace AND n.ai_assist
		 LEFT JOIN task_explanations e ON e.task_id = t.id AND e.attempt = t.attempt
		 WHERE t.status = 'FAILED' AND e.task_id IS NULL
		   AND t.failed_at > (NOW()) - make_interval(secs => $1)
		 ORDER BY t.failed_at DESC LIMIT $2`, since.Seconds(), limit)
	if err != nil {
		return nil, fmt.Errorf("failed to list unexplained failures: %w", err)
	}
	defer rows.Close()
	var out []*Task
	for rows.Next() {
		t, err := scanTask(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan task: %w", err)
		}
		out = append(out, t)
	}
	return out, rows.Err()
}
