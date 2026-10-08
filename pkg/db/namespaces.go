package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/lib/pq"
)

type Namespace struct {
	Name            string
	MaxPendingTasks *int
	MaxConcurrency  *int
	// Daily model spend limits; new llm tasks are refused once reached.
	MaxLLMTokensPerDay *int64
	MaxLLMCostPerDay   *float64
	// AIAssist opts in to features that send redacted task output to the
	// model provider (failure explanations).
	AIAssist  bool
	CreatedAt time.Time
}

const namespaceColumns = `name, max_pending_tasks, max_concurrency, max_llm_tokens_per_day, max_llm_cost_per_day,
	ai_assist, created_at`

// ErrExists is returned when creating something whose name is taken.
var ErrExists = errors.New("already exists")

func scanNamespace(row scanner) (*Namespace, error) {
	n := &Namespace{}
	err := row.Scan(&n.Name, &n.MaxPendingTasks, &n.MaxConcurrency, &n.MaxLLMTokensPerDay, &n.MaxLLMCostPerDay,
		&n.AIAssist, &n.CreatedAt)
	return n, err
}

func (db *DB) CreateNamespace(ctx context.Context, n Namespace) (*Namespace, error) {
	out, err := scanNamespace(db.q.QueryRowContext(ctx,
		`INSERT INTO namespaces (name, max_pending_tasks, max_concurrency, max_llm_tokens_per_day,
		                         max_llm_cost_per_day, ai_assist)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 RETURNING `+namespaceColumns,
		n.Name, n.MaxPendingTasks, n.MaxConcurrency, n.MaxLLMTokensPerDay, n.MaxLLMCostPerDay, n.AIAssist))
	if isUniqueViolation(err) {
		return nil, ErrExists
	}
	if err != nil {
		return nil, fmt.Errorf("failed to create namespace: %w", err)
	}
	return out, nil
}

// UpdateNamespace replaces a namespace's quotas. It returns nil if the
// namespace doesn't exist.
func (db *DB) UpdateNamespace(ctx context.Context, n Namespace) (*Namespace, error) {
	out, err := scanNamespace(db.q.QueryRowContext(ctx,
		`UPDATE namespaces SET max_pending_tasks = $2, max_concurrency = $3, max_llm_tokens_per_day = $4,
		     max_llm_cost_per_day = $5, ai_assist = $6
		 WHERE name = $1
		 RETURNING `+namespaceColumns,
		n.Name, n.MaxPendingTasks, n.MaxConcurrency, n.MaxLLMTokensPerDay, n.MaxLLMCostPerDay, n.AIAssist))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to update namespace: %w", err)
	}
	return out, nil
}

// GetNamespace returns a namespace, or nil if it doesn't exist.
func (db *DB) GetNamespace(ctx context.Context, name string) (*Namespace, error) {
	n, err := scanNamespace(db.q.QueryRowContext(ctx,
		`SELECT `+namespaceColumns+` FROM namespaces WHERE name = $1`, name))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get namespace: %w", err)
	}
	return n, nil
}

func (db *DB) ListNamespaces(ctx context.Context) ([]*Namespace, error) {
	rows, err := db.q.QueryContext(ctx,
		`SELECT `+namespaceColumns+` FROM namespaces ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("failed to list namespaces: %w", err)
	}
	defer rows.Close()

	var out []*Namespace
	for rows.Next() {
		n, err := scanNamespace(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

func isUniqueViolation(err error) bool {
	var pqErr *pq.Error
	return errors.As(err, &pqErr) && pqErr.Code == "23505"
}
