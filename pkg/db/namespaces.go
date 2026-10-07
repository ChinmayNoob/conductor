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
	CreatedAt       time.Time
}

// ErrExists is returned when creating something whose name is taken.
var ErrExists = errors.New("already exists")

func scanNamespace(row scanner) (*Namespace, error) {
	n := &Namespace{}
	err := row.Scan(&n.Name, &n.MaxPendingTasks, &n.MaxConcurrency, &n.CreatedAt)
	return n, err
}

func (db *DB) CreateNamespace(ctx context.Context, n Namespace) (*Namespace, error) {
	out, err := scanNamespace(db.q.QueryRowContext(ctx,
		`INSERT INTO namespaces (name, max_pending_tasks, max_concurrency) VALUES ($1, $2, $3)
		 RETURNING name, max_pending_tasks, max_concurrency, created_at`,
		n.Name, n.MaxPendingTasks, n.MaxConcurrency))
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
		`UPDATE namespaces SET max_pending_tasks = $2, max_concurrency = $3 WHERE name = $1
		 RETURNING name, max_pending_tasks, max_concurrency, created_at`,
		n.Name, n.MaxPendingTasks, n.MaxConcurrency))
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
		`SELECT name, max_pending_tasks, max_concurrency, created_at FROM namespaces WHERE name = $1`, name))
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
		`SELECT name, max_pending_tasks, max_concurrency, created_at FROM namespaces ORDER BY name`)
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
