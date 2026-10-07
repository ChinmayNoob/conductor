package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// Queue holds limits for one queue. Tasks may use queues that have no row;
// they are unlimited.
type Queue struct {
	Namespace         string
	Name              string
	ConcurrencyLimit  *int
	RateLimit         *int
	RatePeriodSeconds int
	Paused            bool
	UpdatedAt         time.Time

	// Live counts, filled in by ListQueues.
	Queued  int
	Running int
}

// UpsertQueue creates or replaces a queue's settings.
func (db *DB) UpsertQueue(ctx context.Context, q Queue) (*Queue, error) {
	if q.RatePeriodSeconds == 0 {
		q.RatePeriodSeconds = 1
	}
	out := &Queue{}
	err := db.q.QueryRowContext(ctx,
		`INSERT INTO queues (namespace, name, concurrency_limit, rate_limit, rate_period_seconds, paused)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 ON CONFLICT (namespace, name) DO UPDATE
		 SET concurrency_limit = EXCLUDED.concurrency_limit, rate_limit = EXCLUDED.rate_limit,
		     rate_period_seconds = EXCLUDED.rate_period_seconds, paused = EXCLUDED.paused, updated_at = NOW()
		 RETURNING namespace, name, concurrency_limit, rate_limit, rate_period_seconds, paused, updated_at`,
		q.Namespace, q.Name, q.ConcurrencyLimit, q.RateLimit, q.RatePeriodSeconds, q.Paused,
	).Scan(&out.Namespace, &out.Name, &out.ConcurrencyLimit, &out.RateLimit, &out.RatePeriodSeconds, &out.Paused, &out.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("failed to save queue: %w", err)
	}
	return out, nil
}

// GetQueue returns a queue's settings, or nil if it has none.
func (db *DB) GetQueue(ctx context.Context, namespace, name string) (*Queue, error) {
	q := &Queue{}
	err := db.q.QueryRowContext(ctx,
		`SELECT namespace, name, concurrency_limit, rate_limit, rate_period_seconds, paused, updated_at
		 FROM queues WHERE namespace = $1 AND name = $2`, namespace, name,
	).Scan(&q.Namespace, &q.Name, &q.ConcurrencyLimit, &q.RateLimit, &q.RatePeriodSeconds, &q.Paused, &q.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get queue: %w", err)
	}
	return q, nil
}

// ListQueues returns every queue that has settings or tasks, with live counts.
func (db *DB) ListQueues(ctx context.Context, namespace string) ([]*Queue, error) {
	rows, err := db.q.QueryContext(ctx,
		`WITH names AS (
		     SELECT name FROM queues WHERE namespace = $1
		     UNION
		     SELECT DISTINCT queue FROM tasks WHERE namespace = $1 AND status IN ('QUEUED', 'STARTED')
		 )
		 SELECT n.name, q.concurrency_limit, q.rate_limit, COALESCE(q.rate_period_seconds, 1),
		        COALESCE(q.paused, false), COALESCE(q.updated_at, NOW()),
		        (SELECT count(*) FROM tasks t WHERE t.namespace = $1 AND t.queue = n.name
		           AND t.status = 'QUEUED' AND t.picked_at IS NULL),
		        (SELECT count(*) FROM tasks t WHERE t.namespace = $1 AND t.queue = n.name
		           AND t.picked_at IS NOT NULL AND t.status IN ('QUEUED', 'STARTED'))
		 FROM names n LEFT JOIN queues q ON q.namespace = $1 AND q.name = n.name
		 ORDER BY n.name`, namespace)
	if err != nil {
		return nil, fmt.Errorf("failed to list queues: %w", err)
	}
	defer rows.Close()

	var out []*Queue
	for rows.Next() {
		q := &Queue{Namespace: namespace}
		if err := rows.Scan(&q.Name, &q.ConcurrencyLimit, &q.RateLimit, &q.RatePeriodSeconds, &q.Paused,
			&q.UpdatedAt, &q.Queued, &q.Running); err != nil {
			return nil, err
		}
		out = append(out, q)
	}
	return out, rows.Err()
}

// DeleteQueue removes a queue's settings (its tasks are unaffected). It
// returns false if the queue had none.
func (db *DB) DeleteQueue(ctx context.Context, namespace, name string) (bool, error) {
	result, err := db.q.ExecContext(ctx, `DELETE FROM queues WHERE namespace = $1 AND name = $2`, namespace, name)
	if err != nil {
		return false, fmt.Errorf("failed to delete queue: %w", err)
	}
	return rowsChanged(result)
}
