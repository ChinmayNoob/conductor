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
	TokensPerMinute   *int64 // model tokens per minute (llm tasks)
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
		`INSERT INTO queues (namespace, name, concurrency_limit, rate_limit, rate_period_seconds, paused, tokens_per_minute)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)
		 ON CONFLICT (namespace, name) DO UPDATE
		 SET concurrency_limit = EXCLUDED.concurrency_limit, rate_limit = EXCLUDED.rate_limit,
		     rate_period_seconds = EXCLUDED.rate_period_seconds, paused = EXCLUDED.paused,
		     tokens_per_minute = EXCLUDED.tokens_per_minute, updated_at = NOW()
		 RETURNING namespace, name, concurrency_limit, rate_limit, rate_period_seconds, paused, updated_at, tokens_per_minute`,
		q.Namespace, q.Name, q.ConcurrencyLimit, q.RateLimit, q.RatePeriodSeconds, q.Paused, q.TokensPerMinute,
	).Scan(&out.Namespace, &out.Name, &out.ConcurrencyLimit, &out.RateLimit, &out.RatePeriodSeconds, &out.Paused,
		&out.UpdatedAt, &out.TokensPerMinute)
	if err != nil {
		return nil, fmt.Errorf("failed to save queue: %w", err)
	}
	return out, nil
}

// GetQueue returns a queue's settings, or nil if it has none.
func (db *DB) GetQueue(ctx context.Context, namespace, name string) (*Queue, error) {
	q := &Queue{}
	err := db.q.QueryRowContext(ctx,
		`SELECT namespace, name, concurrency_limit, rate_limit, rate_period_seconds, paused, updated_at, tokens_per_minute
		 FROM queues WHERE namespace = $1 AND name = $2`, namespace, name,
	).Scan(&q.Namespace, &q.Name, &q.ConcurrencyLimit, &q.RateLimit, &q.RatePeriodSeconds, &q.Paused, &q.UpdatedAt,
		&q.TokensPerMinute)
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
		        COALESCE(q.paused, false), COALESCE(q.updated_at, NOW()), q.tokens_per_minute,
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
			&q.UpdatedAt, &q.TokensPerMinute, &q.Queued, &q.Running); err != nil {
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

// QueueDepth is what a queue holds right now.
type QueueDepth struct {
	Namespace, Queue string
	Ready            int // due and waiting for a worker
	Delayed          int // waiting for their scheduled time (or a retry)
	Paused           int // waiting in a paused queue
	Running          int // claimed and not finished
	OldestReady      *time.Time
}

// QueueDepths counts waiting and running tasks per queue. Each half matches a
// partial index's predicate, so it reads only unfinished tasks.
func (db *DB) QueueDepths(ctx context.Context) ([]QueueDepth, error) {
	rows, err := db.q.QueryContext(ctx,
		`WITH waiting AS (
			 SELECT t.namespace, t.queue,
			        count(*) FILTER (WHERE t.scheduled_at <= NOW() AND NOT COALESCE(q.paused, false)) AS ready,
			        count(*) FILTER (WHERE t.scheduled_at > NOW() AND NOT COALESCE(q.paused, false)) AS delayed,
			        count(*) FILTER (WHERE COALESCE(q.paused, false)) AS paused,
			        min(t.scheduled_at) FILTER (WHERE t.scheduled_at <= NOW() AND NOT COALESCE(q.paused, false)) AS oldest
			 FROM tasks t
			 LEFT JOIN queues q ON q.namespace = t.namespace AND q.name = t.queue
			 WHERE t.status = 'QUEUED' AND t.picked_at IS NULL
			 GROUP BY 1, 2
		 ), running AS (
			 SELECT namespace, queue, count(*) AS running
			 FROM tasks WHERE picked_at IS NOT NULL AND status IN ('QUEUED', 'STARTED')
			 GROUP BY 1, 2
		 )
		 SELECT namespace, queue, COALESCE(w.ready, 0), COALESCE(w.delayed, 0), COALESCE(w.paused, 0),
		        COALESCE(r.running, 0), w.oldest
		 FROM waiting w FULL JOIN running r USING (namespace, queue)`)
	if err != nil {
		return nil, fmt.Errorf("failed to count queued tasks: %w", err)
	}
	defer rows.Close()
	var out []QueueDepth
	for rows.Next() {
		var d QueueDepth
		if err := rows.Scan(&d.Namespace, &d.Queue, &d.Ready, &d.Delayed, &d.Paused, &d.Running, &d.OldestReady); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}
