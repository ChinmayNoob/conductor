package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"
)

type TaskStatus string

const (
	StatusQueued    TaskStatus = "QUEUED"
	StatusStarted   TaskStatus = "STARTED"
	StatusCompleted TaskStatus = "COMPLETED"
	StatusFailed    TaskStatus = "FAILED"
	StatusCancelled TaskStatus = "CANCELLED"
)

// TaskStatuses lists every status, in lifecycle order.
var TaskStatuses = []TaskStatus{StatusQueued, StatusStarted, StatusCompleted, StatusFailed, StatusCancelled}

func (s TaskStatus) Valid() bool {
	return slices.Contains(TaskStatuses, s)
}

// Terminal reports whether a task in this status will never run again.
func (s TaskStatus) Terminal() bool {
	return s == StatusCompleted || s == StatusFailed || s == StatusCancelled
}

type Task struct {
	ID                uuid.UUID
	Data              string
	Status            TaskStatus
	ScheduledAt       time.Time
	PickedAt          *time.Time
	StartedAt         *time.Time
	CompletedAt       *time.Time
	FailedAt          *time.Time
	CancelledAt       *time.Time
	Priority          int
	MaxRetries        int
	RetryCount        int
	RetryDelaySeconds int
	TimeoutSeconds    int
	Output            string
	ErrorMessage      string
	CreatedAt         time.Time
}

type TaskOptions struct {
	Priority          int
	MaxRetries        int
	RetryDelaySeconds int
	TimeoutSeconds    int
	ScheduledAt       time.Time
}

func DefaultTaskOptions() TaskOptions {
	return TaskOptions{
		Priority:          5,
		MaxRetries:        3,
		RetryDelaySeconds: 60,
		TimeoutSeconds:    300,
		ScheduledAt:       time.Now().UTC(),
	}
}

const taskColumns = `id, data, status, scheduled_at, picked_at, started_at, completed_at, failed_at,
	cancelled_at, priority, max_retries, retry_count, retry_delay_seconds, timeout_seconds,
	output, error_message, created_at`

type scanner interface{ Scan(dest ...any) error }

func scanTask(row scanner) (*Task, error) {
	t := &Task{}
	var output, errMsg sql.NullString
	err := row.Scan(&t.ID, &t.Data, &t.Status, &t.ScheduledAt, &t.PickedAt, &t.StartedAt,
		&t.CompletedAt, &t.FailedAt, &t.CancelledAt, &t.Priority, &t.MaxRetries, &t.RetryCount,
		&t.RetryDelaySeconds, &t.TimeoutSeconds, &output, &errMsg, &t.CreatedAt)
	if err != nil {
		return nil, err
	}
	t.Output = output.String
	t.ErrorMessage = errMsg.String
	return t, nil
}

func (db *DB) CreateTask(ctx context.Context, data string, opts TaskOptions) (*Task, error) {
	def := DefaultTaskOptions()
	if opts.Priority == 0 {
		opts.Priority = def.Priority
	}
	if opts.MaxRetries == 0 {
		opts.MaxRetries = def.MaxRetries
	}
	if opts.RetryDelaySeconds == 0 {
		opts.RetryDelaySeconds = def.RetryDelaySeconds
	}
	if opts.TimeoutSeconds == 0 {
		opts.TimeoutSeconds = def.TimeoutSeconds
	}
	if opts.ScheduledAt.IsZero() {
		opts.ScheduledAt = def.ScheduledAt
	}

	row := db.q.QueryRowContext(ctx,
		`INSERT INTO tasks (data, status, priority, max_retries, retry_delay_seconds, timeout_seconds, scheduled_at)
		 VALUES ($1, 'QUEUED', $2, $3, $4, $5, $6)
		 RETURNING `+taskColumns,
		data, opts.Priority, opts.MaxRetries, opts.RetryDelaySeconds, opts.TimeoutSeconds, opts.ScheduledAt,
	)
	t, err := scanTask(row)
	if err != nil {
		return nil, fmt.Errorf("failed to create task: %w", err)
	}
	return t, nil
}

// GetTask returns the task, or nil if it doesn't exist.
func (db *DB) GetTask(ctx context.Context, id uuid.UUID) (*Task, error) {
	t, err := scanTask(db.q.QueryRowContext(ctx, `SELECT `+taskColumns+` FROM tasks WHERE id = $1`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get task: %w", err)
	}
	return t, nil
}

// ListTasks returns the most recent tasks, optionally filtered by status.
func (db *DB) ListTasks(ctx context.Context, status TaskStatus, limit int) ([]*Task, error) {
	rows, err := db.q.QueryContext(ctx,
		`SELECT `+taskColumns+` FROM tasks
		 WHERE ($1 = '' OR status::text = $1)
		 ORDER BY created_at DESC
		 LIMIT $2`,
		string(status), limit,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to list tasks: %w", err)
	}
	defer rows.Close()

	var tasks []*Task
	for rows.Next() {
		t, err := scanTask(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan task: %w", err)
		}
		tasks = append(tasks, t)
	}
	return tasks, rows.Err()
}

// TaskCounts returns the number of tasks in each status.
func (db *DB) TaskCounts(ctx context.Context) (map[TaskStatus]int, error) {
	rows, err := db.q.QueryContext(ctx, `SELECT status, count(*) FROM tasks GROUP BY status`)
	if err != nil {
		return nil, fmt.Errorf("failed to count tasks: %w", err)
	}
	defer rows.Close()

	counts := make(map[TaskStatus]int)
	for rows.Next() {
		var s TaskStatus
		var n int
		if err := rows.Scan(&s, &n); err != nil {
			return nil, err
		}
		counts[s] = n
	}
	return counts, rows.Err()
}

// PickNextTask claims the highest-priority runnable task, or returns nil if
// there is none. SKIP LOCKED lets concurrent pickers claim different tasks.
func (db *DB) PickNextTask(ctx context.Context) (*Task, error) {
	t, err := scanTask(db.q.QueryRowContext(ctx,
		`UPDATE tasks
		 SET picked_at = NOW()
		 WHERE id = (
			 SELECT id FROM tasks
			 WHERE status = 'QUEUED'
			   AND picked_at IS NULL
			   AND scheduled_at <= NOW()
			 ORDER BY priority ASC, scheduled_at ASC
			 LIMIT 1
			 FOR UPDATE SKIP LOCKED
		 )
		 RETURNING `+taskColumns,
	))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to pick task: %w", err)
	}
	return t, nil
}

// RequeueTask puts a task that was picked but never started back in the queue,
// e.g. when no worker could accept it.
func (db *DB) RequeueTask(ctx context.Context, id uuid.UUID) error {
	_, err := db.q.ExecContext(ctx,
		`UPDATE tasks SET picked_at = NULL WHERE id = $1 AND status = 'QUEUED'`, id)
	if err != nil {
		return fmt.Errorf("failed to requeue task: %w", err)
	}
	return nil
}

// The transitions below only apply to a task that is currently dispatched
// (picked and not yet finished). A late or duplicate report from a worker
// therefore changes nothing, and each returns whether the task was updated.
const dispatchedCondition = `status IN ('QUEUED', 'STARTED') AND picked_at IS NOT NULL`

// maxRetryDelay caps the exponential back-off between retries.
const maxRetryDelay = time.Hour

// RetryTask requeues a failed task with exponential back-off if it has retries
// left. It returns false when the retries are used up.
func (db *DB) RetryTask(ctx context.Context, id uuid.UUID, errorMessage string) (bool, error) {
	result, err := db.q.ExecContext(ctx,
		`UPDATE tasks
		 SET retry_count = retry_count + 1,
		     status = 'QUEUED',
		     picked_at = NULL,
		     started_at = NULL,
		     failed_at = NULL,
		     error_message = $2,
		     scheduled_at = NOW() + make_interval(secs => LEAST(retry_delay_seconds * POWER(2, retry_count), $3))
		 WHERE id = $1
		   AND `+dispatchedCondition+`
		   AND retry_count < max_retries`,
		id, errorMessage, maxRetryDelay.Seconds(),
	)
	if err != nil {
		return false, fmt.Errorf("failed to retry task: %w", err)
	}
	return rowsChanged(result)
}

func (db *DB) MarkTaskStarted(ctx context.Context, id uuid.UUID) (bool, error) {
	result, err := db.q.ExecContext(ctx,
		`UPDATE tasks SET status = 'STARTED', started_at = NOW()
		 WHERE id = $1 AND status = 'QUEUED' AND picked_at IS NOT NULL`,
		id,
	)
	if err != nil {
		return false, fmt.Errorf("failed to mark task started: %w", err)
	}
	return rowsChanged(result)
}

func (db *DB) MarkTaskCompleted(ctx context.Context, id uuid.UUID, output string) (bool, error) {
	result, err := db.q.ExecContext(ctx,
		`UPDATE tasks SET status = 'COMPLETED', completed_at = NOW(), output = $2, error_message = NULL
		 WHERE id = $1 AND `+dispatchedCondition,
		id, output,
	)
	if err != nil {
		return false, fmt.Errorf("failed to mark task completed: %w", err)
	}
	return rowsChanged(result)
}

func (db *DB) MarkTaskFailed(ctx context.Context, id uuid.UUID, output, errorMessage string) (bool, error) {
	result, err := db.q.ExecContext(ctx,
		`UPDATE tasks SET status = 'FAILED', failed_at = NOW(), output = $2, error_message = $3
		 WHERE id = $1 AND `+dispatchedCondition,
		id, output, errorMessage,
	)
	if err != nil {
		return false, fmt.Errorf("failed to mark task failed: %w", err)
	}
	return rowsChanged(result)
}

// CancelTask cancels a task that hasn't finished. It returns the task as it
// was before cancelling, or nil if the task doesn't exist or already finished.
func (db *DB) CancelTask(ctx context.Context, id uuid.UUID) (*Task, error) {
	// The subquery reads the pre-update row, so callers can tell whether a
	// worker was running the task.
	t, err := scanTask(db.q.QueryRowContext(ctx,
		`WITH before AS (SELECT `+taskColumns+` FROM tasks WHERE id = $1 FOR UPDATE)
		 UPDATE tasks t SET status = 'CANCELLED', cancelled_at = NOW(), error_message = 'cancelled'
		 FROM before
		 WHERE t.id = before.id AND before.status IN ('QUEUED', 'STARTED')
		 RETURNING before.id, before.data, before.status, before.scheduled_at, before.picked_at,
		           before.started_at, before.completed_at, before.failed_at, before.cancelled_at,
		           before.priority, before.max_retries, before.retry_count, before.retry_delay_seconds,
		           before.timeout_seconds, before.output, before.error_message, before.created_at`,
		id,
	))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to cancel task: %w", err)
	}
	return t, nil
}

// ListOverdueTasks returns STARTED tasks that have run past their timeout plus
// grace without the worker reporting a result, i.e. the worker was lost.
func (db *DB) ListOverdueTasks(ctx context.Context, grace time.Duration) ([]uuid.UUID, error) {
	rows, err := db.q.QueryContext(ctx,
		`SELECT id FROM tasks
		 WHERE status = 'STARTED'
		   AND started_at + make_interval(secs => timeout_seconds + $1::float8) < NOW()`,
		grace.Seconds(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to list overdue tasks: %w", err)
	}
	defer rows.Close()

	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("failed to scan task id: %w", err)
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// ResetStaleTasks requeues tasks that were picked but never started within
// staleThreshold, e.g. because the dispatch was lost.
func (db *DB) ResetStaleTasks(ctx context.Context, staleThreshold time.Duration) (int64, error) {
	result, err := db.q.ExecContext(ctx,
		`UPDATE tasks
		 SET picked_at = NULL
		 WHERE status = 'QUEUED'
		   AND picked_at IS NOT NULL
		   AND picked_at < NOW() - make_interval(secs => $1)`,
		staleThreshold.Seconds(),
	)
	if err != nil {
		return 0, fmt.Errorf("failed to reset stale tasks: %w", err)
	}
	return result.RowsAffected()
}
