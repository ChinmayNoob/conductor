package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
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
	Namespace         string
	Queue             string
	Type              string
	Data              string
	Spec              json.RawMessage // type-specific settings (http, container)
	Env               StringMap
	Requirements      StringMap // labels a worker must have
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
	// Attempt numbers dispatches: it goes up every time the task is handed
	// to a worker, and status reports must quote the current one.
	Attempt        int
	TraceParent    string
	dispatchKey    *time.Time
	Output         string
	Outputs        StringMap // key=value pairs a step wrote to $CONDUCTOR_OUTPUT
	ErrorMessage   string
	IdempotencyKey string
	WorkflowID     *uuid.UUID
	WorkerID       *int64
	CreatedAt      time.Time
}

// NewTask describes a task to create. Zero values are stored as given, so
// callers should start from DefaultTask.
type NewTask struct {
	Namespace         string
	Queue             string
	Type              string
	Data              string
	Spec              json.RawMessage
	Env               StringMap
	Requirements      StringMap
	Priority          int
	MaxRetries        int
	RetryDelaySeconds int
	TimeoutSeconds    int
	ScheduledAt       time.Time
	IdempotencyKey    string
	WorkflowID        *uuid.UUID
	TraceParent       string // W3C traceparent of the submitter, if traced
}

// DefaultTask returns a shell task in the default namespace and queue.
func DefaultTask(data string) NewTask {
	return NewTask{
		Namespace:         "default",
		Queue:             "default",
		Type:              "shell",
		Data:              data,
		Priority:          5,
		MaxRetries:        3,
		RetryDelaySeconds: 60,
		TimeoutSeconds:    300,
	}
}

const taskColumns = `id, namespace, queue, type, data, spec, env, requirements, status, scheduled_at,
	picked_at, started_at, completed_at, failed_at, cancelled_at, priority, max_retries, retry_count,
	retry_delay_seconds, timeout_seconds, output, outputs, error_message, idempotency_key,
	workflow_id, worker_id, attempt, dispatch_key, trace_parent, created_at`

type scanner interface{ Scan(dest ...any) error }

func scanTask(row scanner) (*Task, error) {
	t := &Task{}
	var spec nullJSON
	var output, errMsg, idemKey, traceParent sql.NullString
	err := row.Scan(&t.ID, &t.Namespace, &t.Queue, &t.Type, &t.Data, &spec, &t.Env, &t.Requirements,
		&t.Status, &t.ScheduledAt, &t.PickedAt, &t.StartedAt, &t.CompletedAt, &t.FailedAt, &t.CancelledAt,
		&t.Priority, &t.MaxRetries, &t.RetryCount, &t.RetryDelaySeconds, &t.TimeoutSeconds, &output,
		&t.Outputs, &errMsg, &idemKey, &t.WorkflowID, &t.WorkerID, &t.Attempt, &t.dispatchKey, &traceParent, &t.CreatedAt)
	if err != nil {
		return nil, err
	}
	t.Spec = spec.RawMessage
	t.Output = output.String
	t.ErrorMessage = errMsg.String
	t.IdempotencyKey = idemKey.String
	t.TraceParent = traceParent.String
	return t, nil
}

// CreateTask inserts a task. If the idempotency key was already used in the
// namespace, it returns the existing task and created=false.
func (db *DB) CreateTask(ctx context.Context, n NewTask) (task *Task, created bool, err error) {
	if n.ScheduledAt.IsZero() {
		n.ScheduledAt = time.Now()
	}
	// scheduled_at has no time zone; store UTC to match the session.
	n.ScheduledAt = n.ScheduledAt.UTC()
	var idemKey any
	if n.IdempotencyKey != "" {
		idemKey = n.IdempotencyKey
	}

	t, err := scanTask(db.q.QueryRowContext(ctx,
		`INSERT INTO tasks (namespace, queue, type, data, spec, env, requirements, status, priority,
		                    max_retries, retry_delay_seconds, timeout_seconds, scheduled_at,
		                    idempotency_key, workflow_id, trace_parent)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, 'QUEUED', $8, $9, $10, $11, $12, $13, $14, NULLIF($15, ''))
		 ON CONFLICT (namespace, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
		 RETURNING `+taskColumns,
		n.Namespace, n.Queue, n.Type, n.Data, jsonValue(n.Spec), n.Env, n.Requirements, n.Priority,
		n.MaxRetries, n.RetryDelaySeconds, n.TimeoutSeconds, n.ScheduledAt, idemKey, n.WorkflowID, n.TraceParent,
	))
	if errors.Is(err, sql.ErrNoRows) && n.IdempotencyKey != "" {
		t, err = scanTask(db.q.QueryRowContext(ctx,
			`SELECT `+taskColumns+` FROM tasks WHERE namespace = $1 AND idempotency_key = $2`,
			n.Namespace, n.IdempotencyKey))
		if err != nil {
			return nil, false, fmt.Errorf("failed to load task for idempotency key: %w", err)
		}
		return t, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("failed to create task: %w", err)
	}
	return t, true, nil
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

type TaskFilter struct {
	Namespace  string
	Status     TaskStatus // optional
	Queue      string     // optional
	WorkflowID *uuid.UUID // optional
	// DeadLetter selects permanently failed tasks that aren't workflow steps.
	DeadLetter bool
	// Search matches a task ID prefix or part of the command (optional).
	Search string
	// Before pages backwards: only tasks created before this (optional).
	Before *time.Time
	Limit  int
}

// ListTasks returns the most recent tasks matching the filter.
func (db *DB) ListTasks(ctx context.Context, f TaskFilter) ([]*Task, error) {
	rows, err := db.q.QueryContext(ctx,
		`SELECT `+taskColumns+` FROM tasks
		 WHERE namespace = $1
		   AND ($2 = '' OR status::text = $2)
		   AND ($3 = '' OR queue = $3)
		   AND ($4::uuid IS NULL OR workflow_id = $4)
		   AND (NOT $5 OR (status = 'FAILED' AND workflow_id IS NULL))
		   AND ($7 = '' OR id::text LIKE $7 || '%' OR data ILIKE '%' || $7 || '%')
		   AND ($8::timestamp IS NULL OR created_at < $8)
		 ORDER BY created_at DESC
		 LIMIT $6`,
		f.Namespace, string(f.Status), f.Queue, f.WorkflowID, f.DeadLetter, f.Limit,
		strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(f.Search), f.Before,
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

// TaskCounts returns the number of tasks in each status in a namespace.
func (db *DB) TaskCounts(ctx context.Context, namespace string) (map[TaskStatus]int, error) {
	rows, err := db.q.QueryContext(ctx,
		`SELECT status, count(*) FROM tasks WHERE namespace = $1 GROUP BY status`, namespace)
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

// TimelinePoint counts the tasks created in one minute, by current status.
type TimelinePoint struct {
	Minute time.Time
	Status TaskStatus
	Count  int
}

// Timeline counts tasks created per minute over the last `minutes`, by
// their status now. It reads through the (namespace, created_at) index.
func (db *DB) Timeline(ctx context.Context, namespace string, minutes int) ([]TimelinePoint, error) {
	rows, err := db.q.QueryContext(ctx,
		`SELECT date_trunc('minute', created_at) AS minute, status, count(*)
		 FROM tasks
		 WHERE namespace = $1 AND created_at > NOW() - make_interval(mins => $2)
		 GROUP BY 1, 2 ORDER BY 1`, namespace, minutes)
	if err != nil {
		return nil, fmt.Errorf("failed to build timeline: %w", err)
	}
	defer rows.Close()
	var out []TimelinePoint
	for rows.Next() {
		var p TimelinePoint
		if err := rows.Scan(&p.Minute, &p.Status, &p.Count); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// CountPendingTasks counts tasks waiting to run in a namespace.
func (db *DB) CountPendingTasks(ctx context.Context, namespace string) (int, error) {
	var n int
	err := db.q.QueryRowContext(ctx,
		`SELECT count(*) FROM tasks WHERE namespace = $1 AND status = 'QUEUED'`, namespace).Scan(&n)
	return n, err
}

// PickOptions constrain which tasks PickTasks may claim.
type PickOptions struct {
	// WorkerLabels holds the label set of every worker with a free slot. A
	// task's requirements must be a subset of at least one of them.
	WorkerLabels []map[string]string
}

// PickNextTask claims the most urgent runnable task, or returns nil.
func (db *DB) PickNextTask(ctx context.Context, opts PickOptions) (*Task, error) {
	tasks, err := db.PickTasks(ctx, opts, 1)
	if err != nil || len(tasks) == 0 {
		return nil, err
	}
	return tasks[0], nil
}

// PickTasks claims up to limit runnable tasks, most urgent first (by
// dispatch_key, which folds in priority aging). A task is runnable when it
// is due, its queue isn't paused or at its concurrency or rate limit, its
// namespace is under its concurrency limit, and some free worker has the
// labels it requires. SKIP LOCKED lets concurrent pickers claim different
// tasks.
//
// Limits hold within a batch too: candidates are ranked per queue and per
// namespace, and only as many as each limit still allows are claimed.
func (db *DB) PickTasks(ctx context.Context, opts PickOptions, limit int) ([]*Task, error) {
	if len(opts.WorkerLabels) == 0 || limit < 1 {
		return nil, nil
	}
	labels, err := json.Marshal(opts.WorkerLabels)
	if err != nil {
		return nil, err
	}

	query, args := db.fence(
		`WITH candidates AS (
			 SELECT t.id, t.namespace, t.queue, t.dispatch_key
			 FROM tasks t
			 LEFT JOIN queues q ON q.namespace = t.namespace AND q.name = t.queue
			 LEFT JOIN namespaces n ON n.name = t.namespace
			 WHERE t.status = 'QUEUED'
			   AND t.picked_at IS NULL
			   AND t.scheduled_at <= NOW()
			   AND NOT COALESCE(q.paused, false)
			   AND EXISTS (SELECT 1 FROM jsonb_array_elements($1::jsonb) w WHERE w @> t.requirements)
			   AND (q.concurrency_limit IS NULL OR q.concurrency_limit > (`+queueRunning+`))
			   AND (q.rate_limit IS NULL OR q.rate_limit > (`+queueRecent+`))
			   AND (n.max_concurrency IS NULL OR n.max_concurrency > (`+namespaceRunning+`))
			 ORDER BY t.dispatch_key
			 LIMIT $2 * 4
			 FOR UPDATE OF t SKIP LOCKED
		 ),
		 ranked AS (
			 SELECT c.*,
			        row_number() OVER (PARTITION BY c.namespace, c.queue ORDER BY c.dispatch_key) AS in_queue,
			        row_number() OVER (PARTITION BY c.namespace ORDER BY c.dispatch_key) AS in_namespace
			 FROM candidates c
		 ),
		 allowed AS (
			 SELECT t.id
			 FROM ranked t
			 LEFT JOIN queues q ON q.namespace = t.namespace AND q.name = t.queue
			 LEFT JOIN namespaces n ON n.name = t.namespace
			 WHERE (q.concurrency_limit IS NULL OR t.in_queue <= q.concurrency_limit - (`+queueRunning+`))
			   AND (q.rate_limit IS NULL OR t.in_queue <= q.rate_limit - (`+queueRecent+`))
			   AND (n.max_concurrency IS NULL OR t.in_namespace <= n.max_concurrency - (`+namespaceRunning+`))
			 ORDER BY t.dispatch_key
			 LIMIT $2
		 )
		 UPDATE tasks
		 SET picked_at = NOW(), last_dispatched_at = NOW(), attempt = attempt + 1
		 WHERE id IN (SELECT id FROM allowed) `+fenceMarker+`
		 RETURNING `+taskColumns,
		labels, limit,
	)
	rows, err := db.q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to pick tasks: %w", err)
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
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to pick tasks: %w", err)
	}
	if len(tasks) == 0 {
		return nil, db.unchanged(ctx)
	}
	// RETURNING has no order; restore dispatch order.
	slices.SortFunc(tasks, func(a, b *Task) int {
		if a.dispatchKey == nil || b.dispatchKey == nil {
			return 0
		}
		return a.dispatchKey.Compare(*b.dispatchKey)
	})
	return tasks, nil
}

// Counts used by the pick query, correlated on the candidate row t.
const (
	queueRunning = `SELECT count(*) FROM tasks r WHERE r.namespace = t.namespace AND r.queue = t.queue
		AND r.picked_at IS NOT NULL AND r.status IN ('QUEUED', 'STARTED')`
	queueRecent = `SELECT count(*) FROM tasks r WHERE r.namespace = t.namespace AND r.queue = t.queue
		AND r.last_dispatched_at > NOW() - make_interval(secs => q.rate_period_seconds)`
	namespaceRunning = `SELECT count(*) FROM tasks r WHERE r.namespace = t.namespace
		AND r.picked_at IS NOT NULL AND r.status IN ('QUEUED', 'STARTED')`
)

// SetPriorityAging stores the aging interval used for dispatch order and,
// if it changed, re-keys every queued task.
func (db *DB) SetPriorityAging(ctx context.Context, interval time.Duration) error {
	return db.WithTx(ctx, func(tx *DB) error {
		result, err := tx.q.ExecContext(ctx,
			`UPDATE scheduler_settings SET priority_aging_seconds = $1
			 WHERE id = 1 AND priority_aging_seconds IS DISTINCT FROM $1`, interval.Seconds())
		if err != nil {
			return fmt.Errorf("failed to save priority aging: %w", err)
		}
		if changed, _ := rowsChanged(result); !changed {
			return nil
		}
		_, err = tx.q.ExecContext(ctx,
			`UPDATE tasks SET dispatch_key = conductor_dispatch_key(priority, scheduled_at) WHERE status = 'QUEUED'`)
		return err
	})
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

// The transitions below only apply to the current dispatch of a task: it must
// be picked and unfinished, and the report must quote the current attempt.
// A late or duplicate report, or one from an earlier dispatch of the same
// task, therefore changes nothing. Each returns whether the task was updated.
const dispatchedCondition = `status IN ('QUEUED', 'STARTED') AND picked_at IS NOT NULL`

// maxRetryDelay caps the exponential back-off between retries.
const maxRetryDelay = time.Hour

// RetryTask requeues a failed task with exponential back-off if it has retries
// left, keeping the failed attempt's output for debugging. It returns false
// when the retries are used up.
func (db *DB) RetryTask(ctx context.Context, id uuid.UUID, attempt int, output, errorMessage string) (bool, error) {
	// One statement: lock the attempt, record it in task_attempts, and
	// requeue the task.
	var n int
	err := db.q.QueryRowContext(ctx,
		`WITH failed AS (
			 SELECT id, attempt, worker_id, COALESCE(started_at, picked_at) AS started
			 FROM tasks
			 WHERE id = $1
			   AND `+dispatchedCondition+`
			   AND attempt = $5
			   AND retry_count < max_retries
			 FOR UPDATE
		 ), retried AS (
			 UPDATE tasks t
			 SET retry_count = retry_count + 1,
			     status = 'QUEUED',
			     picked_at = NULL,
			     started_at = NULL,
			     failed_at = NULL,
			     error_message = $2,
			     output = $4,
			     scheduled_at = NOW() + make_interval(secs => LEAST(retry_delay_seconds * POWER(2, retry_count), $3))
			 FROM failed WHERE t.id = failed.id
			 RETURNING t.id
		 ), kept AS (
			 INSERT INTO task_attempts (task_id, attempt, worker_id, started_at, finished_at, status, error_message, output)
			 SELECT f.id, f.attempt, f.worker_id, f.started, NOW(), 'FAILED', NULLIF($2, ''), NULLIF($4, '')
			 FROM failed f JOIN retried USING (id)
			 ON CONFLICT DO NOTHING
		 )
		 SELECT count(*) FROM retried`,
		id, errorMessage, maxRetryDelay.Seconds(), output, attempt,
	).Scan(&n)
	if err != nil {
		return false, fmt.Errorf("failed to retry task: %w", err)
	}
	return n > 0, nil
}

func (db *DB) MarkTaskStarted(ctx context.Context, id uuid.UUID, attempt int, workerID int64) (bool, error) {
	query, args := db.fence(
		`UPDATE tasks SET status = 'STARTED', started_at = NOW(), worker_id = $2
		 WHERE id = $1 AND status = 'QUEUED' AND picked_at IS NOT NULL AND attempt = $3 `+fenceMarker,
		id, workerID, attempt,
	)
	result, err := db.q.ExecContext(ctx, query, args...)
	if err != nil {
		return false, fmt.Errorf("failed to mark task started: %w", err)
	}
	changed, err := rowsChanged(result)
	if err == nil && !changed {
		err = db.unchanged(ctx)
	}
	return changed, err
}

// TaskResult is returned by the terminal transitions: whether the task was
// updated, and the workflow it belongs to (if any) so it can be advanced.
type TaskResult struct {
	Updated    bool
	WorkflowID *uuid.UUID
}

func (db *DB) finish(ctx context.Context, query string, args ...any) (TaskResult, error) {
	var r TaskResult
	err := db.q.QueryRowContext(ctx, query, args...).Scan(&r.WorkflowID)
	if errors.Is(err, sql.ErrNoRows) {
		return TaskResult{}, nil
	}
	if err != nil {
		return TaskResult{}, err
	}
	r.Updated = true
	return r, nil
}

// finishColumns fill in what the STARTED transition records, for a task
// whose result arrived before the coordinator marked it started. $5 is the
// worker ID (0 when unknown).
const finishColumns = `started_at = COALESCE(started_at, picked_at), worker_id = COALESCE(NULLIF($5::bigint, 0), worker_id)`

func (db *DB) MarkTaskCompleted(ctx context.Context, id uuid.UUID, attempt int, workerID int64, output string, outputs StringMap) (TaskResult, error) {
	query, args := db.fence(
		`UPDATE tasks SET status = 'COMPLETED', completed_at = NOW(), output = $2, outputs = $3, error_message = NULL,
		     `+finishColumns+`
		 WHERE id = $1 AND `+dispatchedCondition+` AND attempt = $4 `+fenceMarker+`
		 RETURNING workflow_id`,
		id, output, outputs, attempt, workerID,
	)
	r, err := db.finish(ctx, query, args...)
	if err == nil && !r.Updated {
		err = db.unchanged(ctx)
	}
	if err != nil {
		return r, fmt.Errorf("failed to mark task completed: %w", err)
	}
	return r, nil
}

func (db *DB) MarkTaskFailed(ctx context.Context, id uuid.UUID, attempt int, workerID int64, output, errorMessage string) (TaskResult, error) {
	r, err := db.finish(ctx,
		`UPDATE tasks SET status = 'FAILED', failed_at = NOW(), output = $2, error_message = $3,
		     `+finishColumns+`
		 WHERE id = $1 AND `+dispatchedCondition+` AND attempt = $4
		 RETURNING workflow_id`,
		id, output, errorMessage, attempt, workerID,
	)
	if err != nil {
		return r, fmt.Errorf("failed to mark task failed: %w", err)
	}
	return r, nil
}

// CancelTask cancels a task that hasn't finished. It returns the task as it
// was before cancelling, or nil if the task doesn't exist or already finished.
func (db *DB) CancelTask(ctx context.Context, id uuid.UUID) (*Task, error) {
	// RETURNING the CTE's columns yields the pre-update row, so callers can
	// tell whether a worker was running the task.
	t, err := scanTask(db.q.QueryRowContext(ctx,
		`WITH before AS (SELECT `+taskColumns+` FROM tasks WHERE id = $1 FOR UPDATE)
		 UPDATE tasks t SET status = 'CANCELLED', cancelled_at = NOW(), error_message = 'cancelled'
		 FROM before
		 WHERE t.id = before.id AND before.status IN ('QUEUED', 'STARTED')
		 RETURNING `+prefixColumns("before", taskColumns),
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

// RequeueFailedTask gives a permanently failed task a fresh set of retries,
// e.g. after fixing whatever made it fail. Workflow steps can't be requeued:
// their workflow has already moved on. It returns nil if the task isn't
// eligible.
func (db *DB) RequeueFailedTask(ctx context.Context, id uuid.UUID) (*Task, error) {
	// The failed attempt is kept in task_attempts before the reset.
	t, err := scanTask(db.q.QueryRowContext(ctx,
		`WITH failed AS (
			 SELECT id, attempt, worker_id, COALESCE(started_at, picked_at) AS started, failed_at, error_message, output
			 FROM tasks WHERE id = $1 AND status = 'FAILED' AND workflow_id IS NULL
			 FOR UPDATE
		 ), kept AS (
			 INSERT INTO task_attempts (task_id, attempt, worker_id, started_at, finished_at, status, error_message, output)
			 SELECT id, attempt, worker_id, started, COALESCE(failed_at, NOW()), 'FAILED', error_message, output
			 FROM failed WHERE attempt > 0
			 ON CONFLICT DO NOTHING
		 )
		 UPDATE tasks t
		 SET status = 'QUEUED', retry_count = 0, picked_at = NULL, started_at = NULL,
		     failed_at = NULL, error_message = NULL, scheduled_at = NOW()
		 FROM failed WHERE t.id = failed.id
		 RETURNING `+prefixColumns("t", taskColumns),
		id,
	))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to requeue task: %w", err)
	}
	return t, nil
}

// AttemptRecord is an earlier attempt of a task, kept when it failed.
type AttemptRecord struct {
	Attempt      int
	WorkerID     *int64
	StartedAt    *time.Time
	FinishedAt   time.Time
	Status       string
	ErrorMessage string
	Output       string
}

// ListAttempts returns a task's earlier attempts, oldest first. The latest
// attempt is the task row itself.
func (db *DB) ListAttempts(ctx context.Context, id uuid.UUID) ([]AttemptRecord, error) {
	rows, err := db.q.QueryContext(ctx,
		`SELECT attempt, worker_id, started_at, finished_at, status, COALESCE(error_message, ''), COALESCE(output, '')
		 FROM task_attempts WHERE task_id = $1 ORDER BY attempt`, id)
	if err != nil {
		return nil, fmt.Errorf("failed to list attempts: %w", err)
	}
	defer rows.Close()
	var out []AttemptRecord
	for rows.Next() {
		var a AttemptRecord
		if err := rows.Scan(&a.Attempt, &a.WorkerID, &a.StartedAt, &a.FinishedAt, &a.Status, &a.ErrorMessage, &a.Output); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// TaskAttempt identifies one dispatch of a task.
type TaskAttempt struct {
	ID      uuid.UUID
	Attempt int
}

// ListOverdueTasks returns STARTED tasks that have run past their timeout plus
// grace without the worker reporting a result, i.e. the worker was lost.
func (db *DB) ListOverdueTasks(ctx context.Context, grace time.Duration) ([]TaskAttempt, error) {
	rows, err := db.q.QueryContext(ctx,
		`SELECT id, attempt FROM tasks
		 WHERE status = 'STARTED'
		   AND started_at + make_interval(secs => timeout_seconds + $1::float8) < NOW()`,
		grace.Seconds(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to list overdue tasks: %w", err)
	}
	defer rows.Close()

	var out []TaskAttempt
	for rows.Next() {
		var a TaskAttempt
		if err := rows.Scan(&a.ID, &a.Attempt); err != nil {
			return nil, fmt.Errorf("failed to scan task: %w", err)
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// DispatchedTask is a task currently handed to a worker, as a new leader
// sees it when it takes over.
type DispatchedTask struct {
	ID             uuid.UUID
	Namespace      string
	Queue          string
	Attempt        int
	WorkerID       *int64
	StartedAt      *time.Time
	TimeoutSeconds int
}

// ListDispatchedTasks returns every task that is picked and unfinished.
func (db *DB) ListDispatchedTasks(ctx context.Context) ([]DispatchedTask, error) {
	rows, err := db.q.QueryContext(ctx,
		`SELECT id, namespace, queue, attempt, worker_id, started_at, timeout_seconds FROM tasks
		 WHERE picked_at IS NOT NULL AND status IN ('QUEUED', 'STARTED')`)
	if err != nil {
		return nil, fmt.Errorf("failed to list dispatched tasks: %w", err)
	}
	defer rows.Close()

	var out []DispatchedTask
	for rows.Next() {
		var d DispatchedTask
		if err := rows.Scan(&d.ID, &d.Namespace, &d.Queue, &d.Attempt, &d.WorkerID, &d.StartedAt, &d.TimeoutSeconds); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// NextDueAt returns when the earliest waiting task becomes due, or nil if
// none is waiting for a future time. The dispatcher sleeps until then.
func (db *DB) NextDueAt(ctx context.Context) (*time.Time, error) {
	var t *time.Time
	err := db.q.QueryRowContext(ctx,
		`SELECT min(scheduled_at) FROM tasks
		 WHERE status = 'QUEUED' AND picked_at IS NULL AND scheduled_at > NOW()`).Scan(&t)
	if err != nil {
		return nil, fmt.Errorf("failed to find next due task: %w", err)
	}
	if t != nil {
		// scheduled_at has no time zone and holds UTC.
		u := time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.UTC)
		t = &u
	}
	return t, nil
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

// prefixColumns qualifies each column in a comma-separated list with table.
func prefixColumns(table, columns string) string {
	var out []byte
	start := true
	for i := 0; i < len(columns); i++ {
		c := columns[i]
		if start && c != ' ' && c != '\t' && c != '\n' {
			out = append(out, table...)
			out = append(out, '.')
			start = false
		}
		out = append(out, c)
		if c == ',' {
			start = true
		}
	}
	return string(out)
}
