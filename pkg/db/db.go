package db

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

type TaskStatus string

const (
	StatusQueued    TaskStatus = "QUEUED"
	StatusStarted   TaskStatus = "STARTED"
	StatusCompleted TaskStatus = "COMPLETED"
	StatusFailed    TaskStatus = "FAILED"
)

type Task struct {
	ID                uuid.UUID
	Data              string
	Status            TaskStatus
	ScheduledAt       time.Time
	PickedAt          *time.Time
	StartedAt         *time.Time
	CompletedAt       *time.Time
	FailedAt          *time.Time
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

type DB struct {
	conn *sql.DB
}

func New() (*DB, error) {
	host := os.Getenv("POSTGRES_HOST")
	port := os.Getenv("POSTGRES_PORT")
	user := os.Getenv("POSTGRES_USER")
	password := os.Getenv("POSTGRES_PASSWORD")
	dbname := os.Getenv("POSTGRES_DB")

	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)

	conn, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	if err := conn.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &DB{conn: conn}, nil
}

func (db *DB) Close() error {
	return db.conn.Close()
}

func (db *DB) CreateTaskWithOptions(data string, opts TaskOptions) (*Task, error) {
	if opts.Priority == 0 {
		opts.Priority = 5
	}
	if opts.MaxRetries == 0 {
		opts.MaxRetries = 3
	}
	if opts.RetryDelaySeconds == 0 {
		opts.RetryDelaySeconds = 60
	}
	if opts.TimeoutSeconds == 0 {
		opts.TimeoutSeconds = 300
	}
	if opts.ScheduledAt.IsZero() {
		opts.ScheduledAt = time.Now().UTC()
	}

	task := &Task{
		Data:              data,
		Status:            StatusQueued,
		Priority:          opts.Priority,
		MaxRetries:        opts.MaxRetries,
		RetryDelaySeconds: opts.RetryDelaySeconds,
		TimeoutSeconds:    opts.TimeoutSeconds,
		ScheduledAt:       opts.ScheduledAt,
	}

	err := db.conn.QueryRow(
		`INSERT INTO tasks (data, status, priority, max_retries, retry_delay_seconds, timeout_seconds, scheduled_at) 
		 VALUES ($1, $2, $3, $4, $5, $6, $7) 
		 RETURNING id, created_at`,
		data, StatusQueued, opts.Priority, opts.MaxRetries, opts.RetryDelaySeconds, opts.TimeoutSeconds, opts.ScheduledAt,
	).Scan(&task.ID, &task.CreatedAt)

	if err != nil {
		return nil, fmt.Errorf("failed to create task: %w", err)
	}

	return task, nil
}

func (db *DB) CreateTask(data string) (*Task, error) {
	return db.CreateTaskWithOptions(data, DefaultTaskOptions())
}

func (db *DB) CreateTaskScheduled(data string, scheduledAt time.Time) (*Task, error) {
	opts := DefaultTaskOptions()
	opts.ScheduledAt = scheduledAt
	return db.CreateTaskWithOptions(data, opts)
}

func (db *DB) GetTask(taskID uuid.UUID) (*Task, error) {
	task := &Task{}

	var output, errorMsg sql.NullString

	err := db.conn.QueryRow(
		`SELECT id, data, status, scheduled_at, picked_at, started_at, completed_at, failed_at,
		        priority, max_retries, retry_count, retry_delay_seconds, timeout_seconds,
		        output, error_message, created_at
		 FROM tasks WHERE id = $1`,
		taskID,
	).Scan(&task.ID, &task.Data, &task.Status, &task.ScheduledAt,
		&task.PickedAt, &task.StartedAt, &task.CompletedAt, &task.FailedAt,
		&task.Priority, &task.MaxRetries, &task.RetryCount, &task.RetryDelaySeconds, &task.TimeoutSeconds,
		&output, &errorMsg, &task.CreatedAt)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get task: %w", err)
	}

	task.Output = output.String
	task.ErrorMessage = errorMsg.String

	return task, nil
}

func (db *DB) PickNextTask() (*Task, error) {
	task := &Task{}

	err := db.conn.QueryRow(
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
		 RETURNING id, data, status, scheduled_at, picked_at, priority, 
		           max_retries, retry_count, retry_delay_seconds, timeout_seconds`,
	).Scan(&task.ID, &task.Data, &task.Status, &task.ScheduledAt, &task.PickedAt,
		&task.Priority, &task.MaxRetries, &task.RetryCount, &task.RetryDelaySeconds, &task.TimeoutSeconds)

	if err == sql.ErrNoRows {
		return nil, nil // No task available
	}
	if err != nil {
		return nil, fmt.Errorf("failed to pick task: %w", err)
	}

	return task, nil
}

// RequeueTask puts a task that was picked but never started back in the queue,
// e.g. when no worker could accept it.
func (db *DB) RequeueTask(taskID uuid.UUID) error {
	_, err := db.conn.Exec(
		`UPDATE tasks SET picked_at = NULL WHERE id = $1 AND status = 'QUEUED'`,
		taskID,
	)
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
func (db *DB) RetryTask(taskID uuid.UUID, errorMessage string) (bool, error) {
	result, err := db.conn.Exec(
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
		taskID, errorMessage, maxRetryDelay.Seconds(),
	)
	if err != nil {
		return false, fmt.Errorf("failed to retry task: %w", err)
	}
	return rowsChanged(result)
}

func (db *DB) MarkTaskStarted(taskID uuid.UUID) (bool, error) {
	result, err := db.conn.Exec(
		`UPDATE tasks SET status = 'STARTED', started_at = NOW()
		 WHERE id = $1 AND status = 'QUEUED' AND picked_at IS NOT NULL`,
		taskID,
	)
	if err != nil {
		return false, fmt.Errorf("failed to mark task started: %w", err)
	}
	return rowsChanged(result)
}

func (db *DB) MarkTaskCompleted(taskID uuid.UUID, output string) (bool, error) {
	result, err := db.conn.Exec(
		`UPDATE tasks SET status = 'COMPLETED', completed_at = NOW(), output = $2, error_message = NULL
		 WHERE id = $1 AND `+dispatchedCondition,
		taskID, output,
	)
	if err != nil {
		return false, fmt.Errorf("failed to mark task completed: %w", err)
	}
	return rowsChanged(result)
}

func (db *DB) MarkTaskFailed(taskID uuid.UUID, output, errorMessage string) (bool, error) {
	result, err := db.conn.Exec(
		`UPDATE tasks SET status = 'FAILED', failed_at = NOW(), output = $2, error_message = $3
		 WHERE id = $1 AND `+dispatchedCondition,
		taskID, output, errorMessage,
	)
	if err != nil {
		return false, fmt.Errorf("failed to mark task failed: %w", err)
	}
	return rowsChanged(result)
}

// ListOverdueTasks returns STARTED tasks that have run past their timeout plus
// grace without the worker reporting a result, i.e. the worker was lost.
func (db *DB) ListOverdueTasks(grace time.Duration) ([]uuid.UUID, error) {
	rows, err := db.conn.Query(
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

func rowsChanged(result sql.Result) (bool, error) {
	n, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

func (db *DB) ListTasksByStatus(status TaskStatus, limit int) ([]*Task, error) {
	rows, err := db.conn.Query(
		`SELECT id, data, status, scheduled_at, picked_at, started_at, completed_at, failed_at,
		        priority, max_retries, retry_count, timeout_seconds, created_at
		 FROM tasks 
		 WHERE status = $1 
		 ORDER BY priority ASC, scheduled_at ASC
		 LIMIT $2`,
		status, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to list tasks: %w", err)
	}
	defer rows.Close()

	var tasks []*Task
	for rows.Next() {
		task := &Task{}
		err := rows.Scan(&task.ID, &task.Data, &task.Status, &task.ScheduledAt,
			&task.PickedAt, &task.StartedAt, &task.CompletedAt, &task.FailedAt,
			&task.Priority, &task.MaxRetries, &task.RetryCount, &task.TimeoutSeconds, &task.CreatedAt)
		if err != nil {
			return nil, fmt.Errorf("failed to scan task: %w", err)
		}
		tasks = append(tasks, task)
	}

	return tasks, nil
}

// --- Workflow types and constants ---

type WorkflowStatus string
type StepStatus string

const (
	WorkflowRunning      WorkflowStatus = "RUNNING"
	WorkflowCompensating WorkflowStatus = "COMPENSATING"
	WorkflowCompleted    WorkflowStatus = "COMPLETED"
	WorkflowFailed       WorkflowStatus = "FAILED"
)

const (
	StepPending      StepStatus = "PENDING"
	StepRunning      StepStatus = "RUNNING"
	StepCompleted    StepStatus = "COMPLETED"
	StepFailed       StepStatus = "FAILED"
	StepCompensating StepStatus = "COMPENSATING"
	StepCompensated  StepStatus = "COMPENSATED"
)

type Workflow struct {
	ID           uuid.UUID
	Type         string
	Status       WorkflowStatus
	CurrentStep  int
	Context      json.RawMessage
	ErrorMessage string
	CreatedAt    time.Time
	UpdatedAt    time.Time
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

// --- Workflow CRUD ---

func (db *DB) CreateWorkflow(wfType string, ctx json.RawMessage, totalSteps int) (*Workflow, error) {
	if ctx == nil {
		ctx = json.RawMessage("{}")
	}
	wf := &Workflow{
		Type:    wfType,
		Status:  WorkflowRunning,
		Context: ctx,
	}
	err := db.conn.QueryRow(
		`INSERT INTO workflows (type, status, current_step, context)
		 VALUES ($1, $2, 0, $3)
		 RETURNING id, created_at, updated_at`,
		wfType, WorkflowRunning, ctx,
	).Scan(&wf.ID, &wf.CreatedAt, &wf.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("failed to create workflow: %w", err)
	}
	return wf, nil
}

func (db *DB) GetWorkflow(id uuid.UUID) (*Workflow, error) {
	wf := &Workflow{}
	var errMsg sql.NullString
	err := db.conn.QueryRow(
		`SELECT id, type, status, current_step, context, error_message, created_at, updated_at
		 FROM workflows WHERE id = $1`, id,
	).Scan(&wf.ID, &wf.Type, &wf.Status, &wf.CurrentStep, &wf.Context, &errMsg, &wf.CreatedAt, &wf.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get workflow: %w", err)
	}
	wf.ErrorMessage = errMsg.String
	return wf, nil
}

func (db *DB) UpdateWorkflowStatus(id uuid.UUID, status WorkflowStatus, errMsg string) error {
	_, err := db.conn.Exec(
		`UPDATE workflows SET status = $2, error_message = $3, updated_at = NOW() WHERE id = $1`,
		id, status, errMsg,
	)
	return err
}

func (db *DB) AdvanceWorkflowStep(id uuid.UUID, step int) error {
	_, err := db.conn.Exec(
		`UPDATE workflows SET current_step = $2, updated_at = NOW() WHERE id = $1`,
		id, step,
	)
	return err
}

func (db *DB) ListWorkflows(limit int) ([]*Workflow, error) {
	rows, err := db.conn.Query(
		`SELECT id, type, status, current_step, context, error_message, created_at, updated_at
		 FROM workflows ORDER BY created_at DESC LIMIT $1`, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to list workflows: %w", err)
	}
	defer rows.Close()

	var workflows []*Workflow
	for rows.Next() {
		wf := &Workflow{}
		var errMsg sql.NullString
		if err := rows.Scan(&wf.ID, &wf.Type, &wf.Status, &wf.CurrentStep, &wf.Context, &errMsg, &wf.CreatedAt, &wf.UpdatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan workflow: %w", err)
		}
		wf.ErrorMessage = errMsg.String
		workflows = append(workflows, wf)
	}
	return workflows, nil
}

// --- WorkflowStep CRUD ---

func (db *DB) CreateWorkflowStep(workflowID uuid.UUID, stepNumber int, name string) (*WorkflowStep, error) {
	step := &WorkflowStep{
		WorkflowID: workflowID,
		StepNumber: stepNumber,
		Name:       name,
		Status:     StepPending,
	}
	err := db.conn.QueryRow(
		`INSERT INTO workflow_steps (workflow_id, step_number, name, status)
		 VALUES ($1, $2, $3, $4)
		 RETURNING id, created_at, updated_at`,
		workflowID, stepNumber, name, StepPending,
	).Scan(&step.ID, &step.CreatedAt, &step.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("failed to create workflow step: %w", err)
	}
	return step, nil
}

func (db *DB) GetWorkflowSteps(workflowID uuid.UUID) ([]*WorkflowStep, error) {
	rows, err := db.conn.Query(
		`SELECT id, workflow_id, step_number, name, task_id, compensation_task_id, status, created_at, updated_at
		 FROM workflow_steps WHERE workflow_id = $1 ORDER BY step_number ASC`, workflowID,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get workflow steps: %w", err)
	}
	defer rows.Close()

	var steps []*WorkflowStep
	for rows.Next() {
		s := &WorkflowStep{}
		if err := rows.Scan(&s.ID, &s.WorkflowID, &s.StepNumber, &s.Name, &s.TaskID, &s.CompensationTaskID, &s.Status, &s.CreatedAt, &s.UpdatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan workflow step: %w", err)
		}
		steps = append(steps, s)
	}
	return steps, nil
}

func (db *DB) UpdateStepStatus(stepID uuid.UUID, status StepStatus) error {
	_, err := db.conn.Exec(
		`UPDATE workflow_steps SET status = $2, updated_at = NOW() WHERE id = $1`,
		stepID, status,
	)
	return err
}

func (db *DB) LinkTaskToStep(stepID uuid.UUID, taskID uuid.UUID) error {
	_, err := db.conn.Exec(
		`UPDATE workflow_steps SET task_id = $2, updated_at = NOW() WHERE id = $1`,
		stepID, taskID,
	)
	return err
}

func (db *DB) LinkCompensationTaskToStep(stepID uuid.UUID, taskID uuid.UUID) error {
	_, err := db.conn.Exec(
		`UPDATE workflow_steps SET compensation_task_id = $2, updated_at = NOW() WHERE id = $1`,
		stepID, taskID,
	)
	return err
}

func (db *DB) GetStepByTaskID(taskID uuid.UUID) (*WorkflowStep, error) {
	s := &WorkflowStep{}
	err := db.conn.QueryRow(
		`SELECT id, workflow_id, step_number, name, task_id, compensation_task_id, status, created_at, updated_at
		 FROM workflow_steps WHERE task_id = $1 OR compensation_task_id = $1`, taskID,
	).Scan(&s.ID, &s.WorkflowID, &s.StepNumber, &s.Name, &s.TaskID, &s.CompensationTaskID, &s.Status, &s.CreatedAt, &s.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get step by task: %w", err)
	}
	return s, nil
}

func (db *DB) ResetStaleTasks(staleThreshold time.Duration) (int64, error) {
	result, err := db.conn.Exec(
		`UPDATE tasks 
		 SET picked_at = NULL, status = 'QUEUED'
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
