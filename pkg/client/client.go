// Package client is a Go SDK for Conductor's HTTP API.
//
//	c := client.New("http://localhost:8081", os.Getenv("CONDUCTOR_API_KEY"))
//	task, err := c.SubmitTask(ctx, client.TaskRequest{Command: "echo hello"})
//	task, err = c.WaitForTask(ctx, task.ID)
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type Client struct {
	baseURL   string
	apiKey    string
	namespace string
	http      *http.Client
}

// New returns a client for the API at baseURL, e.g. "http://localhost:8081".
func New(baseURL, apiKey string) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		apiKey:  apiKey,
		http:    &http.Client{Timeout: 30 * time.Second},
	}
}

// WithNamespace returns a copy of the client that acts in another namespace.
// Only admin keys may switch namespaces; other keys are bound to their own.
func (c *Client) WithNamespace(ns string) *Client {
	cp := *c
	cp.namespace = ns
	return &cp
}

// APIError is returned for non-2xx responses.
type APIError struct {
	StatusCode int
	Message    string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("conductor API: %s (HTTP %d)", e.Message, e.StatusCode)
}

// StatusCode returns the HTTP status of an API error, or 0.
func StatusCode(err error) int {
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return apiErr.StatusCode
	}
	return 0
}

// IsNotFound reports whether err is an API 404.
func IsNotFound(err error) bool { return StatusCode(err) == http.StatusNotFound }

// do sends a request. body may be nil, a []byte (sent as-is), or a value
// marshalled to JSON. It returns the HTTP status code.
func (c *Client) do(ctx context.Context, method, path string, body, out any) (int, error) {
	var reader io.Reader
	contentType := "application/json"
	switch b := body.(type) {
	case nil:
	case []byte:
		reader = bytes.NewReader(b)
		contentType = "application/yaml"
	default:
		raw, err := json.Marshal(body)
		if err != nil {
			return 0, err
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
	if err != nil {
		return 0, err
	}
	if body != nil {
		req.Header.Set("Content-Type", contentType)
	}
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}
	if c.namespace != "" {
		req.Header.Set("X-Conductor-Namespace", c.namespace)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		var e struct {
			Error string `json:"error"`
		}
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		if json.Unmarshal(raw, &e) != nil || e.Error == "" {
			e.Error = strings.TrimSpace(string(raw))
		}
		return resp.StatusCode, &APIError{StatusCode: resp.StatusCode, Message: e.Error}
	}
	if out == nil {
		return resp.StatusCode, nil
	}
	if raw, ok := out.(*[]byte); ok {
		*raw, err = io.ReadAll(resp.Body)
		return resp.StatusCode, err
	}
	return resp.StatusCode, json.NewDecoder(resp.Body).Decode(out)
}

// Health checks that the API is up and can reach its database.
func (c *Client) Health(ctx context.Context) error {
	_, err := c.do(ctx, http.MethodGet, "/health", nil, nil)
	return err
}

// --- Tasks ---

type HTTPSpec struct {
	Method       string            `json:"method,omitempty"`
	URL          string            `json:"url"`
	Headers      map[string]string `json:"headers,omitempty"`
	Body         string            `json:"body,omitempty"`
	ExpectStatus []int             `json:"expect_status,omitempty"`
}

type ContainerSpec struct {
	Image   string   `json:"image"`
	Command []string `json:"command,omitempty"`
	Memory  string   `json:"memory,omitempty"`
	CPUs    float64  `json:"cpus,omitempty"`
	Network string   `json:"network,omitempty"`
}

type TaskRequest struct {
	Type              string            `json:"type,omitempty"` // shell (default), http, container
	Command           string            `json:"command,omitempty"`
	HTTP              *HTTPSpec         `json:"http,omitempty"`
	Container         *ContainerSpec    `json:"container,omitempty"`
	Env               map[string]string `json:"env,omitempty"`
	Labels            map[string]string `json:"labels,omitempty"`
	Queue             string            `json:"queue,omitempty"`
	Priority          int               `json:"priority,omitempty"`
	MaxRetries        *int              `json:"max_retries,omitempty"`
	RetryDelaySeconds int               `json:"retry_delay_seconds,omitempty"`
	TimeoutSeconds    int               `json:"timeout_seconds,omitempty"`
	DelaySeconds      int               `json:"delay_seconds,omitempty"`
	ScheduledAt       int64             `json:"scheduled_at,omitempty"`
	IdempotencyKey    string            `json:"idempotency_key,omitempty"`
}

// Retries is a helper for TaskRequest.MaxRetries.
func Retries(n int) *int { return &n }

type Task struct {
	ID             string            `json:"id"`
	Namespace      string            `json:"namespace"`
	Queue          string            `json:"queue"`
	Type           string            `json:"type"`
	Command        string            `json:"command"`
	Spec           json.RawMessage   `json:"spec,omitempty"`
	Labels         map[string]string `json:"labels,omitempty"`
	Status         string            `json:"status"`
	Priority       int               `json:"priority"`
	MaxRetries     int               `json:"max_retries"`
	RetryCount     int               `json:"retry_count"`
	TimeoutSeconds int               `json:"timeout_seconds"`
	ScheduledAt    time.Time         `json:"scheduled_at"`
	PickedAt       *time.Time        `json:"picked_at,omitempty"`
	StartedAt      *time.Time        `json:"started_at,omitempty"`
	CompletedAt    *time.Time        `json:"completed_at,omitempty"`
	FailedAt       *time.Time        `json:"failed_at,omitempty"`
	CancelledAt    *time.Time        `json:"cancelled_at,omitempty"`
	Output         string            `json:"output,omitempty"`
	Outputs        map[string]string `json:"outputs,omitempty"`
	ErrorMessage   string            `json:"error_message,omitempty"`
	IdempotencyKey string            `json:"idempotency_key,omitempty"`
	WorkflowID     string            `json:"workflow_id,omitempty"`
	WorkerID       *int64            `json:"worker_id,omitempty"`
	CreatedAt      time.Time         `json:"created_at"`

	// Created is false when SubmitTask matched an existing idempotency key.
	Created bool `json:"-"`
}

// Done reports whether the task reached a final status.
func (t *Task) Done() bool {
	return t.Status == "COMPLETED" || t.Status == "FAILED" || t.Status == "CANCELLED"
}

func (c *Client) SubmitTask(ctx context.Context, req TaskRequest) (*Task, error) {
	var t Task
	code, err := c.do(ctx, http.MethodPost, "/v1/tasks", req, &t)
	t.Created = code == http.StatusCreated
	return &t, err
}

func (c *Client) GetTask(ctx context.Context, id string) (*Task, error) {
	var t Task
	_, err := c.do(ctx, http.MethodGet, "/v1/tasks/"+url.PathEscape(id), nil, &t)
	return &t, err
}

type TaskFilter struct {
	Status     string
	Queue      string
	WorkflowID string
	Limit      int
}

// ListTasks returns recent tasks, newest first.
func (c *Client) ListTasks(ctx context.Context, f TaskFilter) ([]Task, error) {
	q := url.Values{}
	for k, v := range map[string]string{"status": f.Status, "queue": f.Queue, "workflow_id": f.WorkflowID} {
		if v != "" {
			q.Set(k, v)
		}
	}
	if f.Limit > 0 {
		q.Set("limit", strconv.Itoa(f.Limit))
	}
	var tasks []Task
	_, err := c.do(ctx, http.MethodGet, "/v1/tasks?"+q.Encode(), nil, &tasks)
	return tasks, err
}

func (c *Client) CancelTask(ctx context.Context, id string) (*Task, error) {
	var t Task
	_, err := c.do(ctx, http.MethodPost, "/v1/tasks/"+url.PathEscape(id)+"/cancel", nil, &t)
	return &t, err
}

// RequeueTask gives a permanently failed task a fresh set of retries.
func (c *Client) RequeueTask(ctx context.Context, id string) (*Task, error) {
	var t Task
	_, err := c.do(ctx, http.MethodPost, "/v1/tasks/"+url.PathEscape(id)+"/requeue", nil, &t)
	return &t, err
}

// DeadLetter lists permanently failed tasks that aren't workflow steps.
func (c *Client) DeadLetter(ctx context.Context, limit int) ([]Task, error) {
	path := "/v1/dead-letter"
	if limit > 0 {
		path += "?limit=" + strconv.Itoa(limit)
	}
	var tasks []Task
	_, err := c.do(ctx, http.MethodGet, path, nil, &tasks)
	return tasks, err
}

// Stats returns task counts by status, plus "total".
func (c *Client) Stats(ctx context.Context) (map[string]int, error) {
	var stats map[string]int
	_, err := c.do(ctx, http.MethodGet, "/v1/stats", nil, &stats)
	return stats, err
}

// WaitForTask polls until the task finishes or ctx is done.
func (c *Client) WaitForTask(ctx context.Context, id string) (*Task, error) {
	return poll(ctx, func() (*Task, bool, error) {
		t, err := c.GetTask(ctx, id)
		if err != nil {
			return nil, false, err
		}
		return t, t.Done(), nil
	})
}

// --- Workflow definitions ---

type Definition struct {
	Name        string          `json:"name"`
	Version     int             `json:"version"`
	Description string          `json:"description,omitempty"`
	Steps       int             `json:"steps"`
	CreatedAt   time.Time       `json:"created_at"`
	Definition  json.RawMessage `json:"definition,omitempty"`

	// Created is false when the uploaded definition matched the latest version.
	Created bool `json:"-"`
}

// ApplyDefinition uploads a workflow definition (YAML or JSON). An unchanged
// definition doesn't create a new version.
func (c *Client) ApplyDefinition(ctx context.Context, spec []byte) (*Definition, error) {
	var d Definition
	code, err := c.do(ctx, http.MethodPut, "/v1/workflow-definitions", spec, &d)
	d.Created = code == http.StatusCreated
	return &d, err
}

func (c *Client) ListDefinitions(ctx context.Context) ([]Definition, error) {
	var defs []Definition
	_, err := c.do(ctx, http.MethodGet, "/v1/workflow-definitions", nil, &defs)
	return defs, err
}

// GetDefinition returns a version of a definition (0 = latest).
func (c *Client) GetDefinition(ctx context.Context, name string, version int) (*Definition, error) {
	var d Definition
	_, err := c.do(ctx, http.MethodGet, definitionPath(name, version, ""), nil, &d)
	return &d, err
}

// GetDefinitionYAML returns a definition as YAML.
func (c *Client) GetDefinitionYAML(ctx context.Context, name string, version int) ([]byte, error) {
	var raw []byte
	_, err := c.do(ctx, http.MethodGet, definitionPath(name, version, "yaml"), nil, &raw)
	return raw, err
}

func definitionPath(name string, version int, format string) string {
	q := url.Values{}
	if version > 0 {
		q.Set("version", strconv.Itoa(version))
	}
	if format != "" {
		q.Set("format", format)
	}
	return "/v1/workflow-definitions/" + url.PathEscape(name) + "?" + q.Encode()
}

// --- Workflow runs ---

type Workflow struct {
	ID              string          `json:"id"`
	Namespace       string          `json:"namespace"`
	Workflow        string          `json:"workflow"`
	Version         *int            `json:"version,omitempty"`
	Status          string          `json:"status"`
	Input           json.RawMessage `json:"input"`
	ErrorMessage    string          `json:"error_message,omitempty"`
	CancelRequested bool            `json:"cancel_requested"`
	IdempotencyKey  string          `json:"idempotency_key,omitempty"`
	CreatedAt       time.Time       `json:"created_at"`
	UpdatedAt       time.Time       `json:"updated_at"`
	Steps           []WorkflowStep  `json:"steps,omitempty"`

	// Created is false when StartWorkflow matched an existing idempotency key.
	Created bool `json:"-"`
}

type WorkflowStep struct {
	Name                   string            `json:"name"`
	Status                 string            `json:"status"`
	DependsOn              []string          `json:"depends_on,omitempty"`
	TaskID                 string            `json:"task_id,omitempty"`
	TaskStatus             string            `json:"task_status,omitempty"`
	Error                  string            `json:"error,omitempty"`
	Outputs                map[string]string `json:"outputs,omitempty"`
	CompensationTaskID     string            `json:"compensation_task_id,omitempty"`
	CompensationTaskStatus string            `json:"compensation_task_status,omitempty"`
}

// Step returns the step with this name, or nil.
func (w *Workflow) Step(name string) *WorkflowStep {
	for i := range w.Steps {
		if w.Steps[i].Name == name {
			return &w.Steps[i]
		}
	}
	return nil
}

// Done reports whether the workflow reached a final status.
func (w *Workflow) Done() bool {
	return w.Status == "COMPLETED" || w.Status == "FAILED" || w.Status == "CANCELLED"
}

type WorkflowRequest struct {
	Workflow       string `json:"workflow"`          // definition name
	Version        int    `json:"version,omitempty"` // 0 = latest
	Input          any    `json:"input,omitempty"`
	IdempotencyKey string `json:"idempotency_key,omitempty"`
}

func (c *Client) StartWorkflow(ctx context.Context, req WorkflowRequest) (*Workflow, error) {
	var wf Workflow
	code, err := c.do(ctx, http.MethodPost, "/v1/workflows", req, &wf)
	wf.Created = code == http.StatusCreated
	return &wf, err
}

func (c *Client) GetWorkflow(ctx context.Context, id string) (*Workflow, error) {
	var wf Workflow
	_, err := c.do(ctx, http.MethodGet, "/v1/workflows/"+url.PathEscape(id), nil, &wf)
	return &wf, err
}

func (c *Client) ListWorkflows(ctx context.Context, limit int) ([]Workflow, error) {
	path := "/v1/workflows"
	if limit > 0 {
		path += "?limit=" + strconv.Itoa(limit)
	}
	var wfs []Workflow
	_, err := c.do(ctx, http.MethodGet, path, nil, &wfs)
	return wfs, err
}

func (c *Client) CancelWorkflow(ctx context.Context, id string) (*Workflow, error) {
	var wf Workflow
	_, err := c.do(ctx, http.MethodPost, "/v1/workflows/"+url.PathEscape(id)+"/cancel", nil, &wf)
	return &wf, err
}

// WaitForWorkflow polls until the workflow finishes or ctx is done.
func (c *Client) WaitForWorkflow(ctx context.Context, id string) (*Workflow, error) {
	return poll(ctx, func() (*Workflow, bool, error) {
		wf, err := c.GetWorkflow(ctx, id)
		if err != nil {
			return nil, false, err
		}
		return wf, wf.Done(), nil
	})
}

// --- Schedules ---

type ScheduleRequest struct {
	Name          string             `json:"name"`
	Cron          string             `json:"cron"`               // "*/5 * * * *", "@hourly", "@every 30s"
	Timezone      string             `json:"timezone,omitempty"` // default UTC
	MisfirePolicy string             `json:"misfire_policy,omitempty"`
	Enabled       *bool              `json:"enabled,omitempty"`
	Task          *TaskRequest       `json:"task,omitempty"`
	Workflow      *ScheduledWorkflow `json:"workflow,omitempty"`
}

type ScheduledWorkflow struct {
	Name    string `json:"name"`
	Version int    `json:"version,omitempty"`
	Input   any    `json:"input,omitempty"`
}

type Schedule struct {
	ID            string          `json:"id"`
	Name          string          `json:"name"`
	Cron          string          `json:"cron"`
	Timezone      string          `json:"timezone"`
	MisfirePolicy string          `json:"misfire_policy"`
	Enabled       bool            `json:"enabled"`
	Target        json.RawMessage `json:"target"`
	NextRunAt     *time.Time      `json:"next_run_at,omitempty"`
	Upcoming      []time.Time     `json:"upcoming,omitempty"`
	LastRunAt     *time.Time      `json:"last_run_at,omitempty"`
	LastRunID     string          `json:"last_run_id,omitempty"`
	LastError     string          `json:"last_error,omitempty"`
	CreatedAt     time.Time       `json:"created_at"`
}

func (c *Client) CreateSchedule(ctx context.Context, req ScheduleRequest) (*Schedule, error) {
	var s Schedule
	_, err := c.do(ctx, http.MethodPost, "/v1/schedules", req, &s)
	return &s, err
}

func (c *Client) ListSchedules(ctx context.Context) ([]Schedule, error) {
	var list []Schedule
	_, err := c.do(ctx, http.MethodGet, "/v1/schedules", nil, &list)
	return list, err
}

func (c *Client) GetSchedule(ctx context.Context, name string) (*Schedule, error) {
	var s Schedule
	_, err := c.do(ctx, http.MethodGet, "/v1/schedules/"+url.PathEscape(name), nil, &s)
	return &s, err
}

func (c *Client) DeleteSchedule(ctx context.Context, name string) error {
	_, err := c.do(ctx, http.MethodDelete, "/v1/schedules/"+url.PathEscape(name), nil, nil)
	return err
}

// ScheduleAction is "pause", "resume" or "trigger" (fire now).
func (c *Client) ScheduleAction(ctx context.Context, name, action string) (*Schedule, error) {
	var s Schedule
	_, err := c.do(ctx, http.MethodPost, "/v1/schedules/"+url.PathEscape(name)+"/"+action, nil, &s)
	return &s, err
}

// --- Queues, workers, namespaces ---

type Queue struct {
	Name              string    `json:"name"`
	ConcurrencyLimit  *int      `json:"concurrency_limit"`
	RateLimit         *int      `json:"rate_limit"`
	RatePeriodSeconds int       `json:"rate_period_seconds"`
	Paused            bool      `json:"paused"`
	Queued            int       `json:"queued"`
	Running           int       `json:"running"`
	UpdatedAt         time.Time `json:"updated_at"`
}

type QueueSettings struct {
	ConcurrencyLimit  *int `json:"concurrency_limit"`
	RateLimit         *int `json:"rate_limit"`
	RatePeriodSeconds int  `json:"rate_period_seconds,omitempty"`
	Paused            bool `json:"paused"`
}

func (c *Client) ListQueues(ctx context.Context) ([]Queue, error) {
	var list []Queue
	_, err := c.do(ctx, http.MethodGet, "/v1/queues", nil, &list)
	return list, err
}

func (c *Client) PutQueue(ctx context.Context, name string, s QueueSettings) (*Queue, error) {
	var q Queue
	_, err := c.do(ctx, http.MethodPut, "/v1/queues/"+url.PathEscape(name), s, &q)
	return &q, err
}

func (c *Client) DeleteQueue(ctx context.Context, name string) error {
	_, err := c.do(ctx, http.MethodDelete, "/v1/queues/"+url.PathEscape(name), nil, nil)
	return err
}

type Worker struct {
	ID        int64             `json:"id"`
	Address   string            `json:"address"`
	Status    string            `json:"status"`
	Slots     int               `json:"slots"`
	Running   int               `json:"running"`
	Labels    map[string]string `json:"labels"`
	FirstSeen time.Time         `json:"first_seen"`
	LastSeen  time.Time         `json:"last_seen"`
}

// ListWorkers lists recently seen workers (admin).
func (c *Client) ListWorkers(ctx context.Context) ([]Worker, error) {
	var list []Worker
	_, err := c.do(ctx, http.MethodGet, "/v1/workers", nil, &list)
	return list, err
}

type Namespace struct {
	Name            string    `json:"name"`
	MaxPendingTasks *int      `json:"max_pending_tasks"`
	MaxConcurrency  *int      `json:"max_concurrency"`
	CreatedAt       time.Time `json:"created_at"`
}

func namespaceBody(n Namespace) map[string]any {
	return map[string]any{"name": n.Name, "max_pending_tasks": n.MaxPendingTasks, "max_concurrency": n.MaxConcurrency}
}

func (c *Client) CreateNamespace(ctx context.Context, n Namespace) (*Namespace, error) {
	var out Namespace
	_, err := c.do(ctx, http.MethodPost, "/v1/namespaces", namespaceBody(n), &out)
	return &out, err
}

func (c *Client) UpdateNamespace(ctx context.Context, n Namespace) (*Namespace, error) {
	var out Namespace
	_, err := c.do(ctx, http.MethodPut, "/v1/namespaces/"+url.PathEscape(n.Name), namespaceBody(n), &out)
	return &out, err
}

func (c *Client) ListNamespaces(ctx context.Context) ([]Namespace, error) {
	var list []Namespace
	_, err := c.do(ctx, http.MethodGet, "/v1/namespaces", nil, &list)
	return list, err
}

// --- API keys (admin only) ---

type APIKey struct {
	ID        string     `json:"id"`
	Name      string     `json:"name"`
	Prefix    string     `json:"prefix"`
	Namespace string     `json:"namespace"`
	Admin     bool       `json:"admin"`
	CreatedAt time.Time  `json:"created_at"`
	RevokedAt *time.Time `json:"revoked_at,omitempty"`
	// Key is the secret, only returned by CreateAPIKey.
	Key string `json:"key,omitempty"`
}

// CreateAPIKey creates a key bound to a namespace ("" = default).
func (c *Client) CreateAPIKey(ctx context.Context, name, namespace string, admin bool) (*APIKey, error) {
	var k APIKey
	_, err := c.do(ctx, http.MethodPost, "/v1/api-keys",
		map[string]any{"name": name, "namespace": namespace, "admin": admin}, &k)
	return &k, err
}

func (c *Client) ListAPIKeys(ctx context.Context) ([]APIKey, error) {
	var keys []APIKey
	_, err := c.do(ctx, http.MethodGet, "/v1/api-keys", nil, &keys)
	return keys, err
}

func (c *Client) RevokeAPIKey(ctx context.Context, id string) error {
	_, err := c.do(ctx, http.MethodDelete, "/v1/api-keys/"+url.PathEscape(id), nil, nil)
	return err
}

// poll calls check every 500ms until it reports done, fails, or ctx ends.
func poll[T any](ctx context.Context, check func() (T, bool, error)) (T, error) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		v, done, err := check()
		if err != nil || done {
			return v, err
		}
		select {
		case <-ctx.Done():
			return v, ctx.Err()
		case <-ticker.C:
		}
	}
}
