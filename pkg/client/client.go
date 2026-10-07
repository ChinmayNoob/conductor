// Package client is a Go SDK for Conductor's HTTP API.
//
//	c := client.New("http://localhost:8081", os.Getenv("CONDUCTOR_API_KEY"))
//	task, err := c.SubmitTask(ctx, client.TaskRequest{Data: "echo hello"})
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
	baseURL string
	apiKey  string
	http    *http.Client
}

// New returns a client for the API at baseURL, e.g. "http://localhost:8081".
func New(baseURL, apiKey string) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		apiKey:  apiKey,
		http:    &http.Client{Timeout: 30 * time.Second},
	}
}

// APIError is returned for non-2xx responses.
type APIError struct {
	StatusCode int
	Message    string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("conductor API: %s (HTTP %d)", e.Message, e.StatusCode)
}

// IsNotFound reports whether err is an API 404.
func IsNotFound(err error) bool {
	var apiErr *APIError
	return errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusNotFound
}

func (c *Client) do(ctx context.Context, method, path string, body, out any) error {
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return err
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
		return &APIError{StatusCode: resp.StatusCode, Message: e.Error}
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// Health checks that the API is up and can reach its database.
func (c *Client) Health(ctx context.Context) error {
	return c.do(ctx, http.MethodGet, "/health", nil, nil)
}

// --- Tasks ---

type TaskRequest struct {
	Data              string `json:"data"`
	Priority          int    `json:"priority,omitempty"`
	MaxRetries        int    `json:"max_retries,omitempty"`
	RetryDelaySeconds int    `json:"retry_delay_seconds,omitempty"`
	TimeoutSeconds    int    `json:"timeout_seconds,omitempty"`
	DelaySeconds      int    `json:"delay_seconds,omitempty"`
	ScheduledAt       int64  `json:"scheduled_at,omitempty"`
}

type Task struct {
	ID             string     `json:"id"`
	Data           string     `json:"data"`
	Status         string     `json:"status"`
	Priority       int        `json:"priority"`
	MaxRetries     int        `json:"max_retries"`
	RetryCount     int        `json:"retry_count"`
	TimeoutSeconds int        `json:"timeout_seconds"`
	ScheduledAt    time.Time  `json:"scheduled_at"`
	PickedAt       *time.Time `json:"picked_at,omitempty"`
	StartedAt      *time.Time `json:"started_at,omitempty"`
	CompletedAt    *time.Time `json:"completed_at,omitempty"`
	FailedAt       *time.Time `json:"failed_at,omitempty"`
	CancelledAt    *time.Time `json:"cancelled_at,omitempty"`
	Output         string     `json:"output,omitempty"`
	ErrorMessage   string     `json:"error_message,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
}

// Done reports whether the task reached a final status.
func (t *Task) Done() bool {
	return t.Status == "COMPLETED" || t.Status == "FAILED" || t.Status == "CANCELLED"
}

func (c *Client) SubmitTask(ctx context.Context, req TaskRequest) (*Task, error) {
	var t Task
	return &t, c.do(ctx, http.MethodPost, "/v1/tasks", req, &t)
}

func (c *Client) GetTask(ctx context.Context, id string) (*Task, error) {
	var t Task
	return &t, c.do(ctx, http.MethodGet, "/v1/tasks/"+url.PathEscape(id), nil, &t)
}

// ListTasks returns recent tasks, newest first. status may be empty.
func (c *Client) ListTasks(ctx context.Context, status string, limit int) ([]Task, error) {
	q := url.Values{}
	if status != "" {
		q.Set("status", status)
	}
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	var tasks []Task
	return tasks, c.do(ctx, http.MethodGet, "/v1/tasks?"+q.Encode(), nil, &tasks)
}

func (c *Client) CancelTask(ctx context.Context, id string) (*Task, error) {
	var t Task
	return &t, c.do(ctx, http.MethodPost, "/v1/tasks/"+url.PathEscape(id)+"/cancel", nil, &t)
}

// Stats returns task counts by status, plus "total".
func (c *Client) Stats(ctx context.Context) (map[string]int, error) {
	var stats map[string]int
	return stats, c.do(ctx, http.MethodGet, "/v1/stats", nil, &stats)
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

// --- Workflows ---

type Workflow struct {
	ID              string          `json:"id"`
	Type            string          `json:"type"`
	Status          string          `json:"status"`
	CurrentStep     int             `json:"current_step"`
	Input           json.RawMessage `json:"input"`
	ErrorMessage    string          `json:"error_message,omitempty"`
	CancelRequested bool            `json:"cancel_requested"`
	CreatedAt       time.Time       `json:"created_at"`
	UpdatedAt       time.Time       `json:"updated_at"`
	Steps           []WorkflowStep  `json:"steps,omitempty"`
}

type WorkflowStep struct {
	Number             int    `json:"number"`
	Name               string `json:"name"`
	Status             string `json:"status"`
	TaskID             string `json:"task_id,omitempty"`
	CompensationTaskID string `json:"compensation_task_id,omitempty"`
}

// Done reports whether the workflow reached a final status.
func (w *Workflow) Done() bool {
	return w.Status == "COMPLETED" || w.Status == "FAILED" || w.Status == "CANCELLED"
}

// StartWorkflow starts a workflow of the given type. input is marshalled to a
// JSON object; nil means no input.
func (c *Client) StartWorkflow(ctx context.Context, wfType string, input any) (*Workflow, error) {
	body := map[string]any{"type": wfType}
	if input != nil {
		body["input"] = input
	}
	var wf Workflow
	return &wf, c.do(ctx, http.MethodPost, "/v1/workflows", body, &wf)
}

func (c *Client) GetWorkflow(ctx context.Context, id string) (*Workflow, error) {
	var wf Workflow
	return &wf, c.do(ctx, http.MethodGet, "/v1/workflows/"+url.PathEscape(id), nil, &wf)
}

func (c *Client) ListWorkflows(ctx context.Context, limit int) ([]Workflow, error) {
	path := "/v1/workflows"
	if limit > 0 {
		path += "?limit=" + strconv.Itoa(limit)
	}
	var wfs []Workflow
	return wfs, c.do(ctx, http.MethodGet, path, nil, &wfs)
}

func (c *Client) CancelWorkflow(ctx context.Context, id string) (*Workflow, error) {
	var wf Workflow
	return &wf, c.do(ctx, http.MethodPost, "/v1/workflows/"+url.PathEscape(id)+"/cancel", nil, &wf)
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

// --- API keys (admin only) ---

type APIKey struct {
	ID        string     `json:"id"`
	Name      string     `json:"name"`
	Prefix    string     `json:"prefix"`
	Admin     bool       `json:"admin"`
	CreatedAt time.Time  `json:"created_at"`
	RevokedAt *time.Time `json:"revoked_at,omitempty"`
	// Key is the secret, only returned by CreateAPIKey.
	Key string `json:"key,omitempty"`
}

func (c *Client) CreateAPIKey(ctx context.Context, name string, admin bool) (*APIKey, error) {
	var k APIKey
	return &k, c.do(ctx, http.MethodPost, "/v1/api-keys", map[string]any{"name": name, "admin": admin}, &k)
}

func (c *Client) ListAPIKeys(ctx context.Context) ([]APIKey, error) {
	var keys []APIKey
	return keys, c.do(ctx, http.MethodGet, "/v1/api-keys", nil, &keys)
}

func (c *Client) RevokeAPIKey(ctx context.Context, id string) error {
	return c.do(ctx, http.MethodDelete, "/v1/api-keys/"+url.PathEscape(id), nil, nil)
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
