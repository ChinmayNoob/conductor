package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ChinmayNoob/conductor/pkg/db"
	"github.com/ChinmayNoob/conductor/pkg/grpcapi"
	"github.com/ChinmayNoob/conductor/pkg/task"
	"github.com/google/uuid"
)

type createTaskRequest struct {
	task.Template
	// Data is the older name for Command.
	Data           string `json:"data,omitempty"`
	DelaySeconds   int    `json:"delay_seconds,omitempty"`
	ScheduledAt    int64  `json:"scheduled_at,omitempty"` // Unix seconds
	IdempotencyKey string `json:"idempotency_key,omitempty"`
}

type taskJSON struct {
	ID             uuid.UUID         `json:"id"`
	Namespace      string            `json:"namespace"`
	Queue          string            `json:"queue"`
	Type           string            `json:"type"`
	Command        string            `json:"command"`
	Spec           json.RawMessage   `json:"spec,omitempty"`
	Labels         map[string]string `json:"labels,omitempty"`
	Status         db.TaskStatus     `json:"status"`
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
	WorkflowID     *uuid.UUID        `json:"workflow_id,omitempty"`
	WorkerID       *int64            `json:"worker_id,omitempty"`
	Attempt        int               `json:"attempt"`
	CreatedAt      time.Time         `json:"created_at"`
}

func toTaskJSON(t *db.Task) taskJSON {
	// Hide the internal type.* labels; they are implied by the type.
	labels := make(map[string]string)
	for k, v := range t.Requirements {
		if k != "type."+t.Type {
			labels[k] = v
		}
	}
	return taskJSON{
		ID: t.ID, Namespace: t.Namespace, Queue: t.Queue, Type: t.Type, Command: t.Data, Spec: t.Spec,
		Labels: labels, Status: t.Status, Priority: t.Priority, MaxRetries: t.MaxRetries,
		RetryCount: t.RetryCount, TimeoutSeconds: t.TimeoutSeconds, ScheduledAt: t.ScheduledAt,
		PickedAt: t.PickedAt, StartedAt: t.StartedAt, CompletedAt: t.CompletedAt, FailedAt: t.FailedAt,
		CancelledAt: t.CancelledAt, Output: t.Output, Outputs: t.Outputs, ErrorMessage: t.ErrorMessage,
		IdempotencyKey: userKey(t.IdempotencyKey), WorkflowID: t.WorkflowID, WorkerID: t.WorkerID,
		Attempt: t.Attempt, CreatedAt: t.CreatedAt,
	}
}

func (s *Server) handleCreateTask(w http.ResponseWriter, r *http.Request) {
	var req createTaskRequest
	if !s.decode(w, r, &req) {
		return
	}
	if req.Command == "" {
		req.Command = req.Data
	}
	switch {
	case req.DelaySeconds < 0:
		writeError(w, http.StatusBadRequest, "delay_seconds must not be negative")
		return
	case req.DelaySeconds > 0 && req.ScheduledAt > 0:
		writeError(w, http.StatusBadRequest, "set either delay_seconds or scheduled_at, not both")
		return
	}
	if err := req.Template.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	scheduledAt := req.ScheduledAt
	if req.DelaySeconds > 0 {
		scheduledAt = time.Now().Add(time.Duration(req.DelaySeconds) * time.Second).Unix()
	}
	tmpl, err := json.Marshal(req.Template)
	if err != nil {
		s.internalError(w, "Failed to encode task", err)
		return
	}

	resp, err := s.coordinator.SubmitTask(r.Context(), &grpcapi.ClientTaskRequest{
		Namespace:      namespace(r),
		TemplateJson:   tmpl,
		ScheduledAt:    scheduledAt,
		IdempotencyKey: idempotencyKey(req.IdempotencyKey),
	})
	if err != nil {
		s.writeRPCError(w, err)
		return
	}
	s.respondWithTask(w, r, resp.TaskId, createdOr(resp.Created))
}

// loadTask returns the task if it exists in the request's namespace. It
// writes the error response itself and returns nil otherwise.
func (s *Server) loadTask(w http.ResponseWriter, r *http.Request, rawID string) *db.Task {
	id, err := uuid.Parse(rawID)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid task ID")
		return nil
	}
	t, err := s.db.GetTask(r.Context(), id)
	if err != nil {
		s.internalError(w, "Failed to get task", err)
		return nil
	}
	if t == nil || t.Namespace != namespace(r) {
		writeError(w, http.StatusNotFound, "task not found")
		return nil
	}
	return t
}

func (s *Server) respondWithTask(w http.ResponseWriter, r *http.Request, rawID string, code int) {
	if t := s.loadTask(w, r, rawID); t != nil {
		writeJSON(w, code, toTaskJSON(t))
	}
}

func (s *Server) handleGetTask(w http.ResponseWriter, r *http.Request) {
	s.respondWithTask(w, r, r.PathValue("id"), http.StatusOK)
}

func (s *Server) handleListTasks(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	st := db.TaskStatus(q.Get("status"))
	if st != "" && !st.Valid() {
		writeError(w, http.StatusBadRequest, "unknown status "+strconv.Quote(string(st)))
		return
	}
	f := db.TaskFilter{Namespace: namespace(r), Status: st, Queue: q.Get("queue")}
	if wf := q.Get("workflow_id"); wf != "" {
		id, err := uuid.Parse(wf)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid workflow_id")
			return
		}
		f.WorkflowID = &id
	}
	s.listTasks(w, r, f)
}

// handleDeadLetter lists tasks that failed permanently and aren't part of a
// workflow (workflow failures are handled by compensation).
func (s *Server) handleDeadLetter(w http.ResponseWriter, r *http.Request) {
	s.listTasks(w, r, db.TaskFilter{Namespace: namespace(r), Queue: r.URL.Query().Get("queue"), DeadLetter: true})
}

func (s *Server) listTasks(w http.ResponseWriter, r *http.Request, f db.TaskFilter) {
	limit, ok := parseLimit(w, r)
	if !ok {
		return
	}
	f.Limit = limit
	tasks, err := s.db.ListTasks(r.Context(), f)
	if err != nil {
		s.internalError(w, "Failed to list tasks", err)
		return
	}
	out := make([]taskJSON, 0, len(tasks))
	for _, t := range tasks {
		out = append(out, toTaskJSON(t))
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleCancelTask(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := uuid.Parse(id); err != nil {
		writeError(w, http.StatusBadRequest, "invalid task ID")
		return
	}
	resp, err := s.coordinator.CancelTask(r.Context(), &grpcapi.CancelTaskRequest{TaskId: id, Namespace: namespace(r)})
	if err != nil {
		s.writeRPCError(w, err)
		return
	}
	if !resp.Cancelled {
		writeError(w, http.StatusConflict, "task has already finished")
		return
	}
	s.respondWithTask(w, r, id, http.StatusOK)
}

// handleRequeueTask gives a dead-lettered task a fresh set of retries.
func (s *Server) handleRequeueTask(w http.ResponseWriter, r *http.Request) {
	t := s.loadTask(w, r, r.PathValue("id"))
	if t == nil {
		return
	}
	requeued, err := s.db.RequeueFailedTask(r.Context(), t.ID)
	if err != nil {
		s.internalError(w, "Failed to requeue task", err)
		return
	}
	if requeued == nil {
		writeError(w, http.StatusConflict, "only permanently failed tasks outside workflows can be requeued")
		return
	}
	writeJSON(w, http.StatusOK, toTaskJSON(requeued))
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	counts, err := s.db.TaskCounts(r.Context(), namespace(r))
	if err != nil {
		s.internalError(w, "Failed to count tasks", err)
		return
	}
	out := map[string]int{"total": 0}
	for _, st := range db.TaskStatuses {
		out[string(st)] = counts[st]
		out["total"] += counts[st]
	}
	writeJSON(w, http.StatusOK, out)
}

// autoKeyPrefix marks idempotency keys the API generates itself.
const autoKeyPrefix = "auto:"

// idempotencyKey returns the caller's key, or a fresh one. Every submission
// carries a key so that if a coordinator fails over mid-request and the
// client library retries, the retry can't create a duplicate.
func idempotencyKey(key string) string {
	if key != "" {
		return key
	}
	return autoKeyPrefix + uuid.NewString()
}

// userKey hides generated keys from API responses.
func userKey(key string) string {
	if strings.HasPrefix(key, autoKeyPrefix) {
		return ""
	}
	return key
}
