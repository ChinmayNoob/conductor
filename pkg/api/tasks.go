package api

import (
	"net/http"
	"strconv"
	"time"

	"github.com/ChinmayNoob/conductor/pkg/db"
	"github.com/ChinmayNoob/conductor/pkg/grpcapi"
	"github.com/google/uuid"
)

type createTaskRequest struct {
	Data              string `json:"data"`
	Priority          int    `json:"priority,omitempty"`
	MaxRetries        int    `json:"max_retries,omitempty"`
	RetryDelaySeconds int    `json:"retry_delay_seconds,omitempty"`
	TimeoutSeconds    int    `json:"timeout_seconds,omitempty"`
	DelaySeconds      int    `json:"delay_seconds,omitempty"`
	ScheduledAt       int64  `json:"scheduled_at,omitempty"` // Unix seconds
}

type taskJSON struct {
	ID             uuid.UUID     `json:"id"`
	Data           string        `json:"data"`
	Status         db.TaskStatus `json:"status"`
	Priority       int           `json:"priority"`
	MaxRetries     int           `json:"max_retries"`
	RetryCount     int           `json:"retry_count"`
	TimeoutSeconds int           `json:"timeout_seconds"`
	ScheduledAt    time.Time     `json:"scheduled_at"`
	PickedAt       *time.Time    `json:"picked_at,omitempty"`
	StartedAt      *time.Time    `json:"started_at,omitempty"`
	CompletedAt    *time.Time    `json:"completed_at,omitempty"`
	FailedAt       *time.Time    `json:"failed_at,omitempty"`
	CancelledAt    *time.Time    `json:"cancelled_at,omitempty"`
	Output         string        `json:"output,omitempty"`
	ErrorMessage   string        `json:"error_message,omitempty"`
	CreatedAt      time.Time     `json:"created_at"`
}

func toTaskJSON(t *db.Task) taskJSON {
	return taskJSON{
		ID: t.ID, Data: t.Data, Status: t.Status, Priority: t.Priority, MaxRetries: t.MaxRetries,
		RetryCount: t.RetryCount, TimeoutSeconds: t.TimeoutSeconds, ScheduledAt: t.ScheduledAt,
		PickedAt: t.PickedAt, StartedAt: t.StartedAt, CompletedAt: t.CompletedAt, FailedAt: t.FailedAt,
		CancelledAt: t.CancelledAt, Output: t.Output, ErrorMessage: t.ErrorMessage, CreatedAt: t.CreatedAt,
	}
}

func (s *Server) handleCreateTask(w http.ResponseWriter, r *http.Request) {
	var req createTaskRequest
	if !s.decode(w, r, &req) {
		return
	}
	switch {
	case req.Data == "":
		writeError(w, http.StatusBadRequest, "data is required")
		return
	case req.Priority < 0 || req.Priority > 10:
		writeError(w, http.StatusBadRequest, "priority must be between 1 and 10")
		return
	case req.MaxRetries < 0 || req.RetryDelaySeconds < 0 || req.TimeoutSeconds < 0 || req.DelaySeconds < 0:
		writeError(w, http.StatusBadRequest, "max_retries, retry_delay_seconds, timeout_seconds and delay_seconds must not be negative")
		return
	case req.DelaySeconds > 0 && req.ScheduledAt > 0:
		writeError(w, http.StatusBadRequest, "set either delay_seconds or scheduled_at, not both")
		return
	}

	scheduledAt := req.ScheduledAt
	if req.DelaySeconds > 0 {
		scheduledAt = time.Now().Add(time.Duration(req.DelaySeconds) * time.Second).Unix()
	}

	resp, err := s.coordinator.SubmitTask(r.Context(), &grpcapi.ClientTaskRequest{
		Data:              req.Data,
		Priority:          int32(req.Priority),
		MaxRetries:        int32(req.MaxRetries),
		RetryDelaySeconds: int32(req.RetryDelaySeconds),
		TimeoutSeconds:    int32(req.TimeoutSeconds),
		ScheduledAt:       scheduledAt,
	})
	if err != nil {
		s.writeRPCError(w, err)
		return
	}
	s.respondWithTask(w, r, resp.TaskId, http.StatusCreated)
}

func (s *Server) respondWithTask(w http.ResponseWriter, r *http.Request, rawID string, code int) {
	id, err := uuid.Parse(rawID)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid task ID")
		return
	}
	t, err := s.db.GetTask(r.Context(), id)
	if err != nil {
		s.internalError(w, "Failed to get task", err)
		return
	}
	if t == nil {
		writeError(w, http.StatusNotFound, "task not found")
		return
	}
	writeJSON(w, code, toTaskJSON(t))
}

func (s *Server) handleGetTask(w http.ResponseWriter, r *http.Request) {
	s.respondWithTask(w, r, r.PathValue("id"), http.StatusOK)
}

func (s *Server) handleListTasks(w http.ResponseWriter, r *http.Request) {
	st := db.TaskStatus(r.URL.Query().Get("status"))
	if st != "" && !st.Valid() {
		writeError(w, http.StatusBadRequest, "unknown status "+strconv.Quote(string(st)))
		return
	}
	limit, ok := parseLimit(w, r)
	if !ok {
		return
	}
	tasks, err := s.db.ListTasks(r.Context(), st, limit)
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
	resp, err := s.coordinator.CancelTask(r.Context(), &grpcapi.CancelTaskRequest{TaskId: id})
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

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	counts, err := s.db.TaskCounts(r.Context())
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

// parseLimit reads ?limit= (default 100, max 1000).
func parseLimit(w http.ResponseWriter, r *http.Request) (int, bool) {
	raw := r.URL.Query().Get("limit")
	if raw == "" {
		return 100, true
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 || n > 1000 {
		writeError(w, http.StatusBadRequest, "limit must be between 1 and 1000")
		return 0, false
	}
	return n, true
}
