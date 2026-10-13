package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/ChinmayNoob/conductor/pkg/db"
	"github.com/ChinmayNoob/conductor/pkg/schedule"
	"github.com/google/uuid"
)

type scheduleJSON struct {
	ID            uuid.UUID       `json:"id"`
	Name          string          `json:"name"`
	Cron          string          `json:"cron"`
	Timezone      string          `json:"timezone"`
	MisfirePolicy string          `json:"misfire_policy"`
	Enabled       bool            `json:"enabled"`
	Target        json.RawMessage `json:"target"`
	NextRunAt     *time.Time      `json:"next_run_at,omitempty"`
	Upcoming      []time.Time     `json:"upcoming,omitempty"`
	LastRunAt     *time.Time      `json:"last_run_at,omitempty"`
	LastRunID     *uuid.UUID      `json:"last_run_id,omitempty"`
	LastError     string          `json:"last_error,omitempty"`
	CreatedAt     time.Time       `json:"created_at"`
}

func toScheduleJSON(s *db.Schedule) scheduleJSON {
	out := scheduleJSON{
		ID: s.ID, Name: s.Name, Cron: s.Cron, Timezone: s.Timezone, MisfirePolicy: s.MisfirePolicy,
		Enabled: s.Enabled, Target: s.Target, LastRunAt: s.LastRunAt, LastRunID: s.LastRunID,
		LastError: s.LastError, CreatedAt: s.CreatedAt,
	}
	if s.Enabled {
		next := s.NextRunAt
		out.NextRunAt = &next
		if spec, err := schedule.Parse(s.Cron, s.Timezone); err == nil {
			t := next
			for range 3 {
				t = spec.Next(t)
				out.Upcoming = append(out.Upcoming, t)
			}
		}
	}
	return out
}

func (s *Server) handleCreateSchedule(w http.ResponseWriter, r *http.Request) {
	sch, ok := s.decodeSchedule(w, r, "")
	if !ok {
		return
	}
	created, err := s.db.CreateSchedule(r.Context(), sch)
	if errors.Is(err, db.ErrExists) {
		writeError(w, http.StatusConflict, "a schedule named "+sch.Name+" already exists")
		return
	}
	if err != nil {
		s.internalError(w, "Failed to create schedule", err)
		return
	}
	writeJSON(w, http.StatusCreated, toScheduleJSON(created))
}

// handlePutSchedule creates the named schedule or replaces its settings, for
// declarative tools such as the Kubernetes operator. The run history is
// kept, and so is the next run unless the timing changed.
func (s *Server) handlePutSchedule(w http.ResponseWriter, r *http.Request) {
	sch, ok := s.decodeSchedule(w, r, r.PathValue("name"))
	if !ok {
		return
	}
	stored, created, err := s.db.PutSchedule(r.Context(), sch)
	if err != nil {
		s.internalError(w, "Failed to save schedule", err)
		return
	}
	writeJSON(w, createdOr(created), toScheduleJSON(stored))
}

// decodeSchedule reads and validates a schedule from the request body. A
// name from the URL (pathName) is used if given; the body may repeat it.
func (s *Server) decodeSchedule(w http.ResponseWriter, r *http.Request, pathName string) (db.Schedule, bool) {
	var req struct {
		Name          string `json:"name"`
		Cron          string `json:"cron"`
		Timezone      string `json:"timezone,omitempty"`
		MisfirePolicy string `json:"misfire_policy,omitempty"`
		Enabled       *bool  `json:"enabled,omitempty"`
		schedule.Target
	}
	if !s.decode(w, r, &req) {
		return db.Schedule{}, false
	}
	if pathName != "" {
		if req.Name != "" && req.Name != pathName {
			writeError(w, http.StatusBadRequest, "the name in the body must match the URL")
			return db.Schedule{}, false
		}
		req.Name = pathName
	}
	if req.Timezone == "" {
		req.Timezone = "UTC"
	}
	if req.MisfirePolicy == "" {
		req.MisfirePolicy = schedule.Skip
	}
	if !nameRe.MatchString(req.Name) {
		writeError(w, http.StatusBadRequest, "name must be lowercase letters, digits, '-' or '_'")
		return db.Schedule{}, false
	}
	if !schedule.ValidPolicy(req.MisfirePolicy) {
		writeError(w, http.StatusBadRequest, "misfire_policy must be skip, run_once or catch_up")
		return db.Schedule{}, false
	}
	spec, err := schedule.Parse(req.Cron, req.Timezone)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return db.Schedule{}, false
	}
	if err := req.Target.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return db.Schedule{}, false
	}
	if wf := req.Target.Workflow; wf != nil {
		d, err := s.db.GetDefinition(r.Context(), namespace(r), wf.Name, wf.Version)
		if err != nil {
			s.internalError(w, "Failed to look up workflow definition", err)
			return db.Schedule{}, false
		}
		if d == nil {
			writeError(w, http.StatusBadRequest, "no workflow definition named "+wf.Name)
			return db.Schedule{}, false
		}
	}
	target, err := json.Marshal(req.Target)
	if err != nil {
		s.internalError(w, "Failed to encode schedule target", err)
		return db.Schedule{}, false
	}
	return db.Schedule{
		Namespace: namespace(r), Name: req.Name, Cron: req.Cron, Timezone: req.Timezone,
		MisfirePolicy: req.MisfirePolicy, Target: target, Enabled: req.Enabled == nil || *req.Enabled,
		NextRunAt: spec.Next(time.Now()),
	}, true
}

func (s *Server) handleListSchedules(w http.ResponseWriter, r *http.Request) {
	list, err := s.db.ListSchedules(r.Context(), namespace(r))
	if err != nil {
		s.internalError(w, "Failed to list schedules", err)
		return
	}
	out := make([]scheduleJSON, 0, len(list))
	for _, sch := range list {
		out = append(out, toScheduleJSON(sch))
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleGetSchedule(w http.ResponseWriter, r *http.Request) {
	sch, err := s.db.GetSchedule(r.Context(), namespace(r), r.PathValue("name"))
	if err != nil {
		s.internalError(w, "Failed to get schedule", err)
		return
	}
	if sch == nil {
		writeError(w, http.StatusNotFound, "schedule not found")
		return
	}
	writeJSON(w, http.StatusOK, toScheduleJSON(sch))
}

func (s *Server) handleDeleteSchedule(w http.ResponseWriter, r *http.Request) {
	ok, err := s.db.DeleteSchedule(r.Context(), namespace(r), r.PathValue("name"))
	if err != nil {
		s.internalError(w, "Failed to delete schedule", err)
		return
	}
	if !ok {
		writeError(w, http.StatusNotFound, "schedule not found")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handlePauseSchedule(w http.ResponseWriter, r *http.Request) {
	s.setScheduleEnabled(w, r, false)
}

func (s *Server) handleResumeSchedule(w http.ResponseWriter, r *http.Request) {
	s.setScheduleEnabled(w, r, true)
}

func (s *Server) setScheduleEnabled(w http.ResponseWriter, r *http.Request, enabled bool) {
	name := r.PathValue("name")
	existing, err := s.db.GetSchedule(r.Context(), namespace(r), name)
	if err != nil {
		s.internalError(w, "Failed to get schedule", err)
		return
	}
	if existing == nil {
		writeError(w, http.StatusNotFound, "schedule not found")
		return
	}
	// Resuming starts from now, so runs missed while paused don't fire.
	next := existing.NextRunAt
	if spec, err := schedule.Parse(existing.Cron, existing.Timezone); err == nil {
		next = spec.Next(time.Now())
	}
	sch, err := s.db.SetScheduleEnabled(r.Context(), namespace(r), name, enabled, next)
	if err != nil {
		s.internalError(w, "Failed to update schedule", err)
		return
	}
	writeJSON(w, http.StatusOK, toScheduleJSON(sch))
}

// handleTriggerSchedule makes a schedule fire now (within a second).
func (s *Server) handleTriggerSchedule(w http.ResponseWriter, r *http.Request) {
	sch, err := s.db.TriggerSchedule(r.Context(), namespace(r), r.PathValue("name"))
	if err != nil {
		s.internalError(w, "Failed to trigger schedule", err)
		return
	}
	if sch == nil {
		writeError(w, http.StatusNotFound, "no enabled schedule with that name")
		return
	}
	writeJSON(w, http.StatusAccepted, toScheduleJSON(sch))
}
