package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/ChinmayNoob/conductor/pkg/db"
	"github.com/ChinmayNoob/conductor/pkg/grpcapi"
	"github.com/google/uuid"
)

type workflowJSON struct {
	ID              uuid.UUID         `json:"id"`
	Type            string            `json:"type"`
	Status          db.WorkflowStatus `json:"status"`
	CurrentStep     int               `json:"current_step"`
	Input           json.RawMessage   `json:"input"`
	ErrorMessage    string            `json:"error_message,omitempty"`
	CancelRequested bool              `json:"cancel_requested"`
	CreatedAt       time.Time         `json:"created_at"`
	UpdatedAt       time.Time         `json:"updated_at"`
	Steps           []stepJSON        `json:"steps,omitempty"`
}

type stepJSON struct {
	Number             int           `json:"number"`
	Name               string        `json:"name"`
	Status             db.StepStatus `json:"status"`
	TaskID             *uuid.UUID    `json:"task_id,omitempty"`
	CompensationTaskID *uuid.UUID    `json:"compensation_task_id,omitempty"`
}

func toWorkflowJSON(wf *db.Workflow, steps []*db.WorkflowStep) workflowJSON {
	out := workflowJSON{
		ID: wf.ID, Type: wf.Type, Status: wf.Status, CurrentStep: wf.CurrentStep, Input: wf.Context,
		ErrorMessage: wf.ErrorMessage, CancelRequested: wf.CancelRequested,
		CreatedAt: wf.CreatedAt, UpdatedAt: wf.UpdatedAt,
	}
	for _, s := range steps {
		out.Steps = append(out.Steps, stepJSON{
			Number: s.StepNumber, Name: s.Name, Status: s.Status,
			TaskID: s.TaskID, CompensationTaskID: s.CompensationTaskID,
		})
	}
	return out
}

func (s *Server) handleCreateWorkflow(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Type  string          `json:"type"`
		Input json.RawMessage `json:"input"`
	}
	if !s.decode(w, r, &req) {
		return
	}
	if req.Type == "" {
		writeError(w, http.StatusBadRequest, "type is required")
		return
	}
	input := "{}"
	if len(req.Input) > 0 {
		input = string(req.Input)
	}

	resp, err := s.coordinator.SubmitWorkflow(r.Context(), &grpcapi.WorkflowRequest{
		WorkflowType: req.Type,
		InputJson:    input,
	})
	if err != nil {
		s.writeRPCError(w, err)
		return
	}
	s.respondWithWorkflow(w, r, resp.WorkflowId, http.StatusCreated)
}

func (s *Server) respondWithWorkflow(w http.ResponseWriter, r *http.Request, rawID string, code int) {
	id, err := uuid.Parse(rawID)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid workflow ID")
		return
	}
	wf, err := s.db.GetWorkflow(r.Context(), id)
	if err != nil {
		s.internalError(w, "Failed to get workflow", err)
		return
	}
	if wf == nil {
		writeError(w, http.StatusNotFound, "workflow not found")
		return
	}
	steps, err := s.db.GetWorkflowSteps(r.Context(), id)
	if err != nil {
		s.internalError(w, "Failed to get workflow steps", err)
		return
	}
	writeJSON(w, code, toWorkflowJSON(wf, steps))
}

func (s *Server) handleGetWorkflow(w http.ResponseWriter, r *http.Request) {
	s.respondWithWorkflow(w, r, r.PathValue("id"), http.StatusOK)
}

func (s *Server) handleListWorkflows(w http.ResponseWriter, r *http.Request) {
	limit, ok := parseLimit(w, r)
	if !ok {
		return
	}
	workflows, err := s.db.ListWorkflows(r.Context(), limit)
	if err != nil {
		s.internalError(w, "Failed to list workflows", err)
		return
	}
	out := make([]workflowJSON, 0, len(workflows))
	for _, wf := range workflows {
		out = append(out, toWorkflowJSON(wf, nil))
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleCancelWorkflow(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := uuid.Parse(id); err != nil {
		writeError(w, http.StatusBadRequest, "invalid workflow ID")
		return
	}
	resp, err := s.coordinator.CancelWorkflow(r.Context(), &grpcapi.CancelWorkflowRequest{WorkflowId: id})
	if err != nil {
		s.writeRPCError(w, err)
		return
	}
	if !resp.Cancelled {
		writeError(w, http.StatusConflict, "workflow has already finished")
		return
	}
	s.respondWithWorkflow(w, r, id, http.StatusAccepted)
}
