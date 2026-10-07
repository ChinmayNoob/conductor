package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/ChinmayNoob/conductor/pkg/db"
	"github.com/ChinmayNoob/conductor/pkg/grpcapi"
	"github.com/ChinmayNoob/conductor/pkg/workflow"
	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
)

// --- Definitions ---

type definitionJSON struct {
	Name        string               `json:"name"`
	Version     int                  `json:"version"`
	Description string               `json:"description,omitempty"`
	Steps       int                  `json:"steps"`
	CreatedAt   time.Time            `json:"created_at"`
	Definition  *workflow.Definition `json:"definition,omitempty"`
}

func toDefinitionJSON(d *db.Definition, full bool) (definitionJSON, error) {
	def, err := workflow.FromJSON(d.Spec)
	if err != nil {
		return definitionJSON{}, err
	}
	out := definitionJSON{Name: d.Name, Version: d.Version, Description: def.Description, Steps: len(def.Steps), CreatedAt: d.CreatedAt}
	if full {
		out.Definition = def
	}
	return out, nil
}

// handleSaveDefinition stores a workflow definition sent as YAML or JSON.
// Re-sending an unchanged definition returns the existing version (200)
// instead of creating a new one (201).
func (s *Server) handleSaveDefinition(w http.ResponseWriter, r *http.Request) {
	body, ok := s.readBody(w, r)
	if !ok {
		return
	}
	def, err := workflow.Parse(body)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	spec, err := json.Marshal(def)
	if err != nil {
		s.internalError(w, "Failed to encode definition", err)
		return
	}
	stored, created, err := s.db.SaveDefinition(r.Context(), namespace(r), def.Name, spec)
	if err != nil {
		s.internalError(w, "Failed to save definition", err)
		return
	}
	out, err := toDefinitionJSON(stored, true)
	if err != nil {
		s.internalError(w, "Failed to decode definition", err)
		return
	}
	writeJSON(w, createdOr(created), out)
}

func (s *Server) handleListDefinitions(w http.ResponseWriter, r *http.Request) {
	defs, err := s.db.ListDefinitions(r.Context(), namespace(r))
	if err != nil {
		s.internalError(w, "Failed to list definitions", err)
		return
	}
	out := make([]definitionJSON, 0, len(defs))
	for _, d := range defs {
		j, err := toDefinitionJSON(d, false)
		if err != nil {
			s.internalError(w, "Failed to decode definition", err)
			return
		}
		out = append(out, j)
	}
	writeJSON(w, http.StatusOK, out)
}

// handleGetDefinition returns a definition (?version=N, default latest).
// With ?format=yaml it returns just the definition as YAML.
func (s *Server) handleGetDefinition(w http.ResponseWriter, r *http.Request) {
	version := 0
	if v := r.URL.Query().Get("version"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			writeError(w, http.StatusBadRequest, "version must be a positive integer")
			return
		}
		version = n
	}
	d, err := s.db.GetDefinition(r.Context(), namespace(r), r.PathValue("name"), version)
	if err != nil {
		s.internalError(w, "Failed to get definition", err)
		return
	}
	if d == nil {
		writeError(w, http.StatusNotFound, "workflow definition not found")
		return
	}
	out, err := toDefinitionJSON(d, true)
	if err != nil {
		s.internalError(w, "Failed to decode definition", err)
		return
	}
	if r.URL.Query().Get("format") == "yaml" {
		b, err := yaml.Marshal(out.Definition)
		if err != nil {
			s.internalError(w, "Failed to encode YAML", err)
			return
		}
		w.Header().Set("Content-Type", "application/yaml")
		_, _ = w.Write(b)
		return
	}
	writeJSON(w, http.StatusOK, out)
}

// --- Runs ---

type workflowJSON struct {
	ID              uuid.UUID         `json:"id"`
	Namespace       string            `json:"namespace"`
	Workflow        string            `json:"workflow"`
	Version         *int              `json:"version,omitempty"`
	Status          db.WorkflowStatus `json:"status"`
	Input           json.RawMessage   `json:"input"`
	ErrorMessage    string            `json:"error_message,omitempty"`
	CancelRequested bool              `json:"cancel_requested"`
	IdempotencyKey  string            `json:"idempotency_key,omitempty"`
	CreatedAt       time.Time         `json:"created_at"`
	UpdatedAt       time.Time         `json:"updated_at"`
	Steps           []stepJSON        `json:"steps,omitempty"`
}

type stepJSON struct {
	Name                   string            `json:"name"`
	Status                 db.StepStatus     `json:"status"`
	DependsOn              []string          `json:"depends_on,omitempty"`
	TaskID                 *uuid.UUID        `json:"task_id,omitempty"`
	TaskStatus             string            `json:"task_status,omitempty"`
	Error                  string            `json:"error,omitempty"`
	Outputs                map[string]string `json:"outputs,omitempty"`
	CompensationTaskID     *uuid.UUID        `json:"compensation_task_id,omitempty"`
	CompensationTaskStatus string            `json:"compensation_task_status,omitempty"`
}

func toWorkflowJSON(wf *db.Workflow, steps []*db.StepState) workflowJSON {
	out := workflowJSON{
		ID: wf.ID, Namespace: wf.Namespace, Workflow: wf.Type, Version: wf.DefinitionVersion, Status: wf.Status,
		Input: wf.Input, ErrorMessage: wf.ErrorMessage, CancelRequested: wf.CancelRequested,
		IdempotencyKey: wf.IdempotencyKey, CreatedAt: wf.CreatedAt, UpdatedAt: wf.UpdatedAt,
	}
	deps := map[string][]string{}
	if def, err := workflow.FromJSON(wf.Definition); err == nil && wf.Definition != nil {
		for _, st := range def.Steps {
			deps[st.Name] = st.DependsOn
		}
	}
	for _, st := range steps {
		out.Steps = append(out.Steps, stepJSON{
			Name: st.Name, Status: st.Status, DependsOn: deps[st.Name], TaskID: st.TaskID,
			TaskStatus: st.TaskStatus, Error: st.TaskError, Outputs: st.Outputs,
			CompensationTaskID: st.CompensationTaskID, CompensationTaskStatus: st.CompensationStatus,
		})
	}
	return out
}

func (s *Server) handleCreateWorkflow(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Workflow       string          `json:"workflow"`
		Version        int             `json:"version,omitempty"`
		Input          json.RawMessage `json:"input,omitempty"`
		IdempotencyKey string          `json:"idempotency_key,omitempty"`
	}
	if !s.decode(w, r, &req) {
		return
	}
	if req.Workflow == "" {
		writeError(w, http.StatusBadRequest, "workflow (the definition name) is required")
		return
	}
	resp, err := s.coordinator.SubmitWorkflow(r.Context(), &grpcapi.WorkflowRequest{
		Namespace:      namespace(r),
		Name:           req.Workflow,
		Version:        int32(req.Version),
		InputJson:      string(req.Input),
		IdempotencyKey: req.IdempotencyKey,
	})
	if err != nil {
		s.writeRPCError(w, err)
		return
	}
	s.respondWithWorkflow(w, r, resp.WorkflowId, createdOr(resp.Created))
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
	if wf == nil || wf.Namespace != namespace(r) {
		writeError(w, http.StatusNotFound, "workflow not found")
		return
	}
	steps, err := s.db.GetStepStates(r.Context(), id)
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
	q := r.URL.Query()
	workflows, err := s.db.ListWorkflows(r.Context(), db.WorkflowFilter{
		Namespace: namespace(r), Status: db.WorkflowStatus(q.Get("status")), Name: q.Get("workflow"), Limit: limit,
	})
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
	resp, err := s.coordinator.CancelWorkflow(r.Context(), &grpcapi.CancelWorkflowRequest{WorkflowId: id, Namespace: namespace(r)})
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
