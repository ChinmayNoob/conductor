package api

import (
	"encoding/json"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"github.com/ChinmayNoob/conductor/pkg/db"
	"github.com/ChinmayNoob/conductor/pkg/grpcapi"
	"github.com/ChinmayNoob/conductor/pkg/tracing"
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
	TraceID         string            `json:"trace_id,omitempty"`
	Spend           *spendJSON        `json:"llm_spend,omitempty"`
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
	Wait                   *waitJSON         `json:"wait,omitempty"` // approval and signal steps
	AgentRunID             *uuid.UUID        `json:"agent_run_id,omitempty"`
}

// waitJSON is what a waiting step waits for, and how it was decided.
type waitJSON struct {
	Kind      string     `json:"kind"`              // approval or signal
	Message   string     `json:"message,omitempty"` // approval: the question; signal: its name
	Deadline  *time.Time `json:"deadline,omitempty"`
	Waiting   bool       `json:"waiting"`
	DecidedBy string     `json:"decided_by,omitempty"`
}

func toWorkflowJSON(wf *db.Workflow, steps []*db.StepState) workflowJSON {
	out := workflowJSON{
		ID: wf.ID, Namespace: wf.Namespace, Workflow: wf.Type, Version: wf.DefinitionVersion, Status: wf.Status,
		Input: wf.Input, ErrorMessage: wf.ErrorMessage, CancelRequested: wf.CancelRequested,
		IdempotencyKey: userKey(wf.IdempotencyKey), TraceID: tracing.TraceID(wf.TraceParent),
		CreatedAt: wf.CreatedAt, UpdatedAt: wf.UpdatedAt,
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
			Wait: waitOf(st), AgentRunID: st.AgentRunID,
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
		IdempotencyKey: idempotencyKey(req.IdempotencyKey),
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
	out := toWorkflowJSON(wf, steps)
	spend, err := s.db.WorkflowSpend(r.Context(), id)
	if err != nil {
		s.internalError(w, "Failed to sum workflow spend", err)
		return
	}
	if spend.Tokens > 0 {
		out.Spend = &spendJSON{Tokens: spend.Tokens, CostUSD: spend.CostUSD}
	}
	writeJSON(w, code, out)
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

// spendJSON is model usage summed over a workflow run's tasks.
type spendJSON struct {
	Tokens  int64   `json:"tokens"`
	CostUSD float64 `json:"cost_usd"`
}

func waitOf(st *db.StepState) *waitJSON {
	if st.WaitKind == "" {
		return nil
	}
	return &waitJSON{Kind: st.WaitKind, Message: st.WaitMessage, Deadline: st.WaitDeadline,
		Waiting: st.Status == db.StepStatus("RUNNING") && st.TaskStatus == "", DecidedBy: st.DecidedBy}
}

// handleDecideStep approves or rejects a waiting approval step.
func (s *Server) handleDecideStep(approve bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Comment string `json:"comment"`
		}
		if r.ContentLength != 0 && !s.decode(w, r, &req) {
			return
		}
		_, err := s.coordinator.DecideStep(r.Context(), &grpcapi.DecideStepRequest{
			WorkflowId: r.PathValue("id"), Namespace: namespace(r), Step: r.PathValue("step"),
			Approve: approve, Comment: req.Comment, DecidedBy: keyName(r),
		})
		if err != nil {
			s.writeRPCError(w, err)
			return
		}
		s.respondWithWorkflow(w, r, r.PathValue("id"), http.StatusOK)
	}
}

// handleSignal sends a signal (a JSON object) to a run.
func (s *Server) handleSignal(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if !signalNameRe.MatchString(name) {
		writeError(w, http.StatusBadRequest, "signal names are letters, digits, '-' or '_'")
		return
	}
	body, ok := s.readBody(w, r)
	if !ok {
		return
	}
	resp, err := s.coordinator.SignalWorkflow(r.Context(), &grpcapi.SignalRequest{
		WorkflowId: r.PathValue("id"), Namespace: namespace(r), Name: name, PayloadJson: body, SentBy: keyName(r),
	})
	if err != nil {
		s.writeRPCError(w, err)
		return
	}
	code := http.StatusOK
	if !resp.Delivered {
		code = http.StatusAccepted // kept until a step waits for it
	}
	writeJSON(w, code, map[string]bool{"delivered": resp.Delivered})
}

// handleListApprovals lists approval steps waiting in the namespace.
func (s *Server) handleListApprovals(w http.ResponseWriter, r *http.Request) {
	list, err := s.db.ListWaiting(r.Context(), namespace(r), db.WaitApproval)
	if err != nil {
		s.internalError(w, "Failed to list approvals", err)
		return
	}
	type approvalJSON struct {
		WorkflowID uuid.UUID  `json:"workflow_id"`
		Workflow   string     `json:"workflow"`
		Step       string     `json:"step"`
		Message    string     `json:"message"`
		Deadline   *time.Time `json:"deadline,omitempty"`
		OnTimeout  string     `json:"on_timeout,omitempty"`
		Since      time.Time  `json:"since"`
	}
	out := make([]approvalJSON, 0, len(list))
	for _, a := range list {
		out = append(out, approvalJSON{WorkflowID: a.WorkflowID, Workflow: a.Workflow, Step: a.Step,
			Message: a.Message, Deadline: a.Deadline, OnTimeout: a.OnTimeout, Since: a.Since})
	}
	writeJSON(w, http.StatusOK, out)
}

var signalNameRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,62}$`)

// keyName names the API key that made a request, for the record.
func keyName(r *http.Request) string {
	if k, _ := r.Context().Value(apiKeyCtxKey).(*db.APIKey); k != nil {
		return k.Name
	}
	return ""
}

// handleGetAgentRun shows an agent run: its progress, the conversation, and
// every model and tool call it made (each a task).
func (s *Server) handleGetAgentRun(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid agent run ID")
		return
	}
	run, err := s.db.GetAgentRun(r.Context(), id)
	if err != nil {
		s.internalError(w, "Failed to get agent run", err)
		return
	}
	if run == nil || run.Namespace != namespace(r) {
		writeError(w, http.StatusNotFound, "agent run not found")
		return
	}
	tasks, err := s.db.AgentTasks(r.Context(), id, 0)
	if err != nil {
		s.internalError(w, "Failed to list agent tasks", err)
		return
	}
	spend, err := s.db.AgentRunSpend(r.Context(), id)
	if err != nil {
		s.internalError(w, "Failed to sum agent spend", err)
		return
	}
	type callJSON struct {
		TaskID     uuid.UUID     `json:"task_id"`
		Turn       int           `json:"turn"`
		Role       string        `json:"role"` // llm or tool
		ToolCallID string        `json:"tool_call_id,omitempty"`
		Command    string        `json:"command"`
		Status     db.TaskStatus `json:"status"`
		Attempt    int           `json:"attempt"`
	}
	calls := make([]callJSON, 0, len(tasks))
	for _, t := range tasks {
		calls = append(calls, callJSON{TaskID: t.ID, Turn: t.AgentTurn, Role: t.AgentRole, ToolCallID: t.ToolCallID,
			Command: t.Data, Status: t.Status, Attempt: t.Attempt})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"id": run.ID, "workflow_id": run.WorkflowID, "status": run.Status, "turn": run.Turn, "phase": run.Phase,
		"tool_calls": run.ToolCalls, "answer": run.Answer, "error": run.Error, "deadline": run.Deadline,
		"messages": run.Messages, "tasks": calls, "llm_spend": spendJSON{Tokens: spend.Tokens, CostUSD: spend.CostUSD},
		"created_at": run.CreatedAt, "updated_at": run.UpdatedAt,
	})
}
