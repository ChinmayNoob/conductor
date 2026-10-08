package api

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/ChinmayNoob/conductor/pkg/ai"
	"github.com/ChinmayNoob/conductor/pkg/db"
	"github.com/ChinmayNoob/conductor/pkg/llm"
)

// SetAssistant enables the operations assistant endpoints. Without one they
// answer 503.
func (s *Server) SetAssistant(a *ai.Assistant) { s.assistant = a }

// assistantFor returns the assistant if this request's namespace opted in.
// What the assistant sends to a model is redacted, but it is still task
// output leaving the cluster, so it is opt-in per namespace.
func (s *Server) assistantFor(w http.ResponseWriter, r *http.Request) *ai.Assistant {
	if s.assistant == nil {
		writeError(w, http.StatusServiceUnavailable, "the operations assistant is not available on this server")
		return nil
	}
	ns, err := s.db.GetNamespace(r.Context(), namespace(r))
	if err != nil {
		s.internalError(w, "Failed to get namespace", err)
		return nil
	}
	if ns == nil || !ns.AIAssist {
		writeError(w, http.StatusForbidden,
			"AI assist is off for this namespace; an admin can turn it on with ai_assist: true (redacted task output is then sent to the configured model)")
		return nil
	}
	return s.assistant
}

// modelError turns a provider failure into a response.
func (s *Server) modelError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ai.ErrNotConfigured):
		writeError(w, http.StatusServiceUnavailable, err.Error())
	case errors.Is(err, llm.ErrNotConfigured):
		writeError(w, http.StatusServiceUnavailable, err.Error())
	default:
		s.log.Warn("assistant request failed", "error", err)
		writeError(w, http.StatusBadGateway, "the model could not answer: "+err.Error())
	}
}

type explanationJSON struct {
	TaskID       string    `json:"task_id"`
	Attempt      int       `json:"attempt"`
	Class        string    `json:"class"`
	Confidence   float64   `json:"confidence"`
	Source       string    `json:"source"` // rules or model
	Cause        string    `json:"cause,omitempty"`
	Fix          string    `json:"fix,omitempty"`
	Model        string    `json:"model,omitempty"`
	InputTokens  int64     `json:"input_tokens"`
	OutputTokens int64     `json:"output_tokens"`
	CostUSD      float64   `json:"cost_usd"`
	CreatedAt    time.Time `json:"created_at"`
}

func toExplanationJSON(e *db.Explanation) explanationJSON {
	return explanationJSON{
		TaskID: e.TaskID.String(), Attempt: e.Attempt, Class: e.Class, Confidence: e.Confidence, Source: e.Source,
		Cause: e.Cause, Fix: e.Fix, Model: e.Model, InputTokens: e.InputTokens, OutputTokens: e.OutputTokens,
		CostUSD: e.CostUSD, CreatedAt: e.CreatedAt,
	}
}

// GET /v1/tasks/{id}/explanation: the stored explanation, if any.
func (s *Server) handleGetExplanation(w http.ResponseWriter, r *http.Request) {
	t := s.loadTask(w, r, r.PathValue("id"))
	if t == nil {
		return
	}
	e, err := s.db.GetExplanation(r.Context(), t.ID)
	if err != nil {
		s.internalError(w, "Failed to get explanation", err)
		return
	}
	if e == nil {
		writeError(w, http.StatusNotFound, "no explanation for this task")
		return
	}
	writeJSON(w, http.StatusOK, toExplanationJSON(e))
}

// POST /v1/tasks/{id}/explain: explain a failed task now.
func (s *Server) handleExplainTask(w http.ResponseWriter, r *http.Request) {
	t := s.loadTask(w, r, r.PathValue("id"))
	if t == nil {
		return
	}
	a := s.assistantFor(w, r)
	if a == nil {
		return
	}
	if t.Status != db.StatusFailed {
		writeError(w, http.StatusConflict, "only failed tasks can be explained; this one is "+string(t.Status))
		return
	}
	ctx, cancel := contextWithTimeout(r, 60*time.Second)
	defer cancel()
	if _, err := a.ExplainAndSave(ctx, t); err != nil {
		s.modelError(w, err)
		return
	}
	e, err := s.db.GetExplanation(r.Context(), t.ID)
	if err != nil || e == nil {
		s.internalError(w, "Failed to get explanation", err)
		return
	}
	writeJSON(w, http.StatusOK, toExplanationJSON(e))
}

// POST /v1/ai/workflow: draft a workflow definition from a description. The
// draft is returned for a person to read; nothing is saved.
func (s *Server) handleAIWorkflow(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Description string `json:"description"`
	}
	if !s.decode(w, r, &req) {
		return
	}
	a := s.assistantFor(w, r)
	if a == nil {
		return
	}
	ctx, cancel := contextWithTimeout(r, 90*time.Second)
	defer cancel()
	draft, err := a.GenerateWorkflow(ctx, req.Description)
	if err != nil {
		if draft == nil && (errors.Is(err, ai.ErrNotConfigured) || isModelError(err)) {
			s.modelError(w, err)
			return
		}
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, draft)
}

// POST /v1/ai/ask: answer a question about the namespace, read-only.
func (s *Server) handleAIAsk(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Question string `json:"question"`
	}
	if !s.decode(w, r, &req) {
		return
	}
	a := s.assistantFor(w, r)
	if a == nil {
		return
	}
	ctx, cancel := contextWithTimeout(r, 90*time.Second)
	defer cancel()
	ans, err := a.Ask(ctx, namespace(r), req.Question)
	if err != nil {
		if errors.Is(err, ai.ErrNotConfigured) || isModelError(err) {
			s.modelError(w, err)
			return
		}
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, ans)
}

func contextWithTimeout(r *http.Request, d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(r.Context(), d)
}

// isModelError reports whether err came from talking to the model, as
// opposed to a bad request.
func isModelError(err error) bool {
	return errors.Is(err, llm.ErrTransient) || errors.Is(err, llm.ErrPermanent) || errors.Is(err, llm.ErrNotConfigured)
}
