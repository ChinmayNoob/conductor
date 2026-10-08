package ai

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/ChinmayNoob/conductor/pkg/db"
	"github.com/ChinmayNoob/conductor/pkg/llm"
)

// ErrNotConfigured is returned when a feature needs a model and none is set.
var ErrNotConfigured = errors.New("no language model is configured (set OPENAI_API_KEY or CONDUCTOR_LLM_BASE_URL)")

// Assistant answers with a model's help. Provider may be nil: failures are
// then still classified by rules.
type Assistant struct {
	DB       *db.DB
	Provider llm.Provider
	Prices   llm.Prices
	Model    string // "" = the provider's default
	Log      *slog.Logger
}

func (a *Assistant) log() *slog.Logger {
	if a.Log != nil {
		return a.Log
	}
	return slog.Default()
}

// Explanation is what the assistant says about a failed task.
type Explanation struct {
	Classification
	Cause string `json:"cause,omitempty"`
	Fix   string `json:"fix,omitempty"`
	Model string `json:"model,omitempty"`
	Usage Usage  `json:"usage"`
}

// Usage is what model calls cost.
type Usage struct {
	InputTokens  int64   `json:"input_tokens"`
	OutputTokens int64   `json:"output_tokens"`
	CostUSD      float64 `json:"cost_usd"`
}

func (u Usage) add(o Usage) Usage {
	return Usage{u.InputTokens + o.InputTokens, u.OutputTokens + o.OutputTokens, u.CostUSD + o.CostUSD}
}

func (a *Assistant) complete(ctx context.Context, req llm.Request) (*llm.Response, Usage, error) {
	if a.Provider == nil {
		return nil, Usage{}, ErrNotConfigured
	}
	if req.Model == "" {
		req.Model = a.Model
	}
	resp, err := a.Provider.Complete(ctx, req)
	if err != nil {
		return nil, Usage{}, err
	}
	u := Usage{InputTokens: int64(resp.Usage.InputTokens), OutputTokens: int64(resp.Usage.OutputTokens)}
	model := resp.Model
	if model == "" {
		model = req.Model
	}
	if cost, ok := a.Prices.Cost(model, resp.Usage); ok {
		u.CostUSD = cost
	}
	return resp, u, nil
}

const explainSchema = `{
  "type": "object",
  "properties": {
    "class": {"type": "string", "enum": ["transient", "permanent", "needs_attention", "unknown"]},
    "confidence": {"type": "number"},
    "cause": {"type": "string"},
    "fix": {"type": "string"}
  },
  "required": ["class", "confidence", "cause", "fix"],
  "additionalProperties": false
}`

// Explain reads a failed task and says what class of failure it is, the
// likely cause and a suggested fix. Rules classify first; the model's class
// is used only when no rule matches. It is advice: nothing here changes how
// the task is retried.
func (a *Assistant) Explain(ctx context.Context, t *db.Task) (*Explanation, error) {
	if t.Status != db.StatusFailed {
		return nil, fmt.Errorf("task %s is %s, not FAILED", t.ID, t.Status)
	}
	var history []string
	if a.DB != nil {
		if attempts, err := a.DB.ListAttempts(ctx, t.ID); err == nil {
			for _, at := range attempts {
				history = append(history, fmt.Sprintf("attempt %d: %s", at.Attempt, head(llm.Redact(at.ErrorMessage), 300)))
			}
		}
	}
	if len(history) > 4 {
		history = history[len(history)-4:]
	}

	out := &Explanation{}
	if c, ok := ClassifyRules(t.ErrorMessage, t.Output); ok {
		out.Classification = c
	} else {
		out.Classification = Classification{Class: ClassUnknown, Source: "rules", Reason: "no rule matched"}
	}
	if a.Provider == nil {
		return out, nil
	}

	resp, usage, err := a.complete(ctx, llm.Request{
		Messages: []llm.Message{
			{Role: "system", Content: explainSystem},
			{Role: "user", Content: explainPrompt(t, history, out.Classification)},
		},
		Schema: json.RawMessage(explainSchema), SchemaName: "explanation",
		MaxTokens: 600,
	})
	if err != nil {
		return nil, err
	}
	out.Usage, out.Model = usage, resp.Model
	var answer struct {
		Class      string  `json:"class"`
		Confidence float64 `json:"confidence"`
		Cause      string  `json:"cause"`
		Fix        string  `json:"fix"`
	}
	if err := json.Unmarshal([]byte(resp.Message.Content), &answer); err != nil {
		return nil, fmt.Errorf("the model's explanation is not valid JSON: %w", err)
	}
	// Redact again: the model may repeat something it should not have seen.
	out.Cause, out.Fix = llm.Redact(strings.TrimSpace(answer.Cause)), llm.Redact(strings.TrimSpace(answer.Fix))
	if out.Source == "rules" && out.Class == ClassUnknown && validClass(answer.Class) {
		out.Classification = Classification{
			Class: answer.Class, Confidence: clamp01(answer.Confidence), Source: "model",
		}
	}
	return out, nil
}

// ExplainAndSave explains a task and stores the explanation.
func (a *Assistant) ExplainAndSave(ctx context.Context, t *db.Task) (*Explanation, error) {
	e, err := a.Explain(ctx, t)
	if err != nil {
		return nil, err
	}
	err = a.DB.SaveExplanation(ctx, db.Explanation{
		TaskID: t.ID, Attempt: t.Attempt, Class: e.Class, Confidence: e.Confidence, Source: e.Source,
		Cause: e.Cause, Fix: e.Fix, Model: e.Model,
		InputTokens: e.Usage.InputTokens, OutputTokens: e.Usage.OutputTokens, CostUSD: e.Usage.CostUSD,
	})
	return e, err
}

// Run explains new failures in namespaces that opted in, until ctx ends. It
// does nothing without a model.
func (a *Assistant) Run(ctx context.Context) {
	if a.Provider == nil {
		return
	}
	tick := time.NewTicker(15 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		tasks, err := a.DB.UnexplainedFailures(ctx, 24*time.Hour, 5)
		if err != nil {
			a.log().Warn("failed to list failures to explain", "error", err)
			continue
		}
		for _, t := range tasks {
			cctx, cancel := context.WithTimeout(ctx, 60*time.Second)
			_, err := a.ExplainAndSave(cctx, t)
			cancel()
			if err != nil {
				// Record the failure so this one is not retried forever; the
				// explain button can try again.
				a.log().Warn("failed to explain a failure", "task_id", t.ID, "error", err)
				_ = a.DB.SaveExplanation(ctx, db.Explanation{TaskID: t.ID, Attempt: t.Attempt, Class: ClassUnknown, Source: "rules",
					Cause: "The assistant could not explain this failure: " + head(err.Error(), 200)})
			}
		}
	}
}

func clamp01(f float64) float64 {
	return min(max(f, 0), 1)
}
