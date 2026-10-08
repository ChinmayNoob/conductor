package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ChinmayNoob/conductor/pkg/grpcapi"
	"github.com/ChinmayNoob/conductor/pkg/llm"
	"github.com/ChinmayNoob/conductor/pkg/workflow"
)

// LabelLLM is advertised by workers that can reach a language model.
const LabelLLM = "type.llm"

// result is what a task produced, for the report to the coordinator.
type result struct {
	output  string
	outputs map[string]string
	usage   *grpcapi.LLMUsage
}

// runLLM asks the model for one completion. The answer is the output; with
// a schema, the JSON object's top-level fields become outputs, otherwise
// the answer is the "text" output, so later steps can use either.
func (s *Server) runLLM(ctx context.Context, specJSON []byte, timeout time.Duration) (result, error) {
	var spec workflow.LLMSpec
	if err := json.Unmarshal(specJSON, &spec); err != nil {
		return result{}, permanent(fmt.Errorf("invalid llm spec: %w", err))
	}
	req := llm.Request{Model: spec.Model, MaxTokens: spec.MaxTokens, Temperature: spec.Temperature, Tools: spec.Tools}
	agentTurn := len(spec.Messages) > 0
	switch {
	case agentTurn:
		req.Messages = spec.Messages
	default:
		if spec.System != "" {
			req.Messages = append(req.Messages, llm.Message{Role: "system", Content: spec.System})
		}
		req.Messages = append(req.Messages, llm.Message{Role: "user", Content: spec.Prompt})
	}
	if spec.Schema != nil {
		schema, err := json.Marshal(spec.Schema)
		if err != nil {
			return result{}, permanent(fmt.Errorf("invalid llm schema: %w", err))
		}
		req.Schema, req.SchemaName = schema, "output"
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	resp, err := s.llm.Complete(ctx, req)
	if err != nil {
		return result{}, err
	}

	res := result{output: resp.Message.Content, usage: s.usage(resp, req.Model)}
	if agentTurn {
		// An agent's turn: hand the whole reply, tool calls included, back
		// to the agent engine.
		msg, err := json.Marshal(resp.Message)
		if err != nil {
			return res, err
		}
		res.outputs = map[string]string{"message": string(msg)}
		if res.output == "" && len(resp.Message.ToolCalls) > 0 {
			names := make([]string, len(resp.Message.ToolCalls))
			for i, c := range resp.Message.ToolCalls {
				names[i] = c.Name
			}
			res.output = "calling " + strings.Join(names, ", ")
		}
		return res, nil
	}
	if spec.Schema == nil {
		res.outputs = map[string]string{"text": truncate(resp.Message.Content, maxOutputsBytes)}
		return res, nil
	}
	var fields map[string]any
	if err := json.Unmarshal([]byte(resp.Message.Content), &fields); err != nil {
		// Usually a cut-off answer; another attempt may well succeed.
		return res, fmt.Errorf("the model's answer is not the requested JSON object: %w", err)
	}
	res.outputs = make(map[string]string, len(fields))
	for k, v := range fields {
		if str, ok := v.(string); ok {
			res.outputs[k] = str
			continue
		}
		b, _ := json.Marshal(v)
		res.outputs[k] = string(b)
	}
	return res, nil
}

// usage reports what a completion spent, priced if the model has a price.
func (s *Server) usage(resp *llm.Response, requested string) *grpcapi.LLMUsage {
	model := resp.Model
	if model == "" {
		model = requested
	}
	u := &grpcapi.LLMUsage{
		Model:        model,
		InputTokens:  int64(resp.Usage.InputTokens),
		OutputTokens: int64(resp.Usage.OutputTokens),
	}
	if cost, ok := s.llmPrices.Cost(model, resp.Usage); ok {
		u.CostUsd = cost
	} else if cost, ok := s.llmPrices.Cost(requested, resp.Usage); ok {
		u.CostUsd = cost
	}
	return u
}

// permanentError marks a failure that retrying can't fix.
type permanentError struct{ error }

func (e permanentError) Unwrap() error { return e.error }

func permanent(err error) error { return permanentError{err} }

// retryAdvice reads a failure for the coordinator: how long to wait before a
// retry (a provider's Retry-After), and whether to retry at all.
func retryAdvice(err error) (retryAfter time.Duration, isPermanent bool) {
	var rl *llm.RateLimitError
	if errors.As(err, &rl) {
		return rl.RetryAfter, false
	}
	var pe permanentError
	return 0, errors.As(err, &pe) || errors.Is(err, llm.ErrPermanent)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
