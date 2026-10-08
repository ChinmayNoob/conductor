package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// openAI speaks the Chat Completions API.
type openAI struct {
	base  string
	key   string
	model string
	http  *http.Client
}

func newOpenAI(c Config) *openAI {
	return &openAI{
		base:  strings.TrimRight(c.BaseURL, "/"),
		key:   c.APIKey,
		model: c.DefaultModel,
		http:  &http.Client{Timeout: c.Timeout},
	}
}

type oaMessage struct {
	Role       string       `json:"role"`
	Content    *string      `json:"content"`
	ToolCalls  []oaToolCall `json:"tool_calls,omitempty"`
	ToolCallID string       `json:"tool_call_id,omitempty"`
}

type oaToolCall struct {
	ID       string `json:"id"`
	Type     string `json:"type"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

type oaRequest struct {
	Model               string      `json:"model"`
	Messages            []oaMessage `json:"messages"`
	Tools               []any       `json:"tools,omitempty"`
	ResponseFormat      any         `json:"response_format,omitempty"`
	MaxCompletionTokens int         `json:"max_completion_tokens,omitempty"`
	Temperature         *float64    `json:"temperature,omitempty"`
}

type oaResponse struct {
	Model   string `json:"model"`
	Choices []struct {
		Message      oaMessage `json:"message"`
		FinishReason string    `json:"finish_reason"`
	} `json:"choices"`
	Usage struct {
		PromptTokens     int `json:"prompt_tokens"`
		CompletionTokens int `json:"completion_tokens"`
	} `json:"usage"`
}

func (p *openAI) Complete(ctx context.Context, req Request) (*Response, error) {
	body := oaRequest{Model: req.Model, MaxCompletionTokens: req.MaxTokens, Temperature: req.Temperature}
	if body.Model == "" {
		body.Model = p.model
	}
	for _, m := range req.Messages {
		content := m.Content
		om := oaMessage{Role: m.Role, Content: &content, ToolCallID: m.ToolCallID}
		if m.Role == "assistant" && content == "" && len(m.ToolCalls) > 0 {
			om.Content = nil // the API wants null content alongside tool calls
		}
		for _, tc := range m.ToolCalls {
			c := oaToolCall{ID: tc.ID, Type: "function"}
			c.Function.Name, c.Function.Arguments = tc.Name, tc.Arguments
			om.ToolCalls = append(om.ToolCalls, c)
		}
		body.Messages = append(body.Messages, om)
	}
	for _, t := range req.Tools {
		params := t.Parameters
		if len(params) == 0 {
			params = json.RawMessage(`{"type":"object","properties":{}}`)
		}
		body.Tools = append(body.Tools, map[string]any{
			"type": "function",
			"function": map[string]any{
				"name": t.Name, "description": t.Description, "parameters": params,
			},
		})
	}
	if len(req.Schema) > 0 {
		name := req.SchemaName
		if name == "" {
			name = "result"
		}
		body.ResponseFormat = map[string]any{
			"type":        "json_schema",
			"json_schema": map[string]any{"name": name, "schema": req.Schema, "strict": true},
		}
	}

	raw, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrPermanent, err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, p.base+"/chat/completions", bytes.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrPermanent, err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if p.key != "" {
		httpReq.Header.Set("Authorization", "Bearer "+p.key)
	}

	resp, err := p.http.Do(httpReq)
	if err != nil {
		// Network failures and timeouts: the request may succeed later.
		return nil, fmt.Errorf("%w: %v", ErrTransient, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, fmt.Errorf("%w: reading the response: %v", ErrTransient, err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, statusError(resp, data)
	}

	var out oaResponse
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, fmt.Errorf("%w: unreadable response: %v", ErrTransient, err)
	}
	if len(out.Choices) == 0 {
		return nil, fmt.Errorf("%w: the response has no choices", ErrTransient)
	}
	choice := out.Choices[0]
	msg := Message{Role: "assistant"}
	if choice.Message.Content != nil {
		msg.Content = *choice.Message.Content
	}
	for _, tc := range choice.Message.ToolCalls {
		msg.ToolCalls = append(msg.ToolCalls, ToolCall{ID: tc.ID, Name: tc.Function.Name, Arguments: tc.Function.Arguments})
	}
	return &Response{
		Message:      msg,
		Usage:        Usage{InputTokens: out.Usage.PromptTokens, OutputTokens: out.Usage.CompletionTokens},
		Model:        out.Model,
		FinishReason: choice.FinishReason,
	}, nil
}

// statusError classifies a non-200 response.
func statusError(resp *http.Response, body []byte) error {
	msg := strings.TrimSpace(string(body))
	var e struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal(body, &e) == nil && e.Error.Message != "" {
		msg = e.Error.Message
	}
	if len(msg) > 500 {
		msg = msg[:500] + "…"
	}
	switch {
	case resp.StatusCode == http.StatusTooManyRequests:
		return &RateLimitError{RetryAfter: retryAfter(resp.Header), Message: msg}
	case resp.StatusCode >= 500 || resp.StatusCode == http.StatusRequestTimeout:
		return fmt.Errorf("%w: %s: %s", ErrTransient, resp.Status, msg)
	default:
		return fmt.Errorf("%w: %s: %s", ErrPermanent, resp.Status, msg)
	}
}

// retryAfter reads how long a 429 asks us to wait: retry-after-ms,
// Retry-After in seconds or as a date, or a default.
func retryAfter(h http.Header) time.Duration {
	if ms, err := strconv.Atoi(h.Get("retry-after-ms")); err == nil && ms > 0 {
		return time.Duration(ms) * time.Millisecond
	}
	v := h.Get("Retry-After")
	if secs, err := strconv.ParseFloat(v, 64); err == nil && secs > 0 {
		return time.Duration(secs * float64(time.Second))
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := time.Until(t); d > 0 {
			return d
		}
	}
	return 10 * time.Second
}
