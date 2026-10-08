// Package llm talks to language models for Conductor's AI features: the llm
// task type, durable agents, and the operations assistant.
//
// Providers sit behind the Provider interface. The OpenAI implementation
// speaks the Chat Completions API, which many other servers (Ollama, vLLM,
// LiteLLM, Azure OpenAI) also serve, so CONDUCTOR_LLM_BASE_URL can point at
// any of them. Tests use the mock server in this package.
package llm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"
)

// Message is one turn of a conversation.
type Message struct {
	Role       string     `json:"role"` // system, user, assistant, tool
	Content    string     `json:"content"`
	ToolCalls  []ToolCall `json:"tool_calls,omitempty"`   // assistant: tools the model wants run
	ToolCallID string     `json:"tool_call_id,omitempty"` // tool: which call this answers
}

// Tool is a function the model may call.
type Tool struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters,omitempty"` // a JSON Schema object
}

// ToolCall is the model asking for a tool to be run.
type ToolCall struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Arguments string `json:"arguments"` // JSON, as the model wrote it
}

// Request is one completion.
type Request struct {
	Model    string
	Messages []Message
	Tools    []Tool
	// Schema, if set, asks for JSON output matching this JSON Schema.
	Schema      json.RawMessage
	SchemaName  string
	MaxTokens   int      // 0 = provider default
	Temperature *float64 // nil = provider default
}

// Usage counts tokens.
type Usage struct {
	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
}

func (u Usage) Add(o Usage) Usage {
	return Usage{u.InputTokens + o.InputTokens, u.OutputTokens + o.OutputTokens}
}

// Response is the model's reply.
type Response struct {
	Message      Message
	Usage        Usage
	Model        string // the model that actually answered
	FinishReason string
}

// Provider completes conversations.
type Provider interface {
	Complete(ctx context.Context, req Request) (*Response, error)
}

// Error classes. Callers decide retries from these.
var (
	// ErrTransient: worth retrying (timeouts, 5xx, overloaded).
	ErrTransient = errors.New("transient model error")
	// ErrPermanent: retrying won't help (bad request, auth, unknown model).
	ErrPermanent = errors.New("permanent model error")
	// ErrNotConfigured: no API key or endpoint.
	ErrNotConfigured = errors.New("no language model is configured: set OPENAI_API_KEY (or CONDUCTOR_LLM_API_KEY)")
)

// RateLimitError is a 429: retry after RetryAfter.
type RateLimitError struct {
	RetryAfter time.Duration
	Message    string
}

func (e *RateLimitError) Error() string {
	return fmt.Sprintf("rate limited by the model provider (retry after %v): %s", e.RetryAfter, e.Message)
}

func (e *RateLimitError) Unwrap() error { return ErrTransient }

// Config selects and configures the provider.
type Config struct {
	BaseURL      string // e.g. https://api.openai.com/v1
	APIKey       string
	DefaultModel string
	Timeout      time.Duration
}

// ConfigFromEnv reads CONDUCTOR_LLM_BASE_URL, CONDUCTOR_LLM_API_KEY (or
// OPENAI_API_KEY), CONDUCTOR_LLM_MODEL and CONDUCTOR_LLM_TIMEOUT.
func ConfigFromEnv() Config {
	c := Config{
		BaseURL:      os.Getenv("CONDUCTOR_LLM_BASE_URL"),
		APIKey:       os.Getenv("CONDUCTOR_LLM_API_KEY"),
		DefaultModel: os.Getenv("CONDUCTOR_LLM_MODEL"),
		Timeout:      2 * time.Minute,
	}
	if c.BaseURL == "" {
		c.BaseURL = "https://api.openai.com/v1"
	}
	if c.APIKey == "" {
		c.APIKey = os.Getenv("OPENAI_API_KEY")
	}
	if c.DefaultModel == "" {
		c.DefaultModel = "gpt-4o-mini"
	}
	if d, err := time.ParseDuration(os.Getenv("CONDUCTOR_LLM_TIMEOUT")); err == nil && d > 0 {
		c.Timeout = d
	}
	return c
}

// Configured reports whether a provider can be built. A non-default base URL
// counts even without a key, for local servers that don't need one.
func (c Config) Configured() bool {
	return c.APIKey != "" || c.BaseURL != "https://api.openai.com/v1"
}

// New returns the provider for c.
func New(c Config) (Provider, error) {
	if !c.Configured() {
		return nil, ErrNotConfigured
	}
	return newOpenAI(c), nil
}
