package llm

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func mockProvider(t *testing.T) (Provider, *Mock) {
	t.Helper()
	m := NewMock()
	srv := httptest.NewServer(m)
	t.Cleanup(srv.Close)
	p, err := New(Config{BaseURL: srv.URL + "/v1", DefaultModel: "mock-model", Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return p, m
}

func TestToolCallingRoundTrip(t *testing.T) {
	p, _ := mockProvider(t)
	ctx := context.Background()
	msgs := []Message{
		{Role: "system", Content: `[[mock: {"tool_calls": [[{"name": "lookup", "arguments": {"id": 7}}]], "answer": "order 7 shipped"}]]`},
		{Role: "user", Content: "where is order 7?"},
	}
	tools := []Tool{{Name: "lookup", Parameters: json.RawMessage(`{"type":"object","properties":{"id":{"type":"integer"}}}`)}}

	r, err := p.Complete(ctx, Request{Messages: msgs, Tools: tools})
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Message.ToolCalls) != 1 || r.Message.ToolCalls[0].Name != "lookup" || r.Message.ToolCalls[0].Arguments != `{"id":7}` {
		t.Fatalf("first turn = %+v, want a lookup call", r.Message)
	}
	if r.Usage.InputTokens == 0 || r.Usage.OutputTokens == 0 {
		t.Fatalf("usage not reported: %+v", r.Usage)
	}

	// Send the tool's result back; the model then answers.
	msgs = append(msgs, r.Message, Message{Role: "tool", ToolCallID: r.Message.ToolCalls[0].ID, Content: `{"status":"shipped"}`})
	r, err = p.Complete(ctx, Request{Messages: msgs, Tools: tools})
	if err != nil {
		t.Fatal(err)
	}
	if r.Message.Content != "order 7 shipped" || len(r.Message.ToolCalls) != 0 {
		t.Fatalf("second turn = %+v", r.Message)
	}
}

func TestStructuredOutput(t *testing.T) {
	p, _ := mockProvider(t)
	schema := json.RawMessage(`{"type":"object","properties":{"cause":{"type":"string"},"transient":{"type":"boolean"}},"required":["cause","transient"]}`)
	r, err := p.Complete(context.Background(), Request{Messages: []Message{{Role: "user", Content: "classify"}}, Schema: schema})
	if err != nil {
		t.Fatal(err)
	}
	var out struct {
		Cause     string `json:"cause"`
		Transient bool   `json:"transient"`
	}
	if err := json.Unmarshal([]byte(r.Message.Content), &out); err != nil || out.Cause == "" {
		t.Fatalf("content %q is not the requested JSON: %v", r.Message.Content, err)
	}
}

func TestErrorClasses(t *testing.T) {
	p, _ := mockProvider(t)
	ask := func(directive string) error {
		_, err := p.Complete(context.Background(), Request{Messages: []Message{{Role: "user", Content: "[[mock: " + directive + "]]"}}})
		return err
	}

	err := ask(`{"fail": 429, "retry_after": 7}`)
	var rl *RateLimitError
	if !errors.As(err, &rl) || rl.RetryAfter != 7*time.Second || !errors.Is(err, ErrTransient) {
		t.Fatalf("429 gave %v, want a transient rate limit with a 7s retry", err)
	}
	if err := ask(`{"fail": 429, "retry_after": 7}`); err != nil {
		t.Fatalf("the mock fails once by default, then answers; got %v", err)
	}
	if err := ask(`{"fail": 503}`); !errors.Is(err, ErrTransient) {
		t.Fatalf("503 gave %v, want transient", err)
	}
	if err := ask(`{"fail": 400}`); !errors.Is(err, ErrPermanent) {
		t.Fatalf("400 gave %v, want permanent", err)
	}
}

func TestNotConfigured(t *testing.T) {
	if _, err := New(Config{BaseURL: "https://api.openai.com/v1"}); !errors.Is(err, ErrNotConfigured) {
		t.Fatalf("got %v, want ErrNotConfigured", err)
	}
}

func TestRedact(t *testing.T) {
	for in, leak := range map[string]string{
		"Authorization: Bearer abcdefghijklmnop1234":                                                       "abcdefghijklmnop1234",
		"export OPENAI_API_KEY=sk-proj-abcdefghijklmnopqrstuv":                                             "abcdefghijklmnopqrstuv",
		"postgres://app:hunter2secret@db:5432/prod":                                                        "hunter2secret",
		`{"password": "s3cr3t-pa55"}`:                                                                      "s3cr3t-pa55",
		"token=ghp_abcdefghijklmnopqrstuvwxyz0123":                                                         "ghp_abcdefghij",
		"aws AKIAABCDEFGHIJKLMNOP in config":                                                               "AKIAABCDEFGHIJKLMNOP",
		"key cnd_0123456789abcdef0123456789abcdef0123456789abcdef used":                                    "cnd_0123456789abcdef",
		"jwt eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dozjgNryP4J3jVmNHl0w5N_XgL0n3I9PlFUP0THsR8U": "dozjgNryP4J3jVmNHl0w5N",
	} {
		out := Redact(in)
		if strings.Contains(out, leak) {
			t.Errorf("Redact(%q) = %q still contains %q", in, out, leak)
		}
	}
	if got := Redact("connection refused on port 5432"); got != "connection refused on port 5432" {
		t.Errorf("ordinary text changed: %q", got)
	}
}

func TestCost(t *testing.T) {
	p := Prices{"gpt-4o": {Input: 2.5, Output: 10}, "gpt-4o-mini": {Input: 0.15, Output: 0.6}}
	cost, ok := p.Cost("gpt-4o-mini-2024-07-18", Usage{InputTokens: 1_000_000, OutputTokens: 1_000_000})
	if !ok || cost != 0.75 {
		t.Fatalf("dated mini variant cost %v (priced %v), want 0.75 from the longest matching name", cost, ok)
	}
	if _, ok := p.Cost("some-other-model", Usage{InputTokens: 10}); ok {
		t.Fatal("an unpriced model was priced")
	}
}
