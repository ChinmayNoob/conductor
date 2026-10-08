package worker

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ChinmayNoob/conductor/pkg/llm"
)

func llmWorker(t *testing.T) *Server {
	t.Helper()
	srv := httptest.NewServer(llm.NewMock())
	t.Cleanup(srv.Close)
	p, err := llm.New(llm.Config{BaseURL: srv.URL + "/v1", DefaultModel: "mock-model", Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return NewServer(Options{LLM: p, LLMPrices: llm.Prices{"mock-model": {Input: 1, Output: 2}}})
}

func spec(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestLLMTaskText(t *testing.T) {
	w := llmWorker(t)
	if w.Labels()[LabelLLM] != "true" {
		t.Fatal("a worker with a model doesn't advertise llm tasks")
	}
	res, err := w.runLLM(context.Background(), spec(t, map[string]any{"prompt": "say hi"}), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if res.output != "Mock reply: say hi" || res.outputs["text"] != res.output {
		t.Fatalf("output %q, outputs %v", res.output, res.outputs)
	}
	if res.usage == nil || res.usage.Model != "mock-model" || res.usage.InputTokens == 0 || res.usage.CostUsd == 0 {
		t.Fatalf("usage = %+v, want tokens and a price", res.usage)
	}
}

func TestLLMTaskSchemaFieldsBecomeOutputs(t *testing.T) {
	w := llmWorker(t)
	res, err := w.runLLM(context.Background(), spec(t, map[string]any{
		"prompt": `[[mock: {"json": {"sentiment": "positive", "score": 0.9, "tags": ["a"]}}]] classify`,
		"schema": map[string]any{"type": "object"},
	}), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if res.outputs["sentiment"] != "positive" || res.outputs["score"] != "0.9" || res.outputs["tags"] != `["a"]` {
		t.Fatalf("outputs = %v", res.outputs)
	}

	// An answer that isn't the requested JSON fails, and may be retried.
	_, err = w.runLLM(context.Background(), spec(t, map[string]any{
		"prompt": `[[mock: {"answer": "not json"}]]`, "schema": map[string]any{"type": "object"},
	}), time.Minute)
	if after, perm := retryAdvice(err); err == nil || perm || after != 0 {
		t.Fatalf("non-JSON answer: err=%v permanent=%v", err, perm)
	}
}

func TestLLMRetryAdvice(t *testing.T) {
	w := llmWorker(t)
	_, err := w.runLLM(context.Background(), spec(t, map[string]any{"prompt": `[[mock: {"fail": 429, "retry_after": 12}]]`}), time.Minute)
	if after, perm := retryAdvice(err); after != 12*time.Second || perm {
		t.Fatalf("429: retry after %v, permanent %v; want 12s and retryable", after, perm)
	}
	_, err = w.runLLM(context.Background(), spec(t, map[string]any{"prompt": `[[mock: {"fail": 400}]]`}), time.Minute)
	if _, perm := retryAdvice(err); !perm {
		t.Fatalf("400 (%v) should not be retried", err)
	}
}

func TestNoModelNoLLMTasks(t *testing.T) {
	w := NewServer(Options{})
	if _, ok := w.Labels()[LabelLLM]; ok {
		t.Fatal("a worker without a model advertises llm tasks")
	}
}
