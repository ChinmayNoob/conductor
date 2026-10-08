package llm

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Mock is a stand-in model server speaking the Chat Completions API, for
// tests and demos (conductor mock-llm). It is deterministic and free.
//
// A conversation can script it with a directive anywhere in its messages:
//
//		[[mock: {"tool_calls": [[{"name": "lookup", "arguments": {"id": 7}}]], "answer": "done"}]]
//
//	  - tool_calls: per assistant turn, the calls to make; after the last
//	    scripted turn the model answers.
//	  - answer: the final text. json: a final JSON value instead.
//	  - fail: an HTTP status to return (429, 500, 400) fail_times times
//	    (default 1) for this directive, with retry_after seconds on 429.
//	  - slow_ms: wait this long before answering.
//
// Without a directive it echoes the last user message, or, when a JSON
// schema is requested, fills the schema's properties with placeholders.
type Mock struct {
	mu       sync.Mutex
	failures map[string]int
	requests int
}

type mockDirective struct {
	ToolCalls  [][]mockCall `json:"tool_calls"`
	Answer     string       `json:"answer"`
	JSON       any          `json:"json"`
	Fail       int          `json:"fail"`
	FailTimes  int          `json:"fail_times"`
	RetryAfter float64      `json:"retry_after"`
	SlowMS     int          `json:"slow_ms"`
}

type mockCall struct {
	Name      string `json:"name"`
	Arguments any    `json:"arguments"`
}

// findDirective returns the JSON object after "[[mock:" in text, if any.
// It is read with a JSON decoder: the JSON itself may contain "]]".
func findDirective(text string) (json.RawMessage, bool) {
	i := strings.Index(text, "[[mock:")
	if i < 0 {
		return nil, false
	}
	var raw json.RawMessage
	if err := json.NewDecoder(strings.NewReader(text[i+len("[[mock:"):])).Decode(&raw); err != nil {
		return json.RawMessage(text[i:]), true // reported as invalid below
	}
	return raw, true
}

func NewMock() *Mock { return &Mock{failures: map[string]int{}} }

// Requests returns how many completions were asked for.
func (m *Mock) Requests() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.requests
}

func (m *Mock) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/mock/stats"):
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"requests":%d}`, m.Requests())
		return
	case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/models"):
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"object":"list","data":[{"id":"mock-model","object":"model"}]}`)
		return
	case r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, "/chat/completions"):
		http.NotFound(w, r)
		return
	}

	var req struct {
		Model          string      `json:"model"`
		Messages       []oaMessage `json:"messages"`
		ResponseFormat *struct {
			JSONSchema struct {
				Schema json.RawMessage `json:"schema"`
			} `json:"json_schema"`
		} `json:"response_format"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		mockError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	m.mu.Lock()
	m.requests++
	m.mu.Unlock()

	var d mockDirective
	var key, lastUser string
	turn := 0
	promptChars := 0
	for _, msg := range req.Messages {
		text := ""
		if msg.Content != nil {
			text = *msg.Content
		}
		promptChars += len(text)
		if raw, ok := findDirective(text); ok && key == "" {
			key = string(raw)
			if err := json.Unmarshal(raw, &d); err != nil {
				mockError(w, http.StatusBadRequest, "invalid mock directive: "+err.Error())
				return
			}
		}
		switch msg.Role {
		case "assistant":
			turn++
		case "user":
			lastUser = text
		}
	}

	if d.SlowMS > 0 {
		time.Sleep(time.Duration(d.SlowMS) * time.Millisecond)
	}
	if d.Fail != 0 {
		times := max(d.FailTimes, 1)
		m.mu.Lock()
		m.failures[key]++
		n := m.failures[key]
		m.mu.Unlock()
		if n <= times {
			if d.Fail == http.StatusTooManyRequests && d.RetryAfter > 0 {
				w.Header().Set("Retry-After", strconv.FormatFloat(d.RetryAfter, 'f', -1, 64))
			}
			mockError(w, d.Fail, fmt.Sprintf("mock failure %d of %d", n, times))
			return
		}
	}

	model := req.Model
	if model == "" {
		model = "mock-model"
	}
	msg := oaMessage{Role: "assistant"}
	finish := "stop"
	switch {
	case turn < len(d.ToolCalls):
		for i, c := range d.ToolCalls[turn] {
			args, _ := json.Marshal(c.Arguments)
			if c.Arguments == nil {
				args = []byte("{}")
			}
			tc := oaToolCall{ID: fmt.Sprintf("call_%d_%d", turn, i), Type: "function"}
			tc.Function.Name, tc.Function.Arguments = c.Name, string(args)
			msg.ToolCalls = append(msg.ToolCalls, tc)
		}
		finish = "tool_calls"
	case d.JSON != nil:
		b, _ := json.Marshal(d.JSON)
		msg.Content = ptr(string(b))
	case d.Answer != "":
		msg.Content = ptr(d.Answer)
	case req.ResponseFormat != nil:
		b, _ := json.Marshal(fillSchema(req.ResponseFormat.JSONSchema.Schema))
		msg.Content = ptr(string(b))
	default:
		reply := lastUser
		if i := strings.Index(reply, "[[mock:"); i >= 0 {
			reply = reply[:i]
		}
		if len(reply) > 200 {
			reply = reply[:200]
		}
		msg.Content = ptr("Mock reply: " + strings.TrimSpace(reply))
	}

	out := map[string]any{
		"id": "chatcmpl-mock", "object": "chat.completion", "model": model,
		"choices": []any{map[string]any{"index": 0, "message": msg, "finish_reason": finish}},
		"usage": map[string]int{
			"prompt_tokens":     promptChars/4 + 8,
			"completion_tokens": len(derefStr(msg.Content))/4 + 4*len(msg.ToolCalls) + 2,
		},
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

// fillSchema builds a value matching a JSON Schema with placeholder values.
func fillSchema(raw json.RawMessage) any {
	var s map[string]any
	if json.Unmarshal(raw, &s) != nil {
		return map[string]any{}
	}
	return fillValue(s, "value")
}

func fillValue(s map[string]any, name string) any {
	if enum, ok := s["enum"].([]any); ok && len(enum) > 0 {
		return enum[0]
	}
	switch s["type"] {
	case "object":
		out := map[string]any{}
		props, _ := s["properties"].(map[string]any)
		for k, v := range props {
			if sub, ok := v.(map[string]any); ok {
				out[k] = fillValue(sub, k)
			}
		}
		return out
	case "array":
		return []any{}
	case "number", "integer":
		return 0
	case "boolean":
		return false
	default:
		return "mock " + name
	}
}

func mockError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"message": msg, "type": "mock_error"}})
}

func ptr(s string) *string { return &s }

func derefStr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
