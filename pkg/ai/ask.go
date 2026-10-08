package ai

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/ChinmayNoob/conductor/pkg/db"
	"github.com/ChinmayNoob/conductor/pkg/llm"
	"github.com/google/uuid"
)

// maxAskTurns bounds how many times the model may look things up.
const maxAskTurns = 6

// Answer is the reply to a question about the cluster.
type Answer struct {
	Answer string `json:"answer"`
	// Lookups lists the read-only queries the model ran to answer.
	Lookups []string `json:"lookups,omitempty"`
	Usage   Usage    `json:"usage"`
}

var askTools = []llm.Tool{
	{
		Name:        "task_counts",
		Description: "Number of tasks in each status (QUEUED, STARTED, COMPLETED, FAILED, CANCELLED).",
		Parameters:  json.RawMessage(`{"type":"object","properties":{}}`),
	},
	{
		Name:        "list_tasks",
		Description: "The most recent tasks, newest first, optionally only one status or queue.",
		Parameters: json.RawMessage(`{"type":"object","properties":{
			"status":{"type":"string","enum":["QUEUED","STARTED","COMPLETED","FAILED","CANCELLED"]},
			"queue":{"type":"string"},
			"search":{"type":"string","description":"part of the command or a task ID prefix"},
			"limit":{"type":"integer","description":"1 to 25, default 10"}}}`),
	},
	{
		Name:        "get_task",
		Description: "One task in detail: command, output, error, attempts and the assistant's explanation if there is one.",
		Parameters:  json.RawMessage(`{"type":"object","properties":{"id":{"type":"string"}},"required":["id"]}`),
	},
	{
		Name:        "list_workflows",
		Description: "The most recent workflow runs, newest first, optionally only one status or definition name.",
		Parameters: json.RawMessage(`{"type":"object","properties":{
			"status":{"type":"string","enum":["RUNNING","COMPENSATING","COMPLETED","FAILED","CANCELLED"]},
			"name":{"type":"string"},
			"limit":{"type":"integer","description":"1 to 25, default 10"}}}`),
	},
	{
		Name:        "list_workers",
		Description: "The workers seen recently, with their slots, load and status.",
		Parameters:  json.RawMessage(`{"type":"object","properties":{}}`),
	},
	{
		Name:        "llm_spend",
		Description: "Model tokens and cost spent in this namespace over the last N hours (default 24).",
		Parameters:  json.RawMessage(`{"type":"object","properties":{"hours":{"type":"integer"}}}`),
	},
}

// Ask answers a question about a namespace's tasks, workflows and workers.
// The model can only call the read-only lookups above, scoped to the
// namespace; what they return is redacted and trimmed first.
func (a *Assistant) Ask(ctx context.Context, namespace, question string) (*Answer, error) {
	question = strings.TrimSpace(question)
	if question == "" {
		return nil, fmt.Errorf("ask a question")
	}
	if len(question) > 2000 {
		return nil, fmt.Errorf("the question is too long (2000 characters at most)")
	}
	messages := []llm.Message{
		{Role: "system", Content: askSystem + "\nThe time now is " + time.Now().UTC().Format(time.RFC3339) + "."},
		{Role: "user", Content: llm.Redact(question)},
	}
	out := &Answer{}
	for range maxAskTurns {
		resp, usage, err := a.complete(ctx, llm.Request{Messages: messages, Tools: askTools, MaxTokens: 1200})
		if err != nil {
			return nil, err
		}
		out.Usage = out.Usage.add(usage)
		if len(resp.Message.ToolCalls) == 0 {
			out.Answer = llm.Redact(strings.TrimSpace(resp.Message.Content))
			return out, nil
		}
		messages = append(messages, resp.Message)
		for _, call := range resp.Message.ToolCalls {
			out.Lookups = append(out.Lookups, call.Name+" "+head(call.Arguments, 120))
			result := a.lookup(ctx, namespace, call)
			messages = append(messages, llm.Message{Role: "tool", ToolCallID: call.ID, Content: result})
		}
	}
	// Out of lookups: ask for the best answer so far, without tools.
	messages = append(messages, llm.Message{Role: "user", Content: "Answer now with what you have found."})
	resp, usage, err := a.complete(ctx, llm.Request{Messages: messages, MaxTokens: 1200})
	if err != nil {
		return nil, err
	}
	out.Usage = out.Usage.add(usage)
	out.Answer = llm.Redact(strings.TrimSpace(resp.Message.Content))
	return out, nil
}

// lookup runs one of the model's read-only queries and returns its result as
// text for the model. Errors go back to the model too, so it can adjust.
func (a *Assistant) lookup(ctx context.Context, namespace string, call llm.ToolCall) string {
	var args struct {
		Status string `json:"status"`
		Queue  string `json:"queue"`
		Search string `json:"search"`
		Name   string `json:"name"`
		ID     string `json:"id"`
		Limit  int    `json:"limit"`
		Hours  int    `json:"hours"`
	}
	if call.Arguments != "" {
		if err := json.Unmarshal([]byte(call.Arguments), &args); err != nil {
			return "error: the arguments are not valid JSON: " + err.Error()
		}
	}
	limit := min(max(args.Limit, 1), 25)
	if args.Limit == 0 {
		limit = 10
	}

	var result any
	var err error
	switch call.Name {
	case "task_counts":
		result, err = a.DB.TaskCounts(ctx, namespace)
	case "list_tasks":
		st := db.TaskStatus(args.Status)
		if st != "" && !st.Valid() {
			return "error: unknown status " + args.Status
		}
		var tasks []*db.Task
		tasks, err = a.DB.ListTasks(ctx, db.TaskFilter{Namespace: namespace, Status: st, Queue: args.Queue, Search: args.Search, Limit: limit})
		rows := make([]map[string]any, 0, len(tasks))
		for _, t := range tasks {
			rows = append(rows, taskRow(t))
		}
		result = rows
	case "get_task":
		id, perr := uuid.Parse(args.ID)
		if perr != nil {
			return "error: id is not a task ID"
		}
		var t *db.Task
		if t, err = a.DB.GetTask(ctx, id); err == nil {
			if t == nil || t.Namespace != namespace {
				return "error: no such task"
			}
			row := taskRow(t)
			row["output"] = tail(llm.Redact(t.Output), 1500)
			row["command"] = head(llm.Redact(t.Data), 600)
			if attempts, aerr := a.DB.ListAttempts(ctx, id); aerr == nil {
				var list []string
				for _, at := range attempts {
					list = append(list, fmt.Sprintf("attempt %d %s: %s", at.Attempt, at.Status, head(llm.Redact(at.ErrorMessage), 200)))
				}
				row["earlier_attempts"] = list
			}
			if e, eerr := a.DB.GetExplanation(ctx, id); eerr == nil && e != nil {
				row["explanation"] = map[string]any{"class": e.Class, "cause": e.Cause, "fix": e.Fix}
			}
			result = row
		}
	case "list_workflows":
		var wfs []*db.Workflow
		wfs, err = a.DB.ListWorkflows(ctx, db.WorkflowFilter{Namespace: namespace, Status: db.WorkflowStatus(args.Status), Name: args.Name, Limit: limit})
		rows := make([]map[string]any, 0, len(wfs))
		for _, w := range wfs {
			rows = append(rows, map[string]any{
				"id": w.ID, "name": w.Type, "status": w.Status, "created_at": w.CreatedAt.Format(time.RFC3339),
				"error": head(llm.Redact(w.ErrorMessage), 300),
			})
		}
		result = rows
	case "list_workers":
		var workers []*db.WorkerRecord
		workers, err = a.DB.ListWorkers(ctx, 2*time.Minute)
		rows := make([]map[string]any, 0, len(workers))
		for _, w := range workers {
			rows = append(rows, map[string]any{"id": w.ID, "status": w.Status, "slots": w.Slots, "running": w.Running, "labels": w.Labels})
		}
		result = rows
	case "llm_spend":
		hours := args.Hours
		if hours <= 0 || hours > 24*30 {
			hours = 24
		}
		var s db.Spend
		if s, err = a.DB.NamespaceSpend(ctx, namespace, time.Now().UTC().Add(-time.Duration(hours)*time.Hour)); err == nil {
			result = map[string]any{"hours": hours, "tokens": s.Tokens, "cost_usd": s.CostUSD}
		}
	default:
		return "error: unknown tool " + call.Name
	}
	if err != nil {
		return "error: the lookup failed"
	}
	b, _ := json.Marshal(result)
	return head(string(b), 6000)
}

func taskRow(t *db.Task) map[string]any {
	row := map[string]any{
		"id": t.ID, "type": t.Type, "queue": t.Queue, "status": t.Status,
		"attempt": t.Attempt, "retries_used": t.RetryCount, "created_at": t.CreatedAt.Format(time.RFC3339),
		"command": head(llm.Redact(t.Data), 100),
	}
	if t.ErrorMessage != "" {
		row["error"] = head(llm.Redact(t.ErrorMessage), 300)
	}
	if t.WorkflowID != nil {
		row["workflow_id"] = t.WorkflowID
	}
	return row
}
