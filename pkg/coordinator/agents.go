package coordinator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strings"
	"time"

	"github.com/ChinmayNoob/conductor/pkg/db"
	"github.com/ChinmayNoob/conductor/pkg/llm"
	"github.com/ChinmayNoob/conductor/pkg/task"
	"github.com/ChinmayNoob/conductor/pkg/workflow"
	"github.com/google/uuid"
)

// Durable agents.
//
// An agent step's run lives in agent_runs: its conversation, turn and
// phase. Each turn is one model call (an llm task) and then the tool calls
// the model asked for (one shell task each). advanceAgent moves a run along
// whenever one of its tasks finishes; it is idempotent and runs under the
// run's row lock, so the sweep can call it after a failover too.
//
// Nothing is ever repeated: a task's idempotency key is its place in the run
// (agent:<run>:<turn>:<call>), so re-creating one returns the existing
// task, and a finished call's result is read from its task. Only a call
// that was in flight when its worker died runs again, as its task's retry.

// agentState is what a run needs beyond its conversation: the resolved spec
// and how its tasks are scheduled.
type agentState struct {
	Agent      workflow.AgentSpec `json:"agent"`
	Env        map[string]string  `json:"env,omitempty"`
	Queue      string             `json:"queue"`
	Priority   int                `json:"priority"`
	Retries    int                `json:"retries"`
	RetryDelay time.Duration      `json:"retry_delay"`
	Timeout    time.Duration      `json:"timeout"` // per model call
	Labels     map[string]string  `json:"labels,omitempty"`
}

const (
	toolOutputLimit = 16 << 10 // what the model sees of a tool's output
	llmRole         = "llm"
)

// startAgent creates an agent step's run and its first model call.
func (s *Server) startAgent(ctx context.Context, tx *db.DB, wf *db.Workflow, step *db.StepState, spec *workflow.TaskSpec) error {
	st := agentState{
		Agent: *spec.Agent, Env: spec.Env, Queue: spec.Queue, Priority: spec.Priority, Retries: spec.Retries,
		RetryDelay: spec.RetryDelay, Timeout: spec.Timeout, Labels: spec.Labels,
	}
	var msgs []llm.Message
	if st.Agent.System != "" {
		msgs = append(msgs, llm.Message{Role: "system", Content: st.Agent.System})
	}
	msgs = append(msgs, llm.Message{Role: "user", Content: st.Agent.Prompt})
	rawState, err := json.Marshal(st)
	if err != nil {
		return err
	}
	rawMsgs, err := json.Marshal(msgs)
	if err != nil {
		return err
	}
	deadline := time.Now().Add(time.Duration(st.Agent.MaxDuration))
	run, err := tx.CreateAgentRun(ctx, db.AgentRun{
		Namespace: wf.Namespace, WorkflowID: wf.ID, StepID: step.ID, Spec: rawState, Messages: rawMsgs, Deadline: &deadline,
	})
	if err != nil {
		return err
	}
	return s.agentModelCall(ctx, tx, wf, run, &st, msgs)
}

// agentModelCall creates the model call for a run's current turn.
func (s *Server) agentModelCall(ctx context.Context, tx *db.DB, wf *db.Workflow, run *db.AgentRun, st *agentState, msgs []llm.Message) error {
	tools := make([]llm.Tool, 0, len(st.Agent.Tools))
	for _, t := range st.Agent.Tools {
		params, err := json.Marshal(t.Parameters)
		if err != nil || t.Parameters == nil {
			params = nil
		}
		tools = append(tools, llm.Tool{Name: t.Name, Description: t.Description, Parameters: params})
	}
	spec, err := json.Marshal(workflow.LLMSpec{
		Model: st.Agent.Model, MaxTokens: st.Agent.MaxTokens, Temperature: st.Agent.Temperature,
		Messages: msgs, Tools: tools,
	})
	if err != nil {
		return err
	}
	n := db.NewTask{
		Namespace: run.Namespace, Queue: st.Queue, Type: workflow.TypeLLM,
		Data: fmt.Sprintf("agent turn %d: %s", run.Turn, summarize(st.Agent.Prompt)),
		Spec: spec, Requirements: task.Requirements(workflow.TypeLLM, st.Labels),
		Priority: st.Priority, MaxRetries: st.Retries,
		RetryDelaySeconds: max(1, int(math.Ceil(st.RetryDelay.Seconds()))),
		TimeoutSeconds:    max(1, int(math.Ceil(st.Timeout.Seconds()))),
		WorkflowID:        &wf.ID, TraceParent: wf.TraceParent,
		AgentRunID: &run.ID, AgentTurn: run.Turn, AgentRole: db.AgentRoleLLM,
		IdempotencyKey: fmt.Sprintf("agent:%s:%d:%s", run.ID, run.Turn, llmRole),
	}
	_, _, err = tx.CreateTask(ctx, n)
	return err
}

// argEnvRe accepts argument names that make safe environment variable names.
var argEnvRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,63}$`)

// agentToolCall creates the task for one tool call. The model's arguments
// reach the command only through the environment.
func agentToolCall(ctx context.Context, tx *db.DB, wf *db.Workflow, run *db.AgentRun, st *agentState, tool *workflow.AgentTool, call llm.ToolCall) error {
	env := make(map[string]string, len(st.Env)+8)
	for k, v := range st.Env {
		env[k] = v
	}
	args := call.Arguments
	if strings.TrimSpace(args) == "" {
		args = "{}"
	}
	env["TOOL_NAME"], env["TOOL_CALL_ID"], env["TOOL_ARGS"] = tool.Name, call.ID, args
	var fields map[string]any
	if json.Unmarshal([]byte(args), &fields) == nil {
		for k, v := range fields {
			if !argEnvRe.MatchString(k) {
				continue
			}
			if str, ok := v.(string); ok {
				env["ARG_"+strings.ToUpper(k)] = str
			} else if b, err := json.Marshal(v); err == nil {
				env["ARG_"+strings.ToUpper(k)] = string(b)
			}
		}
	}
	timeout := time.Duration(tool.Timeout)
	if timeout <= 0 {
		timeout = workflow.DefaultToolTimeout
	}
	n := db.NewTask{
		Namespace: run.Namespace, Queue: st.Queue, Type: workflow.TypeShell, Data: tool.Run, Env: env,
		Requirements: task.Requirements(workflow.TypeShell, st.Labels),
		Priority:     st.Priority, MaxRetries: st.Retries,
		RetryDelaySeconds: max(1, int(math.Ceil(st.RetryDelay.Seconds()))),
		TimeoutSeconds:    max(1, int(math.Ceil(timeout.Seconds()))),
		WorkflowID:        &wf.ID, TraceParent: wf.TraceParent,
		AgentRunID: &run.ID, AgentTurn: run.Turn, AgentRole: db.AgentRoleTool, ToolCallID: call.ID,
		IdempotencyKey: fmt.Sprintf("agent:%s:%d:%s", run.ID, run.Turn, call.ID),
	}
	_, _, err := tx.CreateTask(ctx, n)
	return err
}

// advanceAgent moves a run along as far as its finished tasks allow.
func (s *Server) advanceAgent(ctx context.Context, runID uuid.UUID) {
	log := s.log.With("agent_run_id", runID)
	var finished *uuid.UUID // the workflow to reconcile when the run ends
	var toKill []uuid.UUID
	created := false

	err := s.fenced(ctx, func(tx *db.DB) error {
		finished, toKill, created = nil, nil, false
		run, err := tx.LockAgentRun(ctx, runID)
		if err != nil || run == nil || run.Status != "RUNNING" {
			return err
		}
		var st agentState
		if err := json.Unmarshal(run.Spec, &st); err != nil {
			return fmt.Errorf("invalid agent state: %w", err)
		}
		var msgs []llm.Message
		if err := json.Unmarshal(run.Messages, &msgs); err != nil {
			return fmt.Errorf("invalid agent conversation: %w", err)
		}
		wf, err := tx.GetWorkflow(ctx, run.WorkflowID)
		if err != nil || wf == nil {
			return err
		}
		tasks, err := tx.AgentTasks(ctx, run.ID, run.Turn)
		if err != nil {
			return err
		}
		save := func() error {
			raw, err := json.Marshal(msgs)
			if err != nil {
				return err
			}
			run.Messages = raw
			return tx.SaveAgentRun(ctx, run)
		}
		end := func(status, answer, reason string) error {
			run.Status, run.Answer, run.Error = status, answer, reason
			if status != "COMPLETED" {
				// Stop whatever of this turn still runs.
				for _, t := range tasks {
					if t.Status.Terminal() {
						continue
					}
					before, err := tx.CancelTask(ctx, t.ID)
					if err != nil {
						return err
					}
					if before != nil && before.PickedAt != nil {
						toKill = append(toKill, t.ID)
					}
				}
			}
			finished = &run.WorkflowID
			log.Info("Agent run finished", "status", status, "turns", run.Turn, "tool_calls", run.ToolCalls, "reason", reason)
			return save()
		}

		// Limits, checked whenever the run moves.
		if run.Deadline != nil && time.Now().After(*run.Deadline) {
			return end("FAILED", "", fmt.Sprintf("the agent ran longer than its max_duration (%v)", time.Duration(st.Agent.MaxDuration)))
		}
		if st.Agent.Budget != nil {
			spend, err := tx.AgentRunSpend(ctx, run.ID)
			if err != nil {
				return err
			}
			if reason := st.Agent.Budget.Exceeded(spend.Tokens, spend.CostUSD); reason != "" {
				return end("FAILED", "", "agent "+reason)
			}
		}

		switch run.Phase {
		case db.AgentThink:
			var call *db.Task
			for _, t := range tasks {
				if t.AgentRole == db.AgentRoleLLM {
					call = t
				}
			}
			if call == nil { // e.g. creating it failed last time
				created = true
				return s.agentModelCall(ctx, tx, wf, run, &st, msgs)
			}
			switch call.Status {
			case db.StatusFailed:
				return end("FAILED", "", "the model call failed: "+call.ErrorMessage)
			case db.StatusCancelled:
				return end("FAILED", "", "the model call was cancelled")
			case db.StatusCompleted:
			default:
				return nil // still thinking
			}
			var reply llm.Message
			if err := json.Unmarshal([]byte(call.Outputs["message"]), &reply); err != nil {
				return end("FAILED", "", "the model's reply could not be read")
			}
			reply.Role = "assistant"
			msgs = append(msgs, reply)
			if len(reply.ToolCalls) == 0 {
				return end("COMPLETED", reply.Content, "")
			}
			if run.ToolCalls+len(reply.ToolCalls) > st.Agent.MaxToolCalls {
				return end("FAILED", "", fmt.Sprintf("the agent would exceed max_tool_calls (%d)", st.Agent.MaxToolCalls))
			}
			for _, c := range reply.ToolCalls {
				if tool := findTool(&st, c.Name); tool != nil {
					if err := agentToolCall(ctx, tx, wf, run, &st, tool, c); err != nil {
						return err
					}
					created = true
				}
			}
			run.ToolCalls += len(reply.ToolCalls)
			run.Phase = db.AgentAct
			log.Info("Agent calling tools", "turn", run.Turn, "calls", len(reply.ToolCalls))
			return save()

		case db.AgentAct:
			if len(msgs) == 0 || msgs[len(msgs)-1].Role != "assistant" {
				return end("FAILED", "", "the agent's conversation is out of order")
			}
			calls := msgs[len(msgs)-1].ToolCalls
			byCall := make(map[string]*db.Task, len(tasks))
			for _, t := range tasks {
				if t.AgentRole == db.AgentRoleTool {
					byCall[t.ToolCallID] = t
				}
			}
			for _, c := range calls {
				tool := findTool(&st, c.Name)
				if tool == nil {
					continue
				}
				t := byCall[c.ID]
				if t == nil { // recreate (idempotent) after a crash mid-turn
					created = true
					if err := agentToolCall(ctx, tx, wf, run, &st, tool, c); err != nil {
						return err
					}
					return nil
				}
				if !t.Status.Terminal() {
					return nil // still acting
				}
			}
			for _, c := range calls {
				msgs = append(msgs, llm.Message{Role: "tool", ToolCallID: c.ID, Content: toolResult(&st, c, byCall[c.ID])})
			}
			if run.Turn >= st.Agent.MaxTurns {
				return end("FAILED", "", fmt.Sprintf("the agent did not answer within max_turns (%d)", st.Agent.MaxTurns))
			}
			run.Turn++
			run.Phase = db.AgentThink
			if err := save(); err != nil {
				return err
			}
			created = true
			return s.agentModelCall(ctx, tx, wf, run, &st, msgs)
		}
		return nil
	})
	if err != nil {
		if !errors.Is(err, errNotLeader) && ctx.Err() == nil {
			log.Error("Failed to advance agent run", "error", err)
		}
		return
	}
	for _, id := range toKill {
		s.killOnWorker(ctx, id)
	}
	if created {
		s.wakeDispatcher()
	}
	if finished != nil {
		s.reconcile(ctx, *finished)
	}
}

// cancelAgent stops a run its workflow no longer needs. It returns the
// tasks to kill on their workers.
func cancelAgent(ctx context.Context, tx *db.DB, runID uuid.UUID) ([]uuid.UUID, error) {
	run, err := tx.LockAgentRun(ctx, runID)
	if err != nil || run == nil || run.Status != "RUNNING" {
		return nil, err
	}
	tasks, err := tx.AgentTasks(ctx, run.ID, run.Turn)
	if err != nil {
		return nil, err
	}
	var kill []uuid.UUID
	for _, t := range tasks {
		if t.Status.Terminal() {
			continue
		}
		before, err := tx.CancelTask(ctx, t.ID)
		if err != nil {
			return nil, err
		}
		if before != nil && before.PickedAt != nil {
			kill = append(kill, t.ID)
		}
	}
	run.Status, run.Error = "CANCELLED", "cancelled"
	return kill, tx.SaveAgentRun(ctx, run)
}

// toolResult is what the model is told about one tool call. Tool output is
// redacted: it is sent to the model provider.
func toolResult(st *agentState, call llm.ToolCall, t *db.Task) string {
	if findTool(st, call.Name) == nil {
		return fmt.Sprintf("error: there is no tool named %q", call.Name)
	}
	out := llm.Redact(t.Output)
	if len(out) > toolOutputLimit {
		out = out[:toolOutputLimit] + "\n[output truncated]"
	}
	switch t.Status {
	case db.StatusCompleted:
		return out
	case db.StatusCancelled:
		return "error: the tool call was cancelled"
	default:
		return "error: " + llm.Redact(t.ErrorMessage) + "\n" + out
	}
}

func findTool(st *agentState, name string) *workflow.AgentTool {
	for i := range st.Agent.Tools {
		if st.Agent.Tools[i].Name == name {
			return &st.Agent.Tools[i]
		}
	}
	return nil
}

func summarize(s string) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > 60 {
		return s[:60] + "…"
	}
	return s
}
