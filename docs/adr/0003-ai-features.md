# ADR 0003: How the AI features are built

- **Status:** Accepted (Phase 5)

## Context

Phase 5 adds model calls (`llm` tasks), durable agents, human approvals and an
operations assistant. Models are slow, flaky, rate limited and priced per
token, and what they return is untrusted text. The design has to keep three
promises: a crash never repeats finished work, a model's output is never run
without a person's say-so, and nothing leaves the cluster that the operator
did not opt in to.

## Decisions

**1. A model call is an ordinary task.** An `llm` step is a task of type `llm`
that workers with a model configured pick up (label `type.llm`). It therefore
inherits retries, timeouts, queues, rate limits, cancellation, traces and the
dashboard for free. The alternative, calling models from the coordinator, would
put slow network calls on the scheduling path and give the leader a new way to
stall.

**2. Providers sit behind an interface; the first one speaks Chat Completions.**
That API is served by OpenAI and by Ollama, vLLM, LiteLLM and Azure OpenAI, so
`CONDUCTOR_LLM_BASE_URL` covers most of them. A mock server (`conductor
mock-llm`) speaks it too, scripted from the prompt, so every test and CI run is
free and deterministic.

**3. Costs come from the operator's price list only.** Conductor counts tokens
always and prices them only for models listed in `CONDUCTOR_LLM_PRICES`. A
built-in table would go stale and make cost budgets lie.

**4. Budgets are enforced where state lives.** Workflow budgets fail the run
through the reconciler (`RunState.Abort`), so it compensates like any failure.
Namespace daily limits are checked on submission (HTTP 429) and when a step
starts. Queue `tokens_per_minute` is part of the pick query. A provider's
`Retry-After` becomes the task's minimum retry delay.

**5. An agent is a state machine over tasks.** Each model call and each tool
call is its own task with an idempotency key `agent:<run>:<turn>:<call>`; the
conversation lives in an `agent_runs` row advanced under a row lock. After a
crash the coordinator re-reads the run and finds every finished call already
stored, so only the call in flight runs again. Tools are shell commands whose
arguments arrive as environment variables, never in the command line, and runs
are bounded by turns, tool calls, duration and a budget.

**6. Waiting is a state, not a task.** Approval and signal steps hold no
worker. A signal that arrives early is kept until its step starts.

**7. The assistant advises; it does not act.**
- It runs only for namespaces with `ai_assist` on, because redacted task
  output still leaves the cluster.
- Everything is redacted before it is sent (keys, tokens, JWTs, passwords,
  credentials in URLs, Conductor keys) and the model's reply is redacted again.
- Failures are classified by rules first; the model decides only what no rule
  matches, and its words (cause, fix) never override a rule's class. The class
  is shown, never used to change retries.
- A workflow drafted from English is parsed by the real validator, dry run
  through the reconciler, scanned for risky commands, and returned for a
  person to save. The assistant has no path to save it.
- "Ask your cluster" can call only read-only lookups scoped to the namespace.

**8. Prompts have evaluation sets.** A labelled set of failures holds the rule
classifier to its labels on every run, and runs against a real model when
prompts change.

## Consequences

- Model spend by the assistant itself (explanations, drafts, questions) is
  stored for explanations and returned for the rest, but is not yet counted
  against namespace budgets.
- Every API replica runs the explanation loop, so a failure may occasionally be
  explained twice; the later write wins.
- Response caching for identical prompts is not built; it would trade
  freshness for cost and is better done once real usage shows where it pays.
