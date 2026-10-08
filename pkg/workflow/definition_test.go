package workflow

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestParseFullDefinition(t *testing.T) {
	d, err := Parse([]byte(`
name: order-pipeline
description: demo
inputs:
  order_id: {required: true}
  amount: {default: "10"}
defaults:
  retries: 1
  timeout: 2m
steps:
  - name: charge
    run: echo charging "$INPUT_ORDER_ID"
    compensate: echo refund "$OUTPUT_CHARGE_ID"
  - name: notify
    depends_on: [charge]
    type: http
    http:
      method: post
      url: https://example.com/orders/${{ inputs.order_id }}
      body: '{"charge": "${{ steps.charge.outputs.charge_id }}"}'
    retry_delay: 3
  - name: render
    depends_on: [charge]
    type: container
    container:
      image: alpine:3.20
      command: [echo, "${{ workflow.id }}"]
    compensate:
      type: http
      http: {method: DELETE, url: "https://example.com/render"}
`))
	if err != nil {
		t.Fatal(err)
	}
	if got := d.Steps[1].RetryDelay; time.Duration(got) != 3*time.Second {
		t.Errorf("integer retry_delay = %v, want 3s", time.Duration(got))
	}
	if d.Steps[0].Compensate.Run != `echo refund "$OUTPUT_CHARGE_ID"` {
		t.Errorf("string compensation not parsed: %+v", d.Steps[0].Compensate)
	}
	if d.Steps[2].Compensate.HTTP == nil {
		t.Errorf("object compensation not parsed")
	}

	// Stored as JSON and loaded back, the definition must be identical.
	b, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	back, err := FromJSON(b)
	if err != nil {
		t.Fatal(err)
	}
	b2, _ := json.Marshal(back)
	if string(b) != string(b2) {
		t.Fatalf("JSON round trip changed the definition:\n%s\n%s", b, b2)
	}
}

func TestValidationErrors(t *testing.T) {
	cases := map[string]struct{ yaml, want string }{
		"cycle": {`
name: x
steps:
  - {name: a, run: a, depends_on: [b]}
  - {name: b, run: b, depends_on: [a]}`, "cycle"},
		"unknown dependency": {`
name: x
steps: [{name: a, run: a, depends_on: [nope]}]`, `unknown step "nope"`},
		"expression in run": {`
name: x
steps: [{name: a, run: "echo ${{ inputs.x }}"}]`, "cannot contain ${{ }}"},
		"output of a non-ancestor": {`
name: x
steps:
  - {name: a, run: a}
  - {name: b, run: b, env: {X: "${{ steps.a.outputs.k }}"}}`, "must be a dependency"},
		"undeclared input": {`
name: x
inputs: {a: {}}
steps: [{name: s, run: s, env: {X: "${{ inputs.b }}"}}]`, `input "b" is not declared`},
		"http without url": {`
name: x
steps: [{name: a, type: http, http: {method: GET}}]`, "need 'http.url'"},
		"unknown type": {`
name: x
steps: [{name: a, type: lambda, run: x}]`, "unknown type"},
		"unknown field": {`
name: x
steps: [{name: a, rnu: x}]`, "rnu"},
		"bad name": {`
name: Bad Name
steps: [{name: a, run: a}]`, "lowercase"},
		"duplicate step": {`
name: x
steps: [{name: a, run: a}, {name: a, run: b}]`, "duplicate step"},
		"no steps": {`
name: x
steps: []`, "at least one step"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Parse([]byte(c.yaml))
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("got %v, want an error containing %q", err, c.want)
			}
		})
	}
}

func TestPrepareInputs(t *testing.T) {
	d, err := Parse([]byte(`
name: x
inputs:
  id: {required: true}
  region: {default: eu}
steps: [{name: s, run: s}]`))
	if err != nil {
		t.Fatal(err)
	}

	in, err := d.PrepareInputs(json.RawMessage(`{"id": 12345678901234567890}`))
	if err != nil {
		t.Fatal(err)
	}
	if in["id"] != "12345678901234567890" || in["region"] != "eu" {
		t.Fatalf("inputs = %v", in)
	}

	for raw, want := range map[string]string{
		`{}`:                      "missing required input",
		`{"id": 1, "other": 2}`:   "unknown input",
		`[1]`:                     "JSON object",
		`{"id": 1, "bad-key": 2}`: "letters, digits",
	} {
		if _, err := d.PrepareInputs(json.RawMessage(raw)); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("input %s: got %v, want %q", raw, err, want)
		}
	}

	// Without declarations, anything goes, and structured values become JSON.
	open, _ := Parse([]byte(`{name: y, steps: [{name: s, run: s}]}`))
	in, err = open.PrepareInputs(json.RawMessage(`{"tags": ["a", "b"], "ok": true}`))
	if err != nil {
		t.Fatal(err)
	}
	if in["tags"] != `["a","b"]` || in["ok"] != "true" {
		t.Fatalf("inputs = %v", in)
	}
}

func TestStepTaskResolvesEnvNotShell(t *testing.T) {
	d, err := Parse([]byte(`
name: x
defaults: {retries: 4, queue: batch}
steps:
  - name: a
    run: echo a
  - name: b
    depends_on: [a]
    run: echo "$ID $TOKEN"
    env:
      TOKEN: "${{ steps.a.outputs.token }}"
    timeout: 30s
    labels: {gpu: "true"}
`))
	if err != nil {
		t.Fatal(err)
	}
	ctx := Context{
		WorkflowID: "wf-1",
		Inputs:     map[string]string{"id": "1; rm -rf /"},
		Outputs:    map[string]map[string]string{"a": {"token": "t0k3n"}},
	}
	spec, err := d.StepTask("b", ctx)
	if err != nil {
		t.Fatal(err)
	}
	if spec.Command != `echo "$ID $TOKEN"` {
		t.Fatalf("command was modified: %q", spec.Command)
	}
	if spec.Env["INPUT_ID"] != "1; rm -rf /" || spec.Env["TOKEN"] != "t0k3n" || spec.Env["CONDUCTOR_WORKFLOW_ID"] != "wf-1" {
		t.Fatalf("env = %v", spec.Env)
	}
	if spec.Retries != 4 || spec.Queue != "batch" || spec.Timeout != 30*time.Second || spec.Labels["gpu"] != "true" {
		t.Fatalf("options not applied: %+v", spec)
	}

	if _, err := d.StepTask("b", Context{Outputs: map[string]map[string]string{}}); err == nil {
		t.Fatal("missing output should be an error")
	}
}

func TestCompensationSeesOwnOutputs(t *testing.T) {
	d, err := Parse([]byte(`
name: x
steps:
  - name: charge
    run: echo charge
    compensate: echo refund "$OUTPUT_CHARGE_ID"
`))
	if err != nil {
		t.Fatal(err)
	}
	spec, err := d.CompensationTask("charge", Context{Outputs: map[string]map[string]string{"charge": {"charge_id": "ch_1"}}})
	if err != nil {
		t.Fatal(err)
	}
	if spec.Env["OUTPUT_CHARGE_ID"] != "ch_1" {
		t.Fatalf("env = %v", spec.Env)
	}
}

func TestLLMSteps(t *testing.T) {
	d, err := Parse([]byte(`
name: triage
budget: {max_tokens: 5000, max_cost_usd: 0.25}
steps:
  - name: fetch
    run: echo "body=$(cat ticket.txt)" >> "$CONDUCTOR_OUTPUT"
  - name: classify
    depends_on: [fetch]
    type: llm
    llm:
      model: gpt-4o-mini
      system: You triage support tickets.
      prompt: "Classify this ticket: ${{ steps.fetch.outputs.body }}"
      schema:
        type: object
        properties: {severity: {type: string, enum: [low, high]}}
      temperature: 0.2
`))
	if err != nil {
		t.Fatal(err)
	}
	if d.Budget == nil || d.Budget.MaxTokens != 5000 {
		t.Fatalf("budget = %+v", d.Budget)
	}
	spec, err := d.StepTask("classify", Context{Outputs: map[string]map[string]string{"fetch": {"body": "site is down"}}})
	if err != nil {
		t.Fatal(err)
	}
	if spec.Type != TypeLLM || spec.LLM.Prompt != "Classify this ticket: site is down" || !strings.HasPrefix(spec.Command, "llm gpt-4o-mini: Classify") {
		t.Fatalf("spec = %+v / %+v", spec, spec.LLM)
	}
	if got := d.Budget.Exceeded(4000, 0.30); !strings.Contains(got, "$0.3000 of $0.2500") {
		t.Fatalf("Exceeded = %q", got)
	}
	if got := d.Budget.Exceeded(4000, 0.10); got != "" {
		t.Fatalf("under budget, Exceeded = %q", got)
	}

	for yaml, want := range map[string]string{
		"name: x\nsteps:\n  - name: a\n    type: llm\n":                                                     "llm steps need 'llm.prompt'",
		"name: x\nsteps:\n  - name: a\n    type: llm\n    llm: {prompt: hi, schema: {type: string}}\n":      "type: object",
		"name: x\nsteps:\n  - name: a\n    type: llm\n    llm: {prompt: hi, temperature: 3}\n":              "between 0 and 2",
		"name: x\nsteps:\n  - name: a\n    type: llm\n    llm: {prompt: \"${{ steps.nope.outputs.x }}\"}\n": "nope",
	} {
		if _, err := Parse([]byte(yaml)); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("Parse(%q) = %v, want an error mentioning %q", yaml, err, want)
		}
	}
}
