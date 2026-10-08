package workflow

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

// exprRe matches ${{ ... }} expressions.
var exprRe = regexp.MustCompile(`\$\{\{\s*([^}]*?)\s*\}\}`)

// Supported references:
//
//	inputs.<name>
//	steps.<step>.outputs.<key>
//	workflow.id
func findRefs(s string) []string {
	var refs []string
	for _, m := range exprRe.FindAllStringSubmatch(s, -1) {
		refs = append(refs, m[1])
	}
	return refs
}

func (d *Definition) checkRef(ref string, visibleSteps map[string]bool) error {
	parts := strings.Split(ref, ".")
	switch {
	case len(parts) == 2 && parts[0] == "inputs":
		if d.Inputs != nil {
			if _, ok := d.Inputs[parts[1]]; !ok {
				return fmt.Errorf("${{ %s }}: input %q is not declared", ref, parts[1])
			}
		}
		return nil
	case len(parts) == 4 && parts[0] == "steps" && parts[2] == "outputs":
		if d.step(parts[1]) == nil {
			return fmt.Errorf("${{ %s }}: unknown step %q", ref, parts[1])
		}
		if !visibleSteps[parts[1]] {
			return fmt.Errorf("${{ %s }}: step %q must be a dependency (direct or indirect) to use its outputs", ref, parts[1])
		}
		return nil
	case ref == "workflow.id":
		return nil
	}
	return fmt.Errorf("${{ %s }}: unknown expression (use inputs.NAME, steps.STEP.outputs.KEY or workflow.id)", ref)
}

// Context is everything expressions can refer to while a run executes.
type Context struct {
	WorkflowID string
	Inputs     map[string]string
	Outputs    map[string]map[string]string // step -> key -> value
}

// Resolve replaces every expression in s with its value.
func (c Context) Resolve(s string) (string, error) {
	var firstErr error
	out := exprRe.ReplaceAllStringFunc(s, func(m string) string {
		ref := exprRe.FindStringSubmatch(m)[1]
		v, err := c.lookup(ref)
		if err != nil && firstErr == nil {
			firstErr = err
		}
		return v
	})
	return out, firstErr
}

func (c Context) lookup(ref string) (string, error) {
	parts := strings.Split(ref, ".")
	switch {
	case len(parts) == 2 && parts[0] == "inputs":
		v, ok := c.Inputs[parts[1]]
		if !ok {
			return "", fmt.Errorf("input %q was not provided", parts[1])
		}
		return v, nil
	case len(parts) == 4 && parts[0] == "steps" && parts[2] == "outputs":
		v, ok := c.Outputs[parts[1]][parts[3]]
		if !ok {
			return "", fmt.Errorf("step %q has no output %q", parts[1], parts[3])
		}
		return v, nil
	case ref == "workflow.id":
		return c.WorkflowID, nil
	}
	return "", fmt.Errorf("unknown expression %q", ref)
}

// PrepareInputs checks a run's input against the declared inputs, fills in
// defaults, and turns every value into a string. Objects and arrays become
// compact JSON.
func (d *Definition) PrepareInputs(raw json.RawMessage) (map[string]string, error) {
	in := make(map[string]any)
	if len(bytes.TrimSpace(raw)) > 0 && string(bytes.TrimSpace(raw)) != "null" {
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.UseNumber()
		if err := dec.Decode(&in); err != nil {
			return nil, fmt.Errorf("input must be a JSON object: %w", err)
		}
	}

	out := make(map[string]string, len(in))
	for k, v := range in {
		if !envKeyRe.MatchString(k) {
			return nil, fmt.Errorf("input name %q must be letters, digits and '_'", k)
		}
		if d.Inputs != nil {
			if _, ok := d.Inputs[k]; !ok {
				return nil, fmt.Errorf("unknown input %q (declared: %s)", k, strings.Join(slices.Sorted(maps.Keys(d.Inputs)), ", "))
			}
		}
		switch v := v.(type) {
		case string:
			out[k] = v
		case json.Number:
			out[k] = v.String()
		case bool:
			out[k] = strconv.FormatBool(v)
		case nil:
			out[k] = ""
		default:
			b, _ := json.Marshal(v)
			out[k] = string(b)
		}
	}
	for name, spec := range d.Inputs {
		if _, ok := out[name]; ok {
			continue
		}
		switch {
		case spec.Default != nil:
			out[name] = *spec.Default
		case spec.Required:
			return nil, fmt.Errorf("missing required input %q", name)
		}
	}
	return out, nil
}

// TaskSpec is a fully resolved step, ready to become a task.
type TaskSpec struct {
	Type       string
	Command    string // shell command, or a summary for other types
	HTTP       *HTTPSpec
	Container  *ContainerSpec
	LLM        *LLMSpec
	Wait       *WaitSpec  // approval and signal steps: no task
	Agent      *AgentSpec // agent steps: a run of model and tool tasks
	Env        map[string]string
	Retries    int
	RetryDelay time.Duration
	Timeout    time.Duration
	Priority   int
	Queue      string
	Labels     map[string]string
}

// StepTask resolves the task for a step.
func (d *Definition) StepTask(name string, c Context) (*TaskSpec, error) {
	s := d.step(name)
	if s == nil {
		return nil, fmt.Errorf("unknown step %q", name)
	}
	return d.resolve(s, s.Action, nil, c)
}

// CompensationTask resolves the task that undoes a step, or returns nil if
// the step has no compensation. The step's own outputs are available as
// OUTPUT_<KEY> environment variables.
func (d *Definition) CompensationTask(name string, c Context) (*TaskSpec, error) {
	s := d.step(name)
	if s == nil {
		return nil, fmt.Errorf("unknown step %q", name)
	}
	if s.Compensate == nil {
		return nil, nil
	}
	return d.resolve(s, s.Compensate.Action, c.Outputs[name], c)
}

// HasCompensation reports whether a step has something to undo.
func (d *Definition) HasCompensation(name string) bool {
	s := d.step(name)
	return s != nil && s.Compensate != nil
}

func (d *Definition) resolve(s *Step, a Action, ownOutputs map[string]string, c Context) (*TaskSpec, error) {
	env := map[string]string{
		"CONDUCTOR_WORKFLOW_ID": c.WorkflowID,
		"CONDUCTOR_STEP":        s.Name,
	}
	for k, v := range c.Inputs {
		env["INPUT_"+strings.ToUpper(k)] = v
	}
	for k, v := range ownOutputs {
		if envKeyRe.MatchString(k) {
			env["OUTPUT_"+strings.ToUpper(k)] = v
		}
	}
	for k, v := range a.Env {
		r, err := c.Resolve(v)
		if err != nil {
			return nil, fmt.Errorf("env %s: %w", k, err)
		}
		env[k] = r
	}

	spec := &TaskSpec{
		Type:       a.typ(),
		Env:        env,
		Retries:    2,
		RetryDelay: 5 * time.Second,
		Timeout:    5 * time.Minute,
		Priority:   5,
		Queue:      "default",
	}
	// Workflow defaults, then step options, override the built-in defaults.
	for _, o := range []Options{d.Defaults, s.Options} {
		if o.Retries != nil {
			spec.Retries = *o.Retries
		}
		if o.RetryDelay > 0 {
			spec.RetryDelay = time.Duration(o.RetryDelay)
		}
		if o.Timeout > 0 {
			spec.Timeout = time.Duration(o.Timeout)
		}
		if o.Priority > 0 {
			spec.Priority = o.Priority
		}
		if o.Queue != "" {
			spec.Queue = o.Queue
		}
		if len(o.Labels) > 0 {
			spec.Labels = maps.Clone(o.Labels)
		}
	}

	var err error
	switch spec.Type {
	case TypeShell:
		spec.Command = a.Run
	case TypeHTTP:
		h := *a.HTTP
		if h.URL, err = c.Resolve(h.URL); err != nil {
			return nil, fmt.Errorf("http.url: %w", err)
		}
		if h.Body, err = c.Resolve(h.Body); err != nil {
			return nil, fmt.Errorf("http.body: %w", err)
		}
		h.Headers = maps.Clone(h.Headers)
		for k, v := range h.Headers {
			if h.Headers[k], err = c.Resolve(v); err != nil {
				return nil, fmt.Errorf("http.headers.%s: %w", k, err)
			}
		}
		if h.Method == "" {
			h.Method = "GET"
		}
		h.Method = strings.ToUpper(h.Method)
		spec.HTTP = &h
		spec.Command = h.Method + " " + h.URL
	case TypeContainer:
		ct := *a.Container
		ct.Command = slices.Clone(ct.Command)
		for i, arg := range ct.Command {
			if ct.Command[i], err = c.Resolve(arg); err != nil {
				return nil, fmt.Errorf("container.command[%d]: %w", i, err)
			}
		}
		spec.Container = &ct
		spec.Command = strings.TrimSpace(ct.Image + " " + strings.Join(ct.Command, " "))
	case TypeLLM:
		l := *a.LLM
		if l.Prompt, err = c.Resolve(l.Prompt); err != nil {
			return nil, fmt.Errorf("llm.prompt: %w", err)
		}
		if l.System, err = c.Resolve(l.System); err != nil {
			return nil, fmt.Errorf("llm.system: %w", err)
		}
		spec.LLM = &l
		spec.Command = LLMSummary(&l)
	case TypeApproval:
		w := &WaitSpec{Kind: TypeApproval, OnTimeout: "reject", Message: "Approve step " + s.Name + "?"}
		if a.Approval != nil {
			if a.Approval.Message != "" {
				if w.Message, err = c.Resolve(a.Approval.Message); err != nil {
					return nil, fmt.Errorf("approval.message: %w", err)
				}
			}
			w.Timeout = time.Duration(a.Approval.Timeout)
			if a.Approval.OnTimeout != "" {
				w.OnTimeout = a.Approval.OnTimeout
			}
		}
		spec.Wait = w
		spec.Command = "approval: " + w.Message
	case TypeSignal:
		spec.Wait = &WaitSpec{Kind: TypeSignal, Signal: a.Signal.Name, Timeout: time.Duration(a.Signal.Timeout), OnTimeout: "fail"}
		spec.Command = "signal: " + a.Signal.Name
	case TypeAgent:
		ag := *a.Agent
		if ag.Prompt, err = c.Resolve(ag.Prompt); err != nil {
			return nil, fmt.Errorf("agent.prompt: %w", err)
		}
		if ag.System, err = c.Resolve(ag.System); err != nil {
			return nil, fmt.Errorf("agent.system: %w", err)
		}
		if ag.MaxTurns == 0 {
			ag.MaxTurns = DefaultAgentTurns
		}
		if ag.MaxToolCalls == 0 {
			ag.MaxToolCalls = DefaultAgentToolCalls
		}
		if ag.MaxDuration == 0 {
			ag.MaxDuration = Duration(DefaultAgentDuration)
		}
		spec.Agent = &ag
		spec.Command = "agent: " + LLMSummary(&LLMSpec{Model: ag.Model, Prompt: ag.Prompt})[len("llm "):]
	}
	return spec, nil
}

// LLMSummary is a one-line description of an llm task, for lists.
func LLMSummary(l *LLMSpec) string {
	p := strings.Join(strings.Fields(l.Prompt), " ")
	if len(p) > 80 {
		p = p[:80] + "…"
	}
	model := l.Model
	if model == "" {
		model = "default model"
	}
	return "llm " + model + ": " + p
}

// WaitSpec is what a waiting step waits for.
type WaitSpec struct {
	Kind      string // approval or signal
	Message   string // approval: what a person is asked
	Signal    string // signal: its name
	Timeout   time.Duration
	OnTimeout string // approval: approve or reject; signal: fail
}
