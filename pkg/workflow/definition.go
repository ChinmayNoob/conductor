// Package workflow defines Conductor's workflow format: a DAG of steps written
// in YAML (or JSON), each with an optional compensating action, and the pure
// logic that decides what a workflow run should do next.
//
//	name: order
//	inputs:
//	  order_id: {required: true}
//	steps:
//	  - name: charge
//	    run: ./charge.sh "$INPUT_ORDER_ID"
//	    compensate: ./refund.sh "$OUTPUT_CHARGE_ID"
//	  - name: ship
//	    depends_on: [charge]
//	    run: ./ship.sh
//
// Values never get spliced into shell commands: inputs and step outputs reach
// commands as environment variables, so there is no injection to defend
// against.
package workflow

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	TypeShell     = "shell"
	TypeHTTP      = "http"
	TypeContainer = "container"
	TypeLLM       = "llm"
	// Waiting steps run no task: they wait for a person or a signal.
	TypeApproval = "approval"
	TypeSignal   = "signal"
)

var (
	nameRe   = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,62}$`)
	envKeyRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
)

type Definition struct {
	Name        string           `yaml:"name" json:"name"`
	Description string           `yaml:"description,omitempty" json:"description,omitempty"`
	Inputs      map[string]Input `yaml:"inputs,omitempty" json:"inputs,omitempty"`
	Defaults    Options          `yaml:"defaults,omitempty" json:"defaults,omitempty"`
	Budget      *Budget          `yaml:"budget,omitempty" json:"budget,omitempty"`
	Steps       []Step           `yaml:"steps" json:"steps"`
}

// Input declares a workflow input. When a definition declares inputs, runs
// may only pass those; without declarations any input is accepted.
type Input struct {
	Required    bool    `yaml:"required,omitempty" json:"required,omitempty"`
	Default     *string `yaml:"default,omitempty" json:"default,omitempty"`
	Description string  `yaml:"description,omitempty" json:"description,omitempty"`
}

// Options control how a step's task is scheduled.
type Options struct {
	Retries    *int              `yaml:"retries,omitempty" json:"retries,omitempty"`
	RetryDelay Duration          `yaml:"retry_delay,omitempty" json:"retry_delay,omitempty"`
	Timeout    Duration          `yaml:"timeout,omitempty" json:"timeout,omitempty"`
	Priority   int               `yaml:"priority,omitempty" json:"priority,omitempty"`
	Queue      string            `yaml:"queue,omitempty" json:"queue,omitempty"`
	Labels     map[string]string `yaml:"labels,omitempty" json:"labels,omitempty"`
}

// Action is what a step (or its compensation) runs.
type Action struct {
	Type      string            `yaml:"type,omitempty" json:"type,omitempty"`
	Run       string            `yaml:"run,omitempty" json:"run,omitempty"`
	HTTP      *HTTPSpec         `yaml:"http,omitempty" json:"http,omitempty"`
	Container *ContainerSpec    `yaml:"container,omitempty" json:"container,omitempty"`
	LLM       *LLMSpec          `yaml:"llm,omitempty" json:"llm,omitempty"`
	Approval  *ApprovalSpec     `yaml:"approval,omitempty" json:"approval,omitempty"`
	Signal    *SignalSpec       `yaml:"signal,omitempty" json:"signal,omitempty"`
	Env       map[string]string `yaml:"env,omitempty" json:"env,omitempty"`
}

// ApprovalSpec pauses the run until a person approves (the step completes)
// or rejects (it fails, and completed steps are compensated). Message may
// use ${{ }} expressions. With a timeout, OnTimeout decides: reject
// (default) or approve.
// An approved step's outputs are comment and decided_by (the API key's
// name, or "timeout").
type ApprovalSpec struct {
	Message   string   `yaml:"message,omitempty" json:"message,omitempty"`
	Timeout   Duration `yaml:"timeout,omitempty" json:"timeout,omitempty"`
	OnTimeout string   `yaml:"on_timeout,omitempty" json:"on_timeout,omitempty"`
}

// SignalSpec pauses the run until a signal of this name is sent to it; the
// signal's JSON fields become the step's outputs. A signal sent before the
// step starts is kept for it. With a timeout the step fails.
type SignalSpec struct {
	Name    string   `yaml:"name" json:"name"`
	Timeout Duration `yaml:"timeout,omitempty" json:"timeout,omitempty"`
}

// LLMSpec asks a language model for a completion. Prompt and System may use
// ${{ }} expressions (a prompt is not code). With a Schema the model must
// answer with a JSON object matching it, and its top-level fields become
// the step's outputs.
type LLMSpec struct {
	Model       string         `yaml:"model,omitempty" json:"model,omitempty"` // default CONDUCTOR_LLM_MODEL
	System      string         `yaml:"system,omitempty" json:"system,omitempty"`
	Prompt      string         `yaml:"prompt" json:"prompt"`
	Schema      map[string]any `yaml:"schema,omitempty" json:"schema,omitempty"`
	MaxTokens   int            `yaml:"max_tokens,omitempty" json:"max_tokens,omitempty"`
	Temperature *float64       `yaml:"temperature,omitempty" json:"temperature,omitempty"`
}

// Budget caps what a workflow run may spend on language models. Once a run
// goes over, it fails and compensates like any failed run.
type Budget struct {
	MaxTokens  int64   `yaml:"max_tokens,omitempty" json:"max_tokens,omitempty"`
	MaxCostUSD float64 `yaml:"max_cost_usd,omitempty" json:"max_cost_usd,omitempty"`
}

// Exceeded describes how spend breaks the budget, or returns "".
func (b *Budget) Exceeded(tokens int64, costUSD float64) string {
	switch {
	case b == nil:
		return ""
	case b.MaxTokens > 0 && tokens > b.MaxTokens:
		return fmt.Sprintf("budget exceeded: used %d model tokens of %d", tokens, b.MaxTokens)
	case b.MaxCostUSD > 0 && costUSD > b.MaxCostUSD:
		return fmt.Sprintf("budget exceeded: spent $%.4f of $%.4f on models", costUSD, b.MaxCostUSD)
	}
	return ""
}

type HTTPSpec struct {
	Method       string            `yaml:"method,omitempty" json:"method,omitempty"`
	URL          string            `yaml:"url" json:"url"`
	Headers      map[string]string `yaml:"headers,omitempty" json:"headers,omitempty"`
	Body         string            `yaml:"body,omitempty" json:"body,omitempty"`
	ExpectStatus []int             `yaml:"expect_status,omitempty" json:"expect_status,omitempty"`
}

type ContainerSpec struct {
	Image   string   `yaml:"image" json:"image"`
	Command []string `yaml:"command,omitempty" json:"command,omitempty"`
	Memory  string   `yaml:"memory,omitempty" json:"memory,omitempty"` // e.g. "256m"
	CPUs    float64  `yaml:"cpus,omitempty" json:"cpus,omitempty"`
	Network string   `yaml:"network,omitempty" json:"network,omitempty"` // "bridge" (default) or "none"
}

type Step struct {
	Name       string   `yaml:"name" json:"name"`
	DependsOn  []string `yaml:"depends_on,omitempty" json:"depends_on,omitempty"`
	Action     `yaml:",inline"`
	Compensate *Compensation `yaml:"compensate,omitempty" json:"compensate,omitempty"`
	Options    `yaml:",inline"`
}

// Compensation undoes a step. In YAML it may be a plain string, shorthand for
// a shell command.
type Compensation struct {
	Action
}

func (c *Compensation) UnmarshalYAML(n *yaml.Node) error {
	if n.Kind == yaml.ScalarNode {
		c.Run = n.Value
		return nil
	}
	return n.Decode(&c.Action)
}

func (c *Compensation) UnmarshalJSON(b []byte) error {
	var s string
	if json.Unmarshal(b, &s) == nil {
		c.Run = s
		return nil
	}
	return json.Unmarshal(b, &c.Action)
}

// Duration accepts Go duration strings ("90s", "5m") or whole seconds.
type Duration time.Duration

func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(time.Duration(d).String()) }

func (d *Duration) UnmarshalJSON(b []byte) error {
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	return d.set(v)
}

func (d *Duration) UnmarshalYAML(n *yaml.Node) error {
	var v any
	if err := n.Decode(&v); err != nil {
		return err
	}
	return d.set(v)
}

func (d Duration) MarshalYAML() (any, error) { return time.Duration(d).String(), nil }

func (d *Duration) set(v any) error {
	switch v := v.(type) {
	case string:
		p, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("invalid duration %q", v)
		}
		*d = Duration(p)
	case int:
		*d = Duration(time.Duration(v) * time.Second)
	case float64:
		*d = Duration(time.Duration(v * float64(time.Second)))
	default:
		return fmt.Errorf("invalid duration %v", v)
	}
	return nil
}

// Parse decodes and validates a definition from YAML or JSON (JSON is YAML).
// Unknown fields are rejected so typos don't silently do nothing.
func Parse(data []byte) (*Definition, error) {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	var d Definition
	if err := dec.Decode(&d); err != nil {
		return nil, fmt.Errorf("invalid workflow definition: %w", err)
	}
	if err := d.Validate(); err != nil {
		return nil, err
	}
	return &d, nil
}

// FromJSON decodes a stored definition snapshot.
func FromJSON(data []byte) (*Definition, error) {
	var d Definition
	if err := json.Unmarshal(data, &d); err != nil {
		return nil, err
	}
	return &d, nil
}

func (d *Definition) step(name string) *Step {
	for i := range d.Steps {
		if d.Steps[i].Name == name {
			return &d.Steps[i]
		}
	}
	return nil
}

// Dependents returns the steps that depend directly on name.
func (d *Definition) Dependents(name string) []string {
	var out []string
	for _, s := range d.Steps {
		if slices.Contains(s.DependsOn, name) {
			out = append(out, s.Name)
		}
	}
	return out
}

// ancestors returns every step that name depends on, directly or not.
func (d *Definition) ancestors(name string) map[string]bool {
	seen := make(map[string]bool)
	var walk func(string)
	walk = func(n string) {
		s := d.step(n)
		if s == nil {
			return
		}
		for _, dep := range s.DependsOn {
			if !seen[dep] {
				seen[dep] = true
				walk(dep)
			}
		}
	}
	walk(name)
	return seen
}

// Validate checks the definition and returns every problem found.
func (d *Definition) Validate() error {
	var errs []error
	add := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	if !nameRe.MatchString(d.Name) {
		add("name %q must be lowercase letters, digits, '-' or '_' (max 63)", d.Name)
	}
	for name := range d.Inputs {
		if !envKeyRe.MatchString(name) {
			add("input %q must be a valid identifier (letters, digits, '_')", name)
		}
	}
	if len(d.Steps) == 0 {
		add("a workflow needs at least one step")
	}
	if err := d.Defaults.validate("defaults"); err != nil {
		errs = append(errs, err)
	}

	seen := make(map[string]bool)
	for _, s := range d.Steps {
		if !nameRe.MatchString(s.Name) {
			add("step name %q must be lowercase letters, digits, '-' or '_' (max 63)", s.Name)
		}
		if seen[s.Name] {
			add("duplicate step name %q", s.Name)
		}
		seen[s.Name] = true
	}
	for _, s := range d.Steps {
		where := fmt.Sprintf("step %q", s.Name)
		for _, dep := range s.DependsOn {
			switch {
			case dep == s.Name:
				add("%s depends on itself", where)
			case !seen[dep]:
				add("%s depends on unknown step %q", where, dep)
			}
		}
		if len(slices.Compact(slices.Sorted(slices.Values(s.DependsOn)))) != len(s.DependsOn) {
			add("%s lists a dependency twice", where)
		}
	}
	if cycle := d.findCycle(); cycle != nil {
		add("steps form a cycle: %s", strings.Join(cycle, " -> "))
	}
	if len(errs) > 0 {
		// Expression checks need a valid graph.
		return joinErrors(errs)
	}

	for _, s := range d.Steps {
		where := fmt.Sprintf("step %q", s.Name)
		visible := d.ancestors(s.Name)
		if err := d.validateAction(s.Action, where, visible); err != nil {
			errs = append(errs, err)
		}
		if s.Compensate != nil {
			// A compensation runs after its own step, so it may use its outputs.
			visible[s.Name] = true
			if err := d.validateAction(s.Compensate.Action, where+" compensation", visible); err != nil {
				errs = append(errs, err)
			}
		}
		if err := s.Options.validate(where); err != nil {
			errs = append(errs, err)
		}
	}
	return joinErrors(errs)
}

func (d *Definition) validateAction(a Action, where string, visibleSteps map[string]bool) error {
	var errs []error
	add := func(format string, args ...any) { errs = append(errs, fmt.Errorf(where+": "+format, args...)) }
	checkExpr := func(field, s string) {
		for _, ref := range findRefs(s) {
			if err := d.checkRef(ref, visibleSteps); err != nil {
				add("%s: %v", field, err)
			}
		}
	}

	switch a.typ() {
	case TypeShell:
		if strings.TrimSpace(a.Run) == "" {
			add("'run' is required for shell steps")
		}
		if strings.Contains(a.Run, "${{") {
			add("'run' cannot contain ${{ }} expressions; set them in 'env' and use $VARIABLE instead, " +
				"so values can never be interpreted as shell code")
		}
		if a.HTTP != nil || a.Container != nil {
			add("shell steps cannot have 'http' or 'container'")
		}
	case TypeHTTP:
		if a.HTTP == nil || a.HTTP.URL == "" {
			add("http steps need 'http.url'")
			break
		}
		if a.Run != "" || a.Container != nil || a.LLM != nil {
			add("http steps cannot have 'run', 'container' or 'llm'")
		}
		if m := a.HTTP.Method; m != "" && !slices.Contains([]string{"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD"}, strings.ToUpper(m)) {
			add("unsupported HTTP method %q", m)
		}
		checkExpr("http.url", a.HTTP.URL)
		checkExpr("http.body", a.HTTP.Body)
		for k, v := range a.HTTP.Headers {
			checkExpr("http.headers."+k, v)
		}
	case TypeContainer:
		if a.Container == nil || a.Container.Image == "" {
			add("container steps need 'container.image'")
			break
		}
		if a.Run != "" || a.HTTP != nil || a.LLM != nil {
			add("container steps cannot have 'run', 'http' or 'llm'")
		}
		if n := a.Container.Network; n != "" && n != "bridge" && n != "none" {
			add("container.network must be 'bridge' or 'none'")
		}
		for i, arg := range a.Container.Command {
			checkExpr(fmt.Sprintf("container.command[%d]", i), arg)
		}
	case TypeLLM:
		if a.LLM == nil || strings.TrimSpace(a.LLM.Prompt) == "" {
			add("llm steps need 'llm.prompt'")
			break
		}
		if a.Run != "" || a.HTTP != nil || a.Container != nil {
			add("llm steps cannot have 'run', 'http' or 'container'")
		}
		if err := ValidateLLM(a.LLM); err != nil {
			add("%v", err)
		}
		checkExpr("llm.prompt", a.LLM.Prompt)
		checkExpr("llm.system", a.LLM.System)
	case TypeApproval:
		if a.Run != "" || a.HTTP != nil || a.Container != nil || a.LLM != nil || a.Signal != nil {
			add("approval steps run nothing: no 'run', 'http', 'container', 'llm' or 'signal'")
		}
		if a.Approval != nil {
			if o := a.Approval.OnTimeout; o != "" && o != "approve" && o != "reject" {
				add("approval.on_timeout must be approve or reject")
			}
			if a.Approval.Timeout < 0 {
				add("approval.timeout must not be negative")
			}
			checkExpr("approval.message", a.Approval.Message)
		}
	case TypeSignal:
		if a.Signal == nil || !signalNameRe.MatchString(a.Signal.Name) {
			add("signal steps need 'signal.name' (letters, digits, '-' or '_')")
			break
		}
		if a.Run != "" || a.HTTP != nil || a.Container != nil || a.LLM != nil || a.Approval != nil {
			add("signal steps run nothing: no 'run', 'http', 'container', 'llm' or 'approval'")
		}
		if a.Signal.Timeout < 0 {
			add("signal.timeout must not be negative")
		}
	default:
		add("unknown type %q (want shell, http, container, llm, approval or signal)", a.Type)
	}

	for k, v := range a.Env {
		if !envKeyRe.MatchString(k) {
			add("env name %q is not a valid environment variable name", k)
		}
		checkExpr("env."+k, v)
	}
	return joinErrors(errs)
}

func (a Action) typ() string {
	if a.Type == "" {
		return TypeShell
	}
	return a.Type
}

func (o Options) validate(where string) error {
	var errs []error
	if o.Retries != nil && *o.Retries < 0 {
		errs = append(errs, fmt.Errorf("%s: retries must not be negative", where))
	}
	if o.RetryDelay < 0 || o.Timeout < 0 {
		errs = append(errs, fmt.Errorf("%s: durations must not be negative", where))
	}
	if o.Priority < 0 || o.Priority > 10 {
		errs = append(errs, fmt.Errorf("%s: priority must be between 1 and 10", where))
	}
	if o.Queue != "" && !nameRe.MatchString(o.Queue) {
		errs = append(errs, fmt.Errorf("%s: queue %q must be lowercase letters, digits, '-' or '_'", where, o.Queue))
	}
	for k := range o.Labels {
		if k == "" {
			errs = append(errs, fmt.Errorf("%s: label names must not be empty", where))
		}
	}
	return errors.Join(errs...)
}

// findCycle returns a dependency cycle, or nil if the graph is acyclic.
func (d *Definition) findCycle() []string {
	const (
		unvisited = iota
		inProgress
		done
	)
	state := make(map[string]int)
	var stack []string
	var visit func(string) []string
	visit = func(n string) []string {
		state[n] = inProgress
		stack = append(stack, n)
		if s := d.step(n); s != nil {
			for _, dep := range s.DependsOn {
				switch state[dep] {
				case inProgress:
					i := slices.Index(stack, dep)
					return append(slices.Clone(stack[i:]), dep)
				case unvisited:
					if c := visit(dep); c != nil {
						return c
					}
				}
			}
		}
		stack = stack[:len(stack)-1]
		state[n] = done
		return nil
	}
	for _, s := range d.Steps {
		if state[s.Name] == unvisited {
			if c := visit(s.Name); c != nil {
				return c
			}
		}
	}
	return nil
}

// joinErrors joins errors one per line.
func joinErrors(errs []error) error {
	if len(errs) == 0 {
		return nil
	}
	msgs := make([]string, len(errs))
	for i, e := range errs {
		msgs[i] = e.Error()
	}
	return fmt.Errorf("invalid workflow definition:\n  - %s", strings.Join(msgs, "\n  - "))
}

// MarshalYAML writes a compensation as its action's fields, not nested.
func (c Compensation) MarshalYAML() (any, error) { return c.Action, nil }

// ValidateLLM checks an llm spec's settings (not its expressions).
func ValidateLLM(l *LLMSpec) error {
	var errs []error
	if l.Schema != nil {
		if t, _ := l.Schema["type"].(string); t != "object" {
			errs = append(errs, fmt.Errorf("llm.schema must be a JSON Schema with type: object"))
		}
	}
	if l.MaxTokens < 0 {
		errs = append(errs, fmt.Errorf("llm.max_tokens must not be negative"))
	}
	if l.Temperature != nil && (*l.Temperature < 0 || *l.Temperature > 2) {
		errs = append(errs, fmt.Errorf("llm.temperature must be between 0 and 2"))
	}
	return joinErrors(errs)
}

var signalNameRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,62}$`)

// IsWait reports whether a step type waits instead of running a task.
func IsWait(stepType string) bool { return stepType == TypeApproval || stepType == TypeSignal }
