package ai

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/ChinmayNoob/conductor/pkg/llm"
	"github.com/ChinmayNoob/conductor/pkg/workflow"
)

// Draft is a workflow the assistant wrote from a description. It is never
// saved by the assistant: a person reads it, then saves it (or not) through
// the normal definitions API.
type Draft struct {
	YAML  string `json:"yaml"`
	Valid bool   `json:"valid"`
	// Errors is why the draft is not valid, when it is not.
	Errors string `json:"errors,omitempty"`
	Name   string `json:"name,omitempty"`
	// Plan is a dry run of the workflow engine: the steps that would run
	// together, in order. Nothing is executed.
	Plan [][]DraftStep `json:"plan,omitempty"`
	// Warnings are things to look at before approving.
	Warnings []string `json:"warnings,omitempty"`
	Usage    Usage    `json:"usage"`
}

// DraftStep is one step of a dry run.
type DraftStep struct {
	Name    string `json:"name"`
	Type    string `json:"type"`
	Summary string `json:"summary,omitempty"`
}

var fenceRe = regexp.MustCompile("(?s)```(?:ya?ml)?\\s*\n(.*?)```")

// extractYAML takes the YAML out of a model's answer, which may wrap it in a
// code fence or add a sentence around it.
func extractYAML(answer string) string {
	if m := fenceRe.FindStringSubmatch(answer); m != nil {
		return strings.TrimSpace(m[1]) + "\n"
	}
	return strings.TrimSpace(answer) + "\n"
}

// GenerateWorkflow drafts a workflow definition from a description. The draft
// is validated with the real parser; when it is invalid the model gets one
// chance to fix it, given the errors. The result says whether it is valid
// either way.
func (a *Assistant) GenerateWorkflow(ctx context.Context, description string) (*Draft, error) {
	description = strings.TrimSpace(description)
	if description == "" {
		return nil, fmt.Errorf("describe the workflow you want")
	}
	if len(description) > 4000 {
		return nil, fmt.Errorf("the description is too long (4000 characters at most)")
	}
	messages := []llm.Message{
		{Role: "system", Content: generateSystem},
		{Role: "user", Content: llm.Redact(description)},
	}
	draft := &Draft{}
	for attempt := 0; attempt < 2; attempt++ {
		resp, usage, err := a.complete(ctx, llm.Request{Messages: messages, MaxTokens: 2000})
		if err != nil {
			return nil, err
		}
		draft.Usage = draft.Usage.add(usage)
		draft.YAML = extractYAML(resp.Message.Content)

		def, err := workflow.Parse([]byte(draft.YAML))
		if err == nil {
			draft.Valid, draft.Errors = true, ""
			draft.Name = def.Name
			draft.Plan = dryRun(def)
			draft.Warnings = warnings(def)
			return draft, nil
		}
		draft.Valid, draft.Errors = false, err.Error()
		messages = append(messages,
			llm.Message{Role: "assistant", Content: resp.Message.Content},
			llm.Message{Role: "user", Content: "That definition is not valid:\n" + err.Error() +
				"\nReply with the corrected YAML only."})
	}
	return draft, nil
}

// dryRun walks the definition through the real reconciler, completing every
// started step at once, and records which steps start together.
func dryRun(def *workflow.Definition) [][]DraftStep {
	state := workflow.RunState{Status: workflow.StatusRunning, Steps: map[string]workflow.StepState{}}
	for _, s := range def.Steps {
		state.Steps[s.Name] = workflow.StepState{Status: workflow.StepPending}
	}
	var waves [][]DraftStep
	for range len(def.Steps) + 2 {
		plan := workflow.Reconcile(def, state)
		if plan.Empty() {
			break
		}
		for name, status := range plan.SetStep {
			s := state.Steps[name]
			s.Status = status
			state.Steps[name] = s
		}
		if len(plan.Start) > 0 {
			var wave []DraftStep
			for _, name := range plan.Start {
				step := DraftStep{Name: name, Type: stepType(def, name)}
				if spec, err := def.StepTask(name, workflow.Context{Inputs: placeholderInputs(def), Outputs: map[string]map[string]string{}}); err == nil {
					step.Summary = head(spec.Command, 120)
				}
				wave = append(wave, step)
				state.Steps[name] = workflow.StepState{Status: workflow.StepRunning, TaskStatus: "COMPLETED"}
			}
			waves = append(waves, wave)
		}
		if plan.SetStatus != "" {
			state.Status = plan.SetStatus
		}
	}
	return waves
}

func placeholderInputs(def *workflow.Definition) map[string]string {
	in := map[string]string{}
	for name, d := range def.Inputs {
		in[name] = "<" + name + ">"
		if d.Default != nil {
			in[name] = *d.Default
		}
	}
	return in
}

func stepType(def *workflow.Definition, name string) string {
	for _, s := range def.Steps {
		if s.Name == name {
			if s.Type != "" {
				return s.Type
			}
			switch {
			case s.HTTP != nil:
				return workflow.TypeHTTP
			case s.Container != nil:
				return workflow.TypeContainer
			case s.LLM != nil:
				return workflow.TypeLLM
			case s.Approval != nil:
				return workflow.TypeApproval
			case s.Signal != nil:
				return workflow.TypeSignal
			case s.Agent != nil:
				return workflow.TypeAgent
			}
			return workflow.TypeShell
		}
	}
	return ""
}

var riskyCommands = []struct {
	re   *regexp.Regexp
	warn string
}{
	{regexp.MustCompile(`\brm\s+-[a-zA-Z]*[rf]`), "deletes files recursively (rm -rf)"},
	{regexp.MustCompile(`\b(curl|wget)\b[^|\n]*\|\s*(sudo\s+)?(ba|z)?sh\b`), "pipes a download into a shell"},
	{regexp.MustCompile(`\bsudo\b`), "uses sudo"},
	{regexp.MustCompile(`(?i)\b(drop|truncate)\s+(table|database)\b`), "drops or truncates data"},
	{regexp.MustCompile(`\bchmod\s+(-R\s+)?7[0-7][0-7]\b|\bmkfs\b|\bdd\s+if=`), "changes permissions or devices broadly"},
	{regexp.MustCompile(`(?i)\b(password|secret|token|api[_-]?key)\s*=\s*\S+`), "contains something that looks like a hard-coded secret"},
}

// warnings lists what a reviewer should look at before saving a draft.
func warnings(def *workflow.Definition) []string {
	var out []string
	check := func(where, cmd string) {
		for _, r := range riskyCommands {
			if r.re.MatchString(cmd) {
				out = append(out, fmt.Sprintf("%s: %s", where, r.warn))
			}
		}
	}
	for _, s := range def.Steps {
		check("step "+s.Name, s.Run)
		if s.Compensate != nil {
			check("compensation of "+s.Name, s.Compensate.Run)
		}
		if s.Agent != nil {
			for _, t := range s.Agent.Tools {
				check("agent tool "+t.Name, t.Run)
			}
		}
		if s.HTTP != nil && strings.HasPrefix(strings.ToLower(s.HTTP.URL), "http://") {
			out = append(out, fmt.Sprintf("step %s: calls a URL without TLS", s.Name))
		}
	}
	return out
}
