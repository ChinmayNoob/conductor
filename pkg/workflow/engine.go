package workflow

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

type StepDefinition struct {
	Name               string
	CommandTemplate    string
	CompensateTemplate string
}

type WorkflowDefinition struct {
	Type  string
	Steps []StepDefinition
}

type Registry struct {
	mu          sync.RWMutex
	definitions map[string]*WorkflowDefinition
}

func NewRegistry() *Registry {
	r := &Registry{
		definitions: make(map[string]*WorkflowDefinition),
	}
	r.registerBuiltins()
	return r
}

func (r *Registry) Register(def *WorkflowDefinition) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.definitions[def.Type] = def
}

func (r *Registry) Get(wfType string) (*WorkflowDefinition, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	def, ok := r.definitions[wfType]
	if !ok {
		return nil, fmt.Errorf("unknown workflow type: %s", wfType)
	}
	return def, nil
}

func (r *Registry) ListTypes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	types := make([]string, 0, len(r.definitions))
	for t := range r.definitions {
		types = append(types, t)
	}
	return types
}

// safeValue matches input values that can be pasted into a shell command
// as-is: no whitespace, quotes or shell metacharacters, and no leading "-" that
// could be read as a command-line flag.
var safeValue = regexp.MustCompile(`^([A-Za-z0-9_.,:@+][A-Za-z0-9_.,:@+-]*)?$`)

// ParseInput decodes workflow input, which must be a JSON object of strings,
// numbers or booleans, and rejects any value that is unsafe to substitute into
// a shell command.
func ParseInput(input json.RawMessage) (map[string]string, error) {
	dec := json.NewDecoder(bytes.NewReader(input))
	dec.UseNumber()
	var raw map[string]any
	if err := dec.Decode(&raw); err != nil {
		return nil, fmt.Errorf("input must be a JSON object: %w", err)
	}

	values := make(map[string]string, len(raw))
	for k, v := range raw {
		var s string
		switch v := v.(type) {
		case string:
			s = v
		case json.Number:
			s = v.String()
		case bool:
			s = strconv.FormatBool(v)
		default:
			return nil, fmt.Errorf("input %q must be a string, number or boolean", k)
		}
		if !safeValue.MatchString(s) {
			return nil, fmt.Errorf("input %q has characters that are not allowed in commands: %q", k, s)
		}
		values[k] = s
	}
	return values, nil
}

// ExpandCommand replaces {{key}} placeholders with values from the context JSON.
func ExpandCommand(template string, ctx json.RawMessage) (string, error) {
	values, err := ParseInput(ctx)
	if err != nil {
		return "", err
	}
	result := template
	for k, v := range values {
		placeholder := fmt.Sprintf("{{%s}}", k)
		result = strings.ReplaceAll(result, placeholder, v)
	}
	return result, nil
}

func (r *Registry) registerBuiltins() {
	// Happy path: all steps succeed
	r.Register(&WorkflowDefinition{
		Type: "trip_booking",
		Steps: []StepDefinition{
			{
				Name:               "Book Flight",
				CommandTemplate:    "echo booking flight for user={{user_id}}",
				CompensateTemplate: "echo cancelling flight for user={{user_id}}",
			},
			{
				Name:               "Book Hotel",
				CommandTemplate:    "echo booking hotel for user={{user_id}}",
				CompensateTemplate: "echo cancelling hotel for user={{user_id}}",
			},
			{
				Name:               "Charge Payment",
				CommandTemplate:    "echo charging amount={{amount}} for user={{user_id}}",
				CompensateTemplate: "echo refunding amount={{amount}} for user={{user_id}}",
			},
		},
	})

	// Slow path: the hotel step takes a while, leaving time to cancel the
	// workflow and watch the flight booking get compensated.
	r.Register(&WorkflowDefinition{
		Type: "trip_booking_slow",
		Steps: []StepDefinition{
			{
				Name:               "Book Flight",
				CommandTemplate:    "echo booking flight for user={{user_id}}",
				CompensateTemplate: "echo cancelling flight for user={{user_id}}",
			},
			{
				Name:               "Book Hotel",
				CommandTemplate:    "echo waiting for hotel for user={{user_id}} && sleep 30",
				CompensateTemplate: "echo cancelling hotel for user={{user_id}}",
			},
			{
				Name:               "Charge Payment",
				CommandTemplate:    "echo charging amount={{amount}} for user={{user_id}}",
				CompensateTemplate: "echo refunding amount={{amount}} for user={{user_id}}",
			},
		},
	})

	// Failure path: step 2 (Book Hotel) uses a command that will exit with code 1,
	// triggering compensation for all previously completed steps.
	r.Register(&WorkflowDefinition{
		Type: "trip_booking_fail",
		Steps: []StepDefinition{
			{
				Name:               "Book Flight",
				CommandTemplate:    "echo booking flight for user={{user_id}}",
				CompensateTemplate: "echo cancelling flight for user={{user_id}}",
			},
			{
				Name:               "Book Hotel",
				CommandTemplate:    "echo hotel unavailable for user={{user_id}} && exit 1",
				CompensateTemplate: "echo cancelling hotel for user={{user_id}}",
			},
			{
				Name:               "Charge Payment",
				CommandTemplate:    "echo charging amount={{amount}} for user={{user_id}}",
				CompensateTemplate: "echo refunding amount={{amount}} for user={{user_id}}",
			},
		},
	})
}
