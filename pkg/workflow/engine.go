package workflow

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
)

type StepDefinition struct {
	Name              string
	CommandTemplate   string
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

// ExpandCommand replaces {{key}} placeholders with values from the context JSON.
func ExpandCommand(template string, ctx json.RawMessage) string {
	var data map[string]interface{}
	if err := json.Unmarshal(ctx, &data); err != nil {
		return template
	}
	result := template
	for k, v := range data {
		placeholder := fmt.Sprintf("{{%s}}", k)
		result = strings.ReplaceAll(result, placeholder, fmt.Sprintf("%v", v))
	}
	return result
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
