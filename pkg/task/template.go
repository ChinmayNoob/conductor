// Package task defines the template a task is created from. The HTTP API,
// the coordinator and cron schedules all share it, so a task is validated the
// same way however it is submitted.
package task

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"

	"github.com/ChinmayNoob/conductor/pkg/db"
	"github.com/ChinmayNoob/conductor/pkg/workflow"
)

var (
	queueRe  = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,62}$`)
	envKeyRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
)

// Template describes a task. For a shell task, Command is run with `sh -c`.
type Template struct {
	Type              string                  `json:"type,omitempty"` // shell (default), http, container
	Command           string                  `json:"command,omitempty"`
	HTTP              *workflow.HTTPSpec      `json:"http,omitempty"`
	Container         *workflow.ContainerSpec `json:"container,omitempty"`
	Env               map[string]string       `json:"env,omitempty"`
	Labels            map[string]string       `json:"labels,omitempty"` // a worker must have all of these
	Queue             string                  `json:"queue,omitempty"`
	Priority          int                     `json:"priority,omitempty"`
	MaxRetries        *int                    `json:"max_retries,omitempty"`
	RetryDelaySeconds int                     `json:"retry_delay_seconds,omitempty"`
	TimeoutSeconds    int                     `json:"timeout_seconds,omitempty"`
}

func (t *Template) typ() string {
	if t.Type == "" {
		return workflow.TypeShell
	}
	return t.Type
}

// Validate returns every problem with the template.
func (t *Template) Validate() error {
	var errs []error
	add := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	switch t.typ() {
	case workflow.TypeShell:
		if strings.TrimSpace(t.Command) == "" {
			add("command is required for shell tasks")
		}
		if t.HTTP != nil || t.Container != nil {
			add("shell tasks cannot have http or container settings")
		}
	case workflow.TypeHTTP:
		if t.HTTP == nil || t.HTTP.URL == "" {
			add("http tasks need http.url")
		} else if m := strings.ToUpper(t.HTTP.Method); m != "" &&
			!slices.Contains([]string{"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD"}, m) {
			add("unsupported HTTP method %q", t.HTTP.Method)
		}
		if t.Container != nil {
			add("http tasks cannot have container settings")
		}
	case workflow.TypeContainer:
		if t.Container == nil || t.Container.Image == "" {
			add("container tasks need container.image")
		} else if n := t.Container.Network; n != "" && n != "bridge" && n != "none" {
			add("container.network must be bridge or none")
		}
		if t.HTTP != nil {
			add("container tasks cannot have http settings")
		}
	default:
		add("unknown type %q (want shell, http or container)", t.Type)
	}

	if t.Queue != "" && !queueRe.MatchString(t.Queue) {
		add("queue %q must be lowercase letters, digits, '-' or '_'", t.Queue)
	}
	if t.Priority < 0 || t.Priority > 10 {
		add("priority must be between 1 and 10")
	}
	if (t.MaxRetries != nil && *t.MaxRetries < 0) || t.RetryDelaySeconds < 0 || t.TimeoutSeconds < 0 {
		add("max_retries, retry_delay_seconds and timeout_seconds must not be negative")
	}
	for k := range t.Env {
		if !envKeyRe.MatchString(k) {
			add("env name %q is not a valid environment variable name", k)
		}
	}
	for k := range t.Labels {
		if k == "" {
			add("label names must not be empty")
		}
	}
	return errors.Join(errs...)
}

// NewTask turns a validated template into a task to insert. Unset fields get
// the defaults: priority 5, 3 retries 60s apart, a 5 minute timeout.
func (t *Template) NewTask(namespace string) (db.NewTask, error) {
	if err := t.Validate(); err != nil {
		return db.NewTask{}, err
	}
	n := db.DefaultTask(t.Command)
	n.Namespace = namespace
	n.Type = t.typ()
	n.Env = t.Env
	if t.Queue != "" {
		n.Queue = t.Queue
	}
	if t.Priority > 0 {
		n.Priority = t.Priority
	}
	if t.MaxRetries != nil {
		n.MaxRetries = *t.MaxRetries
	}
	if t.RetryDelaySeconds > 0 {
		n.RetryDelaySeconds = t.RetryDelaySeconds
	}
	if t.TimeoutSeconds > 0 {
		n.TimeoutSeconds = t.TimeoutSeconds
	}

	var spec any
	switch n.Type {
	case workflow.TypeHTTP:
		h := *t.HTTP
		h.Method = strings.ToUpper(h.Method)
		if h.Method == "" {
			h.Method = "GET"
		}
		spec = h
		n.Data = h.Method + " " + h.URL
	case workflow.TypeContainer:
		spec = t.Container
		n.Data = strings.TrimSpace(t.Container.Image + " " + strings.Join(t.Container.Command, " "))
	}
	if spec != nil {
		b, err := json.Marshal(spec)
		if err != nil {
			return db.NewTask{}, err
		}
		n.Spec = b
	}
	n.Requirements = Requirements(n.Type, t.Labels)
	return n, nil
}

// Requirements returns the labels a worker needs to run a task: the task's
// own labels plus the one advertising support for its type.
func Requirements(taskType string, labels map[string]string) db.StringMap {
	req := maps.Clone(labels)
	if req == nil {
		req = make(map[string]string)
	}
	req["type."+taskType] = "true"
	return req
}
