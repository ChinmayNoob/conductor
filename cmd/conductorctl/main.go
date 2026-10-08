// Command conductorctl is the command-line client for Conductor's HTTP API.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/ChinmayNoob/conductor/pkg/client"
)

const usage = `Usage: conductorctl [global flags] <command> [args] [flags]

Tasks:
  task submit -cmd "echo hi" [task flags] [-wait]
  task submit -type http -url URL [-method POST] [-body JSON] [-header K=V]
  task submit -type container -image alpine:3.20 [-- command args...]
  task submit -prompt "Summarize: ..." [-model M] [-system S] [-schema JSON]
  task get|cancel|requeue <id>
  task logs <id> [-f]          Output; -f follows a running task live
  task attempts <id>           Every attempt, with why each failed
  task list [-status S] [-queue Q] [-limit N]
  dlq                          Permanently failed tasks (dead-letter queue)
  stats

  task flags: -env K=V -label K=V -queue Q -priority N -retries N
              -retry-delay S -timeout S -delay S -key IDEMPOTENCY_KEY

Workflows:
  workflow apply -f file.yaml  Upload a definition (new version if changed)
  workflow defs                List definitions
  workflow show <name> [-version N]
  workflow start <name> [-input JSON] [-version N] [-key K] [-wait]
  workflow get|watch|cancel <id>
  workflow approve|reject <id> <step> [-comment C]
  workflow signal <id> <name> [-data JSON]
  approvals                    Approval steps waiting for a decision
  workflow list [-limit N]

Schedules:
  schedule create <name> -cron "*/5 * * * *" [-tz Asia/Kolkata] [-misfire skip|run_once|catch_up]
                  (-cmd "..." [task flags] | -workflow NAME [-input JSON])
  schedule list
  schedule get|delete|pause|resume|trigger <name>

Queues and workers:
  queue list
  queue set <name> [-concurrency N] [-rate N -per SECONDS] [-pause]
  queue delete <name>
  workers                      (admin)
  cluster                      Leader, epoch and coordinators (admin)

Admin:
  namespace create|set <name> [-max-pending N] [-max-concurrency N]
  namespace list
  apikey create <name> [-namespace NS] [-admin]
  apikey list
  apikey revoke <id>

Global flags:
  -url URL       API URL (env CONDUCTOR_URL, default http://localhost:8081)
  -api-key KEY   API key (env CONDUCTOR_API_KEY)
  -n NAMESPACE   Act in another namespace (admin keys only; env CONDUCTOR_NAMESPACE)
  -o FORMAT      Output: text or json (default text)
`

type cli struct {
	c      *client.Client
	output string
}

func main() {
	global := flag.NewFlagSet("conductorctl", flag.ExitOnError)
	global.Usage = func() { fmt.Fprint(os.Stderr, usage) }
	url := global.String("url", envOr("CONDUCTOR_URL", "http://localhost:8081"), "")
	apiKey := global.String("api-key", os.Getenv("CONDUCTOR_API_KEY"), "")
	ns := global.String("n", os.Getenv("CONDUCTOR_NAMESPACE"), "")
	output := global.String("o", "text", "")
	_ = global.Parse(os.Args[1:])

	args := global.Args()
	if len(args) == 0 {
		global.Usage()
		os.Exit(2)
	}
	if *output != "text" && *output != "json" {
		fail(errors.New("-o must be text or json"))
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	c := client.New(*url, *apiKey)
	if *ns != "" {
		c = c.WithNamespace(*ns)
	}
	app := &cli{c: c, output: *output}
	if err := app.run(ctx, args); err != nil {
		fail(err)
	}
}

func (a *cli) run(ctx context.Context, args []string) error {
	switch args[0] {
	case "stats":
		return a.stats(ctx)
	case "dlq":
		return a.dlq(ctx, args[1:])
	case "workers":
		return a.workers(ctx)
	case "cluster":
		return a.cluster(ctx)
	case "approvals":
		return a.approvals(ctx)
	}
	if len(args) < 2 {
		return fmt.Errorf("usage: conductorctl %s <subcommand>; run conductorctl -h for help", args[0])
	}
	cmd, sub, rest := args[0], args[1], args[2:]
	id := func(fn func(string) error) error {
		if len(rest) != 1 {
			return errors.New("expected exactly one argument")
		}
		return fn(rest[0])
	}

	switch cmd + " " + sub {
	case "task submit":
		return a.taskSubmit(ctx, rest)
	case "task get":
		return id(func(x string) error { return a.printTask(a.c.GetTask(ctx, x)) })
	case "task cancel":
		return id(func(x string) error { return a.printTask(a.c.CancelTask(ctx, x)) })
	case "task requeue":
		return id(func(x string) error { return a.printTask(a.c.RequeueTask(ctx, x)) })
	case "task list":
		return a.taskList(ctx, rest)
	case "task logs":
		return a.taskLogs(ctx, rest)
	case "task attempts":
		return id(func(x string) error { return a.taskAttempts(ctx, x) })

	case "workflow apply":
		return a.workflowApply(ctx, rest)
	case "workflow defs":
		return a.workflowDefs(ctx)
	case "workflow show":
		return a.workflowShow(ctx, rest)
	case "workflow start":
		return a.workflowStart(ctx, rest)
	case "workflow get":
		return id(func(x string) error { return a.printWorkflow(a.c.GetWorkflow(ctx, x)) })
	case "workflow watch":
		return id(func(x string) error { return a.workflowWatch(ctx, x) })
	case "workflow cancel":
		return id(func(x string) error { return a.printWorkflow(a.c.CancelWorkflow(ctx, x)) })
	case "workflow approve", "workflow reject":
		return a.workflowDecide(ctx, sub == "approve", rest)
	case "workflow signal":
		return a.workflowSignal(ctx, rest)
	case "workflow list":
		return a.workflowList(ctx, rest)

	case "schedule create":
		return a.scheduleCreate(ctx, rest)
	case "schedule list":
		return a.scheduleList(ctx)
	case "schedule get":
		return id(func(x string) error { return a.printSchedule(a.c.GetSchedule(ctx, x)) })
	case "schedule delete":
		return id(func(x string) error { return a.done(a.c.DeleteSchedule(ctx, x), "deleted schedule "+x) })
	case "schedule pause", "schedule resume", "schedule trigger":
		return id(func(x string) error { return a.printSchedule(a.c.ScheduleAction(ctx, x, sub)) })

	case "queue list":
		return a.queueList(ctx)
	case "queue set":
		return a.queueSet(ctx, rest)
	case "queue delete":
		return id(func(x string) error { return a.done(a.c.DeleteQueue(ctx, x), "removed limits from queue "+x) })

	case "namespace create", "namespace set":
		return a.namespaceSave(ctx, sub, rest)
	case "namespace list":
		return a.namespaceList(ctx)

	case "apikey create":
		return a.apikeyCreate(ctx, rest)
	case "apikey list":
		return a.apikeyList(ctx)
	case "apikey revoke":
		return id(func(x string) error { return a.done(a.c.RevokeAPIKey(ctx, x), "revoked "+x) })
	}
	return fmt.Errorf("unknown command %q; run conductorctl -h for help", cmd+" "+sub)
}

func (a *cli) done(err error, msg string) error {
	if err == nil {
		fmt.Println(msg)
	}
	return err
}

// positional splits leading positional arguments from flags.
func positional(args []string, n int, usage string) ([]string, []string, error) {
	if len(args) < n {
		return nil, nil, errors.New("usage: " + usage)
	}
	for _, a := range args[:n] {
		if strings.HasPrefix(a, "-") {
			return nil, nil, errors.New("usage: " + usage)
		}
	}
	return args[:n], args[n:], nil
}

// kv is a repeatable key=value flag.
type kv map[string]string

func (m kv) String() string { return "" }
func (m kv) Set(s string) error {
	k, v, ok := strings.Cut(s, "=")
	if !ok || k == "" {
		return fmt.Errorf("%q must look like key=value", s)
	}
	m[k] = v
	return nil
}

// --- tasks ---

// taskFlags registers the flags shared by "task submit" and "schedule create".
func taskFlags(fs *flag.FlagSet) func() (client.TaskRequest, error) {
	var req client.TaskRequest
	env, labels, headers := kv{}, kv{}, kv{}
	var url, method, body, image, prompt, model, system, schema string
	retries := -1
	fs.StringVar(&req.Type, "type", "", "shell (default), http, container or llm")
	fs.StringVar(&req.Command, "cmd", "", "shell command")
	fs.StringVar(&url, "url", "", "http: URL")
	fs.StringVar(&method, "method", "", "http: method (default GET)")
	fs.StringVar(&body, "body", "", "http: request body")
	fs.Var(headers, "header", "http: header K=V (repeatable)")
	fs.StringVar(&image, "image", "", "container: image")
	fs.StringVar(&prompt, "prompt", "", "llm: the prompt")
	fs.StringVar(&model, "model", "", "llm: model (default: the workers' CONDUCTOR_LLM_MODEL)")
	fs.StringVar(&system, "system", "", "llm: system message")
	fs.StringVar(&schema, "schema", "", "llm: JSON Schema (an object) the answer must match; its fields become outputs")
	fs.Var(env, "env", "environment variable K=V (repeatable)")
	fs.Var(labels, "label", "required worker label K=V (repeatable)")
	fs.StringVar(&req.Queue, "queue", "", "queue name")
	fs.IntVar(&req.Priority, "priority", 0, "1 (highest) to 10")
	fs.IntVar(&retries, "retries", -1, "max retries")
	fs.IntVar(&req.RetryDelaySeconds, "retry-delay", 0, "base retry delay in seconds")
	fs.IntVar(&req.TimeoutSeconds, "timeout", 0, "timeout in seconds")

	return func() (client.TaskRequest, error) {
		if retries >= 0 {
			req.MaxRetries = client.Retries(retries)
		}
		if len(env) > 0 {
			req.Env = env
		}
		if len(labels) > 0 {
			req.Labels = labels
		}
		switch {
		case url != "":
			if req.Type == "" {
				req.Type = "http"
			}
			req.HTTP = &client.HTTPSpec{URL: url, Method: method, Body: body}
			if len(headers) > 0 {
				req.HTTP.Headers = headers
			}
		case image != "":
			if req.Type == "" {
				req.Type = "container"
			}
			req.Container = &client.ContainerSpec{Image: image, Command: fs.Args()}
		case prompt != "":
			if req.Type == "" {
				req.Type = "llm"
			}
			req.LLM = &client.LLMSpec{Prompt: prompt, Model: model, System: system}
			if schema != "" {
				if err := json.Unmarshal([]byte(schema), &req.LLM.Schema); err != nil {
					return req, fmt.Errorf("-schema is not a JSON object: %w", err)
				}
			}
		case req.Command == "":
			return req, errors.New("one of -cmd, -url, -image or -prompt is required")
		}
		return req, nil
	}
}

func (a *cli) taskSubmit(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("task submit", flag.ExitOnError)
	build := taskFlags(fs)
	delay := fs.Int("delay", 0, "start after this many seconds")
	key := fs.String("key", "", "idempotency key")
	wait := fs.Bool("wait", false, "wait for the task to finish")
	_ = fs.Parse(args)

	req, err := build()
	if err != nil {
		return err
	}
	req.DelaySeconds, req.IdempotencyKey = *delay, *key

	t, err := a.c.SubmitTask(ctx, req)
	if err != nil {
		return err
	}
	if !t.Created && a.output == "text" {
		fmt.Println("(idempotency key matched an existing task)")
	}
	if *wait {
		t, err = a.c.WaitForTask(ctx, t.ID)
	}
	return a.printTask(t, err)
}

func (a *cli) taskList(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("task list", flag.ExitOnError)
	status := fs.String("status", "", "filter by status")
	queue := fs.String("queue", "", "filter by queue")
	limit := fs.Int("limit", 20, "max tasks")
	_ = fs.Parse(args)

	tasks, err := a.c.ListTasks(ctx, client.TaskFilter{Status: strings.ToUpper(*status), Queue: *queue, Limit: *limit})
	if err != nil {
		return err
	}
	return a.printTasks(tasks)
}

func (a *cli) dlq(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("dlq", flag.ExitOnError)
	limit := fs.Int("limit", 20, "max tasks")
	_ = fs.Parse(args)
	tasks, err := a.c.DeadLetter(ctx, *limit)
	if err != nil {
		return err
	}
	return a.printTasks(tasks)
}

func (a *cli) printTasks(tasks []client.Task) error {
	if a.output == "json" {
		return printJSON(tasks)
	}
	tw := table("ID", "STATUS", "TYPE", "QUEUE", "PRI", "TRIES", "CREATED", "COMMAND")
	for _, t := range tasks {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%d\t%d/%d\t%s\t%s\n", t.ID, t.Status, t.Type, t.Queue, t.Priority,
			t.RetryCount, t.MaxRetries, t.CreatedAt.Local().Format(time.DateTime), truncate(t.Command, 50))
	}
	return tw.Flush()
}

func (a *cli) printTask(t *client.Task, err error) error {
	if err != nil {
		return err
	}
	if a.output == "json" {
		return printJSON(t)
	}
	fmt.Printf("ID:       %s\nStatus:   %s\nType:     %s\nQueue:    %s\nCommand:  %s\nPriority: %d\nRetries:  %d/%d\n",
		t.ID, t.Status, t.Type, t.Queue, t.Command, t.Priority, t.RetryCount, t.MaxRetries)
	if len(t.Labels) > 0 {
		fmt.Printf("Labels:   %s\n", formatMap(t.Labels))
	}
	if t.WorkflowID != "" {
		fmt.Printf("Workflow: %s\n", t.WorkflowID)
	}
	if t.ErrorMessage != "" {
		fmt.Printf("Error:    %s\n", t.ErrorMessage)
	}
	if len(t.Outputs) > 0 {
		fmt.Printf("Outputs:  %s\n", formatMap(t.Outputs))
	}
	if u := t.LLMUsage; u != nil {
		fmt.Printf("Model:    %s (%d tokens in, %d out", u.Model, u.InputTokens, u.OutputTokens)
		if u.CostUSD > 0 {
			fmt.Printf(", $%.6f", u.CostUSD)
		}
		fmt.Println(")")
	}
	if t.Output != "" {
		fmt.Printf("Output:\n%s\n", indent(t.Output))
	}
	return nil
}

func (a *cli) stats(ctx context.Context) error {
	stats, err := a.c.Stats(ctx)
	if err != nil {
		return err
	}
	if a.output == "json" {
		return printJSON(stats)
	}
	tw := table("STATUS", "COUNT")
	for _, s := range []string{"QUEUED", "STARTED", "COMPLETED", "FAILED", "CANCELLED", "total"} {
		fmt.Fprintf(tw, "%s\t%d\n", s, stats[s])
	}
	return tw.Flush()
}

// --- workflows ---

func (a *cli) workflowApply(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("workflow apply", flag.ExitOnError)
	file := fs.String("f", "", "definition file (YAML or JSON); - for stdin")
	_ = fs.Parse(args)
	if *file == "" {
		return errors.New("-f is required")
	}
	var spec []byte
	var err error
	if *file == "-" {
		spec, err = io.ReadAll(os.Stdin)
	} else {
		spec, err = os.ReadFile(*file)
	}
	if err != nil {
		return err
	}
	d, err := a.c.ApplyDefinition(ctx, spec)
	if err != nil {
		return err
	}
	if a.output == "json" {
		return printJSON(d)
	}
	if d.Created {
		fmt.Printf("workflow %s: saved as version %d (%d steps)\n", d.Name, d.Version, d.Steps)
	} else {
		fmt.Printf("workflow %s: unchanged (version %d)\n", d.Name, d.Version)
	}
	return nil
}

func (a *cli) workflowDefs(ctx context.Context) error {
	defs, err := a.c.ListDefinitions(ctx)
	if err != nil {
		return err
	}
	if a.output == "json" {
		return printJSON(defs)
	}
	tw := table("NAME", "VERSION", "STEPS", "UPDATED", "DESCRIPTION")
	for _, d := range defs {
		fmt.Fprintf(tw, "%s\t%d\t%d\t%s\t%s\n", d.Name, d.Version, d.Steps,
			d.CreatedAt.Local().Format(time.DateTime), truncate(d.Description, 60))
	}
	return tw.Flush()
}

func (a *cli) workflowShow(ctx context.Context, args []string) error {
	pos, rest, err := positional(args, 1, "workflow show <name> [-version N]")
	if err != nil {
		return err
	}
	fs := flag.NewFlagSet("workflow show", flag.ExitOnError)
	version := fs.Int("version", 0, "version (default latest)")
	_ = fs.Parse(rest)
	if a.output == "json" {
		d, err := a.c.GetDefinition(ctx, pos[0], *version)
		if err != nil {
			return err
		}
		return printJSON(d)
	}
	y, err := a.c.GetDefinitionYAML(ctx, pos[0], *version)
	if err != nil {
		return err
	}
	fmt.Print(string(y))
	return nil
}

func (a *cli) workflowStart(ctx context.Context, args []string) error {
	pos, rest, err := positional(args, 1, "workflow start <name> [-input JSON] [-wait]")
	if err != nil {
		return err
	}
	fs := flag.NewFlagSet("workflow start", flag.ExitOnError)
	input := fs.String("input", "", "input as a JSON object")
	version := fs.Int("version", 0, "definition version (default latest)")
	key := fs.String("key", "", "idempotency key")
	wait := fs.Bool("wait", false, "follow the workflow until it finishes")
	_ = fs.Parse(rest)

	in, err := parseJSONFlag(*input)
	if err != nil {
		return err
	}
	wf, err := a.c.StartWorkflow(ctx, client.WorkflowRequest{Workflow: pos[0], Version: *version, Input: in, IdempotencyKey: *key})
	if err != nil {
		return err
	}
	if *wait {
		return a.workflowWatch(ctx, wf.ID)
	}
	return a.printWorkflow(wf, nil)
}

func (a *cli) workflowList(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("workflow list", flag.ExitOnError)
	limit := fs.Int("limit", 20, "max workflows")
	_ = fs.Parse(args)

	wfs, err := a.c.ListWorkflows(ctx, *limit)
	if err != nil {
		return err
	}
	if a.output == "json" {
		return printJSON(wfs)
	}
	tw := table("ID", "WORKFLOW", "VERSION", "STATUS", "CREATED")
	for _, wf := range wfs {
		v := "-"
		if wf.Version != nil {
			v = fmt.Sprint(*wf.Version)
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", wf.ID, wf.Workflow, v, wf.Status, wf.CreatedAt.Local().Format(time.DateTime))
	}
	return tw.Flush()
}

// workflowWatch prints each change to a run until it finishes.
func (a *cli) workflowWatch(ctx context.Context, id string) error {
	last := ""
	for {
		wf, err := a.c.GetWorkflow(ctx, id)
		if err != nil {
			return err
		}
		if a.output == "json" {
			if wf.Done() {
				return printJSON(wf)
			}
		} else if snap := workflowSnapshot(wf); snap != last {
			fmt.Printf("[%s] %s\n", time.Now().Format(time.TimeOnly), snap)
			last = snap
		}
		if wf.Done() {
			if a.output == "text" {
				fmt.Println()
				_ = a.printWorkflow(wf, nil)
			}
			if wf.Status != "COMPLETED" {
				return fmt.Errorf("workflow %s", strings.ToLower(wf.Status))
			}
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func workflowSnapshot(wf *client.Workflow) string {
	parts := make([]string, 0, len(wf.Steps))
	for _, s := range wf.Steps {
		parts = append(parts, s.Name+"="+s.Status)
	}
	return fmt.Sprintf("%-12s %s", wf.Status, strings.Join(parts, "  "))
}

func (a *cli) printWorkflow(wf *client.Workflow, err error) error {
	if err != nil {
		return err
	}
	if a.output == "json" {
		return printJSON(wf)
	}
	v := ""
	if wf.Version != nil {
		v = fmt.Sprintf(" (version %d)", *wf.Version)
	}
	fmt.Printf("ID:       %s\nWorkflow: %s%s\nStatus:   %s\nInput:    %s\n", wf.ID, wf.Workflow, v, wf.Status, wf.Input)
	if wf.ErrorMessage != "" {
		fmt.Printf("Error:    %s\n", wf.ErrorMessage)
	}
	if len(wf.Steps) > 0 {
		fmt.Println()
		tw := table("STEP", "STATUS", "AFTER", "OUTPUTS")
		for _, s := range wf.Steps {
			after := strings.Join(s.DependsOn, ",")
			if after == "" {
				after = "-"
			}
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", s.Name, s.Status, after, truncate(formatMap(s.Outputs), 60))
		}
		return tw.Flush()
	}
	return nil
}

// --- schedules ---

func (a *cli) scheduleCreate(ctx context.Context, args []string) error {
	pos, rest, err := positional(args, 1, "schedule create <name> -cron EXPR (-cmd ... | -workflow NAME)")
	if err != nil {
		return err
	}
	fs := flag.NewFlagSet("schedule create", flag.ExitOnError)
	req := client.ScheduleRequest{Name: pos[0]}
	fs.StringVar(&req.Cron, "cron", "", `cron expression, e.g. "*/5 * * * *" or "@every 30s"`)
	fs.StringVar(&req.Timezone, "tz", "", "time zone (default UTC)")
	fs.StringVar(&req.MisfirePolicy, "misfire", "", "skip (default), run_once or catch_up")
	wfName := fs.String("workflow", "", "start this workflow")
	input := fs.String("input", "", "workflow input as JSON")
	build := taskFlags(fs)
	_ = fs.Parse(rest)

	if *wfName != "" {
		in, err := parseJSONFlag(*input)
		if err != nil {
			return err
		}
		req.Workflow = &client.ScheduledWorkflow{Name: *wfName, Input: in}
	} else {
		t, err := build()
		if err != nil {
			return fmt.Errorf("a schedule needs -workflow or a task (%w)", err)
		}
		req.Task = &t
	}
	return a.printSchedule(a.c.CreateSchedule(ctx, req))
}

func (a *cli) scheduleList(ctx context.Context) error {
	list, err := a.c.ListSchedules(ctx)
	if err != nil {
		return err
	}
	if a.output == "json" {
		return printJSON(list)
	}
	tw := table("NAME", "CRON", "TZ", "ENABLED", "NEXT RUN", "LAST RUN", "TARGET")
	for _, s := range list {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%v\t%s\t%s\t%s\n", s.Name, s.Cron, s.Timezone, s.Enabled,
			timeOrDash(s.NextRunAt), timeOrDash(s.LastRunAt), targetSummary(s.Target))
	}
	return tw.Flush()
}

func (a *cli) printSchedule(s *client.Schedule, err error) error {
	if err != nil {
		return err
	}
	if a.output == "json" {
		return printJSON(s)
	}
	fmt.Printf("Name:     %s\nCron:     %s (%s)\nMisfire:  %s\nEnabled:  %v\nTarget:   %s\nNext run: %s\nLast run: %s\n",
		s.Name, s.Cron, s.Timezone, s.MisfirePolicy, s.Enabled, targetSummary(s.Target),
		timeOrDash(s.NextRunAt), timeOrDash(s.LastRunAt))
	if s.LastRunID != "" {
		fmt.Printf("Last ID:  %s\n", s.LastRunID)
	}
	if s.LastError != "" {
		fmt.Printf("Error:    %s\n", s.LastError)
	}
	for i, t := range s.Upcoming {
		if i == 0 {
			fmt.Print("Then:     ")
		} else {
			fmt.Print("          ")
		}
		fmt.Println(t.Local().Format(time.DateTime))
	}
	return nil
}

func targetSummary(raw json.RawMessage) string {
	var t struct {
		Task *struct {
			Type    string `json:"type"`
			Command string `json:"command"`
		} `json:"task"`
		Workflow *struct {
			Name string `json:"name"`
		} `json:"workflow"`
	}
	if json.Unmarshal(raw, &t) != nil {
		return "?"
	}
	switch {
	case t.Workflow != nil:
		return "workflow " + t.Workflow.Name
	case t.Task != nil && t.Task.Command != "":
		return "task: " + truncate(t.Task.Command, 40)
	case t.Task != nil:
		return t.Task.Type + " task"
	}
	return "?"
}

// --- queues, workers, namespaces ---

func (a *cli) queueList(ctx context.Context) error {
	list, err := a.c.ListQueues(ctx)
	if err != nil {
		return err
	}
	if a.output == "json" {
		return printJSON(list)
	}
	tw := table("QUEUE", "QUEUED", "RUNNING", "CONCURRENCY", "RATE", "PAUSED")
	for _, q := range list {
		rate := "-"
		if q.RateLimit != nil {
			rate = fmt.Sprintf("%d/%ds", *q.RateLimit, q.RatePeriodSeconds)
		}
		fmt.Fprintf(tw, "%s\t%d\t%d\t%s\t%s\t%v\n", q.Name, q.Queued, q.Running, intOrDash(q.ConcurrencyLimit), rate, q.Paused)
	}
	return tw.Flush()
}

func (a *cli) queueSet(ctx context.Context, args []string) error {
	pos, rest, err := positional(args, 1, "queue set <name> [-concurrency N] [-rate N -per S] [-pause]")
	if err != nil {
		return err
	}
	fs := flag.NewFlagSet("queue set", flag.ExitOnError)
	concurrency := fs.Int("concurrency", 0, "max tasks running at once (0 = unlimited)")
	rate := fs.Int("rate", 0, "max dispatches per period (0 = unlimited)")
	per := fs.Int("per", 1, "rate period in seconds")
	pause := fs.Bool("pause", false, "stop dispatching from this queue")
	_ = fs.Parse(rest)

	s := client.QueueSettings{RatePeriodSeconds: *per, Paused: *pause}
	if *concurrency > 0 {
		s.ConcurrencyLimit = concurrency
	}
	if *rate > 0 {
		s.RateLimit = rate
	}
	q, err := a.c.PutQueue(ctx, pos[0], s)
	if err != nil {
		return err
	}
	if a.output == "json" {
		return printJSON(q)
	}
	fmt.Printf("queue %s: concurrency %s, rate %s per %ds, paused %v\n",
		q.Name, intOrDash(q.ConcurrencyLimit), intOrDash(q.RateLimit), q.RatePeriodSeconds, q.Paused)
	return nil
}

func (a *cli) workers(ctx context.Context) error {
	list, err := a.c.ListWorkers(ctx)
	if err != nil {
		return err
	}
	if a.output == "json" {
		return printJSON(list)
	}
	tw := table("ID", "ADDRESS", "STATUS", "RUNNING", "LAST SEEN", "LABELS")
	for _, w := range list {
		fmt.Fprintf(tw, "%d\t%s\t%s\t%d/%d\t%s\t%s\n", w.ID, w.Address, w.Status, w.Running, w.Slots,
			w.LastSeen.Local().Format(time.TimeOnly), formatMap(w.Labels))
	}
	return tw.Flush()
}

func (a *cli) cluster(ctx context.Context) error {
	cl, err := a.c.Cluster(ctx)
	if err != nil {
		return err
	}
	if a.output == "json" {
		return printJSON(cl)
	}
	if cl.Leader != nil {
		fmt.Printf("Leader: %s (%s), epoch %d, elected %s\n\n", cl.Leader.ID, cl.Leader.Address, cl.Leader.Epoch,
			timeOrDash(cl.Leader.ElectedAt))
	} else {
		fmt.Print("Leader: none elected yet\n\n")
	}
	tw := table("COORDINATOR", "ADDRESS", "ROLE", "STARTED", "LAST SEEN")
	for _, c := range cl.Coordinators {
		role := "standby"
		if c.Leader {
			role = "leader"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", c.ID, c.Address, role,
			c.StartedAt.Local().Format(time.DateTime), c.LastSeen.Local().Format(time.TimeOnly))
	}
	return tw.Flush()
}

func (a *cli) namespaceSave(ctx context.Context, sub string, args []string) error {
	pos, rest, err := positional(args, 1, "namespace "+sub+" <name> [-max-pending N] [-max-concurrency N]")
	if err != nil {
		return err
	}
	fs := flag.NewFlagSet("namespace", flag.ExitOnError)
	pending := fs.Int("max-pending", 0, "max queued tasks (0 = unlimited)")
	concurrency := fs.Int("max-concurrency", 0, "max running tasks (0 = unlimited)")
	_ = fs.Parse(rest)

	n := client.Namespace{Name: pos[0]}
	if *pending > 0 {
		n.MaxPendingTasks = pending
	}
	if *concurrency > 0 {
		n.MaxConcurrency = concurrency
	}
	var out *client.Namespace
	if sub == "create" {
		out, err = a.c.CreateNamespace(ctx, n)
	} else {
		out, err = a.c.UpdateNamespace(ctx, n)
	}
	if err != nil {
		return err
	}
	if a.output == "json" {
		return printJSON(out)
	}
	fmt.Printf("namespace %s: max pending %s, max concurrency %s\n", out.Name, intOrDash(out.MaxPendingTasks), intOrDash(out.MaxConcurrency))
	return nil
}

func (a *cli) namespaceList(ctx context.Context) error {
	list, err := a.c.ListNamespaces(ctx)
	if err != nil {
		return err
	}
	if a.output == "json" {
		return printJSON(list)
	}
	tw := table("NAME", "MAX PENDING", "MAX CONCURRENCY", "CREATED")
	for _, n := range list {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", n.Name, intOrDash(n.MaxPendingTasks), intOrDash(n.MaxConcurrency),
			n.CreatedAt.Local().Format(time.DateTime))
	}
	return tw.Flush()
}

// --- API keys ---

func (a *cli) apikeyCreate(ctx context.Context, args []string) error {
	pos, rest, err := positional(args, 1, "apikey create <name> [-namespace NS] [-admin]")
	if err != nil {
		return err
	}
	fs := flag.NewFlagSet("apikey create", flag.ExitOnError)
	admin := fs.Bool("admin", false, "allow admin endpoints and any namespace")
	ns := fs.String("namespace", "", "namespace the key is bound to (default: default)")
	_ = fs.Parse(rest)

	k, err := a.c.CreateAPIKey(ctx, pos[0], *ns, *admin)
	if err != nil {
		return err
	}
	if a.output == "json" {
		return printJSON(k)
	}
	fmt.Printf("Created API key %q for namespace %s (%s)\n\n  %s\n\nStore it now: it won't be shown again.\n",
		k.Name, k.Namespace, k.ID, k.Key)
	return nil
}

func (a *cli) apikeyList(ctx context.Context) error {
	keys, err := a.c.ListAPIKeys(ctx)
	if err != nil {
		return err
	}
	if a.output == "json" {
		return printJSON(keys)
	}
	tw := table("ID", "NAME", "NAMESPACE", "PREFIX", "ADMIN", "CREATED", "REVOKED")
	for _, k := range keys {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s…\t%v\t%s\t%s\n", k.ID, k.Name, k.Namespace, k.Prefix, k.Admin,
			k.CreatedAt.Local().Format(time.DateTime), timeOrDash(k.RevokedAt))
	}
	return tw.Flush()
}

// --- helpers ---

func table(headers ...string) *tabwriter.Writer {
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, strings.Join(headers, "\t"))
	return tw
}

func printJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func parseJSONFlag(s string) (any, error) {
	if s == "" {
		return nil, nil
	}
	var v any
	if err := json.Unmarshal([]byte(s), &v); err != nil {
		return nil, fmt.Errorf("-input is not valid JSON: %w", err)
	}
	return v, nil
}

func formatMap(m map[string]string) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = k + "=" + m[k]
	}
	return strings.Join(parts, " ")
}

func timeOrDash(t *time.Time) string {
	if t == nil {
		return "-"
	}
	return t.Local().Format(time.DateTime)
}

func intOrDash(n *int) string {
	if n == nil {
		return "-"
	}
	return fmt.Sprint(*n)
}

func truncate(s string, n int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}

func indent(s string) string {
	return "  " + strings.ReplaceAll(strings.TrimRight(s, "\n"), "\n", "\n  ")
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}

func (a *cli) taskLogs(ctx context.Context, args []string) error {
	pos, rest, err := positional(args, 1, "task logs <id> [-f]")
	if err != nil {
		return err
	}
	fs := flag.NewFlagSet("task logs", flag.ExitOnError)
	follow := fs.Bool("f", false, "follow a running task")
	_ = fs.Parse(rest)
	return a.c.TaskLogs(ctx, pos[0], *follow, os.Stdout)
}

func (a *cli) taskAttempts(ctx context.Context, id string) error {
	list, err := a.c.TaskAttempts(ctx, id)
	if err != nil {
		return err
	}
	if a.output == "json" {
		return printJSON(list)
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "ATTEMPT\tSTATUS\tWORKER\tSTARTED\tTOOK\tERROR")
	for _, at := range list {
		worker, started, took := "-", "-", "-"
		if at.WorkerID != nil {
			worker = fmt.Sprint(*at.WorkerID)
		}
		if at.StartedAt != nil {
			started = at.StartedAt.Local().Format(time.DateTime)
			if at.FinishedAt != nil {
				took = at.FinishedAt.Sub(*at.StartedAt).Round(time.Millisecond).String()
			}
		}
		fmt.Fprintf(tw, "%d\t%s\t%s\t%s\t%s\t%s\n", at.Attempt, at.Status, worker, started, took, firstLine(at.Error))
	}
	return tw.Flush()
}

func firstLine(s string) string {
	line, _, _ := strings.Cut(s, "\n")
	return line
}

func (a *cli) workflowDecide(ctx context.Context, approve bool, args []string) error {
	pos, rest, err := positional(args, 2, "workflow approve|reject <id> <step> [-comment C]")
	if err != nil {
		return err
	}
	fs := flag.NewFlagSet("workflow decide", flag.ExitOnError)
	comment := fs.String("comment", "", "why")
	_ = fs.Parse(rest)
	if approve {
		return a.printWorkflow(a.c.ApproveStep(ctx, pos[0], pos[1], *comment))
	}
	return a.printWorkflow(a.c.RejectStep(ctx, pos[0], pos[1], *comment))
}

func (a *cli) workflowSignal(ctx context.Context, args []string) error {
	pos, rest, err := positional(args, 2, "workflow signal <id> <name> [-data JSON]")
	if err != nil {
		return err
	}
	fs := flag.NewFlagSet("workflow signal", flag.ExitOnError)
	data := fs.String("data", "{}", "the signal's payload, a JSON object")
	_ = fs.Parse(rest)
	var payload map[string]any
	if err := json.Unmarshal([]byte(*data), &payload); err != nil {
		return fmt.Errorf("-data is not a JSON object: %w", err)
	}
	delivered, err := a.c.Signal(ctx, pos[0], pos[1], payload)
	if err != nil {
		return err
	}
	if delivered {
		fmt.Println("Delivered to the step waiting for it.")
	} else {
		fmt.Println("No step waits for it yet; it is kept until one does.")
	}
	return nil
}

func (a *cli) approvals(ctx context.Context) error {
	list, err := a.c.ListApprovals(ctx)
	if err != nil {
		return err
	}
	if a.output == "json" {
		return printJSON(list)
	}
	tw := table("WORKFLOW RUN", "DEFINITION", "STEP", "WAITING", "QUESTION")
	for _, ap := range list {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", ap.WorkflowID, ap.Workflow, ap.Step,
			time.Since(ap.Since).Round(time.Second), truncate(ap.Message, 60))
	}
	return tw.Flush()
}
