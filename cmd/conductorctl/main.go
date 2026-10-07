// Command conductorctl is the command-line client for Conductor's HTTP API.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/ChinmayNoob/conductor/pkg/client"
)

const usage = `Usage: conductorctl [global flags] <command> [flags] [args]

Tasks:
  task submit -cmd "echo hi" [-priority N] [-retries N] [-retry-delay S]
              [-timeout S] [-delay S] [-wait]
  task get <id>
  task list [-status S] [-limit N]
  task cancel <id>
  stats

Workflows:
  workflow start <type> [-input JSON] [-wait]
  workflow get <id>
  workflow list [-limit N]
  workflow watch <id>        Follow a workflow until it finishes
  workflow cancel <id>

API keys (admin):
  apikey create <name> [-admin]
  apikey list
  apikey revoke <id>

Global flags:
  -url URL       API URL (env CONDUCTOR_URL, default http://localhost:8081)
  -api-key KEY   API key (env CONDUCTOR_API_KEY)
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

	app := &cli{c: client.New(*url, *apiKey), output: *output}
	if err := app.run(ctx, args); err != nil {
		fail(err)
	}
}

func (a *cli) run(ctx context.Context, args []string) error {
	cmd := args[0]
	if cmd == "stats" {
		return a.stats(ctx)
	}
	if len(args) < 2 {
		return fmt.Errorf("usage: conductorctl %s <subcommand>; run conductorctl -h for help", cmd)
	}
	sub, rest := args[1], args[2:]

	switch cmd + " " + sub {
	case "task submit":
		return a.taskSubmit(ctx, rest)
	case "task get":
		return a.withID(rest, func(id string) error { return a.printTask(a.c.GetTask(ctx, id)) })
	case "task list":
		return a.taskList(ctx, rest)
	case "task cancel":
		return a.withID(rest, func(id string) error { return a.printTask(a.c.CancelTask(ctx, id)) })
	case "workflow start":
		return a.workflowStart(ctx, rest)
	case "workflow get":
		return a.withID(rest, func(id string) error { return a.printWorkflow(a.c.GetWorkflow(ctx, id)) })
	case "workflow list":
		return a.workflowList(ctx, rest)
	case "workflow watch":
		return a.withID(rest, func(id string) error { return a.workflowWatch(ctx, id) })
	case "workflow cancel":
		return a.withID(rest, func(id string) error { return a.printWorkflow(a.c.CancelWorkflow(ctx, id)) })
	case "apikey create":
		return a.apikeyCreate(ctx, rest)
	case "apikey list":
		return a.apikeyList(ctx)
	case "apikey revoke":
		return a.withID(rest, func(id string) error {
			if err := a.c.RevokeAPIKey(ctx, id); err != nil {
				return err
			}
			fmt.Println("revoked", id)
			return nil
		})
	}
	return fmt.Errorf("unknown command %q; run conductorctl -h for help", cmd+" "+sub)
}

func (a *cli) withID(args []string, fn func(id string) error) error {
	if len(args) != 1 {
		return errors.New("expected exactly one ID argument")
	}
	return fn(args[0])
}

// --- tasks ---

func (a *cli) taskSubmit(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("task submit", flag.ExitOnError)
	var req client.TaskRequest
	fs.StringVar(&req.Data, "cmd", "", "shell command to run (required)")
	fs.IntVar(&req.Priority, "priority", 0, "1 (highest) to 10")
	fs.IntVar(&req.MaxRetries, "retries", 0, "max retries")
	fs.IntVar(&req.RetryDelaySeconds, "retry-delay", 0, "base retry delay in seconds")
	fs.IntVar(&req.TimeoutSeconds, "timeout", 0, "timeout in seconds")
	fs.IntVar(&req.DelaySeconds, "delay", 0, "start after this many seconds")
	wait := fs.Bool("wait", false, "wait for the task to finish")
	_ = fs.Parse(args)
	if req.Data == "" {
		return errors.New("-cmd is required")
	}

	t, err := a.c.SubmitTask(ctx, req)
	if err != nil {
		return err
	}
	if *wait {
		t, err = a.c.WaitForTask(ctx, t.ID)
	}
	return a.printTask(t, err)
}

func (a *cli) taskList(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("task list", flag.ExitOnError)
	status := fs.String("status", "", "filter by status")
	limit := fs.Int("limit", 20, "max tasks")
	_ = fs.Parse(args)

	tasks, err := a.c.ListTasks(ctx, strings.ToUpper(*status), *limit)
	if err != nil {
		return err
	}
	if a.output == "json" {
		return printJSON(tasks)
	}
	tw := table("ID", "STATUS", "PRI", "RETRIES", "CREATED", "COMMAND")
	for _, t := range tasks {
		fmt.Fprintf(tw, "%s\t%s\t%d\t%d/%d\t%s\t%s\n", t.ID, t.Status, t.Priority, t.RetryCount,
			t.MaxRetries, t.CreatedAt.Local().Format(time.DateTime), truncate(t.Data, 50))
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
	fmt.Printf("ID:       %s\nStatus:   %s\nCommand:  %s\nPriority: %d\nRetries:  %d/%d\n",
		t.ID, t.Status, t.Data, t.Priority, t.RetryCount, t.MaxRetries)
	if t.ErrorMessage != "" {
		fmt.Printf("Error:    %s\n", t.ErrorMessage)
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

func (a *cli) workflowStart(ctx context.Context, args []string) error {
	if len(args) == 0 || strings.HasPrefix(args[0], "-") {
		return errors.New("usage: workflow start <type> [-input JSON] [-wait]")
	}
	wfType := args[0]
	fs := flag.NewFlagSet("workflow start", flag.ExitOnError)
	input := fs.String("input", "", "input as a JSON object")
	wait := fs.Bool("wait", false, "follow the workflow until it finishes")
	_ = fs.Parse(args[1:])

	var in any
	if *input != "" {
		if err := json.Unmarshal([]byte(*input), &in); err != nil {
			return fmt.Errorf("-input is not valid JSON: %w", err)
		}
	}
	wf, err := a.c.StartWorkflow(ctx, wfType, in)
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
	tw := table("ID", "TYPE", "STATUS", "CREATED")
	for _, wf := range wfs {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", wf.ID, wf.Type, wf.Status, wf.CreatedAt.Local().Format(time.DateTime))
	}
	return tw.Flush()
}

// workflowWatch prints each change to the workflow until it finishes.
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
			if wf.Status != "COMPLETED" {
				return fmt.Errorf("workflow %s: %s", wf.Status, wf.ErrorMessage)
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
	fmt.Printf("ID:     %s\nType:   %s\nStatus: %s\nInput:  %s\n", wf.ID, wf.Type, wf.Status, wf.Input)
	if wf.ErrorMessage != "" {
		fmt.Printf("Error:  %s\n", wf.ErrorMessage)
	}
	if len(wf.Steps) > 0 {
		fmt.Println()
		tw := table("#", "STEP", "STATUS", "TASK", "COMPENSATION")
		for _, s := range wf.Steps {
			fmt.Fprintf(tw, "%d\t%s\t%s\t%s\t%s\n", s.Number, s.Name, s.Status, s.TaskID, s.CompensationTaskID)
		}
		return tw.Flush()
	}
	return nil
}

// --- API keys ---

func (a *cli) apikeyCreate(ctx context.Context, args []string) error {
	if len(args) == 0 || strings.HasPrefix(args[0], "-") {
		return errors.New("usage: apikey create <name> [-admin]")
	}
	name := args[0]
	fs := flag.NewFlagSet("apikey create", flag.ExitOnError)
	admin := fs.Bool("admin", false, "allow managing API keys")
	_ = fs.Parse(args[1:])

	k, err := a.c.CreateAPIKey(ctx, name, *admin)
	if err != nil {
		return err
	}
	if a.output == "json" {
		return printJSON(k)
	}
	fmt.Printf("Created API key %q (%s)\n\n  %s\n\nStore it now: it won't be shown again.\n", k.Name, k.ID, k.Key)
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
	tw := table("ID", "NAME", "PREFIX", "ADMIN", "CREATED", "REVOKED")
	for _, k := range keys {
		revoked := ""
		if k.RevokedAt != nil {
			revoked = k.RevokedAt.Local().Format(time.DateTime)
		}
		fmt.Fprintf(tw, "%s\t%s\t%s…\t%v\t%s\t%s\n", k.ID, k.Name, k.Prefix, k.Admin,
			k.CreatedAt.Local().Format(time.DateTime), revoked)
	}
	return tw.Flush()
}

// --- output helpers ---

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
