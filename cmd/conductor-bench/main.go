// Command conductor-bench measures a running Conductor cluster:
//
//   - ingest: how fast the API accepts tasks
//   - drain: how fast the cluster works through a backlog (tasks are
//     submitted to a paused queue, which is then resumed)
//   - latency: dispatch and end-to-end latency at a steady load (a fraction
//     of the measured drain rate), and on an idle cluster
//   - workflows: end-to-end time of a four-step diamond workflow
//
// Every task is a no-op shell command, so the numbers measure Conductor's
// own overhead. It submits work through the HTTP API like any client, then
// reads the timestamps Postgres recorded (all from the database clock).
//
//	go run ./cmd/conductor-bench -tasks 4000 -workflows 100 -dsn postgres://...
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ChinmayNoob/conductor/pkg/client"
	_ "github.com/lib/pq"
)

type Result struct {
	Label             string  `json:"label"`
	Workers           int     `json:"workers"`
	Slots             int     `json:"slots"`
	Tasks             int     `json:"tasks"`
	IngestPerSec      float64 `json:"ingest_per_sec"`
	DrainPerSec       float64 `json:"drain_per_sec"`
	LoadPerSec        float64 `json:"load_per_sec"`
	DispatchP50Ms     float64 `json:"dispatch_p50_ms"`
	DispatchP95Ms     float64 `json:"dispatch_p95_ms"`
	DispatchP99Ms     float64 `json:"dispatch_p99_ms"`
	EndToEndP50Ms     float64 `json:"end_to_end_p50_ms"`
	EndToEndP99Ms     float64 `json:"end_to_end_p99_ms"`
	IdleDispatchP50Ms float64 `json:"idle_dispatch_p50_ms"`
	IdleDispatchP99Ms float64 `json:"idle_dispatch_p99_ms"`
	Workflows         int     `json:"workflows"`
	WorkflowP50Ms     float64 `json:"workflow_p50_ms"`
	WorkflowP99Ms     float64 `json:"workflow_p99_ms"`
	WorkflowsPerSec   float64 `json:"workflows_per_sec"`
}

func main() {
	url := flag.String("url", "http://localhost:8081", "API URL")
	key := flag.String("key", os.Getenv("CONDUCTOR_API_KEY"), "admin API key")
	dsn := flag.String("dsn", "postgres://postgres:postgres@localhost:5433/taskscheduler?sslmode=disable&timezone=UTC", "Postgres DSN, for timings")
	tasks := flag.Int("tasks", 4000, "tasks in the drain run")
	loadFraction := flag.Float64("load", 0.5, "steady load as a fraction of the drain rate")
	loadSeconds := flag.Int("load-seconds", 10, "length of the steady-load run")
	workflows := flag.Int("workflows", 50, "workflow runs to start")
	concurrency := flag.Int("concurrency", 32, "parallel submitters")
	label := flag.String("label", "", "label for this run")
	out := flag.String("out", "", "append the result as a JSON line to this file")
	flag.Parse()

	ctx := context.Background()
	c := client.New(*url, *key)
	db, err := sql.Open("postgres", *dsn)
	must(err)
	defer db.Close()

	res := Result{Label: *label, Tasks: *tasks, Workflows: *workflows}
	ws, err := c.ListWorkers(ctx)
	must(err)
	for _, w := range ws {
		if w.Status == "healthy" && w.Labels["type.shell"] == "true" {
			res.Workers++
			res.Slots += w.Slots
		}
	}
	fmt.Printf("cluster: %d workers, %d slots\n", res.Workers, res.Slots)

	// 1. Idle dispatch latency: one task at a time on an idle cluster.
	queue := fmt.Sprintf("bench-idle-%d", time.Now().UnixNano())
	for range 30 {
		t, err := c.SubmitTask(ctx, client.TaskRequest{Command: "true", Queue: queue})
		must(err)
		_, err = c.WaitForTask(ctx, t.ID)
		must(err)
	}
	idle := durations(db, queue, "picked_at", "created_at")
	res.IdleDispatchP50Ms, res.IdleDispatchP99Ms = pct(idle, 50), pct(idle, 99)

	// 2. Ingest and drain: fill a paused queue, then resume it.
	queue = fmt.Sprintf("bench-drain-%d", time.Now().UnixNano())
	_, err = c.PutQueue(ctx, queue, client.QueueSettings{Paused: true})
	must(err)
	start := time.Now()
	parallel(*tasks, *concurrency, func(int) {
		_, err := c.SubmitTask(ctx, client.TaskRequest{Command: "true", Queue: queue, MaxRetries: client.Retries(0)})
		must(err)
	})
	res.IngestPerSec = float64(*tasks) / time.Since(start).Seconds()
	resumed, err := c.PutQueue(ctx, queue, client.QueueSettings{Paused: false})
	must(err)
	waitDone(db, `SELECT count(*) FROM tasks WHERE queue = $1 AND status NOT IN ('COMPLETED','FAILED','CANCELLED')`, queue)
	var span float64
	must(db.QueryRow(`SELECT EXTRACT(EPOCH FROM max(completed_at) - $2) FROM tasks WHERE queue = $1`,
		queue, resumed.UpdatedAt).Scan(&span))
	res.DrainPerSec = float64(*tasks) / span
	must(c.DeleteQueue(ctx, queue))

	// 3. Latency under a steady load, submitted at a fixed rate (open loop).
	rate := res.DrainPerSec * *loadFraction
	n := int(rate * float64(*loadSeconds))
	queue = fmt.Sprintf("bench-load-%d", time.Now().UnixNano())
	start = time.Now()
	parallel(n, *concurrency, func(i int) {
		time.Sleep(time.Until(start.Add(time.Duration(float64(i) / rate * float64(time.Second)))))
		_, err := c.SubmitTask(ctx, client.TaskRequest{Command: "true", Queue: queue, MaxRetries: client.Retries(0)})
		must(err)
	})
	res.LoadPerSec = float64(n) / time.Since(start).Seconds()
	waitDone(db, `SELECT count(*) FROM tasks WHERE queue = $1 AND status NOT IN ('COMPLETED','FAILED','CANCELLED')`, queue)
	dispatch := durations(db, queue, "picked_at", "created_at")
	e2e := durations(db, queue, "completed_at", "created_at")
	res.DispatchP50Ms, res.DispatchP95Ms, res.DispatchP99Ms = pct(dispatch, 50), pct(dispatch, 95), pct(dispatch, 99)
	res.EndToEndP50Ms, res.EndToEndP99Ms = pct(e2e, 50), pct(e2e, 99)

	// 3. Workflows: a four-step diamond of no-op steps.
	if *workflows > 0 {
		name := "bench_diamond"
		_, err := c.ApplyDefinition(ctx, []byte(`
name: bench_diamond
description: Benchmark workflow - a diamond of no-op steps
defaults: {retries: 0}
steps:
  - {name: a, run: "true"}
  - {name: b, run: "true", depends_on: [a]}
  - {name: c, run: "true", depends_on: [a]}
  - {name: d, run: "true", depends_on: [b, c]}
`))
		must(err)
		var ids sync.Map
		start := time.Now()
		parallel(*workflows, *concurrency, func(i int) {
			wf, err := c.StartWorkflow(ctx, client.WorkflowRequest{Workflow: name})
			must(err)
			ids.Store(i, wf.ID)
		})
		var list []string
		ids.Range(func(_, v any) bool { list = append(list, v.(string)); return true })
		for {
			var running int
			must(db.QueryRow(`SELECT count(*) FROM workflows WHERE id::text = ANY($1) AND status IN ('RUNNING','COMPENSATING')`,
				pqArray(list)).Scan(&running))
			if running == 0 {
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
		res.WorkflowsPerSec = float64(*workflows) / time.Since(start).Seconds()
		rows, err := db.Query(`SELECT EXTRACT(EPOCH FROM updated_at - created_at) * 1000 FROM workflows WHERE id::text = ANY($1)`, pqArray(list))
		must(err)
		var wfd []float64
		for rows.Next() {
			var ms float64
			must(rows.Scan(&ms))
			wfd = append(wfd, ms)
		}
		rows.Close()
		slices.Sort(wfd)
		res.WorkflowP50Ms, res.WorkflowP99Ms = pct(wfd, 50), pct(wfd, 99)
	}

	b, _ := json.MarshalIndent(res, "", "  ")
	fmt.Println(string(b))
	if *out != "" {
		f, err := os.OpenFile(*out, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		must(err)
		line, _ := json.Marshal(res)
		fmt.Fprintln(f, string(line))
		f.Close()
	}
}

// durations returns sorted millisecond gaps between two task timestamps.
func durations(db *sql.DB, queue, end, start string) []float64 {
	rows, err := db.Query(fmt.Sprintf(
		`SELECT EXTRACT(EPOCH FROM %s - %s) * 1000 FROM tasks WHERE queue = $1 AND %s IS NOT NULL`, end, start, end), queue)
	must(err)
	defer rows.Close()
	var out []float64
	for rows.Next() {
		var ms float64
		must(rows.Scan(&ms))
		out = append(out, ms)
	}
	slices.Sort(out)
	return out
}

func pct(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	i := int(float64(len(sorted)-1) * p / 100)
	return float64(int(sorted[i]*10)) / 10
}

func waitDone(db *sql.DB, query string, args ...any) {
	for {
		var n int
		must(db.QueryRow(query, args...).Scan(&n))
		if n == 0 {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func parallel(n, workers int, fn func(int)) {
	var next atomic.Int64
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1)) - 1
				if i >= n {
					return
				}
				fn(i)
			}
		}()
	}
	wg.Wait()
}

func pqArray(ids []string) string {
	b, _ := json.Marshal(ids)
	s := string(b)
	return "{" + s[1:len(s)-1] + "}"
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
