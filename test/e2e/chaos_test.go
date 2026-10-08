//go:build e2e && chaos

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ChinmayNoob/conductor/pkg/client"
)

// TestChaos runs a steady workload of tasks and saga workflows while
// injecting one fault after another, then checks that nothing was lost,
// nothing is stuck, and every workflow ended the way its definition says it
// must. It needs the chaos overlay (Toxiproxy in front of Postgres):
//
//	docker compose -f docker-compose.yml -f docker-compose.chaos.yml --profile containers up -d --build --scale worker=3
//	E2E_COMPOSE_FILES=docker-compose.yml,docker-compose.chaos.yml go test -tags 'e2e chaos' -run TestChaos -v -timeout 30m ./test/e2e/
func TestChaos(t *testing.T) {
	c := newClient(t)
	toxiproxy(t, http.MethodGet, "/proxies/postgres", nil) // fail fast without the overlay
	t.Cleanup(func() { heal(t) })

	run := uniqueName("chaos")
	w := &workload{c: c, name: run, flows: map[string]bool{}}
	w.okDef = applyDefinition(t, c, sagaDefinition(run+"-ok", false)).Name
	w.failDef = applyDefinition(t, c, sagaDefinition(run+"-fail", true)).Name

	ctx, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	go w.run(ctx, done)

	pause := func() { time.Sleep(8 * time.Second) }
	fault := func(name string, inject func()) {
		start := time.Now()
		t.Logf("fault: %s", name)
		inject()
		t.Logf("fault: %s healed after %v", name, time.Since(start).Round(time.Second))
		pause()
	}
	pause()

	fault("kill a worker", func() {
		victim := strings.Fields(compose(t, "ps", "-q", "worker"))[0]
		docker(t, "kill", victim)
		time.Sleep(10 * time.Second)
		docker(t, "start", victim)
	})
	fault("kill the leader coordinator", func() {
		_, victim := leaderEventually(t, c)
		docker(t, "kill", victim)
		time.Sleep(10 * time.Second)
		docker(t, "start", victim)
	})
	fault("Postgres latency 150ms ± 100ms for 15s", func() {
		toxiproxy(t, http.MethodPost, "/proxies/postgres/toxics", map[string]any{
			"name": "latency", "type": "latency", "stream": "downstream",
			"attributes": map[string]int{"latency": 150, "jitter": 100},
		})
		time.Sleep(15 * time.Second)
		toxiproxy(t, http.MethodDelete, "/proxies/postgres/toxics/latency", nil)
	})
	fault("partition a worker from the network for 25s", func() {
		victim := strings.Fields(compose(t, "ps", "-q", "worker"))[1]
		network := networkOf(t, victim)
		docker(t, "network", "disconnect", network, victim)
		time.Sleep(25 * time.Second)
		docker(t, "network", "connect", network, victim)
		// Reconnecting may change its IP; a restart re-detects it.
		docker(t, "restart", "-t", "1", victim)
	})
	fault("cut coordinators and API off from Postgres for 8s", func() {
		toxiproxy(t, http.MethodPost, "/proxies/postgres", map[string]any{"enabled": false})
		time.Sleep(8 * time.Second)
		toxiproxy(t, http.MethodPost, "/proxies/postgres", map[string]any{"enabled": true})
	})
	fault("restart Postgres", func() {
		compose(t, "restart", "postgres")
	})
	fault("kill both coordinators", func() {
		victims := strings.Fields(compose(t, "ps", "-q", "coordinator"))
		for _, v := range victims {
			docker(t, "kill", v)
		}
		time.Sleep(5 * time.Second)
		for _, v := range victims {
			docker(t, "start", v)
		}
	})

	stop()
	<-done
	t.Logf("workload: %d tasks and %d workflows submitted; %d submissions needed retries",
		len(w.tasks), len(w.flows), w.retried.Load())
	if n := w.abandoned.Load(); n > 0 {
		t.Errorf("%d submissions never got through", n)
	}

	w.check(t, 6*time.Minute)
}

// sagaDefinition is a four-step diamond whose steps all have compensations.
// The failing variant's last step always fails, which must compensate the
// three steps before it.
func sagaDefinition(name string, fail bool) string {
	last := "echo shipped"
	retries := 10
	if fail {
		last, retries = "echo carrier down; exit 1", 0
	}
	return fmt.Sprintf(`
name: %s
defaults: {retries: 10, retry_delay: 1s, timeout: 30s}
steps:
  - {name: reserve, run: "sleep 0.5; echo reserved", compensate: "echo released"}
  - {name: charge, depends_on: [reserve], run: "sleep 1; echo charged", compensate: "echo refunded"}
  - {name: notify, depends_on: [reserve], run: "sleep 0.3; echo notified", compensate: "echo retracted"}
  - {name: ship, depends_on: [charge, notify], retries: %d, run: "%s"}
`, name, retries, last)
}

type workload struct {
	c              *client.Client
	name           string
	okDef, failDef string
	mu             sync.Mutex
	tasks          []string
	flows          map[string]bool // workflow ID → must fail
	retried        atomic.Int64
	abandoned      atomic.Int64
}

// run submits a task every 200ms and starts a workflow every 800ms (every
// fourth one failing) until ctx ends, then waits for submissions in flight.
func (w *workload) run(ctx context.Context, done chan<- struct{}) {
	defer close(done)
	var wg sync.WaitGroup
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()
	for i := 0; ; i++ {
		select {
		case <-ctx.Done():
			wg.Wait()
			return
		case <-tick.C:
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.submitTask(i)
		}()
		if i%4 == 0 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				w.startWorkflow(i / 4)
			}()
		}
	}
}

func (w *workload) submitTask(i int) {
	sleep := []string{"0.2", "1", "3"}[i%3]
	req := client.TaskRequest{
		Command:           fmt.Sprintf("sleep %s; echo %s-%d", sleep, w.name, i),
		MaxRetries:        client.Retries(10),
		RetryDelaySeconds: 1,
		TimeoutSeconds:    30,
		IdempotencyKey:    fmt.Sprintf("%s-task-%d", w.name, i),
	}
	w.retry(func(ctx context.Context) error {
		task, err := w.c.SubmitTask(ctx, req)
		if err == nil {
			w.mu.Lock()
			w.tasks = append(w.tasks, task.ID)
			w.mu.Unlock()
		}
		return err
	})
}

func (w *workload) startWorkflow(i int) {
	fail := i%4 == 3
	req := client.WorkflowRequest{Workflow: w.okDef, IdempotencyKey: fmt.Sprintf("%s-flow-%d", w.name, i)}
	if fail {
		req.Workflow = w.failDef
	}
	w.retry(func(ctx context.Context) error {
		wf, err := w.c.StartWorkflow(ctx, req)
		if err == nil {
			w.mu.Lock()
			w.flows[wf.ID] = fail
			w.mu.Unlock()
		}
		return err
	})
}

// retry calls fn until it succeeds, for up to three minutes: during a fault
// the API may be unreachable or fail. Idempotency keys make retries safe.
func (w *workload) retry(fn func(ctx context.Context) error) {
	deadline := time.Now().Add(3 * time.Minute)
	for n := 0; ; n++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := fn(ctx)
		cancel()
		if err == nil {
			if n > 0 {
				w.retried.Add(1)
			}
			return
		}
		if time.Now().After(deadline) {
			w.abandoned.Add(1)
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// check waits for every task and workflow to finish, then verifies the
// invariants.
func (w *workload) check(t *testing.T, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	ctx := func() context.Context { return ctxTimeout(t, 10*time.Second) }

	redispatched := 0
	for _, id := range w.tasks {
		task := pollUntil(t, deadline, func() (*client.Task, bool) {
			task, err := w.c.GetTask(ctx(), id)
			return task, err == nil && terminalTask(task.Status)
		})
		switch {
		case task == nil:
			t.Errorf("task %s is lost: it can no longer be read", id)
		case !terminalTask(task.Status):
			t.Errorf("task %s is stuck in %s (attempt %d)", id, task.Status, task.Attempt)
		case task.Status != "COMPLETED":
			t.Errorf("task %s ended %s: %s", id, task.Status, task.ErrorMessage)
		}
		if task != nil && task.Attempt > 1 {
			redispatched++
		}
	}

	for id, mustFail := range w.flows {
		wf := pollUntil(t, deadline, func() (*client.Workflow, bool) {
			wf, err := w.c.GetWorkflow(ctx(), id)
			return wf, err == nil && terminalWorkflow(wf)
		})
		switch {
		case wf == nil:
			t.Errorf("workflow %s is lost", id)
			continue
		case !terminalWorkflow(wf):
			t.Errorf("workflow %s is stuck in %s", id, wf.Status)
			continue
		}
		if !mustFail {
			if wf.Status != "COMPLETED" {
				t.Errorf("workflow %s ended %s, want COMPLETED: %s", id, wf.Status, wf.ErrorMessage)
			}
			for _, s := range wf.Steps {
				if s.Status != "COMPLETED" || s.CompensationTaskID != "" {
					t.Errorf("workflow %s step %s: %s (compensation %q); want COMPLETED and never compensated",
						id, s.Name, s.Status, s.CompensationTaskID)
				}
			}
			continue
		}
		if wf.Status != "FAILED" {
			t.Errorf("workflow %s ended %s, want FAILED", id, wf.Status)
		}
		for _, s := range wf.Steps {
			want, compensated := "COMPENSATED", true
			if s.Name == "ship" {
				want, compensated = "FAILED", false
			}
			if s.Status != want {
				t.Errorf("workflow %s step %s is %s, want %s", id, s.Name, s.Status, want)
			}
			if compensated && s.CompensationTaskStatus != "COMPLETED" {
				t.Errorf("workflow %s step %s: compensation task is %q, want COMPLETED", id, s.Name, s.CompensationTaskStatus)
			}
			if !compensated && s.CompensationTaskID != "" {
				t.Errorf("workflow %s: the failed step %s was compensated", id, s.Name)
			}
		}
	}
	t.Logf("all %d tasks and %d workflows checked; %d tasks were dispatched more than once",
		len(w.tasks), len(w.flows), redispatched)
}

// pollUntil calls get until it reports done or the deadline passes, and
// returns the last value it got.
func pollUntil[T any](t *testing.T, deadline time.Time, get func() (T, bool)) T {
	t.Helper()
	for {
		v, done := get()
		if done || time.Now().After(deadline) {
			return v
		}
		time.Sleep(time.Second)
	}
}

func terminalTask(status string) bool {
	return status == "COMPLETED" || status == "FAILED" || status == "CANCELLED"
}

func terminalWorkflow(wf *client.Workflow) bool {
	if wf.Status != "COMPLETED" && wf.Status != "FAILED" && wf.Status != "CANCELLED" {
		return false
	}
	// A failed run is finished once every compensation has.
	for _, s := range wf.Steps {
		if s.CompensationTaskID != "" && !terminalTask(s.CompensationTaskStatus) {
			return false
		}
	}
	return true
}

// leaderEventually is leader() for a cluster that may be mid-election.
func leaderEventually(t *testing.T, c *client.Client) (int64, string) {
	t.Helper()
	deadline := time.Now().Add(time.Minute)
	for {
		if cl, err := c.Cluster(ctxTimeout(t, 5*time.Second)); err == nil && cl.Leader != nil {
			return leader(t, c)
		}
		if time.Now().After(deadline) {
			t.Fatal("no leader for a minute")
		}
		time.Sleep(time.Second)
	}
}

// toxiproxy calls the Toxiproxy API.
func toxiproxy(t *testing.T, method, path string, body any) {
	t.Helper()
	if err := toxiproxyCall(method, path, body); err != nil {
		t.Fatalf("toxiproxy %s %s: %v (is the chaos overlay running?)", method, path, err)
	}
}

func toxiproxyCall(method, path string, body any) error {
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			return err
		}
	}
	req, err := http.NewRequest(method, env("E2E_TOXIPROXY_URL", "http://localhost:8474")+path, &buf)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("status %s", resp.Status)
	}
	return nil
}

// heal undoes every fault, whatever state the test stopped in.
func heal(t *testing.T) {
	_ = toxiproxyCall(http.MethodDelete, "/proxies/postgres/toxics/latency", nil)
	_ = toxiproxyCall(http.MethodPost, "/proxies/postgres", map[string]any{"enabled": true})
	args := []string{"compose"}
	for _, f := range strings.Split(env("E2E_COMPOSE_FILES", "docker-compose.yml"), ",") {
		args = append(args, "-f", repoRoot(t)+"/"+f)
	}
	ids, _ := exec.Command("docker", append(args, "ps", "-aq")...).Output()
	for _, id := range strings.Fields(string(ids)) {
		_ = exec.Command("docker", "start", id).Run()
	}
}
