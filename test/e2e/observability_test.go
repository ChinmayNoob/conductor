//go:build e2e

package e2e

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ChinmayNoob/conductor/pkg/client"
)

// scrape fetches a container's Prometheus metrics from inside it.
func scrape(t *testing.T, container string) string {
	t.Helper()
	return docker(t, "exec", container, "wget", "-qO-", "http://localhost:9090/metrics")
}

func wantMetric(t *testing.T, who, metrics, prefix string) {
	t.Helper()
	for _, line := range strings.Split(metrics, "\n") {
		if strings.HasPrefix(line, prefix) {
			return
		}
	}
	t.Errorf("%s exposes no %s", who, prefix)
}

func TestMetrics(t *testing.T) {
	c := newClient(t)
	task := submit(t, c, client.TaskRequest{Command: "echo metrics"})
	if task = waitTask(t, c, task.ID, time.Minute); task.Status != "COMPLETED" {
		t.Fatalf("task %s", task.Status)
	}

	_, leaderContainer := leader(t, c)
	m := scrape(t, leaderContainer)
	for _, prefix := range []string{
		"conductor_leader 1",
		`conductor_task_results_total{namespace="default",queue="default",result="completed"}`,
		`conductor_tasks_dispatched_total{namespace="default",queue="default"}`,
		"conductor_dispatch_latency_seconds_count",
		"conductor_task_turnaround_seconds_count",
		`conductor_workers{state="healthy"}`,
		`conductor_slots{state="total"}`,
		"go_goroutines",
	} {
		wantMetric(t, "the leader", m, prefix)
	}

	// The standby says so, and leaves cluster state to the leader.
	for _, id := range strings.Fields(compose(t, "ps", "-q", "coordinator")) {
		if id == leaderContainer {
			continue
		}
		m := scrape(t, id)
		wantMetric(t, "the standby", m, "conductor_leader 0")
		if strings.Contains(m, "conductor_workers{") {
			t.Error("a standby reports worker counts")
		}
	}

	wantMetric(t, "a worker", scrape(t, strings.Fields(compose(t, "ps", "-q", "worker"))[0]), "conductor_worker_slots")
	wantMetric(t, "the API", scrape(t, strings.TrimSpace(compose(t, "ps", "-q", "api"))),
		`conductor_http_requests_total{code="201",route="POST /v1/tasks"}`)
}

// TestTracing needs tracing on (OTEL_EXPORTER_OTLP_ENDPOINT); it checks the
// whole trace in Jaeger when E2E_JAEGER_URL is set.
func TestTracing(t *testing.T) {
	c := newClient(t)
	task := submit(t, c, client.TaskRequest{Command: `echo "traceparent=$TRACEPARENT"`})
	task = waitTask(t, c, task.ID, time.Minute)
	if task.TraceID == "" {
		t.Skip("tracing is off; set OTEL_EXPORTER_OTLP_ENDPOINT")
	}
	// User code sees a traceparent in the task's own trace.
	if !strings.Contains(task.Output, "traceparent=00-"+task.TraceID+"-") {
		t.Fatalf("output %q: TRACEPARENT is not in trace %s", task.Output, task.TraceID)
	}

	jaeger := env("E2E_JAEGER_URL", "")
	if jaeger == "" {
		return
	}
	want := map[string]bool{
		"conductor-api/POST /v1/tasks":                                      false,
		"conductor-coordinator/dispatch":                                    false,
		"conductor-worker/run shell":                                        false,
		"conductor-coordinator/grpcapi.CoordinatorService/UpdateTaskStatus": false,
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		for _, s := range jaegerSpans(t, jaeger, task.TraceID) {
			if _, ok := want[s]; ok {
				want[s] = true
			}
		}
		missing := 0
		for _, found := range want {
			if !found {
				missing++
			}
		}
		if missing == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("trace %s is missing spans: %v", task.TraceID, want)
		}
		time.Sleep(time.Second)
	}
}

// jaegerSpans returns "service/operation" for every span in a trace.
func jaegerSpans(t *testing.T, jaeger, traceID string) []string {
	t.Helper()
	resp, err := http.Get(jaeger + "/api/traces/" + traceID)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body struct {
		Data []struct {
			Spans []struct {
				OperationName string `json:"operationName"`
				ProcessID     string `json:"processID"`
			} `json:"spans"`
			Processes map[string]struct {
				ServiceName string `json:"serviceName"`
			} `json:"processes"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil // not indexed yet
	}
	var out []string
	for _, tr := range body.Data {
		for _, s := range tr.Spans {
			out = append(out, tr.Processes[s.ProcessID].ServiceName+"/"+s.OperationName)
		}
	}
	return out
}

// lockedBuffer collects streamed output from another goroutine.
type lockedBuffer struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestFollowLogsLive(t *testing.T) {
	c := newClient(t)
	task := submit(t, c, client.TaskRequest{Command: "for i in 1 2 3 4 5 6; do echo line-$i; sleep 1; done"})

	var out lockedBuffer
	done := make(chan error, 1)
	go func() { done <- c.TaskLogs(ctxTimeout(t, time.Minute), task.ID, true, &out) }()

	// The first line must arrive while the task is still running.
	deadline := time.Now().Add(15 * time.Second)
	for !strings.Contains(out.String(), "line-1") {
		if time.Now().After(deadline) {
			t.Fatalf("no live output after 15s: %q", out.String())
		}
		time.Sleep(100 * time.Millisecond)
	}
	if got, err := c.GetTask(ctxTimeout(t, 5*time.Second), task.ID); err != nil || got.Status == "COMPLETED" {
		t.Fatalf("output arrived only after the task finished (status %v, err %v)", got.Status, err)
	}

	if err := <-done; err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 6; i++ {
		if n := strings.Count(out.String(), fmt.Sprintf("line-%d\n", i)); n != 1 {
			t.Fatalf("line-%d appears %d times in the followed output:\n%s", i, n, out.String())
		}
	}
}

func TestAttemptsAndLogsAcrossRetries(t *testing.T) {
	c := newClient(t)
	task := submit(t, c, client.TaskRequest{
		Command:           `echo "try $CONDUCTOR_ATTEMPT"; sleep 1; [ "$CONDUCTOR_RETRY" -ge 1 ] || { echo boom >&2; exit 3; }`,
		MaxRetries:        client.Retries(1),
		RetryDelaySeconds: 1,
	})
	var out lockedBuffer
	if err := c.TaskLogs(ctxTimeout(t, time.Minute), task.ID, true, &out); err != nil {
		t.Fatal(err)
	}
	if task = waitTask(t, c, task.ID, time.Minute); task.Status != "COMPLETED" {
		t.Fatalf("task %s", task.Status)
	}

	attempts, err := c.TaskAttempts(ctxTimeout(t, 10*time.Second), task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(attempts) != 2 || attempts[0].Status != "FAILED" || !strings.Contains(attempts[0].Error, "exit status 3") ||
		!strings.Contains(attempts[0].Output, "boom") || attempts[1].Status != "COMPLETED" || !attempts[1].Current {
		t.Fatalf("attempts = %+v", attempts)
	}
	logs := out.String()
	if !strings.Contains(logs, "try 1") || !strings.Contains(logs, "--- attempt 2 ---") || !strings.Contains(logs, "try 2") {
		t.Fatalf("followed logs don't show both attempts:\n%s", logs)
	}
}
