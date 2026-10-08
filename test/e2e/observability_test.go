//go:build e2e

package e2e

import (
	"strings"
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
