//go:build e2e

package e2e

import (
	"strings"
	"testing"
	"time"

	"github.com/ChinmayNoob/conductor/pkg/client"
)

// These tests stop and kill worker containers, so they run last (Go runs test
// files in name order). Each restarts the container it stopped.

func TestGracefulDrainFinishesRunningTask(t *testing.T) {
	c := newClient(t)
	marker := uniqueName("drain")

	task := submit(t, c, client.TaskRequest{Command: "sleep 6; echo " + marker})
	container := workerRunning(t, marker)
	restartLater(t, container)

	docker(t, "stop", "-t", "30", container) // SIGTERM, then wait for the drain

	task = waitTask(t, c, task.ID, 2*time.Minute)
	if task.Status != "COMPLETED" || task.RetryCount != 0 || !strings.Contains(task.Output, marker) {
		t.Fatalf("task = %s with %d retries and output %q; want COMPLETED on the first attempt",
			task.Status, task.RetryCount, task.Output)
	}
}

func TestWorkerCrashIsRecovered(t *testing.T) {
	c := newClient(t)
	marker := uniqueName("crash")

	task := submit(t, c, client.TaskRequest{
		Command:           "sleep 15; echo " + marker,
		RetryDelaySeconds: 1,
		TimeoutSeconds:    60,
	})
	container := workerRunning(t, marker)
	restartLater(t, container)

	docker(t, "kill", container) // no chance to drain

	task = waitTask(t, c, task.ID, 3*time.Minute)
	if task.Status != "COMPLETED" || task.RetryCount != 1 {
		t.Fatalf("task = %s with %d retries; want COMPLETED after 1 retry", task.Status, task.RetryCount)
	}
}
