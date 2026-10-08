//go:build e2e

package e2e

import (
	"fmt"
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

func TestLeaderFailover(t *testing.T) {
	c := newClient(t)
	epoch, victim := leader(t, c)
	restartLater(t, victim)

	wf := startWorkflow(t, c, "order_pipeline", map[string]any{"order_id": uniqueName("ha")})
	var tasks []*client.Task
	for i := range 8 {
		tasks = append(tasks, submit(t, c, client.TaskRequest{Command: fmt.Sprintf("sleep 3; echo ha-%d", i)}))
	}
	time.Sleep(time.Second)

	killed := time.Now()
	docker(t, "kill", victim)

	// A standby must take over quickly.
	var newEpoch int64
	for newEpoch <= epoch {
		if time.Since(killed) > 15*time.Second {
			t.Fatalf("no new leader 15s after killing the old one (epoch still %d)", epoch)
		}
		time.Sleep(200 * time.Millisecond)
		if cl, err := c.Cluster(ctxTimeout(t, 5*time.Second)); err == nil && cl.Leader != nil {
			newEpoch = cl.Leader.Epoch
		}
	}
	t.Logf("new leader (epoch %d) elected %v after the kill", newEpoch, time.Since(killed).Round(100*time.Millisecond))

	// Nothing may be lost: every task and the workflow finish successfully.
	for _, task := range tasks {
		task = waitTask(t, c, task.ID, 2*time.Minute)
		if task.Status != "COMPLETED" {
			t.Fatalf("task %s ended %s: %s", task.ID, task.Status, task.ErrorMessage)
		}
		// Results must arrive promptly, not after a long client timeout.
		if lag := task.CompletedAt.Sub(*task.StartedAt) - 3*time.Second; lag > 10*time.Second {
			t.Errorf("task %s result arrived %v after it finished", task.ID, lag.Round(100*time.Millisecond))
		}
	}
	if wf = waitWorkflow(t, c, wf.ID, 2*time.Minute); wf.Status != "COMPLETED" {
		t.Fatalf("workflow %s: %s", wf.Status, wf.ErrorMessage)
	}
}

func TestPartitionedWorkerIsFenced(t *testing.T) {
	c := newClient(t)
	marker := uniqueName("partition")

	// The task reports which attempt produced its output.
	task := submit(t, c, client.TaskRequest{
		Command:           `sleep 40; echo "` + marker + ` attempt=$CONDUCTOR_ATTEMPT"`,
		RetryDelaySeconds: 1,
		TimeoutSeconds:    120,
	})
	container := workerRunning(t, marker)
	network := networkOf(t, container)

	// Cut the worker off. The coordinator stops hearing from it, declares it
	// dead and retries the task elsewhere as attempt 2. Meanwhile attempt 1
	// keeps running on the isolated worker.
	// Restart it at the end so it re-detects its IP, which reconnecting may
	// have changed. (Cleanups run last-registered first, so this runs after
	// the reconnect below.)
	t.Cleanup(func() {
		docker(t, "restart", "-t", "1", container)
		time.Sleep(3 * time.Second)
	})
	docker(t, "network", "disconnect", network, container)
	reconnected := false
	t.Cleanup(func() {
		if !reconnected {
			docker(t, "network", "connect", network, container)
		}
	})

	deadline := time.Now().Add(90 * time.Second)
	for {
		got, err := c.GetTask(ctxTimeout(t, 10*time.Second), task.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got.Attempt >= 2 && got.Status == "STARTED" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the task was not retried after its worker was partitioned: %+v", got)
		}
		time.Sleep(time.Second)
	}

	// Heal the partition. The old worker has finished attempt 1 by now (or
	// soon will) and reports it; that report must be ignored.
	docker(t, "network", "connect", network, container)
	reconnected = true

	task = waitTask(t, c, task.ID, 3*time.Minute)
	if task.Status != "COMPLETED" || task.Attempt != 2 {
		t.Fatalf("task = %s at attempt %d, want COMPLETED by attempt 2", task.Status, task.Attempt)
	}
	if !strings.Contains(task.Output, "attempt=2") {
		t.Fatalf("output %q: the stale attempt's report was accepted", task.Output)
	}
}
