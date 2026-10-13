//go:build e2e

package e2e

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ChinmayNoob/conductor/pkg/client"
)

// TestRollingRestartLosesNothing restarts every component the way an
// upgrade does (kubectl rollout restart) while tasks and workflows run.
// Nothing may be lost, and the API must keep answering throughout.
func TestRollingRestartLosesNothing(t *testing.T) {
	k := kube(t)
	c := newClient(t)

	var tasks []*client.Task
	for i := range 12 {
		tasks = append(tasks, submit(t, c, client.TaskRequest{Command: fmt.Sprintf("sleep 4; echo rollout-%d", i), RetryDelaySeconds: 1}))
	}
	var wfs []*client.Workflow
	for range 3 {
		wfs = append(wfs, startWorkflow(t, c, "order_pipeline", map[string]any{"order_id": uniqueName("rollout")}))
	}

	// Poll the API all along.
	var requests, failures atomic.Int64
	var lastErr atomic.Value
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			case <-time.After(200 * time.Millisecond):
			}
			requests.Add(1)
			if _, err := c.GetTask(ctxTimeout(t, 10*time.Second), tasks[0].ID); err != nil {
				failures.Add(1)
				lastErr.Store(err.Error())
			}
		}
	}()

	start := time.Now()
	for _, component := range []string{"coordinator", "worker", "api"} {
		k.kubectl(t, "rollout", "restart", "deployment", k.fullname()+"-"+component)
	}
	k.kubectl(t, "rollout", "status", "deployment", "-l", "app.kubernetes.io/instance="+k.release, "--timeout=5m")
	t.Logf("rolled every component in %v", time.Since(start).Round(time.Second))
	time.Sleep(5 * time.Second)
	close(stop)
	wg.Wait()

	for _, task := range tasks {
		if task = waitTask(t, c, task.ID, 3*time.Minute); task.Status != "COMPLETED" {
			t.Errorf("task %s ended %s: %s", task.ID, task.Status, task.ErrorMessage)
		}
	}
	for _, wf := range wfs {
		if wf = waitWorkflow(t, c, wf.ID, 3*time.Minute); wf.Status != "COMPLETED" {
			t.Errorf("workflow %s ended %s: %s", wf.ID, wf.Status, wf.ErrorMessage)
		}
	}
	if n := failures.Load(); n > 0 {
		t.Errorf("%d of %d API requests failed during the rollout; last: %v", n, requests.Load(), lastErr.Load())
	}
}
