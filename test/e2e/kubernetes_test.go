//go:build e2e

package e2e

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ChinmayNoob/conductor/pkg/client"
)

// Tests of Kubernetes-only features: k8s tasks and worker autoscaling. They
// run against the chart installed by scripts/kind-up.sh.

func kube(t *testing.T) kubePlatform {
	t.Helper()
	needKubernetes(t)
	return plat.(kubePlatform)
}

// fullname is the chart's name prefix for the release's resources.
func (k kubePlatform) fullname() string {
	if strings.Contains(k.release, "conductor") {
		return k.release
	}
	return k.release + "-conductor"
}

// taskPods lists the pods of a k8s task.
func (k kubePlatform) taskPods(t *testing.T, taskID string) []string {
	t.Helper()
	return strings.Fields(k.kubectl(t, "get", "pods", "-l", "conductor.dev/task-id="+taskID,
		"-o", "jsonpath={.items[*].metadata.name}"))
}

func (k kubePlatform) waitNoTaskPods(t *testing.T, taskID string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for len(k.taskPods(t, taskID)) > 0 {
		if time.Now().After(deadline) {
			t.Fatalf("task %s's pods were not deleted: %v", taskID, k.taskPods(t, taskID))
		}
		time.Sleep(time.Second)
	}
}

func k8sTask(spec client.K8sSpec) client.TaskRequest {
	return client.TaskRequest{Type: "k8s", K8s: &spec, TimeoutSeconds: 120, RetryDelaySeconds: 1}
}

func TestK8sTask(t *testing.T) {
	k := kube(t)
	c := newClient(t)
	marker := uniqueName("pod")

	task := submit(t, c, k8sTask(client.K8sSpec{
		Image:   "alpine:3.20",
		Command: []string{"sh", "-c", `echo "$1 says hi"; id -u; echo "greeting=hello $1" >> "$CONDUCTOR_OUTPUT"`, "sh", marker},
		Memory:  "32Mi",
		CPU:     "100m",
	}))
	task = waitTask(t, c, task.ID, 3*time.Minute)
	if task.Status != "COMPLETED" {
		t.Fatalf("task %s: %s\n%s", task.Status, task.ErrorMessage, task.Output)
	}
	if !strings.Contains(task.Output, marker+" says hi") || !strings.Contains(task.Output, "65534") {
		t.Errorf("output %q: want the greeting, from user 65534", task.Output)
	}
	if got := task.Outputs["greeting"]; got != "hello "+marker {
		t.Errorf("outputs = %v", task.Outputs)
	}
	k.waitNoTaskPods(t, task.ID)
}

func TestK8sTaskFailureAndOutputsInWorkflow(t *testing.T) {
	kube(t)
	c := newClient(t)
	name := uniqueName("k8s-flow")
	applyDefinition(t, c, fmt.Sprintf(`
name: %s
inputs:
  release: {required: true}
steps:
  - name: build
    type: k8s
    k8s:
      image: alpine:3.20
      command: [sh, -c, 'echo "version=$1-built" >> "$CONDUCTOR_OUTPUT"', sh, '${{ inputs.release }}']
  - name: announce
    depends_on: [build]
    run: echo "shipping $VERSION"
    env: {VERSION: '${{ steps.build.outputs.version }}'}
`, name))
	wf := waitWorkflow(t, c, startWorkflow(t, c, name, map[string]any{"release": "v1; rm -rf /"}).ID, 3*time.Minute)
	if wf.Status != "COMPLETED" {
		t.Fatalf("workflow %s: %s", wf.Status, wf.ErrorMessage)
	}
	announce := waitTask(t, c, wf.Step("announce").TaskID, time.Minute)
	if !strings.Contains(announce.Output, "shipping v1; rm -rf /-built") {
		t.Fatalf("announce output %q", announce.Output)
	}

	failing := waitTask(t, c, submit(t, c, client.TaskRequest{
		Type: "k8s", K8s: &client.K8sSpec{Image: "alpine:3.20", Command: []string{"sh", "-c", "echo oops; exit 3"}},
		MaxRetries: client.Retries(0),
	}).ID, 2*time.Minute)
	if failing.Status != "FAILED" || !strings.Contains(failing.ErrorMessage, "status 3") || !strings.Contains(failing.Output, "oops") {
		t.Fatalf("failing task = %s (%s), output %q", failing.Status, failing.ErrorMessage, failing.Output)
	}
}

func TestK8sTaskServiceAccounts(t *testing.T) {
	kube(t)
	c := newClient(t)
	probe := `test -e /var/run/secrets/kubernetes.io/serviceaccount/token && echo token=mounted || echo token=none`

	// By default the pod gets no token.
	plain := waitTask(t, c, submit(t, c, k8sTask(client.K8sSpec{Image: "alpine:3.20", Command: []string{"sh", "-c", probe}})).ID, 2*time.Minute)
	if plain.Status != "COMPLETED" || !strings.Contains(plain.Output, "token=none") {
		t.Fatalf("default account: %s, output %q", plain.Status, plain.Output)
	}
	// An allowed account is mounted.
	reader := waitTask(t, c, submit(t, c, k8sTask(client.K8sSpec{
		Image: "alpine:3.20", Command: []string{"sh", "-c", probe}, ServiceAccount: "conductor-e2e-reader",
	})).ID, 2*time.Minute)
	if reader.Status != "COMPLETED" || !strings.Contains(reader.Output, "token=mounted") {
		t.Fatalf("allowed account: %s, output %q", reader.Status, reader.Output)
	}
	// Any other is refused for good: no retries.
	refused := waitTask(t, c, submit(t, c, k8sTask(client.K8sSpec{
		Image: "alpine:3.20", Command: []string{"true"}, ServiceAccount: "default",
	})).ID, 2*time.Minute)
	if refused.Status != "FAILED" || refused.RetryCount != 0 || !strings.Contains(refused.ErrorMessage, "not allowed") {
		t.Fatalf("other account: %s after %d retries: %s", refused.Status, refused.RetryCount, refused.ErrorMessage)
	}
}

func TestK8sTaskCancelDeletesPod(t *testing.T) {
	k := kube(t)
	c := newClient(t)
	task := submit(t, c, k8sTask(client.K8sSpec{Image: "alpine:3.20", Command: []string{"sleep", "300"}}))

	deadline := time.Now().Add(2 * time.Minute)
	for len(k.taskPods(t, task.ID)) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("no pod was created for the task")
		}
		time.Sleep(500 * time.Millisecond)
	}
	if _, err := c.CancelTask(ctxTimeout(t, 30*time.Second), task.ID); err != nil {
		t.Fatal(err)
	}
	k.waitNoTaskPods(t, task.ID)
}

// TestWorkersScaleOnQueueDepth: the dedicated burst pool (values-e2e.yaml)
// sits at zero workers, KEDA scales it up for tasks that ask for it, and
// back to zero when they're done. Other tasks never run there.
func TestWorkersScaleOnQueueDepth(t *testing.T) {
	k := kube(t)
	c := newClient(t)
	deploy := k.fullname() + "-worker-burst"
	replicas := func() int {
		n, _ := strconv.Atoi(k.kubectl(t, "get", "deployment", deploy, "-o", "jsonpath={.status.readyReplicas}"))
		return n
	}
	waitReplicas := func(ok func(int) bool, within time.Duration, what string) {
		t.Helper()
		deadline := time.Now().Add(within)
		for !ok(replicas()) {
			if time.Now().After(deadline) {
				t.Fatalf("%s: the burst pool has %d ready workers after %v", what, replicas(), within)
			}
			time.Sleep(2 * time.Second)
		}
	}
	waitReplicas(func(n int) bool { return n == 0 }, 3*time.Minute, "idle at the start")

	// 6 tasks, 2 slots per worker: KEDA wants 3 workers.
	start := time.Now()
	var tasks []*client.Task
	for i := range 6 {
		tasks = append(tasks, submit(t, c, client.TaskRequest{
			Command: fmt.Sprintf("sleep 20; echo burst-%d", i), Labels: map[string]string{"pool": "burst"},
		}))
	}
	plain := submit(t, c, client.TaskRequest{Command: "sleep 2; echo plain"})
	waitReplicas(func(n int) bool { return n >= 2 }, 2*time.Minute, "under load")
	t.Logf("burst pool scaled from 0 to %d workers in %v", replicas(), time.Since(start).Round(time.Second))

	var done []*client.Task
	for _, task := range append(tasks, plain) {
		task = waitTask(t, c, task.ID, 3*time.Minute)
		if task.Status != "COMPLETED" {
			t.Fatalf("task %s: %s", task.ID, task.ErrorMessage)
		}
		done = append(done, task)
	}
	ws, err := c.ListWorkers(ctxTimeout(t, 10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	pool := map[int64]string{}
	for _, w := range ws {
		pool[w.ID] = w.Labels["pool"]
	}
	for _, task := range done {
		want := "burst"
		if task.ID == plain.ID {
			want = "default"
		}
		if got := pool[*task.WorkerID]; got != want {
			t.Errorf("task %q ran in the %q pool, want %s", task.Command, got, want)
		}
	}

	waitReplicas(func(n int) bool { return n == 0 }, 3*time.Minute, "after the burst")
	t.Logf("and back to 0 %v after starting", time.Since(start).Round(time.Second))
}
