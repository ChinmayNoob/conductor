//go:build e2e

package e2e

import (
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/ChinmayNoob/conductor/pkg/client"
)

// apply applies manifests with kubectl in the release's namespace.
func (k kubePlatform) apply(t *testing.T, manifests string) {
	t.Helper()
	cmd := exec.Command("kubectl", "--context", k.context, "-n", k.namespace, "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(manifests)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("kubectl apply: %v\n%s", err, out)
	}
}

// waitFor polls a kubectl jsonpath until it prints want.
func (k kubePlatform) waitFor(t *testing.T, resource, jsonpath, want string, within time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		got, _ := k.try("get", resource, "-o", "jsonpath="+jsonpath)
		if strings.TrimSpace(got) == want {
			return got
		}
		if time.Now().After(deadline) {
			status, _ := k.try("get", resource, "-o", "jsonpath={.status}")
			t.Fatalf("%s %s is %q after %v, want %q; status: %s", resource, jsonpath, got, within, want, status)
		}
		time.Sleep(time.Second)
	}
}

// TestOperatorManagesWorkflowsRunsAndSchedules applies a Workflow, a
// WorkflowRun of it and a Schedule, the GitOps way, and checks that
// Conductor follows: the definition is applied, the run completes, and the
// schedule follows the resource through a change and its deletion.
func TestOperatorManagesWorkflowsRunsAndSchedules(t *testing.T) {
	k := kube(t)
	// Resources land in the Conductor namespace named after their own.
	c := newClient(t).WithNamespace(k.namespace)
	name := uniqueName("gitops")
	t.Cleanup(func() {
		_, _ = k.try("delete", "workflowrun,schedule,workflow", "-l", "e2e="+name, "--wait=false")
	})

	// A run applied with its workflow: it waits for the definition.
	k.apply(t, fmt.Sprintf(`
apiVersion: conductor.dev/v1alpha1
kind: WorkflowRun
metadata: {name: %[1]s-1, labels: {e2e: %[1]s}}
spec:
  workflow: %[1]s
  input: {who: kubernetes}
---
apiVersion: conductor.dev/v1alpha1
kind: Workflow
metadata: {name: %[1]s, labels: {e2e: %[1]s}}
spec:
  definition:
    inputs: {who: {required: true}}
    steps:
      - name: greet
        run: echo "hello $WHO"
        env: {WHO: '${{ inputs.who }}'}
---
apiVersion: conductor.dev/v1alpha1
kind: Schedule
metadata: {name: %[1]s, labels: {e2e: %[1]s}}
spec:
  cron: "0 3 * * *"
  timezone: Europe/Berlin
  workflow: {name: %[1]s, input: {who: cron}}
`, name))

	k.waitFor(t, "workflow/"+name, `{.status.conditions[?(@.type=="Ready")].status}`, "True", time.Minute)
	k.waitFor(t, "workflowrun/"+name+"-1", "{.status.phase}", "COMPLETED", 2*time.Minute)
	runID := strings.TrimSpace(k.kubectl(t, "get", "workflowrun/"+name+"-1", "-o", "jsonpath={.status.runID}"))
	wf, err := c.GetWorkflow(ctxTimeout(t, 10*time.Second), runID)
	if err != nil {
		t.Fatal(err)
	}
	greet := waitTask(t, c, wf.Step("greet").TaskID, time.Minute)
	if !strings.Contains(greet.Output, "hello kubernetes") {
		t.Fatalf("greet output %q", greet.Output)
	}

	k.waitFor(t, "schedule/"+name, `{.status.conditions[?(@.type=="Ready")].status}`, "True", time.Minute)
	sch, err := c.GetSchedule(ctxTimeout(t, 10*time.Second), name)
	if err != nil || !sch.Enabled || sch.Timezone != "Europe/Berlin" {
		t.Fatalf("schedule in Conductor = %+v, %v", sch, err)
	}

	// Suspending the resource pauses the schedule.
	k.kubectl(t, "patch", "schedule/"+name, "--type=merge", "-p", `{"spec": {"suspend": true}}`)
	waitSchedule(t, c, name, func(s *client.Schedule, err error) bool { return err == nil && !s.Enabled }, "paused")

	// Deleting it deletes the schedule.
	k.kubectl(t, "delete", "schedule/"+name)
	waitSchedule(t, c, name, func(_ *client.Schedule, err error) bool { return client.IsNotFound(err) }, "deleted")
}

func waitSchedule(t *testing.T, c *client.Client, name string, ok func(*client.Schedule, error) bool, what string) {
	t.Helper()
	deadline := time.Now().Add(time.Minute)
	for {
		s, err := c.GetSchedule(ctxTimeout(t, 10*time.Second), name)
		if ok(s, err) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("schedule %s not %s in Conductor: %+v, %v", name, what, s, err)
		}
		time.Sleep(time.Second)
	}
}
