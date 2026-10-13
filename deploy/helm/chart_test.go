// Package helm tests the Helm chart: its copies of files kept elsewhere in
// the repository, and (when helm is installed) how it renders.
package helm

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The chart can only read files inside it, so it keeps copies. They must
// match the originals.
func TestChartCopiesAreCurrent(t *testing.T) {
	copies := map[string]string{
		"conductor/files/alerts.yml":     "../../observability/alerts.yml",
		"conductor/files/conductor.json": "../../observability/grafana/dashboards/conductor.json",
	}
	crds, _ := filepath.Glob("../../operator/config/crd/*.yaml")
	for _, src := range crds {
		copies["conductor/files/crds/"+filepath.Base(src)] = src
	}
	for dst, src := range copies {
		want, err := os.ReadFile(src)
		if err != nil {
			t.Fatal(err)
		}
		got, err := os.ReadFile(dst)
		if err != nil {
			t.Errorf("%s: %v (copy it from %s)", dst, err, src)
			continue
		}
		if !bytes.Equal(bytes.ReplaceAll(got, []byte("\r\n"), []byte("\n")), bytes.ReplaceAll(want, []byte("\r\n"), []byte("\n"))) {
			t.Errorf("%s differs from %s; copy it again", dst, src)
		}
	}
}

func render(t *testing.T, args ...string) string {
	t.Helper()
	helm, err := exec.LookPath("helm")
	if err != nil {
		t.Skip("helm is not installed")
	}
	out, err := exec.Command(helm, append([]string{"template", "t", "conductor"}, args...)...).CombinedOutput()
	if err != nil {
		t.Fatalf("helm template %v: %v\n%s", args, err, out)
	}
	return string(out)
}

func TestChartRenders(t *testing.T) {
	cases := map[string][]string{
		"defaults":      nil,
		"e2e on kind":   {"-f", "../kind/values-e2e.yaml"},
		"cloudnativepg": {"--set", "postgres.mode=cloudnativepg"},
		"external": {"--set", "postgres.mode=external", "--set", "postgres.external.host=db.example.com",
			"--set", "postgres.external.existingSecret=db"},
		"everything": {"--set", "kubernetesTasks.enabled=true", "--set", "worker.autoscaling.enabled=true",
			"--set", "workerPools.gpu.dedicated=true", "--set", "workerPools.gpu.labels.gpu=true",
			"--set", "operator.enabled=true", "--set", "metrics.serviceMonitor.enabled=true",
			"--set", "metrics.prometheusRule.enabled=true", "--set", "metrics.grafanaDashboard.enabled=true",
			"--set", "tls.enabled=false", "--set", "api.ingress.enabled=true"},
	}
	for name, args := range cases {
		t.Run(name, func(t *testing.T) {
			out := render(t, args...)
			if strings.Contains(out, "%!") || strings.Contains(out, "<no value>") {
				t.Fatalf("rendering left a formatting error:\n%s", out)
			}
		})
	}
}

func TestDedicatedPoolScalesOnItsOwnTasks(t *testing.T) {
	out := render(t, "--set", "workerPools.gpu.dedicated=true", "--set", "workerPools.gpu.autoscaling.enabled=true",
		"--set", "workerPools.gpu.labels.gpu=true")
	for _, want := range []string{
		`value: "gpu=true,pool=gpu"`,
		"name: CONDUCTOR_WORKER_EXCLUSIVE\n              value: pool",
		`conductor_slot_demand('{\"gpu\":\"true\",\"pool\":\"gpu\",\"type.http\":\"true\",\"type.shell\":\"true\"}'::jsonb, '{\"pool\":\"gpu\"}'::jsonb)`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("rendered chart lacks %s", want)
		}
	}
}

func TestBadDatabaseSettingsFailClearly(t *testing.T) {
	helm, err := exec.LookPath("helm")
	if err != nil {
		t.Skip("helm is not installed")
	}
	out, err := exec.Command(helm, "template", "t", "conductor", "--set", "postgres.mode=external").CombinedOutput()
	if err == nil || !strings.Contains(string(out), "postgres.external.host is required") {
		t.Fatalf("external without a host: %v\n%s", err, out)
	}
}

// Shell tasks run as their worker's user, so only the pool that runs k8s
// tasks, and nothing else, may hold Kubernetes credentials.
func TestOnlyTheK8sPoolHoldsCredentials(t *testing.T) {
	out := render(t, "--set", "kubernetesTasks.enabled=true", "--set", "workerPools.gpu.dedicated=true")
	if n := strings.Count(out, "automountServiceAccountToken: true"); n != 1 {
		t.Fatalf("%d pod specs mount a token, want only the k8s pool's", n)
	}
	if !strings.Contains(out, "serviceAccountName: t-conductor-task-runner\n      automountServiceAccountToken: true") {
		t.Fatal("the k8s pool doesn't run as the task-runner account")
	}

	helm, _ := exec.LookPath("helm")
	mixed, err := exec.Command(helm, "template", "t", "conductor", "--set", "kubernetesTasks.enabled=true",
		"--set", "workerPools.mixed.taskTypes={k8s,shell}").CombinedOutput()
	if err == nil || !strings.Contains(string(mixed), "can't also run shell or container tasks") {
		t.Fatalf("a pool running k8s and shell tasks was accepted: %v\n%s", err, mixed)
	}
}

func TestChartRules(t *testing.T) {
	helm, err := exec.LookPath("helm")
	if err != nil {
		t.Skip("helm is not installed")
	}
	// Two pools that take any task would both scale for the same tasks.
	out, err := exec.Command(helm, "template", "t", "conductor", "--set", "worker.autoscaling.enabled=true",
		"--set", "workerPools.highmem.autoscaling.enabled=true").CombinedOutput()
	if err == nil || !strings.Contains(string(out), "make all but one dedicated") {
		t.Errorf("two generic autoscaled pools were accepted: %v\n%s", err, out)
	}

	// "false" as a string is not dedicated, for the workers and KEDA alike.
	rendered := render(t, "--set-string", "workerPools.batch.dedicated=false", "--set", "workerPools.batch.autoscaling.enabled=true")
	if strings.Contains(rendered, "CONDUCTOR_WORKER_EXCLUSIVE") || !strings.Contains(rendered, `'{}'::jsonb`) {
		t.Error(`dedicated: "false" made a dedicated pool`)
	}

	// Restricting the API keeps the chart's own callers in.
	rendered = render(t, "--set", "operator.enabled=true", "--set", "networkPolicy.apiFrom[0].ipBlock.cidr=10.0.0.0/8",
		"-s", "templates/networkpolicies.yaml")
	want := "            cidr: 10.0.0.0/8\n" +
		"        # The chart's own callers: the operator and helm test.\n" +
		"        - podSelector:\n            matchLabels:\n              app.kubernetes.io/name: conductor\n" +
		"              app.kubernetes.io/instance: t\n              app.kubernetes.io/component: operator\n" +
		"        - podSelector:\n            matchLabels:\n              app.kubernetes.io/name: conductor\n" +
		"              app.kubernetes.io/instance: t\n              app.kubernetes.io/component: test\n"
	if !strings.Contains(rendered, want) {
		t.Errorf("the API's NetworkPolicy doesn't admit the operator and helm test:\n%s", rendered)
	}
}
