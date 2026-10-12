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
