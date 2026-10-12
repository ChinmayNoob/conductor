//go:build e2e

package e2e

import (
	"os/exec"
	"strings"
	"testing"
	"time"
)

// platform is where the stack under test runs: Docker Compose (the
// default) or Kubernetes (E2E_PLATFORM=kubernetes; see scripts/kind-up.sh).
// Instances are containers or pods.
type platform interface {
	name() string
	// instances lists the running instances of a component: coordinator,
	// api or worker (workers of the default pool on Kubernetes).
	instances(t *testing.T, component string) []string
	exec(t *testing.T, id string, args ...string) (string, error)
	processes(t *testing.T, id string) string
	ip(t *testing.T, id string) string
	// kill stops an instance without letting it clean up; stop sends
	// SIGTERM and waits up to grace for it to exit.
	kill(t *testing.T, id string)
	stop(t *testing.T, id string, grace time.Duration)
	// restoreLater brings the stack back to full strength when the test ends.
	restoreLater(t *testing.T, id string)
	// internalAPI is the API's base URL as tasks see it.
	internalAPI() string
}

var plat = newPlatform()

func newPlatform() platform {
	if env("E2E_PLATFORM", "compose") == "kubernetes" {
		return kubePlatform{
			context:   env("E2E_K8S_CONTEXT", "kind-conductor"),
			namespace: env("E2E_K8S_NAMESPACE", "conductor"),
			release:   env("E2E_K8S_RELEASE", "conductor"),
		}
	}
	return composePlatform{}
}

func onKubernetes() bool { return plat.name() == "kubernetes" }

// needCompose skips tests that rely on Docker Compose (networks, its Postgres).
func needCompose(t *testing.T, why string) {
	t.Helper()
	if onKubernetes() {
		t.Skip("needs Docker Compose: " + why)
	}
}

func needKubernetes(t *testing.T) {
	t.Helper()
	if !onKubernetes() {
		t.Skip("needs Kubernetes: set E2E_PLATFORM=kubernetes (see scripts/kind-up.sh)")
	}
}

// --- Docker Compose ---

type composePlatform struct{}

func (composePlatform) name() string { return "compose" }

func (composePlatform) internalAPI() string { return "http://api:8081" }

func (composePlatform) instances(t *testing.T, component string) []string {
	t.Helper()
	services := []string{component}
	if component == "worker" {
		services = append(services, "container-worker")
	}
	return strings.Fields(compose(t, append([]string{"ps", "-q"}, services...)...))
}

func (composePlatform) exec(t *testing.T, id string, args ...string) (string, error) {
	out, err := exec.Command("docker", append([]string{"exec", id}, args...)...).CombinedOutput()
	return string(out), err
}

func (composePlatform) processes(t *testing.T, id string) string {
	t.Helper()
	return docker(t, "top", id)
}

func (composePlatform) ip(t *testing.T, id string) string {
	t.Helper()
	return strings.TrimSpace(docker(t, "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", id))
}

func (composePlatform) kill(t *testing.T, id string) {
	t.Helper()
	docker(t, "kill", id)
}

func (composePlatform) stop(t *testing.T, id string, grace time.Duration) {
	t.Helper()
	docker(t, "stop", "-t", strings.TrimSuffix(grace.Round(time.Second).String(), "s"), id)
}

func (composePlatform) restoreLater(t *testing.T, id string) {
	t.Cleanup(func() {
		docker(t, "start", id)
		time.Sleep(3 * time.Second)
	})
}

// --- Kubernetes ---

type kubePlatform struct {
	context, namespace, release string
}

func (kubePlatform) name() string { return "kubernetes" }

func (k kubePlatform) internalAPI() string { return "http://" + k.fullname() + "-api:8081" }

// kubectl runs kubectl in the release's namespace.
func (k kubePlatform) kubectl(t *testing.T, args ...string) string {
	t.Helper()
	out, err := k.try(args...)
	if err != nil {
		t.Fatalf("kubectl %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return out
}

func (k kubePlatform) try(args ...string) (string, error) {
	full := append([]string{"--context", k.context, "-n", k.namespace}, args...)
	out, err := exec.Command("kubectl", full...).CombinedOutput()
	return string(out), err
}

func (k kubePlatform) selector(component string) string {
	sel := "app.kubernetes.io/instance=" + k.release + ",app.kubernetes.io/component=" + component
	if component == "worker" {
		sel += ",conductor.dev/pool=default"
	}
	return sel
}

func (k kubePlatform) instances(t *testing.T, component string) []string {
	t.Helper()
	// Running pods that aren't being deleted.
	out := k.kubectl(t, "get", "pods", "-l", k.selector(component), "-o",
		`jsonpath={range .items[?(@.status.phase=="Running")]}{.metadata.name} {.metadata.deletionTimestamp}{"\n"}{end}`)
	var pods []string
	for _, line := range strings.Split(out, "\n") {
		if f := strings.Fields(line); len(f) == 1 {
			pods = append(pods, f[0])
		}
	}
	return pods
}

func (k kubePlatform) exec(t *testing.T, pod string, args ...string) (string, error) {
	return k.try(append([]string{"exec", pod, "--"}, args...)...)
}

func (k kubePlatform) processes(t *testing.T, pod string) string {
	t.Helper()
	out, _ := k.exec(t, pod, "ps", "-o", "args") // the pod may have just gone
	return out
}

func (k kubePlatform) ip(t *testing.T, pod string) string {
	t.Helper()
	return strings.TrimSpace(k.kubectl(t, "get", "pod", pod, "-o", "jsonpath={.status.podIP}"))
}

// kill force-deletes the pod: the kubelet kills its container at once (after
// at most a 2-second grace period), and its Deployment starts a new one.
func (k kubePlatform) kill(t *testing.T, pod string) {
	t.Helper()
	k.kubectl(t, "delete", "pod", pod, "--grace-period=0", "--force", "--wait=false")
}

func (k kubePlatform) stop(t *testing.T, pod string, grace time.Duration) {
	t.Helper()
	k.kubectl(t, "delete", "pod", pod, "--grace-period="+strings.TrimSuffix(grace.Round(time.Second).String(), "s"))
}

// restoreLater waits for every Deployment to be back at full strength:
// they replace deleted pods themselves.
func (k kubePlatform) restoreLater(t *testing.T, _ string) {
	t.Cleanup(func() {
		k.kubectl(t, "rollout", "status", "deployment", "-l", "app.kubernetes.io/instance="+k.release, "--timeout=3m")
		time.Sleep(3 * time.Second)
	})
}
