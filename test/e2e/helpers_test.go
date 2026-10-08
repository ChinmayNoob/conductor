//go:build e2e

// Package e2e tests a running Conductor stack end to end. Start it first:
//
//	docker compose --profile containers up -d --build --scale worker=3
//	go test -tags e2e -count=1 -v ./test/e2e/
//
// Settings (environment):
//
//	E2E_API_URL        default http://localhost:8081
//	E2E_API_KEY        default insecure-dev-api-key
//	E2E_WORKERS        number of `worker` replicas, default 3
//	E2E_COMPOSE_FILES  comma-separated compose files, default docker-compose.yml
package e2e

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ChinmayNoob/conductor/pkg/client"
)

var (
	apiURL  = env("E2E_API_URL", "http://localhost:8081")
	apiKey  = env("E2E_API_KEY", "insecure-dev-api-key")
	workers = envInt("E2E_WORKERS", 3)
)

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	n, err := strconv.Atoi(os.Getenv(key))
	if err != nil {
		return fallback
	}
	return n
}

func newClient(t *testing.T) *client.Client {
	t.Helper()
	c := client.New(apiURL, apiKey)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for {
		if err := c.Health(ctx); err == nil {
			return c
		}
		select {
		case <-ctx.Done():
			t.Fatalf("API at %s is not healthy; is the stack running?", apiURL)
		case <-time.After(time.Second):
		}
	}
}

func ctxTimeout(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

func repoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	return root
}

// compose runs docker compose against the repository's compose files.
func compose(t *testing.T, args ...string) string {
	t.Helper()
	root := repoRoot(t)
	var full []string
	for _, f := range strings.Split(env("E2E_COMPOSE_FILES", "docker-compose.yml"), ",") {
		full = append(full, "-f", filepath.Join(root, f))
	}
	full = append(append([]string{"compose", "--profile", "containers"}, full...), args...)
	out, err := exec.Command("docker", full...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker %s: %v\n%s", strings.Join(full, " "), err, out)
	}
	return string(out)
}

func docker(t *testing.T, args ...string) string {
	t.Helper()
	out, err := exec.Command("docker", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return string(out)
}

// workerContainers lists the running containers of every worker service.
func workerContainers(t *testing.T) []string {
	t.Helper()
	return strings.Fields(compose(t, "ps", "-q", "worker", "container-worker"))
}

// workerRunning returns the worker container whose processes include marker,
// waiting up to 15s for the task to start.
func workerRunning(t *testing.T, marker string) string {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if id := containerRunning(t, marker); id != "" {
			return id
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatalf("no worker is running a process containing %q", marker)
	return ""
}

// containerRunning returns the worker container running marker, or "".
func containerRunning(t *testing.T, marker string) string {
	t.Helper()
	for _, id := range workerContainers(t) {
		if strings.Contains(docker(t, "top", id), marker) {
			return id
		}
	}
	return ""
}

// restartLater starts a stopped container again when the test ends, and
// waits for it to register.
func restartLater(t *testing.T, container string) {
	t.Cleanup(func() {
		docker(t, "start", container)
		time.Sleep(3 * time.Second)
	})
}

// capacity returns the total slots of healthy workers that run shell tasks.
func capacity(t *testing.T, c *client.Client) int {
	t.Helper()
	ws, err := c.ListWorkers(ctxTimeout(t, 10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	total := 0
	for _, w := range ws {
		if w.Status == "healthy" && w.Labels["type.shell"] == "true" {
			total += w.Slots
		}
	}
	return total
}

// hasContainerWorker reports whether some healthy worker runs container tasks.
func hasContainerWorker(t *testing.T, c *client.Client) bool {
	t.Helper()
	ws, err := c.ListWorkers(ctxTimeout(t, 10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	for _, w := range ws {
		if w.Status == "healthy" && w.Labels["type.container"] == "true" {
			return true
		}
	}
	return false
}

func uniqueName(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

func submit(t *testing.T, c *client.Client, req client.TaskRequest) *client.Task {
	t.Helper()
	task, err := c.SubmitTask(ctxTimeout(t, 30*time.Second), req)
	if err != nil {
		t.Fatal(err)
	}
	return task
}

func waitTask(t *testing.T, c *client.Client, id string, timeout time.Duration) *client.Task {
	t.Helper()
	task, err := c.WaitForTask(ctxTimeout(t, timeout), id)
	if err != nil {
		t.Fatal(err)
	}
	return task
}

func waitWorkflow(t *testing.T, c *client.Client, id string, timeout time.Duration) *client.Workflow {
	t.Helper()
	wf, err := c.WaitForWorkflow(ctxTimeout(t, timeout), id)
	if err != nil {
		t.Fatal(err)
	}
	return wf
}

func applyDefinition(t *testing.T, c *client.Client, yaml string) *client.Definition {
	t.Helper()
	d, err := c.ApplyDefinition(ctxTimeout(t, 30*time.Second), []byte(yaml))
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func startWorkflow(t *testing.T, c *client.Client, name string, input any) *client.Workflow {
	t.Helper()
	wf, err := c.StartWorkflow(ctxTimeout(t, 30*time.Second), client.WorkflowRequest{Workflow: name, Input: input})
	if err != nil {
		t.Fatal(err)
	}
	return wf
}

func wantSteps(t *testing.T, wf *client.Workflow, want map[string]string) {
	t.Helper()
	for name, status := range want {
		s := wf.Step(name)
		if s == nil {
			t.Errorf("no step %s", name)
		} else if s.Status != status {
			t.Errorf("step %s is %s, want %s", name, s.Status, status)
		}
	}
}

// containerByIP returns the container in the list whose IP is ip.
func containerByIP(t *testing.T, ids []string, ip string) string {
	t.Helper()
	for _, id := range ids {
		got := strings.TrimSpace(docker(t, "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", id))
		if got == ip {
			return id
		}
	}
	t.Fatalf("no container has IP %s", ip)
	return ""
}

// leader returns the elected leader's epoch and container.
func leader(t *testing.T, c *client.Client) (epoch int64, container string) {
	t.Helper()
	cl, err := c.Cluster(ctxTimeout(t, 10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if cl.Leader == nil {
		t.Fatal("no leader elected")
	}
	ip, _, _ := strings.Cut(cl.Leader.Address, ":")
	return cl.Leader.Epoch, containerByIP(t, strings.Fields(compose(t, "ps", "-q", "coordinator")), ip)
}

// networkOf returns the compose network a container is attached to.
func networkOf(t *testing.T, container string) string {
	t.Helper()
	return strings.TrimSpace(docker(t, "inspect", "-f", "{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}", container))
}
