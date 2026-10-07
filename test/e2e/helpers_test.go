//go:build e2e

// Package e2e tests a running Conductor stack end to end. Start it first:
//
//	docker compose up -d --build --scale worker=3
//	go test -tags e2e -count=1 -v ./test/e2e/
//
// Settings (environment):
//
//	E2E_API_URL        default http://localhost:8081
//	E2E_API_KEY        default insecure-dev-api-key
//	E2E_WORKERS        number of worker replicas, default 3
//	E2E_COMPOSE_FILES  comma-separated compose files, default docker-compose.yml
package e2e

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
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

// compose runs docker compose against the repository's compose files.
func compose(t *testing.T, args ...string) string {
	t.Helper()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	var full []string
	for _, f := range strings.Split(env("E2E_COMPOSE_FILES", "docker-compose.yml"), ",") {
		full = append(full, "-f", filepath.Join(root, f))
	}
	full = append(append([]string{"compose"}, full...), args...)
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

// workerRunning returns the ID of the worker container whose processes
// include marker, waiting up to 15s for the task to start.
func workerRunning(t *testing.T, marker string) string {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		for _, id := range strings.Fields(compose(t, "ps", "-q", "worker")) {
			if strings.Contains(docker(t, "top", id), marker) {
				return id
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatalf("no worker is running a process containing %q", marker)
	return ""
}

// processRunning reports whether any worker has a process containing marker.
func processRunning(t *testing.T, marker string) bool {
	t.Helper()
	for _, id := range strings.Fields(compose(t, "ps", "-q", "worker")) {
		if strings.Contains(docker(t, "top", id), marker) {
			return true
		}
	}
	return false
}

// restoreWorkers brings the worker count back after a test stops one, and
// waits for the replacement to register.
func restoreWorkers(t *testing.T) {
	t.Helper()
	compose(t, "up", "-d", "--scale", fmt.Sprintf("worker=%d", workers), "--no-recreate", "worker")
	time.Sleep(3 * time.Second)
}

// registeredWorkers returns the distinct worker IDs in the coordinator log.
func registeredWorkers(t *testing.T) map[string]string {
	t.Helper()
	re := regexp.MustCompile(`Worker registered.*worker_id=(\d+) address=(\S+)`)
	ids := make(map[string]string)
	for _, m := range re.FindAllStringSubmatch(compose(t, "logs", "coordinator"), -1) {
		ids[m[1]] = m[2]
	}
	return ids
}

// dispatchedTo maps task IDs to the worker each was last dispatched to,
// according to the coordinator log.
func dispatchedTo(t *testing.T) map[string]string {
	t.Helper()
	re := regexp.MustCompile(`Task dispatched.*task_id=(\S+) worker_id=(\d+)`)
	out := make(map[string]string)
	for _, m := range re.FindAllStringSubmatch(compose(t, "logs", "coordinator"), -1) {
		out[m[1]] = m[2]
	}
	return out
}

func uniqueMarker(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}
