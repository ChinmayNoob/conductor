package worker

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func skipOnWindows(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("uses POSIX shell syntax")
	}
}

func TestShellOutputsAndEnv(t *testing.T) {
	skipOnWindows(t)
	env := taskEnv(baseEnv(nil), map[string]string{"GREETING": "hi there"})
	out, outputs, err := runShell(context.Background(),
		`echo "$GREETING"; echo "answer=42" >> "$CONDUCTOR_OUTPUT"; echo "ignored line" >> "$CONDUCTOR_OUTPUT"`,
		env, 10*time.Second, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(out) != "hi there" {
		t.Errorf("output = %q", out)
	}
	if len(outputs) != 1 || outputs["answer"] != "42" {
		t.Errorf("outputs = %v", outputs)
	}
}

func TestTasksDoNotInheritWorkerSecrets(t *testing.T) {
	skipOnWindows(t)
	t.Setenv("CONDUCTOR_CLUSTER_TOKEN", "super-secret")
	t.Setenv("ALLOWED_VAR", "visible")

	env := taskEnv(baseEnv([]string{"ALLOWED_VAR"}), nil)
	out, _, err := runShell(context.Background(), `env`, env, 10*time.Second, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out, "super-secret") {
		t.Fatal("the cluster token leaked into the task environment")
	}
	if !strings.Contains(out, "ALLOWED_VAR=visible") {
		t.Fatal("an allow-listed variable was not passed through")
	}
}

func TestShellTimeoutKillsChildren(t *testing.T) {
	skipOnWindows(t)
	start := time.Now()
	_, _, err := runShell(context.Background(), `sleep 30; echo done`, baseEnv(nil), 500*time.Millisecond, 1<<20)
	if err == nil || !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("err = %v, want a timeout", err)
	}
	if time.Since(start) > 5*time.Second {
		t.Fatalf("timeout took %v: the child process kept the task alive", time.Since(start))
	}
}

func TestReadOutputsRejectsHugeFiles(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out")
	if err := os.WriteFile(path, []byte(strings.Repeat("k=v\n", maxOutputsBytes)), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readOutputs(path); err == nil {
		t.Fatal("oversized outputs file was accepted")
	}
}

func TestHTTPTask(t *testing.T) {
	var gotMethod, gotBody, gotHeader string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotHeader = r.Method, r.Header.Get("X-Token")
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		if r.URL.Path == "/missing" {
			http.NotFound(w, r)
			return
		}
		w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	spec, _ := json.Marshal(httpSpec{Method: "POST", URL: srv.URL + "/hook", Body: `{"a":1}`, Headers: map[string]string{"X-Token": "t"}})
	out, outputs, err := runHTTP(context.Background(), spec, 5*time.Second, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if gotMethod != "POST" || gotBody != `{"a":1}` || gotHeader != "t" {
		t.Fatalf("server saw %s %q header=%q", gotMethod, gotBody, gotHeader)
	}
	if outputs["status"] != "200" || outputs["body"] != `{"ok":true}` || !strings.Contains(out, "200") {
		t.Fatalf("output %q outputs %v", out, outputs)
	}

	spec, _ = json.Marshal(httpSpec{URL: srv.URL + "/missing"})
	if _, _, err := runHTTP(context.Background(), spec, 5*time.Second, 1<<20); err == nil {
		t.Fatal("a 404 should fail the task")
	}
	spec, _ = json.Marshal(httpSpec{URL: srv.URL + "/missing", ExpectStatus: []int{404}})
	if _, _, err := runHTTP(context.Background(), spec, 5*time.Second, 1<<20); err != nil {
		t.Fatalf("expect_status 404 should accept a 404: %v", err)
	}
}

func TestParseBytes(t *testing.T) {
	for in, want := range map[string]int64{"": 0, "512": 512, "4k": 4096, "256m": 256 << 20, "1g": 1 << 30} {
		if got, err := parseBytes(in); err != nil || got != want {
			t.Errorf("parseBytes(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	if _, err := parseBytes("lots"); err == nil {
		t.Error("parseBytes accepted garbage")
	}
}
