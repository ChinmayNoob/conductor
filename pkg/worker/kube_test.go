package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeKube is a Kubernetes API server for task pods: it stores pods, serves
// gets, watches, lists and logs, and lets a test script each pod's life.
type fakeKube struct {
	t      *testing.T
	script func(f *fakeKube, name string) // runs after each pod is created

	mu        sync.Mutex
	pods      map[string]*pod
	logs      map[string]string
	manifests []map[string]any
	deleted   []string
	rv        int
	changed   chan struct{} // closed on every change
}

func newFakeKube(t *testing.T, script func(f *fakeKube, name string)) (*fakeKube, *kube) {
	f := &fakeKube{t: t, script: script, pods: map[string]*pod{}, logs: map[string]string{}, changed: make(chan struct{})}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	k := newKube(srv.URL, srv.Client(), func() (string, error) { return "token", nil }, "jobs", KubeOptions{
		PodName: "worker-abc", PodUID: "uid-1", ServiceAccounts: []string{"reports"},
	})
	return f, k
}

// update changes a pod and wakes watchers.
func (f *fakeKube) update(name string, change func(p *pod)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	p := f.pods[name]
	if p == nil {
		return
	}
	change(p)
	f.bump(p)
}

func (f *fakeKube) bump(p *pod) {
	f.rv++
	p.Metadata.ResourceVersion = fmt.Sprint(f.rv)
	close(f.changed)
	f.changed = make(chan struct{})
}

// Scripted states.
func running(p *pod) {
	p.Status.Phase = "Running"
	p.Status.ContainerStatuses = []containerStatus{{Name: k8sContainer}}
	p.Status.ContainerStatuses[0].State.Running = &struct{}{}
}

func exited(code int, reason, message string) func(p *pod) {
	return func(p *pod) {
		p.Status.Phase = "Succeeded"
		if code != 0 {
			p.Status.Phase = "Failed"
		}
		c := containerStatus{Name: k8sContainer}
		c.State.Terminated = &struct {
			ExitCode   int       `json:"exitCode"`
			Reason     string    `json:"reason"`
			Message    string    `json:"message"`
			FinishedAt time.Time `json:"finishedAt"`
		}{ExitCode: code, Reason: reason, Message: message, FinishedAt: time.Now()}
		p.Status.ContainerStatuses = []containerStatus{c}
	}
}

func waiting(reason string) func(p *pod) {
	return func(p *pod) {
		c := containerStatus{Name: k8sContainer}
		c.State.Waiting = &struct {
			Reason  string `json:"reason"`
			Message string `json:"message"`
		}{Reason: reason, Message: "back-off pulling image"}
		p.Status.ContainerStatuses = []containerStatus{c}
	}
}

func (f *fakeKube) setLogs(name, logs string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.logs[name] = logs
}

func (f *fakeKube) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Authorization") != "Bearer token" {
		http.Error(w, `{"message": "unauthorized"}`, http.StatusUnauthorized)
		return
	}
	const prefix = "/api/v1/namespaces/jobs/pods"
	rest, ok := strings.CutPrefix(r.URL.Path, prefix)
	if !ok {
		http.NotFound(w, r)
		return
	}
	name, sub, _ := strings.Cut(strings.TrimPrefix(rest, "/"), "/")
	switch {
	case r.Method == http.MethodPost && name == "":
		f.create(w, r)
	case r.Method == http.MethodGet && name == "" && r.URL.Query().Get("watch") == "true":
		f.watch(w, r)
	case r.Method == http.MethodGet && name == "":
		f.list(w, r)
	case r.Method == http.MethodGet && sub == "log":
		f.serveLogs(w, r, name)
	case r.Method == http.MethodGet:
		f.mu.Lock()
		p := f.pods[name]
		f.mu.Unlock()
		if p == nil {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, `{"reason": "NotFound", "message": "pods not found"}`)
			return
		}
		f.write(w, p)
	case r.Method == http.MethodDelete:
		f.mu.Lock()
		f.deleted = append(f.deleted, name)
		if p := f.pods[name]; p != nil {
			delete(f.pods, name)
			f.bump(p)
		}
		f.mu.Unlock()
		fmt.Fprint(w, `{}`)
	default:
		http.Error(w, "unexpected request", http.StatusBadRequest)
	}
}

func (f *fakeKube) write(w http.ResponseWriter, p *pod) {
	f.mu.Lock()
	b, _ := json.Marshal(p)
	f.mu.Unlock()
	_, _ = w.Write(b)
}

func (f *fakeKube) create(w http.ResponseWriter, r *http.Request) {
	var m map[string]any
	if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
		f.t.Errorf("bad manifest: %v", err)
	}
	f.mu.Lock()
	f.manifests = append(f.manifests, m)
	p := &pod{}
	p.Metadata.Name = fmt.Sprintf("conductor-task-%d", len(f.manifests))
	p.Metadata.CreationTimestamp = time.Now()
	labels := map[string]string{}
	for k, v := range m["metadata"].(map[string]any)["labels"].(map[string]any) {
		labels[k] = v.(string)
	}
	p.Metadata.Labels = labels
	p.Status.Phase = "Pending"
	f.pods[p.Metadata.Name] = p
	f.bump(p)
	f.mu.Unlock()
	if f.script != nil {
		go f.script(f, p.Metadata.Name)
	}
	f.write(w, p)
}

func (f *fakeKube) watch(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Query().Get("fieldSelector"), "metadata.name=")
	seen := r.URL.Query().Get("resourceVersion")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.(http.Flusher).Flush()
	for {
		f.mu.Lock()
		p, changed := f.pods[name], f.changed
		var event []byte
		switch {
		case p == nil:
			event = []byte(`{"type": "DELETED", "object": {}}`)
		case p.Metadata.ResourceVersion != seen:
			seen = p.Metadata.ResourceVersion
			obj, _ := json.Marshal(p)
			event, _ = json.Marshal(map[string]any{"type": "MODIFIED", "object": json.RawMessage(obj)})
		}
		f.mu.Unlock()
		if event != nil {
			_, _ = w.Write(append(event, '\n'))
			w.(http.Flusher).Flush()
			if p == nil {
				return
			}
		}
		select {
		case <-changed:
		case <-r.Context().Done():
			return
		}
	}
}

func (f *fakeKube) list(w http.ResponseWriter, r *http.Request) {
	want := map[string]string{}
	for pair := range strings.SplitSeq(r.URL.Query().Get("labelSelector"), ",") {
		k, v, _ := strings.Cut(pair, "=")
		want[k] = v
	}
	f.mu.Lock()
	var items []*pod
	for _, p := range f.pods {
		match := true
		for k, v := range want {
			match = match && p.Metadata.Labels[k] == v
		}
		if match {
			items = append(items, p)
		}
	}
	b, _ := json.Marshal(map[string]any{"items": items})
	f.mu.Unlock()
	_, _ = w.Write(b)
}

// serveLogs writes a pod's logs; following, it waits for the pod to finish
// first.
func (f *fakeKube) serveLogs(w http.ResponseWriter, r *http.Request, name string) {
	for r.URL.Query().Get("follow") == "true" {
		f.mu.Lock()
		p, changed := f.pods[name], f.changed
		done := p == nil || p.finished()
		f.mu.Unlock()
		if done {
			break
		}
		select {
		case <-changed:
		case <-r.Context().Done():
			return
		}
	}
	f.mu.Lock()
	logs := f.logs[name]
	f.mu.Unlock()
	fmt.Fprint(w, logs)
}

// lifecycle scripts a pod that runs, logs, and exits.
func lifecycle(logs string, exit func(p *pod)) func(f *fakeKube, name string) {
	return func(f *fakeKube, name string) {
		time.Sleep(20 * time.Millisecond)
		f.update(name, running)
		f.setLogs(name, logs)
		time.Sleep(20 * time.Millisecond)
		f.update(name, exit)
	}
}

func runKube(t *testing.T, k *kube, spec string, timeout time.Duration) (string, map[string]string, error) {
	t.Helper()
	return k.run(context.Background(), "task-1", 2, []byte(spec), map[string]string{"INPUT_X": "1; rm -rf /"}, timeout, newCappedBuffer(1<<20))
}

func TestKubeTaskSucceeds(t *testing.T) {
	f, k := newFakeKube(t, lifecycle("hello from the pod\n", exited(0, "Completed", "answer=42\nnot an output\nnote=hi\n")))
	out, outputs, err := runKube(t, k, `{"image": "alpine:3.20", "command": ["sh", "-c", "echo hi"], "cpu": "250m", "memory": "64Mi"}`, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if out != "hello from the pod\n" {
		t.Errorf("output = %q", out)
	}
	if outputs["answer"] != "42" || outputs["note"] != "hi" || len(outputs) != 2 {
		t.Errorf("outputs = %v", outputs)
	}
	if len(f.deleted) != 1 || f.deleted[0] != "conductor-task-1" {
		t.Errorf("deleted = %v, want the task's pod", f.deleted)
	}

	// The pod: least privilege, and everything the task needs.
	m := f.manifests[0]
	meta, spec := m["metadata"].(map[string]any), m["spec"].(map[string]any)
	labels := meta["labels"].(map[string]any)
	if labels[k8sLabelTask] != "task-1" || labels[k8sLabelAttempt] != "2" || labels[k8sLabelWorker] != "worker-abc" {
		t.Errorf("labels = %v", labels)
	}
	if owners := meta["ownerReferences"].([]any); len(owners) != 1 || owners[0].(map[string]any)["uid"] != "uid-1" {
		t.Errorf("ownerReferences = %v", owners)
	}
	if spec["automountServiceAccountToken"] != false || spec["serviceAccountName"] != "default" ||
		spec["enableServiceLinks"] != false || spec["restartPolicy"] != "Never" || spec["activeDeadlineSeconds"] != 120.0 {
		t.Errorf("pod spec = %v", spec)
	}
	sec := spec["securityContext"].(map[string]any)
	if sec["runAsNonRoot"] != true || sec["runAsUser"] != 65534.0 {
		t.Errorf("pod securityContext = %v", sec)
	}
	c := spec["containers"].([]any)[0].(map[string]any)
	if c["securityContext"].(map[string]any)["allowPrivilegeEscalation"] != false {
		t.Errorf("container securityContext = %v", c["securityContext"])
	}
	limits := c["resources"].(map[string]any)["limits"].(map[string]any)
	if limits["cpu"] != "250m" || limits["memory"] != "64Mi" {
		t.Errorf("resources = %v", c["resources"])
	}
	env := map[string]string{}
	for _, e := range c["env"].([]any) {
		e := e.(map[string]any)
		env[e["name"].(string)] = e["value"].(string)
	}
	if env["INPUT_X"] != "1; rm -rf /" || env["CONDUCTOR_OUTPUT"] != k8sOutputPath {
		t.Errorf("env = %v", env)
	}
}

func TestKubeTaskFailures(t *testing.T) {
	cases := map[string]struct {
		script    func(f *fakeKube, name string)
		want      string
		permanent bool
	}{
		"non-zero exit": {lifecycle("boom\n", exited(3, "Error", "")), "exited with status 3", false},
		"out of memory": {lifecycle("", exited(137, "OOMKilled", "")), "ran out of memory", false},
		"image pull back-off": {func(f *fakeKube, name string) {
			f.update(name, waiting("ErrImagePull"))
			f.update(name, waiting("ImagePullBackOff"))
		}, "ImagePullBackOff", false},
		"invalid image": {func(f *fakeKube, name string) { f.update(name, waiting("InvalidImageName")) }, "InvalidImageName", true},
		"evicted": {func(f *fakeKube, name string) {
			f.update(name, func(p *pod) { p.Status.Phase, p.Status.Reason, p.Status.Message = "Failed", "Evicted", "low on memory" })
		}, "Evicted: low on memory", false},
		"deleted by someone": {func(f *fakeKube, name string) {
			f.mu.Lock()
			p := f.pods[name]
			delete(f.pods, name)
			f.bump(p)
			f.mu.Unlock()
		}, "pod was deleted", false},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			f, k := newFakeKube(t, c.script)
			_, _, err := runKube(t, k, `{"image": "alpine"}`, time.Minute)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("err = %v, want %q", err, c.want)
			}
			if _, permanent := retryAdvice(err); permanent != c.permanent {
				t.Errorf("permanent = %v, want %v", permanent, c.permanent)
			}
			if len(f.deleted) != 1 {
				t.Errorf("deleted = %v, want the pod deleted", f.deleted)
			}
		})
	}
}

func TestKubeTaskTimeoutAndCancel(t *testing.T) {
	stuck := func(f *fakeKube, name string) { f.update(name, running) }

	f, k := newFakeKube(t, stuck)
	if _, _, err := runKube(t, k, `{"image": "alpine"}`, 300*time.Millisecond); err == nil || !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("err = %v, want a timeout", err)
	}
	if len(f.deleted) != 1 {
		t.Errorf("timed-out pod not deleted: %v", f.deleted)
	}

	f, k = newFakeKube(t, stuck)
	ctx, cancel := context.WithCancelCause(context.Background())
	time.AfterFunc(200*time.Millisecond, func() { cancel(errCancelled) })
	_, _, err := k.run(ctx, "task-1", 1, []byte(`{"image": "alpine"}`), nil, time.Minute, newCappedBuffer(1024))
	if !errors.Is(err, errCancelled) {
		t.Fatalf("err = %v, want errCancelled", err)
	}
	if len(f.deleted) != 1 {
		t.Errorf("cancelled pod not deleted: %v", f.deleted)
	}
}

func TestKubeTaskTimeoutExplainsPendingPod(t *testing.T) {
	_, k := newFakeKube(t, func(f *fakeKube, name string) {
		f.update(name, func(p *pod) {
			p.Status.Conditions = []podCondition{{Type: "PodScheduled", Status: "False", Message: "0/3 nodes have a GPU"}}
		})
	})
	_, _, err := runKube(t, k, `{"image": "alpine"}`, 300*time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "never scheduled: 0/3 nodes have a GPU") {
		t.Fatalf("err = %v", err)
	}
}

func TestKubeServiceAccounts(t *testing.T) {
	f, k := newFakeKube(t, lifecycle("", exited(0, "Completed", "")))
	_, _, err := runKube(t, k, `{"image": "alpine", "service_account": "cluster-admin"}`, time.Minute)
	if _, permanent := retryAdvice(err); err == nil || !permanent || !strings.Contains(err.Error(), "not allowed") {
		t.Fatalf("err = %v (permanent %v), want a permanent refusal", err, permanent)
	}
	if len(f.manifests) != 0 {
		t.Fatal("a pod was created for a refused service account")
	}

	if _, _, err := runKube(t, k, `{"image": "alpine", "service_account": "reports"}`, time.Minute); err != nil {
		t.Fatal(err)
	}
	spec := f.manifests[0]["spec"].(map[string]any)
	if spec["serviceAccountName"] != "reports" || spec["automountServiceAccountToken"] != true {
		t.Fatalf("pod spec = %v, want the reports account with its token", spec)
	}
}

func TestKubeLeftovers(t *testing.T) {
	f, k := newFakeKube(t, nil)
	add := func(name, worker string, change func(p *pod)) {
		p := &pod{}
		p.Metadata.Name = name
		p.Metadata.CreationTimestamp = time.Now()
		p.Metadata.Labels = map[string]string{k8sLabelManagedBy: "conductor", k8sLabelWorker: worker}
		change(p)
		f.pods[name] = p
	}
	add("mine-running", "worker-abc", running)
	add("theirs-running", "worker-xyz", running)
	add("theirs-done-now", "worker-xyz", exited(0, "Completed", ""))
	add("theirs-done-long-ago", "worker-xyz", func(p *pod) {
		exited(1, "Error", "")(p)
		p.Status.ContainerStatuses[0].State.Terminated.FinishedAt = time.Now().Add(-time.Hour)
	})

	k.deleteOwnLeftovers(context.Background())
	if len(f.deleted) != 1 || f.deleted[0] != "mine-running" {
		t.Fatalf("deleted %v, want only this worker's leftover", f.deleted)
	}

	var orphans []string
	for name, p := range f.pods {
		if p.orphaned(time.Now()) {
			orphans = append(orphans, name)
		}
	}
	if len(orphans) != 1 || orphans[0] != "theirs-done-long-ago" {
		t.Fatalf("orphans = %v", orphans)
	}
}

func TestLabelValue(t *testing.T) {
	if got := labelValue("conductor-worker-7d9f-x2"); got != "conductor-worker-7d9f-x2" {
		t.Errorf("valid value changed to %q", got)
	}
	long := strings.Repeat("a", 70)
	if got := labelValue(long); got == long || !labelValueRe.MatchString(got) {
		t.Errorf("labelValue(70 chars) = %q", got)
	}
}
