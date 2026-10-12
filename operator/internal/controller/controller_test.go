package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ChinmayNoob/conductor/operator/api/v1alpha1"
	conductor "github.com/ChinmayNoob/conductor/pkg/client"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// fakeConductor serves the part of Conductor's API the operator uses.
type fakeConductor struct {
	mu          sync.Mutex
	definitions map[string][]string // ns/name -> versions' bodies
	runs        map[string]*conductor.Workflow
	keys        map[string]string // idempotency key -> run ID
	schedules   map[string]map[string]any
	namespaces  map[string]bool
	started     int
}

func newFakeConductor(t *testing.T) (*fakeConductor, *conductor.Client) {
	f := &fakeConductor{definitions: map[string][]string{}, runs: map[string]*conductor.Workflow{},
		keys: map[string]string{}, schedules: map[string]map[string]any{}, namespaces: map[string]bool{"default": true}}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	return f, conductor.New(srv.URL, "key")
}

func reply(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func (f *fakeConductor) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	ns := r.Header.Get("X-Conductor-Namespace")
	if ns == "" {
		ns = "default"
	}
	body, _ := io.ReadAll(r.Body)
	switch {
	case (r.Method == http.MethodPut || r.Method == http.MethodPost) && r.URL.Path == "/v1/workflow-definitions":
		var def map[string]any
		_ = json.Unmarshal(body, &def)
		if steps, _ := def["steps"].([]any); len(steps) == 0 {
			reply(w, http.StatusBadRequest, map[string]string{"error": "a workflow needs at least one step"})
			return
		}
		key := ns + "/" + def["name"].(string)
		versions := f.definitions[key]
		created := len(versions) == 0 || versions[len(versions)-1] != string(body)
		if created {
			f.definitions[key] = append(versions, string(body))
		}
		code := http.StatusOK
		if created {
			code = http.StatusCreated
		}
		reply(w, code, map[string]any{"name": def["name"], "version": len(f.definitions[key])})

	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/workflow-definitions/"):
		name := strings.TrimPrefix(r.URL.Path, "/v1/workflow-definitions/")
		if versions := f.definitions[ns+"/"+name]; len(versions) > 0 {
			reply(w, http.StatusOK, map[string]any{"name": name, "version": len(versions)})
			return
		}
		reply(w, http.StatusNotFound, map[string]string{"error": "workflow definition not found"})

	case r.Method == http.MethodPost && r.URL.Path == "/v1/workflows":
		var req conductor.WorkflowRequest
		_ = json.Unmarshal(body, &req)
		if len(f.definitions[ns+"/"+req.Workflow]) == 0 {
			reply(w, http.StatusNotFound, map[string]string{"error": "workflow definition not found"})
			return
		}
		if id, ok := f.keys[req.IdempotencyKey]; ok {
			reply(w, http.StatusOK, f.runs[id])
			return
		}
		f.started++
		wf := &conductor.Workflow{ID: fmt.Sprintf("run-%d", f.started), Namespace: ns, Workflow: req.Workflow,
			Status: "RUNNING", Input: mustJSON(req.Input), UpdatedAt: time.Now(),
			Steps: []conductor.WorkflowStep{{Name: "only", Status: "RUNNING"}}}
		f.runs[wf.ID], f.keys[req.IdempotencyKey] = wf, wf.ID
		reply(w, http.StatusCreated, wf)

	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/workflows/"):
		if wf := f.runs[strings.TrimPrefix(r.URL.Path, "/v1/workflows/")]; wf != nil {
			reply(w, http.StatusOK, wf)
			return
		}
		reply(w, http.StatusNotFound, map[string]string{"error": "workflow not found"})

	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/cancel"):
		id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/workflows/"), "/cancel")
		wf := f.runs[id]
		if wf == nil {
			reply(w, http.StatusNotFound, map[string]string{"error": "workflow not found"})
			return
		}
		wf.Status = "CANCELLED"
		reply(w, http.StatusOK, wf)

	case r.Method == http.MethodPost && r.URL.Path == "/v1/namespaces":
		var n map[string]any
		_ = json.Unmarshal(body, &n)
		if f.namespaces[n["name"].(string)] {
			reply(w, http.StatusConflict, map[string]string{"error": "namespace exists"})
			return
		}
		f.namespaces[n["name"].(string)] = true
		reply(w, http.StatusCreated, n)

	case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/v1/schedules/"):
		var s map[string]any
		_ = json.Unmarshal(body, &s)
		if s["cron"] == "bogus" {
			reply(w, http.StatusBadRequest, map[string]string{"error": "invalid cron expression"})
			return
		}
		if wf, ok := s["workflow"].(map[string]any); ok && len(f.definitions[ns+"/"+wf["name"].(string)]) == 0 {
			reply(w, http.StatusBadRequest, map[string]string{"error": "no workflow definition named " + wf["name"].(string)})
			return
		}
		key := ns + "/" + strings.TrimPrefix(r.URL.Path, "/v1/schedules/")
		_, existed := f.schedules[key]
		f.schedules[key] = s
		next := time.Now().Add(time.Hour)
		out := map[string]any{"name": s["name"], "cron": s["cron"], "enabled": s["enabled"], "next_run_at": next}
		code := http.StatusCreated
		if existed {
			code = http.StatusOK
		}
		reply(w, code, out)

	case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/v1/schedules/"):
		key := ns + "/" + strings.TrimPrefix(r.URL.Path, "/v1/schedules/")
		if _, ok := f.schedules[key]; !ok {
			reply(w, http.StatusNotFound, map[string]string{"error": "schedule not found"})
			return
		}
		delete(f.schedules, key)
		w.WriteHeader(http.StatusNoContent)

	default:
		reply(w, http.StatusNotFound, map[string]string{"error": "no route " + r.Method + " " + r.URL.Path})
	}
}

func mustJSON(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}

func raw(s string) runtime.RawExtension { return runtime.RawExtension{Raw: []byte(s)} }

func setup(t *testing.T, objs ...client.Object) (*fakeConductor, client.Client, Options) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	kc := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).
		WithStatusSubresource(&v1alpha1.Workflow{}, &v1alpha1.WorkflowRun{}, &v1alpha1.Schedule{}).Build()
	f, cc := newFakeConductor(t)
	return f, kc, Options{Conductor: cc, DefaultNamespace: "default", PollInterval: time.Second, Resync: time.Minute}
}

type reconciler interface {
	Reconcile(context.Context, ctrl.Request) (ctrl.Result, error)
}

func reconcile(t *testing.T, r reconciler, name string) (ctrl.Result, error) {
	t.Helper()
	return r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "apps", Name: name}})
}

func ready(conds []metav1.Condition) *metav1.Condition {
	return meta.FindStatusCondition(conds, v1alpha1.ConditionReady)
}

func TestWorkflowIsAppliedAndVersioned(t *testing.T) {
	wf := &v1alpha1.Workflow{
		ObjectMeta: metav1.ObjectMeta{Name: "nightly-report", Namespace: "apps", Generation: 1},
		Spec: v1alpha1.WorkflowSpec{ConductorNamespace: "reports",
			Definition: raw(`{"steps": [{"name": "build", "run": "make report"}]}`)},
	}
	f, kc, opts := setup(t, wf)
	r := &WorkflowReconciler{Client: kc, Options: opts}

	if _, err := reconcile(t, r, "nightly-report"); err != nil {
		t.Fatal(err)
	}
	var got v1alpha1.Workflow
	_ = kc.Get(context.Background(), client.ObjectKeyFromObject(wf), &got)
	if c := ready(got.Status.Conditions); got.Status.Version != 1 || got.Status.Name != "nightly-report" || c == nil || c.Status != metav1.ConditionTrue {
		t.Fatalf("status = %+v", got.Status)
	}
	if len(f.definitions["reports/nightly-report"]) != 1 {
		t.Fatalf("definitions = %v; want it in the reports namespace, named after the resource", f.definitions)
	}

	// Re-applying the same spec makes no new version; a change does.
	if _, err := reconcile(t, r, "nightly-report"); err != nil {
		t.Fatal(err)
	}
	_ = kc.Get(context.Background(), client.ObjectKeyFromObject(wf), &got)
	got.Spec.Definition = raw(`{"steps": [{"name": "build", "run": "make report-v2"}]}`)
	if err := kc.Update(context.Background(), &got); err != nil {
		t.Fatal(err)
	}
	if _, err := reconcile(t, r, "nightly-report"); err != nil {
		t.Fatal(err)
	}
	_ = kc.Get(context.Background(), client.ObjectKeyFromObject(wf), &got)
	if got.Status.Version != 2 || len(f.definitions["reports/nightly-report"]) != 2 {
		t.Fatalf("after a change: version %d, %d stored", got.Status.Version, len(f.definitions["reports/nightly-report"]))
	}
}

func TestInvalidWorkflowIsReportedNotRetried(t *testing.T) {
	wf := &v1alpha1.Workflow{
		ObjectMeta: metav1.ObjectMeta{Name: "empty", Namespace: "apps", Generation: 1},
		Spec:       v1alpha1.WorkflowSpec{Definition: raw(`{"steps": []}`)},
	}
	_, kc, opts := setup(t, wf)
	res, err := reconcile(t, &WorkflowReconciler{Client: kc, Options: opts}, "empty")
	if err != nil {
		t.Fatalf("an invalid spec should not be retried, got %v", err)
	}
	var got v1alpha1.Workflow
	_ = kc.Get(context.Background(), client.ObjectKeyFromObject(wf), &got)
	c := ready(got.Status.Conditions)
	if c == nil || c.Status != metav1.ConditionFalse || c.Reason != v1alpha1.ReasonInvalid || !strings.Contains(c.Message, "at least one step") {
		t.Fatalf("Ready = %+v", c)
	}
	if res.RequeueAfter == 0 {
		t.Error("still resynced periodically")
	}
}

func TestWorkflowRunStartsOnceAndFollows(t *testing.T) {
	run := &v1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: "report-1", Namespace: "apps", UID: "uid-1", Generation: 1},
		Spec:       v1alpha1.WorkflowRunSpec{Workflow: "nightly-report", Input: &runtime.RawExtension{Raw: []byte(`{"day": "monday"}`)}},
	}
	f, kc, opts := setup(t, run)
	r := &WorkflowRunReconciler{Client: kc, Options: opts}
	get := func() v1alpha1.WorkflowRun {
		var got v1alpha1.WorkflowRun
		_ = kc.Get(context.Background(), client.ObjectKeyFromObject(run), &got)
		return got
	}

	// Its workflow doesn't exist yet (GitOps applies in any order): wait.
	res, err := reconcile(t, r, "report-1")
	if err != nil || res.RequeueAfter == 0 {
		t.Fatalf("waiting for the definition: %v, %+v", err, res)
	}
	if c := ready(get().Status.Conditions); c == nil || c.Reason != v1alpha1.ReasonWaiting {
		t.Fatalf("Ready = %+v", c)
	}

	f.definitions["default/nightly-report"] = []string{"{}"}
	if _, err := reconcile(t, r, "report-1"); err != nil {
		t.Fatal(err)
	}
	got := get()
	if got.Status.RunID != "run-1" || got.Status.Phase != "RUNNING" || got.Status.StartTime == nil || len(got.Finalizers) != 1 {
		t.Fatalf("after start: %+v, finalizers %v", got.Status, got.Finalizers)
	}
	if !strings.Contains(string(f.runs["run-1"].Input), "monday") {
		t.Fatalf("input not passed: %s", f.runs["run-1"].Input)
	}

	// Losing the status (say, an update conflict) must not start it twice.
	got.Status.RunID = ""
	_ = kc.Status().Update(context.Background(), &got)
	if _, err := reconcile(t, r, "report-1"); err != nil {
		t.Fatal(err)
	}
	if f.started != 1 || get().Status.RunID != "run-1" {
		t.Fatalf("started %d runs, status run %q; want the same single run", f.started, get().Status.RunID)
	}

	f.runs["run-1"].Status = "COMPLETED"
	f.runs["run-1"].Steps[0].Status = "COMPLETED"
	res, err = reconcile(t, r, "report-1")
	if err != nil || res.RequeueAfter != 0 {
		t.Fatalf("finished run still polled: %v, %+v", err, res)
	}
	if got := get(); got.Status.Phase != "COMPLETED" || got.Status.CompletionTime == nil || got.Status.Steps[0].Status != "COMPLETED" {
		t.Fatalf("after completion: %+v", got.Status)
	}
}

func TestDeletingAWorkflowRunCancelsIt(t *testing.T) {
	run := &v1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: "report-2", Namespace: "apps", UID: "uid-2", Generation: 1},
		Spec:       v1alpha1.WorkflowRunSpec{Workflow: "nightly-report"},
	}
	f, kc, opts := setup(t, run)
	f.definitions["default/nightly-report"] = []string{"{}"}
	r := &WorkflowRunReconciler{Client: kc, Options: opts}
	if _, err := reconcile(t, r, "report-2"); err != nil {
		t.Fatal(err)
	}

	var got v1alpha1.WorkflowRun
	_ = kc.Get(context.Background(), client.ObjectKeyFromObject(run), &got)
	if err := kc.Delete(context.Background(), &got); err != nil {
		t.Fatal(err)
	}
	if _, err := reconcile(t, r, "report-2"); err != nil {
		t.Fatal(err)
	}
	if f.runs["run-1"].Status != "CANCELLED" {
		t.Fatalf("run is %s, want CANCELLED", f.runs["run-1"].Status)
	}
	if err := kc.Get(context.Background(), client.ObjectKeyFromObject(run), &got); err == nil {
		t.Fatal("the resource is still there: the finalizer was not removed")
	}
}

func TestWorkflowRunWithBadInputFails(t *testing.T) {
	run := &v1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: "report-3", Namespace: "apps", UID: "uid-3", Generation: 1},
		Spec:       v1alpha1.WorkflowRunSpec{Workflow: "x", Input: &runtime.RawExtension{Raw: []byte(`["not", "an", "object"]`)}},
	}
	f, kc, opts := setup(t, run)
	r := &WorkflowRunReconciler{Client: kc, Options: opts}
	if _, err := reconcile(t, r, "report-3"); err != nil {
		t.Fatal(err)
	}
	var got v1alpha1.WorkflowRun
	_ = kc.Get(context.Background(), client.ObjectKeyFromObject(run), &got)
	if got.Status.Phase != "FAILED" || !strings.Contains(got.Status.Error, "input must be an object") {
		t.Fatalf("status = %+v", got.Status)
	}

	// Failed for good: later reconciles (an operator restart) start nothing.
	f.definitions["default/x"] = []string{"{}"}
	if _, err := reconcile(t, r, "report-3"); err != nil {
		t.Fatal(err)
	}
	if f.started != 0 {
		t.Fatalf("a failed WorkflowRun started %d runs", f.started)
	}
}

// With no default, resources map to the Conductor namespace named after
// their Kubernetes namespace (created on demand), so same-named resources in
// two Kubernetes namespaces stay apart.
func TestResourcesMapToTheirKubernetesNamespace(t *testing.T) {
	def := raw(`{"steps": [{"name": "s", "run": "true"}]}`)
	a := &v1alpha1.Workflow{ObjectMeta: metav1.ObjectMeta{Name: "nightly", Namespace: "team-a", Generation: 1}, Spec: v1alpha1.WorkflowSpec{Definition: def}}
	b := &v1alpha1.Workflow{ObjectMeta: metav1.ObjectMeta{Name: "nightly", Namespace: "team-b", Generation: 1}, Spec: v1alpha1.WorkflowSpec{Definition: def}}
	f, kc, opts := setup(t, a, b)
	opts.DefaultNamespace, opts.Known = "", &sync.Map{}
	r := &WorkflowReconciler{Client: kc, Options: opts}
	for _, ns := range []string{"team-a", "team-b"} {
		if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "nightly"}}); err != nil {
			t.Fatal(err)
		}
	}
	if !f.namespaces["team-a"] || !f.namespaces["team-b"] {
		t.Fatalf("namespaces = %v, want both created", f.namespaces)
	}
	if len(f.definitions["team-a/nightly"]) != 1 || len(f.definitions["team-b/nightly"]) != 1 || len(f.definitions["default/nightly"]) != 0 {
		t.Fatalf("definitions = %v", f.definitions)
	}
}

func TestScheduleLifecycle(t *testing.T) {
	sch := &v1alpha1.Schedule{
		ObjectMeta: metav1.ObjectMeta{Name: "nightly", Namespace: "apps", Generation: 1},
		Spec: v1alpha1.ScheduleSpec{Cron: "0 3 * * *", ConductorNamespace: "reports",
			Workflow: &v1alpha1.ScheduledWorkflow{Name: "nightly-report", Input: &runtime.RawExtension{Raw: []byte(`{"day": "today"}`)}}},
	}
	f, kc, opts := setup(t, sch)
	r := &ScheduleReconciler{Client: kc, Options: opts}
	get := func() v1alpha1.Schedule {
		var got v1alpha1.Schedule
		_ = kc.Get(context.Background(), client.ObjectKeyFromObject(sch), &got)
		return got
	}

	// Applied before its Workflow (GitOps applies in any order): it waits.
	res, err := reconcile(t, r, "nightly")
	if err != nil || res.RequeueAfter == 0 || len(f.schedules) != 0 {
		t.Fatalf("before the workflow exists: %v, %+v, schedules %v", err, res, f.schedules)
	}
	if c := ready(get().Status.Conditions); c == nil || c.Reason != v1alpha1.ReasonWaiting {
		t.Fatalf("Ready = %+v, want Waiting", c)
	}
	f.definitions["reports/nightly-report"] = []string{"{}"}
	f.definitions["ops/nightly-report"] = []string{"{}"}

	if _, err := reconcile(t, r, "nightly"); err != nil {
		t.Fatal(err)
	}
	got := get()
	if s := f.schedules["reports/nightly"]; s == nil || s["enabled"] != true {
		t.Fatalf("schedules = %v", f.schedules)
	}
	if got.Status.NextRunTime == nil || got.Status.ConductorNamespace != "reports" || ready(got.Status.Conditions).Status != metav1.ConditionTrue {
		t.Fatalf("status = %+v", got.Status)
	}

	// Suspend it, and move it to another Conductor namespace.
	got.Spec.Suspend, got.Spec.ConductorNamespace = true, "ops"
	if err := kc.Update(context.Background(), &got); err != nil {
		t.Fatal(err)
	}
	if _, err := reconcile(t, r, "nightly"); err != nil {
		t.Fatal(err)
	}
	if _, old := f.schedules["reports/nightly"]; old || f.schedules["ops/nightly"]["enabled"] != false {
		t.Fatalf("after the move: %v; want it only in ops, paused", f.schedules)
	}

	// A bad spec is reported on the resource.
	got = get()
	got.Spec.Cron = "bogus"
	if err := kc.Update(context.Background(), &got); err != nil {
		t.Fatal(err)
	}
	if _, err := reconcile(t, r, "nightly"); err != nil {
		t.Fatalf("invalid spec retried: %v", err)
	}
	if c := ready(get().Status.Conditions); c.Reason != v1alpha1.ReasonInvalid {
		t.Fatalf("Ready = %+v", c)
	}

	// Deleting the resource deletes the schedule.
	got = get()
	_ = kc.Delete(context.Background(), &got)
	if _, err := reconcile(t, r, "nightly"); err != nil {
		t.Fatal(err)
	}
	if len(f.schedules) != 0 {
		t.Fatalf("schedules left: %v", f.schedules)
	}
}

func TestScheduleTaskIsCheckedStrictly(t *testing.T) {
	sch := &v1alpha1.Schedule{
		ObjectMeta: metav1.ObjectMeta{Name: "cleanup", Namespace: "apps", Generation: 1},
		Spec: v1alpha1.ScheduleSpec{Cron: "@hourly",
			Task: &runtime.RawExtension{Raw: []byte(`{"command": "rm -rf /tmp/cache", "comand": "typo"}`)}},
	}
	f, kc, opts := setup(t, sch)
	if _, err := reconcile(t, &ScheduleReconciler{Client: kc, Options: opts}, "cleanup"); err != nil {
		t.Fatal(err)
	}
	var got v1alpha1.Schedule
	_ = kc.Get(context.Background(), client.ObjectKeyFromObject(sch), &got)
	if c := ready(got.Status.Conditions); c.Reason != v1alpha1.ReasonInvalid || !strings.Contains(c.Message, "comand") {
		t.Fatalf("Ready = %+v", c)
	}
	if len(f.schedules) != 0 {
		t.Fatal("a misspelled task was applied")
	}
}
