package controller

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"time"

	"github.com/ChinmayNoob/conductor/operator/api/v1alpha1"
	conductor "github.com/ChinmayNoob/conductor/pkg/client"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// runFinalizer lets a deleted WorkflowRun cancel its run first.
const runFinalizer = "conductor.dev/cancel-run"

// WorkflowRunReconciler starts each WorkflowRun's run once and follows it
// until it finishes.
type WorkflowRunReconciler struct {
	client.Client
	Options
}

// +kubebuilder:rbac:groups=conductor.dev,resources=workflowruns,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=conductor.dev,resources=workflowruns/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=conductor.dev,resources=workflowruns/finalizers,verbs=update

func (r *WorkflowRunReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var run v1alpha1.WorkflowRun
	if err := r.Get(ctx, req.NamespacedName, &run); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	cc := r.client(r.namespace(run.Spec.ConductorNamespace))

	if !run.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, r.finalize(ctx, cc, &run)
	}
	if controllerutil.AddFinalizer(&run, runFinalizer) {
		if err := r.Update(ctx, &run); err != nil {
			return ctrl.Result{}, err
		}
	}
	before := run.Status.DeepCopy()

	if run.Status.RunID == "" {
		result, err := r.start(ctx, cc, &run)
		if err != nil || run.Status.RunID == "" {
			if serr := r.updateStatus(ctx, &run, before); serr != nil {
				return ctrl.Result{}, serr
			}
			return result, err
		}
	}

	wf, err := cc.GetWorkflow(ctx, run.Status.RunID)
	switch {
	case conductor.IsNotFound(err):
		setReady(&run.Status.Conditions, run.Generation, false, v1alpha1.ReasonUnreachable,
			"run "+run.Status.RunID+" no longer exists in Conductor")
		return ctrl.Result{RequeueAfter: r.Resync}, r.updateStatus(ctx, &run, before)
	case err != nil:
		return ctrl.Result{}, err
	}
	follow(&run.Status, wf)
	if err := r.updateStatus(ctx, &run, before); err != nil {
		return ctrl.Result{}, err
	}
	if run.Status.Finished() {
		return ctrl.Result{}, nil
	}
	return ctrl.Result{RequeueAfter: r.PollInterval}, nil
}

// start starts the run. The resource's UID is the idempotency key, so a
// retry after a failed status update finds the same run instead of
// starting another.
func (r *WorkflowRunReconciler) start(ctx context.Context, cc *conductor.Client, run *v1alpha1.WorkflowRun) (ctrl.Result, error) {
	input, err := object(run.Spec.Input, "input")
	if err != nil {
		r.fail(run, err)
		return ctrl.Result{}, nil
	}
	req := conductor.WorkflowRequest{
		Workflow: run.Spec.Workflow, Version: run.Spec.Version, IdempotencyKey: "k8s-" + string(run.UID),
	}
	if input != nil {
		req.Input = input
	}
	wf, err := cc.StartWorkflow(ctx, req)
	switch {
	case conductor.IsNotFound(err):
		// Applied in any order (GitOps): wait for the definition.
		setReady(&run.Status.Conditions, run.Generation, false, v1alpha1.ReasonWaiting,
			fmt.Sprintf("waiting for workflow %q to exist", run.Spec.Workflow))
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	case rejected(err):
		r.fail(run, err) // e.g. a missing input: this spec can never run
		return ctrl.Result{}, nil
	case err != nil:
		failure(&run.Status.Conditions, run.Generation, err)
		return ctrl.Result{}, err
	}
	if wf.Created {
		log.FromContext(ctx).Info("Started workflow run", "workflow", run.Spec.Workflow, "run", wf.ID)
	}
	run.Status.RunID = wf.ID
	now := metav1.Now()
	run.Status.StartTime = &now
	setReady(&run.Status.Conditions, run.Generation, true, v1alpha1.ReasonStarted, "run "+wf.ID)
	return ctrl.Result{}, nil
}

// fail marks a run that can't start as failed for good.
func (r *WorkflowRunReconciler) fail(run *v1alpha1.WorkflowRun, err error) {
	run.Status.Phase, run.Status.Error = "FAILED", err.Error()
	now := metav1.Now()
	run.Status.CompletionTime = &now
	setReady(&run.Status.Conditions, run.Generation, false, v1alpha1.ReasonInvalid, err.Error())
}

// follow copies the run's progress into the status.
func follow(s *v1alpha1.WorkflowRunStatus, wf *conductor.Workflow) {
	s.Phase, s.Error = wf.Status, wf.ErrorMessage
	s.Steps = s.Steps[:0]
	for _, st := range wf.Steps {
		s.Steps = append(s.Steps, v1alpha1.StepStatus{Name: st.Name, Status: st.Status, Error: st.Error})
	}
	if s.Finished() && s.CompletionTime == nil {
		s.CompletionTime = timePtr(&wf.UpdatedAt)
	}
}

// finalize cancels an unfinished run (unless told to keep it) and lets the
// resource go.
func (r *WorkflowRunReconciler) finalize(ctx context.Context, cc *conductor.Client, run *v1alpha1.WorkflowRun) error {
	if !controllerutil.ContainsFinalizer(run, runFinalizer) {
		return nil
	}
	if run.Status.RunID != "" && !run.Status.Finished() && !run.Spec.KeepRunningOnDelete {
		_, err := cc.CancelWorkflow(ctx, run.Status.RunID)
		if code := conductor.StatusCode(err); err != nil && code != http.StatusNotFound && code != http.StatusConflict {
			return err
		}
		log.FromContext(ctx).Info("Cancelled the run of a deleted WorkflowRun", "run", run.Status.RunID)
	}
	controllerutil.RemoveFinalizer(run, runFinalizer)
	return r.Update(ctx, run)
}

func (r *WorkflowRunReconciler) updateStatus(ctx context.Context, run *v1alpha1.WorkflowRun, before *v1alpha1.WorkflowRunStatus) error {
	if reflect.DeepEqual(before, &run.Status) {
		return nil
	}
	return r.Status().Update(ctx, run)
}

func (r *WorkflowRunReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.WorkflowRun{}, builder.WithPredicates(specChanges)).
		Named("workflowrun").
		Complete(r)
}
