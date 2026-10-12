package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/ChinmayNoob/conductor/operator/api/v1alpha1"
	conductor "github.com/ChinmayNoob/conductor/pkg/client"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// scheduleFinalizer lets a deleted Schedule be removed from Conductor first.
const scheduleFinalizer = "conductor.dev/delete-schedule"

// ScheduleReconciler keeps each Schedule applied in Conductor, and deletes
// it there when the resource goes.
type ScheduleReconciler struct {
	client.Client
	Options
}

// +kubebuilder:rbac:groups=conductor.dev,resources=schedules,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=conductor.dev,resources=schedules/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=conductor.dev,resources=schedules/finalizers,verbs=update

func (r *ScheduleReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var sch v1alpha1.Schedule
	if err := r.Get(ctx, req.NamespacedName, &sch); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	ns := r.namespace(sch.Spec.ConductorNamespace)

	if !sch.DeletionTimestamp.IsZero() {
		if !controllerutil.ContainsFinalizer(&sch, scheduleFinalizer) {
			return ctrl.Result{}, nil
		}
		if err := r.remove(ctx, applied(&sch, ns), sch.Name); err != nil {
			return ctrl.Result{}, err
		}
		controllerutil.RemoveFinalizer(&sch, scheduleFinalizer)
		return ctrl.Result{}, r.Update(ctx, &sch)
	}
	if controllerutil.AddFinalizer(&sch, scheduleFinalizer) {
		if err := r.Update(ctx, &sch); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Moved to another Conductor namespace: remove the old one first, or
	// it would keep firing.
	if old := sch.Status.ConductorNamespace; old != "" && old != ns {
		if err := r.remove(ctx, old, sch.Name); err != nil {
			return ctrl.Result{}, err
		}
		sch.Status.ConductorNamespace = ""
	}

	err := r.apply(ctx, &sch, ns)
	var wait *waitingError
	switch {
	case errors.As(err, &wait):
		// Applied in any order (GitOps): the workflow will come.
		setReady(&sch.Status.Conditions, sch.Generation, false, v1alpha1.ReasonWaiting, wait.Error())
	case err != nil:
		failure(&sch.Status.Conditions, sch.Generation, err)
		log.FromContext(ctx).Info("Schedule not applied", "error", err.Error())
	}
	if serr := r.Status().Update(ctx, &sch); serr != nil {
		return ctrl.Result{}, serr
	}
	switch {
	case wait != nil:
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	case err != nil && !isPermanent(err):
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: r.Resync}, nil
}

// waitingError means a resource needs another to exist first.
type waitingError struct{ what string }

func (e *waitingError) Error() string { return "waiting for " + e.what + " to exist" }

// applied returns the namespace the schedule was last applied in.
func applied(sch *v1alpha1.Schedule, fallback string) string {
	if sch.Status.ConductorNamespace != "" {
		return sch.Status.ConductorNamespace
	}
	return fallback
}

func (r *ScheduleReconciler) remove(ctx context.Context, namespace, name string) error {
	err := r.client(namespace).DeleteSchedule(ctx, name)
	if err != nil && conductor.StatusCode(err) != http.StatusNotFound {
		return err
	}
	return nil
}

func (r *ScheduleReconciler) apply(ctx context.Context, sch *v1alpha1.Schedule, ns string) error {
	enabled := !sch.Spec.Suspend
	req := conductor.ScheduleRequest{
		Name: sch.Name, Cron: sch.Spec.Cron, Timezone: sch.Spec.Timezone,
		MisfirePolicy: sch.Spec.MisfirePolicy, Enabled: &enabled,
	}
	if w := sch.Spec.Workflow; w != nil {
		input, err := object(w.Input, "workflow.input")
		if err != nil {
			return err
		}
		// Conductor refuses schedules of unknown workflows; its Workflow
		// resource may simply not be applied yet.
		if _, err := r.client(ns).GetDefinition(ctx, w.Name, w.Version); conductor.IsNotFound(err) {
			return &waitingError{fmt.Sprintf("workflow %q", w.Name)}
		} else if err != nil {
			return err
		}
		req.Workflow = &conductor.ScheduledWorkflow{Name: w.Name, Version: w.Version}
		if input != nil {
			req.Workflow.Input = input
		}
	}
	if sch.Spec.Task != nil {
		// Strictly: a misspelled field should fail here, not be dropped.
		dec := json.NewDecoder(bytes.NewReader(sch.Spec.Task.Raw))
		dec.DisallowUnknownFields()
		req.Task = &conductor.TaskRequest{}
		if err := dec.Decode(req.Task); err != nil {
			return fmt.Errorf("%w: task: %w", errInvalidSpec, err)
		}
	}

	s, created, err := r.client(ns).PutSchedule(ctx, req)
	if err != nil {
		return err
	}
	if created {
		log.FromContext(ctx).Info("Created schedule", "name", s.Name, "namespace", ns)
	}
	sch.Status.ObservedGeneration = sch.Generation
	sch.Status.ConductorNamespace = ns
	sch.Status.NextRunTime, sch.Status.LastRunTime = timePtr(s.NextRunAt), timePtr(s.LastRunAt)
	sch.Status.LastRunID, sch.Status.LastError = s.LastRunID, s.LastError
	msg := "suspended"
	if s.NextRunAt != nil {
		msg = "next run " + s.NextRunAt.UTC().Format("2006-01-02 15:04:05 MST")
	}
	setReady(&sch.Status.Conditions, sch.Generation, true, v1alpha1.ReasonApplied, msg)
	return nil
}

func (r *ScheduleReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.Schedule{}, builder.WithPredicates(specChanges)).
		Named("schedule").
		Complete(r)
}
