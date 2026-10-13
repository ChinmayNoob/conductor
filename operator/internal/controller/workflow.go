package controller

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ChinmayNoob/conductor/operator/api/v1alpha1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// WorkflowReconciler keeps each Workflow's definition applied in Conductor.
// Applying an unchanged definition is a no-op there; a changed one becomes
// a new version.
type WorkflowReconciler struct {
	client.Client
	Options
}

// +kubebuilder:rbac:groups=conductor.dev,resources=workflows,verbs=get;list;watch
// +kubebuilder:rbac:groups=conductor.dev,resources=workflows/status,verbs=get;update;patch

func (r *WorkflowReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var wf v1alpha1.Workflow
	if err := r.Get(ctx, req.NamespacedName, &wf); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !wf.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil // Conductor keeps definitions: past runs refer to them
	}

	err := r.apply(ctx, &wf)
	if err != nil {
		failure(&wf.Status.Conditions, wf.Generation, err)
		log.FromContext(ctx).Info("Workflow not applied", "error", err.Error())
	}
	if serr := r.Status().Update(ctx, &wf); serr != nil {
		return ctrl.Result{}, serr
	}
	if err != nil && !isPermanent(err) {
		return ctrl.Result{}, err // retried with back-off
	}
	return ctrl.Result{RequeueAfter: r.Resync}, nil
}

func (r *WorkflowReconciler) apply(ctx context.Context, wf *v1alpha1.Workflow) error {
	def, err := object(&wf.Spec.Definition, "definition")
	if err != nil {
		return err
	}
	if def == nil {
		def = map[string]any{}
	}
	if _, ok := def["name"]; !ok {
		def["name"] = wf.Name
	}
	body, err := json.Marshal(def)
	if err != nil {
		return fmt.Errorf("%w: %w", errInvalidSpec, err)
	}
	ns := r.namespace(wf, wf.Spec.ConductorNamespace)
	if err := r.ensureNamespace(ctx, ns); err != nil {
		return err
	}
	d, err := r.client(ns).ApplyDefinition(ctx, body)
	if err != nil {
		return err
	}
	if d.Created {
		log.FromContext(ctx).Info("Applied a new workflow version", "name", d.Name, "version", d.Version)
	}
	wf.Status.ObservedGeneration = wf.Generation
	wf.Status.Name, wf.Status.Version = d.Name, d.Version
	setReady(&wf.Status.Conditions, wf.Generation, true, v1alpha1.ReasonApplied, fmt.Sprintf("version %d", d.Version))
	return nil
}

func (r *WorkflowReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.Workflow{}, builder.WithPredicates(specChanges)).
		Named("workflow").
		Complete(r)
}
