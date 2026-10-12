// Package controller reconciles the operator's resources with Conductor
// through its HTTP API.
package controller

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/ChinmayNoob/conductor/operator/api/v1alpha1"
	conductor "github.com/ChinmayNoob/conductor/pkg/client"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

// Options configure every controller.
type Options struct {
	// Conductor is a client for its API, with a key allowed to act in every
	// namespace the resources name (an admin key).
	Conductor *conductor.Client
	// DefaultNamespace is the Conductor namespace for resources that don't
	// name one.
	DefaultNamespace string
	// PollInterval is how often unfinished WorkflowRuns are checked.
	PollInterval time.Duration
	// Resync is how often applied Workflows and Schedules are re-applied
	// (repairing drift) and their status refreshed.
	Resync time.Duration
}

func (o Options) namespace(spec string) string {
	if spec != "" {
		return spec
	}
	if o.DefaultNamespace != "" {
		return o.DefaultNamespace
	}
	return "default"
}

func (o Options) client(namespace string) *conductor.Client {
	return o.Conductor.WithNamespace(namespace)
}

// rejected reports whether Conductor refused the request as invalid, so
// retrying the same spec can't help.
func rejected(err error) bool {
	code := conductor.StatusCode(err)
	return code == http.StatusBadRequest || code == http.StatusUnprocessableEntity
}

// errInvalidSpec wraps spec problems found before calling Conductor.
var errInvalidSpec = errors.New("invalid spec")

// isPermanent reports errors only a spec change can fix: no point retrying.
func isPermanent(err error) bool {
	return errors.Is(err, errInvalidSpec) || rejected(err)
}

// setReady sets the Ready condition.
func setReady(conds *[]metav1.Condition, generation int64, ready bool, reason, message string) {
	status := metav1.ConditionFalse
	if ready {
		status = metav1.ConditionTrue
	}
	meta.SetStatusCondition(conds, metav1.Condition{
		Type: v1alpha1.ConditionReady, Status: status, Reason: reason, Message: message, ObservedGeneration: generation,
	})
}

// failure records why a resource isn't applied: invalid (fix the spec) or
// unreachable (Conductor failed; retry).
func failure(conds *[]metav1.Condition, generation int64, err error) {
	if isPermanent(err) {
		setReady(conds, generation, false, v1alpha1.ReasonInvalid, err.Error())
		return
	}
	setReady(conds, generation, false, v1alpha1.ReasonUnreachable, err.Error())
}

// object decodes a free-form field (definition, input, task); nil stays nil.
func object(raw *runtime.RawExtension, field string) (map[string]any, error) {
	if raw == nil || len(raw.Raw) == 0 {
		return nil, nil
	}
	var v map[string]any
	if err := json.Unmarshal(raw.Raw, &v); err != nil {
		return nil, fmt.Errorf("%w: %s must be an object: %w", errInvalidSpec, field, err)
	}
	return v, nil
}

// specChanges passes events that need reconciling: a changed spec (new
// generation) or a deletion starting. Status and finalizer updates don't.
var specChanges = predicate.Or(predicate.GenerationChangedPredicate{}, predicate.Funcs{
	UpdateFunc: func(e event.UpdateEvent) bool {
		return e.ObjectNew.GetDeletionTimestamp() != nil
	},
})

func timePtr(t *time.Time) *metav1.Time {
	if t == nil {
		return nil
	}
	mt := metav1.NewTime(*t)
	return &mt
}
