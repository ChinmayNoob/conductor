// Package v1alpha1 holds the Conductor operator's resources: Workflow
// (a workflow definition), WorkflowRun (one run of it) and Schedule (cron).
//
// +kubebuilder:object:generate=true
// +groupName=conductor.dev
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	// GroupVersion is the API group and version of these resources.
	GroupVersion = schema.GroupVersion{Group: "conductor.dev", Version: "v1alpha1"}

	// SchemeBuilder registers the types with a runtime scheme.
	SchemeBuilder = runtime.NewSchemeBuilder(func(s *runtime.Scheme) error {
		s.AddKnownTypes(GroupVersion,
			&Workflow{}, &WorkflowList{}, &WorkflowRun{}, &WorkflowRunList{}, &Schedule{}, &ScheduleList{})
		metav1.AddToGroupVersion(s, GroupVersion)
		return nil
	})

	// AddToScheme adds the types to a scheme.
	AddToScheme = SchemeBuilder.AddToScheme
)

// Condition types and reasons the controllers set.
const (
	// ConditionReady: the resource is applied in Conductor (for a
	// WorkflowRun: the run was started).
	ConditionReady = "Ready"

	ReasonApplied     = "Applied"
	ReasonInvalid     = "Invalid"     // Conductor rejected it; fix the spec
	ReasonUnreachable = "Unreachable" // Conductor's API failed; retrying
	ReasonWaiting     = "Waiting"     // e.g. for its workflow's definition
	ReasonStarted     = "Started"
)
