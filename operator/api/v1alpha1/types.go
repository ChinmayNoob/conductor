package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// --- Workflow ---

// WorkflowSpec is a workflow definition to keep applied in Conductor.
type WorkflowSpec struct {
	// ConductorNamespace is the Conductor namespace to apply it in; default
	// the operator's (--default-namespace).
	// +optional
	ConductorNamespace string `json:"conductorNamespace,omitempty"`

	// Definition is the workflow, in the format `conductorctl workflow
	// apply` takes (steps, inputs, defaults, budget). Its name defaults to
	// the resource's. Conductor validates it; problems show in the Ready
	// condition.
	// +kubebuilder:validation:Type=object
	// +kubebuilder:pruning:PreserveUnknownFields
	Definition runtime.RawExtension `json:"definition"`
}

// WorkflowStatus reports the applied definition.
type WorkflowStatus struct {
	// ObservedGeneration is the generation last applied.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// Name of the definition in Conductor.
	// +optional
	Name string `json:"name,omitempty"`
	// Version is the definition version in Conductor; a changed spec makes
	// a new one, and new runs use it.
	// +optional
	Version int `json:"version,omitempty"`
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// Workflow is a Conductor workflow definition, managed declaratively.
// Deleting it stops managing the definition; Conductor keeps its versions,
// which past runs refer to.
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=cwf;cwfs,categories=conductor
// +kubebuilder:printcolumn:name="Version",type=integer,JSONPath=`.status.version`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type Workflow struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   WorkflowSpec   `json:"spec"`
	Status WorkflowStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// WorkflowList is a list of Workflows.
type WorkflowList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Workflow `json:"items"`
}

// --- WorkflowRun ---

// WorkflowRunSpec starts one run of a workflow. It can't change once
// created: make a new WorkflowRun to run again.
// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="spec is immutable; create another WorkflowRun"
type WorkflowRunSpec struct {
	// +optional
	ConductorNamespace string `json:"conductorNamespace,omitempty"`

	// Workflow is the definition's name in Conductor (a Workflow's
	// definition name). The run waits for it to exist.
	// +kubebuilder:validation:MinLength=1
	Workflow string `json:"workflow"`

	// Version of the definition; default the latest.
	// +optional
	// +kubebuilder:validation:Minimum=0
	Version int `json:"version,omitempty"`

	// Input for the run's declared inputs.
	// +optional
	// +kubebuilder:validation:Type=object
	// +kubebuilder:pruning:PreserveUnknownFields
	Input *runtime.RawExtension `json:"input,omitempty"`

	// KeepRunningOnDelete leaves the run going when this resource is
	// deleted; by default an unfinished run is cancelled (and compensated).
	// +optional
	KeepRunningOnDelete bool `json:"keepRunningOnDelete,omitempty"`
}

// StepStatus is one step of a run.
type StepStatus struct {
	Name   string `json:"name"`
	Status string `json:"status"`
	// +optional
	Error string `json:"error,omitempty"`
}

// WorkflowRunStatus follows the run in Conductor.
type WorkflowRunStatus struct {
	// RunID is the run's ID in Conductor.
	// +optional
	RunID string `json:"runID,omitempty"`
	// Phase is the run's status: RUNNING, COMPENSATING, COMPLETED, FAILED
	// or CANCELLED (empty until it starts).
	// +optional
	Phase string `json:"phase,omitempty"`
	// +optional
	Error string `json:"error,omitempty"`
	// +optional
	Steps []StepStatus `json:"steps,omitempty"`
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// Finished reports whether the run reached a final status.
func (s *WorkflowRunStatus) Finished() bool {
	return s.Phase == "COMPLETED" || s.Phase == "FAILED" || s.Phase == "CANCELLED"
}

// WorkflowRun is one run of a Conductor workflow. It starts exactly once,
// and its status follows the run until it finishes.
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=cwr;cwrs,categories=conductor
// +kubebuilder:printcolumn:name="Workflow",type=string,JSONPath=`.spec.workflow`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Run",type=string,JSONPath=`.status.runID`,priority=1
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type WorkflowRun struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   WorkflowRunSpec   `json:"spec"`
	Status WorkflowRunStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// WorkflowRunList is a list of WorkflowRuns.
type WorkflowRunList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []WorkflowRun `json:"items"`
}

// --- Schedule ---

// ScheduledWorkflow is a workflow a schedule starts.
type ScheduledWorkflow struct {
	// Name of the definition in Conductor.
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
	// +optional
	Version int `json:"version,omitempty"`
	// +optional
	// +kubebuilder:validation:Type=object
	// +kubebuilder:pruning:PreserveUnknownFields
	Input *runtime.RawExtension `json:"input,omitempty"`
}

// ScheduleSpec runs a workflow or a task on a cron schedule.
// +kubebuilder:validation:XValidation:rule="has(self.workflow) != has(self.task)",message="set exactly one of workflow and task"
type ScheduleSpec struct {
	// +optional
	ConductorNamespace string `json:"conductorNamespace,omitempty"`

	// Cron is a cron expression ("*/5 * * * *") or a descriptor such as
	// "@hourly" or "@every 30s".
	// +kubebuilder:validation:MinLength=1
	Cron string `json:"cron"`

	// Timezone for the cron expression, e.g. Europe/Berlin; default UTC.
	// +optional
	Timezone string `json:"timezone,omitempty"`

	// MisfirePolicy decides what happens to runs missed while Conductor was
	// down: skip them, run once, or catch up on each.
	// +optional
	// +kubebuilder:validation:Enum=skip;run_once;catch_up
	MisfirePolicy string `json:"misfirePolicy,omitempty"`

	// Suspend pauses the schedule.
	// +optional
	Suspend bool `json:"suspend,omitempty"`

	// +optional
	Workflow *ScheduledWorkflow `json:"workflow,omitempty"`

	// Task to submit, as POST /v1/tasks takes it (type, command, k8s, ...).
	// +optional
	// +kubebuilder:validation:Type=object
	// +kubebuilder:pruning:PreserveUnknownFields
	Task *runtime.RawExtension `json:"task,omitempty"`
}

// ScheduleStatus reports the schedule in Conductor.
type ScheduleStatus struct {
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// ConductorNamespace is where the schedule was applied, so it can be
	// removed from there if the spec moves it.
	// +optional
	ConductorNamespace string `json:"conductorNamespace,omitempty"`
	// +optional
	NextRunTime *metav1.Time `json:"nextRunTime,omitempty"`
	// +optional
	LastRunTime *metav1.Time `json:"lastRunTime,omitempty"`
	// LastRunID is the task or workflow run it last started.
	// +optional
	LastRunID string `json:"lastRunID,omitempty"`
	// +optional
	LastError string `json:"lastError,omitempty"`
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// Schedule is a Conductor cron schedule. Deleting it deletes the schedule.
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=csch,categories=conductor
// +kubebuilder:printcolumn:name="Cron",type=string,JSONPath=`.spec.cron`
// +kubebuilder:printcolumn:name="Suspended",type=boolean,JSONPath=`.spec.suspend`
// +kubebuilder:printcolumn:name="Next run",type=string,JSONPath=`.status.nextRunTime`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
type Schedule struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ScheduleSpec   `json:"spec"`
	Status ScheduleStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ScheduleList is a list of Schedules.
type ScheduleList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Schedule `json:"items"`
}
