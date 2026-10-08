package coordinator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"time"

	"github.com/ChinmayNoob/conductor/examples"
	"github.com/ChinmayNoob/conductor/pkg/db"
	"github.com/ChinmayNoob/conductor/pkg/grpcapi"
	"github.com/ChinmayNoob/conductor/pkg/metrics"
	"github.com/ChinmayNoob/conductor/pkg/task"
	"github.com/ChinmayNoob/conductor/pkg/tracing"
	"github.com/ChinmayNoob/conductor/pkg/workflow"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// SubmitWorkflow starts a run of a stored workflow definition.
func (s *Server) SubmitWorkflow(ctx context.Context, req *grpcapi.WorkflowRequest) (*grpcapi.WorkflowResponse, error) {
	ns := namespaceOf(req.Namespace)
	if _, err := s.checkNamespace(ctx, ns, false); err != nil {
		return nil, err
	}
	wfID, created, err := s.startWorkflow(ctx, s.db, ns, req.Name, int(req.Version), json.RawMessage(req.InputJson), req.IdempotencyKey)
	if err != nil {
		return nil, err
	}
	if created {
		s.reconcile(ctx, wfID)
	}
	return &grpcapi.WorkflowResponse{WorkflowId: wfID.String(), Created: created}, nil
}

// startWorkflow creates a run with its steps; the caller reconciles it to
// start the first steps. q may be a transaction.
func (s *Server) startWorkflow(ctx context.Context, q *db.DB, ns, name string, version int, input json.RawMessage, idemKey string) (uuid.UUID, bool, error) {
	stored, err := q.GetDefinition(ctx, ns, name, version)
	if err != nil {
		return uuid.Nil, false, status.Error(codes.Internal, "failed to load workflow definition")
	}
	if stored == nil {
		if version > 0 {
			return uuid.Nil, false, status.Errorf(codes.NotFound, "workflow %q has no version %d", name, version)
		}
		return uuid.Nil, false, status.Errorf(codes.NotFound, "no workflow definition named %q", name)
	}
	def, err := workflow.FromJSON(stored.Spec)
	if err != nil {
		return uuid.Nil, false, status.Error(codes.Internal, "stored workflow definition is invalid")
	}
	inputs, err := def.PrepareInputs(input)
	if err != nil {
		return uuid.Nil, false, status.Error(codes.InvalidArgument, "invalid workflow input: "+err.Error())
	}
	normalized, err := json.Marshal(inputs)
	if err != nil {
		return uuid.Nil, false, status.Error(codes.Internal, err.Error())
	}

	names := make([]string, len(def.Steps))
	for i, st := range def.Steps {
		names[i] = st.Name
	}
	// Every step's spans join this one's trace.
	ctx, span := tracing.Start(ctx, "workflow "+name, trace.WithAttributes(attribute.String("conductor.workflow", name)))
	defer span.End()
	wf, created, err := q.CreateWorkflow(ctx, db.NewWorkflow{
		Namespace:         ns,
		Name:              name,
		DefinitionVersion: stored.Version,
		Definition:        stored.Spec,
		Input:             normalized,
		Steps:             names,
		IdempotencyKey:    idemKey,
		TraceParent:       tracing.TraceParent(ctx),
	})
	if err != nil {
		s.log.Error("Failed to create workflow", "name", name, "error", err)
		return uuid.Nil, false, status.Error(codes.Internal, "failed to create workflow")
	}
	if created {
		s.log.Info("Workflow started", "workflow_id", wf.ID, "name", name, "version", stored.Version, "steps", len(names))
	}
	return wf.ID, created, nil
}

// CancelWorkflow stops a run: running steps are cancelled, and completed
// steps are compensated, after which the run is CANCELLED.
func (s *Server) CancelWorkflow(ctx context.Context, req *grpcapi.CancelWorkflowRequest) (*grpcapi.CancelWorkflowResponse, error) {
	wfID, err := uuid.Parse(req.WorkflowId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid workflow ID")
	}
	wf, err := s.db.GetWorkflow(ctx, wfID)
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to look up workflow")
	}
	if wf == nil || wf.Namespace != namespaceOf(req.Namespace) {
		return nil, status.Error(codes.NotFound, "workflow not found")
	}
	ok, err := s.db.RequestWorkflowCancel(ctx, wfID)
	if err != nil {
		s.log.Error("Failed to cancel workflow", "workflow_id", wfID, "error", err)
		return nil, status.Error(codes.Internal, "failed to cancel workflow")
	}
	if !ok {
		return &grpcapi.CancelWorkflowResponse{Cancelled: false}, nil // already finished
	}
	s.log.Info("Workflow cancellation requested", "workflow_id", wfID)
	s.reconcile(context.WithoutCancel(ctx), wfID)
	return &grpcapi.CancelWorkflowResponse{Cancelled: true}, nil
}

// reconcile advances a workflow run as far as it can go right now. It loads
// the run under a row lock, asks workflow.Reconcile what to do, applies that
// plan, and repeats until the plan is empty. The lock serializes reconciles of
// the same run, e.g. when two parallel steps finish at once.
func (s *Server) reconcile(ctx context.Context, wfID uuid.UUID) {
	log := s.log.With("workflow_id", wfID)
	var toKill []uuid.UUID
	createdTasks := false
	var finished db.WorkflowStatus // set when this pass ends the run
	compensations := 0
	namespace := ""

	err := s.fenced(ctx, func(tx *db.DB) error {
		wf, err := tx.LockWorkflow(ctx, wfID)
		if err != nil || wf == nil || wf.Status.Terminal() {
			return err
		}
		namespace = wf.Namespace
		finished, compensations = "", 0 // a retried transaction starts over
		if wf.Definition == nil {
			log.Warn("Workflow predates workflow definitions and cannot continue")
			return tx.UpdateWorkflowStatus(ctx, wf.ID, db.WorkflowFailed, "created by an older version without a definition")
		}
		def, err := workflow.FromJSON(wf.Definition)
		if err != nil {
			return fmt.Errorf("invalid definition snapshot: %w", err)
		}
		var inputs map[string]string
		if err := json.Unmarshal(wf.Input, &inputs); err != nil {
			return fmt.Errorf("invalid workflow input: %w", err)
		}

		for range 50 {
			states, err := tx.GetStepStates(ctx, wf.ID)
			if err != nil {
				return err
			}
			byName := make(map[string]*db.StepState, len(states))
			run := workflow.RunState{Status: string(wf.Status), CancelRequested: wf.CancelRequested,
				Steps: make(map[string]workflow.StepState, len(states))}
			outputs := make(map[string]map[string]string)
			for _, st := range states {
				byName[st.Name] = st
				run.Steps[st.Name] = workflow.StepState{
					Status:             string(st.Status),
					TaskStatus:         st.TaskStatus,
					CompensationStatus: st.CompensationStatus,
					Error:              st.TaskError,
				}
				outputs[st.Name] = st.Outputs
			}

			if def.Budget != nil && wf.Status == db.WorkflowRunning {
				spend, err := tx.WorkflowSpend(ctx, wf.ID)
				if err != nil {
					return err
				}
				run.Abort = def.Budget.Exceeded(spend.Tokens, spend.CostUSD)
			}
			plan := workflow.Reconcile(def, run)
			if plan.Empty() {
				return tx.TouchWorkflow(ctx, wf.ID)
			}

			for name, st := range plan.SetStep {
				if err := tx.UpdateStepStatus(ctx, byName[name].ID, db.StepStatus(st)); err != nil {
					return err
				}
			}
			if plan.SetStatus != "" {
				errMsg := wf.ErrorMessage
				switch {
				case plan.SetStatus == workflow.StatusCompleted:
					errMsg = ""
				case plan.Error != "":
					// Set when the run starts failing. With nothing to undo it
					// goes straight to FAILED in the same plan, so take the
					// error whatever the new status is.
					errMsg = plan.Error
				}
				if err := tx.UpdateWorkflowStatus(ctx, wf.ID, db.WorkflowStatus(plan.SetStatus), errMsg); err != nil {
					return err
				}
				wf.Status, wf.ErrorMessage = db.WorkflowStatus(plan.SetStatus), errMsg
				if wf.Status.Terminal() {
					finished = wf.Status
				}
				log.Info("Workflow status changed", "status", plan.SetStatus, "error", errMsg)
			}

			exprCtx := workflow.Context{WorkflowID: wf.ID.String(), Inputs: inputs, Outputs: outputs}
			for _, name := range plan.Start {
				step := byName[name]
				spec, err := def.StepTask(name, exprCtx)
				if err != nil {
					// The step can't even be built (e.g. a missing output), so
					// it fails without running.
					log.Warn("Cannot start step", "step", name, "error", err)
					if err := tx.UpdateStepStatus(ctx, step.ID, workflow.StepFailed); err != nil {
						return err
					}
					wf.Status, wf.ErrorMessage = db.WorkflowCompensating, fmt.Sprintf("step %s: %v", name, err)
					if err := tx.UpdateWorkflowStatus(ctx, wf.ID, wf.Status, wf.ErrorMessage); err != nil {
						return err
					}
					continue
				}
				if spec.Agent != nil {
					if err := s.startAgent(ctx, tx, wf, step, spec); err != nil {
						return err
					}
					createdTasks = true
					log.Info("Workflow step started an agent", "step", name)
					continue
				}
				if spec.Wait != nil {
					if err := s.startWait(ctx, tx, wf, step, spec.Wait); err != nil {
						return err
					}
					log.Info("Workflow step waiting", "step", name, "for", spec.Wait.Kind)
					continue
				}
				if spec.Type == workflow.TypeLLM {
					msg, err := s.llmBudgetError(ctx, tx, wf.Namespace)
					if err != nil {
						return err
					}
					if msg != "" {
						log.Warn("Cannot start step", "step", name, "error", msg)
						if err := tx.UpdateStepStatus(ctx, step.ID, workflow.StepFailed); err != nil {
							return err
						}
						wf.Status, wf.ErrorMessage = db.WorkflowCompensating, fmt.Sprintf("step %s: %s", name, msg)
						if err := tx.UpdateWorkflowStatus(ctx, wf.ID, wf.Status, wf.ErrorMessage); err != nil {
							return err
						}
						continue
					}
				}
				t, err := s.createStepTask(ctx, tx, wf, spec)
				if err != nil {
					return err
				}
				if err := tx.StartStep(ctx, step.ID, t.ID); err != nil {
					return err
				}
				createdTasks = true
				log.Info("Workflow step started", "step", name, "task_id", t.ID)
			}

			for _, name := range plan.Compensate {
				step := byName[name]
				spec, err := def.CompensationTask(name, exprCtx)
				if err != nil || spec == nil {
					log.Error("Cannot compensate step", "step", name, "error", err)
					if err := tx.UpdateStepStatus(ctx, step.ID, workflow.StepCompensationFailed); err != nil {
						return err
					}
					continue
				}
				t, err := s.createStepTask(ctx, tx, wf, spec)
				if err != nil {
					return err
				}
				if err := tx.StartCompensation(ctx, step.ID, t.ID); err != nil {
					return err
				}
				createdTasks = true
				compensations++
				log.Info("Compensating workflow step", "step", name, "task_id", t.ID)
			}

			for _, name := range plan.Cancel {
				step := byName[name]
				if step.WaitKind != "" {
					// A waiting step has no task: just stop waiting.
					if err := cancelWait(ctx, tx, step); err != nil {
						return err
					}
					continue
				}
				if step.AgentRunID != nil {
					kill, err := cancelAgent(ctx, tx, *step.AgentRunID)
					if err != nil {
						return err
					}
					toKill = append(toKill, kill...)
					continue
				}
				if step.TaskID == nil {
					continue
				}
				before, err := tx.CancelTask(ctx, *step.TaskID)
				if err != nil {
					return err
				}
				if before == nil {
					// The task finished meanwhile; the next pass picks up its
					// result.
					continue
				}
				if err := tx.UpdateStepStatus(ctx, step.ID, workflow.StepCancelled); err != nil {
					return err
				}
				if before.PickedAt != nil {
					toKill = append(toKill, *step.TaskID)
				}
				log.Info("Workflow step cancelled", "step", name)
			}
		}
		return fmt.Errorf("workflow did not settle after 50 passes")
	})
	if err != nil {
		if !errors.Is(err, errNotLeader) && ctx.Err() == nil {
			log.Error("Failed to advance workflow", "error", err)
		}
		return
	}

	if finished != "" {
		metrics.WorkflowsFinished.WithLabelValues(namespace, string(finished)).Inc()
	}
	metrics.StepsCompensated.Add(float64(compensations))
	for _, id := range toKill {
		s.killOnWorker(ctx, id)
	}
	if createdTasks {
		s.wakeDispatcher()
	}
}

// createStepTask inserts the task for a step or compensation.
func (s *Server) createStepTask(ctx context.Context, tx *db.DB, wf *db.Workflow, spec *workflow.TaskSpec) (*db.Task, error) {
	n := db.NewTask{
		Namespace:         wf.Namespace,
		Queue:             spec.Queue,
		Type:              spec.Type,
		Data:              spec.Command,
		Env:               spec.Env,
		Requirements:      task.Requirements(spec.Type, spec.Labels),
		Priority:          spec.Priority,
		MaxRetries:        spec.Retries,
		RetryDelaySeconds: max(1, int(math.Ceil(spec.RetryDelay.Seconds()))),
		TimeoutSeconds:    max(1, int(math.Ceil(spec.Timeout.Seconds()))),
		WorkflowID:        &wf.ID,
		TraceParent:       wf.TraceParent,
	}
	var detail any
	switch {
	case spec.HTTP != nil:
		detail = spec.HTTP
	case spec.Container != nil:
		detail = spec.Container
	case spec.LLM != nil:
		detail = spec.LLM
	}
	if detail != nil {
		b, err := json.Marshal(detail)
		if err != nil {
			return nil, err
		}
		n.Spec = b
	}
	t, _, err := tx.CreateTask(ctx, n)
	return t, err
}

// workflowSweepLoop periodically reconciles every active run. Normally runs
// advance as soon as a task finishes; this catches anything missed, e.g. if
// the coordinator restarted between a task finishing and its run advancing.
func (s *Server) workflowSweepLoop(ctx context.Context) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ids, err := s.db.ListActiveWorkflowIDs(ctx, 500)
			if err != nil {
				s.log.Error("Failed to list active workflows", "error", err)
				continue
			}
			for _, id := range ids {
				if ctx.Err() != nil {
					return
				}
				s.reconcile(ctx, id)
			}
			// Agent runs too: one may have missed a task's completion
			// across a failover. Advancing is idempotent.
			runs, err := s.db.RunningAgentRuns(ctx, 500)
			if err != nil {
				s.log.Error("Failed to list agent runs", "error", err)
				continue
			}
			for _, id := range runs {
				if ctx.Err() != nil {
					return
				}
				s.advanceAgent(ctx, id)
			}
		}
	}
}

// registerExamples stores the bundled example workflows in the default
// namespace. Unchanged examples don't create new versions.
func (s *Server) registerExamples(ctx context.Context) error {
	files, err := fs.Glob(examples.Workflows, "workflows/*.yaml")
	if err != nil {
		return err
	}
	for _, f := range files {
		data, err := fs.ReadFile(examples.Workflows, f)
		if err != nil {
			return err
		}
		def, err := workflow.Parse(data)
		if err != nil {
			return fmt.Errorf("%s: %w", f, err)
		}
		spec, err := json.Marshal(def)
		if err != nil {
			return err
		}
		stored, created, err := s.db.SaveDefinition(ctx, "default", def.Name, spec)
		if err != nil {
			return err
		}
		if created {
			s.log.Info("Registered example workflow", "name", def.Name, "version", stored.Version)
		}
	}
	return nil
}
