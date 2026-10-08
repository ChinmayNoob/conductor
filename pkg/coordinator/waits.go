package coordinator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/ChinmayNoob/conductor/pkg/db"
	"github.com/ChinmayNoob/conductor/pkg/grpcapi"
	"github.com/ChinmayNoob/conductor/pkg/workflow"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Waiting steps (approval, signal) run no task. A decision resolves them
// like a finished task; the reconciler then carries on as usual.

// startWait puts a step into its waiting state. A signal step that already
// has its signal is resolved at once.
func (s *Server) startWait(ctx context.Context, tx *db.DB, wf *db.Workflow, step *db.StepState, w *workflow.WaitSpec) error {
	wait := db.Wait{Kind: w.Kind, Signal: w.Signal, Message: w.Message, OnTimeout: w.OnTimeout}
	if w.Timeout > 0 {
		deadline := time.Now().Add(w.Timeout)
		wait.Deadline = &deadline
	}
	if err := tx.StartWait(ctx, step.ID, wait); err != nil {
		return err
	}
	if w.Kind != db.WaitSignal {
		return nil
	}
	sig, err := tx.TakeSignal(ctx, wf.ID, w.Signal, step.ID)
	if err != nil || sig == nil {
		return err
	}
	_, err = tx.Decide(ctx, step.ID, db.Decision{Status: "COMPLETED", By: sig.SentBy, Outputs: flatten(sig.Payload)})
	return err
}

// cancelWait resolves a waiting step that its run no longer needs.
func cancelWait(ctx context.Context, tx *db.DB, step *db.StepState) error {
	_, err := tx.Decide(ctx, step.ID, db.Decision{Status: "CANCELLED", Error: "cancelled"})
	return err
}

// DecideStep approves or rejects a waiting approval step.
func (s *Server) DecideStep(ctx context.Context, req *grpcapi.DecideStepRequest) (*grpcapi.DecideStepResponse, error) {
	wfID, err := s.ownWorkflow(ctx, req.WorkflowId, req.Namespace)
	if err != nil {
		return nil, err
	}
	err = s.fenced(ctx, func(tx *db.DB) error {
		w, err := tx.GetWaitingStep(ctx, wfID, req.Step)
		if err != nil {
			return err
		}
		if w == nil || w.Kind != db.WaitApproval {
			return status.Errorf(codes.FailedPrecondition, "step %s is not waiting for approval", req.Step)
		}
		// Approvals always have the same outputs, however they were decided,
		// so later steps can rely on them.
		d := db.Decision{Status: "COMPLETED", By: req.DecidedBy,
			Outputs: db.StringMap{"comment": req.Comment, "decided_by": req.DecidedBy}}
		if !req.Approve {
			d.Status, d.Error = "FAILED", "rejected"
			if req.DecidedBy != "" {
				d.Error += " by " + req.DecidedBy
			}
			if req.Comment != "" {
				d.Error += ": " + req.Comment
			}
		}
		_, err = tx.Decide(ctx, w.StepID, d)
		return err
	})
	if err != nil {
		return nil, rpcError(err)
	}
	s.log.Info("Approval decided", "workflow_id", wfID, "step", req.Step, "approved", req.Approve, "by", req.DecidedBy)
	s.reconcile(context.WithoutCancel(ctx), wfID)
	return &grpcapi.DecideStepResponse{}, nil
}

// SignalWorkflow delivers a signal to the step waiting for it, or keeps it
// for the step that will.
func (s *Server) SignalWorkflow(ctx context.Context, req *grpcapi.SignalRequest) (*grpcapi.SignalResponse, error) {
	wfID, err := s.ownWorkflow(ctx, req.WorkflowId, req.Namespace)
	if err != nil {
		return nil, err
	}
	payload := json.RawMessage(req.PayloadJson)
	if len(payload) == 0 {
		payload = json.RawMessage("{}")
	}
	var obj map[string]any
	if err := json.Unmarshal(payload, &obj); err != nil {
		return nil, status.Error(codes.InvalidArgument, "the signal's payload must be a JSON object")
	}
	delivered := false
	err = s.fenced(ctx, func(tx *db.DB) error {
		wf, err := tx.LockWorkflow(ctx, wfID)
		if err != nil {
			return err
		}
		if wf.Status.Terminal() {
			return status.Errorf(codes.FailedPrecondition, "the workflow has already finished (%s)", wf.Status)
		}
		if _, err := tx.AddSignal(ctx, wfID, req.Name, payload, req.SentBy); err != nil {
			return err
		}
		waiting, err := tx.ListWaitingInRun(ctx, wfID)
		if err != nil {
			return err
		}
		for _, w := range waiting {
			if w.Kind != db.WaitSignal || w.Signal != req.Name {
				continue
			}
			sig, err := tx.TakeSignal(ctx, wfID, req.Name, w.StepID)
			if err != nil || sig == nil {
				return err
			}
			delivered, err = tx.Decide(ctx, w.StepID, db.Decision{Status: "COMPLETED", By: sig.SentBy, Outputs: flatten(sig.Payload)})
			return err
		}
		return nil
	})
	if err != nil {
		return nil, rpcError(err)
	}
	s.log.Info("Signal received", "workflow_id", wfID, "signal", req.Name, "delivered", delivered)
	if delivered {
		s.reconcile(context.WithoutCancel(ctx), wfID)
	}
	return &grpcapi.SignalResponse{Delivered: delivered}, nil
}

// waitTimeoutLoop resolves waiting steps whose deadline has passed.
func (s *Server) waitTimeoutLoop(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		expired, err := s.db.ExpiredWaits(ctx, 100)
		if err != nil {
			if ctx.Err() == nil {
				s.log.Warn("Failed to list expired waits", "error", err)
			}
			continue
		}
		for _, w := range expired {
			d := db.Decision{Status: "FAILED", By: "timeout"}
			switch {
			case w.Kind == db.WaitApproval && w.OnTimeout == "approve":
				d.Status, d.Outputs = "COMPLETED", db.StringMap{"comment": "", "decided_by": "timeout"}
			case w.Kind == db.WaitApproval:
				d.Error = "approval timed out"
			default:
				d.Error = fmt.Sprintf("no %q signal arrived in time", w.Signal)
			}
			err := s.fenced(ctx, func(tx *db.DB) error {
				_, err := tx.Decide(ctx, w.StepID, d)
				return err
			})
			if err != nil {
				if ctx.Err() == nil && !errors.Is(err, errNotLeader) {
					s.log.Warn("Failed to time out a wait", "step", w.Step, "error", err)
				}
				continue
			}
			s.log.Info("Wait timed out", "workflow_id", w.WorkflowID, "step", w.Step, "outcome", d.Status)
			s.reconcile(ctx, w.WorkflowID)
		}
	}
}

// ownWorkflow parses a run ID and checks the run is in the namespace.
func (s *Server) ownWorkflow(ctx context.Context, rawID, namespace string) (uuid.UUID, error) {
	id, err := uuid.Parse(rawID)
	if err != nil {
		return uuid.Nil, status.Error(codes.InvalidArgument, "invalid workflow ID")
	}
	wf, err := s.db.GetWorkflow(ctx, id)
	if err != nil {
		return uuid.Nil, status.Error(codes.Internal, "failed to load the workflow")
	}
	if wf == nil || wf.Namespace != namespaceOf(namespace) {
		return uuid.Nil, status.Error(codes.NotFound, "workflow not found")
	}
	return id, nil
}

// rpcError passes gRPC status errors through and hides the rest.
func rpcError(err error) error {
	if _, ok := status.FromError(err); ok {
		return err
	}
	if errors.Is(err, errNotLeader) || errors.Is(err, db.ErrFenced) {
		return status.Error(codes.Unavailable, "leadership changed; retry")
	}
	return status.Error(codes.Internal, "internal error")
}

// flatten turns a JSON object's top-level fields into step outputs: strings
// as they are, anything else as JSON.
func flatten(raw json.RawMessage) db.StringMap {
	var obj map[string]any
	if json.Unmarshal(raw, &obj) != nil {
		return nil
	}
	out := make(db.StringMap, len(obj))
	for k, v := range obj {
		if str, ok := v.(string); ok {
			out[k] = str
			continue
		}
		b, _ := json.Marshal(v)
		out[k] = string(b)
	}
	return out
}
