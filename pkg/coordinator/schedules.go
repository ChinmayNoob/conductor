package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ChinmayNoob/conductor/pkg/db"
	"github.com/ChinmayNoob/conductor/pkg/metrics"
	"github.com/ChinmayNoob/conductor/pkg/schedule"
	"github.com/ChinmayNoob/conductor/pkg/tracing"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// scheduleLoop fires due cron schedules, sleeping until the next one is due
// (or a notification says schedules changed).
func (s *Server) scheduleLoop(ctx context.Context) {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		case <-s.wakeSched:
		}
		s.fireDueSchedules(ctx)

		wait := 10 * time.Second
		if next, err := s.db.NextScheduleAt(ctx); err == nil && next != nil {
			wait = min(max(time.Until(*next), 10*time.Millisecond), wait)
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(wait)
	}
}

// fireDueSchedules starts the task or workflow of every due schedule and
// moves it to its next run, all in one transaction: a run is never fired
// without its schedule advancing, or vice versa.
func (s *Server) fireDueSchedules(ctx context.Context) {
	var started []uuid.UUID // workflow runs to reconcile after commit
	fired := 0

	err := s.fenced(ctx, func(tx *db.DB) error {
		due, err := tx.ClaimDueSchedules(ctx, 50)
		if err != nil {
			return err
		}
		now := time.Now().UTC()
		for _, sch := range due {
			log := s.log.With("schedule", sch.Name, "namespace", sch.Namespace)

			spec, err := schedule.Parse(sch.Cron, sch.Timezone)
			if err != nil {
				// Shouldn't happen (validated on create); retry in an hour.
				log.Error("Invalid schedule", "error", err)
				if err := tx.RecordScheduleRun(ctx, sch.ID, now.Add(time.Hour), true, nil, err.Error()); err != nil {
					return err
				}
				continue
			}

			d := schedule.Decide(spec, sch.MisfirePolicy, sch.NextRunAt, now)
			var runID *uuid.UUID
			var runErr string
			if d.Fire {
				id, isWorkflow, err := s.fireSchedule(ctx, tx, sch, d.FireAt)
				if err != nil {
					runErr = err.Error()
					log.Warn("Schedule failed to start its run", "error", err)
				} else {
					runID = &id
					fired++
					if isWorkflow {
						started = append(started, id)
					}
					log.Info("Schedule fired", "run_id", id, "due", d.FireAt)
				}
			} else {
				log.Warn("Skipping missed run", "due", sch.NextRunAt, "policy", sch.MisfirePolicy)
			}
			if err := tx.RecordScheduleRun(ctx, sch.ID, d.Next, d.Fire, runID, runErr); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		if ctx.Err() == nil {
			s.log.Error("Failed to fire schedules", "error", err)
		}
		return
	}

	for _, id := range started {
		s.reconcile(ctx, id)
	}
	if fired > 0 {
		metrics.SchedulesFired.Add(float64(fired))
		s.wakeDispatcher()
	}
}

// fireSchedule starts a schedule's target. The idempotency key ties the run
// to the schedule and the run time, so a given run can never start twice.
func (s *Server) fireSchedule(ctx context.Context, tx *db.DB, sch *db.Schedule, at time.Time) (id uuid.UUID, isWorkflow bool, err error) {
	var target schedule.Target
	if err := json.Unmarshal(sch.Target, &target); err != nil {
		return uuid.Nil, false, fmt.Errorf("invalid target: %w", err)
	}
	key := fmt.Sprintf("schedule:%s:%d", sch.ID, at.Unix())
	// Each run starts its own trace.
	ctx, span := tracing.Start(ctx, "schedule "+sch.Name, trace.WithNewRoot(),
		trace.WithAttributes(attribute.String("conductor.schedule", sch.Name)))
	defer span.End()

	if target.Workflow != nil {
		w := target.Workflow
		id, _, err := s.startWorkflow(ctx, tx, sch.Namespace, w.Name, w.Version, w.Input, key)
		return id, true, err
	}
	if target.Task == nil {
		return uuid.Nil, false, fmt.Errorf("schedule has no target")
	}
	n, err := target.Task.NewTask(sch.Namespace)
	if err != nil {
		return uuid.Nil, false, err
	}
	n.IdempotencyKey = key
	n.TraceParent = tracing.TraceParent(ctx)
	t, _, err := tx.CreateTask(ctx, n)
	if err != nil {
		return uuid.Nil, false, err
	}
	return t.ID, false, nil
}
