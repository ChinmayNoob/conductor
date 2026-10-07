package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ChinmayNoob/conductor/pkg/db"
	"github.com/ChinmayNoob/conductor/pkg/schedule"
	"github.com/google/uuid"
)

// scheduleLoop fires due cron schedules once a second.
func (s *Server) scheduleLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.fireDueSchedules(ctx)
		}
	}
}

// fireDueSchedules starts the task or workflow of every due schedule and
// moves it to its next run, all in one transaction: a run is never fired
// without its schedule advancing, or vice versa.
func (s *Server) fireDueSchedules(ctx context.Context) {
	var started []uuid.UUID // workflow runs to reconcile after commit
	fired := 0

	err := s.db.WithTx(ctx, func(tx *db.DB) error {
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
		s.log.Error("Failed to fire schedules", "error", err)
		return
	}

	for _, id := range started {
		s.reconcile(ctx, id)
	}
	if fired > 0 {
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
	t, _, err := tx.CreateTask(ctx, n)
	if err != nil {
		return uuid.Nil, false, err
	}
	return t.ID, false, nil
}
