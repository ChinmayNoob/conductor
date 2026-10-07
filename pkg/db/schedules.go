package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type Schedule struct {
	ID            uuid.UUID
	Namespace     string
	Name          string
	Cron          string
	Timezone      string
	MisfirePolicy string          // skip, run_once or catch_up
	Target        json.RawMessage // {"task": {...}} or {"workflow": {...}}
	Enabled       bool
	NextRunAt     time.Time
	LastRunAt     *time.Time
	LastRunID     *uuid.UUID
	LastError     string
	CreatedAt     time.Time
}

const scheduleColumns = `id, namespace, name, cron, timezone, misfire_policy, target, enabled, next_run_at,
	last_run_at, last_run_id, last_error, created_at`

func scanSchedule(row scanner) (*Schedule, error) {
	s := &Schedule{}
	var lastErr sql.NullString
	err := row.Scan(&s.ID, &s.Namespace, &s.Name, &s.Cron, &s.Timezone, &s.MisfirePolicy, &s.Target,
		&s.Enabled, &s.NextRunAt, &s.LastRunAt, &s.LastRunID, &lastErr, &s.CreatedAt)
	s.LastError = lastErr.String
	return s, err
}

func (db *DB) CreateSchedule(ctx context.Context, s Schedule) (*Schedule, error) {
	out, err := scanSchedule(db.q.QueryRowContext(ctx,
		`INSERT INTO schedules (namespace, name, cron, timezone, misfire_policy, target, enabled, next_run_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		 RETURNING `+scheduleColumns,
		s.Namespace, s.Name, s.Cron, s.Timezone, s.MisfirePolicy, []byte(s.Target), s.Enabled, s.NextRunAt))
	if isUniqueViolation(err) {
		return nil, ErrExists
	}
	if err != nil {
		return nil, fmt.Errorf("failed to create schedule: %w", err)
	}
	return out, nil
}

// GetSchedule returns a schedule by name, or nil if it doesn't exist.
func (db *DB) GetSchedule(ctx context.Context, namespace, name string) (*Schedule, error) {
	s, err := scanSchedule(db.q.QueryRowContext(ctx,
		`SELECT `+scheduleColumns+` FROM schedules WHERE namespace = $1 AND name = $2`, namespace, name))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get schedule: %w", err)
	}
	return s, nil
}

func (db *DB) ListSchedules(ctx context.Context, namespace string) ([]*Schedule, error) {
	rows, err := db.q.QueryContext(ctx,
		`SELECT `+scheduleColumns+` FROM schedules WHERE namespace = $1 ORDER BY name`, namespace)
	if err != nil {
		return nil, fmt.Errorf("failed to list schedules: %w", err)
	}
	defer rows.Close()

	var out []*Schedule
	for rows.Next() {
		s, err := scanSchedule(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

func (db *DB) DeleteSchedule(ctx context.Context, namespace, name string) (bool, error) {
	result, err := db.q.ExecContext(ctx, `DELETE FROM schedules WHERE namespace = $1 AND name = $2`, namespace, name)
	if err != nil {
		return false, fmt.Errorf("failed to delete schedule: %w", err)
	}
	return rowsChanged(result)
}

// SetScheduleEnabled pauses or resumes a schedule. nextRun is used when
// resuming, so a long-paused schedule doesn't fire for missed runs.
func (db *DB) SetScheduleEnabled(ctx context.Context, namespace, name string, enabled bool, nextRun time.Time) (*Schedule, error) {
	s, err := scanSchedule(db.q.QueryRowContext(ctx,
		`UPDATE schedules SET enabled = $3,
		        next_run_at = CASE WHEN $3 AND NOT enabled THEN $4 ELSE next_run_at END
		 WHERE namespace = $1 AND name = $2
		 RETURNING `+scheduleColumns,
		namespace, name, enabled, nextRun))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to update schedule: %w", err)
	}
	return s, nil
}

// ClaimDueSchedules locks enabled schedules whose next run is due. Use it
// inside WithTx; SKIP LOCKED keeps two coordinators from firing the same one.
func (db *DB) ClaimDueSchedules(ctx context.Context, limit int) ([]*Schedule, error) {
	rows, err := db.q.QueryContext(ctx,
		`SELECT `+scheduleColumns+` FROM schedules
		 WHERE enabled AND next_run_at <= NOW()
		 ORDER BY next_run_at
		 LIMIT $1
		 FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to claim schedules: %w", err)
	}
	defer rows.Close()

	var out []*Schedule
	for rows.Next() {
		s, err := scanSchedule(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// RecordScheduleRun stores the outcome of firing a schedule and when it fires next.
func (db *DB) RecordScheduleRun(ctx context.Context, id uuid.UUID, next time.Time, fired bool, runID *uuid.UUID, runErr string) error {
	_, err := db.q.ExecContext(ctx,
		`UPDATE schedules
		 SET next_run_at = $2,
		     last_run_at = CASE WHEN $3 THEN NOW() ELSE last_run_at END,
		     last_run_id = CASE WHEN $3 THEN $4 ELSE last_run_id END,
		     last_error = CASE WHEN $3 THEN NULLIF($5, '') ELSE last_error END
		 WHERE id = $1`,
		id, next, fired, runID, runErr)
	if err != nil {
		return fmt.Errorf("failed to record schedule run: %w", err)
	}
	return nil
}

// TriggerSchedule makes an enabled schedule due now. It returns nil if there
// is no such enabled schedule.
func (db *DB) TriggerSchedule(ctx context.Context, namespace, name string) (*Schedule, error) {
	s, err := scanSchedule(db.q.QueryRowContext(ctx,
		`UPDATE schedules SET next_run_at = NOW() WHERE namespace = $1 AND name = $2 AND enabled
		 RETURNING `+scheduleColumns, namespace, name))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to trigger schedule: %w", err)
	}
	return s, nil
}

// NextScheduleAt returns when the next enabled schedule is due, or nil.
func (db *DB) NextScheduleAt(ctx context.Context) (*time.Time, error) {
	var t *time.Time
	err := db.q.QueryRowContext(ctx, `SELECT min(next_run_at) FROM schedules WHERE enabled`).Scan(&t)
	if err != nil {
		return nil, fmt.Errorf("failed to find next schedule: %w", err)
	}
	return t, nil
}
