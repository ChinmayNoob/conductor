package db

import (
	"context"
	"fmt"
	"time"
)

type WorkerRecord struct {
	ID        int64
	Address   string
	Labels    StringMap
	Slots     int
	Running   int
	Status    string // healthy, draining, unhealthy
	FirstSeen time.Time
	LastSeen  time.Time
}

// UpsertWorker records a worker's latest heartbeat.
func (db *DB) UpsertWorker(ctx context.Context, w WorkerRecord) error {
	_, err := db.q.ExecContext(ctx,
		`INSERT INTO workers (id, address, labels, slots, running, status, last_seen)
		 VALUES ($1, $2, $3, $4, $5, $6, NOW())
		 ON CONFLICT (id) DO UPDATE
		 SET address = EXCLUDED.address, labels = EXCLUDED.labels, slots = EXCLUDED.slots,
		     running = EXCLUDED.running, status = EXCLUDED.status, last_seen = NOW()`,
		w.ID, w.Address, w.Labels, w.Slots, w.Running, w.Status)
	if err != nil {
		return fmt.Errorf("failed to record worker: %w", err)
	}
	return nil
}

func (db *DB) SetWorkerStatus(ctx context.Context, id int64, status string) error {
	_, err := db.q.ExecContext(ctx, `UPDATE workers SET status = $2 WHERE id = $1`, id, status)
	if err != nil {
		return fmt.Errorf("failed to update worker status: %w", err)
	}
	return nil
}

// ListWorkers returns workers seen within the last `since`.
func (db *DB) ListWorkers(ctx context.Context, since time.Duration) ([]*WorkerRecord, error) {
	rows, err := db.q.QueryContext(ctx,
		`SELECT id, address, labels, slots, running, status, first_seen, last_seen FROM workers
		 WHERE last_seen > NOW() - make_interval(secs => $1)
		 ORDER BY id`, since.Seconds())
	if err != nil {
		return nil, fmt.Errorf("failed to list workers: %w", err)
	}
	defer rows.Close()

	var out []*WorkerRecord
	for rows.Next() {
		w := &WorkerRecord{}
		if err := rows.Scan(&w.ID, &w.Address, &w.Labels, &w.Slots, &w.Running, &w.Status, &w.FirstSeen, &w.LastSeen); err != nil {
			return nil, err
		}
		out = append(out, w)
	}
	return out, rows.Err()
}
