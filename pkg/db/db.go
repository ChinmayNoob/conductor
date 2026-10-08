// Package db is Conductor's Postgres data layer: the task queue, workflow
// state, API keys and schema migrations.
package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"time"

	_ "github.com/lib/pq"
)

// querier is satisfied by both *sql.DB and *sql.Tx, so every query method
// works the same inside or outside a transaction.
type querier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type DB struct {
	conn *sql.DB
	q    querier
	// epoch, when set, fences writes to that leader epoch (see Fenced).
	epoch int64
}

// Open connects to Postgres, retrying until ctx is done so components can
// start before the database is ready.
func Open(ctx context.Context, dsn string) (*DB, error) {
	conn, err := sql.Open("postgres", WithUTC(dsn))
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	conn.SetMaxOpenConns(20)
	// Keep every connection: a closed one means forking a new backend with
	// cold caches the next time load spikes.
	conn.SetMaxIdleConns(20)
	conn.SetConnMaxLifetime(30 * time.Minute)

	backoff := 500 * time.Millisecond
	for {
		err = conn.PingContext(ctx)
		if err == nil {
			return &DB{conn: conn, q: conn}, nil
		}
		slog.Warn("Database not ready, retrying", "error", err, "retry_in", backoff)
		select {
		case <-ctx.Done():
			conn.Close()
			return nil, fmt.Errorf("failed to connect to database: %w", errors.Join(err, ctx.Err()))
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 5*time.Second)
	}
}

// WithUTC pins the session time zone to UTC. Several columns are TIMESTAMP
// without a zone and are compared with NOW(), so the session zone must match
// the UTC values the code stores.
func WithUTC(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil || u.Scheme == "" {
		return dsn
	}
	q := u.Query()
	if q.Get("timezone") == "" {
		q.Set("timezone", "UTC")
		u.RawQuery = q.Encode()
	}
	return u.String()
}

// WithLocalWake marks the sessions as the leader coordinator's: writes that
// make a task runnable skip the NOTIFY, because the leader wakes its own
// dispatcher (see migration 0006).
func WithLocalWake(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil || u.Scheme == "" {
		return dsn
	}
	q := u.Query()
	q.Set("conductor.local_wake", "on")
	u.RawQuery = q.Encode()
	return u.String()
}

func (db *DB) Close() error {
	return db.conn.Close()
}

func (db *DB) Ping(ctx context.Context) error {
	return db.conn.PingContext(ctx)
}

// WithTx runs fn in a transaction, committing if it returns nil and rolling
// back otherwise. The *DB passed to fn runs every query in that transaction.
// On a Fenced handle the transaction first checks the epoch, and only
// commits while it is current.
func (db *DB) WithTx(ctx context.Context, fn func(tx *DB) error) error {
	if tx, ok := db.q.(*sql.Tx); ok { // already in a transaction
		inner := &DB{conn: db.conn, q: tx}
		if err := inner.checkFence(ctx, db.epoch); err != nil {
			return err
		}
		return fn(inner)
	}
	tx, err := db.conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	inner := &DB{conn: db.conn, q: tx}
	if err := inner.checkFence(ctx, db.epoch); err != nil {
		tx.Rollback()
		return err
	}
	if err := fn(inner); err != nil {
		tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	return nil
}

// checkFence checks epoch (if set) in the current transaction. The share
// lock it takes is held until commit, so the statements that follow need no
// fence of their own.
func (db *DB) checkFence(ctx context.Context, epoch int64) error {
	if epoch == 0 {
		return nil
	}
	return db.CheckEpoch(ctx, epoch)
}

func rowsChanged(result sql.Result) (bool, error) {
	n, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}
