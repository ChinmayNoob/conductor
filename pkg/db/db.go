// Package db is Conductor's Postgres data layer: the task queue, workflow
// state, API keys and schema migrations.
package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
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
}

// Open connects to Postgres, retrying until ctx is done so components can
// start before the database is ready.
func Open(ctx context.Context, dsn string) (*DB, error) {
	conn, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	conn.SetMaxOpenConns(20)
	conn.SetMaxIdleConns(10)
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

func (db *DB) Close() error {
	return db.conn.Close()
}

func (db *DB) Ping(ctx context.Context) error {
	return db.conn.PingContext(ctx)
}

// WithTx runs fn in a transaction, committing if it returns nil and rolling
// back otherwise. The *DB passed to fn runs every query in that transaction.
func (db *DB) WithTx(ctx context.Context, fn func(tx *DB) error) error {
	if _, ok := db.q.(*sql.Tx); ok {
		return fn(db) // already in a transaction
	}
	tx, err := db.conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	if err := fn(&DB{conn: db.conn, q: tx}); err != nil {
		tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	return nil
}

func rowsChanged(result sql.Result) (bool, error) {
	n, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}
