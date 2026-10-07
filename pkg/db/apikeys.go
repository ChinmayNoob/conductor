package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type APIKey struct {
	ID        uuid.UUID
	Name      string
	Prefix    string
	IsAdmin   bool
	CreatedAt time.Time
	RevokedAt *time.Time
}

const apiKeyColumns = `id, name, prefix, is_admin, created_at, revoked_at`

func scanAPIKey(row scanner) (*APIKey, error) {
	k := &APIKey{}
	err := row.Scan(&k.ID, &k.Name, &k.Prefix, &k.IsAdmin, &k.CreatedAt, &k.RevokedAt)
	return k, err
}

func (db *DB) CreateAPIKey(ctx context.Context, name string, hash []byte, prefix string, admin bool) (*APIKey, error) {
	k, err := scanAPIKey(db.q.QueryRowContext(ctx,
		`INSERT INTO api_keys (name, key_hash, prefix, is_admin) VALUES ($1, $2, $3, $4)
		 RETURNING `+apiKeyColumns,
		name, hash, prefix, admin,
	))
	if err != nil {
		return nil, fmt.Errorf("failed to create API key: %w", err)
	}
	return k, nil
}

// EnsureAPIKey stores a key if its hash isn't already present. It is used for
// the bootstrap key from the environment, so restarts don't create duplicates.
func (db *DB) EnsureAPIKey(ctx context.Context, name string, hash []byte, prefix string, admin bool) error {
	_, err := db.q.ExecContext(ctx,
		`INSERT INTO api_keys (name, key_hash, prefix, is_admin) VALUES ($1, $2, $3, $4)
		 ON CONFLICT (key_hash) DO NOTHING`,
		name, hash, prefix, admin,
	)
	if err != nil {
		return fmt.Errorf("failed to store API key: %w", err)
	}
	return nil
}

// LookupAPIKey returns the active key with this hash, or nil if there is none.
func (db *DB) LookupAPIKey(ctx context.Context, hash []byte) (*APIKey, error) {
	k, err := scanAPIKey(db.q.QueryRowContext(ctx,
		`SELECT `+apiKeyColumns+` FROM api_keys WHERE key_hash = $1 AND revoked_at IS NULL`, hash))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to look up API key: %w", err)
	}
	return k, nil
}

func (db *DB) ListAPIKeys(ctx context.Context) ([]*APIKey, error) {
	rows, err := db.q.QueryContext(ctx, `SELECT `+apiKeyColumns+` FROM api_keys ORDER BY created_at`)
	if err != nil {
		return nil, fmt.Errorf("failed to list API keys: %w", err)
	}
	defer rows.Close()

	var keys []*APIKey
	for rows.Next() {
		k, err := scanAPIKey(rows)
		if err != nil {
			return nil, err
		}
		keys = append(keys, k)
	}
	return keys, rows.Err()
}

// RevokeAPIKey revokes a key. It returns false if no active key has this ID.
func (db *DB) RevokeAPIKey(ctx context.Context, id uuid.UUID) (bool, error) {
	result, err := db.q.ExecContext(ctx,
		`UPDATE api_keys SET revoked_at = NOW() WHERE id = $1 AND revoked_at IS NULL`, id)
	if err != nil {
		return false, fmt.Errorf("failed to revoke API key: %w", err)
	}
	return rowsChanged(result)
}
