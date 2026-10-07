package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type Definition struct {
	Namespace string
	Name      string
	Version   int
	Spec      json.RawMessage
	CreatedAt time.Time
}

const definitionColumns = `namespace, name, version, spec, created_at`

func scanDefinition(row scanner) (*Definition, error) {
	d := &Definition{}
	err := row.Scan(&d.Namespace, &d.Name, &d.Version, &d.Spec, &d.CreatedAt)
	return d, err
}

// SaveDefinition stores spec as the next version of a definition. If it is
// identical to the latest version, nothing is stored and that version is
// returned with created=false, so re-applying the same file is a no-op.
func (db *DB) SaveDefinition(ctx context.Context, namespace, name string, spec json.RawMessage) (d *Definition, created bool, err error) {
	err = db.WithTx(ctx, func(tx *DB) error {
		// Serialize versioning of this one definition.
		if _, err := tx.q.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, namespace+"/"+name); err != nil {
			return err
		}
		latest, err := scanDefinition(tx.q.QueryRowContext(ctx,
			`SELECT `+definitionColumns+` FROM workflow_definitions
			 WHERE namespace = $1 AND name = $2 AND spec = $3::jsonb
			   AND version = (SELECT max(version) FROM workflow_definitions WHERE namespace = $1 AND name = $2)`,
			namespace, name, []byte(spec)))
		if err == nil {
			d = latest
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		d, err = scanDefinition(tx.q.QueryRowContext(ctx,
			`INSERT INTO workflow_definitions (namespace, name, version, spec)
			 SELECT $1, $2, COALESCE(max(version), 0) + 1, $3
			 FROM workflow_definitions WHERE namespace = $1 AND name = $2
			 RETURNING `+definitionColumns,
			namespace, name, []byte(spec)))
		created = err == nil
		return err
	})
	if err != nil {
		return nil, false, fmt.Errorf("failed to save workflow definition: %w", err)
	}
	return d, created, nil
}

// GetDefinition returns a version of a definition (0 means the latest), or
// nil if it doesn't exist.
func (db *DB) GetDefinition(ctx context.Context, namespace, name string, version int) (*Definition, error) {
	d, err := scanDefinition(db.q.QueryRowContext(ctx,
		`SELECT `+definitionColumns+` FROM workflow_definitions
		 WHERE namespace = $1 AND name = $2 AND ($3 = 0 OR version = $3)
		 ORDER BY version DESC LIMIT 1`,
		namespace, name, version))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get workflow definition: %w", err)
	}
	return d, nil
}

// ListDefinitions returns the latest version of every definition.
func (db *DB) ListDefinitions(ctx context.Context, namespace string) ([]*Definition, error) {
	rows, err := db.q.QueryContext(ctx,
		`SELECT DISTINCT ON (name) `+definitionColumns+` FROM workflow_definitions
		 WHERE namespace = $1 ORDER BY name, version DESC`, namespace)
	if err != nil {
		return nil, fmt.Errorf("failed to list workflow definitions: %w", err)
	}
	defer rows.Close()

	var out []*Definition
	for rows.Next() {
		d, err := scanDefinition(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}
