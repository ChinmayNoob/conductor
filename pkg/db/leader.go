package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// ErrFenced is returned by fenced writes when another coordinator has become
// leader since this one was elected.
var ErrFenced = errors.New("fenced: this coordinator is no longer the leader")

// leaderLockID is the advisory lock whose holder is the leader.
const leaderLockID = 0x636f6f72 // "coor"

type Leader struct {
	Epoch         int64
	CoordinatorID string
	Address       string
	ElectedAt     *time.Time
	HeartbeatAt   *time.Time
}

// LeaderSession is a dedicated database session used for leader election.
// Advisory locks belong to a session, so the lock lives exactly as long as
// this connection: if the leader dies or loses the database, Postgres
// releases the lock and a standby can take over.
type LeaderSession struct {
	conn *sql.Conn
}

func (db *DB) NewLeaderSession(ctx context.Context) (*LeaderSession, error) {
	conn, err := db.conn.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to open leader session: %w", err)
	}
	return &LeaderSession{conn: conn}, nil
}

// TryAcquire tries to take the leader lock without waiting.
func (s *LeaderSession) TryAcquire(ctx context.Context) (bool, error) {
	var ok bool
	err := s.conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock($1)`, leaderLockID).Scan(&ok)
	return ok, err
}

// Ping checks that the session (and so the lock) is still alive.
func (s *LeaderSession) Ping(ctx context.Context) error {
	_, err := s.conn.ExecContext(ctx, `SELECT 1`)
	return err
}

// Release gives up the lock and closes the session.
func (s *LeaderSession) Release() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	// Unlock explicitly: Close returns a healthy connection to the pool,
	// where the session (and its lock) would otherwise live on.
	_, _ = s.conn.ExecContext(ctx, `SELECT pg_advisory_unlock_all()`)
	s.conn.Close()
}

// BumpEpoch records a new leader and returns its epoch. The update waits for
// the previous leader's fenced transactions (which hold share locks on the
// row), so once it commits, the previous leader can't write anything.
func (db *DB) BumpEpoch(ctx context.Context, coordinatorID, address string) (int64, error) {
	var epoch int64
	err := db.q.QueryRowContext(ctx,
		`WITH bumped AS (
			 UPDATE coordinator_leader
			 SET epoch = epoch + 1, coordinator_id = $1, address = $2, elected_at = NOW()
			 WHERE id = 1
			 RETURNING epoch
		 ), beat AS (
			 UPDATE leader_heartbeat SET epoch = bumped.epoch, heartbeat_at = NOW() FROM bumped WHERE id = 1
		 )
		 SELECT epoch FROM bumped`, coordinatorID, address).Scan(&epoch)
	if err != nil {
		return 0, fmt.Errorf("failed to record new leader: %w", err)
	}
	return epoch, nil
}

// LeaderHeartbeat marks the leader as alive, for visibility. It lives in
// its own table so it never waits on the fenced writes' share locks on the
// leader row. A deposed leader's late heartbeat changes nothing.
func (db *DB) LeaderHeartbeat(ctx context.Context, epoch int64) error {
	_, err := db.q.ExecContext(ctx,
		`UPDATE leader_heartbeat SET heartbeat_at = NOW() WHERE id = 1 AND epoch = $1`, epoch)
	return err
}

func (db *DB) GetLeader(ctx context.Context) (*Leader, error) {
	l := &Leader{}
	var id, addr sql.NullString
	err := db.q.QueryRowContext(ctx,
		`SELECT l.epoch, l.coordinator_id, l.address, l.elected_at, h.heartbeat_at
		 FROM coordinator_leader l
		 LEFT JOIN leader_heartbeat h ON h.id = 1 AND h.epoch = l.epoch
		 WHERE l.id = 1`,
	).Scan(&l.Epoch, &id, &addr, &l.ElectedAt, &l.HeartbeatAt)
	if err != nil {
		return nil, fmt.Errorf("failed to get leader: %w", err)
	}
	l.CoordinatorID, l.Address = id.String, addr.String
	return l, nil
}

// CheckEpoch returns ErrFenced unless epoch is still current. Inside a
// transaction it takes a share lock on the leader row, which holds off a new
// leader's BumpEpoch until the transaction ends.
func (db *DB) CheckEpoch(ctx context.Context, epoch int64) error {
	var current int64
	err := db.q.QueryRowContext(ctx, `SELECT epoch FROM coordinator_leader WHERE id = 1 FOR SHARE`).Scan(&current)
	if err != nil {
		return fmt.Errorf("failed to check leader epoch: %w", err)
	}
	if current != epoch {
		return ErrFenced
	}
	return nil
}

// WithFencedTx runs fn in a transaction that only commits if this
// coordinator is still the leader for epoch.
func (db *DB) WithFencedTx(ctx context.Context, epoch int64, fn func(tx *DB) error) error {
	return db.Fenced(epoch).WithTx(ctx, fn)
}

// Fenced returns a handle whose writes only apply while epoch is the current
// leader epoch. Transactions check it once up front (WithTx). The hot
// single-statement task transitions (PickTasks, MarkTaskStarted,
// MarkTaskCompleted) instead carry the check inside the statement, which
// saves three round trips: the statement takes the same share lock on the
// leader row as a fenced transaction, so the new leader's epoch bump waits
// for it just the same. When such a statement changes nothing, it returns
// ErrFenced if the epoch has moved on.
func (db *DB) Fenced(epoch int64) *DB {
	f := *db
	f.epoch = epoch
	return &f
}

// fenceMarker marks where a statement takes its epoch check.
const fenceMarker = "/*fence*/"

// fence fills in a statement's fence: on a Fenced handle, a condition that
// holds only while the epoch is current; otherwise nothing. The subquery is
// uncorrelated, so Postgres evaluates it once, before touching any row.
func (db *DB) fence(query string, args ...any) (string, []any) {
	cond := ""
	if db.epoch != 0 {
		args = append(args, db.epoch)
		cond = fmt.Sprintf("AND EXISTS (SELECT 1 FROM coordinator_leader WHERE id = 1 AND epoch = $%d FOR SHARE)", len(args))
	}
	return strings.Replace(query, fenceMarker, cond, 1), args
}

// unchanged explains a fenced statement that changed no rows: ErrFenced if
// the epoch moved on, nil if the rows simply didn't qualify.
func (db *DB) unchanged(ctx context.Context) error {
	if db.epoch == 0 {
		return nil
	}
	return db.CheckEpoch(ctx, db.epoch)
}

type CoordinatorRecord struct {
	ID        string
	Address   string
	StartedAt time.Time
	LastSeen  time.Time
}

// RegisterCoordinator records that a coordinator is running.
func (db *DB) RegisterCoordinator(ctx context.Context, id, address string) error {
	_, err := db.q.ExecContext(ctx,
		`INSERT INTO coordinators (id, address) VALUES ($1, $2)
		 ON CONFLICT (id) DO UPDATE SET address = EXCLUDED.address, last_seen = NOW()`, id, address)
	return err
}

// ListCoordinators returns coordinators seen within `since`.
func (db *DB) ListCoordinators(ctx context.Context, since time.Duration) ([]*CoordinatorRecord, error) {
	rows, err := db.q.QueryContext(ctx,
		`SELECT id, address, started_at, last_seen FROM coordinators
		 WHERE last_seen > NOW() - make_interval(secs => $1) ORDER BY started_at`, since.Seconds())
	if err != nil {
		return nil, fmt.Errorf("failed to list coordinators: %w", err)
	}
	defer rows.Close()
	var out []*CoordinatorRecord
	for rows.Next() {
		c := &CoordinatorRecord{}
		if err := rows.Scan(&c.ID, &c.Address, &c.StartedAt, &c.LastSeen); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}
