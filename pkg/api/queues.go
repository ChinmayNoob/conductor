package api

import (
	"net/http"
	"time"

	"github.com/ChinmayNoob/conductor/pkg/db"
)

type queueJSON struct {
	Name              string    `json:"name"`
	ConcurrencyLimit  *int      `json:"concurrency_limit"`
	RateLimit         *int      `json:"rate_limit"`
	RatePeriodSeconds int       `json:"rate_period_seconds"`
	TokensPerMinute   *int64    `json:"tokens_per_minute"`
	Paused            bool      `json:"paused"`
	Queued            int       `json:"queued"`
	Running           int       `json:"running"`
	UpdatedAt         time.Time `json:"updated_at"`
}

func toQueueJSON(q *db.Queue) queueJSON {
	return queueJSON{Name: q.Name, ConcurrencyLimit: q.ConcurrencyLimit, RateLimit: q.RateLimit,
		RatePeriodSeconds: q.RatePeriodSeconds, TokensPerMinute: q.TokensPerMinute, Paused: q.Paused,
		Queued: q.Queued, Running: q.Running, UpdatedAt: q.UpdatedAt}
}

func (s *Server) handleListQueues(w http.ResponseWriter, r *http.Request) {
	list, err := s.db.ListQueues(r.Context(), namespace(r))
	if err != nil {
		s.internalError(w, "Failed to list queues", err)
		return
	}
	out := make([]queueJSON, 0, len(list))
	for _, q := range list {
		out = append(out, toQueueJSON(q))
	}
	writeJSON(w, http.StatusOK, out)
}

// handlePutQueue sets a queue's limits: at most concurrency_limit tasks
// running at once, and at most rate_limit dispatches per rate_period_seconds.
// Null means unlimited.
func (s *Server) handlePutQueue(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if !nameRe.MatchString(name) {
		writeError(w, http.StatusBadRequest, "queue name must be lowercase letters, digits, '-' or '_'")
		return
	}
	var req struct {
		ConcurrencyLimit  *int   `json:"concurrency_limit"`
		RateLimit         *int   `json:"rate_limit"`
		RatePeriodSeconds int    `json:"rate_period_seconds"`
		TokensPerMinute   *int64 `json:"tokens_per_minute"`
		Paused            bool   `json:"paused"`
	}
	if !s.decode(w, r, &req) {
		return
	}
	if (req.ConcurrencyLimit != nil && *req.ConcurrencyLimit < 1) || (req.RateLimit != nil && *req.RateLimit < 1) ||
		(req.TokensPerMinute != nil && *req.TokensPerMinute < 1) || req.RatePeriodSeconds < 0 {
		writeError(w, http.StatusBadRequest, "limits must be at least 1 (use null for unlimited)")
		return
	}
	q, err := s.db.UpsertQueue(r.Context(), db.Queue{
		Namespace: namespace(r), Name: name, ConcurrencyLimit: req.ConcurrencyLimit, RateLimit: req.RateLimit,
		RatePeriodSeconds: req.RatePeriodSeconds, TokensPerMinute: req.TokensPerMinute, Paused: req.Paused,
	})
	if err != nil {
		s.internalError(w, "Failed to save queue", err)
		return
	}
	writeJSON(w, http.StatusOK, toQueueJSON(q))
}

func (s *Server) handleDeleteQueue(w http.ResponseWriter, r *http.Request) {
	ok, err := s.db.DeleteQueue(r.Context(), namespace(r), r.PathValue("name"))
	if err != nil {
		s.internalError(w, "Failed to delete queue", err)
		return
	}
	if !ok {
		writeError(w, http.StatusNotFound, "queue has no settings")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

type workerJSON struct {
	ID        int64             `json:"id"`
	Address   string            `json:"address"`
	Status    string            `json:"status"`
	Slots     int               `json:"slots"`
	Running   int               `json:"running"`
	Labels    map[string]string `json:"labels"`
	FirstSeen time.Time         `json:"first_seen"`
	LastSeen  time.Time         `json:"last_seen"`
}

// handleListWorkers lists workers seen in the last five minutes.
func (s *Server) handleListWorkers(w http.ResponseWriter, r *http.Request) {
	list, err := s.db.ListWorkers(r.Context(), 5*time.Minute)
	if err != nil {
		s.internalError(w, "Failed to list workers", err)
		return
	}
	out := make([]workerJSON, 0, len(list))
	for _, wk := range list {
		out = append(out, workerJSON{ID: wk.ID, Address: wk.Address, Status: wk.Status, Slots: wk.Slots,
			Running: wk.Running, Labels: wk.Labels, FirstSeen: wk.FirstSeen, LastSeen: wk.LastSeen})
	}
	writeJSON(w, http.StatusOK, out)
}

type clusterJSON struct {
	Leader       *leaderJSON       `json:"leader"`
	Coordinators []coordinatorJSON `json:"coordinators"`
}

type leaderJSON struct {
	ID          string     `json:"id"`
	Address     string     `json:"address"`
	Epoch       int64      `json:"epoch"`
	ElectedAt   *time.Time `json:"elected_at,omitempty"`
	HeartbeatAt *time.Time `json:"heartbeat_at,omitempty"`
}

type coordinatorJSON struct {
	ID        string    `json:"id"`
	Address   string    `json:"address"`
	Leader    bool      `json:"leader"`
	StartedAt time.Time `json:"started_at"`
	LastSeen  time.Time `json:"last_seen"`
}

// handleCluster shows the elected leader and every live coordinator.
func (s *Server) handleCluster(w http.ResponseWriter, r *http.Request) {
	l, err := s.db.GetLeader(r.Context())
	if err != nil {
		s.internalError(w, "Failed to get leader", err)
		return
	}
	coords, err := s.db.ListCoordinators(r.Context(), 30*time.Second)
	if err != nil {
		s.internalError(w, "Failed to list coordinators", err)
		return
	}
	out := clusterJSON{Coordinators: []coordinatorJSON{}}
	if l.CoordinatorID != "" {
		out.Leader = &leaderJSON{ID: l.CoordinatorID, Address: l.Address, Epoch: l.Epoch, ElectedAt: l.ElectedAt, HeartbeatAt: l.HeartbeatAt}
	}
	for _, c := range coords {
		out.Coordinators = append(out.Coordinators, coordinatorJSON{
			ID: c.ID, Address: c.Address, Leader: c.ID == l.CoordinatorID, StartedAt: c.StartedAt, LastSeen: c.LastSeen,
		})
	}
	writeJSON(w, http.StatusOK, out)
}
