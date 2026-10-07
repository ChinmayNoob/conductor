package api

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/ChinmayNoob/conductor/pkg/db"
	"github.com/ChinmayNoob/conductor/pkg/security"
	"github.com/google/uuid"
)

type ctxKey int

const (
	apiKeyCtxKey ctxKey = iota
	namespaceCtxKey
)

// namespaceHeader lets admin keys act in any namespace. Other keys are bound
// to their own.
const namespaceHeader = "X-Conductor-Namespace"

func (s *Server) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || token == "" {
			w.Header().Set("WWW-Authenticate", `Bearer realm="conductor"`)
			writeError(w, http.StatusUnauthorized, "missing API key: send 'Authorization: Bearer <key>'")
			return
		}
		key, err := s.db.LookupAPIKey(r.Context(), security.HashAPIKey(token))
		if err != nil {
			s.internalError(w, "Failed to look up API key", err)
			return
		}
		if key == nil {
			writeError(w, http.StatusUnauthorized, "invalid or revoked API key")
			return
		}

		ns := key.Namespace
		if h := r.Header.Get(namespaceHeader); h != "" && h != ns {
			if !key.IsAdmin {
				writeError(w, http.StatusForbidden, "this API key can only use namespace "+key.Namespace)
				return
			}
			exists, err := s.db.GetNamespace(r.Context(), h)
			if err != nil {
				s.internalError(w, "Failed to look up namespace", err)
				return
			}
			if exists == nil {
				writeError(w, http.StatusNotFound, "namespace "+h+" does not exist")
				return
			}
			ns = h
		}

		ctx := context.WithValue(r.Context(), apiKeyCtxKey, key)
		ctx = context.WithValue(ctx, namespaceCtxKey, ns)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// namespace returns the namespace a request acts in.
func namespace(r *http.Request) string {
	ns, _ := r.Context().Value(namespaceCtxKey).(string)
	return ns
}

func requireAdmin(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key, _ := r.Context().Value(apiKeyCtxKey).(*db.APIKey)
		if key == nil || !key.IsAdmin {
			writeError(w, http.StatusForbidden, "this endpoint requires an admin API key")
			return
		}
		next(w, r)
	}
}

// --- API keys ---

type apiKeyJSON struct {
	ID        uuid.UUID  `json:"id"`
	Name      string     `json:"name"`
	Prefix    string     `json:"prefix"`
	Namespace string     `json:"namespace"`
	Admin     bool       `json:"admin"`
	CreatedAt time.Time  `json:"created_at"`
	RevokedAt *time.Time `json:"revoked_at,omitempty"`
	// Key is only returned once, when the key is created.
	Key string `json:"key,omitempty"`
}

func toAPIKeyJSON(k *db.APIKey) apiKeyJSON {
	return apiKeyJSON{ID: k.ID, Name: k.Name, Prefix: k.Prefix, Namespace: k.Namespace, Admin: k.IsAdmin,
		CreatedAt: k.CreatedAt, RevokedAt: k.RevokedAt}
}

func (s *Server) handleCreateAPIKey(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name      string `json:"name"`
		Admin     bool   `json:"admin"`
		Namespace string `json:"namespace"`
	}
	if !s.decode(w, r, &req) {
		return
	}
	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "name is required")
		return
	}
	if req.Namespace == "" {
		req.Namespace = "default"
	}
	ns, err := s.db.GetNamespace(r.Context(), req.Namespace)
	if err != nil {
		s.internalError(w, "Failed to look up namespace", err)
		return
	}
	if ns == nil {
		writeError(w, http.StatusBadRequest, "namespace "+req.Namespace+" does not exist")
		return
	}

	plain := security.GenerateAPIKey()
	k, err := s.db.CreateAPIKey(r.Context(), req.Name, security.HashAPIKey(plain), security.KeyPrefix(plain), req.Namespace, req.Admin)
	if err != nil {
		s.internalError(w, "Failed to create API key", err)
		return
	}
	resp := toAPIKeyJSON(k)
	resp.Key = plain
	writeJSON(w, http.StatusCreated, resp)
}

func (s *Server) handleListAPIKeys(w http.ResponseWriter, r *http.Request) {
	keys, err := s.db.ListAPIKeys(r.Context())
	if err != nil {
		s.internalError(w, "Failed to list API keys", err)
		return
	}
	out := make([]apiKeyJSON, 0, len(keys))
	for _, k := range keys {
		out = append(out, toAPIKeyJSON(k))
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleRevokeAPIKey(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid API key ID")
		return
	}
	ok, err := s.db.RevokeAPIKey(r.Context(), id)
	if err != nil {
		s.internalError(w, "Failed to revoke API key", err)
		return
	}
	if !ok {
		writeError(w, http.StatusNotFound, "API key not found or already revoked")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// --- Namespaces ---

type namespaceJSON struct {
	Name            string    `json:"name"`
	MaxPendingTasks *int      `json:"max_pending_tasks"`
	MaxConcurrency  *int      `json:"max_concurrency"`
	CreatedAt       time.Time `json:"created_at"`
}

type namespaceRequest struct {
	Name            string `json:"name"`
	MaxPendingTasks *int   `json:"max_pending_tasks"`
	MaxConcurrency  *int   `json:"max_concurrency"`
}

func (q namespaceRequest) validate() error {
	if (q.MaxPendingTasks != nil && *q.MaxPendingTasks < 1) || (q.MaxConcurrency != nil && *q.MaxConcurrency < 1) {
		return errors.New("limits must be at least 1 (omit or use null for no limit)")
	}
	return nil
}

func toNamespaceJSON(n *db.Namespace) namespaceJSON {
	return namespaceJSON{Name: n.Name, MaxPendingTasks: n.MaxPendingTasks, MaxConcurrency: n.MaxConcurrency, CreatedAt: n.CreatedAt}
}

func (s *Server) handleCreateNamespace(w http.ResponseWriter, r *http.Request) {
	var req namespaceRequest
	if !s.decode(w, r, &req) {
		return
	}
	if !nameRe.MatchString(req.Name) {
		writeError(w, http.StatusBadRequest, "name must be lowercase letters, digits, '-' or '_'")
		return
	}
	if err := req.validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	n, err := s.db.CreateNamespace(r.Context(), db.Namespace{Name: req.Name, MaxPendingTasks: req.MaxPendingTasks, MaxConcurrency: req.MaxConcurrency})
	if errors.Is(err, db.ErrExists) {
		writeError(w, http.StatusConflict, "namespace "+req.Name+" already exists")
		return
	}
	if err != nil {
		s.internalError(w, "Failed to create namespace", err)
		return
	}
	writeJSON(w, http.StatusCreated, toNamespaceJSON(n))
}

func (s *Server) handleUpdateNamespace(w http.ResponseWriter, r *http.Request) {
	var req namespaceRequest
	if !s.decode(w, r, &req) {
		return
	}
	if err := req.validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	n, err := s.db.UpdateNamespace(r.Context(), db.Namespace{Name: r.PathValue("name"), MaxPendingTasks: req.MaxPendingTasks, MaxConcurrency: req.MaxConcurrency})
	if err != nil {
		s.internalError(w, "Failed to update namespace", err)
		return
	}
	if n == nil {
		writeError(w, http.StatusNotFound, "namespace not found")
		return
	}
	writeJSON(w, http.StatusOK, toNamespaceJSON(n))
}

func (s *Server) handleListNamespaces(w http.ResponseWriter, r *http.Request) {
	list, err := s.db.ListNamespaces(r.Context())
	if err != nil {
		s.internalError(w, "Failed to list namespaces", err)
		return
	}
	out := make([]namespaceJSON, 0, len(list))
	for _, n := range list {
		out = append(out, toNamespaceJSON(n))
	}
	writeJSON(w, http.StatusOK, out)
}
