package api

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/ChinmayNoob/conductor/pkg/db"
	"github.com/ChinmayNoob/conductor/pkg/security"
	"github.com/google/uuid"
)

type ctxKey int

const apiKeyCtxKey ctxKey = iota

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
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), apiKeyCtxKey, key)))
	})
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

type apiKeyJSON struct {
	ID        uuid.UUID  `json:"id"`
	Name      string     `json:"name"`
	Prefix    string     `json:"prefix"`
	Admin     bool       `json:"admin"`
	CreatedAt time.Time  `json:"created_at"`
	RevokedAt *time.Time `json:"revoked_at,omitempty"`
	// Key is only returned once, when the key is created.
	Key string `json:"key,omitempty"`
}

func toAPIKeyJSON(k *db.APIKey) apiKeyJSON {
	return apiKeyJSON{ID: k.ID, Name: k.Name, Prefix: k.Prefix, Admin: k.IsAdmin, CreatedAt: k.CreatedAt, RevokedAt: k.RevokedAt}
}

func (s *Server) handleCreateAPIKey(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name  string `json:"name"`
		Admin bool   `json:"admin"`
	}
	if !s.decode(w, r, &req) {
		return
	}
	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "name is required")
		return
	}
	plain := security.GenerateAPIKey()
	k, err := s.db.CreateAPIKey(r.Context(), req.Name, security.HashAPIKey(plain), security.KeyPrefix(plain), req.Admin)
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
