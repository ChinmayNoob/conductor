// Package api is Conductor's HTTP API. It reads state straight from Postgres
// and sends commands (submit, cancel) to the coordinator over gRPC.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/ChinmayNoob/conductor/pkg/db"
	"github.com/ChinmayNoob/conductor/pkg/grpcapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	db              *db.DB
	coordinator     grpcapi.CoordinatorServiceClient
	maxRequestBytes int64
	log             *slog.Logger
}

func NewServer(database *db.DB, coordinator grpcapi.CoordinatorServiceClient, maxRequestBytes int64) *Server {
	return &Server{
		db:              database,
		coordinator:     coordinator,
		maxRequestBytes: maxRequestBytes,
		log:             slog.Default(),
	}
}

// Handler returns the API's HTTP handler with all routes and middleware.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handleHealth)

	authed := func(pattern string, h http.HandlerFunc) {
		mux.Handle(pattern, s.authenticate(h))
	}
	admin := func(pattern string, h http.HandlerFunc) {
		mux.Handle(pattern, s.authenticate(requireAdmin(h)))
	}

	authed("POST /v1/tasks", s.handleCreateTask)
	authed("GET /v1/tasks", s.handleListTasks)
	authed("GET /v1/tasks/{id}", s.handleGetTask)
	authed("POST /v1/tasks/{id}/cancel", s.handleCancelTask)
	authed("GET /v1/stats", s.handleStats)

	authed("POST /v1/workflows", s.handleCreateWorkflow)
	authed("GET /v1/workflows", s.handleListWorkflows)
	authed("GET /v1/workflows/{id}", s.handleGetWorkflow)
	authed("POST /v1/workflows/{id}/cancel", s.handleCancelWorkflow)

	admin("POST /v1/api-keys", s.handleCreateAPIKey)
	admin("GET /v1/api-keys", s.handleListAPIKeys)
	admin("DELETE /v1/api-keys/{id}", s.handleRevokeAPIKey)

	return s.logRequests(cors(mux))
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.db.Ping(ctx); err != nil {
		writeError(w, http.StatusServiceUnavailable, "database unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// --- helpers ---

type errorResponse struct {
	Error string `json:"error"`
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		slog.Debug("Failed to write response", "error", err)
	}
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, errorResponse{Error: msg})
}

// decode reads a JSON body into v, rejecting unknown fields and oversized
// bodies. It writes the error response itself and returns false on failure.
func (s *Server) decode(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, s.maxRequestBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		var tooBig *http.MaxBytesError
		switch {
		case errors.As(err, &tooBig):
			writeError(w, http.StatusRequestEntityTooLarge, fmt.Sprintf("request body exceeds %d bytes", tooBig.Limit))
		case errors.Is(err, io.EOF):
			writeError(w, http.StatusBadRequest, "request body is empty")
		default:
			writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		}
		return false
	}
	return true
}

// writeRPCError translates a coordinator error into an HTTP response.
func (s *Server) writeRPCError(w http.ResponseWriter, err error) {
	st, _ := status.FromError(err)
	switch st.Code() {
	case codes.InvalidArgument:
		writeError(w, http.StatusBadRequest, st.Message())
	case codes.NotFound:
		writeError(w, http.StatusNotFound, st.Message())
	case codes.FailedPrecondition:
		writeError(w, http.StatusConflict, st.Message())
	case codes.Unavailable, codes.DeadlineExceeded:
		writeError(w, http.StatusServiceUnavailable, "coordinator unavailable")
	default:
		s.log.Error("Coordinator call failed", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
	}
}

func (s *Server) internalError(w http.ResponseWriter, msg string, err error) {
	s.log.Error(msg, "error", err)
	writeError(w, http.StatusInternalServerError, "internal error")
}

func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Auth uses bearer tokens, not cookies, so allowing any origin is safe.
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

type statusRecorder struct {
	http.ResponseWriter
	code int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.code = code
	r.ResponseWriter.WriteHeader(code)
}

func (s *Server) logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, code: http.StatusOK}
		next.ServeHTTP(rec, r)

		level := slog.LevelInfo
		if r.Method == http.MethodGet && rec.code < 500 {
			level = slog.LevelDebug // polling would drown out everything else
		}
		s.log.Log(r.Context(), level, "HTTP request",
			"method", r.Method, "path", r.URL.Path, "status", rec.code, "duration", time.Since(start))
	})
}
