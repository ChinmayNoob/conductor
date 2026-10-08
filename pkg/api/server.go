// Package api is Conductor's HTTP API. It reads state straight from Postgres
// and sends commands (submit, cancel) to the coordinator over gRPC.
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"github.com/ChinmayNoob/conductor/pkg/db"
	"github.com/ChinmayNoob/conductor/pkg/grpcapi"
	"github.com/ChinmayNoob/conductor/pkg/metrics"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// nameRe validates names of namespaces, queues, schedules and definitions.
var nameRe = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,62}$`)

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
	mux.Handle("GET /ui/", uiHandler())
	mux.Handle("GET /{$}", http.RedirectHandler("/ui/", http.StatusFound))

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
	authed("POST /v1/tasks/{id}/requeue", s.handleRequeueTask)
	authed("GET /v1/tasks/{id}/attempts", s.handleTaskAttempts)
	authed("GET /v1/tasks/{id}/logs", s.handleTaskLogs)
	authed("GET /v1/dead-letter", s.handleDeadLetter)
	authed("GET /v1/stats", s.handleStats)
	authed("GET /v1/stats/timeline", s.handleTimeline)

	authed("PUT /v1/workflow-definitions", s.handleSaveDefinition)
	authed("POST /v1/workflow-definitions", s.handleSaveDefinition)
	authed("GET /v1/workflow-definitions", s.handleListDefinitions)
	authed("GET /v1/workflow-definitions/{name}", s.handleGetDefinition)

	authed("POST /v1/workflows", s.handleCreateWorkflow)
	authed("GET /v1/workflows", s.handleListWorkflows)
	authed("GET /v1/workflows/{id}", s.handleGetWorkflow)
	authed("POST /v1/workflows/{id}/cancel", s.handleCancelWorkflow)

	authed("POST /v1/schedules", s.handleCreateSchedule)
	authed("GET /v1/schedules", s.handleListSchedules)
	authed("GET /v1/schedules/{name}", s.handleGetSchedule)
	authed("DELETE /v1/schedules/{name}", s.handleDeleteSchedule)
	authed("POST /v1/schedules/{name}/pause", s.handlePauseSchedule)
	authed("POST /v1/schedules/{name}/resume", s.handleResumeSchedule)
	authed("POST /v1/schedules/{name}/trigger", s.handleTriggerSchedule)

	authed("GET /v1/queues", s.handleListQueues)
	authed("PUT /v1/queues/{name}", s.handlePutQueue)
	authed("DELETE /v1/queues/{name}", s.handleDeleteQueue)

	admin("GET /v1/workers", s.handleListWorkers)
	admin("GET /v1/cluster", s.handleCluster)

	admin("POST /v1/namespaces", s.handleCreateNamespace)
	admin("GET /v1/namespaces", s.handleListNamespaces)
	admin("PUT /v1/namespaces/{name}", s.handleUpdateNamespace)

	admin("POST /v1/api-keys", s.handleCreateAPIKey)
	admin("GET /v1/api-keys", s.handleListAPIKeys)
	admin("DELETE /v1/api-keys/{id}", s.handleRevokeAPIKey)

	// Tracing wraps everything but health checks. otelhttp names each span
	// once the mux has matched a route.
	return otelhttp.NewHandler(s.logRequests(cors(mux)), "http",
		otelhttp.WithFilter(func(r *http.Request) bool { return r.URL.Path != "/health" }),
		otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string {
			if r.Pattern != "" {
				return r.Pattern
			}
			return r.Method
		}))
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

// createdOr returns 201 for a new resource and 200 for an existing one (e.g.
// a repeated idempotency key).
func createdOr(created bool) int {
	if created {
		return http.StatusCreated
	}
	return http.StatusOK
}

// readBody reads a request body, enforcing the size limit. It writes the
// error response itself and returns false on failure.
func (s *Server) readBody(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, s.maxRequestBytes))
	var tooBig *http.MaxBytesError
	switch {
	case errors.As(err, &tooBig):
		writeError(w, http.StatusRequestEntityTooLarge, fmt.Sprintf("request body exceeds %d bytes", tooBig.Limit))
		return nil, false
	case err != nil:
		writeError(w, http.StatusBadRequest, "failed to read request body")
		return nil, false
	case len(body) == 0:
		writeError(w, http.StatusBadRequest, "request body is empty")
		return nil, false
	}
	return body, true
}

// decode reads a JSON body into v, rejecting unknown fields and oversized
// bodies. It writes the error response itself and returns false on failure.
func (s *Server) decode(w http.ResponseWriter, r *http.Request, v any) bool {
	body, ok := s.readBody(w, r)
	if !ok {
		return false
	}
	return decodeJSON(w, body, v)
}

func decodeJSON(w http.ResponseWriter, body []byte, v any) bool {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
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
	case codes.FailedPrecondition, codes.AlreadyExists:
		writeError(w, http.StatusConflict, st.Message())
	case codes.ResourceExhausted:
		writeError(w, http.StatusTooManyRequests, st.Message())
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

// parseLimit reads ?limit= (default 100, max 1000).
func parseLimit(w http.ResponseWriter, r *http.Request) (int, bool) {
	raw := r.URL.Query().Get("limit")
	if raw == "" {
		return 100, true
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 || n > 1000 {
		writeError(w, http.StatusBadRequest, "limit must be between 1 and 1000")
		return 0, false
	}
	return n, true
}

func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Auth uses bearer tokens, not cookies, so allowing any origin is safe.
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, X-Conductor-Namespace")
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

// Unwrap lets http.ResponseController reach the real writer (to flush).
func (r *statusRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

func (s *Server) logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, code: http.StatusOK}
		next.ServeHTTP(rec, r)

		// The mux records the matched pattern on the request: a bounded
		// label, unlike the path.
		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}
		metrics.HTTPRequests.WithLabelValues(route, strconv.Itoa(rec.code)).Inc()
		metrics.HTTPDuration.WithLabelValues(route).Observe(time.Since(start).Seconds())

		level := slog.LevelInfo
		if r.Method == http.MethodGet && rec.code < 500 {
			level = slog.LevelDebug // polling would drown out everything else
		}
		s.log.Log(r.Context(), level, "HTTP request",
			"method", r.Method, "path", r.URL.Path, "status", rec.code, "duration", time.Since(start))
	})
}
