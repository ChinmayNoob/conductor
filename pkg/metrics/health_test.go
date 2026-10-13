package metrics

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestProbes(t *testing.T) {
	probe := func(h http.HandlerFunc) (int, string) {
		rec := httptest.NewRecorder()
		h(rec, httptest.NewRequest(http.MethodGet, "/", nil))
		return rec.Code, rec.Body.String()
	}

	if code, _ := probe(handleHealthz); code != http.StatusOK {
		t.Fatalf("healthz = %d", code)
	}
	if code, _ := probe(handleReadyz); code != http.StatusOK {
		t.Fatalf("readyz with no checks = %d", code)
	}

	var dbErr error
	AddReadyCheck("database", func(context.Context) error { return dbErr })
	t.Cleanup(func() { readyChecks = nil })
	if code, _ := probe(handleReadyz); code != http.StatusOK {
		t.Fatalf("readyz with a passing check = %d", code)
	}
	dbErr = errors.New("connection refused")
	code, body := probe(handleReadyz)
	if code != http.StatusServiceUnavailable || !strings.Contains(body, "database: connection refused") {
		t.Fatalf("readyz with a failing check = %d %q", code, body)
	}
	// Liveness doesn't depend on readiness.
	if code, _ := probe(handleHealthz); code != http.StatusOK {
		t.Fatalf("healthz = %d while not ready", code)
	}
}
