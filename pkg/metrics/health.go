package metrics

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Probes for orchestrators such as Kubernetes, served next to /metrics:
//
//	/healthz  the process is up (liveness)
//	/readyz   every registered readiness check passes (readiness)

var (
	checksMu    sync.Mutex
	readyChecks []readyCheck
)

type readyCheck struct {
	name  string
	check func(context.Context) error
}

// AddReadyCheck registers a condition /readyz requires, such as "the database
// answers". Checks run on every probe, so they must be cheap.
func AddReadyCheck(name string, check func(context.Context) error) {
	checksMu.Lock()
	defer checksMu.Unlock()
	readyChecks = append(readyChecks, readyCheck{name, check})
}

func handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write([]byte("ok\n"))
}

func handleReadyz(w http.ResponseWriter, r *http.Request) {
	checksMu.Lock()
	checks := append([]readyCheck(nil), readyChecks...)
	checksMu.Unlock()

	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	var failed []string
	for _, c := range checks {
		if err := c.check(ctx); err != nil {
			failed = append(failed, fmt.Sprintf("%s: %v", c.name, err))
		}
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	if len(failed) > 0 {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(strings.Join(failed, "\n") + "\n"))
		return
	}
	_, _ = w.Write([]byte("ok\n"))
}
