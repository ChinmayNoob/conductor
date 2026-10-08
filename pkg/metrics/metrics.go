// Package metrics defines Conductor's Prometheus metrics and serves them.
//
// Counters and histograms are updated on the paths that already do the work
// (dispatch, results, HTTP requests). Gauges that describe state, such as
// queue depth and worker slots, are computed when Prometheus scrapes, so they
// cost nothing in between.
package metrics

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const ns = "conductor"

// Latency buckets from 1 ms to about 2 minutes.
var latencyBuckets = prometheus.ExponentialBuckets(0.001, 2.5, 14)

// Coordinator.
var (
	TasksDispatched = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "tasks_dispatched_total",
		Help: "Tasks handed to a worker.",
	}, []string{"namespace", "queue"})

	TaskResults = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "task_results_total",
		Help: "Task attempts by outcome: completed, failed (permanently), retried, or stale (a report for an old attempt, ignored).",
	}, []string{"namespace", "queue", "result"})

	TasksLost = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Name: "tasks_lost_total",
		Help: "Running tasks given up on because their worker died or they never reported.",
	})

	DispatchRejected = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Name: "dispatch_failures_total",
		Help: "Dispatches a worker refused or couldn't be reached for; the task was requeued.",
	})

	DispatchLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: ns, Name: "dispatch_latency_seconds",
		Help:    "From a task becoming due (submitted, or its retry or delay elapsing) to being claimed for a worker, including any time its queue was paused.",
		Buckets: latencyBuckets,
	})

	TaskRunTime = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: ns, Name: "task_turnaround_seconds",
		Help:    "From dispatch to the result reaching the coordinator.",
		Buckets: latencyBuckets,
	})

	WorkflowsFinished = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "workflows_finished_total",
		Help: "Workflow runs that reached a final status.",
	}, []string{"namespace", "status"})

	StepsCompensated = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Name: "workflow_compensations_total",
		Help: "Compensation tasks started for workflow steps.",
	})

	SchedulesFired = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Name: "schedule_runs_total",
		Help: "Schedule runs started.",
	})

	LeaderElections = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Name: "leader_elections_total",
		Help: "Times this coordinator became leader.",
	})
)

// Worker.
var (
	WorkerTaskDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: ns, Name: "worker_task_duration_seconds",
		Help:    "How long tasks ran on this worker, by type and outcome.",
		Buckets: latencyBuckets,
	}, []string{"type", "result"})

	WorkerReportFailures = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Name: "worker_report_failures_total",
		Help: "Task results this worker could not deliver to a coordinator.",
	})
)

// API.
var (
	HTTPRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "http_requests_total",
		Help: "API requests by route and status code.",
	}, []string{"route", "code"})

	HTTPDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: ns, Name: "http_request_duration_seconds",
		Help:    "API request latency by route.",
		Buckets: latencyBuckets,
	}, []string{"route"})
)

var once sync.Once

// Serve exposes /metrics on addr until ctx ends. In dev mode every component
// calls it; only the first call listens. An empty addr disables it.
func Serve(ctx context.Context, addr string) {
	if addr == "" {
		return
	}
	once.Do(func() {
		mux := http.NewServeMux()
		mux.Handle("GET /metrics", promhttp.Handler())
		srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
		lis, err := net.Listen("tcp", addr)
		if err != nil {
			slog.Warn("Metrics disabled: cannot listen", "addr", addr, "error", err)
			return
		}
		slog.Info("Metrics listening", "addr", lis.Addr().String())
		go func() {
			if err := srv.Serve(lis); err != nil && !errors.Is(err, http.ErrServerClosed) {
				slog.Warn("Metrics server stopped", "error", err)
			}
		}()
		go func() {
			<-ctx.Done()
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = srv.Shutdown(shutdownCtx)
		}()
	})
}
