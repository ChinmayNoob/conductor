package coordinator

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Gauges computed when Prometheus scrapes. Only the leader reports workers
// and queues: standbys know nothing about them, and one source per cluster
// keeps dashboards from double counting.
var (
	leaderDesc = prometheus.NewDesc("conductor_leader",
		"1 if this coordinator is the leader.", nil, nil)
	epochDesc = prometheus.NewDesc("conductor_leader_epoch",
		"The leadership epoch this coordinator holds (0 when standing by).", nil, nil)
	workersDesc = prometheus.NewDesc("conductor_workers",
		"Registered workers by state.", []string{"state"}, nil)
	slotsDesc = prometheus.NewDesc("conductor_slots",
		"Task slots across healthy workers: total, and busy.", []string{"state"}, nil)
	queueTasksDesc = prometheus.NewDesc("conductor_queue_tasks",
		"Unfinished tasks per queue: ready (due, waiting for a worker), delayed, paused (in a paused queue), and running.",
		[]string{"namespace", "queue", "state"}, nil)
	queueOldestDesc = prometheus.NewDesc("conductor_queue_oldest_ready_seconds",
		"How long the oldest due task in each queue has been waiting.",
		[]string{"namespace", "queue"}, nil)
)

// Collector reports the coordinator's state. Register it once per process.
func (s *Server) Collector() prometheus.Collector { return collector{s} }

type collector struct{ s *Server }

func (c collector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{leaderDesc, epochDesc, workersDesc, slotsDesc, queueTasksDesc, queueOldestDesc} {
		ch <- d
	}
}

func (c collector) Collect(ch chan<- prometheus.Metric) {
	s := c.s
	gauge := func(d *prometheus.Desc, v float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, labels...)
	}

	s.mu.RLock()
	epoch := s.epoch
	states := map[string]int{"healthy": 0, "unhealthy": 0, "draining": 0}
	total, busy := 0, 0
	for _, w := range s.workers {
		states[w.status()]++
		if w.IsHealthy {
			total += w.Slots
			busy += w.inFlight
		}
	}
	s.mu.RUnlock()

	leader := 0.0
	if epoch != 0 {
		leader = 1
	}
	gauge(leaderDesc, leader)
	gauge(epochDesc, float64(epoch))
	if epoch == 0 {
		return
	}
	for state, n := range states {
		gauge(workersDesc, float64(n), state)
	}
	gauge(slotsDesc, float64(total), "total")
	gauge(slotsDesc, float64(busy), "busy")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	depths, err := s.db.QueueDepths(ctx)
	if err != nil {
		s.log.Warn("Failed to collect queue metrics", "error", err)
		return
	}
	for _, d := range depths {
		gauge(queueTasksDesc, float64(d.Ready), d.Namespace, d.Queue, "ready")
		gauge(queueTasksDesc, float64(d.Delayed), d.Namespace, d.Queue, "delayed")
		gauge(queueTasksDesc, float64(d.Paused), d.Namespace, d.Queue, "paused")
		gauge(queueTasksDesc, float64(d.Running), d.Namespace, d.Queue, "running")
		oldest := 0.0
		if d.OldestReady != nil {
			oldest = max(time.Since(*d.OldestReady).Seconds(), 0)
		}
		gauge(queueOldestDesc, oldest, d.Namespace, d.Queue)
	}
}
