// Command conductor-operator manages Conductor workflows, runs and
// schedules as Kubernetes resources (Workflow, WorkflowRun, Schedule), so
// they can live in Git and be applied with kubectl or Argo CD.
//
// It talks to Conductor's HTTP API: CONDUCTOR_URL and CONDUCTOR_API_KEY (an
// admin key if resources name other Conductor namespaces).
package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/ChinmayNoob/conductor/operator/api/v1alpha1"
	"github.com/ChinmayNoob/conductor/operator/internal/controller"
	conductor "github.com/ChinmayNoob/conductor/pkg/client"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

func main() {
	var (
		url              = flag.String("conductor-url", envOr("CONDUCTOR_URL", "http://localhost:8081"), "Conductor API base URL")
		defaultNamespace = flag.String("default-namespace", os.Getenv("CONDUCTOR_NAMESPACE"),
			"Conductor namespace for resources that name none; empty means the one named after each resource's Kubernetes namespace (created if missing)")
		watch       = flag.String("watch-namespaces", os.Getenv("WATCH_NAMESPACES"), "Kubernetes namespaces to watch, comma separated; empty watches all")
		poll        = flag.Duration("poll-interval", 5*time.Second, "how often unfinished WorkflowRuns are checked")
		resync      = flag.Duration("resync", 5*time.Minute, "how often Workflows and Schedules are re-applied and their status refreshed")
		metricsAddr = flag.String("metrics-bind-address", ":8080", "metrics endpoint; 0 disables it")
		probeAddr   = flag.String("health-probe-bind-address", ":8081", "health and readiness probes")
		leaderElect = flag.Bool("leader-elect", false, "elect a leader, so several replicas can run")
	)
	opts := zap.Options{}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	log := ctrl.Log.WithName("setup")

	apiKey := os.Getenv("CONDUCTOR_API_KEY")
	if apiKey == "" {
		log.Error(nil, "CONDUCTOR_API_KEY is required")
		os.Exit(1)
	}

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		panic(err)
	}
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		panic(err)
	}

	mgrOpts := ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: *metricsAddr},
		HealthProbeBindAddress: *probeAddr,
		LeaderElection:         *leaderElect,
		LeaderElectionID:       "conductor-operator.conductor.dev",
	}
	if namespaces := splitList(*watch); len(namespaces) > 0 {
		mgrOpts.Cache.DefaultNamespaces = map[string]cache.Config{}
		for _, ns := range namespaces {
			mgrOpts.Cache.DefaultNamespaces[ns] = cache.Config{}
		}
	}
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), mgrOpts)
	if err != nil {
		log.Error(err, "Failed to create the manager")
		os.Exit(1)
	}

	c := controller.Options{
		Conductor:        conductor.New(*url, apiKey),
		DefaultNamespace: *defaultNamespace,
		Known:            &sync.Map{},
		PollInterval:     *poll,
		Resync:           *resync,
	}
	for name, setup := range map[string]interface{ SetupWithManager(ctrl.Manager) error }{
		"Workflow":    &controller.WorkflowReconciler{Client: mgr.GetClient(), Options: c},
		"WorkflowRun": &controller.WorkflowRunReconciler{Client: mgr.GetClient(), Options: c},
		"Schedule":    &controller.ScheduleReconciler{Client: mgr.GetClient(), Options: c},
	} {
		if err := setup.SetupWithManager(mgr); err != nil {
			log.Error(err, "Failed to set up a controller", "kind", name)
			os.Exit(1)
		}
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		panic(err)
	}
	// Ready once Conductor's API answers.
	if err := mgr.AddReadyzCheck("conductor", func(r *http.Request) error {
		if err := c.Conductor.Health(r.Context()); err != nil {
			return fmt.Errorf("conductor API at %s: %w", *url, err)
		}
		return nil
	}); err != nil {
		panic(err)
	}

	log.Info("Starting", "conductor", *url, "default_namespace", *defaultNamespace, "watch", *watch)
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		log.Error(err, "Manager stopped")
		os.Exit(1)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func splitList(s string) []string {
	var out []string
	for _, item := range strings.Split(s, ",") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}
