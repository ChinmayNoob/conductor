// Package app wires components together and runs them until their context is
// cancelled, then shuts them down gracefully.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/ChinmayNoob/conductor/pkg/ai"
	"github.com/ChinmayNoob/conductor/pkg/api"
	"github.com/ChinmayNoob/conductor/pkg/config"
	"github.com/ChinmayNoob/conductor/pkg/coordclient"
	"github.com/ChinmayNoob/conductor/pkg/coordinator"
	"github.com/ChinmayNoob/conductor/pkg/db"
	"github.com/ChinmayNoob/conductor/pkg/grpcapi"
	"github.com/ChinmayNoob/conductor/pkg/llm"
	"github.com/ChinmayNoob/conductor/pkg/metrics"
	"github.com/ChinmayNoob/conductor/pkg/security"
	"github.com/ChinmayNoob/conductor/pkg/tracing"
	"github.com/ChinmayNoob/conductor/pkg/worker"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/stats"
)

// RunCoordinator serves the coordinator's gRPC API and dispatches tasks.
func RunCoordinator(ctx context.Context, cfg *config.Config) error {
	defer startTracing(ctx, "conductor-coordinator")()
	database, err := openDB(ctx, db.WithLocalWake(cfg.DB.DSN()))
	if err != nil {
		return err
	}
	defer database.Close()

	serverOpts, dialOpts, err := grpcOptions(cfg)
	if err != nil {
		return err
	}

	_, port, err := net.SplitHostPort(cfg.CoordinatorListen)
	if err != nil {
		return fmt.Errorf("invalid CONDUCTOR_COORDINATOR_LISTEN: %w", err)
	}
	address := cfg.CoordinatorAdvertiseAddr
	if address == "" {
		address = net.JoinHostPort(worker.LocalIP(net.JoinHostPort(cfg.DB.Host, cfg.DB.Port)), port)
	}
	hostname, _ := os.Hostname()

	srv := coordinator.NewServer(database, coordinator.Options{
		ID:            hostname + "/" + address,
		Address:       address,
		DSN:           db.WithUTC(cfg.DB.DSN()),
		DialOptions:   dialOpts,
		PriorityAging: cfg.PriorityAging,
	})
	prometheus.MustRegister(srv.Collector())
	metrics.Serve(ctx, cfg.MetricsListen)
	// Standbys turn every call away, pointing at the leader.
	grpcServer := grpc.NewServer(append(serverOpts, grpc.ChainUnaryInterceptor(srv.LeaderOnly))...)
	grpcapi.RegisterCoordinatorServiceServer(grpcServer, srv)

	lis, err := net.Listen("tcp", cfg.CoordinatorListen)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	runCtx, stopRun := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		srv.Run(runCtx)
		close(runDone)
	}()

	slog.Info("Coordinator listening", "addr", lis.Addr().String(), "advertise", address)
	err = serveUntilDone(ctx, func() error { return grpcServer.Serve(lis) })

	slog.Info("Shutting down")
	stopRun()
	gracefulStop(grpcServer, cfg.ShutdownTimeout)
	<-runDone
	return err
}

// RunAPI serves the HTTP API.
func RunAPI(ctx context.Context, cfg *config.Config) error {
	defer startTracing(ctx, "conductor-api")()
	database, err := openDB(ctx, cfg.DB.DSN())
	if err != nil {
		return err
	}
	defer database.Close()

	if err := bootstrapAPIKey(ctx, database, cfg); err != nil {
		return err
	}

	_, dialOpts, err := grpcOptions(cfg)
	if err != nil {
		return err
	}
	coord, err := coordclient.New(coordclient.Seeds(cfg.CoordinatorAddr), dialOpts...)
	if err != nil {
		return err
	}
	defer coord.Close()

	provider, prices, err := modelProvider()
	if err != nil {
		return err
	}
	assistant := &ai.Assistant{DB: database, Provider: provider, Prices: prices, Model: os.Getenv("CONDUCTOR_AI_MODEL")}
	go assistant.Run(ctx)

	srv := api.NewServer(database, coord, cfg.MaxRequestBytes)
	srv.SetAssistant(assistant)
	metrics.Serve(ctx, cfg.MetricsListen)
	httpServer := &http.Server{
		Addr:              cfg.APIListen,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	slog.Info("API listening", "addr", cfg.APIListen)
	err = serveUntilDone(ctx, func() error {
		if err := httpServer.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})

	slog.Info("Shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		slog.Warn("HTTP shutdown did not finish cleanly", "error", err)
	}
	return err
}

// RunWorker executes tasks for the coordinator. On shutdown it stops taking
// tasks and lets running ones finish, up to the shutdown timeout.
func RunWorker(ctx context.Context, cfg *config.Config) error {
	defer startTracing(ctx, "conductor-worker")()
	serverOpts, dialOpts, err := grpcOptions(cfg)
	if err != nil {
		return err
	}
	seeds := coordclient.Seeds(cfg.CoordinatorAddr)
	coord, err := coordclient.New(seeds, dialOpts...)
	if err != nil {
		return err
	}
	defer coord.Close()

	address := cfg.Worker.AdvertiseAddr
	if address == "" {
		_, port, err := net.SplitHostPort(cfg.WorkerListen)
		if err != nil {
			return fmt.Errorf("invalid CONDUCTOR_WORKER_LISTEN: %w", err)
		}
		address = net.JoinHostPort(worker.LocalIP(seeds[0]), port)
	}
	id := cfg.Worker.ID
	if id == 0 {
		id = worker.ID(address)
	}
	slog.SetDefault(slog.Default().With("worker_id", id))

	provider, prices, err := modelProvider()
	if err != nil {
		return err
	}
	w := worker.NewServer(worker.Options{
		ID:           id,
		Address:      address,
		Slots:        cfg.Worker.Slots,
		Labels:       cfg.Worker.Labels,
		MaxOutput:    cfg.MaxOutputBytes,
		PassEnv:      cfg.Worker.PassEnv,
		DockerSocket: cfg.Worker.DockerSocket,
		LLM:          provider,
		LLMPrices:    prices,
		Coordinator:  coord,
	})
	prometheus.MustRegister(w.Collector())
	metrics.Serve(ctx, cfg.MetricsListen)
	grpcServer := grpc.NewServer(serverOpts...)
	grpcapi.RegisterWorkerServiceServer(grpcServer, w)

	lis, err := net.Listen("tcp", cfg.WorkerListen)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	heartbeatCtx, stopHeartbeats := context.WithCancel(context.Background())
	defer stopHeartbeats()
	go w.RunHeartbeats(heartbeatCtx)

	slog.Info("Worker listening", "addr", lis.Addr().String(), "advertise", address,
		"slots", cfg.Worker.Slots, "labels", w.Labels())
	err = serveUntilDone(ctx, func() error { return grpcServer.Serve(lis) })

	slog.Info("Shutting down")
	drainCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	w.Drain(drainCtx)
	stopHeartbeats()
	gracefulStop(grpcServer, 5*time.Second)
	return err
}

// RunDev runs the coordinator, the API and one worker in this process: the
// quickest way to try Conductor or develop against it. Only Postgres is
// needed.
func RunDev(ctx context.Context, cfg *config.Config) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Everything talks over loopback.
	_, coordPort, err := net.SplitHostPort(cfg.CoordinatorListen)
	if err != nil {
		return fmt.Errorf("invalid CONDUCTOR_COORDINATOR_LISTEN: %w", err)
	}
	cfg.CoordinatorAddr = net.JoinHostPort("127.0.0.1", coordPort)
	cfg.CoordinatorAdvertiseAddr = cfg.CoordinatorAddr
	if cfg.Worker.AdvertiseAddr == "" {
		_, workerPort, err := net.SplitHostPort(cfg.WorkerListen)
		if err != nil {
			return fmt.Errorf("invalid CONDUCTOR_WORKER_LISTEN: %w", err)
		}
		cfg.Worker.AdvertiseAddr = net.JoinHostPort("127.0.0.1", workerPort)
	}

	// Migrate once up front so the components don't race to do it.
	database, err := openDB(ctx, cfg.DB.DSN())
	if err != nil {
		return err
	}
	database.Close()

	errc := make(chan error, 3)
	for name, run := range map[string]func(context.Context, *config.Config) error{
		"coordinator": RunCoordinator, "api": RunAPI, "worker": RunWorker,
	} {
		go func() {
			if err := run(ctx, cfg); err != nil {
				errc <- fmt.Errorf("%s: %w", name, err)
				return
			}
			errc <- nil
		}()
	}

	slog.Info("Conductor dev mode is up", "api", "http://localhost"+cfg.APIListen, "api_key", cfg.APIKey)
	var firstErr error
	for range 3 {
		if err := <-errc; err != nil && firstErr == nil {
			firstErr = err
			cancel() // one component failed: stop the rest
		}
	}
	return firstErr
}

func openDB(ctx context.Context, dsn string) (*db.DB, error) {
	connectCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	database, err := db.Open(connectCtx, dsn)
	if err != nil {
		return nil, err
	}
	if err := database.Migrate(ctx); err != nil {
		database.Close()
		return nil, fmt.Errorf("failed to migrate database: %w", err)
	}
	return database, nil
}

// bootstrapAPIKey stores the admin key from CONDUCTOR_API_KEY, so a fresh
// install has a way in.
func bootstrapAPIKey(ctx context.Context, database *db.DB, cfg *config.Config) error {
	if cfg.APIKey == "" {
		slog.Warn("CONDUCTOR_API_KEY is not set; only existing API keys will work")
		return nil
	}
	return database.EnsureAPIKey(ctx, "bootstrap", security.HashAPIKey(cfg.APIKey), security.KeyPrefix(cfg.APIKey), true)
}

func grpcOptions(cfg *config.Config) ([]grpc.ServerOption, []grpc.DialOption, error) {
	serverOpts, err := security.ServerOptions(cfg)
	if err != nil {
		return nil, nil, err
	}
	dialOpts, err := security.DialOptions(cfg)
	if err != nil {
		return nil, nil, err
	}
	// Trace every RPC except the steady heartbeat traffic.
	notHeartbeat := otelgrpc.WithFilter(func(info *stats.RPCTagInfo) bool {
		return !strings.HasSuffix(info.FullMethodName, "/SendHeartbeat")
	})
	serverOpts = append(serverOpts, grpc.StatsHandler(otelgrpc.NewServerHandler(notHeartbeat)))
	dialOpts = append(dialOpts, grpc.WithStatsHandler(otelgrpc.NewClientHandler(notHeartbeat)))
	return serverOpts, dialOpts, nil
}

// startTracing sets up tracing for a component and returns a function that
// flushes it on shutdown.
func startTracing(ctx context.Context, service string) func() {
	shutdown, err := tracing.Setup(ctx, service)
	if err != nil {
		slog.Warn("Tracing disabled", "error", err)
	}
	return func() {
		flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdown(flushCtx)
	}
}

// serveUntilDone runs serve until it fails or ctx is cancelled.
func serveUntilDone(ctx context.Context, serve func() error) error {
	errc := make(chan error, 1)
	go func() { errc <- serve() }()
	select {
	case <-ctx.Done():
		return nil
	case err := <-errc:
		return err
	}
}

// gracefulStop lets in-flight RPCs finish, forcing a stop after timeout.
func gracefulStop(s *grpc.Server, timeout time.Duration) {
	done := make(chan struct{})
	go func() {
		s.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		s.Stop()
	}
}

// modelProvider returns the language model provider configured in the
// environment, or nil when there is none (llm tasks are then unavailable).
func modelProvider() (llm.Provider, llm.Prices, error) {
	prices, err := llm.PricesFromEnv()
	if err != nil {
		return nil, nil, err
	}
	c := llm.ConfigFromEnv()
	if !c.Configured() {
		slog.Info("llm tasks disabled: set OPENAI_API_KEY (or CONDUCTOR_LLM_BASE_URL) to enable them")
		return nil, prices, nil
	}
	p, err := llm.New(c)
	if err != nil {
		return nil, nil, err
	}
	slog.Info("llm tasks enabled", "base_url", c.BaseURL, "default_model", c.DefaultModel, "priced_models", len(prices))
	return p, prices, nil
}
