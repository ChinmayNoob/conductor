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
	"time"

	"github.com/ChinmayNoob/conductor/pkg/api"
	"github.com/ChinmayNoob/conductor/pkg/config"
	"github.com/ChinmayNoob/conductor/pkg/coordinator"
	"github.com/ChinmayNoob/conductor/pkg/db"
	"github.com/ChinmayNoob/conductor/pkg/grpcapi"
	"github.com/ChinmayNoob/conductor/pkg/security"
	"github.com/ChinmayNoob/conductor/pkg/worker"
	"google.golang.org/grpc"
)

// RunCoordinator serves the coordinator's gRPC API and dispatches tasks.
func RunCoordinator(ctx context.Context, cfg *config.Config) error {
	database, err := openDB(ctx, cfg)
	if err != nil {
		return err
	}
	defer database.Close()

	serverOpts, dialOpts, err := grpcOptions(cfg)
	if err != nil {
		return err
	}

	srv := coordinator.NewServer(database, dialOpts)
	grpcServer := grpc.NewServer(serverOpts...)
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

	slog.Info("Coordinator listening", "addr", lis.Addr().String())
	err = serveUntilDone(ctx, func() error { return grpcServer.Serve(lis) })

	slog.Info("Shutting down")
	stopRun()
	gracefulStop(grpcServer, cfg.ShutdownTimeout)
	<-runDone
	return err
}

// RunAPI serves the HTTP API.
func RunAPI(ctx context.Context, cfg *config.Config) error {
	database, err := openDB(ctx, cfg)
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
	conn, err := grpc.NewClient(cfg.CoordinatorAddr, dialOpts...)
	if err != nil {
		return fmt.Errorf("failed to create coordinator client: %w", err)
	}
	defer conn.Close()

	srv := api.NewServer(database, grpcapi.NewCoordinatorServiceClient(conn), cfg.MaxRequestBytes)
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
	serverOpts, dialOpts, err := grpcOptions(cfg)
	if err != nil {
		return err
	}
	conn, err := grpc.NewClient(cfg.CoordinatorAddr, dialOpts...)
	if err != nil {
		return fmt.Errorf("failed to create coordinator client: %w", err)
	}
	defer conn.Close()

	address := cfg.Worker.AdvertiseAddr
	if address == "" {
		_, port, err := net.SplitHostPort(cfg.WorkerListen)
		if err != nil {
			return fmt.Errorf("invalid CONDUCTOR_WORKER_LISTEN: %w", err)
		}
		address = net.JoinHostPort(worker.LocalIP(cfg.CoordinatorAddr), port)
	}
	id := cfg.Worker.ID
	if id == 0 {
		id = worker.ID(address)
	}
	slog.SetDefault(slog.Default().With("worker_id", id))

	w := worker.NewServer(worker.Options{
		ID:          id,
		Address:     address,
		Slots:       1,
		MaxOutput:   cfg.MaxOutputBytes,
		Coordinator: grpcapi.NewCoordinatorServiceClient(conn),
	})
	grpcServer := grpc.NewServer(serverOpts...)
	grpcapi.RegisterWorkerServiceServer(grpcServer, w)

	lis, err := net.Listen("tcp", cfg.WorkerListen)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	heartbeatCtx, stopHeartbeats := context.WithCancel(context.Background())
	defer stopHeartbeats()
	go w.RunHeartbeats(heartbeatCtx)

	slog.Info("Worker listening", "addr", lis.Addr().String(), "advertise", address)
	err = serveUntilDone(ctx, func() error { return grpcServer.Serve(lis) })

	slog.Info("Shutting down")
	drainCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	w.Drain(drainCtx)
	stopHeartbeats()
	gracefulStop(grpcServer, 5*time.Second)
	return err
}

func openDB(ctx context.Context, cfg *config.Config) (*db.DB, error) {
	connectCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	database, err := db.Open(connectCtx, cfg.DB.DSN())
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
	return serverOpts, dialOpts, nil
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
