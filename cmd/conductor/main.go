// Command conductor runs one Conductor component: the coordinator, the HTTP
// API, or a worker. Configuration comes from environment variables; see
// .env.example.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ChinmayNoob/conductor/pkg/app"
	"github.com/ChinmayNoob/conductor/pkg/config"
	"github.com/ChinmayNoob/conductor/pkg/llm"
	"github.com/ChinmayNoob/conductor/pkg/logging"
	"github.com/ChinmayNoob/conductor/pkg/security"
	_ "time/tzdata" // schedules may use any time zone, even without OS tz data
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

const usage = `Usage: conductor <command>

Commands:
  dev           Run everything (coordinator, API, one worker) in this process
  coordinator   Dispatch tasks to workers and drive workflows (gRPC, default :8080)
  api           Serve the HTTP API (default :8081)
  worker        Execute tasks (gRPC, default :9000)
  mock-llm      Serve a scripted stand-in for a language model (for tests and demos, default :8090)
  version       Print the version

Configuration is read from environment variables; see .env.example.
`

func main() {
	if len(os.Args) != 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}

	runners := map[string]func(context.Context, *config.Config) error{
		"coordinator": app.RunCoordinator,
		"api":         app.RunAPI,
		"worker":      app.RunWorker,
		"dev":         app.RunDev,
	}

	cmd := os.Args[1]
	switch cmd {
	case "version", "--version":
		fmt.Println(version)
		return
	case "help", "-h", "--help":
		fmt.Print(usage)
		return
	case "mock-llm":
		runMockLLM()
		return
	}
	run, ok := runners[cmd]
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n%s", cmd, usage)
		os.Exit(2)
	}

	if cmd == "dev" {
		devDefaults()
	}
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	log := logging.Setup(cmd, cfg.LogLevel, cfg.LogFormat)
	log.Info("Starting", "version", version)
	cfg.WarnInsecure(log)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg); err != nil {
		slog.Error("Exited with error", "error", err)
		os.Exit(1)
	}
	log.Info("Stopped")
}

// devDefaults fills in secrets for `conductor dev` so it runs with no setup.
// The cluster token is random (everything runs in this process); the API key
// is fixed so it's easy to use, and printed on startup.
func devDefaults() {
	if os.Getenv("CONDUCTOR_CLUSTER_TOKEN") == "" {
		_ = os.Setenv("CONDUCTOR_CLUSTER_TOKEN", security.GenerateAPIKey())
	}
	if os.Getenv("CONDUCTOR_API_KEY") == "" {
		_ = os.Setenv("CONDUCTOR_API_KEY", config.DevPrefix+"api-key")
	}
}

// runMockLLM serves llm.Mock on CONDUCTOR_MOCK_LLM_LISTEN (default :8090),
// an OpenAI-compatible endpoint at /v1. It needs no other configuration.
func runMockLLM() {
	addr := os.Getenv("CONDUCTOR_MOCK_LLM_LISTEN")
	if addr == "" {
		addr = ":8090"
	}
	slog.Info("Mock language model listening", "addr", addr, "base_url", "http://<host>"+addr+"/v1")
	srv := &http.Server{Addr: addr, Handler: llm.NewMock(), ReadHeaderTimeout: 10 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		_ = srv.Shutdown(context.Background())
	}()
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		slog.Error("Mock language model stopped", "error", err)
		os.Exit(1)
	}
}
