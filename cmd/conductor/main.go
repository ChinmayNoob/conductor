// Command conductor runs one Conductor component: the coordinator, the HTTP
// API, or a worker. Configuration comes from environment variables; see
// .env.example.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/ChinmayNoob/conductor/pkg/app"
	"github.com/ChinmayNoob/conductor/pkg/config"
	"github.com/ChinmayNoob/conductor/pkg/logging"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

const usage = `Usage: conductor <command>

Commands:
  coordinator   Dispatch tasks to workers and drive workflows (gRPC, default :8080)
  api           Serve the HTTP API (default :8081)
  worker        Execute tasks (gRPC, default :9000)
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
	}

	cmd := os.Args[1]
	switch cmd {
	case "version", "--version":
		fmt.Println(version)
		return
	case "help", "-h", "--help":
		fmt.Print(usage)
		return
	}
	run, ok := runners[cmd]
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n%s", cmd, usage)
		os.Exit(2)
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
