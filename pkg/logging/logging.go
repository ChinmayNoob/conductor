// Package logging configures the process-wide structured logger.
package logging

import (
	"log/slog"
	"os"
)

// Setup installs a slog logger tagged with the component name as the default
// and returns it.
func Setup(component string, level slog.Level, format string) *slog.Logger {
	opts := &slog.HandlerOptions{Level: level}
	var h slog.Handler
	if format == "json" {
		h = slog.NewJSONHandler(os.Stderr, opts)
	} else {
		h = slog.NewTextHandler(os.Stderr, opts)
	}
	log := slog.New(h).With("component", component)
	slog.SetDefault(log)
	return log
}
