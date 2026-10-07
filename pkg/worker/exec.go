package worker

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// runShell runs command in the platform shell with a timeout. The process and
// everything it starts are killed when ctx is cancelled or the timeout hits.
// Output (stdout and stderr interleaved) is capped at maxOutput bytes.
func runShell(ctx context.Context, command string, timeout time.Duration, maxOutput int) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmd := shellCommand(ctx, command)
	out := newCappedBuffer(maxOutput)
	cmd.Stdout = out
	cmd.Stderr = out
	// If a killed process leaves children holding the output pipes, stop
	// waiting for them after a moment.
	cmd.WaitDelay = 2 * time.Second

	err := cmd.Run()
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return out.String(), fmt.Errorf("task timed out after %v", timeout)
	}
	return out.String(), err
}
