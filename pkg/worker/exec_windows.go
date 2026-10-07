//go:build windows

package worker

import (
	"context"
	"os/exec"
	"strconv"
)

// shellCommand runs command with cmd.exe. Cancelling kills the process tree.
func shellCommand(ctx context.Context, command string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, "cmd", "/C", command)
	cmd.Cancel = func() error {
		return exec.Command("taskkill", "/T", "/F", "/PID", strconv.Itoa(cmd.Process.Pid)).Run()
	}
	return cmd
}
