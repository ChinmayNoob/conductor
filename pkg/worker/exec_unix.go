//go:build !windows

package worker

import (
	"context"
	"os/exec"
	"syscall"
)

// shellCommand runs command with sh in its own process group, so cancelling
// kills the whole group rather than just the shell.
func shellCommand(ctx context.Context, command string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, "sh", "-c", command)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	return cmd
}
