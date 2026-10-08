package worker

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"time"
)

// maxOutputsBytes caps what a task may write to $CONDUCTOR_OUTPUT.
const maxOutputsBytes = 64 << 10

var outputKeyRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_-]{0,127}$`)

// runShell runs command in the platform shell with a timeout and the given
// environment. The process and everything it starts are killed when ctx is
// cancelled or the timeout hits. Output (stdout and stderr interleaved) is
// capped at maxOutput bytes. key=value lines the command appends to the file
// named by $CONDUCTOR_OUTPUT are returned as outputs.
func runShell(ctx context.Context, command string, env []string, timeout time.Duration, out *cappedBuffer) (string, map[string]string, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	outFile, err := os.CreateTemp("", "conductor-output-*")
	if err != nil {
		return "", nil, fmt.Errorf("failed to create output file: %w", err)
	}
	outFile.Close()
	defer func() { _ = os.Remove(outFile.Name()) }()

	cmd := shellCommand(ctx, command)
	cmd.Env = append(env, "CONDUCTOR_OUTPUT="+outFile.Name())
	cmd.Stdout = out
	cmd.Stderr = out
	// If a killed process leaves children holding the output pipes, stop
	// waiting for them after a moment.
	cmd.WaitDelay = 2 * time.Second

	runErr := cmd.Run()
	outputs, err := readOutputs(outFile.Name())
	if err != nil && runErr == nil {
		runErr = err
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return out.String(), outputs, fmt.Errorf("task timed out after %v", timeout)
	}
	return out.String(), outputs, runErr
}

// readOutputs parses key=value lines, GitHub Actions style. Later lines win.
func readOutputs(path string) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil
	}
	defer f.Close()

	data, err := io.ReadAll(io.LimitReader(f, maxOutputsBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxOutputsBytes {
		return nil, fmt.Errorf("$CONDUCTOR_OUTPUT exceeds %d bytes", maxOutputsBytes)
	}

	outputs := make(map[string]string)
	sc := bufio.NewScanner(bytes.NewReader(data))
	sc.Buffer(make([]byte, maxOutputsBytes), maxOutputsBytes)
	for sc.Scan() {
		key, value, ok := bytes.Cut(sc.Bytes(), []byte("="))
		if !ok || !outputKeyRe.Match(key) {
			continue
		}
		outputs[string(key)] = string(bytes.TrimRight(value, "\r"))
	}
	if len(outputs) == 0 {
		return nil, nil
	}
	return outputs, nil
}

// taskEnv builds a task's environment. It starts from a minimal base rather
// than the worker's own environment, which holds cluster secrets.
func taskEnv(base []string, extra map[string]string) []string {
	env := append([]string(nil), base...)
	keys := make([]string, 0, len(extra))
	for k := range extra {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		env = append(env, k+"="+extra[k])
	}
	return env
}

// baseEnv returns the variables every task gets: a sane PATH and locale,
// plus any worker variables explicitly allowed through.
func baseEnv(passThrough []string) []string {
	env := []string{
		"PATH=" + getenv("PATH", "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"),
		"HOME=" + getenv("HOME", os.TempDir()),
		"TMPDIR=" + os.TempDir(),
		"LANG=" + getenv("LANG", "C.UTF-8"),
	}
	if tz := os.Getenv("TZ"); tz != "" {
		env = append(env, "TZ="+tz)
	}
	// Windows needs these to run anything.
	for _, k := range []string{"SystemRoot", "COMSPEC", "PATHEXT", "TEMP", "TMP"} {
		if v, ok := os.LookupEnv(k); ok {
			env = append(env, k+"="+v)
		}
	}
	for _, k := range passThrough {
		if v, ok := os.LookupEnv(k); ok {
			env = append(env, k+"="+v)
		}
	}
	return env
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
