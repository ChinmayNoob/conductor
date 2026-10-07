package worker

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type containerSpec struct {
	Image   string   `json:"image"`
	Command []string `json:"command"`
	Memory  string   `json:"memory"`
	CPUs    float64  `json:"cpus"`
	Network string   `json:"network"`
}

// docker is a minimal client for the Docker Engine API over its Unix socket,
// enough to run a container to completion. It avoids pulling in the Docker
// SDK and its dependency tree.
type docker struct {
	http *http.Client
}

func newDocker(socket string) *docker {
	return &docker{http: &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", socket)
		},
	}}}
}

func (d *docker) do(ctx context.Context, method, path string, body any, out any) error {
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://docker"+path, r)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := d.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		var e struct {
			Message string `json:"message"`
		}
		if json.Unmarshal(msg, &e) == nil && e.Message != "" {
			msg = []byte(e.Message)
		}
		return fmt.Errorf("docker %s %s: %s (HTTP %d)", method, path, bytes.TrimSpace(msg), resp.StatusCode)
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	_, err = io.Copy(io.Discard, resp.Body)
	return err
}

// Ping reports whether the Docker daemon is reachable.
func (d *docker) Ping(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	return d.do(ctx, http.MethodGet, "/_ping", nil, nil)
}

// ensureImage pulls the image unless it is already present.
func (d *docker) ensureImage(ctx context.Context, image string) error {
	if d.do(ctx, http.MethodGet, "/images/"+image+"/json", nil, nil) == nil {
		return nil
	}
	name, tag := image, "latest"
	if i := strings.LastIndex(image, ":"); i > strings.LastIndex(image, "/") {
		name, tag = image[:i], image[i+1:]
	}
	q := url.Values{"fromImage": {name}, "tag": {tag}}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://docker/images/create?"+q.Encode(), nil)
	if err != nil {
		return err
	}
	resp, err := d.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("failed to pull %s: %s", image, bytes.TrimSpace(msg))
	}
	// The pull streams JSON progress messages; errors arrive in-stream.
	dec := json.NewDecoder(resp.Body)
	for {
		var msg struct {
			Error string `json:"error"`
		}
		if err := dec.Decode(&msg); errors.Is(err, io.EOF) {
			return nil
		} else if err != nil {
			return fmt.Errorf("failed to pull %s: %w", image, err)
		}
		if msg.Error != "" {
			return fmt.Errorf("failed to pull %s: %s", image, msg.Error)
		}
	}
}

// run runs a container to completion and returns its combined logs.
func (d *docker) run(ctx context.Context, taskID string, specJSON []byte, env []string, timeout time.Duration, maxOutput int) (string, error) {
	var spec containerSpec
	if err := json.Unmarshal(specJSON, &spec); err != nil {
		return "", fmt.Errorf("invalid container spec: %w", err)
	}
	memory, err := parseBytes(spec.Memory)
	if err != nil {
		return "", err
	}
	network := spec.Network
	if network == "" {
		network = "bridge"
	}

	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if err := d.ensureImage(runCtx, spec.Image); err != nil {
		return "", err
	}

	var created struct{ ID string }
	err = d.do(runCtx, http.MethodPost, "/containers/create", map[string]any{
		"Image":  spec.Image,
		"Cmd":    spec.Command,
		"Env":    env,
		"Labels": map[string]string{"conductor.task_id": taskID},
		"HostConfig": map[string]any{
			"Memory":      memory,
			"NanoCpus":    int64(spec.CPUs * 1e9),
			"NetworkMode": network,
		},
	}, &created)
	if err != nil {
		return "", err
	}
	id := created.ID
	// Always clean up, even if the task was cancelled.
	defer func() {
		rmCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = d.do(rmCtx, http.MethodDelete, "/containers/"+id+"?force=1", nil, nil)
	}()

	if err := d.do(runCtx, http.MethodPost, "/containers/"+id+"/start", nil, nil); err != nil {
		return "", err
	}

	type waitResult struct {
		StatusCode int
		Error      *struct{ Message string }
	}
	done := make(chan error, 1)
	var result waitResult
	go func() {
		// Wait without runCtx so we can still read the exit after a kill.
		done <- d.do(context.Background(), http.MethodPost, "/containers/"+id+"/wait", nil, &result)
	}()

	var stopErr error
	select {
	case err := <-done:
		if err != nil {
			return "", err
		}
	case <-runCtx.Done():
		killCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_ = d.do(killCtx, http.MethodPost, "/containers/"+id+"/kill", nil, nil)
		cancel()
		<-done
		if errors.Is(runCtx.Err(), context.DeadlineExceeded) && ctx.Err() == nil {
			stopErr = fmt.Errorf("task timed out after %v", timeout)
		} else {
			stopErr = context.Cause(ctx)
		}
	}

	logs := d.logs(id, maxOutput)
	if stopErr != nil {
		return logs, stopErr
	}
	if result.Error != nil && result.Error.Message != "" {
		return logs, errors.New(result.Error.Message)
	}
	if result.StatusCode != 0 {
		return logs, fmt.Errorf("container exited with status %d", result.StatusCode)
	}
	return logs, nil
}

// logs returns a container's stdout and stderr. Without a TTY, Docker
// multiplexes them: each frame is an 8-byte header (stream, 0, 0, 0, size)
// followed by size bytes.
func (d *docker) logs(id string, maxOutput int) string {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://docker/containers/"+id+"/logs?stdout=1&stderr=1", nil)
	if err != nil {
		return ""
	}
	resp, err := d.http.Do(req)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()

	out := newCappedBuffer(maxOutput)
	header := make([]byte, 8)
	for {
		if _, err := io.ReadFull(resp.Body, header); err != nil {
			break
		}
		size := int64(binary.BigEndian.Uint32(header[4:]))
		if _, err := io.CopyN(out, resp.Body, size); err != nil {
			break
		}
	}
	return out.String()
}

// parseBytes parses sizes like "512", "256k", "256m" or "1g".
func parseBytes(s string) (int64, error) {
	if s == "" {
		return 0, nil
	}
	mult := int64(1)
	switch suffix := strings.ToLower(s[len(s)-1:]); suffix {
	case "k":
		mult = 1 << 10
	case "m":
		mult = 1 << 20
	case "g":
		mult = 1 << 30
	}
	num := s
	if mult > 1 {
		num = s[:len(s)-1]
	}
	n, err := strconv.ParseInt(num, 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("invalid memory size %q", s)
	}
	return n * mult, nil
}
