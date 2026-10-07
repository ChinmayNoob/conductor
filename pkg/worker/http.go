package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"
)

type httpSpec struct {
	Method       string            `json:"method"`
	URL          string            `json:"url"`
	Headers      map[string]string `json:"headers"`
	Body         string            `json:"body"`
	ExpectStatus []int             `json:"expect_status"`
}

// runHTTP sends one HTTP request. It succeeds when the response status is in
// expect_status (default: any 2xx). Outputs are the status and the body.
func runHTTP(ctx context.Context, specJSON []byte, timeout time.Duration, maxOutput int) (string, map[string]string, error) {
	var spec httpSpec
	if err := json.Unmarshal(specJSON, &spec); err != nil {
		return "", nil, fmt.Errorf("invalid http spec: %w", err)
	}
	if spec.URL == "" {
		return "", nil, errors.New("http spec has no url")
	}
	if spec.Method == "" {
		spec.Method = http.MethodGet
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var body io.Reader
	if spec.Body != "" {
		body = strings.NewReader(spec.Body)
	}
	req, err := http.NewRequestWithContext(ctx, strings.ToUpper(spec.Method), spec.URL, body)
	if err != nil {
		return "", nil, fmt.Errorf("invalid request: %w", err)
	}
	for k, v := range spec.Headers {
		req.Header.Set(k, v)
	}
	if spec.Body != "" && req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("User-Agent", "conductor-worker")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return "", nil, fmt.Errorf("task timed out after %v", timeout)
		}
		return "", nil, err
	}
	defer resp.Body.Close()

	buf := newCappedBuffer(maxOutput)
	if _, err := io.Copy(buf, resp.Body); err != nil {
		return "", nil, fmt.Errorf("failed to read response: %w", err)
	}
	respBody := buf.String()
	output := fmt.Sprintf("HTTP %s\n%s", resp.Status, respBody)
	outputs := map[string]string{"status": strconv.Itoa(resp.StatusCode)}
	if len(respBody) <= maxOutputsBytes {
		outputs["body"] = respBody
	}

	ok := resp.StatusCode >= 200 && resp.StatusCode < 300
	if len(spec.ExpectStatus) > 0 {
		ok = slices.Contains(spec.ExpectStatus, resp.StatusCode)
	}
	if !ok {
		return output, outputs, fmt.Errorf("unexpected HTTP status %s", resp.Status)
	}
	return output, outputs, nil
}
