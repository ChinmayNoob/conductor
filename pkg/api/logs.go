package api

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ChinmayNoob/conductor/pkg/db"
	"github.com/ChinmayNoob/conductor/pkg/grpcapi"
)

type attemptJSON struct {
	Attempt    int        `json:"attempt"`
	Status     string     `json:"status"`
	WorkerID   *int64     `json:"worker_id,omitempty"`
	StartedAt  *time.Time `json:"started_at,omitempty"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
	Error      string     `json:"error,omitempty"`
	Output     string     `json:"output,omitempty"`
	Current    bool       `json:"current"` // the attempt the task row describes
}

// handleTaskAttempts lists every attempt of a task: the earlier, failed ones
// from the history, then the latest from the task itself.
func (s *Server) handleTaskAttempts(w http.ResponseWriter, r *http.Request) {
	t := s.loadTask(w, r, r.PathValue("id"))
	if t == nil {
		return
	}
	past, err := s.db.ListAttempts(r.Context(), t.ID)
	if err != nil {
		s.internalError(w, "Failed to list attempts", err)
		return
	}
	out := make([]attemptJSON, 0, len(past)+1)
	for _, a := range past {
		finished := a.FinishedAt
		out = append(out, attemptJSON{
			Attempt: a.Attempt, Status: a.Status, WorkerID: a.WorkerID, StartedAt: a.StartedAt,
			FinishedAt: &finished, Error: a.ErrorMessage, Output: a.Output,
		})
	}
	// After a retry the row's attempt is already in the history until the
	// next dispatch; otherwise the row is the latest attempt.
	if t.Attempt > 0 && (len(past) == 0 || past[len(past)-1].Attempt < t.Attempt) {
		cur := attemptJSON{
			Attempt: t.Attempt, Status: attemptStatus(t), WorkerID: t.WorkerID, StartedAt: t.StartedAt,
			Error: t.ErrorMessage, Output: t.Output, Current: true,
		}
		for _, at := range []*time.Time{t.CompletedAt, t.FailedAt, t.CancelledAt} {
			if at != nil {
				cur.FinishedAt = at
			}
		}
		out = append(out, cur)
	}
	writeJSON(w, http.StatusOK, out)
}

// attemptStatus describes the latest attempt: a picked task that hasn't
// reported yet is DISPATCHED.
func attemptStatus(t *db.Task) string {
	if t.Status == db.StatusQueued && t.PickedAt != nil {
		return "DISPATCHED"
	}
	return string(t.Status)
}

const logPoll = 500 * time.Millisecond

// handleTaskLogs writes a task's output as plain text. While the task runs,
// the output comes live from its worker (only when someone asks, so the hot
// path pays nothing); once an attempt ends, from the stored output. With
// follow=true it keeps streaming, across retries, until the task finishes:
//
//	curl -N -H "Authorization: Bearer $KEY" "$API/v1/tasks/$ID/logs?follow=true"
func (s *Server) handleTaskLogs(w http.ResponseWriter, r *http.Request) {
	t := s.loadTask(w, r, r.PathValue("id"))
	if t == nil {
		return
	}
	q := r.URL.Query()
	follow := q.Get("follow") == "true" || q.Get("follow") == "1"
	offset, _ := strconv.ParseInt(q.Get("offset"), 10, 64)
	offset = max(offset, 0)

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	rc := http.NewResponseController(w)
	ctx := r.Context()

	attempt := t.Attempt
	ended := false // the current attempt's final output has been written
	for {
		if t.Attempt != attempt {
			// A retry started: what follows is a new attempt's output.
			_, _ = w.Write([]byte("\n--- attempt " + strconv.Itoa(t.Attempt) + " ---\n"))
			attempt, offset, ended = t.Attempt, 0, false
		}
		running := t.PickedAt != nil && !t.Status.Terminal()
		switch {
		case running:
			resp, err := s.coordinator.GetTaskOutput(ctx, &grpcapi.TaskOutputRequest{
				TaskId: t.ID.String(), Attempt: int32(t.Attempt), Offset: offset,
			})
			if err == nil && resp.Running {
				_, _ = w.Write(resp.Data)
				offset = resp.NextOffset
			}
		case t.Attempt > 0 && !ended:
			// The attempt ended (or is waiting to be retried): the rest of
			// its output is stored on the task.
			_, _ = w.Write([]byte(rest(t.Output, offset)))
			ended = true
		}
		_ = rc.Flush()
		if t.Status.Terminal() || !follow {
			return
		}
		if !sleepCtx(ctx, logPoll) {
			return
		}
		next, err := s.db.GetTask(ctx, t.ID)
		if err != nil || next == nil {
			return
		}
		t = next
	}
}

// rest returns the part of a stored output after what was already streamed.
// A truncated output's middle was dropped, so positions after the cut can't
// be matched up; then only a note is added.
func rest(output string, offset int64) string {
	if offset == 0 {
		return output
	}
	if strings.Contains(output, " bytes truncated] ...") {
		return "\n... [output truncated; the task's saved output has the beginning and end] ...\n"
	}
	if offset >= int64(len(output)) {
		return ""
	}
	return output[offset:]
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
