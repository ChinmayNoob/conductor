package worker

import (
	"context"
	"testing"

	"github.com/ChinmayNoob/conductor/pkg/grpcapi"
)

func TestTaskTypesLimitWhatAWorkerRuns(t *testing.T) {
	all := NewServer(Options{Labels: map[string]string{"region": "eu"}})
	if l := all.Labels(); l[LabelShell] != "true" || l[LabelHTTP] != "true" || l["region"] != "eu" {
		t.Fatalf("default labels = %v", l)
	}

	httpOnly := NewServer(Options{Types: []string{"http"}})
	if l := httpOnly.Labels(); l[LabelShell] != "" || l[LabelHTTP] != "true" {
		t.Fatalf("an http-only worker advertises %v", l)
	}
	resp, err := httpOnly.SubmitTask(context.Background(), &grpcapi.TaskRequest{TaskId: "t1", Type: "shell", Data: "id"})
	if err != nil || resp.Success {
		t.Fatalf("an http-only worker accepted a shell task: %+v, %v", resp, err)
	}
	if resp.Message != "shell tasks are not available on this worker" {
		t.Errorf("rejection = %q", resp.Message)
	}
}
