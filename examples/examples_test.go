package examples

import (
	"io/fs"
	"testing"

	"github.com/ChinmayNoob/conductor/pkg/workflow"
)

func TestExamplesAreValid(t *testing.T) {
	files, err := fs.Glob(Workflows, "workflows/*.yaml")
	if err != nil || len(files) == 0 {
		t.Fatalf("no examples found: %v", err)
	}
	for _, f := range files {
		data, err := fs.ReadFile(Workflows, f)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := workflow.Parse(data); err != nil {
			t.Errorf("%s: %v", f, err)
		}
	}
}
