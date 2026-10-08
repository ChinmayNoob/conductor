//go:build e2e

package e2e

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/ChinmayNoob/conductor/pkg/client"
)

func TestDashboardIsServed(t *testing.T) {
	noRedirect := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := noRedirect.Get(apiURL + "/")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusFound || resp.Header.Get("Location") != "/ui/" {
		t.Fatalf("GET / = %d to %q, want a redirect to /ui/", resp.StatusCode, resp.Header.Get("Location"))
	}

	for path, wantType := range map[string]string{
		"/ui/":            "text/html",
		"/ui/app.js":      "text/javascript",
		"/ui/js/views.js": "text/javascript",
		"/ui/js/mimic.js": "text/javascript",
		"/ui/app.css":     "text/css",
	} {
		resp, err := http.Get(apiURL + path)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK || !strings.HasPrefix(resp.Header.Get("Content-Type"), wantType) {
			t.Errorf("GET %s = %d %s", path, resp.StatusCode, resp.Header.Get("Content-Type"))
		}
		// The page holds an API key: only its own scripts may run.
		if csp := resp.Header.Get("Content-Security-Policy"); !strings.Contains(csp, "script-src 'self'") || strings.Contains(csp, "unsafe") {
			t.Errorf("GET %s: weak CSP %q", path, csp)
		}
	}
	for _, path := range []string{"/ui/../go.mod", "/ui/%2e%2e/go.mod", "/ui/nope.js"} {
		resp, err := http.Get(apiURL + path)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			t.Errorf("GET %s = 200, want it refused", path)
		}
	}
}

func TestTaskSearchAndTimeline(t *testing.T) {
	c := newClient(t)
	marker := uniqueName("findme")
	var ids []string
	for i := range 3 {
		task := submit(t, c, client.TaskRequest{Command: "echo " + marker, Priority: i})
		ids = append(ids, task.ID)
		time.Sleep(20 * time.Millisecond) // distinct created_at for paging
	}

	// Search by part of the command, then page with before=.
	var page1 []client.Task
	getJSON(t, "/v1/tasks?limit=2&q="+marker, &page1)
	if len(page1) != 2 || page1[0].ID != ids[2] || page1[1].ID != ids[1] {
		t.Fatalf("search page 1 = %v, want the two newest", taskIDs(page1))
	}
	var page2 []client.Task
	getJSON(t, "/v1/tasks?limit=2&q="+marker+"&before="+page1[1].CreatedAt.Format(time.RFC3339Nano), &page2)
	if len(page2) != 1 || page2[0].ID != ids[0] {
		t.Fatalf("search page 2 = %v, want the oldest", taskIDs(page2))
	}
	// By ID prefix; and LIKE wildcards in the query are taken literally.
	var byID []client.Task
	getJSON(t, "/v1/tasks?q="+ids[0][:13], &byID)
	if len(byID) != 1 || byID[0].ID != ids[0] {
		t.Fatalf("search by ID prefix = %v", taskIDs(byID))
	}
	var wild []client.Task
	getJSON(t, "/v1/tasks?q=%25%25"+marker[len(marker)-6:]+"_none", &wild)
	if len(wild) != 0 {
		t.Fatalf("a wildcard query matched %d tasks", len(wild))
	}

	var points []struct {
		Minute time.Time `json:"minute"`
		Status string    `json:"status"`
		Count  int       `json:"count"`
	}
	getJSON(t, "/v1/stats/timeline?minutes=5", &points)
	total := 0
	for _, p := range points {
		total += p.Count
	}
	if total < 3 {
		t.Fatalf("timeline for the last 5 minutes counts %d tasks, want at least the 3 just submitted", total)
	}
}

func getJSON(t *testing.T, path string, out any) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, apiURL+path, nil)
	req.Header.Set("Authorization", "Bearer "+apiKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s = %d", path, resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		t.Fatal(err)
	}
}

func taskIDs(ts []client.Task) []string {
	var out []string
	for _, t := range ts {
		out = append(out, t.ID[:8])
	}
	return out
}
