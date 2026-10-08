package api

import (
	"embed"
	"io/fs"
	"net/http"
	"path"
	"strings"
)

// The dashboard is a static app embedded in the binary. It runs in the
// browser and calls the /v1 API with the user's key, so it has exactly the
// API's permissions and no server-side surface of its own.
//
//go:embed all:ui
var uiFiles embed.FS

// uiPolicy allows only the dashboard's own files: no inline scripts, no
// third-party anything. The API key lives in the page, so this matters.
const uiPolicy = "default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self' data:; " +
	"connect-src 'self'; font-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'"

var uiTypes = map[string]string{
	".html": "text/html; charset=utf-8",
	".js":   "text/javascript; charset=utf-8",
	".css":  "text/css; charset=utf-8",
	".svg":  "image/svg+xml",
}

func uiHandler() http.Handler {
	root, err := fs.Sub(uiFiles, "ui")
	if err != nil {
		panic(err)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(path.Clean(r.URL.Path), "/ui")
		name = strings.TrimPrefix(name, "/")
		if name == "" {
			name = "index.html"
		}
		data, err := fs.ReadFile(root, name)
		if err != nil {
			http.NotFound(w, r)
			return
		}
		h := w.Header()
		h.Set("Content-Security-Policy", uiPolicy)
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("Cache-Control", "no-cache")
		if t, ok := uiTypes[path.Ext(name)]; ok {
			h.Set("Content-Type", t)
		}
		_, _ = w.Write(data)
	})
}
