// Command docs builds the Conductor documentation in docs/slashdocs into
// self-contained HTML pages, and publishes them to a slashdocs server.
//
//	go run ./scripts/docs build                  # writes docs/slashdocs/dist/*.html
//	go run ./scripts/docs publish                # creates or updates each page in slashdocs
//	go run ./scripts/docs publish -m "Explain fencing"
//
// Publishing uses the slashdocs CLI (logged in with `slashdocs auth login`).
// The first publish creates one document per page; later ones push a new
// version of each page whose HTML changed. Links between pages point at
// their slashdocs documents. What was published where is kept in
// docs/slashdocs/published.json (per server; not committed).
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"html"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

const root = "docs/slashdocs"

type page struct {
	Slug    string `json:"slug"`
	Title   string `json:"title"`
	Summary string `json:"summary"`
}

// published is what a server holds of a page.
type published struct {
	ID      string `json:"id"`
	URL     string `json:"url"`
	Version string `json:"version"`
	SHA256  string `json:"sha256"`
}

// state maps server -> slug -> published page.
type state map[string]map[string]*published

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: docs build [-out DIR] | publish [-server URL] [-m SUMMARY] [-cli PATH]")
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "build":
		fs := flag.NewFlagSet("build", flag.ExitOnError)
		out := fs.String("out", filepath.Join(root, "dist"), "output directory")
		_ = fs.Parse(os.Args[2:])
		err = build(*out, nil)
	case "publish":
		fs := flag.NewFlagSet("publish", flag.ExitOnError)
		server := fs.String("server", envOr("SLASHDOCS_SERVER", "http://localhost:8080"), "slashdocs server")
		summary := fs.String("m", "", "change summary for updated pages (default: the latest commit's subject)")
		cli := fs.String("cli", "slashdocs", "slashdocs CLI")
		_ = fs.Parse(os.Args[2:])
		err = publish(*server, *summary, *cli)
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "docs:", err)
		os.Exit(1)
	}
}

func loadPages() ([]page, error) {
	b, err := os.ReadFile(filepath.Join(root, "pages.json"))
	if err != nil {
		return nil, err
	}
	var pages []page
	return pages, json.Unmarshal(b, &pages)
}

var (
	h2Re   = regexp.MustCompile(`<h2 id="([^"]+)">(.*?)</h2>`)
	linkRe = regexp.MustCompile(`\{\{link:([a-z0-9-]+)(#[A-Za-z0-9_-]+)?\}\}`)
)

// render builds one page. links maps slugs to URLs; unknown slugs link to
// a placeholder until published.
func render(tmpl string, pages []page, i int, links map[string]string) (string, error) {
	p := pages[i]
	body, err := os.ReadFile(filepath.Join(root, "pages", p.Slug+".html"))
	if err != nil {
		return "", err
	}
	content := string(body)
	url := func(slug string) string {
		if u, ok := links[slug]; ok {
			return u
		}
		return "#" + slug
	}

	var missing []string
	content = linkRe.ReplaceAllStringFunc(content, func(m string) string {
		parts := linkRe.FindStringSubmatch(m)
		if !hasPage(pages, parts[1]) {
			missing = append(missing, parts[1])
		}
		return url(parts[1]) + parts[2]
	})
	if len(missing) > 0 {
		return "", fmt.Errorf("%s links to unknown pages: %v", p.Slug, missing)
	}

	var nav, toc strings.Builder
	for j, q := range pages {
		current := ""
		if j == i {
			current = ` aria-current="page"`
		}
		fmt.Fprintf(&nav, "      <li><a href=\"%s\"%s>%d. %s</a></li>\n", url(q.Slug), current, j+1, html.EscapeString(q.Title))
	}
	for _, m := range h2Re.FindAllStringSubmatch(content, -1) {
		fmt.Fprintf(&toc, "      <li><a href=\"#%s\">%s</a></li>\n", m[1], m[2])
	}
	var pager string
	if i > 0 {
		pager += fmt.Sprintf(`<a href="%s">← %s</a>`, url(pages[i-1].Slug), html.EscapeString(pages[i-1].Title))
	} else {
		pager += "<span></span>"
	}
	if i < len(pages)-1 {
		pager += fmt.Sprintf(`<a href="%s">%s →</a>`, url(pages[i+1].Slug), html.EscapeString(pages[i+1].Title))
	}

	out := strings.NewReplacer(
		"{{TITLE}}", html.EscapeString(p.Title),
		"{{SUMMARY}}", html.EscapeString(p.Summary),
		"{{HOME}}", url(pages[0].Slug),
		"{{NAV}}", strings.TrimRight(nav.String(), "\n"),
		"{{TOC}}", strings.TrimRight(toc.String(), "\n"),
		"{{NUMBER}}", strconv.Itoa(i+1),
		"{{TOTAL}}", strconv.Itoa(len(pages)),
		"{{PAGER}}", pager,
		"{{SLUG}}", p.Slug,
		"{{REVISION}}", revision(p.Slug),
		"{{CONTENT}}", strings.TrimRight(content, "\n"),
	).Replace(tmpl)
	// slashdocs renders documents in a sandboxed frame that may open new
	// tabs but not navigate the page around it, so links leaving the page
	// (other documents, other sites) open in a new tab.
	out = externalLinkRe.ReplaceAllString(out, `<a href="$1" target="_blank" rel="noopener">`)
	return out, nil
}

var externalLinkRe = regexp.MustCompile(`<a href="(https?://[^"]*)">`)

func hasPage(pages []page, slug string) bool {
	for _, p := range pages {
		if p.Slug == slug {
			return true
		}
	}
	return false
}

// revision names the last commit that changed a page, so a page's HTML
// changes only when the page (or the template) does.
func revision(slug string) string {
	out, err := exec.Command("git", "log", "-1", "--format=%h", "--",
		filepath.Join(root, "pages", slug+".html"), filepath.Join(root, "template.html")).Output()
	if err != nil || len(bytes.TrimSpace(out)) == 0 {
		return "an uncommitted revision"
	}
	return "commit " + string(bytes.TrimSpace(out))
}

func build(out string, links map[string]string) error {
	pages, err := loadPages()
	if err != nil {
		return err
	}
	tmpl, err := os.ReadFile(filepath.Join(root, "template.html"))
	if err != nil {
		return err
	}
	if err := os.MkdirAll(out, 0o755); err != nil {
		return err
	}
	for i, p := range pages {
		doc, err := render(string(tmpl), pages, i, links)
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(out, p.Slug+".html"), []byte(doc), 0o644); err != nil {
			return err
		}
	}
	fmt.Printf("built %d pages into %s\n", len(pages), out)
	return nil
}

func publish(server, summary, cli string) error {
	pages, err := loadPages()
	if err != nil {
		return err
	}
	statePath := filepath.Join(root, "published.json")
	st := state{}
	if b, err := os.ReadFile(statePath); err == nil {
		if err := json.Unmarshal(b, &st); err != nil {
			return fmt.Errorf("%s: %w", statePath, err)
		}
	}
	if st[server] == nil {
		st[server] = map[string]*published{}
	}
	pubs := st[server]
	save := func() error {
		b, _ := json.MarshalIndent(st, "", "  ")
		return os.WriteFile(statePath, append(b, '\n'), 0o644)
	}
	if summary == "" {
		out, _ := exec.Command("git", "log", "-1", "--format=%s").Output()
		summary = "Update from the repository: " + strings.TrimSpace(string(out))
	}
	dist := filepath.Join(root, "dist")
	links := func() map[string]string {
		m := map[string]string{}
		for slug, p := range pubs {
			m[slug] = p.URL
		}
		return m
	}

	// Create documents for new pages first, so every page has a URL to
	// link to; then push every page whose HTML (with links) changed.
	if err := build(dist, links()); err != nil {
		return err
	}
	for _, p := range pages {
		if pubs[p.Slug] != nil {
			continue
		}
		var created struct {
			ID      string `json:"id"`
			URL     string `json:"url"`
			Version string `json:"current_version_id"`
		}
		if err := run(cli, &created, "--server", server, "--json", "docs", "create",
			filepath.Join(dist, p.Slug+".html"), "--title", "Conductor · "+p.Title); err != nil {
			return fmt.Errorf("creating %s: %w", p.Slug, err)
		}
		pubs[p.Slug] = &published{ID: created.ID, URL: created.URL, Version: created.Version}
		fmt.Printf("created %-12s %s\n", p.Slug, created.URL)
		if err := save(); err != nil {
			return err
		}
	}

	if err := build(dist, links()); err != nil {
		return err
	}
	for _, p := range pages {
		file := filepath.Join(dist, p.Slug+".html")
		b, err := os.ReadFile(file)
		if err != nil {
			return err
		}
		sum := sha256.Sum256(b)
		hash := hex.EncodeToString(sum[:])
		pub := pubs[p.Slug]
		if pub.SHA256 == hash {
			fmt.Printf("unchanged %-10s %s\n", p.Slug, pub.URL)
			continue
		}
		// A page created a moment ago still has placeholder links.
		msg := summary
		if pub.SHA256 == "" {
			msg = "Link to the other pages"
		}
		var pushed struct {
			ID string `json:"id"`
		}
		args := []string{"--server", server, "--json", "docs", "push", pub.ID, file, "--summary", msg}
		if pub.Version != "" {
			args = append(args, "--base-version", pub.Version)
		}
		if err := run(cli, &pushed, args...); err != nil {
			return fmt.Errorf("updating %s: %w", p.Slug, err)
		}
		pub.Version, pub.SHA256 = pushed.ID, hash
		fmt.Printf("updated %-12s %s\n", p.Slug, pub.URL)
		if err := save(); err != nil {
			return err
		}
	}
	fmt.Printf("\nStart here: %s\n", pubs[pages[0].Slug].URL)
	return nil
}

// run runs the CLI and decodes its JSON output into out.
func run(cli string, out any, args ...string) error {
	cmd := exec.Command(cli, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%w: %s", err, strings.TrimSpace(stderr.String()+" "+stdout.String()))
	}
	if err := json.Unmarshal(stdout.Bytes(), out); err != nil {
		return errors.Join(fmt.Errorf("unexpected output from %s: %s", cli, stdout.String()), err)
	}
	return nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
