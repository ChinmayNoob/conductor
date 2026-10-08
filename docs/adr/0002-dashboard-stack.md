# ADR 0002: The dashboard's stack

- **Status:** Accepted (Phase 4)

## Context

Phase 4 adds a web dashboard. The plan suggested React with Vite, or Go
`templ` with htmx to keep a single binary. Whatever we pick must:

- ship inside the one `conductor` binary, with no extra service to run;
- respect namespaces and admin-only views exactly as the API does;
- work offline (no CDN), since Conductor runs in private networks;
- be safe to hold an API key, the only credential Conductor has.

## Options

**React + Vite.** The richest ecosystem. But it needs a Node toolchain in
the build, and a bundle and dependency tree to audit and update, for what
amounts to about ten pages of tables and one diagram.

**Server-rendered (`templ` or `html/template`) + htmx.** No client
framework, but the server grows a second interface: every page and action
re-implements what the JSON handlers already do (auth, namespaces,
validation), and a cookie session brings CSRF to defend against.

**A static app on the JSON API.** Plain JavaScript modules (no framework,
no build step) embedded in the binary, calling `/v1` with the user's API
key as a Bearer header.

## Decision

The static app on the JSON API.

- **One interface.** The dashboard uses exactly the API's endpoints, so it
  has exactly the API's permissions, namespace rules and validation, and
  anything it can do, a script can do too. The only server-side code is a
  file handler.
- **No CSRF surface.** The key travels in an `Authorization` header, never
  a cookie, so other sites can't make the browser act with it.
- **XSS is the risk to manage**, since the page holds the key. Everything
  from the API is inserted with `textContent` (a small `h()` builder; no
  `innerHTML` anywhere), and a strict Content-Security-Policy allows only
  the dashboard's own scripts and styles: no inline code, no third-party
  origins. Even inline style attributes are refused; styles are set through
  the CSSOM.
- **No toolchain.** About 1,400 lines of JS and CSS, readable as shipped.

## Consequences

- The key is stored in `sessionStorage` (or `localStorage` when the user
  ticks "keep me signed in"). Revoking the key signs the dashboard out.
- Live views poll every 2–8 s while visible. That's fine for an operator
  console; a push channel can come later if needed.
- Three small API additions were needed and are useful on their own: task
  search and paging, and a per-minute timeline.
- If the dashboard outgrows plain modules (charts beyond a strip chart,
  heavy interaction), a framework can be adopted without changing the API.
