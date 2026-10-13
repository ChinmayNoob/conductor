# Conductor documentation (slashdocs)

The full documentation, written as pages for [slashdocs](https://github.com/razorpay/slashdocs),
where people read it and comment on exact passages, and new versions are
published from this repository.

| File | What |
|---|---|
| `pages.json` | The pages, in order: slug, title, summary |
| `pages/<slug>.html` | Each page's content (HTML fragments) |
| `template.html` | The shared page: navigation, contents, light/dark theme, Mermaid diagrams |
| `../../scripts/docs` | Builds the pages and publishes them |

## Edit

Write plain HTML in `pages/<slug>.html`. Give every `<h2>` an `id`: it becomes
an entry in the page's contents. Link to another page with
`{{link:slug}}` or `{{link:slug#anchor}}`; the build turns these into the
slashdocs URLs of those documents. Diagrams are Mermaid in
`<pre class="mermaid">` inside a `<figure class="diagram">`; they follow the
reader's theme, and their source stays readable if the script can't load.
Callouts are `<div class="callout">` (or `warn`, `ok`, `danger`).

```sh
make docs                       # build into docs/slashdocs/dist/ (open the files in a browser)
```

The build fails on a link to a page that doesn't exist.

## Publish

```sh
slashdocs auth login https://your-slashdocs   # once
make docs-publish                              # or: go run ./scripts/docs publish -server URL -m "What changed"
```

The first publish creates one document per page; later ones push a new
version of each page whose HTML changed, with the latest commit's subject (or
`-m`) as the change summary. Pages that didn't change get no new version. The
document IDs live in `published.json` (per server, not committed).

To try it locally, run slashdocs with its Docker Compose (`docker compose up`
in a checkout of razorpay/slashdocs, then sign in at http://localhost:8080 as
`admin@example.com` / `password`) and publish to `http://localhost:8080`, the
default.
