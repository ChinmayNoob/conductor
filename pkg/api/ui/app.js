// Conductor dashboard: sign-in, the shell, and a hash router.

import { session, saveKey, setNamespace, signOut, get, h, APIError } from "./js/core.js";
import * as views from "./js/views.js";
import * as ops from "./js/ops.js";

const ROUTES = [
  [/^\/$/, views.overview, "overview"],
  [/^\/tasks$/, views.tasks, "tasks"],
  [/^\/tasks\/([0-9a-f-]{36})$/, views.task, "tasks"],
  [/^\/workflows$/, views.workflows, "workflows"],
  [/^\/workflows\/([0-9a-f-]{36})$/, views.workflow, "workflows"],
  [/^\/approvals$/, ops.approvals, "approvals"],
  [/^\/assistant$/, ops.assistant, "assistant"],
  [/^\/dead-letter$/, views.deadLetter, "dead-letter"],
  [/^\/schedules$/, views.schedules, "schedules"],
  [/^\/workers$/, views.workers, "workers", true],
];

const NAV = [
  ["overview", "#/", "Overview"],
  ["tasks", "#/tasks", "Tasks"],
  ["workflows", "#/workflows", "Workflows"],
  ["approvals", "#/approvals", "Approvals"],
  ["assistant", "#/assistant", "Assistant"],
  ["dead-letter", "#/dead-letter", "Dead letter"],
  ["schedules", "#/schedules", "Schedules"],
  ["workers", "#/workers", "Workers", true],
];

const root = document.getElementById("app");
let current = null; // the mounted view

// --- theme -----------------------------------------------------------------------

function applyTheme(theme) {
  if (theme) document.documentElement.dataset.theme = theme;
  else delete document.documentElement.dataset.theme;
}
function storedTheme() {
  try {
    return localStorage.getItem("conductor.theme") || "";
  } catch {
    return "";
  }
}
applyTheme(storedTheme());

function toggleTheme() {
  const dark = document.documentElement.dataset.theme
    ? document.documentElement.dataset.theme === "dark"
    : matchMedia("(prefers-color-scheme: dark)").matches;
  const next = dark ? "light" : "dark";
  applyTheme(next);
  try {
    localStorage.setItem("conductor.theme", next);
  } catch { /* the toggle still works for this page */ }
}

// --- sign-in -----------------------------------------------------------------------

// whoami checks the key and learns whether it is an admin key, in one call:
// listing namespaces needs an admin key, and a non-admin key gets 403.
async function whoami() {
  try {
    const namespaces = await get("/namespaces");
    session.admin = true;
    return namespaces.map((n) => n.name);
  } catch (err) {
    if (err instanceof APIError && err.status === 403) {
      session.admin = false;
      return [];
    }
    throw err;
  }
}

function login(message) {
  const key = h("input", { id: "key", type: "password", autocomplete: "current-password", required: true, spellcheck: "false" });
  const remember = h("input", { type: "checkbox", id: "remember" });
  const error = h("div", { class: "error", role: "alert" }, message || "");
  const form = h("form", {},
    h("div", { class: "name" }, "CONDUCTOR"),
    h("div", { class: "plate" }, "Operations panel"),
    h("p", {}, "Sign in with an API key. The dashboard acts with that key's permissions, in its namespace."),
    h("div", { class: "field" }, h("label", { class: "plate", for: "key" }, "API key"), key),
    error,
    h("label", { class: "check" }, remember, "Keep me signed in on this device"),
    h("button", { class: "btn primary", type: "submit", style: "width:100%;padding:9px" }, "Sign in"));
  form.addEventListener("submit", async (e) => {
    e.preventDefault();
    session.key = key.value.trim();
    try {
      const namespaces = await whoami();
      saveKey(session.key, remember.checked);
      start(namespaces);
    } catch (err) {
      session.key = "";
      error.textContent = err.status === 401 ? "That key was not accepted. Check it and try again." : err.message;
    }
  });
  root.replaceChildren(h("div", { class: "login" }, form));
  key.focus();
}

// --- shell and routing ------------------------------------------------------------------

function shell(namespaces) {
  const nav = h("nav", { "aria-label": "Sections" },
    NAV.filter(([, , , admin]) => !admin || session.admin).map(([id, href, label]) =>
      h("a", { href, "data-id": id }, h("span", { class: "lever", "aria-hidden": "true" }), label)));

  let ns = h("span", {}, session.namespace || "default");
  if (session.admin && namespaces.length > 1) {
    ns = h("select", { "aria-label": "Namespace" }, namespaces.map((n) =>
      h("option", { value: n, selected: n === (session.namespace || "default") }, n)));
    ns.addEventListener("change", () => {
      setNamespace(ns.value === "default" ? "" : ns.value);
      route();
    });
  }
  const main = h("main", { id: "main" });
  const rail = h("aside", { class: "rail" },
    h("div", { class: "brand" }, h("div", { class: "name" }, "CONDUCTOR"),
      h("div", { class: "ns" }, h("span", { class: "plate", style: "color:#7f909a" }, "Namespace"), ns)),
    nav,
    h("div", { class: "foot" },
      h("button", { type: "button", onclick: toggleTheme }, "Light / dark"),
      h("button", { type: "button", onclick: signOut }, "Sign out")));
  root.replaceChildren(h("div", { class: "shell" }, rail, main));
  return { nav, main };
}

let frame = null;

function route() {
  const [path, qs] = (location.hash.slice(1) || "/").split("?");
  const query = new URLSearchParams(qs || "");
  current?.stop?.();
  current = null;
  for (const [re, view, section, admin] of ROUTES) {
    const m = path.match(re);
    if (!m || (admin && !session.admin)) continue;
    for (const a of frame.nav.querySelectorAll("a")) {
      if (a.dataset.id === section) a.setAttribute("aria-current", "page");
      else a.removeAttribute("aria-current");
    }
    current = view(m.slice(1), query);
    frame.main.replaceChildren(current.el);
    window.scrollTo(0, 0);
    return;
  }
  frame.main.replaceChildren(h("div", { class: "empty" }, "No such page. ", h("a", { href: "#/" }, "Go to the overview")));
}

function start(namespaces) {
  frame = shell(namespaces);
  window.onhashchange = route;
  route();
}

async function boot() {
  if (!session.key) return login();
  root.replaceChildren(h("div", { class: "login faint" }, "Loading…"));
  try {
    start(await whoami());
  } catch (err) {
    login(err.status === 401 ? "Your saved key was not accepted. Sign in again." : err.message);
  }
}

boot();
