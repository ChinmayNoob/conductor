// Shared pieces: the API client, a DOM builder, formatting, and the mapping
// from Conductor's statuses to signal lamps.
//
// Everything that comes from the API is inserted as text, never as HTML.

const KEY = "conductor.key";
const NS = "conductor.namespace";

// --- session -----------------------------------------------------------------

export const session = {
  key: sessionStorage.getItem(KEY) || localStorage.getItem(KEY) || "",
  namespace: sessionStorage.getItem(NS) || "",
  admin: false,
  keyNamespace: "default",
};

export function saveKey(key, remember) {
  session.key = key;
  sessionStorage.setItem(KEY, key);
  if (remember) localStorage.setItem(KEY, key);
}

export function setNamespace(ns) {
  session.namespace = ns;
  sessionStorage.setItem(NS, ns);
}

export function signOut() {
  sessionStorage.removeItem(KEY);
  sessionStorage.removeItem(NS);
  localStorage.removeItem(KEY);
  session.key = "";
  session.namespace = "";
  location.hash = "#/";
  location.reload();
}

// --- API -------------------------------------------------------------------------

export class APIError extends Error {
  constructor(status, message) {
    super(message);
    this.status = status;
  }
}

function headers(extra = {}) {
  const h = { Authorization: "Bearer " + session.key, ...extra };
  if (session.namespace) h["X-Conductor-Namespace"] = session.namespace;
  return h;
}

export async function api(method, path, body) {
  const opts = { method, headers: headers() };
  if (typeof body === "string") {
    // Raw text, e.g. a YAML definition.
    opts.headers["Content-Type"] = "application/yaml";
    opts.body = body;
  } else if (body !== undefined) {
    opts.headers["Content-Type"] = "application/json";
    opts.body = JSON.stringify(body);
  }
  const res = await fetch("/v1" + path, opts);
  if (res.status === 401) {
    signOut();
    throw new APIError(401, "Your API key was rejected. Sign in again.");
  }
  const text = await res.text();
  let data = null;
  try {
    data = text ? JSON.parse(text) : null;
  } catch {
    data = null;
  }
  if (!res.ok) throw new APIError(res.status, (data && data.error) || text || res.statusText);
  return data;
}

export const get = (path) => api("GET", path);

// stream reads a plain-text endpoint as it arrives, calling onChunk with each
// piece. It resolves when the server ends the response or signal aborts.
export async function stream(path, onChunk, signal) {
  const res = await fetch("/v1" + path, { headers: headers(), signal });
  if (!res.ok) throw new APIError(res.status, await res.text());
  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    onChunk(decoder.decode(value, { stream: true }));
  }
}

// --- DOM -----------------------------------------------------------------------------

// h builds an element. Attributes starting with "on" become listeners;
// children may be strings (inserted as text), nodes, arrays, or null.
export function h(tag, attrs, ...children) {
  const el = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs || {})) {
    if (v === null || v === undefined || v === false) continue;
    if (k.startsWith("on")) el.addEventListener(k.slice(2), v);
    else if (k === "class") el.className = v;
    // Through the CSSOM: the page's CSP blocks inline style attributes.
    else if (k === "style") el.style.cssText = v;
    else el.setAttribute(k, v === true ? "" : v);
  }
  append(el, children);
  return el;
}

function append(el, children) {
  for (const c of children) {
    if (c === null || c === undefined || c === false) continue;
    if (Array.isArray(c)) append(el, c);
    else el.append(c instanceof Node ? c : String(c));
  }
}

const SVG = "http://www.w3.org/2000/svg";
export function s(tag, attrs, ...children) {
  const el = document.createElementNS(SVG, tag);
  for (const [k, v] of Object.entries(attrs || {})) {
    if (v === null || v === undefined || v === false) continue;
    if (k.startsWith("on")) el.addEventListener(k.slice(2), v);
    else el.setAttribute(k, v);
  }
  for (const c of children.flat()) {
    if (c === null || c === undefined || c === false) continue;
    el.append(c instanceof Node ? c : String(c));
  }
  return el;
}

export function toast(message) {
  const el = h("div", { class: "toast", role: "status" }, message);
  document.body.append(el);
  setTimeout(() => el.remove(), 3200);
}

// --- formatting ------------------------------------------------------------------------

export const shortID = (id) => (id ? id.slice(0, 8) : "");

export function ago(iso) {
  if (!iso) return "–";
  const secs = (Date.now() - new Date(iso).getTime()) / 1000;
  if (secs < 0) return "in " + span(-secs);
  return span(secs) + " ago";
}

export function span(secs) {
  if (secs < 1) return Math.round(secs * 1000) + " ms";
  if (secs < 60) return (secs < 10 ? secs.toFixed(1) : Math.round(secs)) + " s";
  if (secs < 3600) return Math.floor(secs / 60) + " m " + Math.round(secs % 60) + " s";
  if (secs < 86400) return Math.floor(secs / 3600) + " h " + Math.floor((secs % 3600) / 60) + " m";
  return Math.floor(secs / 86400) + " d";
}

export function took(from, to) {
  if (!from || !to) return "–";
  return span((new Date(to) - new Date(from)) / 1000);
}

export const when = (iso) => (iso ? new Date(iso).toLocaleString() : "–");

// --- statuses as lamps -----------------------------------------------------------------

const LAMPS = {
  COMPLETED: ["green", "Completed"],
  STARTED: ["amber", "Running", true],
  RUNNING: ["amber", "Running", true],
  DISPATCHED: ["amber", "Dispatched", true],
  QUEUED: ["", "Queued"],
  PENDING: ["", "Pending"],
  FAILED: ["red", "Failed"],
  CANCELLED: ["", "Cancelled"],
  SKIPPED: ["", "Skipped"],
  COMPENSATING: ["violet", "Undoing", true],
  COMPENSATED: ["violet", "Undone"],
  COMPENSATION_FAILED: ["red", "Undo failed"],
  healthy: ["green", "Healthy"],
  draining: ["amber", "Draining"],
  unhealthy: ["red", "Unhealthy"],
};

export function lampOf(status) {
  const [color, label, live] = LAMPS[status] || ["", status || "–"];
  return { color, label, live: !!live };
}

export function statusEl(status) {
  const l = lampOf(status);
  return h("span", { class: "status" }, h("span", { class: `lamp ${l.color}${l.live ? " live" : ""}`, "aria-hidden": "true" }), l.label.toUpperCase());
}

// --- polling -----------------------------------------------------------------------------------

// poll runs fn now and then every ms while the view is open (and the tab is
// visible). It returns a stop function; stop.now() runs fn at once, e.g.
// right after an action changed what fn shows.
export function poll(fn, ms) {
  let stopped = false;
  let timer;
  const tick = async () => {
    if (stopped) return;
    if (!document.hidden) {
      try {
        await fn();
      } catch (err) {
        if (err.status !== 401) console.warn(err);
      }
    }
    if (!stopped) timer = setTimeout(tick, ms);
  };
  tick();
  const stop = () => {
    stopped = true;
    clearTimeout(timer);
  };
  stop.now = () => fn().catch(() => {});
  return stop;
}

export function terminalTask(status) {
  return status === "COMPLETED" || status === "FAILED" || status === "CANCELLED";
}

export function terminalWorkflow(status) {
  return status === "COMPLETED" || status === "FAILED" || status === "CANCELLED";
}
