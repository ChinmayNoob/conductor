// The dashboard's pages. Each view returns { el, stop } and keeps itself
// current by polling the API while it is open.

import {
  api, get, stream, h, s, toast, poll, session,
  shortID, ago, took, when, span, statusEl, lampOf, terminalTask, terminalWorkflow,
} from "./core.js";
import * as mimic from "./mimic.js";

// --- building blocks ------------------------------------------------------------

function page(plate, title, actions, ...body) {
  return h("div", {},
    h("div", { class: "head" },
      h("div", {}, h("div", { class: "plate" }, plate), h("h1", {}, title)),
      actions && h("div", { class: "actions" }, actions)),
    body);
}

function panel(plate, ...body) {
  return h("section", { class: "panel" }, plate && h("span", { class: "plate" }, plate), body);
}

function table(columns, rows, { empty = "Nothing here yet.", onRow } = {}) {
  const head = h("tr", {}, columns.map((c) => h("th", { class: c.num ? "num" : null, scope: "col" }, c.label)));
  const body = rows.length
    ? rows.map((r) => h("tr", {
        class: onRow ? "link" : null,
        tabindex: onRow ? "0" : null,
        onclick: onRow ? () => onRow(r) : null,
        onkeydown: onRow ? (e) => { if (e.key === "Enter") onRow(r); } : null,
      }, columns.map((c) => h("td", { class: c.num ? "num" : c.cls || null }, c.cell(r)))))
    : [h("tr", {}, h("td", { colspan: columns.length, class: "empty" }, empty))];
  return h("div", { class: "table-wrap" }, h("table", {}, h("thead", {}, head), h("tbody", {}, body)));
}

// replace swaps a placeholder's content for fresh content.
function replace(slot, ...content) {
  slot.replaceChildren(...content.flat().filter(Boolean));
}

const go = (path) => () => { location.hash = "#" + path; };

function button(label, onclick, cls = "") {
  const b = h("button", { class: "btn " + cls, type: "button" }, label);
  b.addEventListener("click", async () => {
    b.disabled = true;
    try {
      await onclick();
    } catch (err) {
      toast(err.message);
    } finally {
      b.disabled = false;
    }
  });
  return b;
}

const errorNote = (err) => h("div", { class: "empty" }, err.status === 403 ? "Needs an admin API key." : err.message);

function commandOf(t) {
  if (t.type === "http" && t.spec) return `${t.spec.method || "GET"} ${t.spec.url}`;
  if (t.type === "container" && t.spec) return `${t.spec.image} ${(t.spec.command || []).join(" ")}`;
  return t.command;
}

function finishedAt(t) {
  return t.completed_at || t.failed_at || t.cancelled_at;
}

// --- overview --------------------------------------------------------------------

const STRIP = [
  ["COMPLETED", "green", "Completed"],
  ["FAILED", "red", "Failed"],
  ["STARTED", "amber", "Running"],
  ["QUEUED", "", "Waiting"],
  ["CANCELLED", "", "Cancelled"],
];

function stripChart(points) {
  const minutes = 60;
  const now = new Date();
  now.setSeconds(0, 0);
  const start = now.getTime() - (minutes - 1) * 60000;
  const buckets = Array.from({ length: minutes }, () => ({}));
  for (const p of points) {
    const i = Math.round((new Date(p.minute).getTime() - start) / 60000);
    if (i >= 0 && i < minutes) buckets[i][p.status] = (buckets[i][p.status] || 0) + p.count;
  }
  const max = Math.max(1, ...buckets.map((b) => Object.values(b).reduce((a, n) => a + n, 0)));
  const W = 600, H = 116, bw = W / minutes;
  const bars = [];
  buckets.forEach((b, i) => {
    let y = H;
    for (const [st, color] of STRIP) {
      const n = b[st] || 0;
      if (!n) continue;
      const bh = Math.max(1, (n / max) * (H - 6));
      y -= bh;
      bars.push(s("rect", { class: `bulb ${color}`, x: i * bw + 0.5, y, width: Math.max(1, bw - 1.5), height: bh, rx: 1 }));
    }
  });
  const svg = s("svg", { viewBox: `0 0 ${W} ${H + 16}`, preserveAspectRatio: "none", role: "img",
      "aria-label": "Tasks submitted per minute over the last hour, by outcome" },
    s("line", { x1: 0, x2: W, y1: H + 0.5, y2: H + 0.5, stroke: "currentColor", "stroke-opacity": 0.2 }),
    bars);
  const legend = h("div", { class: "legend" },
    STRIP.map(([, color, label]) => h("span", {}, h("span", { class: `lamp ${color}` }), label)),
    h("span", { class: "faint" }, `peak ${max}/min · bars are minutes of submission, coloured by how those tasks ended up`));
  return h("div", { class: "strip" }, svg, legend);
}

function tile(label, value, lamp, sub) {
  return h("div", { class: "tile" },
    h("div", { class: "row" }, h("span", { class: `lamp ${lamp || ""}` }), h("span", { class: "plate" }, label)),
    h("div", { class: "value" }, value, sub && h("small", {}, " " + sub)));
}

export function overview() {
  const board = h("div", { class: "board" });
  const strip = h("div", {});
  const queues = h("div", {});
  const failures = h("div", {});
  const flows = h("div", {});
  const el = page("Overview", "What is running now", null,
    board,
    panel("Last hour", strip),
    h("div", { class: "grid cols-overview" },
      panel("Queues", queues),
      h("div", { class: "stack narrow" }, panel("Recent failures", failures), panel("Active workflows", flows))));

  const stopFast = poll(async () => {
    const [timeline, qs] = await Promise.all([get("/stats/timeline"), get("/queues")]);
    const ready = qs.reduce((a, q) => a + q.queued, 0);
    const running = qs.reduce((a, q) => a + q.running, 0);
    const hour = (st) => timeline.filter((p) => p.status === st).reduce((a, p) => a + p.count, 0);
    const tiles = [
      tile("Waiting", ready, ready ? "amber" : ""),
      tile("Running", running, running ? "amber live" : ""),
      tile("Completed", hour("COMPLETED"), "green", "last hour"),
      tile("Failed", hour("FAILED"), hour("FAILED") ? "red" : "", "last hour"),
    ];
    if (session.admin) {
      try {
        const [workers, cluster] = await Promise.all([get("/workers"), get("/cluster")]);
        const healthy = workers.filter((w) => w.status === "healthy");
        const slots = healthy.reduce((a, w) => a + w.slots, 0);
        tiles.push(tile("Workers", healthy.length, healthy.length ? "green" : "red", `${slots} slots`));
        tiles.push(tile("Leader epoch", cluster.leader ? cluster.leader.epoch : "none",
          cluster.leader ? "blue" : "red", `· ${cluster.coordinators.length} coordinators`));
      } catch { /* not fatal for the overview */ }
    }
    replace(board, tiles);
    replace(strip, stripChart(timeline));
    replace(queues, table([
      { label: "Queue", cell: (q) => h("span", { class: "mono" }, q.name) },
      { label: "State", cell: (q) => q.paused ? h("span", { class: "status" }, h("span", { class: "lamp red" }), "PAUSED") : h("span", { class: "status" }, h("span", { class: "lamp green" }), "OPEN") },
      { label: "Waiting", num: true, cell: (q) => q.queued },
      { label: "Running", num: true, cell: (q) => q.running + (q.concurrency_limit ? ` / ${q.concurrency_limit}` : "") },
      { label: "", cell: (q) => button(q.paused ? "Resume" : "Pause", async () => {
          await api("PUT", "/queues/" + encodeURIComponent(q.name), {
            concurrency_limit: q.concurrency_limit, rate_limit: q.rate_limit,
            rate_period_seconds: q.rate_period_seconds, paused: !q.paused,
          });
          toast(`Queue ${q.name} ${q.paused ? "resumed" : "paused"}`);
          stopFast.now();
        }, "small") },
    ], qs, { empty: "No queues yet. Tasks go to the default queue." }));
  }, 4000);

  const stopSlow = poll(async () => {
    const [failed, running, undoing] = await Promise.all([
      get("/tasks?status=FAILED&limit=6"), get("/workflows?status=RUNNING&limit=6"), get("/workflows?status=COMPENSATING&limit=6"),
    ]);
    replace(failures, table([
      { label: "Task", cell: (t) => h("div", { class: "two-line" }, h("span", { class: "mono" }, shortID(t.id)), h("span", { class: "cmd" }, commandOf(t))) },
      { label: "Why it failed", cls: "cmd", cell: (t) => h("span", { class: "mono" }, t.error_message || "") },
      { label: "When", cell: (t) => h("span", { class: "faint" }, ago(t.failed_at)) },
    ], failed, { empty: "No failures. Good.", onRow: (t) => go("/tasks/" + t.id)() }));
    replace(flows, table([
      { label: "Run", cell: (w) => h("span", { class: "mono" }, shortID(w.id)) },
      { label: "Workflow", cell: (w) => w.workflow },
      { label: "Status", cell: (w) => statusEl(w.status) },
      { label: "Started", cell: (w) => h("span", { class: "faint" }, ago(w.created_at)) },
    ], [...undoing, ...running], { empty: "No workflow is running.", onRow: (w) => go("/workflows/" + w.id)() }));
  }, 8000);

  return { el, stop: () => { stopFast(); stopSlow(); } };
}

// --- tasks -----------------------------------------------------------------------

const TASK_STATUSES = ["", "QUEUED", "STARTED", "COMPLETED", "FAILED", "CANCELLED"];

export function tasks(_, query) {
  const status = h("select", { "aria-label": "Status" }, TASK_STATUSES.map((st) =>
    h("option", { value: st, selected: st === (query.get("status") || "") }, st ? lampOf(st).label : "Any status")));
  const queue = h("input", { placeholder: "Queue", value: query.get("queue") || "", "aria-label": "Queue", style: "min-width:120px" });
  const search = h("input", { placeholder: "Task ID or part of the command", value: query.get("q") || "", "aria-label": "Search", type: "search" });
  const list = h("div", {});
  const more = h("div", { style: "margin-top:12px" });
  let rows = [];
  let paged = false;

  const params = (extra = {}) => {
    const p = new URLSearchParams({ limit: "50" });
    if (status.value) p.set("status", status.value);
    if (queue.value.trim()) p.set("queue", queue.value.trim());
    if (search.value.trim()) p.set("q", search.value.trim());
    for (const [k, v] of Object.entries(extra)) p.set(k, v);
    return p;
  };
  const draw = () => {
    replace(list, table([
      { label: "Task", cell: (t) => h("span", { class: "mono" }, shortID(t.id)) },
      { label: "Status", cell: (t) => statusEl(t.status) },
      { label: "Command", cls: "cmd", cell: (t) => commandOf(t) },
      { label: "Queue", cell: (t) => h("span", { class: "mono soft" }, t.queue) },
      { label: "Attempt", num: true, cell: (t) => t.attempt || "–" },
      { label: "Took", num: true, cell: (t) => took(t.started_at, finishedAt(t)) },
      { label: "Submitted", cell: (t) => h("span", { class: "faint" }, ago(t.created_at)) },
    ], rows, { empty: "No tasks match.", onRow: (t) => go("/tasks/" + t.id)() }));
    replace(more, rows.length >= 50 ? button("Load older", async () => {
      const older = await get("/tasks?" + params({ before: rows[rows.length - 1].created_at }));
      rows = rows.concat(older);
      paged = true;
      draw();
    }) : null);
  };
  const load = async () => {
    if (paged) return; // don't jump the list while someone pages back
    rows = await get("/tasks?" + params());
    draw();
  };
  const apply = () => {
    const p = params();
    p.delete("limit");
    history.replaceState(null, "", "#/tasks" + (p.toString() ? "?" + p : ""));
    paged = false;
    load();
  };
  status.addEventListener("change", apply);
  queue.addEventListener("change", apply);
  search.addEventListener("keydown", (e) => {
    if (e.key !== "Enter") return;
    const v = search.value.trim();
    if (/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(v)) go("/tasks/" + v)();
    else apply();
  });

  const el = page("Tasks", "Tasks", null,
    h("div", { class: "filters" }, search, status, queue),
    panel(null, list, more));
  return { el, stop: poll(load, 5000) };
}

// --- task detail --------------------------------------------------------------------

export function task([id]) {
  const head = h("div", { class: "faint" }, "Loading…");
  const body = h("div", { class: "stack" });
  const consoleEl = h("pre", { class: "console", "aria-live": "polite" });
  const attemptsEl = h("div", {});
  let following = null; // AbortController for the live log
  let current = null;

  const follow = () => {
    following = new AbortController();
    consoleEl.textContent = "";
    stream(`/tasks/${id}/logs?follow=true`, (chunk) => {
      const atBottom = consoleEl.scrollTop + consoleEl.clientHeight >= consoleEl.scrollHeight - 8;
      for (const part of chunk.split(/(\n--- attempt \d+ ---\n)/)) {
        if (/^\n--- attempt \d+ ---\n$/.test(part)) consoleEl.append(h("span", { class: "sep" }, part));
        else if (part) consoleEl.append(part);
      }
      if (atBottom) consoleEl.scrollTop = consoleEl.scrollHeight;
    }, following.signal).catch(() => {}).finally(() => { following = null; });
  };

  const draw = (t) => {
    const actions = [];
    if (!terminalTask(t.status)) {
      actions.push(button("Cancel task", async () => {
        await api("POST", `/tasks/${t.id}/cancel`);
        toast("Task cancelled");
        refresh();
      }, "danger"));
    }
    if (t.status === "FAILED" && !t.workflow_id) {
      actions.push(button("Requeue", async () => {
        await api("POST", `/tasks/${t.id}/requeue`);
        toast("Task requeued with fresh retries");
        refresh();
      }, "primary"));
    }
    replace(head, h("div", { class: "head" },
      h("div", {}, h("div", { class: "plate" }, "Task"),
        h("h1", {}, statusEl(t.status), " ", h("span", { class: "mono" }, t.id))),
      h("div", { class: "actions" }, actions)));

    const facts = [
      ["Command", h("code", { class: "mono" }, commandOf(t))],
      ["Type", t.type], ["Queue", t.queue],
      ["Attempt", `${t.attempt || 0}` + (t.max_retries ? ` · retries ${t.retry_count}/${t.max_retries}` : "")],
      ["Worker", t.worker_id ?? "–"],
      ["Submitted", when(t.created_at)],
      ["Started", when(t.started_at)],
      ["Finished", when(finishedAt(t))],
      ["Took", took(t.started_at, finishedAt(t))],
      ["Priority", t.priority], ["Timeout", span(t.timeout_seconds)],
      t.workflow_id && ["Workflow", h("a", { href: "#/workflows/" + t.workflow_id, class: "mono" }, shortID(t.workflow_id))],
      t.trace_id && ["Trace", h("span", { class: "mono" }, t.trace_id)],
      t.idempotency_key && ["Idempotency key", h("span", { class: "mono" }, t.idempotency_key)],
    ].filter(Boolean);
    const outputs = Object.entries(t.outputs || {});
    replace(body,
      t.error_message && h("div", { class: "banner red" }, h("span", { class: "lamp red" }),
        h("div", {}, h("span", { class: "title" }, t.status === "FAILED" ? "Failed" : "Last attempt failed"), h("span", { class: "mono" }, t.error_message))),
      panel("Details", h("dl", { class: "facts" }, facts.map(([k, v]) => h("div", { class: k === "Command" ? "wide" : null }, h("dt", {}, k), h("dd", {}, v))))),
      outputs.length > 0 && panel("Outputs", h("div", { class: "outputs" }, outputs.map(([k, v]) => h("code", {}, `${k}=${v}`)))),
      panel("Output", consoleEl),
      panel("Attempts", attemptsEl));
  };

  const loadAttempts = async () => {
    const list = await get(`/tasks/${id}/attempts`);
    replace(attemptsEl, table([
      { label: "#", num: true, cell: (a) => a.attempt },
      { label: "Status", cell: (a) => statusEl(a.status) },
      { label: "Worker", cell: (a) => h("span", { class: "mono soft" }, a.worker_id ?? "–") },
      { label: "Started", cell: (a) => h("span", { class: "faint" }, when(a.started_at)) },
      { label: "Took", num: true, cell: (a) => took(a.started_at, a.finished_at) },
      { label: "Reason", cls: "cmd", cell: (a) => h("span", { class: "mono" }, a.error || "") },
    ], list.slice().reverse(), { empty: "Not dispatched yet." }));
  };

  const refresh = async () => {
    const t = await get("/tasks/" + id);
    const changed = !current || current.status !== t.status || current.attempt !== t.attempt;
    current = t;
    if (changed) {
      draw(t);
      await loadAttempts();
      if (terminalTask(t.status) && !following) consoleEl.textContent = t.output || "";
      else if (!following) follow();
    }
  };

  const el = h("div", {}, head, body);
  const stop = poll(refresh, 2000);
  return { el, stop: () => { stop(); following?.abort(); } };
}

// --- workflows -----------------------------------------------------------------------

export function workflows(_, query) {
  const status = h("select", { "aria-label": "Status" }, ["", "RUNNING", "COMPENSATING", "COMPLETED", "FAILED", "CANCELLED"].map((st) =>
    h("option", { value: st, selected: st === (query.get("status") || "") }, st ? lampOf(st).label : "Any status")));
  const list = h("div", {});
  const load = async () => {
    const p = new URLSearchParams({ limit: "100" });
    if (status.value) p.set("status", status.value);
    const rows = await get("/workflows?" + p);
    replace(list, table([
      { label: "Run", cell: (w) => h("span", { class: "mono" }, shortID(w.id)) },
      { label: "Workflow", cell: (w) => w.workflow + (w.version ? ` v${w.version}` : "") },
      { label: "Status", cell: (w) => statusEl(w.status) },
      { label: "Error", cls: "cmd", cell: (w) => h("span", { class: "soft" }, w.error_message || "") },
      { label: "Took", num: true, cell: (w) => terminalWorkflow(w.status) ? took(w.created_at, w.updated_at) : "–" },
      { label: "Started", cell: (w) => h("span", { class: "faint" }, ago(w.created_at)) },
    ], rows, { empty: "No workflow runs yet.", onRow: (w) => go("/workflows/" + w.id)() }));
  };
  status.addEventListener("change", () => {
    history.replaceState(null, "", "#/workflows" + (status.value ? "?status=" + status.value : ""));
    load();
  });
  const el = page("Workflows", "Workflow runs", null, h("div", { class: "filters" }, status), panel(null, list));
  return { el, stop: poll(load, 5000) };
}

// --- workflow run: the mimic diagram ----------------------------------------------------

export function workflow([id]) {
  const head = h("div", { class: "faint" }, "Loading…");
  const banner = h("div", {});
  const board = h("div", { class: "mimic" });
  const stepPanel = h("div", {});
  const stepsTable = h("div", {});
  let selected = null;
  let run = null;
  let stepTask = null; // the selected step's task, for its output

  const pickDefault = (steps) =>
    steps.find((st) => st.status === "FAILED" || st.status === "COMPENSATION_FAILED")?.name ||
    steps.find((st) => st.status === "RUNNING" || st.status === "COMPENSATING")?.name ||
    steps[0]?.name;

  const drawStep = () => {
    const st = run.steps.find((x) => x.name === selected);
    if (!st) return replace(stepPanel);
    const facts = [
      ["Status", statusEl(st.status)],
      st.depends_on?.length && ["After", h("span", { class: "mono" }, st.depends_on.join(", "))],
      st.task_id && ["Task", h("a", { class: "mono", href: "#/tasks/" + st.task_id }, shortID(st.task_id))],
      st.compensation_task_id && ["Undo task", h("span", {},
        h("a", { class: "mono", href: "#/tasks/" + st.compensation_task_id }, shortID(st.compensation_task_id)), " ",
        statusEl(st.compensation_task_status))],
    ].filter(Boolean);
    const outputs = Object.entries(st.outputs || {});
    replace(stepPanel, panel("Step",
      h("div", { class: "step-panel" },
        h("h3", {}, st.name),
        st.error && h("div", { class: "banner red" }, h("span", { class: "lamp red" }),
          h("div", {}, h("span", { class: "title" }, "Why it failed"), h("span", { class: "mono" }, st.error))),
        h("dl", { class: "facts" }, facts.map(([k, v]) => h("div", {}, h("dt", {}, k), h("dd", {}, v)))),
        outputs.length > 0 && h("div", { style: "margin-top:14px" }, h("span", { class: "plate" }, "Outputs"),
          h("div", { class: "outputs", style: "margin-top:8px" }, outputs.map(([k, v]) => h("code", {}, `${k}=${v}`)))),
        stepTask && stepTask.id === st.task_id && h("div", { style: "margin-top:14px" },
          h("span", { class: "plate" }, "Output"),
          h("pre", { class: "console", style: "margin-top:8px;max-height:260px" }, (stepTask.output || "").split("\n").slice(-60).join("\n"))))));
  };

  const select = async (name) => {
    selected = name;
    stepTask = null;
    drawBoard();
    drawStep();
    const st = run.steps.find((x) => x.name === name);
    if (st?.task_id) {
      try {
        stepTask = await get("/tasks/" + st.task_id);
        if (selected === name) drawStep();
      } catch { /* the step panel works without it */ }
    }
  };

  const drawBoard = () => {
    replace(board, mimic.render(run.steps, selected, select), mimic.key());
  };

  const diagnose = (w) => {
    const failed = w.steps.filter((st) => st.status === "FAILED");
    const undone = w.steps.filter((st) => st.status === "COMPENSATED").map((st) => st.name);
    const undoing = w.steps.filter((st) => st.status === "COMPENSATING").map((st) => st.name);
    const stuck = w.steps.filter((st) => st.status === "COMPENSATION_FAILED").map((st) => st.name);
    if (w.status === "COMPLETED" || w.status === "RUNNING") return null;
    const lines = [];
    for (const st of failed) lines.push(h("div", {}, "Step ", h("strong", { class: "mono" }, st.name), " failed: ", h("span", { class: "mono" }, st.error || w.error_message || "no error recorded")));
    if (!failed.length && w.error_message) lines.push(h("div", { class: "mono" }, w.error_message));
    if (undone.length) lines.push(h("div", {}, "Undone (compensated): ", h("span", { class: "mono" }, undone.join(", "))));
    if (undoing.length) lines.push(h("div", {}, "Undoing now: ", h("span", { class: "mono" }, undoing.join(", "))));
    if (stuck.length) lines.push(h("div", {}, "Undo failed, needs a person: ", h("strong", { class: "mono" }, stuck.join(", "))));
    if (!undone.length && !undoing.length && !stuck.length && w.status !== "CANCELLED") lines.push(h("div", { class: "soft" }, "Nothing had to be undone."));
    const color = stuck.length || failed.length ? "red" : "violet";
    const title = { FAILED: "This run failed", COMPENSATING: "This run is being undone", CANCELLED: "This run was cancelled" }[w.status] || w.status;
    return h("div", { class: "banner " + color }, h("span", { class: `lamp ${color}${w.status === "COMPENSATING" ? " live" : ""}` }),
      h("div", {}, h("span", { class: "title" }, title), lines));
  };

  const refresh = async () => {
    const w = await get("/workflows/" + id);
    run = w;
    if (!selected || !w.steps.some((st) => st.name === selected)) selected = pickDefault(w.steps);
    replace(head, h("div", { class: "head" },
      h("div", {}, h("div", { class: "plate" }, "Workflow run · " + w.workflow + (w.version ? ` v${w.version}` : "")),
        h("h1", {}, statusEl(w.status), " ", h("span", { class: "mono" }, w.id))),
      h("div", { class: "actions" },
        !terminalWorkflow(w.status) && button("Cancel run", async () => {
          await api("POST", `/workflows/${w.id}/cancel`);
          toast("Cancelling: completed steps will be undone");
        }, "danger"))));
    replace(banner, diagnose(w));
    drawBoard();
    if (!stepTask || terminalWorkflow(w.status)) await select(selected);
    else drawStep();
    replace(stepsTable, table([
      { label: "Step", cell: (st) => h("span", { class: "mono" }, st.name) },
      { label: "Status", cell: (st) => statusEl(st.status) },
      { label: "Task", cell: (st) => st.task_id ? h("a", { class: "mono", href: "#/tasks/" + st.task_id }, shortID(st.task_id)) : "–" },
      { label: "Undo", cell: (st) => st.compensation_task_id ? statusEl(st.compensation_task_status) : "–" },
      { label: "Error", cls: "cmd", cell: (st) => h("span", { class: "mono soft" }, st.error || "") },
    ], w.steps));
  };

  const facts = h("div", {});
  const el = h("div", {}, head, banner, board, h("div", { class: "grid cols-2", style: "margin-top:16px" }, stepPanel, panel("All steps", stepsTable)), facts);
  let stop = poll(async () => {
    await refresh();
    if (terminalWorkflow(run.status)) { // finished: stop the fast refresh
      stop();
      stop = () => {};
    }
  }, 2000);
  return { el, stop: () => stop() };
}

// --- workers, schedules, dead letter ------------------------------------------------------

export function workers() {
  const list = h("div", {});
  const load = async () => {
    let rows;
    try {
      rows = await get("/workers");
    } catch (err) {
      return replace(list, errorNote(err));
    }
    replace(list, table([
      { label: "Worker", cell: (w) => h("span", { class: "mono" }, w.id) },
      { label: "Status", cell: (w) => statusEl(w.status) },
      { label: "Address", cell: (w) => h("span", { class: "mono soft" }, w.address) },
      { label: "Busy", num: true, cell: (w) => `${w.running} / ${w.slots}` },
      { label: "Labels", cls: "cmd", cell: (w) => h("span", { class: "mono soft" }, Object.entries(w.labels || {}).map(([k, v]) => `${k}=${v}`).join(" ")) },
      { label: "Last seen", cell: (w) => h("span", { class: "faint" }, ago(w.last_seen)) },
    ], rows, { empty: "No worker has checked in during the last five minutes." }));
  };
  return { el: page("Workers", "Workers", null, panel(null, list)), stop: poll(load, 5000) };
}

function targetOf(sch) {
  const t = sch.target || {};
  if (t.workflow) return "workflow " + t.workflow.name;
  if (t.task) return t.task.command || t.task.type || "task";
  return "–";
}

export function schedules() {
  const list = h("div", {});
  const act = (sch, action, done) => button(action[0].toUpperCase() + action.slice(1), async () => {
    await api("POST", `/schedules/${encodeURIComponent(sch.name)}/${action}`);
    toast(done);
    load();
  }, "small");
  const load = async () => {
    const rows = await get("/schedules");
    replace(list, table([
      { label: "Schedule", cell: (x) => h("span", { class: "mono" }, x.name) },
      { label: "State", cell: (x) => x.enabled ? h("span", { class: "status" }, h("span", { class: "lamp green" }), "ON") : h("span", { class: "status" }, h("span", { class: "lamp" }), "PAUSED") },
      { label: "When", cell: (x) => h("span", { class: "mono" }, x.cron + (x.timezone && x.timezone !== "UTC" ? ` (${x.timezone})` : "")) },
      { label: "Runs", cls: "cmd", cell: (x) => targetOf(x) },
      { label: "Next", cell: (x) => h("span", { class: "faint" }, x.enabled ? ago(x.next_run_at) : "–") },
      { label: "Last", cell: (x) => x.last_error ? h("span", { class: "status", title: x.last_error }, h("span", { class: "lamp red" }), "ERROR") : h("span", { class: "faint" }, ago(x.last_run_at)) },
      { label: "", cell: (x) => h("div", { class: "actions" },
          x.enabled ? act(x, "pause", "Schedule paused") : act(x, "resume", "Schedule resumed"),
          x.enabled && act(x, "trigger", "Run started")) },
    ], rows, { empty: "No schedules. Create one with conductorctl schedule create." }));
  };
  return { el: page("Schedules", "Schedules", null, panel(null, list)), stop: poll(load, 5000) };
}

export function deadLetter() {
  const list = h("div", {});
  const load = async () => {
    const rows = await get("/dead-letter?limit=200");
    replace(list, table([
      { label: "Task", cell: (t) => h("a", { class: "mono", href: "#/tasks/" + t.id }, shortID(t.id)) },
      { label: "Command", cls: "cmd", cell: (t) => commandOf(t) },
      { label: "Why it failed", cls: "cmd", cell: (t) => h("span", { class: "mono soft" }, t.error_message || "") },
      { label: "Attempts", num: true, cell: (t) => t.attempt },
      { label: "Failed", cell: (t) => h("span", { class: "faint" }, ago(t.failed_at)) },
      { label: "", cell: (t) => button("Requeue", async () => {
          await api("POST", `/tasks/${t.id}/requeue`);
          toast("Task requeued with fresh retries");
          load();
        }, "small") },
    ], rows, { empty: "Nothing failed for good. Tasks land here after their last retry." }));
  };
  return {
    el: page("Dead letter", "Failed for good", null,
      h("p", { class: "soft", style: "margin:-8px 0 16px" }, "Tasks that used up their retries. Requeue one once the cause is fixed; workflow steps are handled by their run's compensation instead."),
      panel(null, list)),
    stop: poll(load, 6000),
  };
}
