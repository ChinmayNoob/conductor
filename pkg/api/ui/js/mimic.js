// The mimic diagram: a workflow run drawn the way a signal box draws its
// railway. Steps are track sections with a signal lamp; dependencies are
// track. A route lights green as steps complete and red where it stopped;
// compensation runs back along the route as a moving violet line.

import { s, lampOf } from "./core.js";

const W = 176; // section width
const H = 56;
const COL = 92; // gap between columns
const ROW = 22; // gap between rows
const PAD = 14;

// layout places each step in a column by its dependency depth, keeping
// definition order within a column.
export function layout(steps) {
  const byName = new Map(steps.map((st) => [st.name, st]));
  const depth = new Map();
  const visit = (name, seen = new Set()) => {
    if (depth.has(name)) return depth.get(name);
    if (seen.has(name)) return 0; // a cycle can't happen in a valid definition
    seen.add(name);
    const deps = (byName.get(name)?.depends_on || []).filter((d) => byName.has(d));
    const d = deps.length ? 1 + Math.max(...deps.map((x) => visit(x, seen))) : 0;
    depth.set(name, d);
    return d;
  };
  steps.forEach((st) => visit(st.name));

  const cols = [];
  for (const st of steps) {
    const d = depth.get(st.name);
    (cols[d] ||= []).push(st);
  }
  const tallest = Math.max(1, ...cols.map((c) => c.length));
  const height = tallest * H + (tallest - 1) * ROW;
  const pos = new Map();
  cols.forEach((col, i) => {
    const colHeight = col.length * H + (col.length - 1) * ROW;
    col.forEach((st, j) => {
      pos.set(st.name, {
        x: PAD + i * (W + COL),
        y: PAD + (height - colHeight) / 2 + j * (H + ROW),
      });
    });
  });
  return { pos, width: PAD * 2 + cols.length * W + (cols.length - 1) * COL, height: PAD * 2 + height };
}

// trackPath joins two sections: straight out, a diagonal, straight in.
function trackPath(a, b) {
  const x1 = a.x + W;
  const y1 = a.y + H / 2;
  const x2 = b.x;
  const y2 = b.y + H / 2;
  if (y1 === y2) return `M${x1} ${y1} H${x2}`;
  const room = x2 - x1 - 36;
  const run = Math.min(Math.abs(y2 - y1), room);
  const xb = x2 - 18;
  return `M${x1} ${y1} H${xb - run} L${xb} ${y2} H${x2}`;
}

const TRAVELLED = new Set(["COMPLETED", "COMPENSATING", "COMPENSATED", "COMPENSATION_FAILED"]);
const UNDOING = new Set(["COMPENSATING", "COMPENSATED", "COMPENSATION_FAILED"]);

// render draws the diagram. selected names the highlighted step; onSelect is
// called with a step name when a section is clicked or activated.
export function render(steps, selected, onSelect) {
  const { pos, width, height } = layout(steps);
  const byName = new Map(steps.map((st) => [st.name, st]));
  const tracks = [];
  const reverses = [];

  for (const st of steps) {
    for (const dep of st.depends_on || []) {
      const from = byName.get(dep);
      if (!from) continue;
      const d = trackPath(pos.get(dep), pos.get(st.name));
      let cls = "track";
      if (st.status === "FAILED") cls += " stop";
      else if (TRAVELLED.has(from.status) && st.status !== "PENDING" && st.status !== "SKIPPED") cls += " set";
      tracks.push(s("path", { class: cls, d }));
      // The undo travels back from a step to the one it depends on.
      if (UNDOING.has(from.status) && (UNDOING.has(st.status) || st.status === "FAILED" || st.status === "CANCELLED")) {
        reverses.push(s("path", { class: "reverse", d }));
      }
    }
  }

  const sections = steps.map((st) => {
    const p = pos.get(st.name);
    const lamp = lampOf(st.status);
    const cx = p.x + 20;
    const cy = p.y + H / 2;
    const label = st.name.length > 16 ? st.name.slice(0, 15) + "…" : st.name;
    const select = () => onSelect(st.name);
    return s(
      "g",
      {
        class: "section" + (st.name === selected ? " sel" : ""),
        tabindex: "0",
        role: "button",
        "aria-label": `Step ${st.name}: ${lamp.label}`,
        onclick: select,
        onkeydown: (e) => {
          if (e.key === "Enter" || e.key === " ") {
            e.preventDefault();
            select();
          }
        },
      },
      s("title", {}, `${st.name} · ${lamp.label}`),
      s("rect", { x: p.x, y: p.y, width: W, height: H, rx: 5 }),
      lamp.color && s("circle", { class: `halo ${lamp.color}`, cx, cy, r: 12 }),
      s("circle", { class: `bulb ${lamp.color}${lamp.live ? " live" : ""}`, cx, cy, r: 6.5 }),
      s("text", { x: p.x + 38, y: cy - 4 }, label.toUpperCase()),
      s("text", { class: "sub", x: p.x + 38, y: cy + 13 }, lamp.label.toUpperCase()),
    );
  });

  return s(
    "svg",
    { width, height, viewBox: `0 0 ${width} ${height}`, role: "group", "aria-label": "Workflow steps" },
    tracks,
    reverses,
    sections,
  );
}

export function key() {
  const item = (cls) => s("svg", { width: 12, height: 12, "aria-hidden": "true" }, s("circle", { class: `bulb ${cls}`, cx: 6, cy: 6, r: 5 }));
  const row = (cls, label) => {
    const span = document.createElement("span");
    span.append(item(cls), label);
    return span;
  };
  const el = document.createElement("div");
  el.className = "mimic-key";
  el.append(row("green", "DONE"), row("amber", "RUNNING"), row("red", "FAILED"), row("violet", "UNDONE (COMPENSATED)"), row("", "NOT RUN"));
  return el;
}
