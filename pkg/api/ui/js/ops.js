// Approvals and the AI assistant. Like the other views, each returns
// { el, stop } and uses only the /v1 API.

import { api, get, h, toast, poll, shortID, ago } from "./core.js";
import { page, panel, table, replace, button } from "./views.js";

// --- approvals waiting for a decision ------------------------------------------------

export function approvals() {
  const list = h("div", {});
  const load = async () => {
    const rows = await get("/approvals");
    replace(list, table([
      { label: "Run", cell: (a) => h("a", { class: "mono", href: "#/workflows/" + a.workflow_id }, shortID(a.workflow_id)) },
      { label: "Workflow", cell: (a) => a.workflow },
      { label: "Step", cell: (a) => h("span", { class: "mono" }, a.step) },
      { label: "Question", cls: "cmd", cell: (a) => a.message },
      { label: "Waiting", cell: (a) => h("span", { class: "faint" }, ago(a.since)) },
      { label: "Decides itself", cell: (a) => h("span", { class: "faint" }, a.deadline ? `${ago(a.deadline)} (${a.on_timeout || "reject"})` : "never") },
      { label: "", cell: (a) => h("span", { class: "actions-row" },
        button("Approve", async () => {
          await api("POST", `/workflows/${a.workflow_id}/steps/${encodeURIComponent(a.step)}/approve`, {});
          toast("Approved");
          load();
        }, "primary small"),
        button("Reject", async () => {
          await api("POST", `/workflows/${a.workflow_id}/steps/${encodeURIComponent(a.step)}/reject`, {});
          toast("Rejected");
          load();
        }, "danger small")) },
    ], rows, { empty: "Nothing is waiting for a decision." }));
  };
  const el = page("Approvals", "Waiting for a person", null, panel(null, list));
  return { el, stop: poll(load, 4000) };
}

// --- the assistant: ask the cluster, draft workflows ---------------------------------------

function usageNote(u) {
  if (!u || !(u.input_tokens + u.output_tokens)) return null;
  return h("div", { class: "faint", style: "margin-top:8px" },
    `${u.input_tokens + u.output_tokens} tokens` + (u.cost_usd ? ` · $${u.cost_usd.toFixed(4)}` : ""));
}

const optInHint = (err) => err.status === 403
  ? "AI assist is off for this namespace. An admin can turn it on: conductorctl namespace set NAME -ai-assist"
  : err.message;

export function assistant() {
  // Ask your cluster.
  const question = h("input", { type: "text", placeholder: "Which tasks failed in the last hour, and why?", "aria-label": "Question" });
  const answer = h("div", {});
  const ask = button("Ask", async () => {
    if (!question.value.trim()) return;
    replace(answer, h("div", { class: "faint" }, "Looking…"));
    try {
      const a = await api("POST", "/ai/ask", { question: question.value });
      replace(answer,
        h("div", { class: "answer" }, a.answer),
        a.lookups?.length > 0 && h("div", { class: "faint", style: "margin-top:8px" }, "Looked at: " + a.lookups.join(" · ")),
        usageNote(a.usage));
    } catch (err) {
      replace(answer, h("div", { class: "banner red" }, h("span", { class: "lamp red" }), h("div", {}, optInHint(err))));
    }
  }, "primary");
  question.addEventListener("keydown", (e) => { if (e.key === "Enter") ask.click(); });

  // Draft a workflow; saving is a separate, deliberate click.
  const description = h("textarea", { rows: "4", placeholder: "Every order: charge the card and reserve stock in parallel, then ask a person to approve shipping, then ship. Undo the charge if shipping fails.", "aria-label": "Describe the workflow" });
  const draft = h("div", {});
  const make = button("Draft workflow", async () => {
    if (!description.value.trim()) return;
    replace(draft, h("div", { class: "faint" }, "Drafting…"));
    try {
      const d = await api("POST", "/ai/workflow", { description: description.value });
      replace(draft,
        !d.valid && h("div", { class: "banner red" }, h("span", { class: "lamp red" }),
          h("div", {}, h("span", { class: "title" }, "The draft is not valid"), h("span", { class: "mono" }, d.errors))),
        h("pre", { class: "console yaml" }, d.yaml),
        d.valid && h("div", { class: "dry-run" }, h("span", { class: "plate" }, "Dry run: steps in a row start together"),
          h("ol", {}, (d.plan || []).map((wave) => h("li", {}, wave.map((st) => `${st.name} (${st.type})`).join(", "))))),
        (d.warnings || []).map((w) => h("div", { class: "banner violet" }, h("span", { class: "lamp violet" }), h("div", {}, "Look at: " + w))),
        d.valid && h("div", { class: "actions-row" },
          button(`Save ${d.name}`, async () => {
            // The draft is saved exactly as shown.
            const saved = await api("PUT", "/workflow-definitions", d.yaml);
            toast(`Saved ${saved.name} as version ${saved.version}`);
          }, "primary"),
          h("span", { class: "faint" }, "Nothing is saved until you click.")),
        usageNote(d.usage));
    } catch (err) {
      replace(draft, h("div", { class: "banner red" }, h("span", { class: "lamp red" }), h("div", {}, optInHint(err))));
    }
  }, "primary");

  const el = page("Assistant", "Ask and draft", null,
    h("div", { class: "stack" },
      panel("Ask your cluster", h("div", { class: "actions-row" }, question, ask), answer,
        h("p", { class: "faint" }, "Read-only. The assistant looks things up in this namespace and cannot change anything.")),
      panel("Draft a workflow", description, h("div", { class: "actions-row" }, make), draft)));
  return { el, stop() {} };
}
