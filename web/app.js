/* Pointwise data branching console — vanilla JS, no build step. */
"use strict";

const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => [...document.querySelectorAll(sel)];
const api = async (path, opts = {}) => {
  const r = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...opts,
    body: opts.body ? JSON.stringify(opts.body) : undefined,
  });
  const data = r.status === 204 ? {} : await r.json();
  if (!r.ok) throw Object.assign(new Error(data.detail || r.statusText),
                                 { status: r.status, data });
  return data;
};
const esc = (s) => String(s).replace(/[&<>]/g,
  (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;" }[c]));
const jstr = (v) => JSON.stringify(v);

let state = null;
let selectedBranch = null;
let currentMerge = null;
let es = null;
let liveSubId = null;
let lastSeq = 0;
const seenIds = new Set();

// ------------------------------------------------------------- navigation
$$(".tab").forEach((t) => t.addEventListener("click", () => {
  $$(".tab").forEach((x) => x.classList.remove("active"));
  $$(".tab-panel").forEach((x) => x.classList.remove("active"));
  t.classList.add("active");
  $(`#tab-${t.dataset.tab}`).classList.add("active");
  refresh();
}));

function toast(msg, kind = "") {
  const el = $("#toast");
  el.textContent = msg;
  el.className = `toast ${kind}`;
  setTimeout(() => el.classList.add("hidden"), 5000);
  el.classList.remove("hidden");
}

// ---------------------------------------------------------------- refresh
async function refresh() {
  state = await api("/api/state");
  renderMain();
  renderBranches();
  renderMergeTab();
  renderSubs();
  renderFaults();
}

function renderMain() {
  $("#main-head").textContent = `head seq = ${state.main.head_seq}`;
  $("#main-table tbody").innerHTML = state.main.rows.map((r) => `
    <tr>
      <td>${esc(r.key)}</td>
      <td>${esc(jstr(r.payload))}</td>
      <td>${r.version}</td>
    </tr>`).join("") || `<tr><td colspan="3" class="hint">（空）</td></tr>`;

  $("#main-events").innerHTML = state.main.head_seq
    ? "" : `<div class="hint">（暂无事件）</div>`;
  // events fetched separately to avoid bloating /state; last 30
  api(`/api/main/events?limit=1000`).then((events) => {
    $("#main-events").innerHTML = events.slice(-30).map(renderEventLine).join("");
    $("#main-events").scrollTop = $("#main-events").scrollHeight;
  });
}

function renderEventLine(e) {
  const detail = e.op === "merge_switch"
    ? `<span class="op op-merge_switch">MERGE SWITCH #${e.merge_id}</span>`
    : `<span class="op op-${e.op}">${e.op}(${esc(e.key ?? "")})</span>`
      + (e.payload !== null && e.payload !== undefined
         ? ` <span>${esc(jstr(e.payload))}</span>` : "");
  return `<div class="event-line"><span class="seq">#${e.seq}</span>${detail}
          <span class="origin">${esc(e.origin)}</span></div>`;
}

// ---------------------------------------------------------------- branches
function renderBranches() {
  $("#branch-list").innerHTML = state.branches.map((b) => `
    <div class="branch-card ${b.id === selectedBranch ? "active" : ""}">
      <div>
        <b>${esc(b.name)}</b>
        <span class="badge badge-status">${b.status}</span>
        <small> base_seq=${b.base_seq}</small>
      </div>
      <button class="btn" data-branch="${b.id}">查看</button>
    </div>`).join("") || `<p class="hint">还没有分支</p>`;
  $$("#branch-list [data-branch]").forEach((btn) =>
    btn.addEventListener("click", () => loadBranch(Number(btn.dataset.branch))));

  if (!selectedBranch) {
    $("#branch-detail").classList.add("hidden");
    return;
  }
  const b = state.branches.find((x) => x.id === selectedBranch);
  if (!b) { selectedBranch = null; return; }
  $("#branch-detail").classList.remove("hidden");
  $("#bd-title").textContent = `分支 ${b.name}`;
  $("#bd-base").textContent = b.base_seq;
  api(`/api/branches/${b.id}`).then((detail) => {
    $("#branch-rows").innerHTML = detail.rows.map((r) => `
      <tr>
        <td>${esc(r.key)}</td>
        <td class="state-cell">${renderState(r.state)}</td>
        <td>${r.last_seq ?? "—"}</td>
      </tr>`).join("");
    $("#branch-ops").innerHTML = detail.ops.map((o) => `
      <div class="event-line">
        <span class="seq">#${o.seq}</span>
        <span class="op op-${o.op}">${o.op}(${esc(o.key)})</span>
        <span>${o.payload ? esc(jstr(JSON.parse(o.payload))) : ""}</span>
      </div>`).join("") || `<div class="hint">（分支上尚无操作）</div>`;
  });
}

function renderState(s) {
  if (s.kind === "absent") return `<span class="state-absent">absent</span>`;
  if (s.kind === "tombstone")
    return `<span class="state-tombstone">tombstone（已删除）</span>`;
  return esc(jstr(s.payload));
}

async function loadBranch(id) {
  selectedBranch = id;
  renderBranches();
  populateMergeSelect();
}

// ------------------------------------------------------------------ merge
function populateMergeSelect() {
  const sel = $("#merge-branch-select");
  const open = state.branches.filter((b) => b.status === "open");
  sel.innerHTML = open.map((b) =>
    `<option value="${b.id}">${esc(b.name)} (#${b.id}, base_seq=${b.base_seq})</option>`
  ).join("");
  if (!open.length)
    sel.innerHTML = `<option>（没有可合并的 open 分支）</option>`;
}

function renderMergeTab() {
  populateMergeSelect();
  const active = state.merges.filter((m) =>
    ["prepared", "resolving", "committing"].includes(m.status));
  if (active.length) {
    currentMerge = active[active.length - 1];
    renderMergeWorkspace(currentMerge);
  } else {
    currentMerge = null;
    $("#merge-workspace").classList.add("hidden");
  }
  $("#merge-history").innerHTML = state.merges.map((m) => `
    <div class="event-line">
      <span class="seq">#${m.id}</span>
      <span class="badge badge-status">${m.status}</span>
      <span>branch=${m.branch_id} · mode=${m.mode}</span>
      <span class="origin">${m.plan.filter((d) => d.status === "conflict").length}
        个冲突 / ${m.plan.length} 个键</span>
    </div>`).join("") || `<div class="hint">（暂无合并）</div>`;
}

function renderMergeWorkspace(m) {
  $("#merge-workspace").classList.remove("hidden");
  $("#mg-id").textContent = m.id;
  $("#mg-status").textContent = m.status;
  $("#mg-mode").textContent = m.mode;
  const conflicts = m.plan.filter((d) => d.status === "conflict");
  $("#mg-conflict-count").textContent = conflicts.length;
  $("#mg-unresolved-count").textContent =
    conflicts.filter((d) => !d.resolution).length;

  const locked = ["committing", "committed", "aborted"].includes(m.status);
  $("#mg-commit").disabled = locked;
  $("#mg-abort").disabled = locked;
  $("#mg-toggle-mode").disabled = locked;

  $("#merge-plan").innerHTML = m.plan.map((d) => {
    const isConflict = d.status === "conflict";
    const badge = isConflict
      ? `<span class="badge badge-conflict">冲突</span>`
      : `<span class="badge badge-clean">干净</span>`;
    const action = d.resolution
      ? `<span class="badge badge-resolved">已裁决 → ${d.action}</span>`
      : `<span class="badge badge-${d.action}">${d.action}</span>`;
    return `<tr class="${isConflict ? "" : ""}">
      <td><b>${esc(d.key)}</b>
          ${!d.branch_changed ? '<br><small title="分支未修改此行，主线决定一切">stale guard</small>' : ""}
      </td>
      <td class="state-cell">${renderState(d.base)}</td>
      <td class="state-cell">${renderState(d.branch)}
          ${d.branch_changed ? "" : "<br><small>分支未改</small>"}</td>
      <td class="state-cell">${renderState(d.main)}
          ${d.main_changed ? "" : "<br><small>主线未改</small>"}</td>
      <td>${badge}<br>${action}</td>
      <td class="resolution-cell">${
        isConflict && !d.resolution && m.status === "resolving"
          ? resolutionButtons(d)
          : d.resolution
            ? `<small>选择：${esc(d.resolution.choice)}
               ${d.resolution.payload !== undefined
                 ? esc(jstr(d.resolution.payload)) : ""}</small>`
            : `<small class="hint">${isConflict ? "需先进入裁决模式" : "自动处理"}</small>`
      }</td>
    </tr>`;
  }).join("");

  $$("[data-resolve]").forEach((btn) =>
    btn.addEventListener("click", () =>
      resolve(m.id, btn.dataset.key, btn.dataset.resolve)));
  $$("[data-apply-custom]").forEach((btn) =>
    btn.addEventListener("click", () => {
      const input = $(`#custom-${cssEscape(btn.dataset.applyCustom)}`);
      let payload;
      try { payload = JSON.parse(input.value); }
      catch { toast("自定义 payload 不是合法 JSON", "error"); return; }
      resolve(m.id, btn.dataset.applyCustom, "custom", payload);
    }));
}

function cssEscape(s) { return s.replace(/[^a-zA-Z0-9]/g, "_"); }

function resolutionButtons(d) {
  const cid = cssEscape(d.key);
  return `
    <button class="btn" data-resolve="branch" data-key="${esc(d.key)}">
      采用分支值（重放分支变更）</button>
    <button class="btn" data-resolve="main" data-key="${esc(d.key)}">
      保留主线值（丢弃分支变更）</button>
    <button class="btn" data-resolve="delete" data-key="${esc(d.key)}">
      删除该行</button>
    <div class="custom-row">
      <input id="custom-${cid}" placeholder='自定义合并快照 JSON'>
      <button class="btn" data-apply-custom="${esc(d.key)}">应用</button>
    </div>`;
}

async function resolve(mergeId, key, choice, customPayload) {
  try {
    await api(`/api/merges/${mergeId}/resolve/${encodeURIComponent(key)}`, {
      method: "POST",
      body: { choice, custom_payload: customPayload ?? null },
    });
    await refresh();
  } catch (e) { toast(e.message, "error"); }
}

// ------------------------------------------------------------- merge ops
$("#btn-prepare-merge").addEventListener("click", async () => {
  const branchId = Number($("#merge-branch-select").value);
  if (!branchId) return;
  try {
    const m = await api(`/api/branches/${branchId}/merge`, {
      method: "POST", body: { mode: $("#merge-mode").value },
    });
    await api(`/api/merges/${m.id}/begin`, { method: "POST" });
    toast(`合并 #${m.id} 已准备并进入裁决模式`);
    await refresh();
  } catch (e) { toast(e.message, "error"); }
});

$("#mg-toggle-mode").addEventListener("click", async () => {
  if (!currentMerge) return;
  const next = currentMerge.mode === "replay" ? "snapshot" : "replay";
  try {
    await api(`/api/merges/${currentMerge.id}/mode`,
              { method: "POST", body: { mode: next } });
    await refresh();
  } catch (e) { toast(e.message, "error"); }
});

$("#mg-commit").addEventListener("click", async () => {
  if (!currentMerge) return;
  try {
    const r = await api(`/api/merges/${currentMerge.id}/commit`,
                        { method: "POST" });
    if (r.warning) toast(r.warning + "\n" + r.recovery, "warn");
    else toast(`合并 #${currentMerge.id} 已提交，merge_switch 已发布`);
    await refresh();
  } catch (e) { toast(e.message, "error"); }
});

$("#mg-abort").addEventListener("click", async () => {
  if (!currentMerge) return;
  if (!confirm("撤销此合并？分支将重新开放，主线不受任何影响。")) return;
  try {
    await api(`/api/merges/${currentMerge.id}/abort`, { method: "POST" });
    toast("合并已撤销，分支回到 open");
    await refresh();
  } catch (e) { toast(e.message, "error"); }
});

// ------------------------------------------------------------ main writes
$("#m-upsert").addEventListener("click", async () => {
  try {
    const payload = JSON.parse($("#m-payload").value);
    await api("/api/main/rows", {
      method: "POST",
      body: { key: $("#m-key").value, payload },
    });
    await refresh();
  } catch (e) {
    toast(e instanceof SyntaxError ? "payload 不是合法 JSON" : e.message,
          "error");
  }
});
$("#m-delete").addEventListener("click", async () => {
  await api(`/api/main/rows/${encodeURIComponent($("#m-key").value)}`,
            { method: "DELETE" });
  await refresh();
});

// ---------------------------------------------------------- branch writes
$("#b-create").addEventListener("click", async () => {
  try {
    const b = await api("/api/branches",
                        { method: "POST", body: { name: $("#b-name").value } });
    selectedBranch = b.id;
    toast(`分支 ${b.name} 已创建，共同基线 seq=${b.base_seq}`);
    await refresh();
  } catch (e) { toast(e.message, "error"); }
});
$("#b-upsert").addEventListener("click", async () => {
  try {
    const payload = JSON.parse($("#b-payload").value);
    await api(`/api/branches/${selectedBranch}/rows`, {
      method: "POST", body: { key: $("#b-key").value, payload },
    });
    await refresh();
  } catch (e) {
    toast(e instanceof SyntaxError ? "payload 不是合法 JSON" : e.message,
          "error");
  }
});
$("#b-delete").addEventListener("click", async () => {
  await api(`/api/branches/${selectedBranch}/rows/${
    encodeURIComponent($("#b-key").value)}`, { method: "DELETE" });
  await refresh();
});

// --------------------------------------------------------- subscriptions
function renderSubs() {
  $("#sub-list").innerHTML = state.subscriptions.map((s) => `
    <div class="sub-card">
      <div>
        <b>${esc(s.name)}</b> <small>#${s.id}</small>
        <span class="pending-pill ${s.pending === 0 ? "zero" : ""}">
          未送达 ${s.pending}</span>
      </div>
      <button class="btn" data-sub="${s.id}" data-name="${esc(s.name)}">
        打开实时视图</button>
    </div>`).join("") || `<p class="hint">还没有订阅者</p>`;
  $$("#sub-list [data-sub]").forEach((btn) =>
    btn.addEventListener("click", () =>
      openLive(Number(btn.dataset.sub), btn.dataset.name)));
}

$("#sub-create").addEventListener("click", async () => {
  await api("/api/subscriptions", {
    method: "POST", body: { name: $("#sub-name").value },
  });
  await refresh();
});

function openLive(id, name) {
  liveSubId = id;
  lastSeq = 0;
  seenIds.clear();
  $("#sub-live").classList.remove("hidden");
  $("#sub-live-name").textContent = name;
  $("#sub-last-seq").textContent = "0";
  $("#sub-events").innerHTML = "";
  $("#sub-connect").disabled = false;
  $("#sub-disconnect").disabled = true;
}

function appendEvent(e, isDupe) {
  const line = document.createElement("div");
  line.className = `event-line ${isDupe ? "dupe" : ""}`;
  if (e.op === "merge_switch") {
    line.innerHTML = `<span class="seq">#${e.seq}</span>
      <span class="op op-merge_switch">▶ MERGE SWITCH（切换点）</span>
      <span class="origin">merge #${e.merge_id}</span>
      <small>${isDupe ? "重复事件，已去重" : "消费者在此原子跨越分支→主线合并结果"}</small>`;
  } else {
    line.innerHTML = `<span class="seq">#${e.seq}</span>
      <span class="op op-${e.op}">${e.op}(${esc(e.key ?? "")})</span>
      <span>${e.payload !== null && e.payload !== undefined
        ? esc(jstr(e.payload)) : ""}</span>
      <span class="origin">${esc(e.origin)}</span>
      ${isDupe ? "<small>重复，已去重</small>" : ""}`;
  }
  $("#sub-events").appendChild(line);
  $("#sub-events").scrollTop = $("#sub-events").scrollHeight;
}

$("#sub-connect").addEventListener("click", () => {
  if (!liveSubId) return;
  // Reconnect carries last_seq: the server replays the durable backlog
  // first, so a switch marker missed during the disconnect is never skipped.
  es = new EventSource(
    `/api/subscriptions/${liveSubId}/stream?last_seq=${lastSeq}`);
  es.onmessage = (msg) => {
    const e = JSON.parse(msg.data);
    const dupe = seenIds.has(e.seq);
    if (!dupe) seenIds.add(e.seq);
    if (!dupe || $("#sub-dedupe").checked) appendEvent(e, dupe);
    lastSeq = Math.max(lastSeq, e.seq);
    $("#sub-last-seq").textContent = lastSeq;
  };
  es.onerror = () => {
    // Browser auto-reconnects; show the backlog semantics explicitly.
    toast("SSE 连接中断，浏览器将带 last_seq 重连并补齐 backlog", "warn");
  };
  $("#sub-connect").disabled = true;
  $("#sub-disconnect").disabled = false;
});

$("#sub-disconnect").addEventListener("click", () => {
  if (es) { es.close(); es = null; }
  $("#sub-connect").disabled = false;
  $("#sub-disconnect").disabled = true;
});

$("#sub-replay").addEventListener("click", async () => {
  if (!liveSubId) return;
  const r = await api(`/api/subscriptions/${liveSubId}/replay`,
                      { method: "POST" });
  toast(`手动重放 ${r.redelivered} 条未送达事件`);
  r.events.forEach((e) => {
    const dupe = seenIds.has(e.seq);
    if (!dupe) seenIds.add(e.seq);
    if (!dupe || $("#sub-dedupe").checked) appendEvent(e, dupe);
    lastSeq = Math.max(lastSeq, e.seq);
  });
  $("#sub-last-seq").textContent = lastSeq;
  await refresh();
});

// ----------------------------------------------------------------- faults
function renderFaults() {
  $("#f-crash").checked = !!state.faults.crash_in_committing;
  $("#f-drop").checked = !!state.faults.drop_notification_after_commit;
  $("#fault-status").innerHTML = `
    <div class="event-log">
      <div class="event-line">crash_in_committing =
        <b>${state.faults.crash_in_committing}</b></div>
      <div class="event-line">drop_notification_after_commit =
        <b>${state.faults.drop_notification_after_commit}</b></div>
    </div>`;
}
async function pushFaults() {
  await api("/api/faults", {
    method: "POST",
    body: {
      crash_in_committing: $("#f-crash").checked,
      drop_notification_after_commit: $("#f-drop").checked,
    },
  });
  await refresh();
}
$("#f-crash").addEventListener("change", pushFaults);
$("#f-drop").addEventListener("change", pushFaults);
$("#btn-recover").addEventListener("click", async () => {
  const r = await api("/api/recover", { method: "POST" });
  toast(r.notes.length ? r.notes.join("\n") : "没有需要恢复的合并",
        r.notes.length ? "" : "warn");
  await refresh();
});

// ------------------------------------------------------------------ misc
$("#btn-refresh").addEventListener("click", refresh);
$("#btn-reseed").addEventListener("click", async () => {
  if (!confirm("清空全部数据并重新播种演示场景？")) return;
  await api("/api/reset", { method: "POST" });
  selectedBranch = null;
  currentMerge = null;
  toast("已重置，请运行 seed 脚本后刷新，或直接手动构造数据");
  await refresh();
});

refresh().catch((e) => toast(e.message, "error"));
