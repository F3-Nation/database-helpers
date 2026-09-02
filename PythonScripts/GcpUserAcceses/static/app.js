"use strict";

// ---- State -----------------------------------------------------------------
let allRecords = [];
let groupOrder = ["user", "resource", "role"];
let memberTypeFilter = new Set(); // empty = show all
let searchTerm = "";
let currentAssetType = ""; // asset_type of the resource chosen in the Add form
let rolesCache = []; // [{value, label}] for the role combobox
let combosReady = false;

const DIMS = {
  user: {
    label: "User",
    cls: "dim-user",
    key: (r) => r.member,
    display: (r) => r.email + (r.member_type === "serviceAccount" ? " (SA)" : ""),
  },
  resource: {
    label: "Resource",
    cls: "dim-resource",
    key: (r) => r.resource,
    display: (r) => r.resource_short,
  },
  role: {
    label: "Role",
    cls: "dim-role",
    key: (r) => r.role,
    display: (r) => r.role,
  },
};

const esc = (s) =>
  String(s == null ? "" : s).replace(/[&<>"']/g, (c) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c])
  );

// ---- Fetch wrapper + auth popup --------------------------------------------
// Returns parsed JSON. If the server reports missing/expired credentials
// (HTTP 401 or auth_required flag), shows the sign-in popup and throws.
async function apiJson(url, opts) {
  const res = await fetch(url, opts);
  let data = null;
  try {
    data = await res.json();
  } catch (e) {
    /* non-JSON response */
  }
  if (res.status === 401 || (data && data.auth_required)) {
    showAuthModal(data && data.error);
    const err = new Error("auth_required");
    err.authHandled = true;
    throw err;
  }
  return data;
}

const authModal = document.getElementById("authModal");
function showAuthModal(detail) {
  document.getElementById("authDetail").textContent = detail || "(no details)";
  authModal.classList.remove("hidden");
}
function hideAuthModal() {
  authModal.classList.add("hidden");
}
document.getElementById("authClose").addEventListener("click", hideAuthModal);
document.getElementById("authDismiss").addEventListener("click", hideAuthModal);
document.getElementById("authRetry").addEventListener("click", async () => {
  hideAuthModal();
  try {
    await loadData();
    toast("Reloaded.");
  } catch (e) {
    /* handled */
  }
});

// Explains WHY a grant/revoke failed (permission, not found, etc.).
const failModal = document.getElementById("failModal");
function showFailModal(out) {
  const reason = (out && out.reason) || "error";
  document.getElementById("failTitle").textContent =
    reason === "permission_denied"
      ? "You don't have permission"
      : reason === "not_found"
      ? "Not found"
      : "Action failed";
  document.getElementById("failMsg").textContent =
    (out && out.reason_message) || "The command failed.";
  document.getElementById("failDetail").textContent =
    (out && out.stderr) || "(no details)";
  failModal.classList.remove("hidden");
}
function hideFailModal() {
  failModal.classList.add("hidden");
}
document.getElementById("failClose").addEventListener("click", hideFailModal);
document.getElementById("failDismiss").addEventListener("click", hideFailModal);

// ---- Load ------------------------------------------------------------------
async function loadData() {
  const res = await fetch("/api/data");
  const data = await res.json();
  allRecords = data.bindings || [];
  document.getElementById("scopeLabel").textContent = data.scope || "";
  document.getElementById("updatedLabel").textContent = data.generated_at
    ? new Date(data.generated_at).toLocaleString()
    : "never (click Refresh scan)";
  buildMemberTypeFilter();
  render();
}

function buildMemberTypeFilter() {
  const types = [...new Set(allRecords.map((r) => r.member_type))].sort();
  const box = document.getElementById("memberTypeFilter");
  box.innerHTML = "";
  types.forEach((t) => {
    const id = "mt_" + t;
    const wrap = document.createElement("label");
    wrap.innerHTML = `<input type="checkbox" id="${id}" value="${t}" checked> ${esc(t)}`;
    box.appendChild(wrap);
    wrap.querySelector("input").addEventListener("change", (e) => {
      if (e.target.checked) memberTypeFilter.delete(t);
      else memberTypeFilter.add(t);
      render();
    });
  });
}

// ---- Group order chips (drag to reorder) -----------------------------------
function renderChips() {
  const ul = document.getElementById("groupOrder");
  ul.innerHTML = "";
  groupOrder.forEach((dim) => {
    const li = document.createElement("li");
    li.className = DIMS[dim].cls;
    li.draggable = true;
    li.dataset.dim = dim;
    li.innerHTML = `<span class="grip">⋮⋮</span> ${DIMS[dim].label}`;
    ul.appendChild(li);
  });
  let dragged = null;
  ul.querySelectorAll("li").forEach((li) => {
    li.addEventListener("dragstart", () => {
      dragged = li;
      li.classList.add("dragging");
    });
    li.addEventListener("dragend", () => {
      li.classList.remove("dragging");
      groupOrder = [...ul.querySelectorAll("li")].map((n) => n.dataset.dim);
      render();
    });
    li.addEventListener("dragover", (e) => {
      e.preventDefault();
      const after = e.clientX > li.getBoundingClientRect().left + li.offsetWidth / 2;
      if (dragged && dragged !== li) {
        ul.insertBefore(dragged, after ? li.nextSibling : li);
      }
    });
  });
}

// ---- Filtering + tree ------------------------------------------------------
function filteredRecords() {
  const term = searchTerm.toLowerCase();
  return allRecords.filter((r) => {
    if (memberTypeFilter.has(r.member_type)) return false;
    if (!term) return true;
    return (
      (r.email || "").toLowerCase().includes(term) ||
      (r.resource || "").toLowerCase().includes(term) ||
      (r.role || "").toLowerCase().includes(term)
    );
  });
}

function groupBy(records, dim) {
  const d = DIMS[dim];
  const map = new Map();
  for (const r of records) {
    const k = d.key(r);
    if (!map.has(k)) map.set(k, { display: d.display(r), records: [] });
    map.get(k).records.push(r);
  }
  return [...map.entries()].sort((a, b) =>
    a[1].display.localeCompare(b[1].display)
  );
}

function memberPill(r) {
  const cls =
    r.member_type === "serviceAccount"
      ? "pill-sa"
      : r.member_type === "group"
      ? "pill-group"
      : "pill-user";
  return `<span class="pill ${cls}">${esc(r.email)}</span>`;
}

function buildNode(records, depth) {
  if (depth >= groupOrder.length) return "";
  const dim = groupOrder[depth];
  const d = DIMS[dim];
  const groups = groupBy(records, dim);
  const isLeafLevel = depth === groupOrder.length - 1;
  let html = "";
  for (const [, g] of groups) {
    if (isLeafLevel) {
      // Deepest dim: each group is exactly one binding.
      const r = g.records[0];
      html += `<div class="leaf">
        ${leafPills(r)}
        <span class="spacer"></span>
        <button class="btn small btn-danger" onclick='revokeRecord(${JSON.stringify(
          r
        ).replace(/'/g, "&#39;")})'>Revoke</button>
      </div>`;
    } else {
      html += `<details class="node ${d.cls}">
        <summary><span class="twist">▶</span>
          <span class="label">${esc(g.display)}</span>
          <span class="count">${g.records.length}</span></summary>
        ${buildNode(g.records, depth + 1)}
      </details>`;
    }
  }
  return html;
}

function leafPills(r) {
  // The two outer dims are implied by the drill-down path; show only the deepest.
  const deepest = groupOrder[groupOrder.length - 1];
  if (deepest === "user") return memberPill(r);
  if (deepest === "resource")
    return `<span class="pill pill-res">${esc(r.resource_short)}</span>`;
  return `<span class="pill pill-role">${esc(r.role)}</span>`;
}

function render() {
  renderChips();
  const recs = filteredRecords();
  document.getElementById("summaryLine").textContent =
    `${recs.length} of ${allRecords.length} bindings · grouped by ` +
    groupOrder.map((d) => DIMS[d].label).join(" ▸ ");
  document.getElementById("tree").innerHTML = recs.length
    ? buildNode(recs, 0)
    : "<p class='summary'>No bindings match the current filters.</p>";
}

// ---- Revoke ----------------------------------------------------------------
async function revokeRecord(r) {
  if (
    !confirm(
      `Revoke this binding?\n\nMember: ${r.member}\nRole: ${r.role}\nResource: ${r.resource_short}`
    )
  )
    return;
  toast("Revoking…");
  try {
    const out = await apiJson("/api/revoke", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        member: r.member,
        resource: r.resource,
        role: r.role,
        asset_type: r.asset_type,
      }),
    });
    if (out.ok && out.verified !== false) {
      allRecords = allRecords.filter((x) => x.id !== r.id);
      render();
      toast("Revoked and verified.");
    } else {
      showFailModal(out);
    }
  } catch (e) {
    if (!e.authHandled) toast(String(e), true);
  }
}

// ---- Refresh ---------------------------------------------------------------
document.getElementById("refreshBtn").addEventListener("click", async (e) => {
  e.target.disabled = true;
  e.target.textContent = "Scanning…";
  try {
    await apiJson("/api/refresh", { method: "POST" });
    // Force the pickers to reload their (now refreshed) caches on next open.
    rolesCache = [];
    const psel = document.getElementById("projectSelect");
    psel.dataset.loaded = "";
    await loadData();
    toast("Refreshed bindings, projects, and roles.");
  } catch (err) {
    if (!err.authHandled) toast(String(err), true);
  } finally {
    e.target.disabled = false;
    e.target.textContent = "↻ Refresh all";
  }
});

// ---- Add-access modal ------------------------------------------------------
const modal = document.getElementById("addModal");
document.getElementById("addBtn").addEventListener("click", openAddModal);
document.getElementById("addClose").addEventListener("click", closeAddModal);
document.getElementById("addCancel").addEventListener("click", closeAddModal);

async function openAddModal() {
  modal.classList.remove("hidden");
  setStatus("");
  initCombos();
  try {
    await Promise.all([loadProjectsPicker(), loadRolesPicker()]);
  } catch (e) {
    if (!e.authHandled) setStatus(String(e), "err");
  }
}
function closeAddModal() {
  modal.classList.add("hidden");
}

// Reusable searchable dropdown: filters as you type, scrolls, allows free text.
function attachCombo(input, listEl, getItems) {
  const openList = () => {
    const q = input.value.trim().toLowerCase();
    const items = getItems();
    const filtered = (q
      ? items.filter(
          (i) =>
            i.value.toLowerCase().includes(q) ||
            (i.label || "").toLowerCase().includes(q)
        )
      : items
    ).slice(0, 300);
    listEl.innerHTML = filtered.length
      ? filtered
          .map(
            (i) =>
              `<div class="combo-item" data-value="${esc(i.value)}">${esc(i.label)}</div>`
          )
          .join("")
      : `<div class="combo-empty">No matches — free text is allowed</div>`;
    listEl.classList.remove("hidden");
  };
  input.addEventListener("focus", openList);
  input.addEventListener("input", openList);
  input.addEventListener("blur", () =>
    setTimeout(() => listEl.classList.add("hidden"), 150)
  );
  listEl.addEventListener("mousedown", (e) => {
    const item = e.target.closest(".combo-item");
    if (!item) return;
    e.preventDefault(); // keep focus, fire before blur
    input.value = item.dataset.value;
    listEl.classList.add("hidden");
    input.dispatchEvent(new Event("change"));
  });
}

function initCombos() {
  if (combosReady) return;
  attachCombo(
    document.getElementById("memberEmail"),
    document.getElementById("memberEmailList"),
    () =>
      [...new Set(allRecords.map((r) => r.email))]
        .sort()
        .map((e) => ({ value: e, label: e }))
  );
  attachCombo(
    document.getElementById("roleInput"),
    document.getElementById("roleList"),
    () => rolesCache
  );
  combosReady = true;
}

async function loadProjectsPicker() {
  const sel = document.getElementById("projectSelect");
  if (sel.dataset.loaded) return;
  const data = await apiJson("/api/pickers/projects");
  sel.innerHTML =
    `<option value="">Select project…</option>` +
    (data.projects || []).map((p) => `<option value="${esc(p)}">${esc(p)}</option>`).join("");
  sel.dataset.loaded = "1";
}

async function loadRolesPicker() {
  if (rolesCache.length) return;
  const data = await apiJson("/api/pickers/roles");
  rolesCache = (data.roles || []).map((r) => ({
    value: r.name,
    label: r.title ? `${r.name} — ${r.title}` : r.name,
  }));
}

// Bind at project level directly.
document.getElementById("projectSelfBtn").addEventListener("click", () => {
  const p = document.getElementById("projectSelect").value;
  if (!p) return setStatus("Pick a project first.", "err");
  document.getElementById("resourceName").value =
    `//cloudresourcemanager.googleapis.com/projects/${p}`;
  currentAssetType = "cloudresourcemanager.googleapis.com/Project";
  setStatus(`Resource set to project ${p}.`, "ok");
});

// Project -> resource types.
document.getElementById("projectSelect").addEventListener("change", async (e) => {
  const typeSel = document.getElementById("typeSelect");
  const resSel = document.getElementById("resourceSelect");
  typeSel.disabled = true;
  resSel.disabled = true;
  resSel.innerHTML = `<option value="">Resource…</option>`;
  const p = e.target.value;
  if (!p) return;
  typeSel.innerHTML = `<option value="">Loading types…</option>`;
  try {
    const data = await apiJson(
      `/api/pickers/resource-types?project=${encodeURIComponent(p)}`
    );
    if (data.error) {
      typeSel.innerHTML = `<option value="">Resource type…</option>`;
      return setStatus(data.error, "err");
    }
    const bindable = data.types.filter((t) => t.bindable);
    typeSel.innerHTML =
      `<option value="">Resource type…</option>` +
      bindable
        .map((t) => `<option value="${esc(t.asset_type)}">${esc(t.asset_type)} (${t.count})</option>`)
        .join("");
    typeSel.disabled = false;
  } catch (err) {
    typeSel.innerHTML = `<option value="">Resource type…</option>`;
    if (!err.authHandled) setStatus(String(err), "err");
  }
});

// Resource type -> resources.
document.getElementById("typeSelect").addEventListener("change", async (e) => {
  const resSel = document.getElementById("resourceSelect");
  const p = document.getElementById("projectSelect").value;
  const t = e.target.value;
  resSel.disabled = true;
  if (!t) return;
  resSel.innerHTML = `<option value="">Loading…</option>`;
  const url = `/api/pickers/resources?project=${encodeURIComponent(p)}&type=${encodeURIComponent(t)}`;
  try {
    const data = await apiJson(url);
    if (data.error) {
      resSel.innerHTML = `<option value="">Resource…</option>`;
      return setStatus(data.error, "err");
    }
    resSel.innerHTML =
      `<option value="">Resource…</option>` +
      data.resources
        .map((r) => `<option value="${esc(r.name)}" data-type="${esc(r.asset_type)}">${esc(r.display_name)}</option>`)
        .join("");
    resSel.disabled = false;
  } catch (err) {
    resSel.innerHTML = `<option value="">Resource…</option>`;
    if (!err.authHandled) setStatus(String(err), "err");
  }
});

// Resource chosen -> fill the name box.
document.getElementById("resourceSelect").addEventListener("change", (e) => {
  const opt = e.target.selectedOptions[0];
  if (!opt || !opt.value) return;
  document.getElementById("resourceName").value = opt.value;
  currentAssetType = opt.dataset.type || "";
});

// Manual edit clears the known asset type (backend will parse from the URL).
document.getElementById("resourceName").addEventListener("input", () => {
  currentAssetType = "";
});

// Grant.
document.getElementById("grantBtn").addEventListener("click", async () => {
  const type = document.getElementById("memberType").value;
  const email = document.getElementById("memberEmail").value.trim();
  const resource = document.getElementById("resourceName").value.trim();
  const role = document.getElementById("roleInput").value.trim();
  if (!email || !resource || !role)
    return setStatus("Member, resource, and role are all required.", "err");
  const member = `${type}:${email}`;
  if (!confirm(`Grant ${role}\nto ${member}\non ${resource}?`)) return;
  setStatus("Granting…");
  try {
    const out = await apiJson("/api/grant", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ member, resource, role, asset_type: currentAssetType }),
    });
    if (out.ok && out.verified !== false) {
      if (out.record) {
        allRecords = allRecords.filter((x) => x.id !== out.record.id);
        allRecords.push(out.record);
      }
      buildMemberTypeFilter();
      render();
      setStatus("Granted and verified.", "ok");
      toast("Access granted.");
    } else {
      setStatus("");
      showFailModal(out);
    }
  } catch (e) {
    if (!e.authHandled) setStatus(String(e), "err");
  }
});

function setStatus(msg, kind) {
  const el = document.getElementById("addStatus");
  el.textContent = msg;
  el.className = "status" + (kind ? " " + kind : "");
}

// ---- Toast + search --------------------------------------------------------
let toastTimer = null;
function toast(msg, isErr) {
  const el = document.getElementById("toast");
  el.textContent = msg;
  el.className = "toast" + (isErr ? " err" : "");
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => el.classList.add("hidden"), 4000);
}

document.getElementById("searchBox").addEventListener("input", (e) => {
  searchTerm = e.target.value;
  render();
});

loadData();
