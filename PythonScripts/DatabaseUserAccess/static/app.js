"use strict";

// ---- State -----------------------------------------------------------------
let service = null;
let dbname = null;
let snap = { roles: [], memberships: [], privileges: [], default_privileges: [], rls_policies: [] };
let privOrder = ["grantee", "object", "privilege"];
let roleTypeFilter = new Set();     // excluded types
let granteeTypeFilter = new Set();  // excluded types
let schemaFilter = "";
let roleSearch = "";
let privSearch = "";
let privilegesByType = {};
let retryAction = null;

const esc = (s) =>
  String(s == null ? "" : s).replace(/[&<>"']/g, (c) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c])
  );

// ---- Fetch wrapper + connection-error popup --------------------------------
async function apiJson(url, opts) {
  const res = await fetch(url, opts);
  let data = null;
  try { data = await res.json(); } catch (e) { /* non-JSON */ }
  if (res.status === 503 || (data && data.conn_error)) {
    showConnModal(data && data.error);
    const err = new Error("conn_error");
    err.connHandled = true;
    throw err;
  }
  return data;
}

const connModal = document.getElementById("connModal");
function showConnModal(detail) {
  document.getElementById("connDetail").textContent = detail || "(no details)";
  connModal.classList.remove("hidden");
}
function hideConnModal() { connModal.classList.add("hidden"); }
document.getElementById("connClose").addEventListener("click", hideConnModal);
document.getElementById("connDismiss").addEventListener("click", hideConnModal);
document.getElementById("connRetry").addEventListener("click", async () => {
  hideConnModal();
  try { if (retryAction) await retryAction(); } catch (e) { /* handled */ }
});

// Explains WHY a grant/revoke failed (permission, not found, etc.).
const failModal = document.getElementById("failModal");
function showFailModal(out) {
  const reason = (out && out.reason) || "error";
  document.getElementById("failTitle").textContent =
    reason === "permission_denied" ? "You don't have permission"
    : reason === "not_found" ? "Not found"
    : "Action failed";
  document.getElementById("failMsg").textContent =
    (out && (out.reason_message || out.error)) || "The statement failed.";
  document.getElementById("failDetail").textContent = (out && out.error) || "(no details)";
  failModal.classList.remove("hidden");
}
function hideFailModal() { failModal.classList.add("hidden"); }
document.getElementById("failClose").addEventListener("click", hideFailModal);
document.getElementById("failDismiss").addEventListener("click", hideFailModal);

// ---- Bootstrap -------------------------------------------------------------
async function init() {
  try {
    const svc = await apiJson("/api/services");
    const sel = document.getElementById("serviceSelect");
    sel.innerHTML = (svc.services || [])
      .map((s) => `<option value="${esc(s)}">${esc(s)}</option>`).join("");
    service = svc.services.includes(svc.default_service) ? svc.default_service : svc.services[0];
    sel.value = service;
    await loadDatabases(svc.default_dbname);
    await loadData();
  } catch (e) {
    if (!e.connHandled) toast(String(e), true);
  }
}

async function loadDatabases(preferred) {
  retryAction = () => loadDatabases(preferred);
  const data = await apiJson(`/api/databases?service=${encodeURIComponent(service)}`);
  const sel = document.getElementById("dbSelect");
  sel.innerHTML = (data.databases || [])
    .map((d) => `<option value="${esc(d)}">${esc(d)}</option>`).join("");
  dbname = data.databases.includes(preferred) ? preferred : data.databases[0];
  sel.value = dbname;
}

async function loadData() {
  retryAction = loadData;
  const data = await apiJson(
    `/api/data?service=${encodeURIComponent(service)}&dbname=${encodeURIComponent(dbname)}`
  );
  snap = data;
  document.getElementById("updatedLabel").textContent = data.generated_at
    ? `${service} / ${dbname} — ${new Date(data.generated_at).toLocaleString()}`
    : `${service} / ${dbname} — not scanned yet (click Refresh)`;
  document.getElementById("connStatus").textContent =
    `${snap.roles.length} roles · ${snap.privileges.length} grants · ${snap.memberships.length} memberships`;
  buildRoleTypeFilter();
  buildGranteeTypeFilter();
  buildSchemaFilter();
  renderRoles();
  renderPrivTree();
  renderDefaults();
  renderRls();
}

document.getElementById("serviceSelect").addEventListener("change", async (e) => {
  service = e.target.value;
  try { await loadDatabases(dbname); await loadData(); }
  catch (err) { if (!err.connHandled) toast(String(err), true); }
});
document.getElementById("dbSelect").addEventListener("change", async (e) => {
  dbname = e.target.value;
  try { await loadData(); } catch (err) { if (!err.connHandled) toast(String(err), true); }
});

document.getElementById("refreshBtn").addEventListener("click", async (e) => {
  e.target.disabled = true; e.target.textContent = "Introspecting…";
  retryAction = () => document.getElementById("refreshBtn").click();
  try {
    snap = await apiJson("/api/refresh", {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ service, dbname }),
    });
    await loadData();
    toast("Snapshot refreshed.");
  } catch (err) {
    if (!err.connHandled) toast(String(err), true);
  } finally {
    e.target.disabled = false; e.target.textContent = "↻ Refresh snapshot";
  }
});

// ---- Tabs ------------------------------------------------------------------
document.querySelectorAll(".tab").forEach((t) => {
  t.addEventListener("click", () => {
    document.querySelectorAll(".tab").forEach((x) => x.classList.remove("active"));
    document.querySelectorAll(".view").forEach((x) => x.classList.remove("active"));
    t.classList.add("active");
    document.getElementById(t.dataset.view).classList.add("active");
  });
});

// ---- Filter builders -------------------------------------------------------
function classifyRole(r) { return r.rolcanlogin ? "user" : "group"; }

function buildCheckboxFilter(containerId, values, excludedSet, onChange) {
  const box = document.getElementById(containerId);
  box.innerHTML = "";
  values.forEach((v) => {
    const wrap = document.createElement("label");
    wrap.innerHTML = `<input type="checkbox" value="${esc(v)}" ${excludedSet.has(v) ? "" : "checked"}> ${esc(v)}`;
    box.appendChild(wrap);
    wrap.querySelector("input").addEventListener("change", (e) => {
      if (e.target.checked) excludedSet.delete(v); else excludedSet.add(v);
      onChange();
    });
  });
}
function buildRoleTypeFilter() {
  buildCheckboxFilter("roleTypeFilter", ["user", "group"], roleTypeFilter, renderRoles);
}
function buildGranteeTypeFilter() {
  const types = [...new Set(snap.privileges.map((p) => p.grantee_type))].sort();
  buildCheckboxFilter("granteeTypeFilter", types, granteeTypeFilter, renderPrivTree);
}
function buildSchemaFilter() {
  const schemas = [...new Set(snap.privileges.map((p) => p.schema).filter(Boolean))].sort();
  const sel = document.getElementById("schemaFilter");
  sel.innerHTML = `<option value="">all schemas</option>` +
    schemas.map((s) => `<option value="${esc(s)}">${esc(s)}</option>`).join("");
  sel.value = schemaFilter;
}

// ---- Roles view ------------------------------------------------------------
function roleBadges(r) {
  const b = [];
  if (r.rolsuper) b.push(`<span class="badge b-super">SUPERUSER</span>`);
  b.push(r.rolcanlogin ? `<span class="badge b-login">LOGIN</span>` : `<span class="badge b-nologin">NOLOGIN</span>`);
  if (r.rolcreaterole) b.push(`<span class="badge b-attr">CREATEROLE</span>`);
  if (r.rolcreatedb) b.push(`<span class="badge b-attr">CREATEDB</span>`);
  if (r.rolreplication) b.push(`<span class="badge b-attr">REPLICATION</span>`);
  if (r.rolbypassrls) b.push(`<span class="badge b-attr">BYPASSRLS</span>`);
  if (!r.rolinherit) b.push(`<span class="badge b-attr">NOINHERIT</span>`);
  if (r.rolconnlimit != null && r.rolconnlimit >= 0) b.push(`<span class="badge b-attr">CONN ≤ ${r.rolconnlimit}</span>`);
  if (r.rolvaliduntil) b.push(`<span class="badge b-attr">VALID TIL ${esc(String(r.rolvaliduntil).slice(0, 10))}</span>`);
  return b.join("");
}

function renderRoles() {
  const term = roleSearch.toLowerCase();
  const memberOf = {}, members = {};
  snap.memberships.forEach((m) => {
    (memberOf[m.member_role] = memberOf[m.member_role] || []).push(m);
    (members[m.group_role] = members[m.group_role] || []).push(m);
  });
  const roles = snap.roles.filter((r) => {
    if (roleTypeFilter.has(classifyRole(r))) return false;
    return !term || r.rolname.toLowerCase().includes(term);
  });
  const container = document.getElementById("rolesList");
  container.innerHTML = roles.map((r) => {
    const mo = (memberOf[r.rolname] || []);
    const me = (members[r.rolname] || []);
    const moHtml = mo.length
      ? mo.map((m) => memberLine(m.group_role, "member-of", m, r.rolname)).join("")
      : `<div class="member-line" style="color:var(--gray)">— none —</div>`;
    const meHtml = me.length
      ? me.map((m) => memberLine(m.member_role, "has-member", m, r.rolname)).join("")
      : `<div class="member-line" style="color:var(--gray)">— none —</div>`;
    return `<div class="role-row" data-role="${esc(r.rolname)}">
      <div class="role-head">
        <span class="twist">▶</span>
        <span class="role-name">${esc(r.rolname)}</span>
        <span class="role-badges">${roleBadges(r)}</span>
      </div>
      <div class="role-body">
        <div class="member-block"><h4>Member of (groups this role belongs to)</h4>${moHtml}</div>
        <div class="member-block"><h4>Members (roles granted into this role)</h4>${meHtml}</div>
      </div>
    </div>`;
  }).join("") || `<p class="summary">No roles match.</p>`;

  container.querySelectorAll(".role-head").forEach((h) => {
    h.addEventListener("click", () => h.parentElement.classList.toggle("open"));
  });
}

// direction "member-of": current role is a member of `otherGroup`.
// direction "has-member": current role is the group; `otherMember` is a member.
function memberLine(otherName, direction, m, currentRole) {
  const group = direction === "member-of" ? otherName : currentRole;
  const member = direction === "member-of" ? currentRole : otherName;
  const admin = m.admin_option ? ` <span class="badge b-attr">ADMIN</span>` : "";
  return `<div class="member-line">
    <span class="pill pill-grantee">${esc(otherName)}</span>${admin}
    <span class="spacer" style="flex:1"></span>
    <button class="btn small btn-danger"
      onclick='revokeMembership(${JSON.stringify(group)}, ${JSON.stringify(member)})'>Revoke</button>
  </div>`;
}

async function revokeMembership(group, member) {
  if (!confirm(`REVOKE ${group} FROM ${member} ?`)) return;
  await doWrite("/api/revoke-membership", { service, dbname, group_role: group, member_role: member },
    `Revoked ${group} from ${member}.`);
}

// ---- Privileges tree -------------------------------------------------------
const PDIMS = {
  grantee: { label: "Grantee", cls: "dim-grantee", key: (p) => p.grantee, display: (p) => p.grantee },
  object: { label: "Object", cls: "dim-object", key: (p) => p.object_label, display: (p) => p.object_label },
  privilege: { label: "Privilege", cls: "dim-privilege", key: (p) => p.privilege, display: (p) => p.privilege },
};

function renderPrivChips() {
  const ul = document.getElementById("privOrder");
  ul.innerHTML = "";
  privOrder.forEach((dim) => {
    const li = document.createElement("li");
    li.className = PDIMS[dim].cls; li.draggable = true; li.dataset.dim = dim;
    li.innerHTML = `<span class="grip">⋮⋮</span> ${PDIMS[dim].label}`;
    ul.appendChild(li);
  });
  let dragged = null;
  ul.querySelectorAll("li").forEach((li) => {
    li.addEventListener("dragstart", () => { dragged = li; li.classList.add("dragging"); });
    li.addEventListener("dragend", () => {
      li.classList.remove("dragging");
      privOrder = [...ul.querySelectorAll("li")].map((n) => n.dataset.dim);
      renderPrivTree();
    });
    li.addEventListener("dragover", (e) => {
      e.preventDefault();
      const after = e.clientX > li.getBoundingClientRect().left + li.offsetWidth / 2;
      if (dragged && dragged !== li) ul.insertBefore(dragged, after ? li.nextSibling : li);
    });
  });
}

function filteredPrivs() {
  const term = privSearch.toLowerCase();
  return snap.privileges.filter((p) => {
    if (granteeTypeFilter.has(p.grantee_type)) return false;
    if (schemaFilter && p.schema !== schemaFilter) return false;
    if (!term) return true;
    return p.grantee.toLowerCase().includes(term) ||
      p.object_label.toLowerCase().includes(term) ||
      p.privilege.toLowerCase().includes(term);
  });
}

function pGroupBy(records, dim) {
  const d = PDIMS[dim];
  const map = new Map();
  for (const r of records) {
    const k = d.key(r);
    if (!map.has(k)) map.set(k, { display: d.display(r), records: [] });
    map.get(k).records.push(r);
  }
  return [...map.entries()].sort((a, b) => String(a[1].display).localeCompare(String(b[1].display)));
}

function pillFor(dim, p) {
  if (dim === "grantee")
    return `<span class="pill ${p.is_public ? "pill-public" : "pill-grantee"}">${esc(p.grantee)}</span>`;
  if (dim === "object") return `<span class="pill pill-object">${esc(p.object_label)}</span>`;
  return `<span class="pill pill-priv">${esc(p.privilege)}</span>`;
}

function buildPrivNode(records, depth) {
  if (depth >= privOrder.length) return "";
  const dim = privOrder[depth];
  const d = PDIMS[dim];
  const groups = pGroupBy(records, dim);
  const isLeaf = depth === privOrder.length - 1;
  let html = "";
  for (const [, g] of groups) {
    if (isLeaf) {
      const p = g.records[0];
      html += `<div class="leaf">${pillFor(privOrder[privOrder.length - 1], p)}
        <span class="spacer"></span>
        ${p.grantable ? '<span class="badge b-attr">GRANTABLE</span>' : ""}
        <button class="btn small btn-danger"
          onclick='revokePrivilege(${JSON.stringify(p).replace(/'/g, "&#39;")})'>Revoke</button>
      </div>`;
    } else {
      html += `<details class="node ${d.cls}"><summary><span class="twist">▶</span>
        <span class="label">${esc(g.display)}</span>
        <span class="count">${g.records.length}</span></summary>
        ${buildPrivNode(g.records, depth + 1)}</details>`;
    }
  }
  return html;
}

function renderPrivTree() {
  renderPrivChips();
  const recs = filteredPrivs();
  document.getElementById("privSummary").textContent =
    `${recs.length} of ${snap.privileges.length} grants · grouped by ` +
    privOrder.map((d) => PDIMS[d].label).join(" ▸ ");
  document.getElementById("privTree").innerHTML = recs.length
    ? buildPrivNode(recs, 0)
    : `<p class="summary">No grants match the current filters.</p>`;
}

async function revokePrivilege(p) {
  if (!confirm(`REVOKE ${p.privilege} ON ${p.object_label} FROM ${p.grantee} ?`)) return;
  await doWrite("/api/revoke-privilege", {
    service, dbname, object_type: p.object_type, schema: p.schema,
    object: p.object, args: p.args, grantee: p.grantee, privilege: p.privilege,
  }, `Revoked ${p.privilege} on ${p.object_label} from ${p.grantee}.`);
}

// ---- Default privileges + RLS ---------------------------------------------
function renderDefaults() {
  const rows = snap.default_privileges || [];
  document.getElementById("defaultTable").innerHTML = rows.length
    ? `<table class="grid"><thead><tr>
        <th>Owner</th><th>Schema</th><th>Object type</th><th>Grantee</th><th>Privilege</th><th>Grantable</th>
      </tr></thead><tbody>${rows.map((d) => `<tr>
        <td>${esc(d.owner)}</td><td>${esc(d.schema)}</td><td>${esc(d.object_type)}</td>
        <td>${esc(d.grantee)}</td><td>${esc(d.privilege)}</td><td>${d.grantable ? "yes" : ""}</td>
      </tr>`).join("")}</tbody></table>`
    : `<p class="summary">No default privileges configured.</p>`;
}

function renderRls() {
  const rows = snap.rls_policies || [];
  document.getElementById("rlsTable").innerHTML = rows.length
    ? `<table class="grid"><thead><tr>
        <th>Schema</th><th>Table</th><th>Policy</th><th>Perm.</th><th>Roles</th><th>Cmd</th><th>USING</th><th>WITH CHECK</th>
      </tr></thead><tbody>${rows.map((r) => `<tr>
        <td>${esc(r.schema)}</td><td>${esc(r.table)}</td><td>${esc(r.policy)}</td>
        <td>${esc(r.permissive)}</td><td>${esc((r.roles || []).join(", "))}</td><td>${esc(r.cmd)}</td>
        <td class="mono">${esc(r.qual || "")}</td><td class="mono">${esc(r.with_check || "")}</td>
      </tr>`).join("")}</tbody></table>`
    : `<p class="summary">No row-level security policies.</p>`;
}

// ---- Writes ----------------------------------------------------------------
async function doWrite(url, body, okMsg) {
  toast("Applying…");
  try {
    const out = await apiJson(url, {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    if (out.ok && out.verified !== false) {
      await loadData();
      toast(okMsg + (out.verified === null ? " (unverified)" : ""));
      return true;
    }
    showFailModal(out);
    return false;
  } catch (e) {
    if (!e.connHandled) toast(String(e), true);
    return false;
  }
}

// ---- Combobox --------------------------------------------------------------
function attachCombo(input, listEl, getItems) {
  const openList = () => {
    const q = input.value.trim().toLowerCase();
    const items = getItems();
    const filtered = (q
      ? items.filter((i) => i.value.toLowerCase().includes(q) || (i.label || "").toLowerCase().includes(q))
      : items
    ).slice(0, 300);
    listEl.innerHTML = filtered.length
      ? filtered.map((i) => `<div class="combo-item" data-value="${esc(i.value)}">${esc(i.label)}</div>`).join("")
      : `<div class="combo-empty">No matches — free text is allowed</div>`;
    listEl.classList.remove("hidden");
  };
  input.addEventListener("focus", openList);
  input.addEventListener("input", openList);
  input.addEventListener("blur", () => setTimeout(() => listEl.classList.add("hidden"), 150));
  listEl.addEventListener("mousedown", (e) => {
    const item = e.target.closest(".combo-item");
    if (!item) return;
    e.preventDefault();
    input.value = item.dataset.value;
    listEl.classList.add("hidden");
    input.dispatchEvent(new Event("change"));
  });
}

// ---- Add-access modal ------------------------------------------------------
const addModal = document.getElementById("addModal");
let addCombosReady = false;
let addMode = "membership";

document.getElementById("addBtn").addEventListener("click", openAddModal);
document.getElementById("addClose").addEventListener("click", () => addModal.classList.add("hidden"));
document.getElementById("addCancel").addEventListener("click", () => addModal.classList.add("hidden"));

document.querySelectorAll(".mode-tab").forEach((t) => {
  t.addEventListener("click", () => {
    document.querySelectorAll(".mode-tab").forEach((x) => x.classList.remove("active"));
    t.classList.add("active");
    addMode = t.dataset.mode;
    document.getElementById("modeMembership").classList.toggle("hidden", addMode !== "membership");
    document.getElementById("modePrivilege").classList.toggle("hidden", addMode !== "privilege");
    document.getElementById("applyBtn").textContent = "Grant";
    setStatus("");
  });
});

async function openAddModal() {
  addModal.classList.remove("hidden");
  setStatus("");
  initAddCombos();
  if (!Object.keys(privilegesByType).length) {
    try {
      const d = await apiJson("/api/pickers/privileges");
      privilegesByType = d.privileges_by_type || {};
    } catch (e) { /* handled */ }
  }
  updatePrivilegeOptions();
  await loadSchemaPicker();
}

function roleItems(includePublic) {
  const items = snap.roles.map((r) => ({ value: r.rolname, label: `${r.rolname} (${classifyRole(r)})` }));
  if (includePublic) items.unshift({ value: "PUBLIC", label: "PUBLIC" });
  return items;
}

function initAddCombos() {
  if (addCombosReady) return;
  attachCombo(document.getElementById("memGroup"), document.getElementById("memGroupList"), () => roleItems(false));
  attachCombo(document.getElementById("memMember"), document.getElementById("memMemberList"), () => roleItems(false));
  attachCombo(document.getElementById("prvGrantee"), document.getElementById("prvGranteeList"), () => roleItems(true));
  addCombosReady = true;
}

function updatePrivilegeOptions() {
  const type = document.getElementById("prvType").value;
  const privs = privilegesByType[type] || [];
  document.getElementById("prvPrivilege").innerHTML =
    privs.map((p) => `<option value="${esc(p)}">${esc(p)}</option>`).join("");
  // Show/hide schema & object rows by type.
  const needsSchema = ["table", "view", "matview", "sequence", "function", "schema"].includes(type);
  const needsObject = ["table", "view", "matview", "sequence", "function"].includes(type);
  document.getElementById("prvSchemaRow").classList.toggle("hidden", !needsSchema);
  document.getElementById("prvObjectRow").classList.toggle("hidden", !needsObject);
}

document.getElementById("prvType").addEventListener("change", () => {
  updatePrivilegeOptions();
  document.getElementById("prvObject").innerHTML = `<option value="">Object…</option>`;
  document.getElementById("prvObject").disabled = true;
  loadObjectPicker();
});

async function loadSchemaPicker() {
  try {
    const d = await apiJson(`/api/pickers/schemas?service=${encodeURIComponent(service)}&dbname=${encodeURIComponent(dbname)}`);
    const sel = document.getElementById("prvSchema");
    sel.innerHTML = `<option value="">Select schema…</option>` +
      (d.schemas || []).map((s) => `<option value="${esc(s)}">${esc(s)}</option>`).join("");
  } catch (e) { /* handled */ }
}

document.getElementById("prvSchema").addEventListener("change", loadObjectPicker);

async function loadObjectPicker() {
  const type = document.getElementById("prvType").value;
  const schema = document.getElementById("prvSchema").value;
  const objSel = document.getElementById("prvObject");
  if (!["table", "view", "matview", "sequence", "function"].includes(type) || !schema) {
    objSel.disabled = true; objSel.innerHTML = `<option value="">Object…</option>`;
    return;
  }
  objSel.innerHTML = `<option value="">Loading…</option>`;
  try {
    const url = `/api/pickers/objects?service=${encodeURIComponent(service)}&dbname=${encodeURIComponent(dbname)}&schema=${encodeURIComponent(schema)}&type=${encodeURIComponent(type)}`;
    const d = await apiJson(url);
    objSel.innerHTML = `<option value="">Object…</option>` +
      (d.objects || []).map((o) =>
        `<option value="${esc(o.name)}" data-args="${esc(o.args || "")}">${esc(o.name)}${o.args ? "(" + esc(o.args) + ")" : ""}</option>`
      ).join("");
    objSel.disabled = false;
  } catch (e) {
    objSel.innerHTML = `<option value="">Object…</option>`;
  }
}

document.getElementById("applyBtn").addEventListener("click", async () => {
  if (addMode === "membership") {
    const group = document.getElementById("memGroup").value.trim();
    const member = document.getElementById("memMember").value.trim();
    if (!group || !member) return setStatus("Both roles are required.", "err");
    if (!confirm(`GRANT ${group} TO ${member}${document.getElementById("memAdmin").checked ? " WITH ADMIN OPTION" : ""} ?`)) return;
    setStatus("Applying…");
    const ok = await doWrite("/api/grant-membership", {
      service, dbname, group_role: group, member_role: member,
      admin_option: document.getElementById("memAdmin").checked,
    }, `Granted ${group} to ${member}.`);
    if (ok) setStatus("Granted and verified.", "ok");
    else setStatus("");
  } else {
    const grantee = document.getElementById("prvGrantee").value.trim();
    const type = document.getElementById("prvType").value;
    const schema = document.getElementById("prvSchema").value;
    const objOpt = document.getElementById("prvObject").selectedOptions[0];
    const object = type === "database" ? dbname : (type === "schema" ? schema : (objOpt ? objOpt.value : ""));
    const args = objOpt ? objOpt.dataset.args : "";
    const privilege = document.getElementById("prvPrivilege").value;
    if (!grantee || !privilege) return setStatus("Grantee and privilege are required.", "err");
    if (["table", "view", "matview", "sequence", "function"].includes(type) && !object)
      return setStatus("Pick an object.", "err");
    if (type === "schema" && !schema) return setStatus("Pick a schema.", "err");
    const targetLabel = type === "schema" ? schema : type === "database" ? object : `${schema}.${object}`;
    if (!confirm(`GRANT ${privilege} ON ${type} ${targetLabel} TO ${grantee}${document.getElementById("prvGrantOption").checked ? " WITH GRANT OPTION" : ""} ?`)) return;
    setStatus("Applying…");
    const ok = await doWrite("/api/grant-privilege", {
      service, dbname, object_type: type, schema: schema || null,
      object: object || null, args: args || null, grantee, privilege,
      grant_option: document.getElementById("prvGrantOption").checked,
    }, `Granted ${privilege} on ${targetLabel} to ${grantee}.`);
    if (ok) setStatus("Granted and verified.", "ok");
    else setStatus("");
  }
});

function setStatus(msg, kind) {
  const el = document.getElementById("addStatus");
  el.textContent = msg;
  el.className = "status" + (kind ? " " + kind : "");
}

// ---- Search + toast --------------------------------------------------------
document.getElementById("roleSearch").addEventListener("input", (e) => { roleSearch = e.target.value; renderRoles(); });
document.getElementById("privSearch").addEventListener("input", (e) => { privSearch = e.target.value; renderPrivTree(); });
document.getElementById("schemaFilter").addEventListener("change", (e) => { schemaFilter = e.target.value; renderPrivTree(); });

let toastTimer = null;
function toast(msg, isErr) {
  const el = document.getElementById("toast");
  el.textContent = msg;
  el.className = "toast" + (isErr ? " err" : "");
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => el.classList.add("hidden"), 4000);
}

init();
