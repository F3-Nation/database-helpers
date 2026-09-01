"""Core GCP IAM helpers shared by the Flask app and CLI scripts.

Reads (bindings, resource drill-down, roles) use Cloud Asset Inventory / gcloud.
Writes (grant / revoke) shell out to `gcloud ... {add,remove}-iam-policy-binding`
so they run as the currently logged-in gcloud user, then verify the result from
the policy gcloud returns and patch the single affected entry in the cache.
"""

import json
import os
import re
import subprocess
import threading
import time
from datetime import datetime, timezone

from google.api_core.client_options import ClientOptions
from google.cloud import asset_v1

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Load config from a local .env if present (see .env.sample).
try:
    from dotenv import load_dotenv

    load_dotenv(os.path.join(BASE_DIR, ".env"))
except ImportError:
    pass

# --- Configuration (override in .env or the environment) --------------------
SCOPE = os.environ.get("GCP_SCOPE", "organizations/803404482088")
QUOTA_PROJECT_ID = os.environ.get("GCP_QUOTA_PROJECT", "f3-workspace")
ORG_ID = (
    SCOPE.split("/")[-1]
    if SCOPE.startswith("organizations/")
    else os.environ.get("GCP_ORG_ID", "")
)

CACHE_DIR = os.path.join(BASE_DIR, "cache")
BINDINGS_CACHE = os.path.join(CACHE_DIR, "iam_bindings.json")
ROLES_CACHE = os.path.join(CACHE_DIR, "roles.json")
PROJECTS_CACHE = os.path.join(CACHE_DIR, "projects.json")

# Member prefixes we care about. IAM has more, but these cover real people,
# service accounts and groups managing access.
KNOWN_MEMBER_TYPES = ("user", "serviceAccount", "group", "domain")

_lock = threading.Lock()


class AuthError(Exception):
    """Raised when GCP credentials are missing/expired and the user must
    re-run `gcloud auth login`."""


# Substrings that indicate an expired/missing-credentials situation.
_AUTH_HINTS = (
    "gcloud auth login",
    "reauthentication failed",
    "there was a problem refreshing",
    "invalid_grant",
    "request had invalid authentication",
    "invalid authentication credentials",
    "could not automatically determine credentials",
    "default credentials",
    "does not have valid credentials",
    "unable to acquire impersonated credentials",
)


def _looks_like_auth_error(text):
    t = (text or "").lower()
    return any(h in t for h in _AUTH_HINTS)


def _auth_guard(fn):
    """Run a callable, converting credential failures into AuthError."""
    try:
        return fn()
    except AuthError:
        raise
    except Exception as e:  # noqa: BLE001 - inspect message for auth hints
        if _looks_like_auth_error(str(e)) or e.__class__.__name__ in (
            "RefreshError",
            "DefaultCredentialsError",
            "Unauthenticated",
        ):
            raise AuthError(str(e)) from e
        raise



# --- Small utilities --------------------------------------------------------
def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _ensure_cache_dir():
    os.makedirs(CACHE_DIR, exist_ok=True)


def _asset_client():
    options = ClientOptions(quota_project_id=QUOTA_PROJECT_ID)
    return asset_v1.AssetServiceClient(client_options=options)


def split_member(member):
    """'user:a@b.com' -> ('user', 'a@b.com'). Handles 'deleted:...' prefixes."""
    raw = member
    deleted = False
    if raw.startswith("deleted:"):
        deleted = True
        raw = raw[len("deleted:"):]
    if ":" in raw:
        mtype, email = raw.split(":", 1)
    else:
        mtype, email = "other", raw
    if mtype not in KNOWN_MEMBER_TYPES:
        mtype = mtype if mtype in ("projectOwner", "projectEditor", "projectViewer") else "other"
    return mtype, email, deleted


def binding_id(member, resource, role):
    return f"{member}||{resource}||{role}"


# --- Resource parsing -------------------------------------------------------
def parse_resource(resource):
    """Extract project id/number and a short label from an Asset Inventory
    resource name like '//service.googleapis.com/projects/P/...'.
    """
    project = None
    m = re.search(r"/projects/([^/]+)", resource)
    if m:
        project = m.group(1)
    m = re.search(r"/folders/([^/]+)", resource)
    folder = m.group(1) if m else None
    m = re.search(r"/organizations/([^/]+)", resource)
    org = m.group(1) if m else None
    # Short label: strip the //host part and keep the path tail.
    short = re.sub(r"^//[^/]+/", "", resource)
    return {"project": project, "folder": folder, "org": org, "short": short}


class UnsupportedResourceError(Exception):
    pass


def build_iam_command(action, resource, asset_type, member, role):
    """Return a gcloud argv list for add/remove IAM binding on `resource`.

    Raises UnsupportedResourceError if we don't know how to bind on that type.
    """
    if action not in ("add", "remove"):
        raise ValueError("action must be 'add' or 'remove'")
    verb = f"{action}-iam-policy-binding"
    info = parse_resource(resource)
    at = (asset_type or "").lower()

    def secret_name():
        m = re.search(r"/secrets/([^/]+)", resource)
        return m.group(1) if m else None

    def topic_name():
        m = re.search(r"/topics/([^/]+)", resource)
        return m.group(1) if m else None

    def subscription_name():
        m = re.search(r"/subscriptions/([^/]+)", resource)
        return m.group(1) if m else None

    def ar_repo():
        m = re.search(r"/locations/([^/]+)/repositories/([^/]+)", resource)
        return (m.group(1), m.group(2)) if m else (None, None)

    def run_service():
        m = re.search(r"/locations/([^/]+)/services/([^/]+)", resource)
        return (m.group(1), m.group(2)) if m else (None, None)

    cmd = None
    if "cloudresourcemanager.googleapis.com/project" in at or (
        not at and re.search(r"/projects/[^/]+$", resource)
    ):
        cmd = ["gcloud", "projects", verb, info["project"]]
    elif "cloudresourcemanager.googleapis.com/folder" in at or info["folder"]:
        cmd = ["gcloud", "resource-manager", "folders", verb, info["folder"]]
    elif "cloudresourcemanager.googleapis.com/organization" in at or info["org"]:
        cmd = ["gcloud", "organizations", verb, info["org"]]
    elif "storage.googleapis.com/bucket" in at or resource.startswith(
        "//storage.googleapis.com/"
    ):
        bucket = resource.split("storage.googleapis.com/")[-1]
        cmd = ["gcloud", "storage", "buckets", verb, f"gs://{bucket}"]
    elif "secretmanager.googleapis.com/secret" in at and secret_name():
        cmd = ["gcloud", "secrets", verb, secret_name(), f"--project={info['project']}"]
    elif "pubsub.googleapis.com/topic" in at and topic_name():
        cmd = ["gcloud", "pubsub", "topics", verb, topic_name(), f"--project={info['project']}"]
    elif "pubsub.googleapis.com/subscription" in at and subscription_name():
        cmd = ["gcloud", "pubsub", "subscriptions", verb, subscription_name(), f"--project={info['project']}"]
    elif "artifactregistry.googleapis.com/repository" in at:
        loc, repo = ar_repo()
        if repo:
            cmd = ["gcloud", "artifacts", "repositories", verb, repo,
                   f"--location={loc}", f"--project={info['project']}"]
    elif "run.googleapis.com/service" in at:
        loc, svc = run_service()
        if svc:
            cmd = ["gcloud", "run", "services", verb, svc,
                   f"--region={loc}", f"--project={info['project']}"]

    if cmd is None:
        raise UnsupportedResourceError(
            f"No gcloud IAM binding command mapped for asset_type='{asset_type}' "
            f"resource='{resource}'."
        )

    cmd += [f"--member={member}", f"--role={role}", "--condition=None",
            "--format=json", "--quiet"]
    return cmd


def _policy_has_binding(policy, role, member):
    for b in policy.get("bindings", []):
        if b.get("role") == role and member in (b.get("members") or []):
            return True
    return False


def apply_binding(action, member, resource, asset_type, role):
    """Run the gcloud command, verify from the returned policy, return a dict."""
    cmd = build_iam_command(action, resource, asset_type, member, role)
    result = subprocess.run(cmd, capture_output=True, text=True)
    out = (result.stdout or "").strip()
    err = (result.stderr or "").strip()
    ok = result.returncode == 0
    verified = None
    if ok and out:
        try:
            policy = json.loads(out)
            has = _policy_has_binding(policy, role, member)
            verified = has if action == "add" else (not has)
        except json.JSONDecodeError:
            verified = None
    return {
        "ok": ok,
        "verified": verified,
        "command": " ".join(cmd),
        "stdout": out,
        "stderr": err,
        "auth_required": (not ok) and _looks_like_auth_error(err),
    }


# --- Bindings fetch + cache -------------------------------------------------
def fetch_all_bindings(scope=SCOPE):
    """Query Cloud Asset Inventory for every IAM binding under `scope`."""
    client = _asset_client()
    request = asset_v1.SearchAllIamPoliciesRequest(scope=scope, query="")
    records = []

    def _iter():
        for result in client.search_all_iam_policies(request=request):
            resource = result.resource
            asset_type = getattr(result, "asset_type", "") or ""
            info = parse_resource(resource)
            for binding in result.policy.bindings:
                role = binding.role
                for member in binding.members:
                    mtype, email, deleted = split_member(member)
                    records.append({
                        "id": binding_id(member, resource, role),
                        "member": member,
                        "member_type": mtype,
                        "email": email,
                        "deleted": deleted,
                        "resource": resource,
                        "asset_type": asset_type,
                        "project": info["project"],
                        "resource_short": info["short"],
                        "role": role,
                    })

    _auth_guard(_iter)
    # De-dupe on id, keep stable ordering.
    seen = set()
    unique = []
    for r in sorted(records, key=lambda x: (x["email"], x["resource"], x["role"])):
        if r["id"] in seen:
            continue
        seen.add(r["id"])
        unique.append(r)
    return unique


def refresh_bindings_cache(scope=SCOPE):
    _ensure_cache_dir()
    bindings = fetch_all_bindings(scope)
    payload = {"scope": scope, "generated_at": _now_iso(), "bindings": bindings}
    with _lock:
        with open(BINDINGS_CACHE, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
    return payload


def load_bindings_cache():
    if not os.path.exists(BINDINGS_CACHE):
        return {"scope": SCOPE, "generated_at": None, "bindings": []}
    with open(BINDINGS_CACHE, "r", encoding="utf-8") as f:
        return json.load(f)


def _patch_cache(record, remove=False):
    """Add or drop a single binding in the cache after a verified write."""
    with _lock:
        data = load_bindings_cache()
        bindings = data.get("bindings", [])
        bid = record["id"]
        bindings = [b for b in bindings if b["id"] != bid]
        if not remove:
            bindings.append(record)
        bindings.sort(key=lambda x: (x["email"], x["resource"], x["role"]))
        data["bindings"] = bindings
        data["generated_at"] = data.get("generated_at")
        with open(BINDINGS_CACHE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    return data


def revoke(member, resource, role, asset_type=""):
    res = apply_binding("remove", member, resource, asset_type, role)
    if res["ok"] and res["verified"] is not False:
        _patch_cache({"id": binding_id(member, resource, role)}, remove=True)
        res["cache_updated"] = True
    else:
        res["cache_updated"] = False
    return res


def grant(member, resource, role, asset_type=""):
    res = apply_binding("add", member, resource, asset_type, role)
    if res["ok"] and res["verified"] is not False:
        mtype, email, deleted = split_member(member)
        info = parse_resource(resource)
        record = {
            "id": binding_id(member, resource, role),
            "member": member,
            "member_type": mtype,
            "email": email,
            "deleted": deleted,
            "resource": resource,
            "asset_type": asset_type,
            "project": info["project"],
            "resource_short": info["short"],
            "role": role,
        }
        _patch_cache(record, remove=False)
        res["cache_updated"] = True
        res["record"] = record
    else:
        res["cache_updated"] = False
    return res


# --- Pickers: projects, roles, resource drill-down --------------------------
def _run_gcloud_json(args):
    result = subprocess.run(
        ["gcloud"] + args + ["--format=json"],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        err = result.stderr.strip()
        if _looks_like_auth_error(err):
            raise AuthError(err)
        raise RuntimeError(err or "gcloud command failed")
    return json.loads(result.stdout or "[]")


def list_all_projects():
    """Project IDs from Asset Inventory (same creds as the scan), unioned with
    whatever project IDs already appear in the bindings cache.
    """
    projects = set()

    def _iter():
        client = _asset_client()
        request = asset_v1.SearchAllResourcesRequest(
            scope=SCOPE,
            asset_types=["cloudresourcemanager.googleapis.com/Project"],
        )
        for r in client.search_all_resources(request=request):
            pid = None
            try:
                pid = r.additional_attributes["projectId"]
            except Exception:
                pid = None
            if not pid:
                pid = r.display_name or None
            if pid:
                projects.add(pid)

    _auth_guard(_iter)
    # Union with cache; skip bare numeric ids that come from sub-resource paths.
    data = load_bindings_cache()
    for b in data.get("bindings", []):
        p = b.get("project")
        if p and not str(p).isdigit():
            projects.add(p)
    return sorted(projects)


def refresh_projects_cache():
    _ensure_cache_dir()
    items = list_all_projects()
    payload = {"generated_at": _now_iso(), "projects": items}
    with open(PROJECTS_CACHE, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    return payload


def load_projects():
    if os.path.exists(PROJECTS_CACHE):
        with open(PROJECTS_CACHE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if data.get("projects"):
            return data
    return refresh_projects_cache()


def refresh_roles_cache():
    _ensure_cache_dir()
    roles = {}
    for r in _run_gcloud_json(["iam", "roles", "list"]):
        roles[r["name"]] = r.get("title", "")
    if ORG_ID:
        try:
            for r in _run_gcloud_json(["iam", "roles", "list", f"--organization={ORG_ID}"]):
                roles[r["name"]] = r.get("title", "")
        except RuntimeError:
            pass
    items = [{"name": n, "title": t} for n, t in sorted(roles.items())]
    payload = {"generated_at": _now_iso(), "roles": items}
    with open(ROLES_CACHE, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    return payload


def load_roles():
    if os.path.exists(ROLES_CACHE):
        with open(ROLES_CACHE, "r", encoding="utf-8") as f:
            return json.load(f)
    return refresh_roles_cache()


# Asset types the add flow can actually bind on, for the drill-down UI.
BINDABLE_ASSET_TYPES = {
    "cloudresourcemanager.googleapis.com/Project",
    "storage.googleapis.com/Bucket",
    "secretmanager.googleapis.com/Secret",
    "pubsub.googleapis.com/Topic",
    "pubsub.googleapis.com/Subscription",
    "artifactregistry.googleapis.com/Repository",
    "run.googleapis.com/Service",
}


def list_resource_types(project):
    """Group a project's resources by asset type, flagging bindable ones."""
    counts = {}

    def _iter():
        client = _asset_client()
        request = asset_v1.SearchAllResourcesRequest(scope=f"projects/{project}")
        for r in client.search_all_resources(request=request):
            counts[r.asset_type] = counts.get(r.asset_type, 0) + 1

    _auth_guard(_iter)
    types = [
        {"asset_type": t, "count": c, "bindable": t in BINDABLE_ASSET_TYPES}
        for t, c in sorted(counts.items())
    ]
    return types


def list_resources(project, asset_type):
    """List resources of a given asset type in a project for the picker."""
    out = []

    def _iter():
        client = _asset_client()
        request = asset_v1.SearchAllResourcesRequest(
            scope=f"projects/{project}", asset_types=[asset_type]
        )
        for r in client.search_all_resources(request=request):
            out.append({
                "name": r.name,
                "display_name": r.display_name or parse_resource(r.name)["short"],
                "asset_type": r.asset_type,
            })

    _auth_guard(_iter)
    out.sort(key=lambda x: x["display_name"])
    return out
