"""Core Postgres security helpers for the DatabaseUserAccess Flask app.

Connects through libpq services defined in ~/.pg_service.conf (fronted by the
Cloud SQL Auth Proxy), introspects roles / memberships / privileges / default
privileges / RLS into a JSON cache, and applies GRANT/REVOKE changes, verifying
each one by re-querying the catalog and patching the single affected cache entry.
"""

import configparser
import json
import os
import re
import threading
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
from psycopg2 import sql

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

try:
    from dotenv import load_dotenv

    load_dotenv(os.path.join(BASE_DIR, ".env"))
except ImportError:
    pass

# Optional defaults for the initial connection (the UI can switch freely).
DEFAULT_SERVICE = os.environ.get("PG_SERVICE", "prod")
DEFAULT_DBNAME = os.environ.get("PG_DBNAME", "f3_prod")
SERVICE_FILE = os.environ.get("PGSERVICEFILE") or os.path.expanduser("~/.pg_service.conf")

CACHE_DIR = os.path.join(BASE_DIR, "cache")
_lock = threading.Lock()

# Privileges that are valid per object type, used to validate write requests.
PRIVILEGES_BY_TYPE = {
    "table": ["SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER"],
    "view": ["SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER"],
    "matview": ["SELECT"],
    "foreign table": ["SELECT", "INSERT", "UPDATE", "DELETE", "REFERENCES", "TRIGGER"],
    "sequence": ["USAGE", "SELECT", "UPDATE"],
    "schema": ["USAGE", "CREATE"],
    "database": ["CONNECT", "TEMPORARY", "CREATE"],
    "function": ["EXECUTE"],
}

# Object type -> the keyword used in GRANT ... ON <keyword> ...
_ON_KEYWORD = {
    "table": "TABLE",
    "view": "TABLE",
    "matview": "TABLE",
    "foreign table": "TABLE",
    "sequence": "SEQUENCE",
    "schema": "SCHEMA",
    "database": "DATABASE",
    "function": "FUNCTION",
}


class ConnError(Exception):
    """Raised when we cannot reach Postgres (proxy down / bad service / auth)."""


class InputError(Exception):
    """Raised for invalid write requests (bad privilege, unknown object, etc.)."""


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _ensure_cache_dir():
    os.makedirs(CACHE_DIR, exist_ok=True)


def _cache_path(service, dbname):
    safe = re.sub(r"[^A-Za-z0-9_.-]", "_", f"{service}__{dbname}")
    return os.path.join(CACHE_DIR, f"{safe}.json")


# --- Connections ------------------------------------------------------------
def list_services():
    """Service names from the libpq service file."""
    if not os.path.exists(SERVICE_FILE):
        return []
    parser = configparser.ConfigParser()
    try:
        parser.read(SERVICE_FILE)
    except configparser.Error:
        return []
    return sorted(parser.sections())


def _connect(service, dbname=None):
    kwargs = {"service": service}
    if dbname:
        kwargs["dbname"] = dbname
    try:
        conn = psycopg2.connect(**kwargs)
        conn.autocommit = True
        return conn
    except psycopg2.OperationalError as e:
        raise ConnError(
            f"Could not connect to service '{service}'"
            f"{f' / db {dbname}' if dbname else ''}. "
            f"Is the Cloud SQL Auth Proxy running and is the service in "
            f"{SERVICE_FILE}?\n\n{e}"
        ) from e


def _query(service, dbname, query, params=None):
    conn = _connect(service, dbname)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if params:
                cur.execute(query, params)
            else:
                cur.execute(query)
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def list_databases(service):
    rows = _query(
        service,
        None,
        """
        SELECT datname FROM pg_database
        WHERE datallowconn AND NOT datistemplate
        ORDER BY datname
        """,
    )
    return [r["datname"] for r in rows]


# --- Introspection ----------------------------------------------------------
_ROLES_SQL = """
    SELECT rolname,
           rolsuper, rolinherit, rolcreaterole, rolcreatedb,
           rolcanlogin, rolreplication, rolbypassrls,
           rolconnlimit,
           rolvaliduntil::text AS rolvaliduntil,
           CASE WHEN rolpassword IS NULL THEN false ELSE true END AS has_password
    FROM pg_roles
    WHERE rolname NOT LIKE 'pg\\_%'
    ORDER BY rolname
"""

_MEMBERSHIPS_SQL = """
    SELECT g.rolname AS group_role,
           m.rolname AS member_role,
           pg_get_userbyid(am.grantor) AS grantor,
           am.admin_option
    FROM pg_auth_members am
    JOIN pg_roles g ON g.oid = am.roleid
    JOIN pg_roles m ON m.oid = am.member
    ORDER BY g.rolname, m.rolname
"""

# aclexplode-based privilege extraction, unioned across object kinds.
_PRIV_RELATIONS_SQL = """
    SELECT n.nspname AS schema,
           c.relname AS object,
           CASE c.relkind
                WHEN 'r' THEN 'table' WHEN 'p' THEN 'table'
                WHEN 'v' THEN 'view'  WHEN 'm' THEN 'matview'
                WHEN 'S' THEN 'sequence' WHEN 'f' THEN 'foreign table'
                ELSE c.relkind::text END AS object_type,
           pg_get_userbyid(c.relowner) AS owner,
           (SELECT rolname FROM pg_roles WHERE oid = a.grantee) AS grantee,
           a.privilege_type AS privilege,
           a.is_grantable AS grantable,
           pg_get_userbyid(a.grantor) AS grantor,
           NULL::text AS args
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    CROSS JOIN LATERAL aclexplode(c.relacl) a
    WHERE n.nspname NOT LIKE 'pg\\_%' AND n.nspname <> 'information_schema'
      AND c.relkind IN ('r','p','v','m','S','f')
"""

_PRIV_SCHEMAS_SQL = """
    SELECT n.nspname AS schema,
           NULL::text AS object,
           'schema' AS object_type,
           pg_get_userbyid(n.nspowner) AS owner,
           (SELECT rolname FROM pg_roles WHERE oid = a.grantee) AS grantee,
           a.privilege_type AS privilege,
           a.is_grantable AS grantable,
           pg_get_userbyid(a.grantor) AS grantor,
           NULL::text AS args
    FROM pg_namespace n
    CROSS JOIN LATERAL aclexplode(n.nspacl) a
    WHERE n.nspname NOT LIKE 'pg\\_%' AND n.nspname <> 'information_schema'
"""

_PRIV_DATABASE_SQL = """
    SELECT NULL::text AS schema,
           d.datname AS object,
           'database' AS object_type,
           pg_get_userbyid(d.datdba) AS owner,
           (SELECT rolname FROM pg_roles WHERE oid = a.grantee) AS grantee,
           a.privilege_type AS privilege,
           a.is_grantable AS grantable,
           pg_get_userbyid(a.grantor) AS grantor,
           NULL::text AS args
    FROM pg_database d
    CROSS JOIN LATERAL aclexplode(d.datacl) a
    WHERE d.datname = current_database()
"""

_PRIV_FUNCTIONS_SQL = """
    SELECT n.nspname AS schema,
           p.proname AS object,
           'function' AS object_type,
           pg_get_userbyid(p.proowner) AS owner,
           (SELECT rolname FROM pg_roles WHERE oid = a.grantee) AS grantee,
           a.privilege_type AS privilege,
           a.is_grantable AS grantable,
           pg_get_userbyid(a.grantor) AS grantor,
           pg_get_function_identity_arguments(p.oid) AS args
    FROM pg_proc p
    JOIN pg_namespace n ON n.oid = p.pronamespace
    CROSS JOIN LATERAL aclexplode(p.proacl) a
    WHERE n.nspname NOT LIKE 'pg\\_%' AND n.nspname <> 'information_schema'
"""

_DEFAULT_PRIV_SQL = """
    SELECT pg_get_userbyid(d.defaclrole) AS owner,
           COALESCE(n.nspname, '*') AS schema,
           CASE d.defaclobjtype
                WHEN 'r' THEN 'table' WHEN 'S' THEN 'sequence'
                WHEN 'f' THEN 'function' WHEN 'T' THEN 'type'
                WHEN 'n' THEN 'schema' ELSE d.defaclobjtype::text END AS object_type,
           (SELECT rolname FROM pg_roles WHERE oid = a.grantee) AS grantee,
           a.privilege_type AS privilege,
           a.is_grantable AS grantable
    FROM pg_default_acl d
    LEFT JOIN pg_namespace n ON n.oid = d.defaclnamespace
    CROSS JOIN LATERAL aclexplode(d.defaclacl) a
    ORDER BY owner, schema, object_type
"""

_RLS_SQL = """
    SELECT schemaname AS schema, tablename AS table, policyname AS policy,
           permissive, roles, cmd, qual, with_check
    FROM pg_policies
    WHERE schemaname NOT LIKE 'pg\\_%' AND schemaname <> 'information_schema'
    ORDER BY schemaname, tablename, policyname
"""


def _classify_grantee(name, roles_by_name):
    if not name:
        return "PUBLIC"
    r = roles_by_name.get(name)
    if r is None:
        return "other"
    return "user" if r.get("rolcanlogin") else "group"


def _object_label(rec):
    ot = rec["object_type"]
    if ot == "schema":
        return rec["schema"]
    if ot == "database":
        return rec["object"]
    if ot == "function":
        return f"{rec['schema']}.{rec['object']}({rec.get('args') or ''})"
    return f"{rec['schema']}.{rec['object']}"


def fetch_snapshot(service, dbname):
    """Introspect everything for one service+database into a dict."""
    roles = _query(service, dbname, _ROLES_SQL)
    roles_by_name = {r["rolname"]: r for r in roles}
    memberships = _query(service, dbname, _MEMBERSHIPS_SQL)

    priv_rows = []
    for q in (_PRIV_RELATIONS_SQL, _PRIV_SCHEMAS_SQL, _PRIV_DATABASE_SQL, _PRIV_FUNCTIONS_SQL):
        priv_rows.extend(_query(service, dbname, q))

    privileges = []
    for r in priv_rows:
        grantee = r["grantee"]  # None => PUBLIC
        rec = {
            "grantee": grantee or "PUBLIC",
            "grantee_type": _classify_grantee(grantee, roles_by_name),
            "is_public": grantee is None,
            "schema": r["schema"],
            "object": r["object"],
            "object_type": r["object_type"],
            "object_label": _object_label(r),
            "owner": r["owner"],
            "privilege": r["privilege"],
            "grantable": r["grantable"],
            "grantor": r["grantor"],
            "args": r.get("args"),
        }
        rec["id"] = "|".join([
            rec["grantee"], rec["object_type"], rec["object_label"], rec["privilege"]
        ])
        privileges.append(rec)
    privileges.sort(key=lambda x: (x["grantee"], x["object_label"], x["privilege"]))

    defaults = _query(service, dbname, _DEFAULT_PRIV_SQL)
    for d in defaults:
        d["grantee"] = d["grantee"] or "PUBLIC"
    rls = _query(service, dbname, _RLS_SQL)

    return {
        "service": service,
        "dbname": dbname,
        "generated_at": _now_iso(),
        "roles": roles,
        "memberships": memberships,
        "privileges": privileges,
        "default_privileges": defaults,
        "rls_policies": rls,
    }


def refresh_snapshot(service, dbname):
    _ensure_cache_dir()
    snap = fetch_snapshot(service, dbname)
    with _lock:
        with open(_cache_path(service, dbname), "w", encoding="utf-8") as f:
            json.dump(snap, f, indent=2, default=str)
    return snap


def load_snapshot(service, dbname):
    path = _cache_path(service, dbname)
    if not os.path.exists(path):
        return {
            "service": service, "dbname": dbname, "generated_at": None,
            "roles": [], "memberships": [], "privileges": [],
            "default_privileges": [], "rls_policies": [],
        }
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _patch_privilege(service, dbname, record, remove=False):
    with _lock:
        snap = load_snapshot(service, dbname)
        rows = [p for p in snap.get("privileges", []) if p["id"] != record["id"]]
        if not remove:
            rows.append(record)
        rows.sort(key=lambda x: (x["grantee"], x["object_label"], x["privilege"]))
        snap["privileges"] = rows
        with open(_cache_path(service, dbname), "w", encoding="utf-8") as f:
            json.dump(snap, f, indent=2, default=str)


def _patch_membership(service, dbname, group_role, member_role, admin=False, remove=False):
    with _lock:
        snap = load_snapshot(service, dbname)
        rows = [
            m for m in snap.get("memberships", [])
            if not (m["group_role"] == group_role and m["member_role"] == member_role)
        ]
        if not remove:
            rows.append({
                "group_role": group_role, "member_role": member_role,
                "grantor": None, "admin_option": admin,
            })
        rows.sort(key=lambda x: (x["group_role"], x["member_role"]))
        snap["memberships"] = rows
        with open(_cache_path(service, dbname), "w", encoding="utf-8") as f:
            json.dump(snap, f, indent=2, default=str)


# --- Writes -----------------------------------------------------------------
def _object_target(object_type, schema, object_name, args):
    """Build the composed SQL for the GRANT/REVOKE object target."""
    if object_type in ("table", "view", "matview", "foreign table"):
        return sql.SQL("TABLE {}").format(sql.Identifier(schema, object_name))
    if object_type == "sequence":
        return sql.SQL("SEQUENCE {}").format(sql.Identifier(schema, object_name))
    if object_type == "schema":
        return sql.SQL("SCHEMA {}").format(sql.Identifier(schema))
    if object_type == "database":
        return sql.SQL("DATABASE {}").format(sql.Identifier(object_name))
    if object_type == "function":
        # args come straight from pg_get_function_identity_arguments (trusted catalog text).
        ident = sql.Identifier(schema, object_name)
        return sql.SQL("FUNCTION {}({})").format(ident, sql.SQL(args or ""))
    raise InputError(f"Unsupported object type: {object_type}")


def _verify_privilege(service, dbname, object_type, schema, object_name, args, grantee, privilege):
    """Re-query the catalog to confirm a grant exists (or not)."""
    snap_rows = []
    for q in (_PRIV_RELATIONS_SQL, _PRIV_SCHEMAS_SQL, _PRIV_DATABASE_SQL, _PRIV_FUNCTIONS_SQL):
        snap_rows.extend(_query(service, dbname, q))
    target_label = _object_label({
        "object_type": object_type, "schema": schema, "object": object_name, "args": args,
    })
    for r in snap_rows:
        g = r["grantee"] or "PUBLIC"
        if (
            g == grantee
            and r["object_type"] == object_type
            and r["privilege"] == privilege
            and _object_label(r) == target_label
        ):
            return True
    return False


def grant_privilege(service, dbname, object_type, schema, object_name, args,
                    grantee, privilege, grant_option=False):
    privilege = privilege.upper()
    allowed = PRIVILEGES_BY_TYPE.get(object_type, [])
    if privilege not in allowed:
        raise InputError(f"'{privilege}' is not valid for {object_type} (allowed: {allowed}).")

    target = _object_target(object_type, schema, object_name, args)
    grantee_sql = sql.SQL("PUBLIC") if grantee == "PUBLIC" else sql.Identifier(grantee)
    stmt = sql.SQL("GRANT {priv} ON {target} TO {grantee}{opt}").format(
        priv=sql.SQL(privilege),
        target=target,
        grantee=grantee_sql,
        opt=sql.SQL(" WITH GRANT OPTION") if grant_option else sql.SQL(""),
    )
    return _apply_write(service, dbname, stmt, verify=lambda: _verify_privilege(
        service, dbname, object_type, schema, object_name, args, grantee, privilege
    ), on_ok=lambda: _patch_privilege(service, dbname, _new_priv_record(
        service, dbname, object_type, schema, object_name, args, grantee, privilege, grant_option
    )))


def revoke_privilege(service, dbname, object_type, schema, object_name, args,
                     grantee, privilege):
    privilege = privilege.upper()
    target = _object_target(object_type, schema, object_name, args)
    grantee_sql = sql.SQL("PUBLIC") if grantee == "PUBLIC" else sql.Identifier(grantee)
    stmt = sql.SQL("REVOKE {priv} ON {target} FROM {grantee}").format(
        priv=sql.SQL(privilege), target=target, grantee=grantee_sql,
    )
    rec_id = "|".join([
        grantee, object_type,
        _object_label({"object_type": object_type, "schema": schema,
                       "object": object_name, "args": args}),
        privilege,
    ])
    return _apply_write(
        service, dbname, stmt,
        verify=lambda: not _verify_privilege(
            service, dbname, object_type, schema, object_name, args, grantee, privilege
        ),
        on_ok=lambda: _patch_privilege(service, dbname, {"id": rec_id}, remove=True),
    )


def _new_priv_record(service, dbname, object_type, schema, object_name, args,
                     grantee, privilege, grant_option):
    roles = load_snapshot(service, dbname).get("roles", [])
    roles_by_name = {r["rolname"]: r for r in roles}
    rec = {
        "grantee": grantee,
        "grantee_type": _classify_grantee(None if grantee == "PUBLIC" else grantee, roles_by_name),
        "is_public": grantee == "PUBLIC",
        "schema": schema, "object": object_name, "object_type": object_type,
        "object_label": _object_label({"object_type": object_type, "schema": schema,
                                        "object": object_name, "args": args}),
        "owner": None, "privilege": privilege, "grantable": grant_option,
        "grantor": None, "args": args,
    }
    rec["id"] = "|".join([grantee, object_type, rec["object_label"], privilege])
    return rec


def _verify_membership(service, dbname, group_role, member_role):
    rows = _query(service, dbname, _MEMBERSHIPS_SQL)
    return any(
        r["group_role"] == group_role and r["member_role"] == member_role for r in rows
    )


def grant_membership(service, dbname, group_role, member_role, admin_option=False):
    stmt = sql.SQL("GRANT {g} TO {m}{opt}").format(
        g=sql.Identifier(group_role), m=sql.Identifier(member_role),
        opt=sql.SQL(" WITH ADMIN OPTION") if admin_option else sql.SQL(""),
    )
    return _apply_write(
        service, dbname, stmt,
        verify=lambda: _verify_membership(service, dbname, group_role, member_role),
        on_ok=lambda: _patch_membership(service, dbname, group_role, member_role, admin_option),
    )


def revoke_membership(service, dbname, group_role, member_role):
    stmt = sql.SQL("REVOKE {g} FROM {m}").format(
        g=sql.Identifier(group_role), m=sql.Identifier(member_role),
    )
    return _apply_write(
        service, dbname, stmt,
        verify=lambda: not _verify_membership(service, dbname, group_role, member_role),
        on_ok=lambda: _patch_membership(service, dbname, group_role, member_role, remove=True),
    )


def _classify_sql_error(text):
    """Turn a psycopg2 failure into (reason, human message) for the UI popup."""
    t = (text or "").lower()
    if any(h in t for h in (
        "permission denied", "must be owner", "must have admin option",
        "must be superuser", "must be member", "is not allowed",
        "insufficient privilege",
    )):
        return ("permission_denied",
                "Your database role doesn't have the rights to make this change. "
                "Read-only users can browse all security here but can't "
                "GRANT/REVOKE. To change object privileges you must own the object "
                "(or be a superuser); to change role membership you need ADMIN "
                "OPTION on the group (or be a superuser).")
    if "does not exist" in t:
        return ("not_found",
                "The referenced role or object doesn't exist. It may have been "
                "renamed or dropped — refresh the snapshot and try again.")
    return ("error", "The statement failed. See the details below.")


def _apply_write(service, dbname, stmt, verify, on_ok):
    conn = _connect(service, dbname)
    try:
        rendered = stmt.as_string(conn)
        with conn.cursor() as cur:
            cur.execute(stmt)
    except psycopg2.Error as e:
        err = str(e).strip()
        reason, reason_message = _classify_sql_error(err)
        return {"ok": False, "verified": None, "command": _safe_render(stmt, conn),
                "error": err, "reason": reason, "reason_message": reason_message}
    finally:
        conn.close()

    verified = None
    try:
        verified = verify()
    except Exception:  # verification failure shouldn't mask a successful write
        verified = None
    if verified is not False:
        on_ok()
    return {"ok": True, "verified": verified, "command": rendered, "error": None,
            "reason": None, "reason_message": None}


def _safe_render(stmt, conn):
    try:
        return stmt.as_string(conn)
    except Exception:
        return "<unrenderable statement>"


# --- Picker helpers ---------------------------------------------------------
def list_schemas(service, dbname):
    rows = _query(
        service, dbname,
        """SELECT nspname FROM pg_namespace
           WHERE nspname NOT LIKE 'pg\\_%' AND nspname <> 'information_schema'
           ORDER BY nspname""",
    )
    return [r["nspname"] for r in rows]


def list_objects(service, dbname, schema, object_type):
    """Objects of a type in a schema, for the grant picker."""
    if object_type in ("table", "view", "matview", "sequence", "foreign table"):
        kinds = {
            "table": ("r", "p"), "view": ("v",), "matview": ("m",),
            "sequence": ("S",), "foreign table": ("f",),
        }[object_type]
        rows = _query(
            service, dbname,
            """SELECT c.relname AS name, NULL::text AS args
               FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
               WHERE n.nspname = %s AND c.relkind = ANY(%s)
               ORDER BY c.relname""",
            (schema, list(kinds)),
        )
    elif object_type == "function":
        rows = _query(
            service, dbname,
            """SELECT p.proname AS name,
                      pg_get_function_identity_arguments(p.oid) AS args
               FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
               WHERE n.nspname = %s ORDER BY p.proname""",
            (schema,),
        )
    else:
        rows = []
    return rows
