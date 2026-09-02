# Postgres Security Manager

A local Flask app to visualize and edit Postgres security — roles/groups, role
membership, object privileges, default privileges, and row-level security —
across any libpq service and database you can reach. Sibling to the
`GcpUserAcceses` app; same look and feel, different data model.

## What's here

| File | Purpose |
| --- | --- |
| `app.py` | Flask server + JSON API (runs on port **5001**). |
| `pg_security.py` | Core library: connections, catalog introspection, JSON cache, GRANT/REVOKE with verification. Importable from CLI too. |
| `templates/index.html`, `static/` | The UI. |
| `cache/` | Generated per-`service__database` snapshots (gitignored). |

## Prerequisites

- Postgres reachable via a libpq **service** in `~/.pg_service.conf` (e.g. the
  `prod` and `staging` entries you already use with `psql service=prod`).
- If Postgres is behind the **Cloud SQL Auth Proxy**, start the proxy first.
- Your role needs privileges to read the catalogs and to run the GRANT/REVOKE
  statements you issue.

Optional startup defaults live in `.env` (copy from `.env.sample`):

```bash
cp .env.sample .env
#   PG_SERVICE=prod
#   PG_DBNAME=f3_prod
```

You can switch service and database from the top bar at any time.

## Run it

```bash
cd PythonScripts/DatabaseUserAccess
# activate the repo venv, then:
pip install -r requirements.txt
python app.py
# open http://127.0.0.1:5001
```

Pick a service + database and click **↻ Refresh snapshot** to introspect. Later
loads read instantly from the cache; refresh whenever you want fresh data.

## Using it

- **Roles & groups** — every role with its attribute badges (SUPERUSER, LOGIN /
  NOLOGIN, CREATEROLE, etc.). Expand a role to see the groups it belongs to and
  the members granted into it; revoke any membership inline. Filter by
  user/group and search.
- **Privileges** — the "who can do what" matrix as a collapsible tree. Drag the
  three chips (Grantee / Object / Privilege) to set the nesting order. Filter by
  grantee type (user / group / PUBLIC) and schema, search freely, and revoke any
  grant from its leaf.
- **Default privileges** — `ALTER DEFAULT PRIVILEGES` rules, per owner/schema.
- **Row-level security** — every policy with its roles, command, USING and WITH
  CHECK expressions.
- **+ Add access** — two modes:
  - *Role membership*: `GRANT <group> TO <member> [WITH ADMIN OPTION]`.
  - *Object privilege*: pick grantee, object type, schema → object, privilege,
    optional `WITH GRANT OPTION`.

Every grant/revoke runs the real SQL, re-queries the catalog to verify, and
updates just the affected cache entry. Actions apply immediately after a confirm
dialog.

## Notes / limits

- Privilege reads use `aclexplode`, so only **explicit** grants are shown
  (objects with default owner-only access don't appear until something is
  granted). Ownership is shown as an object attribute.
- Writes cover role membership and object privileges on tables, views,
  materialized views, sequences, functions, schemas, and databases. Column-level
  grants, `ALTER DEFAULT PRIVILEGES`, and role creation are not yet wired.
- Identifiers are quoted via `psycopg2.sql.Identifier`; privilege keywords are
  validated against an allow-list per object type.
- **Read-only roles are welcome.** You can browse and slice all security without
  any write permission. If a GRANT/REVOKE fails, a popup explains why (most often
  insufficient privileges) with the raw error underneath.
