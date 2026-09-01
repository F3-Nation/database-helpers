# GCP IAM Access Manager

A local Flask app to inspect **all** IAM access across your org and every
project/resource, slice it however you want, and add or revoke bindings with a
click. It runs entirely on your machine as your logged-in `gcloud` user — no
service to host, no credentials to store.

## What's here

| File | Purpose |
| --- | --- |
| `app.py` | Flask server + JSON API. |
| `gcp_iam.py` | Core library: Asset Inventory reads, JSON cache, gcloud grant/revoke with verification. Importable from CLI too. |
| `templates/index.html`, `static/` | The UI. |
| `gcp_user_access_report.py` | Original CLI exporter (flat `User_Email,Resource,Role` CSV). Still works. |
| `revoke_gcp_iam.py` | Original CSV-driven bulk revoke. Still works. |
| `cache/` | Generated JSON cache (gitignored). |

## Prerequisites

- `gcloud` CLI installed and authenticated as yourself: `gcloud auth login`
  and `gcloud auth application-default login`.
- Your account needs permission to read IAM policies (Asset Inventory) and to
  change bindings on the resources you edit.
- Cloud Asset API enabled on the quota project.

Config is loaded from a local `.env` file (or the environment). Copy the
sample and edit it:

```bash
cp .env.sample .env
# then edit .env:
#   GCP_SCOPE=organizations/803404482088   (or folders/… or projects/…)
#   GCP_QUOTA_PROJECT=f3-workspace
```

`.env` is gitignored; `.env.sample` is the tracked template.

## Run it
Ensure the repo's virtual environment is active.

```bash
cd PythonScripts/GcpUserAcceses
pip install -r requirements.txt
python app.py
# open http://127.0.0.1:5000
```

First launch shows an empty tree — click **↻ Refresh all** to pull every IAM
binding under your scope into the local cache (this also refreshes the project
and role picker caches). Later launches load instantly from the cache; refresh
whenever you want fresh data.

## Using the dashboard

- **Group by** — drag the three chips (User / Resource / Role) into any order.
  The tree nests in exactly that order, e.g. `Resource ▸ Role ▸ User`. Counts
  on every node.
- **Member type** — untick `serviceAccount` to hide service accounts, etc.
- **Search** — filters by email, resource, or role substring.
- **Revoke** — the button on any leaf runs `gcloud remove-iam-policy-binding`,
  verifies the member is gone from the returned policy, and drops that one entry
  from the cache (no full re-scan needed).
- **+ Add access** — pick a member type + email, drill a project → resource
  type → resource (or **Use project** to bind at the project level, or paste a
  full resource name), pick a role from the full predefined + custom list, and
  Grant. On success the new binding is verified and added to the cache.

## Notes / limits

- Grant/revoke are supported on: projects, folders, organization, GCS buckets,
  Secret Manager secrets, Pub/Sub topics & subscriptions, Artifact Registry
  repositories, and Cloud Run services. Other resource types (e.g. BigQuery
  datasets, Dataform repos) are reported as unsupported — those bindings are
  usually inherited and best changed at a parent resource. Add more mappings in
  `build_iam_command()` in `gcp_iam.py`.
- Actions apply immediately (with a confirm dialog); there is no dry-run toggle.
