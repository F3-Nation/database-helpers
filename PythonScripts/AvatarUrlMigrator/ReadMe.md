# Avatar URL Migrator

This script migrates `users.avatar_url` images to the central GCS bucket and updates the database URLs.

It is designed for safe, auditable execution with:

- domain whitelist filtering
- dry-run by default (database rollback)
- full audit artifacts per run
- generated rollback SQL
- deterministic output file naming (`user-avatars/<user_id>.jpg`)
- forced JPEG conversion for every migrated avatar

## What It Does

1. Connects to PostgreSQL using `.env.staging` or `.env.prod`.
2. Reads all users with non-empty `avatar_url`.
3. Skips users whose avatar domain is in your whitelist.
4. Downloads each non-whitelisted image.
5. Converts image data to JPEG.
6. Uploads to GCS at `user-avatars/<user_id>.jpg`.
7. Updates `users.avatar_url` to the new GCS URL.
8. Writes audit and rollback artifacts.

## Buckets and Paths

Default bucket mapping:

- `staging` -> `f3-public-images-staging`
- `prod` -> `f3-public-images`

Default object path:

- `user-avatars/<user_id>.jpg`

Default new URL format:

- `https://storage.googleapis.com/{bucket}/{object_path}`

You can override bucket, prefix, and URL template with CLI flags.

## Safety Model

### Default mode: dry-run

If you run without `--commit`, the script:

- uploads converted files to GCS
- performs DB updates inside a transaction
- rolls back DB changes at the end

This lets you validate migration impact and output artifacts before committing database updates.

### Commit mode

If you run with `--commit`, DB updates are committed.

### Rollback plan

Every run generates `rollback.sql` with guarded updates:

- sets each user back to their original `avatar_url`
- only updates rows where current value still matches migrated URL

This prevents accidental overwrite if a row changed after migration.

The rollback SQL does not automatically delete GCS objects, but includes commented `gsutil rm` lines to clean up uploaded files when needed.

## Audit Trail Artifacts

Each run creates a timestamped output directory:

- `migration.log` - detailed progress and summary
- `audit.csv` - one row per candidate user with status and metadata
- `summary.json` - machine-readable run totals and artifact paths
- `rollback.sql` - SQL rollback script for DB URL restoration

Example output directory:

```bash
PythonScripts/AvatarUrlMigrator/output/avatar_migration_staging_20260512_184500/
```

## Prerequisites

## 1. Python

- Python 3.10+ recommended

## 2. Cloud SQL connectivity

You need DB access and Cloud SQL Auth Proxy configured (see repo root `README.md`).

## 3. GCP auth for storage

Authenticate with GCP credentials that have permission to upload objects to target bucket(s):

```bash
gcloud auth application-default login
```

Or use a service account key:

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/absolute/path/to/key.json
```

Minimum IAM role on the bucket/project: `Storage Object Admin` (or custom role with object create/update rights).

Project resolution order for Storage client:

1. `--gcp_project`
2. `GOOGLE_CLOUD_PROJECT` (or `GCLOUD_PROJECT`) env var
3. active `gcloud` project (`gcloud config get-value project`)

## 4. DB credentials

Create env files in this folder:

- `.env.staging`
- `.env.prod`

You can start from:

```bash
cp .env.example .env.staging
cp .env.example .env.prod
```

Then edit values for each environment.

## Dev Environment Setup (Thorough)

From repo root:

```bash
cd PythonScripts/AvatarUrlMigrator
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

Validate imports quickly:

```bash
python -c "import psycopg2, requests, PIL, google.cloud.storage, dotenv; print('ok')"
```

## Script Arguments

```bash
python migrate_user_avatars.py [options]
```

Options:

- `--environment {staging,prod}`: target DB + default bucket
- `--env_file PATH`: explicit env file path (default `.env.<environment>`)
- `--commit`: commit DB updates (default is rollback)
- `--bucket BUCKET`: override destination bucket
- `--gcs_prefix PREFIX`: destination folder (default `user-avatars`)
- `--public_url_template TEMPLATE`: supports `{bucket}` and `{object_path}`
- `--whitelist_domains DOMAINS`: comma-separated domains to skip
- `--whitelist_file PATH`: text file with one domain per line (or comma-separated per line)
- `--include_url_prefixes PREFIXES`: comma-separated URL prefixes to include
- `--skip_url_prefixes PREFIXES`: comma-separated URL prefixes to skip
- `--include_prefix_file PATH`: text file with URL prefixes to include
- `--skip_prefix_file PATH`: text file with URL prefixes to skip
- `--limit N`: process at most N users
- `--http_timeout_seconds N`: HTTP timeout per download
- `--gcp_project PROJECT`: optional storage client project override
- `--output_dir PATH`: where audit artifacts are written

## Whitelist Domain Behavior

Whitelist matching is hostname-based and supports exact or subdomain matches.

You can store domain skip rules in a text file in this folder named `whitelist_domains.txt`.
If that file exists, it is auto-loaded.
By default, the provided template file has no active domains.

Accepted file format:

- one domain per line, or comma-separated values per line
- blank lines are ignored
- lines starting with `#` are treated as comments

Example:

- whitelist contains `example.com`
- `example.com` is skipped
- `cdn.example.com` is skipped

Example file:

```txt
# Existing central image hosts
storage.googleapis.com
f3-public-images.storage.googleapis.com
f3-public-images-staging.storage.googleapis.com

# Legacy hosts to skip
images.region-one.org
avatars.region-two.org, cdn.region-three.net
```

If whitelist is empty, all non-empty avatar URLs are considered migration candidates.

Whitelist precedence and combination behavior:

- If `--whitelist_file` is passed, that file is loaded.
- Else, if `whitelist_domains.txt` exists in this folder, it is auto-loaded.
- `--whitelist_domains` values are merged in.

## URL Prefix Include/Skip Rules

Use URL prefixes when domain matching is not specific enough.

Examples:

- include: `https://storage.googleapis.com/backblast-images/`
- skip: `https://storage.googleapis.com/f3-public-images/user-avatars/`

The script evaluates in this order:

1. Include prefixes: if include list is non-empty, URL must match one include prefix.
2. Skip prefixes: if URL matches any skip prefix, it is skipped.
3. Domain whitelist: if domain is whitelisted, it is skipped.

Prefix sources and defaults:

- Include prefixes auto-load from `include_url_prefixes.txt` if present.
- Skip prefixes auto-load from `skip_url_prefixes.txt` if present.
- `--include_prefix_file` and `--skip_prefix_file` override auto file paths.
- CLI values from `--include_url_prefixes` and `--skip_url_prefixes` are merged with file values.

## Recommended Execution Workflow

## Phase 1: small dry-run sample

```bash
python migrate_user_avatars.py \
  --environment staging \
  --include_prefix_file include_url_prefixes.txt \
  --skip_prefix_file skip_url_prefixes.txt \
  --limit 25
```

Review output files in the generated run folder.

## Phase 2: full dry-run

```bash
python migrate_user_avatars.py \
  --environment staging \
  --include_prefix_file include_url_prefixes.txt \
  --skip_prefix_file skip_url_prefixes.txt
```

Validate `audit.csv` and failure reasons.

## Phase 3: staging commit

```bash
python migrate_user_avatars.py \
  --environment staging \
  --commit \
  --include_prefix_file include_url_prefixes.txt \
  --skip_prefix_file skip_url_prefixes.txt
```

## Phase 4: prod dry-run, then prod commit

Dry-run:

```bash
python migrate_user_avatars.py \
  --environment prod \
  --include_prefix_file include_url_prefixes.txt \
  --skip_prefix_file skip_url_prefixes.txt
```

Commit:

```bash
python migrate_user_avatars.py \
  --environment prod \
  --commit \
  --include_prefix_file include_url_prefixes.txt \
  --skip_prefix_file skip_url_prefixes.txt
```

Example for your specific case:

```bash
python migrate_user_avatars.py \
  --environment prod \
  --include_url_prefixes https://storage.googleapis.com/backblast-images/ \
  --skip_url_prefixes https://storage.googleapis.com/f3-public-images/user-avatars/
```

## Rollback Procedure

If you need to revert database URLs for a run:

```bash
psql service=staging -f /path/to/run/rollback.sql
```

For production:

```bash
psql service=prod -f /path/to/run/rollback.sql
```

Optional object cleanup:

1. Inspect commented `gsutil rm` lines inside `rollback.sql`
2. Remove comments and run manually after confirming rollback correctness

## Operational Notes

- Uploaded object names are deterministic (`<user_id>.jpg`), so reruns overwrite prior file for that user.
- PNG/WebP/GIF and other supported image formats are converted to JPEG.
- Transparent images are flattened onto a white background.
- Script skips only based on source domain whitelist, not destination path.
- Failed rows never update DB for that row.
- A single failed row does not stop the run.

## Troubleshooting

### `403` uploading to GCS

Your GCP principal lacks bucket object permissions.

### `Project was not passed and could not be determined`

Set a project using one of these options:

```bash
gcloud config set project YOUR_PROJECT_ID
```

or

```bash
export GOOGLE_CLOUD_PROJECT=YOUR_PROJECT_ID
```

or

```bash
python migrate_user_avatars.py --gcp_project YOUR_PROJECT_ID
```

### `could not translate host name` or connection errors

Cloud SQL Auth Proxy or DB env variables are not set correctly.

### many `failed_download` rows

Source URLs may be expired, blocked, or invalid. See `detail` column in `audit.csv`.

### image conversion failures

Corrupt or unsupported source bytes can trigger `failed_process`.

## Suggested Post-Run Verification SQL

Compare counts of avatar domains before/after run:

```sql
SELECT split_part(replace(replace(avatar_url, 'https://', ''), 'http://', ''), '/', 1) AS domain,
       COUNT(*)
FROM users
WHERE avatar_url IS NOT NULL AND btrim(avatar_url) <> ''
GROUP BY 1
ORDER BY COUNT(*) DESC;
```

Spot-check migrated URLs:

```sql
SELECT id, avatar_url
FROM users
WHERE avatar_url LIKE 'https://storage.googleapis.com/f3-public-images%/user-avatars/%'
ORDER BY id
LIMIT 100;
```
