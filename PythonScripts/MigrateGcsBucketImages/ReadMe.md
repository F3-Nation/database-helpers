# Migrate GCS Bucket Images

This script migrates image URLs from database records into a destination GCS bucket.

Supported record targets:
- event_instances: reads meta.files[0]
- orgs: reads logo_url

Supported source URL types:
- GCS URLs (any bucket): copied server-side when accessible
- HTTP/S3 URLs: downloaded and uploaded to destination bucket

Destination naming:
- event_instances -> event_instance_images/<id>.<ext>
- orgs -> org-logos/<id>.<ext>

## Prerequisites

1. Python 3.7+
2. Install dependencies:

   pip install -r requirements.txt

3. Configure environment file in this folder:
- .env.staging for staging runs
- .env.prod for production runs

4. Authenticate Application Default Credentials for Google Cloud Storage access.

## Important Auth Note

If you see unexpected GCS access or billing-related behavior, run:

gcloud auth application-default login --no-launch-browser

This command sets up local Application Default Credentials so the Python GCS client can authenticate correctly.

## Environment Variables

Required database variables:
- PG_HOST
- PG_PORT
- PG_DBNAME
- PG_USER
- PG_PASSWORD

Bucket variables:
- NEW_BUCKET_NAME
- EVENT_INSTANCE_PREFIX
- ORG_LOGO_PREFIX

Optional extension behavior:
- EVENT_FORCE_EXT=source (default) keeps original extension when possible
- ORG_FORCE_EXT=source (default) keeps original extension for org logos
- DEFAULT_IMAGE_EXT=jpg (fallback when source URL has no extension)

Destination collision behavior:
- --if-exists overwrite (default): overwrite destination object if it already exists
- --if-exists skip: do not overwrite existing destination objects
- --if-exists error: treat an existing destination object as an error

Example files included:
- .env.example
- .env.staging
- .env.prod

## Run

Staging (default):

python main.py

Production:

python main.py prod

Migrate org logos:

python main.py prod --target orgs

Missing-only check (no copy), production:

python main.py prod --missing-only

Missing-only check for org logos:

python main.py prod --target orgs --missing-only

Write missing list to a specific CSV:

python main.py prod --missing-csv prod_missing.csv

Write old/new URL map to a specific CSV:

python main.py prod --map-csv prod_url_map.csv

Write run audit JSON to a specific filename:

python main.py prod --audit-json prod_audit.json

Do not overwrite destination files that already exist:

python main.py prod --if-exists skip

Use a persistent tracker file (JSONL) for resume support:

python main.py prod --tracker-file amazon_tracker.jsonl

Tracker behavior by default:

- If you do not pass --tracker-file, the script still creates one automatically in temp/ using the run timestamp.
- Tracker is created for every run mode, including --map-only.

Resume using a prior tracker file:

python main.py prod --tracker-file amazon_tracker.jsonl --resume-from-tracker

Map-only mode (no GCS calls, no copies):

python main.py prod --map-only

Map-only for org logos:

python main.py prod --target orgs --map-only

Tune concurrency for speed:

python main.py prod --workers 32

Control progress log frequency:

python main.py prod --progress-every 500

Adjust HTTP timeout for external URLs:

python main.py prod --http-timeout 30

Filter source URLs (substring style):

python main.py prod --target orgs --source-url-like /f3-logos/

Filter source URLs (SQL LIKE wildcard style):

python main.py prod --target orgs --source-url-like "%/f3-logos/%"

Use multiple source URL filters (OR behavior):

python main.py prod --target orgs --source-url-like "%/f3-logos/%" --source-url-like "%google%logos%"

Map-only with source URL filters:

python main.py prod --target orgs --map-only --source-url-like "%/f3-logos/%"

## What It Does

1. Validates required environment variables.
2. Selects candidate rows from Postgres for the chosen target.
3. Builds a URL mapping to the destination naming pattern.
4. Detects source type (GCS vs HTTP/S3) per row.
5. Copies each object to destination using source-type-appropriate flow.
6. Writes old/new URL mapping to CSV.
7. Writes missing source objects to CSV.
8. Appends per-row tracker records to JSONL (when not map-only).
9. Writes a run-level audit summary JSON.
10. Prints a migration summary:
- Copied
- Exists in source (missing-only mode)
- Missing source objects
- Skipped existing destination objects
- Forbidden
- Other GCS errors

Output files:
- All outputs are written under temp/ (gitignored).
- Default file naming includes one run timestamp + type + target.
- Example default names:
  - 20261002_173015_url_map_type_event_instances.csv
  - 20261002_173015_missing_source_objects_type_event_instances.csv
  - 20261002_173015_tracker_type_event_instances.jsonl
  - 20261002_173015_audit_type_event_instances.json
- All files from a single run share the same timestamp.

Tracker/resume behavior:
- Tracker format is JSONL (one JSON object per processed row).
- To resume after interruption, rerun with the same --tracker-file and add --resume-from-tracker.
- Resume skips rows whose status matches --resume-statuses.
- Default resume statuses: copied,exists,missing,skipped_existing,forbidden,error.
- You can override that list, for example:
  - --resume-statuses copied,exists,skipped_existing

Map-only mode:
- Builds and writes the URL map CSV only.
- Does not call Google Cloud Storage.
- Does not copy any objects.

Missing-only mode:
- Checks source availability only.
- Does not copy any objects.

## Performance Notes

For large runs (for example, 80k objects), speed depends heavily on source type and parallelism.

- Use --workers to increase concurrent operations (GCS copy or HTTP download/upload).
- Start with 16 or 32 workers and test.
- If you see throttling/errors increase, lower workers.
- Avoid very chatty logging; use --progress-every to print every few hundred rows instead of per-object.

Expected throughput guidance (very approximate):
- GCS->GCS copy can be quite fast because data movement is service-side.
- HTTP/S3 -> GCS is usually much slower because each object must be downloaded then uploaded.
- Small files are latency-bound; larger files are bandwidth-bound.
- Parallelism helps, but too much can increase timeouts/retries.

## Troubleshooting

Database env var error:
- Ensure all required PG_* values are set in the chosen env file.

403 Forbidden from GCS:
- Confirm your account has access to both buckets.
- Re-run Application Default Credentials login:

  gcloud auth application-default login --no-launch-browser

Billing disabled message from GCS:
- The source project billing/account state must be fixed by a GCP admin before object reads can succeed.

Object not found warnings:
- The URL may point to a key that no longer exists in the source bucket.

HTTP/S3 source errors:
- 401/403: source URL requires credentials your environment does not have.
- 404: source object does not exist at that URL.
- timeout/network errors: increase --http-timeout or retry with lower --workers.

## Post-Migration SQL Update

After files are copied, run the SQL below to update event_instances.meta.files[0] so the database points at the new bucket path.

This assumes:
- NEW_BUCKET_NAME = f3-public-images
- EVENT_INSTANCE_PREFIX = event_instance_images/
- files[0] contains the original URL

Preview rows that will change:

```sql
SELECT
  id,
  meta->'files'->>0 AS old_url,
  format(
    'https://storage.googleapis.com/%s/%s%s%s',
    'f3-public-images',
    'event_instance_images/',
    id,
    COALESCE(substring(split_part(meta->'files'->>0, '?', 1) from '\\.[A-Za-z0-9]+$'), '')
  ) AS new_url
FROM event_instances
WHERE meta->'files'->>0 LIKE '%/backblast-images/%'
LIMIT 100;
```

Apply update:

```sql
BEGIN;

UPDATE event_instances
SET meta = jsonb_set(
  meta,
  '{files,0}',
  to_jsonb(
    format(
      'https://storage.googleapis.com/%s/%s%s%s',
      'f3-public-images',
      'event_instance_images/',
      id,
      COALESCE(substring(split_part(meta->'files'->>0, '?', 1) from '\\.[A-Za-z0-9]+$'), '')
    )
  )
)
WHERE meta->'files'->>0 LIKE '%/backblast-images/%';

-- Inspect row count changed before commit
-- SELECT ROW_COUNT();

COMMIT;
```

Optional verification after update:

```sql
SELECT COUNT(*) AS remaining_old_urls
FROM event_instances
WHERE meta->'files'->>0 LIKE '%/backblast-images/%';
```
