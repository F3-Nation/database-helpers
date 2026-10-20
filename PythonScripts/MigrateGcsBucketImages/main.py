import argparse
import csv
import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from urllib.parse import urlparse
from google.cloud import storage
import psycopg2
from dotenv import load_dotenv
from google.api_core.exceptions import Forbidden, NotFound, GoogleAPICallError

# Parse command line arguments
def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Migrate image URLs from DB records into a public GCS bucket."
    )
    parser.add_argument(
        "env",
        nargs="?",
        choices=["staging", "prod"],
        default="staging",
        help="Environment to use (.env.staging or .env.prod). Default: staging",
    )
    parser.add_argument(
        "--target",
        choices=["event_instances", "orgs"],
        default="event_instances",
        help="Which DB records to process (default: event_instances).",
    )
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--missing-only",
        action="store_true",
        help="Only check for missing source objects; do not copy files.",
    )
    mode_group.add_argument(
        "--map-only",
        action="store_true",
        help="Only generate old/new URL mapping CSV; do not call GCS.",
    )
    parser.add_argument(
        "--missing-csv",
        default=None,
        help="Filename for missing object rows under temp/ (default is timestamped).",
    )
    parser.add_argument(
        "--map-csv",
        default=None,
        help="Filename for URL mapping rows under temp/ (default is timestamped).",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=16,
        help="Number of concurrent workers for GCS operations (default: 16).",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=250,
        help="Print progress every N rows (default: 250).",
    )
    parser.add_argument(
        "--http-timeout",
        type=int,
        default=20,
        help="Timeout in seconds for HTTP/S3 source checks/downloads (default: 20).",
    )
    parser.add_argument(
        "--if-exists",
        choices=["overwrite", "skip", "error"],
        default="overwrite",
        help="Behavior when destination object already exists (default: overwrite).",
    )
    parser.add_argument(
        "--tracker-file",
        default=None,
        help="JSONL tracker filename under temp/ used to record per-row progress.",
    )
    parser.add_argument(
        "--resume-from-tracker",
        action="store_true",
        help="Skip rows already present in tracker with terminal statuses.",
    )
    parser.add_argument(
        "--resume-statuses",
        default="copied,exists,missing,skipped_existing,forbidden,error",
        help="Comma-separated statuses that count as already processed when resuming.",
    )
    parser.add_argument(
        "--audit-json",
        default=None,
        help="Audit summary JSON filename under temp/ (default is timestamped).",
    )
    parser.add_argument(
        "--source-url-like",
        action="append",
        default=None,
        help="Optional source URL filter (supports SQL LIKE wildcards % and _). Can be provided multiple times.",
    )

    return parser.parse_args()


ARGS = parse_arguments()
ENV = ARGS.env
RUN_TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")

# Load appropriate .env file from the script's directory
script_dir = os.path.dirname(os.path.abspath(__file__))
env_file = os.path.join(script_dir, f".env.{ENV}")
load_dotenv(env_file)

# Configuration loaded from env
NEW_BUCKET_NAME = os.environ.get("NEW_BUCKET_NAME", "f3-public-images")
EVENT_INSTANCE_PREFIX = os.environ.get(
    "EVENT_INSTANCE_PREFIX",
    os.environ.get("NEW_PREFIX", "event_instance_images/"),
)
ORG_LOGO_PREFIX = os.environ.get("ORG_LOGO_PREFIX", "org-logos/")
DEFAULT_IMAGE_EXT = os.environ.get("DEFAULT_IMAGE_EXT", "jpg").lstrip(".")
EVENT_FORCE_EXT = os.environ.get("EVENT_FORCE_EXT", "source")
ORG_FORCE_EXT = os.environ.get("ORG_FORCE_EXT", "source")

DB_CONFIG = {
    "dbname": os.environ.get("PG_DBNAME"),
    "user": os.environ.get("PG_USER"),
    "password": os.environ.get("PG_PASSWORD"),
    "host": os.environ.get("PG_HOST"),
    "port": os.environ.get("PG_PORT", "5432")
}

TARGET_CONFIG = {
    "event_instances": {
        "id_field": "event_instance_id",
        "query": """
            SELECT id, meta->'files'->>0 AS source_url, meta->'file_ids'->>0 AS source_file_id
            FROM event_instances
            WHERE meta->'files'->>0 IS NOT NULL
            ORDER BY 2
        """,
        "prefix": EVENT_INSTANCE_PREFIX,
        "force_ext": EVENT_FORCE_EXT,
    },
    "orgs": {
        "id_field": "org_id",
        "query": """
                        SELECT id, logo_url AS source_url, NULL::text AS source_file_id
            FROM orgs
            WHERE logo_url IS NOT NULL
              AND logo_url <> ''
            ORDER BY 2
        """,
        "prefix": ORG_LOGO_PREFIX,
        "force_ext": ORG_FORCE_EXT,
    },
}


def validate_config():
    required = ["PG_DBNAME", "PG_USER", "PG_PASSWORD", "PG_HOST"]
    missing = [key for key in required if not os.environ.get(key)]
    if missing:
        missing_str = ", ".join(missing)
        raise ValueError(f"Missing required env vars in .env.{ENV}: {missing_str}")


def infer_extension_from_url(file_url):
    """Infer file extension from URL path."""
    path = urlparse(file_url).path
    _, ext = os.path.splitext(path)
    if ext:
        return ext
    return f".{DEFAULT_IMAGE_EXT}"


def source_descriptor_from_url(file_url):
    """Build a source descriptor for gcs/http URL handling."""
    parsed = urlparse(file_url)
    host = parsed.netloc.lower()
    path = parsed.path.lstrip("/")

    if not parsed.scheme.startswith("http"):
        return {"kind": "unsupported", "url": file_url}

    # https://storage.googleapis.com/<bucket>/<object>
    if host == "storage.googleapis.com":
        parts = path.split("/", 1)
        if len(parts) == 2 and parts[0] and parts[1]:
            return {
                "kind": "gcs",
                "bucket": parts[0],
                "object": parts[1],
                "url": file_url,
            }

    # https://<bucket>.storage.googleapis.com/<object>
    if host.endswith(".storage.googleapis.com") and path:
        bucket = host[: -len(".storage.googleapis.com")]
        return {
            "kind": "gcs",
            "bucket": bucket,
            "object": path,
            "url": file_url,
        }

    return {"kind": "http", "url": file_url}


def verify_bucket_access(bucket, bucket_role):
    """Fail fast with a clearer error if the bucket cannot be accessed."""
    try:
        bucket.reload()
    except Forbidden as exc:
        raise RuntimeError(
            f"Cannot access {bucket_role} bucket '{bucket.name}': {exc.message}"
        ) from exc
    except NotFound as exc:
        raise RuntimeError(
            f"{bucket_role.capitalize()} bucket '{bucket.name}' was not found."
        ) from exc


def write_missing_csv(missing_rows, output_path, id_field):
    """Write missing source objects to a CSV file."""
    if not missing_rows:
        print("No missing source objects found; no CSV written.")
        return

    with open(output_path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(
            csvfile,
            fieldnames=[id_field, "source_url", "source_kind", "source_ref"],
        )
        writer.writeheader()
        writer.writerows(missing_rows)

    print(f"Missing source object list written to: {output_path}")


def write_map_csv(map_rows, output_path):
    """Write old/new URL mapping rows to a CSV file."""
    if not map_rows:
        print("No map rows found; no map CSV written.")
        return

    with open(output_path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(
            csvfile,
            fieldnames=[
                "record_type",
                "record_id",
                "old_url",
                "new_url",
                "source_kind",
                "source_ref",
                "destination_blob_name",
            ],
        )
        writer.writeheader()
        writer.writerows(map_rows)

    print(f"URL map written to: {output_path}")


def resolve_output_path(output_arg, output_kind, target_name, extension="csv"):
    """Resolve output path under temp/ with timestamp and target type in filename."""
    output_dir = os.path.join(script_dir, "temp")
    os.makedirs(output_dir, exist_ok=True)

    if output_arg:
        filename = os.path.basename(output_arg)
        return os.path.join(output_dir, filename)

    filename = f"{RUN_TIMESTAMP}_{output_kind}_type_{target_name}.{extension}"
    return os.path.join(output_dir, filename)


def parse_resume_statuses(raw_statuses):
    """Parse comma-separated status list for resume filtering."""
    statuses = [item.strip() for item in raw_statuses.split(",") if item.strip()]
    if not statuses:
        raise ValueError("--resume-statuses must include at least one status")
    return set(statuses)


def append_tracker_record(tracker_path, record):
    """Append one record to tracker JSONL file."""
    with open(tracker_path, "a", encoding="utf-8") as tracker:
        tracker.write(json.dumps(record, separators=(",", ":")) + "\n")


def init_tracker_file(tracker_path):
    """Ensure tracker file exists before processing."""
    os.makedirs(os.path.dirname(tracker_path), exist_ok=True)
    if not os.path.exists(tracker_path):
        with open(tracker_path, "w", encoding="utf-8"):
            pass


def load_resume_keys(tracker_path, target_name, resume_statuses):
    """Load processed keys from tracker for the selected target."""
    keys = set()
    if not os.path.exists(tracker_path):
        return keys

    with open(tracker_path, "r", encoding="utf-8") as tracker:
        for line in tracker:
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
            except json.JSONDecodeError:
                continue
            if data.get("record_type") != target_name:
                continue
            if data.get("status") not in resume_statuses:
                continue
            record_id = data.get("record_id")
            old_url = data.get("old_url")
            if record_id is None or old_url is None:
                continue
            keys.add((record_id, old_url))

    return keys


def write_audit_json(audit_path, payload):
    """Write one audit summary JSON file for the run."""
    with open(audit_path, "w", encoding="utf-8") as audit_file:
        json.dump(payload, audit_file, indent=2)


def sql_quote(value):
    """Quote text for SQL literals."""
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def _write_values_cte_orgs(sql_file, changed_rows):
    sql_file.write("WITH m(id, old_url, new_url) AS (\n")
    sql_file.write("  VALUES\n")
    for idx, row in enumerate(changed_rows):
        suffix = ",\n" if idx < len(changed_rows) - 1 else "\n"
        sql_file.write(
            f"    ({int(row['record_id'])}, {sql_quote(row['old_url'])}, {sql_quote(row['new_url'])}){suffix}"
        )
    sql_file.write(")\n")


def _write_values_cte_event_instances(sql_file, changed_rows):
    sql_file.write("WITH m(id, old_url, new_url, old_file_id, new_file_id) AS (\n")
    sql_file.write("  VALUES\n")
    for idx, row in enumerate(changed_rows):
        suffix = ",\n" if idx < len(changed_rows) - 1 else "\n"
        sql_file.write(
            f"    ({int(row['record_id'])}, {sql_quote(row['old_url'])}, {sql_quote(row['new_url'])}, "
            f"{sql_quote(row.get('old_file_id'))}, {sql_quote(row.get('new_file_id'))}){suffix}"
        )
    sql_file.write(")\n")


def write_update_and_rollback_sql(changed_rows, target_name, update_sql_path, rollback_sql_path):
    """Write SQL update/rollback scripts using only rows changed in this run."""
    if target_name not in TARGET_CONFIG:
        raise ValueError(f"Unsupported target for SQL generation: {target_name}")

    if not changed_rows:
        for path in (update_sql_path, rollback_sql_path):
            with open(path, "w", encoding="utf-8") as sql_file:
                sql_file.write("BEGIN;\n")
                sql_file.write("-- No rows were copied in this run; nothing to update.\n")
                sql_file.write("COMMIT;\n")
        return

    if target_name == "orgs":
        update_stmt = (
            "UPDATE orgs o\n"
            "SET logo_url = m.new_url\n"
            "FROM m\n"
            "WHERE o.id = m.id\n"
            "  AND o.logo_url = m.old_url;\n"
        )
        rollback_stmt = (
            "UPDATE orgs o\n"
            "SET logo_url = m.old_url\n"
            "FROM m\n"
            "WHERE o.id = m.id\n"
            "  AND o.logo_url = m.new_url;\n"
        )
    else:
        update_stmt = (
            "UPDATE event_instances ei\n"
            "SET meta = jsonb_set(\n"
            "  jsonb_set(ei.meta, '{files,0}', to_jsonb(m.new_url::text), true),\n"
            "  '{file_ids,0}',\n"
            "  to_jsonb(m.new_file_id::text),\n"
            "  true\n"
            ")\n"
            "FROM m\n"
            "WHERE ei.id = m.id\n"
            "  AND ei.meta->'files'->>0 = m.old_url;\n"
        )
        rollback_stmt = (
            "UPDATE event_instances ei\n"
            "SET meta = jsonb_set(\n"
            "  jsonb_set(ei.meta, '{files,0}', to_jsonb(m.old_url::text), true),\n"
            "  '{file_ids,0}',\n"
            "  to_jsonb(m.old_file_id::text),\n"
            "  true\n"
            ")\n"
            "FROM m\n"
            "WHERE ei.id = m.id\n"
            "  AND ei.meta->'files'->>0 = m.new_url;\n"
        )

    with open(update_sql_path, "w", encoding="utf-8") as update_sql:
        update_sql.write("BEGIN;\n\n")
        update_sql.write(f"-- Generated for {len(changed_rows)} copied rows\n")
        if target_name == "orgs":
            _write_values_cte_orgs(update_sql, changed_rows)
        else:
            _write_values_cte_event_instances(update_sql, changed_rows)
        update_sql.write(update_stmt)
        update_sql.write("\nCOMMIT;\n")

    with open(rollback_sql_path, "w", encoding="utf-8") as rollback_sql:
        rollback_sql.write("BEGIN;\n\n")
        rollback_sql.write(f"-- Rollback for {len(changed_rows)} copied rows\n")
        if target_name == "orgs":
            _write_values_cte_orgs(rollback_sql, changed_rows)
        else:
            _write_values_cte_event_instances(rollback_sql, changed_rows)
        rollback_sql.write(rollback_stmt)
        rollback_sql.write("\nCOMMIT;\n")


def build_destination_details(row_id, old_url, target_name):
    """Compute destination key and new URL for a row."""
    target = TARGET_CONFIG[target_name]
    forced = target["force_ext"].lower()
    if forced == "source":
        ext = infer_extension_from_url(old_url)
    else:
        ext = f".{forced.lstrip('.')}"

    destination_blob_name = f"{target['prefix']}{row_id}{ext}"
    new_url = f"https://storage.googleapis.com/{NEW_BUCKET_NAME}/{destination_blob_name}"
    return destination_blob_name, new_url


def read_http_source(url, timeout_seconds):
    """Open an HTTP URL and return a response object."""
    request = Request(url, method="GET")
    return urlopen(request, timeout=timeout_seconds)


def http_source_exists(url, timeout_seconds):
    """Best-effort existence check for non-GCS URL sources."""
    try:
        request = Request(url, method="HEAD")
        with urlopen(request, timeout=timeout_seconds) as response:
            return 200 <= response.status < 400
    except HTTPError as exc:
        if exc.code == 405:
            try:
                request = Request(url, method="GET", headers={"Range": "bytes=0-0"})
                with urlopen(request, timeout=timeout_seconds) as response:
                    return 200 <= response.status < 400
            except Exception:
                return False
        if exc.code in (401, 403):
            raise
        return False
    except URLError:
        return False


def source_ref(source_desc):
    """Human-readable source descriptor for logs/CSV."""
    if source_desc["kind"] == "gcs":
        return f"gs://{source_desc['bucket']}/{source_desc['object']}"
    return source_desc.get("url", "")


def _like_pattern_to_regex(pattern):
    """Convert SQL LIKE pattern to regex pattern."""
    parts = []
    for ch in pattern:
        if ch == "%":
            parts.append(".*")
        elif ch == "_":
            parts.append(".")
        else:
            parts.append(re.escape(ch))
    return "^" + "".join(parts) + "$"


def source_url_matches_filters(source_url, filters):
    """Return True when URL matches any filter pattern."""
    if not filters:
        return True

    url = source_url or ""
    for raw_filter in filters:
        pattern = raw_filter.strip()
        if not pattern:
            continue

        if "%" in pattern or "_" in pattern:
            if re.search(_like_pattern_to_regex(pattern), url, flags=re.IGNORECASE):
                return True
        else:
            if pattern.lower() in url.lower():
                return True

    return False


def is_already_in_desired_format(row_id, source_url, target_name):
    """True when URL already matches the final bucket/prefix/id filename shape."""
    source_desc = source_descriptor_from_url(source_url)
    if source_desc["kind"] != "gcs":
        return False

    if source_desc["bucket"] != NEW_BUCKET_NAME:
        return False

    prefix = TARGET_CONFIG[target_name]["prefix"]
    object_name = source_desc["object"]
    if not object_name.startswith(prefix):
        return False

    remainder = object_name[len(prefix):]
    if not remainder or "/" in remainder:
        return False

    expected_id = str(row_id)
    if remainder == expected_id:
        return True

    stem, _ = os.path.splitext(remainder)
    return stem == expected_id


def process_row(row, storage_client, destination_bucket, target_name, missing_only, timeout_seconds):
    """Process one database row and return status/result details."""
    row_id, old_url, old_file_id = row
    id_field = TARGET_CONFIG[target_name]["id_field"]
    destination_blob_name, new_url = build_destination_details(row_id, old_url, target_name)
    source_desc = source_descriptor_from_url(old_url)
    source_kind = source_desc["kind"]
    ref = source_ref(source_desc)

    base = {
        "record_type": target_name,
        "record_id": row_id,
        "id_field": id_field,
        "old_url": old_url,
        "new_url": new_url,
        "source_kind": source_kind,
        "source_ref": ref,
        "destination_blob_name": destination_blob_name,
        "old_file_id": old_file_id,
        "new_file_id": str(row_id),
    }

    def with_status(status, message=None, missing_row=None):
        result = dict(base)
        result["status"] = status
        if message:
            result["message"] = message
        if missing_row is not None:
            result["missing_row"] = missing_row
        return result

    if source_kind == "unsupported":
        return with_status("error", f"Unsupported URL scheme for {old_url}")

    if missing_only:
        try:
            if source_kind == "gcs":
                source_bucket = storage_client.bucket(source_desc["bucket"])
                source_blob = source_bucket.blob(source_desc["object"])
                exists = source_blob.exists()
            else:
                exists = http_source_exists(old_url, timeout_seconds)

            if exists:
                return with_status("exists")

            return with_status(
                "missing",
                missing_row={
                    id_field: row_id,
                    "source_url": old_url,
                    "source_kind": source_kind,
                    "source_ref": ref,
                },
            )
        except Forbidden as exc:
            return with_status("forbidden", exc.message)
        except HTTPError as exc:
            if exc.code in (401, 403):
                return with_status("forbidden", str(exc))
            return with_status("error", str(exc))
        except GoogleAPICallError as exc:
            return with_status("error", str(exc))

    if destination_bucket is None:
        return with_status("error", "Destination bucket is not initialized.")

    dest_bucket = destination_bucket

    try:
        if ARGS.if_exists != "overwrite":
            destination_blob = dest_bucket.blob(destination_blob_name)
            if destination_blob.exists():
                if ARGS.if_exists == "skip":
                    return with_status("skipped_existing")
                return with_status("error", f"Destination exists: {destination_blob_name}")

        if source_kind == "gcs":
            source_bucket = storage_client.bucket(source_desc["bucket"])
            source_blob = source_bucket.blob(source_desc["object"])
            if ARGS.if_exists == "overwrite":
                source_bucket.copy_blob(source_blob, dest_bucket, destination_blob_name)
            else:
                source_bucket.copy_blob(
                    source_blob,
                    dest_bucket,
                    destination_blob_name,
                    if_generation_match=0,
                )
        else:
            with read_http_source(old_url, timeout_seconds) as response:
                destination_blob = dest_bucket.blob(destination_blob_name)
                content_type = response.headers.get("Content-Type")
                if ARGS.if_exists == "overwrite":
                    destination_blob.upload_from_file(response, content_type=content_type)
                else:
                    destination_blob.upload_from_file(
                        response,
                        content_type=content_type,
                        if_generation_match=0,
                    )
        return with_status("copied")
    except NotFound:
        return with_status(
            "missing",
            missing_row={
                id_field: row_id,
                "source_url": old_url,
                "source_kind": source_kind,
                "source_ref": ref,
            },
        )
    except Forbidden as exc:
        return with_status("forbidden", exc.message)
    except HTTPError as exc:
        if exc.code == 404:
            return with_status(
                "missing",
                missing_row={
                    id_field: row_id,
                    "source_url": old_url,
                    "source_kind": source_kind,
                    "source_ref": ref,
                },
            )
        if exc.code in (401, 403):
            return with_status("forbidden", str(exc))
        return with_status("error", str(exc))
    except GoogleAPICallError as exc:
        return with_status("error", str(exc))
    except URLError as exc:
        return with_status("error", str(exc))

def migrate_images():
    validate_config()
    run_start_epoch = time.time()
    run_started_at = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    mode = "map-only" if ARGS.map_only else ("missing-only" if ARGS.missing_only else "migrate")
    target_name = ARGS.target
    target = TARGET_CONFIG[target_name]
    id_field = target["id_field"]
    resume_statuses = parse_resume_statuses(ARGS.resume_statuses)
    tracker_path = None
    if not ARGS.map_only:
        tracker_path = resolve_output_path(ARGS.tracker_file, "tracker", target_name, extension="jsonl")
    audit_json_path = resolve_output_path(ARGS.audit_json, "audit", target_name, extension="json")
    update_sql_path = None
    rollback_sql_path = None
    if not ARGS.map_only:
        update_sql_path = resolve_output_path(None, "update", target_name, extension="sql")
        rollback_sql_path = resolve_output_path(None, "rollback", target_name, extension="sql")

    if ARGS.resume_from_tracker and not ARGS.tracker_file:
        raise ValueError("--resume-from-tracker requires --tracker-file so you can point to a previous run")

    print(f"Environment: {ENV}")
    print(f"Mode: {mode}")
    print(f"Target: {target_name}")
    print(f"Workers: {ARGS.workers}")
    print(f"If destination exists: {ARGS.if_exists}")
    print(f"Run timestamp: {RUN_TIMESTAMP}")
    if tracker_path:
        print(f"Tracker file: {tracker_path}")
    print(f"Audit JSON: {audit_json_path}")
    print("Source: mixed (GCS and HTTP/S3 supported)")
    if not ARGS.missing_only:
        print(f"Destination bucket: {NEW_BUCKET_NAME}")
    print()

    # Fetch relevant records from Postgres
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute(target["query"])
    rows = cursor.fetchall()

    if ARGS.source_url_like:
        rows = [row for row in rows if source_url_matches_filters(row[1], ARGS.source_url_like)]

    rows = [row for row in rows if not is_already_in_desired_format(row[0], row[1], target_name)]

    original_candidate_count = len(rows)
    print(f"Found {original_candidate_count} candidate records.")

    skipped_from_resume = 0
    if ARGS.resume_from_tracker:
        resume_keys = load_resume_keys(tracker_path, target_name, resume_statuses)
        if resume_keys:
            before = len(rows)
            rows = [row for row in rows if (row[0], row[1]) not in resume_keys]
            skipped_from_resume = before - len(rows)
            print(f"Resume active. Skipping {skipped_from_resume} previously processed rows.")

    if not rows:
        print("No matching rows found after filters. No output files created.")
        cursor.close()
        conn.close()
        return

    if tracker_path:
        init_tracker_file(tracker_path)
        append_tracker_record(
            tracker_path,
            {
                "run_timestamp": RUN_TIMESTAMP,
                "record_type": target_name,
                "record_id": None,
                "old_url": None,
                "new_url": None,
                "source_kind": None,
                "source_ref": None,
                "destination_blob_name": None,
                "status": "run_started",
                "message": f"mode={mode}",
                "processed_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
            },
        )

    map_rows = []
    for row_id, old_url, _ in rows:
        destination_blob_name, new_url = build_destination_details(row_id, old_url, target_name)
        src = source_descriptor_from_url(old_url)
        map_rows.append(
            {
                "record_type": target_name,
                "record_id": row_id,
                "old_url": old_url,
                "new_url": new_url,
                "source_kind": src["kind"],
                "source_ref": source_ref(src),
                "destination_blob_name": destination_blob_name,
            }
        )

    map_csv_path = resolve_output_path(ARGS.map_csv, "url_map", target_name)
    write_map_csv(map_rows, map_csv_path)

    if ARGS.map_only:
        run_duration_sec = round(time.time() - run_start_epoch, 3)
        audit_payload = {
            "run_timestamp": RUN_TIMESTAMP,
            "started_at": run_started_at,
            "ended_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
            "duration_seconds": run_duration_sec,
            "environment": ENV,
            "mode": mode,
            "target": target_name,
            "arguments": vars(ARGS),
            "counts": {
                "candidate_rows_total": original_candidate_count,
                "skipped_from_resume": skipped_from_resume,
                "rows_selected_for_run": len(rows),
                "map_rows_written": len(map_rows),
            },
            "outputs": {
                "map_csv": map_csv_path,
                "tracker_jsonl": None,
                "audit_json": audit_json_path,
                "missing_csv": None,
                "update_sql": None,
                "rollback_sql": None,
            },
        }
        write_audit_json(audit_json_path, audit_payload)

        cursor.close()
        conn.close()
        print("Map-only run complete. No GCS operations were performed.")
        print(f"Audit JSON written to: {audit_json_path}")
        return

    # Initialize GCS client
    storage_client = storage.Client()
    destination_bucket = storage_client.bucket(NEW_BUCKET_NAME) if not ARGS.missing_only else None
    if destination_bucket is not None:
        verify_bucket_access(destination_bucket, "destination")

    copied_count = 0
    exists_count = 0
    missing_count = 0
    skipped_existing_count = 0
    forbidden_count = 0
    error_count = 0
    missing_rows = []
    changed_rows = []
    processed_count = 0

    with ThreadPoolExecutor(max_workers=max(1, ARGS.workers)) as executor:
        for idx, result in enumerate(
            executor.map(
                lambda row: process_row(
                    row,
                    storage_client,
                    destination_bucket,
                    target_name,
                    ARGS.missing_only,
                    ARGS.http_timeout,
                ),
                rows,
            ),
            start=1,
        ):
            status = result["status"]
            processed_count += 1

            tracker_record = {
                "run_timestamp": RUN_TIMESTAMP,
                "record_type": result["record_type"],
                "record_id": result["record_id"],
                "old_url": result["old_url"],
                "new_url": result["new_url"],
                "source_kind": result["source_kind"],
                "source_ref": result["source_ref"],
                "destination_blob_name": result["destination_blob_name"],
                "status": status,
                "message": result.get("message"),
                "processed_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
            }
            append_tracker_record(tracker_path, tracker_record)

            if status == "copied":
                copied_count += 1
                changed_rows.append(
                    {
                        "record_id": result["record_id"],
                        "old_url": result["old_url"],
                        "new_url": result["new_url"],
                        "old_file_id": result.get("old_file_id"),
                        "new_file_id": result.get("new_file_id"),
                    }
                )
            elif status == "exists":
                exists_count += 1
            elif status == "missing":
                missing_count += 1
                missing_rows.append(result["missing_row"])
            elif status == "skipped_existing":
                skipped_existing_count += 1
            elif status == "forbidden":
                forbidden_count += 1
            else:
                error_count += 1

            if ARGS.progress_every > 0 and idx % ARGS.progress_every == 0:
                print(
                    f"Processed {idx}/{len(rows)} | copied={copied_count} exists={exists_count} "
                    f"missing={missing_count} skipped={skipped_existing_count} "
                    f"forbidden={forbidden_count} errors={error_count}"
                )

    missing_csv_path = resolve_output_path(ARGS.missing_csv, "missing_source_objects", target_name)
    write_missing_csv(missing_rows, missing_csv_path, id_field)
    write_update_and_rollback_sql(changed_rows, target_name, update_sql_path, rollback_sql_path)

    print("\nMigration summary:")
    if ARGS.missing_only:
        print(f"  Exists in source: {exists_count}")
    print(f"  Copied: {copied_count}")
    print(f"  Missing source objects: {missing_count}")
    print(f"  Skipped existing destination objects: {skipped_existing_count}")
    print(f"  Forbidden: {forbidden_count}")
    print(f"  Other GCS errors: {error_count}")
    print(f"  URL map CSV: {map_csv_path}")
    print(f"  Missing CSV: {missing_csv_path}")
    print(f"  Update SQL: {update_sql_path}")
    print(f"  Rollback SQL: {rollback_sql_path}")

    run_duration_sec = round(time.time() - run_start_epoch, 3)
    audit_payload = {
        "run_timestamp": RUN_TIMESTAMP,
        "started_at": run_started_at,
        "ended_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "duration_seconds": run_duration_sec,
        "environment": ENV,
        "mode": mode,
        "target": target_name,
        "arguments": vars(ARGS),
        "counts": {
            "candidate_rows_total": original_candidate_count,
            "skipped_from_resume": skipped_from_resume,
            "rows_selected_for_run": len(rows),
            "processed_rows": processed_count,
            "copied": copied_count,
            "exists": exists_count,
            "missing": missing_count,
            "skipped_existing": skipped_existing_count,
            "forbidden": forbidden_count,
            "errors": error_count,
        },
        "outputs": {
            "map_csv": map_csv_path,
            "missing_csv": missing_csv_path,
            "tracker_jsonl": tracker_path,
            "audit_json": audit_json_path,
            "update_sql": update_sql_path,
            "rollback_sql": rollback_sql_path,
        },
    }
    write_audit_json(audit_json_path, audit_payload)
    append_tracker_record(
        tracker_path,
        {
            "run_timestamp": RUN_TIMESTAMP,
            "record_type": target_name,
            "record_id": None,
            "old_url": None,
            "new_url": None,
            "source_kind": None,
            "source_ref": None,
            "destination_blob_name": None,
            "status": "run_completed",
            "message": mode,
            "processed_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        },
    )
    print(f"  Tracker JSONL: {tracker_path}")
    print(f"  Audit JSON: {audit_json_path}")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    migrate_images()