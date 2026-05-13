import argparse
import csv
import hashlib
import io
import json
import logging
import os
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse

import psycopg2
import requests
from dotenv import load_dotenv
from google.cloud import storage
from PIL import Image, ImageOps, UnidentifiedImageError

DEFAULT_BUCKETS = {
    "prod": "f3-public-images",
    "staging": "f3-public-images-staging",
}

DEFAULT_PUBLIC_URL_TEMPLATE = "https://storage.googleapis.com/{bucket}/{object_path}"
DEFAULT_GCS_PREFIX = "user-avatars"
DEFAULT_HTTP_TIMEOUT_SECONDS = 20
DEFAULT_AUDIT_DIRNAME = "output"
DEFAULT_WHITELIST_FILENAME = "whitelist_domains.txt"
DEFAULT_INCLUDE_PREFIXES_FILENAME = "include_url_prefixes.txt"
DEFAULT_SKIP_PREFIXES_FILENAME = "skip_url_prefixes.txt"


@dataclass
class MigrationRow:
    user_id: int
    old_avatar_url: str
    source_domain: str


@dataclass
class MigrationResult:
    user_id: int
    old_avatar_url: str
    new_avatar_url: Optional[str]
    source_domain: str
    status: str
    detail: str
    source_http_status: Optional[int] = None
    source_content_type: Optional[str] = None
    source_size_bytes: Optional[int] = None
    jpeg_size_bytes: Optional[int] = None
    gcs_object_path: Optional[str] = None
    gcs_md5_hex: Optional[str] = None


class TeeStream:
    def __init__(self, *streams):
        self.streams = streams

    def write(self, data):
        for stream in self.streams:
            stream.write(data)
            stream.flush()

    def flush(self):
        for stream in self.streams:
            stream.flush()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Migrate users.avatar_url images into GCS user-avatars/<id>.jpg, "
            "updating DB URLs with dry-run/commit support."
        )
    )
    parser.add_argument(
        "--environment",
        choices=["staging", "prod"],
        default="staging",
        help="Target database/bucket environment (default: staging).",
    )
    parser.add_argument(
        "--env_file",
        default=None,
        help="Optional path to env file. Defaults to .env.<environment> in this folder.",
    )
    parser.add_argument(
        "--commit",
        action="store_true",
        help="Persist database updates. By default script runs in dry-run and rolls back.",
    )
    parser.add_argument(
        "--bucket",
        default=None,
        help="Override default bucket for the selected environment.",
    )
    parser.add_argument(
        "--gcs_prefix",
        default=DEFAULT_GCS_PREFIX,
        help="Folder path inside bucket where JPEG avatars are written (default: user-avatars).",
    )
    parser.add_argument(
        "--public_url_template",
        default=DEFAULT_PUBLIC_URL_TEMPLATE,
        help=(
            "Template for updated avatar_url; supports {bucket} and {object_path}. "
            "Default: https://storage.googleapis.com/{bucket}/{object_path}"
        ),
    )
    parser.add_argument(
        "--whitelist_domains",
        default=None,
        help=(
            "Comma-separated list of domains to skip migrating, e.g. "
            "images.example.com,cdn.example.org."
        ),
    )
    parser.add_argument(
        "--whitelist_file",
        default=None,
        help=(
            "Optional path to a text file containing one domain per line. "
            f"If omitted, the script auto-loads {DEFAULT_WHITELIST_FILENAME} from this folder when present."
        ),
    )
    parser.add_argument(
        "--include_url_prefixes",
        default=None,
        help="Comma-separated URL prefixes to include. If set, only matching URLs are processed.",
    )
    parser.add_argument(
        "--skip_url_prefixes",
        default=None,
        help="Comma-separated URL prefixes to skip.",
    )
    parser.add_argument(
        "--include_prefix_file",
        default=None,
        help=(
            "Optional text file containing URL prefixes to include (one per line). "
            f"If omitted, the script auto-loads {DEFAULT_INCLUDE_PREFIXES_FILENAME} when present."
        ),
    )
    parser.add_argument(
        "--skip_prefix_file",
        default=None,
        help=(
            "Optional text file containing URL prefixes to skip (one per line). "
            f"If omitted, the script auto-loads {DEFAULT_SKIP_PREFIXES_FILENAME} when present."
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional max number of users to process (after filtering empty URLs).",
    )
    parser.add_argument(
        "--http_timeout_seconds",
        type=int,
        default=DEFAULT_HTTP_TIMEOUT_SECONDS,
        help=f"HTTP timeout per image request (default: {DEFAULT_HTTP_TIMEOUT_SECONDS}s).",
    )
    parser.add_argument(
        "--gcp_project",
        default=None,
        help="Optional GCP project override for the storage client.",
    )
    parser.add_argument(
        "--output_dir",
        default=None,
        help="Directory for audit log, CSV, summary JSON, and rollback SQL.",
    )
    return parser.parse_args()


def configure_env(args: argparse.Namespace, script_dir: str) -> Dict[str, str]:
    env_file = args.env_file or os.path.join(script_dir, f".env.{args.environment}")
    if not os.path.exists(env_file):
        raise FileNotFoundError(
            f"Environment file not found: {env_file}. Create it with PG_* variables first."
        )

    load_dotenv(env_file)

    missing = [
        key
        for key in ["PG_HOST", "PG_PORT", "PG_DBNAME", "PG_USER", "PG_PASSWORD"]
        if not os.environ.get(key)
    ]
    if missing:
        raise ValueError(f"Missing required DB environment variables in {env_file}: {missing}")

    return {
        "host": os.environ["PG_HOST"],
        "port": int(os.environ["PG_PORT"]),
        "dbname": os.environ["PG_DBNAME"],
        "user": os.environ["PG_USER"],
        "password": os.environ["PG_PASSWORD"],
    }


def setup_output_dir(args: argparse.Namespace, script_dir: str) -> Tuple[str, str]:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_base = args.output_dir or os.path.join(script_dir, DEFAULT_AUDIT_DIRNAME)
    run_dir = os.path.join(output_base, f"avatar_migration_{args.environment}_{timestamp}")
    os.makedirs(run_dir, exist_ok=True)
    return run_dir, timestamp


def parse_whitelist(raw_value: str) -> List[str]:
    if not raw_value:
        return []
    values = [item.strip().lower() for item in raw_value.split(",") if item.strip()]
    return sorted(set(values))


def parse_whitelist_file(file_path: str) -> List[str]:
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Whitelist file not found: {file_path}")

    values: List[str] = []
    with open(file_path, "r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            # Allow comma-separated values on a line as a convenience.
            for value in line.split(","):
                normalized = value.strip().lower()
                if normalized:
                    values.append(normalized)
    return sorted(set(values))


def resolve_whitelist(args: argparse.Namespace, script_dir: str) -> Tuple[List[str], Optional[str], str]:
    cli_domains = parse_whitelist(args.whitelist_domains or "")

    whitelist_file_path = args.whitelist_file
    source = "none"
    file_domains: List[str] = []

    if whitelist_file_path:
        file_domains = parse_whitelist_file(whitelist_file_path)
        source = "explicit_file"
    else:
        auto_file_path = os.path.join(script_dir, DEFAULT_WHITELIST_FILENAME)
        if os.path.exists(auto_file_path):
            whitelist_file_path = auto_file_path
            file_domains = parse_whitelist_file(auto_file_path)
            source = "auto_file"
        elif cli_domains:
            source = "cli"

    combined = sorted(set(cli_domains + file_domains))
    if combined and source == "none":
        source = "cli"

    return combined, whitelist_file_path, source


def parse_prefixes(raw_value: str) -> List[str]:
    if not raw_value:
        return []
    return sorted(set(item.strip() for item in raw_value.split(",") if item.strip()))


def parse_prefix_file(file_path: str) -> List[str]:
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Prefix file not found: {file_path}")

    values: List[str] = []
    with open(file_path, "r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            for value in line.split(","):
                normalized = value.strip()
                if normalized:
                    values.append(normalized)
    return sorted(set(values))


def resolve_prefix_filters(
    cli_value: Optional[str],
    explicit_file: Optional[str],
    script_dir: str,
    default_filename: str,
) -> Tuple[List[str], Optional[str]]:
    cli_prefixes = parse_prefixes(cli_value or "")
    file_path = explicit_file
    file_prefixes: List[str] = []

    if file_path:
        file_prefixes = parse_prefix_file(file_path)
    else:
        auto_path = os.path.join(script_dir, default_filename)
        if os.path.exists(auto_path):
            file_path = auto_path
            file_prefixes = parse_prefix_file(auto_path)

    return sorted(set(cli_prefixes + file_prefixes)), file_path


def starts_with_any(value: str, prefixes: Iterable[str]) -> bool:
    for prefix in prefixes:
        if value.startswith(prefix):
            return True
    return False


def detect_gcloud_project() -> Optional[str]:
    """Return active gcloud project from local CLI config when available."""
    try:
        result = subprocess.run(
            ["gcloud", "config", "get-value", "project"],
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError:
        return None

    if result.returncode != 0:
        return None

    value = (result.stdout or "").strip()
    if not value or value == "(unset)":
        return None
    return value


def resolve_gcp_project(args: argparse.Namespace) -> Optional[str]:
    """Resolve GCP project for Storage client from arg, env, then gcloud config."""
    if args.gcp_project:
        return args.gcp_project

    env_project = os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCLOUD_PROJECT")
    if env_project:
        return env_project

    return detect_gcloud_project()


def extract_domain(url: str) -> str:
    parsed = urlparse(url)
    hostname = (parsed.hostname or "").lower()
    return hostname


def is_whitelisted_domain(domain: str, whitelist: Iterable[str]) -> bool:
    if not domain:
        return False
    for item in whitelist:
        normalized = item.lower().strip()
        if not normalized:
            continue
        if domain == normalized or domain.endswith(f".{normalized}"):
            return True
    return False


def sql_literal(value: Optional[str]) -> str:
    if value is None:
        return "NULL"
    return "'" + value.replace("'", "''") + "'"


def build_public_url(template: str, bucket: str, object_path: str) -> str:
    return template.format(bucket=bucket, object_path=object_path)


def fetch_candidate_rows(
    cur,
    limit: Optional[int],
    include_prefixes: List[str],
    skip_prefixes: List[str],
) -> List[MigrationRow]:
    sql = """
        SELECT id, avatar_url
        FROM users
        WHERE avatar_url IS NOT NULL
          AND btrim(avatar_url) <> ''
    """

    params: List[object] = []

    if include_prefixes:
        include_clauses = []
        for prefix in include_prefixes:
            include_clauses.append("avatar_url LIKE %s")
            params.append(prefix + "%")
        sql += "\n          AND (" + " OR ".join(include_clauses) + ")"

    if skip_prefixes:
        for prefix in skip_prefixes:
            sql += "\n          AND avatar_url NOT LIKE %s"
            params.append(prefix + "%")

    sql += "\n        ORDER BY id"

    if limit is not None:
        sql += " LIMIT %s"
        params.append(limit)

    cur.execute(sql, tuple(params))
    out: List[MigrationRow] = []
    for user_id, avatar_url in cur.fetchall():
        domain = extract_domain(avatar_url)
        out.append(MigrationRow(user_id=user_id, old_avatar_url=avatar_url, source_domain=domain))
    return out


def download_image(url: str, timeout_seconds: int) -> Tuple[bytes, int, str]:
    headers = {"User-Agent": "f3-avatar-migrator/1.0"}
    response = requests.get(url, headers=headers, timeout=timeout_seconds)
    response.raise_for_status()
    content_type = response.headers.get("Content-Type", "")
    return response.content, response.status_code, content_type


def convert_to_jpeg(raw_bytes: bytes) -> bytes:
    try:
        with Image.open(io.BytesIO(raw_bytes)) as img:
            img = ImageOps.exif_transpose(img)
            if img.mode in ("RGBA", "LA") or (img.mode == "P" and "transparency" in img.info):
                background = Image.new("RGB", img.size, (255, 255, 255))
                alpha = img.convert("RGBA")
                background.paste(alpha, mask=alpha.split()[3])
                img = background
            elif img.mode != "RGB":
                img = img.convert("RGB")

            output = io.BytesIO()
            img.save(output, format="JPEG", quality=90, optimize=True)
            return output.getvalue()
    except UnidentifiedImageError as exc:
        raise ValueError("Unsupported or invalid image format") from exc


def upload_to_gcs(
    client: storage.Client,
    bucket_name: str,
    object_path: str,
    content: bytes,
) -> str:
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_path)
    blob.upload_from_string(content, content_type="image/jpeg")
    return blob.md5_hash or ""


def write_audit_csv(path: str, rows: List[MigrationResult]) -> None:
    fieldnames = [
        "user_id",
        "old_avatar_url",
        "new_avatar_url",
        "source_domain",
        "status",
        "detail",
        "source_http_status",
        "source_content_type",
        "source_size_bytes",
        "jpeg_size_bytes",
        "gcs_object_path",
        "gcs_md5_hex",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "user_id": row.user_id,
                    "old_avatar_url": row.old_avatar_url,
                    "new_avatar_url": row.new_avatar_url,
                    "source_domain": row.source_domain,
                    "status": row.status,
                    "detail": row.detail,
                    "source_http_status": row.source_http_status,
                    "source_content_type": row.source_content_type,
                    "source_size_bytes": row.source_size_bytes,
                    "jpeg_size_bytes": row.jpeg_size_bytes,
                    "gcs_object_path": row.gcs_object_path,
                    "gcs_md5_hex": row.gcs_md5_hex,
                }
            )


def write_rollback_sql(path: str, successful_rows: List[MigrationResult], args: argparse.Namespace) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write("-- Rollback SQL generated by migrate_user_avatars.py\n")
        f.write(f"-- Generated at: {datetime.now(timezone.utc).isoformat()}\n")
        f.write(f"-- Environment: {args.environment}\n")
        f.write(f"-- Commit mode used: {args.commit}\n")
        f.write("-- Reverts users.avatar_url only when current value matches migrated value.\n\n")

        f.write("BEGIN;\n")
        for row in successful_rows:
            if not row.new_avatar_url:
                continue
            f.write(
                "UPDATE users "
                f"SET avatar_url = {sql_literal(row.old_avatar_url)} "
                f"WHERE id = {row.user_id} "
                f"AND avatar_url = {sql_literal(row.new_avatar_url)};\n"
            )
        f.write("COMMIT;\n\n")

        object_paths = [row.gcs_object_path for row in successful_rows if row.gcs_object_path]
        f.write("-- Optional: remove uploaded files after rollback\n")
        if object_paths:
            f.write("-- gsutil rm \\\n")
            for idx, object_path in enumerate(object_paths):
                suffix = " \\\n" if idx < len(object_paths) - 1 else "\n"
                f.write(f"--   gs://{args.bucket}/{object_path}{suffix}")
        else:
            f.write("-- No uploaded objects recorded in this run.\n")


def write_summary_json(path: str, summary: Dict[str, object]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)


def main() -> None:
    args = parse_args()
    script_dir = os.path.dirname(os.path.abspath(__file__))
    args.bucket = args.bucket or DEFAULT_BUCKETS[args.environment]

    run_dir, run_timestamp = setup_output_dir(args, script_dir)
    log_file_path = os.path.join(run_dir, "migration.log")
    audit_csv_path = os.path.join(run_dir, "audit.csv")
    summary_json_path = os.path.join(run_dir, "summary.json")
    rollback_sql_path = os.path.join(run_dir, "rollback.sql")

    with open(log_file_path, "w", encoding="utf-8") as log_handle:
        tee = TeeStream(sys.stdout, log_handle)
        sys.stdout = tee
        sys.stderr = tee

        logging.basicConfig(level=logging.INFO, format="%(message)s")
        logger = logging.getLogger("avatar_migrator")

        try:
            db_config = configure_env(args, script_dir)
            whitelist, whitelist_file_path, whitelist_source = resolve_whitelist(args, script_dir)
            include_prefixes, include_prefix_file = resolve_prefix_filters(
                args.include_url_prefixes,
                args.include_prefix_file,
                script_dir,
                DEFAULT_INCLUDE_PREFIXES_FILENAME,
            )
            skip_prefixes, skip_prefix_file = resolve_prefix_filters(
                args.skip_url_prefixes,
                args.skip_prefix_file,
                script_dir,
                DEFAULT_SKIP_PREFIXES_FILENAME,
            )

            logger.info("=" * 80)
            logger.info("USER AVATAR URL MIGRATION")
            logger.info("=" * 80)
            logger.info(f"Environment: {args.environment}")
            logger.info(f"Mode: {'COMMIT' if args.commit else 'DRY RUN (DB rollback)'}")
            resolved_project = resolve_gcp_project(args)
            if not resolved_project:
                raise ValueError(
                    "Unable to determine GCP project for Cloud Storage. "
                    "Set one of: --gcp_project, GOOGLE_CLOUD_PROJECT env var, "
                    "or run 'gcloud config set project <PROJECT_ID>'."
                )

            logger.info(f"Bucket: {args.bucket}")
            logger.info(f"GCP project: {resolved_project}")
            logger.info(f"GCS prefix: {args.gcs_prefix}")
            logger.info(f"Public URL template: {args.public_url_template}")
            logger.info(f"Whitelist source: {whitelist_source}")
            if whitelist_file_path:
                logger.info(f"Whitelist file: {whitelist_file_path}")
            logger.info(f"Whitelist domains ({len(whitelist)}): {', '.join(whitelist) if whitelist else '(none)'}")
            if include_prefix_file:
                logger.info(f"Include prefix file: {include_prefix_file}")
            logger.info(
                f"Include URL prefixes ({len(include_prefixes)}): "
                f"{', '.join(include_prefixes) if include_prefixes else '(none)'}"
            )
            if skip_prefix_file:
                logger.info(f"Skip prefix file: {skip_prefix_file}")
            logger.info(
                f"Skip URL prefixes ({len(skip_prefixes)}): "
                f"{', '.join(skip_prefixes) if skip_prefixes else '(none)'}"
            )
            logger.info(f"Output directory: {run_dir}")
            logger.info("=" * 80)

            conn = psycopg2.connect(**db_config)
            cur = conn.cursor()

            storage_client = storage.Client(project=resolved_project)

            results: List[MigrationResult] = []

            rows = fetch_candidate_rows(cur, args.limit, include_prefixes, skip_prefixes)
            logger.info(f"Found {len(rows)} users with non-empty avatar_url")

            for idx, row in enumerate(rows, 1):
                logger.info(f"[{idx}/{len(rows)}] user_id={row.user_id} source_url={row.old_avatar_url}")

                if include_prefixes and not starts_with_any(row.old_avatar_url, include_prefixes):
                    results.append(
                        MigrationResult(
                            user_id=row.user_id,
                            old_avatar_url=row.old_avatar_url,
                            new_avatar_url=None,
                            source_domain=row.source_domain,
                            status="skipped_not_included",
                            detail="URL did not match any include prefix",
                        )
                    )
                    continue

                if skip_prefixes and starts_with_any(row.old_avatar_url, skip_prefixes):
                    results.append(
                        MigrationResult(
                            user_id=row.user_id,
                            old_avatar_url=row.old_avatar_url,
                            new_avatar_url=None,
                            source_domain=row.source_domain,
                            status="skipped_prefix",
                            detail="URL matched skip prefix",
                        )
                    )
                    continue

                if is_whitelisted_domain(row.source_domain, whitelist):
                    results.append(
                        MigrationResult(
                            user_id=row.user_id,
                            old_avatar_url=row.old_avatar_url,
                            new_avatar_url=None,
                            source_domain=row.source_domain,
                            status="skipped_whitelisted",
                            detail="Domain is whitelisted",
                        )
                    )
                    continue

                object_path = f"{args.gcs_prefix.strip('/')}/{row.user_id}.jpg"
                new_url = build_public_url(args.public_url_template, args.bucket, object_path)

                try:
                    source_bytes, status_code, source_content_type = download_image(
                        row.old_avatar_url, args.http_timeout_seconds
                    )
                    jpg_bytes = convert_to_jpeg(source_bytes)
                    gcs_md5_base64 = upload_to_gcs(
                        storage_client,
                        args.bucket,
                        object_path,
                        jpg_bytes,
                    )
                    gcs_md5_hex = ""
                    if gcs_md5_base64:
                        # Keep a stable hash in audit trail independent of encoding representation.
                        gcs_md5_hex = hashlib.md5(jpg_bytes).hexdigest()

                    cur.execute(
                        "UPDATE users SET avatar_url = %s WHERE id = %s",
                        (new_url, row.user_id),
                    )

                    results.append(
                        MigrationResult(
                            user_id=row.user_id,
                            old_avatar_url=row.old_avatar_url,
                            new_avatar_url=new_url,
                            source_domain=row.source_domain,
                            status="migrated",
                            detail="Uploaded, converted to JPEG, and prepared DB update",
                            source_http_status=status_code,
                            source_content_type=source_content_type,
                            source_size_bytes=len(source_bytes),
                            jpeg_size_bytes=len(jpg_bytes),
                            gcs_object_path=object_path,
                            gcs_md5_hex=gcs_md5_hex,
                        )
                    )
                except requests.RequestException as exc:
                    results.append(
                        MigrationResult(
                            user_id=row.user_id,
                            old_avatar_url=row.old_avatar_url,
                            new_avatar_url=None,
                            source_domain=row.source_domain,
                            status="failed_download",
                            detail=str(exc),
                        )
                    )
                except Exception as exc:
                    results.append(
                        MigrationResult(
                            user_id=row.user_id,
                            old_avatar_url=row.old_avatar_url,
                            new_avatar_url=None,
                            source_domain=row.source_domain,
                            status="failed_process",
                            detail=str(exc),
                        )
                    )

            migrated = [row for row in results if row.status == "migrated"]
            skipped = [row for row in results if row.status.startswith("skipped_")]
            failed = [row for row in results if row.status.startswith("failed_")]

            if args.commit:
                conn.commit()
                logger.info("Committed DB transaction.")
            else:
                conn.rollback()
                logger.info("Dry run complete; rolled back DB transaction.")

            write_audit_csv(audit_csv_path, results)
            write_rollback_sql(rollback_sql_path, migrated, args)

            summary = {
                "run_timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "environment": args.environment,
                "commit": args.commit,
                "bucket": args.bucket,
                "gcp_project": resolved_project,
                "gcs_prefix": args.gcs_prefix,
                "public_url_template": args.public_url_template,
                "whitelist_source": whitelist_source,
                "whitelist_file": whitelist_file_path,
                "whitelist_domains": whitelist,
                "include_prefix_file": include_prefix_file,
                "include_url_prefixes": include_prefixes,
                "skip_prefix_file": skip_prefix_file,
                "skip_url_prefixes": skip_prefixes,
                "totals": {
                    "candidates": len(rows),
                    "migrated": len(migrated),
                    "skipped": len(skipped),
                    "failed": len(failed),
                },
                "artifacts": {
                    "log_file": log_file_path,
                    "audit_csv": audit_csv_path,
                    "summary_json": summary_json_path,
                    "rollback_sql": rollback_sql_path,
                },
            }
            write_summary_json(summary_json_path, summary)

            logger.info("=" * 80)
            logger.info("RUN SUMMARY")
            logger.info("=" * 80)
            logger.info(f"Candidates: {len(rows)}")
            logger.info(f"Migrated: {len(migrated)}")
            logger.info(f"Skipped: {len(skipped)}")
            logger.info(f"Failed: {len(failed)}")
            logger.info(f"Audit CSV: {audit_csv_path}")
            logger.info(f"Rollback SQL: {rollback_sql_path}")
            logger.info(f"Summary JSON: {summary_json_path}")
            logger.info("=" * 80)

            cur.close()
            conn.close()

        finally:
            sys.stdout = sys.__stdout__
            sys.stderr = sys.__stderr__


if __name__ == "__main__":
    main()
