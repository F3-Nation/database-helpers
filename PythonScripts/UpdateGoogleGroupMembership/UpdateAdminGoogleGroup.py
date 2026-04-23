#!/usr/bin/env python3
"""
Sync Google Group membership to exactly match users returned from a Postgres query.

Configuration is loaded from UpdateAdminGoogleGroup.config file and .env file.
"""

import os
import sys
import json
import logging
from typing import Set, Dict, Any

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


# ---------------------------
# Logging
# ---------------------------
logging.basicConfig(level=logging.DEBUG, format="%(levelname)s: %(message)s")


# ---------------------------
# Config Management
# ---------------------------
def load_config(config_file: str) -> Dict[str, Any]:
    """Load configuration from JSON file."""
    if not os.path.exists(config_file):
        raise RuntimeError(f"Config file not found: {config_file}")
    
    with open(config_file, "r") as f:
        configs = json.load(f)
    
    if not isinstance(configs, list):
        raise RuntimeError("Config file must contain a JSON array of configuration objects")
    
    return {cfg.get("id"): cfg for cfg in configs}


def get_config_by_id(config_dict: Dict[str, Any], config_id: str) -> Dict[str, Any]:
    """Get configuration by ID."""
    if config_id not in config_dict:
        available = ", ".join(config_dict.keys())
        raise RuntimeError(
            f"Configuration ID '{config_id}' not found. Available IDs: {available}"
        )
    return config_dict[config_id]


# ---------------------------
# Input Check
# ---------------------------
def require_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def get_config_id() -> str:
    """Get config ID from command line or environment variable."""
    if len(sys.argv) > 1:
        return sys.argv[1]
    
    config_id = os.getenv("CONFIG_ID")
    if config_id and config_id.strip():
        return config_id.strip()
    
    raise RuntimeError("Config ID must be provided as command line argument or CONFIG_ID environment variable")



# ---------------------------
# Helpers
# ---------------------------
def get_db_emails(db_url: str, query: str) -> Set[str]:
    logging.info("Connecting to Postgres...")
    emails = set()

    try:
        with psycopg2.connect(db_url) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                logging.info("Running query...")
                cur.execute(query)
                for row in cur.fetchall():
                    email = row.get("email")
                    if email:
                        emails.add(email.strip().lower())
    except (psycopg2.OperationalError, psycopg2.DatabaseError) as e:
        raise RuntimeError(f"Database error: {e}")

    logging.info("Fetched %d unique emails from DB.", len(emails))
    return emails


def build_directory_service(sa_keyfile: str, impersonate: str):
    scopes = [
        "https://www.googleapis.com/auth/admin.directory.group",
        "https://www.googleapis.com/auth/admin.directory.group.member",
    ]

    creds = service_account.Credentials.from_service_account_file(
        sa_keyfile, scopes=scopes
    )
    delegated = creds.with_subject(impersonate)

    return build(
        "admin",
        "directory_v1",
        credentials=delegated,
        cache_discovery=False,
    )


def get_group_members(service, group_email: str) -> Set[str]:
    logging.info("Fetching members for group %s", group_email)
    members = set()

    request = service.members().list(groupKey=group_email, maxResults=200)
    while request:
        response = request.execute()
        for m in response.get("members", []):
            if m.get("email"):
                members.add(m["email"].strip().lower())
        request = service.members().list_next(request, response)

    logging.info("Group currently has %d members.", len(members))
    return members


def add_member(service, group_email: str, email: str):
    try:
        service.members().insert(
            groupKey=group_email,
            body={"email": email, "role": "MEMBER"},
        ).execute()
        logging.info("Added %s", email)
    except HttpError as e:
        logging.error("Failed to add %s: %s", email, e)


def remove_member(service, group_email: str, email: str):
    try:
        service.members().delete(
            groupKey=group_email,
            memberKey=email,
        ).execute()
        logging.info("Removed %s", email)
    except HttpError as e:
        logging.error("Failed to remove %s: %s", email, e)


# ---------------------------
# Main
# ---------------------------
def main():
    load_dotenv()

    # Required environment variables
    REQUIRED_ENV = [
        "DATABASE_URL",
        "GOOGLE_SERVICE_ACCOUNT_JSON",
        "GOOGLE_IMPERSONATE_USER",
    ]

    missing = [k for k in REQUIRED_ENV if not os.getenv(k)]
    if missing:
        logging.error("Missing required env vars: %s", ", ".join(missing))
        sys.exit(1)

    # Load config
    config_file = "UpdateAdminGoogleGroup.config"
    try:
        config_dict = load_config(config_file)
        config_id = get_config_id()
        config = get_config_by_id(config_dict, config_id)
    except RuntimeError as e:
        logging.error(str(e))
        sys.exit(1)

    db_url = require_env("DATABASE_URL")
    sa_keyfile = require_env("GOOGLE_SERVICE_ACCOUNT_JSON")
    impersonate = require_env("GOOGLE_IMPERSONATE_USER")
    group_email = config.get("google_group")
    query = config.get("postsql_query")

    if not group_email:
        logging.error("Missing 'google_group' in config ID '%s'", config_id)
        sys.exit(1)
    
    if not query:
        logging.error("Missing 'postsql_query' in config ID '%s'", config_id)
        sys.exit(1)

    apply_changes = os.getenv("APPLY_CHANGES", "false").lower() == "true"
    verbose = os.getenv("VERBOSE", "false").lower() == "true"

    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    logging.info("Using config ID: %s", config_id)
    logging.info("Target group: %s", group_email)

    db_emails = get_db_emails(db_url, query)
    service = build_directory_service(sa_keyfile, impersonate)
    group_members = get_group_members(service, group_email)

    to_add = sorted(db_emails - group_members)
    to_remove = sorted(group_members - db_emails)

    print("\n=== Sync Summary ===")
    print(f"DB users:      {len(db_emails)}")
    print(f"Group members: {len(group_members)}")
    print(f"To add:        {len(to_add)}")
    print(f"To remove:     {len(to_remove)}")

    if not apply_changes:
        print("\nDRY RUN — no changes will be made.")
        return

    print("\nApplying changes...")

    for email in to_add:
        add_member(service, group_email, email)

    for email in to_remove:
        remove_member(service, group_email, email)

    print("\nDone.")


if __name__ == "__main__":
    try:
        main()
    except RuntimeError as e:
        logging.error(str(e))
        sys.exit(1)
