import csv
import os
import psycopg2
from psycopg2.extras import execute_batch
import sys
from dotenv import load_dotenv

# Configuration
CSV_FILE = 'posts_to_import.sample.csv'
DB_CONFIG = {
    'host': os.environ['PG_HOST'],
    'port': int(os.environ['PG_PORT']),
    'dbname': os.environ['PG_DBNAME'],
    'user': os.environ['PG_USER'],
    'password': os.environ['PG_PASSWORD']
}

# Required columns
REQUIRED_COLUMNS = ['org_id', 'location_id', 'start_date', 'user_id']

# Determine environment
ENV = 'staging'
if len(sys.argv) > 1 and sys.argv[1].lower() == 'prod':
    ENV = 'prod'

# Load appropriate .env file
env_file = f'.env.{ENV}'
load_dotenv(env_file)

def validate_row(row):
    """Ensure required fields are present and not empty."""
    for col in REQUIRED_COLUMNS:
        if not row.get(col):
            return False, f"Missing required column: {col}"
    return True, None


def connect_db():
    return psycopg2.connect(**DB_CONFIG)


def check_ids_exist(cur, rows):
    """Check that org_id, location_id, series_id, and user_id exist in the DB."""
    org_ids = set(row['org_id'] for row in rows if row['org_id'])
    location_ids = set(row['location_id'] for row in rows if row['location_id'])
    series_ids = set(row['series_id'] for row in rows if row['series_id'])
    user_ids = set(row['user_id'] for row in rows if row['user_id'])

    def check(table, id_set):
        if not id_set:
            return set()
        cur.execute(f"SELECT id FROM {table} WHERE id = ANY(%s)", (list(id_set),))
        found = set(r[0] for r in cur.fetchall())
        missing = id_set - found
        return missing

    missing_orgs = check('orgs', org_ids)
    missing_locations = check('locations', location_ids)
    missing_series = check('event_instances', series_ids)
    missing_users = check('users', user_ids)

    errors = []
    if missing_orgs:
        errors.append(f"Missing org_id(s): {sorted(missing_orgs)}")
    if missing_locations:
        errors.append(f"Missing location_id(s): {sorted(missing_locations)}")
    if missing_series:
        errors.append(f"Missing series_id(s): {sorted(missing_series)}")
    if missing_users:
        errors.append(f"Missing user_id(s): {sorted(missing_users)}")
    return errors


def insert_event_instances(cur, rows):
    """Insert unique events and return a mapping from (org_id, location_id, series_id, start_date, start_time, name) to new event_instance id."""
    event_keys = set()
    for row in rows:
        key = (
            row['org_id'], row['location_id'], row.get('series_id', ''),
            row['start_date'], row.get('start_time', ''), row.get('name', '')
        )
        event_keys.add(key)

    event_map = {}
    for key in event_keys:
        org_id, location_id, series_id, start_date, start_time, name = key
        cur.execute(
            """
            INSERT INTO event_instances (org_id, location_id, series_id, start_date, start_time, name)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (org_id, location_id, series_id if series_id else None, start_date, start_time if start_time else None, name if name else None)
        )
        event_id = cur.fetchone()[0]
        event_map[key] = event_id
    return event_map


def insert_attendance(cur, rows, event_map):
    """Insert attendance rows and return a mapping from (event_instance_id, user_id, post_type) to attendance id."""
    attendance_map = {}
    for row in rows:
        key = (
            row['org_id'], row['location_id'], row.get('series_id', ''),
            row['start_date'], row.get('start_time', ''), row.get('name', '')
        )
        event_instance_id = event_map[key]
        user_id = row['user_id']
        post_type = row.get('post_type', '')
        cur.execute(
            """
            INSERT INTO attendance (event_instance_id, user_id, post_type)
            VALUES (%s, %s, %s)
            RETURNING id
            """,
            (event_instance_id, user_id, post_type if post_type else None)
        )
        attendance_id = cur.fetchone()[0]
        attendance_map[(event_instance_id, user_id, post_type)] = attendance_id
    return attendance_map


def insert_attendance_x_types(cur, rows, attendance_map):
    """Insert attendance_x_attendance_types rows for Q/Co-Q types."""
    for row in rows:
        post_type = row.get('post_type', '')
        if post_type in ('Q', 'Co-Q'):
            key = (
                row['org_id'], row['location_id'], row.get('series_id', ''),
                row['start_date'], row.get('start_time', ''), row.get('name', ''),
                row['user_id'], post_type
            )
            event_key = key[:-2]
            event_instance_id = None
            for k in attendance_map:
                if k[0] == event_instance_id and k[1] == row['user_id'] and k[2] == post_type:
                    attendance_id = attendance_map[k]
                    break
            else:
                continue
            # Map post_type to attendance_type_id
            attendance_type_id = 1 if post_type == 'Q' else 2  # Example: 1=Q, 2=Co-Q
            cur.execute(
                "INSERT INTO attendance_x_attendance_types (attendance_id, attendance_type_id) VALUES (%s, %s)",
                (attendance_id, attendance_type_id)
            )

def main():
    # Read CSV
    with open(CSV_FILE, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    # Validate
    for i, row in enumerate(rows):
        valid, error = validate_row(row)
        if not valid:
            print(f"Row {i+1} error: {error}")
            return

    # Connect to DB
    conn = connect_db()
    cur = conn.cursor()

    # Pre-check IDs exist in DB
    errors = check_ids_exist(cur, rows)
    if errors:
        for error in errors:
            print(error)
        return

    # Insert event_instances
    event_map = insert_event_instances(cur, rows)
    # Insert attendance
    attendance_map = insert_attendance(cur, rows, event_map)
    # Insert attendance_x_attendance_types
    insert_attendance_x_types(cur, rows, attendance_map)

    conn.commit()
    print("Import completed successfully.")

    cur.close()
    conn.close()


if __name__ == '__main__':
    main()
