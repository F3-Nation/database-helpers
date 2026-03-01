import argparse
import csv
import os
import psycopg2
from psycopg2.extras import execute_batch
import sys
import time
from datetime import datetime, timezone
from dotenv import load_dotenv

# Required columns
REQUIRED_COLUMNS = ['org_id', 'location_id', 'start_date', 'user_id']


class TeeStream:
    def __init__(self, primary, secondary):
        self.primary = primary
        self.secondary = secondary

    def write(self, data):
        self.primary.write(data)
        self.secondary.write(data)
        self.primary.flush()
        self.secondary.flush()

    def flush(self):
        self.primary.flush()
        self.secondary.flush()


def parse_args():
    parser = argparse.ArgumentParser(description='Import backblast and attendance data.')
    parser.add_argument('--input_csv', required=True, help='Path to the input CSV file')
    parser.add_argument('--environment', choices=['staging', 'prod'], default='staging', help='Target environment (default is staging)')
    parser.add_argument('--commit', action='store_true', help='Commit changes (default is dry-run/rollback)')
    parser.add_argument('--log_file', default='import_backblasts.log', help='Path to log file')
    return parser.parse_args()


args = parse_args()
ENV = args.environment
COMMIT = args.commit
CSV_FILE = args.input_csv
LOG_FILE = args.log_file

log_handle = open(LOG_FILE, 'w', encoding='utf-8')
log_handle.write(f"Log started: {datetime.now(timezone.utc).isoformat()}Z\n")
log_handle.flush()
sys.stdout = TeeStream(sys.stdout, log_handle)
sys.stderr = TeeStream(sys.stderr, log_handle)

# Load appropriate .env file
env_file = f'.env.{ENV}'
load_dotenv(env_file)

# Configuration (after loading .env)
DB_CONFIG = {
    'host': os.environ['PG_HOST'],
    'port': int(os.environ['PG_PORT']),
    'dbname': os.environ['PG_DBNAME'],
    'user': os.environ['PG_USER'],
    'password': os.environ['PG_PASSWORD']
}

def enrich_rows_with_event_keys(rows):
    """Add event_key to each row for consistent key building throughout the script."""
    for row in rows:
        name = row.get('name', '').strip() or 'Imported Event'
        description = row.get('description', '').strip()
        row['_event_key'] = (
            row['org_id'], row['location_id'], row.get('series_id', ''),
            row['start_date'], row.get('start_time', ''), name,
            description, row.get('backblast', '')
        )
    return rows


def validate_row(row):
    """Ensure required fields are present and not empty."""
    for col in REQUIRED_COLUMNS:
        value = row.get(col, '').strip()
        if not value or value == '#N/A':
            return False, f"Missing or invalid required column: {col} (value: '{row.get(col)}')"
    return True, None


def connect_db():
    return psycopg2.connect(**DB_CONFIG)


def check_ids_exist(cur, rows):
    """Check that org_id, location_id, series_id, and user_id exist in the DB."""
    org_ids = set()
    location_ids = set()
    series_ids = set()
    user_ids = set()
    
    # Convert string IDs to integers, skip invalid values
    for row in rows:
        if row.get('org_id') and row['org_id'].strip() and row['org_id'] != '#N/A':
            try:
                org_ids.add(int(row['org_id']))
            except ValueError:
                pass
        if row.get('location_id') and row['location_id'].strip():
            try:
                location_ids.add(int(row['location_id']))
            except ValueError:
                pass
        if row.get('series_id') and row['series_id'].strip():
            try:
                series_ids.add(int(row['series_id']))
            except ValueError:
                pass
        if row.get('user_id') and row['user_id'].strip():
            try:
                user_ids.add(int(row['user_id']))
            except ValueError:
                pass

    print(f"\n[ID VALIDATION] Checking {len(org_ids)} unique org_id(s): {sorted(org_ids)}")
    print(f"[ID VALIDATION] Checking {len(location_ids)} unique location_id(s): {sorted(location_ids)}")
    print(f"[ID VALIDATION] Checking {len(series_ids)} unique series_id(s): {sorted(series_ids)}")
    print(f"[ID VALIDATION] Checking {len(user_ids)} unique user_id(s): {sorted(user_ids)}")

    def check(table, id_set):
        if not id_set:
            return set()
        cur.execute(f"SELECT id FROM {table} WHERE id = ANY(%s)", (list(id_set),))
        found = set(r[0] for r in cur.fetchall())
        missing = id_set - found
        return missing

    missing_orgs = check('orgs', org_ids)
    missing_locations = check('locations', location_ids)
    missing_series = check('events', series_ids)
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


def check_attendance_duplicates(rows):
    """Check for duplicate attendance records (same user at same event) in the CSV."""
    seen_attendance = {}  # Maps (org_id, location_id, series_id, start_date, name, user_id) -> list of row indices
    duplicates = []
    
    for row_idx, row in enumerate(rows, 1):
        attendance_key = (
            row['org_id'],
            row['location_id'],
            row.get('series_id', ''),
            row['start_date'],
            row.get('name', ''),
            row['user_id']
        )
        
        if attendance_key not in seen_attendance:
            seen_attendance[attendance_key] = []
        
        seen_attendance[attendance_key].append(row_idx)
    
    # Collect duplicates
    for attendance_key, row_indices in seen_attendance.items():
        if len(row_indices) > 1:
            org_id, location_id, series_id, start_date, name, user_id = attendance_key
            duplicates.append({
                'org_id': org_id,
                'location_id': location_id,
                'series_id': series_id,
                'start_date': start_date,
                'name': name,
                'user_id': user_id,
                'row_indices': row_indices,
                'count': len(row_indices)
            })
    
    return duplicates


def check_q_per_event(rows):
    """Check that each event has exactly 1 Q (leader)."""
    events_q_count = {}  # Maps event key -> list of (row_idx, user_id) with post_type='Q'
    
    for row_idx, row in enumerate(rows, 1):
        event_key = row['_event_key']
        
        if event_key not in events_q_count:
            events_q_count[event_key] = []
        
        post_type = row.get('post_type', '')
        if post_type == 'Q':
            events_q_count[event_key].append((row_idx, row['user_id']))
    
    # Check for events with 0 or multiple Qs
    events_no_q = []
    events_multi_q = []
    
    for event_key, qs in events_q_count.items():
        org_id, location_id, series_id, start_date, start_time, name, description, backblast = event_key
        if len(qs) == 0:
            events_no_q.append({
                'org_id': org_id,
                'location_id': location_id,
                'series_id': series_id or 'None',
                'start_date': start_date,
                'start_time': start_time or 'N/A',
                'name': name,
                'description': description or 'N/A'
            })
        elif len(qs) > 1:
            events_multi_q.append({
                'org_id': org_id,
                'location_id': location_id,
                'series_id': series_id or 'None',
                'start_date': start_date,
                'start_time': start_time or 'N/A',
                'name': name,
                'description': description or 'N/A',
                'qs': [(row_idx, user_id) for row_idx, user_id in qs],
                'count': len(qs)
            })
    
    return events_no_q, events_multi_q


def insert_event_instances(cur, rows, id_tracker):
    """Insert unique events and return a mapping from (org_id, location_id, series_id, start_date, start_time, name, description, backblast) to new event_instance id."""
    event_keys = set()
    for row in rows:
        key = row['_event_key']
        event_keys.add(key)
    
    # Calculate pax_count for each event by counting matching rows
    pax_counts = {}
    for key in event_keys:
        pax_counts[key] = sum(1 for row in rows if row['_event_key'] == key)

    print(f"\n[EVENT INSTANCES] Found {len(event_keys)} unique event(s) to create:")
    
    event_map = {}
    for i, key in enumerate(event_keys, 1):
        org_id, location_id, series_id, start_date, start_time, name, description, backblast = key
        pax_count = pax_counts[key]
        print(f"  [{i}] org_id={org_id}, location_id={location_id}, series_id={series_id or 'None'}, date={start_date}, time={start_time or 'N/A'}, name={name or 'N/A'}, pax_count={pax_count}")
        if description:
            print(f"      description: {description[:100]}{'...' if len(description) > 100 else ''}")
        if backblast:
            print(f"      backblast: {backblast[:100]}{'...' if len(backblast) > 100 else ''}")
        
        cur.execute(
            """
            INSERT INTO event_instances (
                org_id, location_id, series_id, is_active, highlight, start_date, start_time, name, description, backblast, pax_count
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                org_id,
                location_id,
                series_id if series_id else None,
                True,
                False,
                start_date,
                start_time if start_time else None,
                name,
                description if description else None,
                backblast if backblast else None,
                pax_count,
            )
        )
        event_id = cur.fetchone()[0]
        event_map[key] = event_id
        id_tracker['event_instance_ids'].append(event_id)
        print(f"      -> event_instance_id: {event_id}")
    
    return event_map


def insert_attendance(cur, rows, event_map, id_tracker):
    """Insert attendance rows and return a mapping from (event_instance_id, user_id) to attendance id."""
    attendance_map = {}
    
    print(f"\n[ATTENDANCE] Creating {len(rows)} attendance record(s):")
    
    for i, row in enumerate(rows, 1):
        key = row['_event_key']
        event_instance_id = event_map[key]
        user_id = row['user_id']
        post_type = row.get('post_type', '')
        
        print(f"  [{i}] event_instance_id={event_instance_id}, user_id={user_id}, post_type={post_type or 'normal'}")
        
        cur.execute(
            """
            INSERT INTO attendance (event_instance_id, user_id, is_planned)
            VALUES (%s, %s, %s)
            RETURNING id
            """,
            (event_instance_id, user_id, False)
        )
        attendance_id = cur.fetchone()[0]
        attendance_map[(event_instance_id, user_id)] = attendance_id
        id_tracker['attendance_ids'].append(attendance_id)
    
    return attendance_map





def generate_backout_sql(id_tracker, csv_file, env):
    """Generate a SQL file with DELETE commands to rollback the import."""
    # Determine output path
    timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    sql_file = f'backout_{env}_{timestamp}.sql'
    
    with open(sql_file, 'w', encoding='utf-8') as f:
        f.write(f"-- Backout SQL for import from {csv_file}\n")
        f.write(f"-- Generated: {datetime.now(timezone.utc).isoformat()}Z\n")
        f.write(f"-- Environment: {env}\n")
        f.write(f"-- This file will rollback all inserted data\n\n")
        
        # Delete in reverse order of insertion to respect foreign keys
        if id_tracker['attendance_with_types']:
            f.write("-- Delete attendance_x_attendance_types records\n")
            attendance_with_types_str = ','.join(str(id) for id in id_tracker['attendance_with_types'])
            f.write(f"DELETE FROM attendance_x_attendance_types WHERE attendance_id IN ({attendance_with_types_str});\n\n")
        
        if id_tracker['attendance_ids']:
            f.write("-- Delete attendance records\n")
            attendance_ids_str = ','.join(str(id) for id in id_tracker['attendance_ids'])
            f.write(f"DELETE FROM attendance WHERE id IN ({attendance_ids_str});\n\n")
        
        if id_tracker['event_instance_ids']:
            f.write("-- Delete event_instances records\n")
            event_instance_ids_str = ','.join(str(id) for id in id_tracker['event_instance_ids'])
            f.write(f"DELETE FROM event_instances WHERE id IN ({event_instance_ids_str});\n\n")
        
        f.write("-- Summary of deleted records\n")
        f.write(f"-- Event instances deleted: {len(id_tracker['event_instance_ids'])}\n")
        f.write(f"-- Attendance records deleted: {len(id_tracker['attendance_ids'])}\n")
        f.write(f"-- Attendance type assignments deleted: {len(id_tracker['attendance_with_types'])}\n")
    
    return sql_file



def insert_attendance_x_types(cur, rows, event_map, attendance_map, id_tracker):
    """Insert attendance_x_attendance_types rows for Q/Co-Q types."""
    q_count = 0
    coq_count = 0
    attendance_with_types = set()
    
    for row in rows:
        post_type = row.get('post_type', '')
        if post_type in ('Q', 'Co-Q'):
            # Get the event key to look up event_instance_id
            event_key = row['_event_key']
            event_instance_id = event_map[event_key]
            
            # Look up the attendance_id
            attendance_key = (event_instance_id, row['user_id'])
            if attendance_key in attendance_map:
                attendance_id = attendance_map[attendance_key]
                attendance_with_types.add(attendance_id)
                
                # Map post_type to attendance_type_id
                attendance_type_id = 2 if post_type == 'Q' else 3  # Example: 2=Q, 3=Co-Q
                print(f"  [ATTENDANCE TYPES] user_id={row['user_id']}, type={post_type}, attendance_type_id={attendance_type_id}")
                
                cur.execute(
                    "INSERT INTO attendance_x_attendance_types (attendance_id, attendance_type_id) VALUES (%s, %s)",
                    (attendance_id, attendance_type_id)
                )
                
                if post_type == 'Q':
                    q_count += 1
                else:
                    coq_count += 1
    
    # Store attendance_ids that had types assigned for backout
    id_tracker['attendance_with_types'] = list(attendance_with_types)
    return q_count, coq_count

def main():
    # Ensure CSV_FILE is not None (should be caught earlier, but satisfies type checker)
    assert CSV_FILE is not None, "CSV_FILE must be provided"
    
    # ID tracking for backout
    id_tracker = {
        'event_instance_ids': [],
        'attendance_ids': [],
        'attendance_with_types': []
    }
    
    # Performance tracking
    timers = {}
    start_total = time.time()
    
    # Print startup info
    print("=" * 80)
    print("BACKBLAST IMPORT SCRIPT")
    print("=" * 80)
    print(f"Environment: {ENV.upper()}")
    print(f"Mode: {'DRY RUN (will rollback)' if not COMMIT else 'COMMIT MODE'}")
    print(f"CSV File: {CSV_FILE}")
    print(f"Log File: {LOG_FILE}")
    print("=" * 80)
    
    # Read CSV and validate
    start_ingest = time.time()
    
    # Read CSV
    with open(CSV_FILE, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    print(f"\n[CSV] Loaded {len(rows)} row(s) from {CSV_FILE}")
    
    # Enrich rows with event keys for consistent use throughout
    enrich_rows_with_event_keys(rows)

    # Validate
    print(f"\n[VALIDATION] Validating {len(rows)} row(s)...")
    validation_errors = 0
    for i, row in enumerate(rows):
        valid, error = validate_row(row)
        if not valid:
            print(f"  Row {i+1} ERROR: {error}")
            validation_errors += 1
    
    if validation_errors > 0:
        print(f"\n[ERROR] {validation_errors} validation error(s) found. Aborting.")
        log_handle.close()
        return
    
    print(f"  ✓ All {len(rows)} row(s) passed validation")

    # Check for duplicate attendance records
    print(f"\n[ATTENDANCE DUPLICATES] Checking for duplicate attendance records...")
    duplicates = check_attendance_duplicates(rows)
    if duplicates:
        print(f"[ERROR] Found {len(duplicates)} duplicate attendance record(s):")
        for dup in duplicates:
            print(f"  Org {dup['org_id']}, Location {dup['location_id']}, Date {dup['start_date']}, User {dup['user_id']}: appears in rows {dup['row_indices']} ({dup['count']} times)")
        print("\nAbort: Cannot proceed with duplicate attendance records. Fix the CSV and retry.")
        log_handle.close()
        return
    
    print(f"  ✓ No duplicate attendance records found")

    # Check that each event has exactly 1 Q (leader)
    print(f"\n[Q VALIDATION] Checking that each event has exactly 1 Q (leader)...")
    events_no_q, events_multi_q = check_q_per_event(rows)
    
    if events_no_q or events_multi_q:
        if events_no_q:
            print(f"[ERROR] Found {len(events_no_q)} event(s) with NO Q (leader):")
            for event in events_no_q:
                print(f"  {event['start_date']} {event['start_time']} - {event['name']} (Org {event['org_id']}, Location {event['location_id']})")
                if event['description'] != 'N/A':
                    print(f"    Description: {event['description'][:80]}")
        
        if events_multi_q:
            print(f"[ERROR] Found {len(events_multi_q)} event(s) with MULTIPLE Qs (leaders):")
            for event in events_multi_q:
                qs_info = ", ".join([f"user_id {user_id} (row {row_idx})" for row_idx, user_id in event['qs']])
                print(f"  {event['start_date']} {event['start_time']} - {event['name']} (Org {event['org_id']}, Location {event['location_id']})")
                print(f"    Qs: {qs_info}")
        
        print("\nAbort: Cannot proceed with Q validation errors. Fix the CSV and retry.")
        log_handle.close()
        return
    
    print(f"  ✓ All events have exactly 1 Q")
    
    timers['ingest_and_validate'] = time.time() - start_ingest

    # Connect to DB
    print(f"\n[DATABASE] Connecting to {ENV} database...")
    conn = connect_db()
    cur = conn.cursor()
    print("  ✓ Connected")

    try:
        # Pre-check IDs exist in DB
        errors = check_ids_exist(cur, rows)
        if errors:
            print(f"\n[ERROR] ID validation failed:")
            for error in errors:
                print(f"  ✗ {error}")
            conn.rollback()
            return
        
        print(f"\n  ✓ All IDs validated successfully")

        # Insert event_instances
        start_events = time.time()
        event_map = insert_event_instances(cur, rows, id_tracker)
        timers['event_instances'] = time.time() - start_events
        
        # Insert attendance
        start_attendance = time.time()
        attendance_map = insert_attendance(cur, rows, event_map, id_tracker)
        timers['attendance'] = time.time() - start_attendance
        
        # Insert attendance_x_types
        print(f"\n[ATTENDANCE TYPES] Processing Q/Co-Q assignments:")
        start_types = time.time()
        q_count, coq_count = insert_attendance_x_types(cur, rows, event_map, attendance_map, id_tracker)
        timers['attendance_types'] = time.time() - start_types
        print(f"  ✓ Created {q_count} Q assignment(s), {coq_count} Co-Q assignment(s)")

        # Calculate summary statistics
        org_ids = set()
        location_ids = set()
        event_dates = []
        
        for event_key in event_map.keys():
            org_id, location_id, series_id, start_date, start_time, name, description, backblast = event_key
            org_ids.add(org_id)
            location_ids.add(location_id)
            event_dates.append(start_date)
        
        oldest_date = min(event_dates) if event_dates else "N/A"
        newest_date = max(event_dates) if event_dates else "N/A"
        unique_orgs = len(org_ids)
        unique_locations = len(location_ids)
        
        # Print summary
        print("\n" + "=" * 80)
        print("IMPORT SUMMARY")
        print("=" * 80)
        print(f"Total rows processed: {len(rows)}")
        print(f"Unique events created: {len(event_map)}")
        print(f"Attendance records created: {len(attendance_map)}")
        print(f"Q assignments: {q_count}")
        print(f"Co-Q assignments: {coq_count}")
        print(f"Unique organizations: {unique_orgs}")
        print(f"Unique locations: {unique_locations}")
        print(f"Oldest event date: {oldest_date}")
        print(f"Most recent event date: {newest_date}")
        print("\n" + "PERFORMANCE METRICS")
        print("-" * 80)
        print(f"Ingest & Validate:    {timers['ingest_and_validate']:8.2f}s")
        print(f"Event Instances:      {timers['event_instances']:8.2f}s")
        print(f"Attendance:           {timers['attendance']:8.2f}s")
        print(f"Attendance Types:     {timers['attendance_types']:8.2f}s")
        print(f"TOTAL:                {time.time() - start_total:8.2f}s")
        print("=" * 80)

        # Commit or rollback based on COMMIT flag
        if COMMIT:
            conn.commit()
            print("\n✓ SUCCESS: Import completed and COMMITTED to database.")
        else:
            conn.rollback()
            print("\n✓ DRY RUN: Import completed successfully (transaction rolled back).")
            print("  Use the --commit flag to persist these changes to the database.")

    except Exception as e:
        conn.rollback()
        print(f"\n[ERROR] Import failed with exception: {e}")
        raise
    finally:
        cur.close()
        conn.close()
        
        # Generate backout SQL file
        backout_file = generate_backout_sql(id_tracker, CSV_FILE, ENV)
        print(f"\n[BACKOUT] SQL rollback file generated: {backout_file}")
        print(f"  To rollback this import, execute: psql -f {backout_file}")
        
        # Restore original stdout/stderr before closing log file
        sys.stdout = sys.__stdout__
        sys.stderr = sys.__stderr__
        log_handle.close()


if __name__ == '__main__':
    main()
