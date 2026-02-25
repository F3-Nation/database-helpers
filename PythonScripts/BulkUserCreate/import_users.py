import csv
import os
import psycopg2
import sys
from dotenv import load_dotenv

# Configuration
# Required columns
REQUIRED_COLUMNS = ['f3_name', 'email', 'home_region_id']

# Parse command line arguments
def parse_arguments():
    """Parse command line arguments. Usage: python import_users.py <csv_file> [prod]"""
    if len(sys.argv) < 2:
        print("Usage: python import_users.py <csv_file> [prod]")
        print("  <csv_file>: Path to the CSV file to import")
        print("  [prod]:     Optional. Use production environment (default: staging)")
        sys.exit(1)
    
    csv_file = sys.argv[1]
    env = 'staging'
    
    if len(sys.argv) > 2 and sys.argv[2].lower() == 'prod':
        env = 'prod'
    
    return csv_file, env

CSV_FILE, ENV = parse_arguments()

# Load appropriate .env file from the script's directory
script_dir = os.path.dirname(os.path.abspath(__file__))
env_file = os.path.join(script_dir, f'.env.{ENV}')
load_dotenv(env_file)

# Initialize DB_CONFIG after loading env
DB_CONFIG = {
    'host': os.environ.get('PG_HOST'),
    'port': int(os.environ.get('PG_PORT', 5432)),
    'dbname': os.environ.get('PG_DBNAME'),
    'user': os.environ.get('PG_USER'),
    'password': os.environ.get('PG_PASSWORD')
}


def validate_row(row):
    """Ensure required fields are present and not empty."""
    for col in REQUIRED_COLUMNS:
        if col not in row or not row.get(col):
            return False, f"Missing or empty required column: {col}"
    return True, None


def connect_db():
    """Connect to database."""
    return psycopg2.connect(**DB_CONFIG)


def check_home_region_ids_exist(cur, rows):
    """Check that home_region_id exists in the orgs table (regions are orgs with type='region')."""
    home_region_ids = set(row['home_region_id'] for row in rows if row.get('home_region_id'))
    
    if not home_region_ids:
        return []
    
    # Convert to integers
    try:
        home_region_ids_int = [int(hrid) for hrid in home_region_ids]
    except ValueError as e:
        return [f"Invalid home_region_id (must be integers): {e}"]
    
    # Check if all home_region_ids exist in orgs table
    cur.execute(f"SELECT id FROM orgs WHERE id = ANY(%s) and org_type='region'", (home_region_ids_int,))
    found = set(r[0] for r in cur.fetchall())
    missing = set(home_region_ids_int) - found
    
    if missing:
        return [f"Missing home_region_id(s): {sorted(missing)}"]
    return []


def upsert_users(cur, rows):
    """
    Upsert users by email and return a list of rows with user IDs added.
    Uses INSERT ... ON CONFLICT DO UPDATE for the upsert.
    """
    result_rows = []
    total = len(rows)
    
    print(f"\nProcessing {total} user(s)...")
    
    for idx, row in enumerate(rows, 1):
        f3_name = row.get('f3_name')
        first_name = row.get('first_name') or None
        last_name = row.get('last_name') or None
        email = row.get('email')
        home_region_id = row.get('home_region_id')
        
        print(f"  [{idx}/{total}] Processing {f3_name} ({email})...", end=' ')
        
        try:
            home_region_id_int = int(home_region_id) if home_region_id else None
        except ValueError:
            print(f"ERROR")
            return None, f"Invalid home_region_id (must be integer): {home_region_id}"
        
        cur.execute(
            """
            INSERT INTO users (f3_name, first_name, last_name, email, home_region_id, status)
            VALUES (%s, %s, %s, %s, %s, 'active')
            ON CONFLICT (email) DO UPDATE SET
                f3_name = COALESCE(EXCLUDED.f3_name, users.f3_name),
                first_name = COALESCE(EXCLUDED.first_name, users.first_name),
                last_name = COALESCE(EXCLUDED.last_name, users.last_name),
                home_region_id = COALESCE(EXCLUDED.home_region_id, users.home_region_id)
            RETURNING id
            """,
            (f3_name, first_name, last_name, email, home_region_id_int)
        )
        user_id = cur.fetchone()[0]
        
        print(f"✓ (ID: {user_id})")
        
        # Add the user_id to the row for output
        output_row = row.copy()
        output_row['id'] = user_id
        result_rows.append(output_row)
    
    return result_rows, None


def write_output_csv(rows, input_filename):
    """Write rows to output CSV file."""
    # Generate output filename: users_to_import.sample.csv -> users_to_import.sample_output.csv
    name, ext = os.path.splitext(input_filename)
    output_filename = f"{name}_output{ext}"
    
    if not rows:
        print(f"  ⚠ No rows to write to output.")
        return output_filename
    
    # Get fieldnames from first row, with 'id' at the end
    fieldnames = [key for key in rows[0].keys() if key != 'id']
    fieldnames.append('id')
    
    with open(output_filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    
    print(f"  ✓ Output written to {output_filename}")
    return output_filename


def main():
    print(f"=== F3 Nation User Import ===")
    print(f"Environment: {ENV}")
    print(f"CSV File: {CSV_FILE}\n")
    
    # Check if CSV file exists
    if not os.path.exists(CSV_FILE):
        print(f"Error: {CSV_FILE} not found.")
        return
    
    # Read CSV
    print("Reading CSV file...")
    with open(CSV_FILE, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    if not rows:
        print(f"Error: {CSV_FILE} is empty.")
        return
    
    print(f"Found {len(rows)} row(s) in CSV.\n")
    
    # Validate rows
    print("Validating rows...")
    for i, row in enumerate(rows):
        valid, error = validate_row(row)
        if not valid:
            print(f"  ✗ Row {i+1} error: {error}")
            return
    print(f"  ✓ All rows validated successfully.\n")
    
    # Connect to DB
    print("Connecting to database...")
    try:
        conn = connect_db()
        print(f"  ✓ Connected to {DB_CONFIG['host']}/{DB_CONFIG['dbname']}\n")
    except psycopg2.Error as e:
        print(f"  ✗ Error connecting to database: {e}")
        return
    
    cur = conn.cursor()
    
    try:
        # Pre-check home_region_ids exist in DB
        print("Validating home_region_ids...")
        errors = check_home_region_ids_exist(cur, rows)
        if errors:
            for error in errors:
                print(f"  ✗ {error}")
            return
        print(f"  ✓ All home_region_ids are valid.\n")
        
        # Upsert users
        result_rows, error = upsert_users(cur, rows)
        if error or result_rows is None:
            print(f"Error upserting users: {error}")
            return
        
        # Commit transaction
        print("\nCommitting transaction...")
        conn.commit()
        print("  ✓ Transaction committed.\n")
        
        # Write output CSV
        print("Writing output CSV...")
        output_file = write_output_csv(result_rows, CSV_FILE)
        
        print(f"\n=== Import Complete ===")
        print(f"Successfully imported {len(result_rows)} user(s).")
        print(f"Output file: {output_file}")
    
    except psycopg2.Error as e:
        conn.rollback()
        print(f"\n✗ Database error: {e}")
        print("Transaction rolled back.")
    
    finally:
        cur.close()
        conn.close()


if __name__ == '__main__':
    main()
