# Bulk User Create

This script allows you to import a batch of users into the F3 Nation database. It reads user data from a CSV file, creates users if they don't exist, and updates existing users by email address.

## Prerequisites

1. Python 3.7+ installed
2. Install required dependencies:
   ```bash
   pip install psycopg2-binary python-dotenv
   ```
3. Database credentials configured in `.env.staging` or `.env.prod`

## Setup

### 1. Configure Environment File

Copy `.env.example` to `.env.staging` (for testing) or `.env.prod` (for production):

```bash
cp .env.example .env.staging
```

Then edit the file with your database credentials:

```
PG_HOST=your.database.host
PG_PORT=5432
PG_DBNAME=f3_nation
PG_USER=your_username
PG_PASSWORD=your_password
```

### 2. Prepare Your CSV File

Create a CSV file with the following columns (case-sensitive):

| Column         | Required? | Description                                                      | Example               |
|---|---|---|---|
| f3_name        | **Yes**   | User's F3 name (the name used in workouts)                       | Dash                  |
| first_name     | **No**   | User's first name                                                | John                  |
| last_name      | **No**   | User's last name                                                 | Smith                 |
| email          | **Yes**   | User's email address (UNIQUE - used to detect duplicates)       | john.smith@example.com|
| home_region_id | **Yes**   | Database ID (integer) of the region where user is homed         | 1                     |

**Important Notes:**

- **Email is the unique key**: If a user with the same email already exists, that user will be updated with the new data (f3_name, first_name, last_name, home_region_id). This prevents duplicate entries.
- `home_region_id` must be a valid region ID that already exists in the database. You can find region IDs in the Admin portal.

### 3. Run the Import

```bash
# For staging environment (default)
python import_users.py users_to_import.csv

# For production environment
python import_users.py users_to_import.csv prod
```

### 4. Review Output

The script will create an output CSV file named `users_to_import.sample_output.csv` (or with your input filename) containing:

- All original columns from your input file
- A new `id` column with the database ID assigned to each user

This output file can be used as reference or input for subsequent processes (like backblast imports).

## Error Handling

The script will stop and report errors if:

- Required columns are missing or empty
- `home_region_id` values don't exist in the database
- Database connection fails
- Other database errors occur

Review the error messages and update your CSV accordingly.

## Example

Given an input CSV file named `my_users.csv`:

```
f3_name,first_name,last_name,email,home_region_id
Dash,John,Smith,john.smith@example.com,1
Bones,,Doe,jane.doe@example.com,2
```

Run the import:
```bash
python import_users.py my_users.csv
```

This creates `my_users_output.csv`:
```
f3_name,first_name,last_name,email,home_region_id,id
Dash,John,Smith,john.smith@example.com,1,42
Bones,,Doe,jane.doe@example.com,2,43
```

## Troubleshooting

### "Missing home_region_id(s)"
Ensure the region IDs you're using exist in the database. Check the Admin portal for valid region IDs.

### "Database connection failed"
Verify your database credentials in the `.env.staging` or `.env.prod` file.

### CSV file not found
Make sure your CSV file is in the same directory as `import_users.py`.
