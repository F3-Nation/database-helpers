# Backblast Import

**THIS IS NOT FOR PAXMINER DATA.** PAXminer data will automatically be imported to the new F3 database when you migrate. This is for data that you maintained in a spreadsheet or region database.

You have to provide the data in a very specific format in order to get it imported.

It has to be a csv file. There has to be 1 row per PAX per workout. Example: if you had 3 guys at your first workout and 8 guys at your 2nd workout, there should be 11 rows.

## Usage

**The CSV file path is required.**

```bash
# Dry-run mode against staging (validates and shows what would be imported, then rolls back)
python import_backblasts.py --input_csv /path/to/harrisburg_backblasts.csv

# Dry-run mode for production environment with custom CSV
python import_backblasts.py --input_csv harrisburg_backblasts.csv --environment prod

# Commit changes to staging database
python import_backblasts.py --input_csv harrisburg_backblasts.csv --commit

# Commit changes to production database
python import_backblasts.py --input_csv harrisburg_backblasts.csv --environment prod --commit

# Custom log file
python import_backblasts.py --input_csv harrisburg_backblasts.csv --log_file backblast_import.log
```

**Arguments:**
- `--input_csv` (REQUIRED) - Path to CSV file
- `--environment` - `staging` or `prod` (defaults to `staging`)
- `--commit` - Commit changes to database (defaults to dry-run with rollback)
- `--log_file` - Path to log file (defaults to `import_backblasts.log`)

By default, the script runs in **dry-run mode** with automatic rollback. Use the `--commit` flag to actually persist changes to the database. This allows you to validate your data and see any errors before making permanent changes.

## Import Summary & Backout Plan

After each import run (successful or failed), the script generates two important artifacts:

### 1. Detailed Import Summary
The log file shows a comprehensive summary at the end:
```
================================================================================
IMPORT SUMMARY
================================================================================
Total rows processed: 1995
Unique events created: 686
Attendance records created: 1995
Q assignments: 686
Co-Q assignments: 5
Unique organizations: 5
Unique locations: 5
Oldest event date: 2021-04-01
Most recent event date: 2023-12-30
```

This helps you quickly verify the scope of your import including date ranges and geographic distribution.

### 2. Automatic SQL Backout File
A timestamped SQL rollback script is automatically generated in the same directory (e.g., `backout_prod_20260228_031233.sql`). This file contains all the SQL DELETE commands needed to completely remove the imported data if something goes wrong:

```bash
# To rollback an import, simply execute:
psql -f backout_prod_20260228_031233.sql
```

**Important:** The backout file is generated:
- ✓ On successful imports (after committed)
- ✓ On failed imports (with whatever data was inserted before the failure)
- ✓ On dry-run tests (with the would-be deleted data)

This allows you to safely test and commit imports knowing you can always roll back if needed.

## Backblast Import CSV Columns

The following table explains the columns required in the `posts_to_import.sample.csv` file for importing backblasts and attendance. Column names have to be exact (case-sensative).

| Column         | Required? | Description                                                      | Source/How to Obtain                    |
|---------------|-----------|------------------------------------------------------------------|------------------------------------------|
| org_id        | **Yes**   | Database ID (integer) of the AO.                                 | https://map.f3nation.com/admin/aos       |
| location_id   | **Yes**   | Database ID (integer) of the Location.                           | https://map.f3nation.com/admin/locations |
| series_id     | No        | Database ID (integer) of the Event (if there is one)             | https://map.f3nation.com/admin/workouts  |
| start_date    | **Yes**   | Date of the event (YYYY-MM-DD)                                   |                                          |
| start_time    | No        | Start time of the event (HHMM, 24hr, e.g., 0530 for 5:30am)      |                                          |
| name          | No        | Name/title of the event (Defaults to AO name)                    |                                          |
| description   | No        | Description of the event (You most likely don't have one)        |                                          |
| backblast     | No        | Backblast text (detailed workout notes)                          |                                          |
| user_id       | **Yes**   | Database ID (integer) of the PAX that attended or Q'd            | https://map.f3nation.com/admin/users/all |
| post_type     | No        | Q, Co-Q, or nothing*                                             |                                          |

**Notes:**
 
- If `post_type` is Q (case sensative), it will log that PAX as having Q'd
- If `post_type` is Co-Q (case sensative), it will log that PAX as having Co-Q'd
- If `post_type` has anything other than Q or Co-Q (case sensative), it will log that PAX as a normal post.
- `org_id`, `location_id`, `series_id`, and `user_id` need to be integers that exist in the F3 Nation database (as visualized in the Admin portal). Prior to import, the script will ensure they all exist. If they don't, the import will not run. If you need help finding IDs - or if you need a database export to help you match IDs, talk to a Nation Admin.
- An Event will be determined by a unique combination of `org_id`, `location_id`, `series_id`, `start_date`, `start_time`, `name`, `description`, `backblast`. If any of these fields differ, the import will treat is as 2 different events.
- Each PAX may only be on an event once. I.e., do not list them as Q and later with no post_type for the same event. The import will not run if there are any duplicates.
- There must be 1 and only 1 Q per event