# Get Centroids And Anomalies

This script analyzes active locations in the database by region and produces two CSV files:

- `region_centroids.csv`: centroid latitude/longitude per region
- `location_anomalies.csv`: location records flagged as coordinate anomalies

## What It Does

1. Loads DB connection settings from environment variables (`.env`).
2. Reads active locations and their region from the database.
3. Groups locations by region.
4. Computes a centroid for each region using valid coordinates.
5. Flags anomalies:
   - invalid coordinate (`null`, out-of-range, or `0,0`)
   - location spread outlier (z-score > 3)
   - extreme distance (> 300 miles from centroid)
6. Writes output CSV files in this folder.

## Requirements

- Python 3.7+
- Dependencies from `requirements.txt`
- Database access with the required env vars

Install dependencies:

```bash
pip install -r requirements.txt
```

## Environment Variables

This script is env-driven. There are no command-line inputs.

Required:

- `DB_NAME`
- `DB_USER`
- `DB_PASSWORD`
- `DB_HOST`

Optional:

- `DB_PORT` (defaults to `5432`)

Template file: `.env.example`

Example:

```env
DB_NAME=your_database_name
DB_USER=your_database_user
DB_PASSWORD=your_database_password
DB_HOST=your_database_host
DB_PORT=5432
```

## Run

Run from this directory:

```bash
python main.py
```

## Output Files

After a successful run, the script writes:

- `region_centroids.csv`
  - columns: `region_name`, `centroid_lat`, `centroid_lon`
  - regions with no valid coordinates have `centroid_lat`/`centroid_lon` as empty values

- `location_anomalies.csv`
  - columns: `region_name`, `location_name`, `anomaly_reason`
  - anomaly reasons include `invalid lat/long`, `location_spread_outlier`, and `extreme_distance`

## Notes

- Only active locations are analyzed (`where l.is_active`).
- Regions without an `org_id` are skipped.
- Distances are computed with geodesic miles.
