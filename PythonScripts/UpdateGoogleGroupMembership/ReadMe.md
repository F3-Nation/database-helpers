# Google Group Membership Sync

This script syncs a Google Group so its membership exactly matches the email addresses returned by a Postgres query.

It reads:

- environment variables from a `.env` file
- sync definitions from `UpdateAdminGoogleGroup.config`

Each config entry defines:

- which Google Group to target
- which SQL query to run against the database

The script compares the database result set to the current Google Group membership and then:

- adds missing members
- removes extra members

By default, it runs in dry-run mode and only prints the summary.

## Prerequisites

1. Python 3.7+
2. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

3. A Google service account JSON key with access to the Admin SDK
4. Domain-wide delegation configured for that service account
5. A Google Workspace admin user that the service account can impersonate
6. Database access via `DATABASE_URL`

## Get the Google Security Key (Service Account JSON)

Use these steps to create the key file used by `GOOGLE_SERVICE_ACCOUNT_JSON`.

1. Open Google Cloud Console and select the project you want to use.
2. Enable required APIs:
  - APIs & Services -> Library
  - Enable **Admin SDK API**
3. Create a service account:
  - IAM & Admin -> Service Accounts -> Create Service Account
  - Give it a name and create it
4. Enable domain-wide delegation on that service account:
  - Open the service account
  - Go to **Show domain-wide delegation** (or **Advanced settings** depending on UI)
  - Check **Enable G Suite Domain-wide Delegation** and save
  - Copy the **Client ID** shown there
5. Create and download a JSON key:
  - Service account -> Keys -> Add Key -> Create new key
  - Choose **JSON** and download the file
  - Store it securely (do not commit it to git)
6. In Google Workspace Admin Console, authorize the service account client ID:
  - Security -> Access and data control -> API controls -> Domain-wide delegation
  - Add new
  - Client ID: paste the service account Client ID
  - OAuth scopes (comma-separated):
    - `https://www.googleapis.com/auth/admin.directory.group`
    - `https://www.googleapis.com/auth/admin.directory.group.member`
7. Set your `.env` values:
  - `GOOGLE_SERVICE_ACCOUNT_JSON` = absolute path to the downloaded JSON key
  - `GOOGLE_IMPERSONATE_USER` = a Workspace admin user email in your domain

Note: If your Admin Console paths differ slightly, search for **Domain-wide delegation** in the admin search bar.

## Required Environment Variables

Create a `.env` file in this folder, or otherwise make these available in your shell:

```env
DATABASE_URL=postgresql://user:password@host:5432/dbname
GOOGLE_SERVICE_ACCOUNT_JSON=/absolute/path/to/service-account.json
GOOGLE_IMPERSONATE_USER=admin@yourdomain.com
CONFIG_ID=AllOrgAdmins
APPLY_CHANGES=false
VERBOSE=false
```

### Variable Notes

- `DATABASE_URL` - Postgres connection string used for the membership query
- `GOOGLE_SERVICE_ACCOUNT_JSON` - Path to the service account key file
- `GOOGLE_IMPERSONATE_USER` - Workspace admin user to impersonate
- `CONFIG_ID` - Required config ID to select which group/query mapping to run
- `APPLY_CHANGES` - Set to `true` to actually add and remove members
- `VERBOSE` - Set to `true` for debug logging

## Config File

The script expects a file named `UpdateAdminGoogleGroup.config` in the same directory.

It must contain a JSON array of config objects like this:

```json
[
  {
    "id": "AllOrgAdmins",
    "google_group": "org.admins@f3nation.com",
    "postsql_query": "SELECT DISTINCT u.email FROM roles_x_users_x_org rxuxo LEFT JOIN users u ON u.id = rxuxo.user_id;"
  }
]
```

### Config Fields

- `id` - Unique identifier used to select the config
- `google_group` - Email address of the Google Group to sync
- `postsql_query` - SQL query that must return a column named `email`

Important:

- The SQL query must return an `email` column
- Email addresses are normalized to lowercase before comparison
- Blank or null email values are ignored

## Running the Script

Run from this directory so the script can find `UpdateAdminGoogleGroup.config`.

This script is configured from environment variables (including `CONFIG_ID`).

### Dry Run

Set `APPLY_CHANGES` to false in the .env and then run:

```bash
python UpdateAdminGoogleGroup.py
```

Dry-run mode prints a summary like:

```text
=== Sync Summary ===
DB users:      25
Group members: 23
To add:        4
To remove:     2

DRY RUN - no changes will be made.
```

### Apply Changes

Set `APPLY_CHANGES` to true in the .env and then run:

```bash
python UpdateAdminGoogleGroup.py
```

## Behavior Notes

- The sync is authoritative: members not returned by the query are removed from the group
- The script uses Google Admin SDK Directory API membership endpoints
- Add/remove failures are logged and processing continues for the remaining users
- The script does not write a change log file; output is printed to stdout/stderr

## Troubleshooting

### "Missing required environment variable"

Make sure the required values are present in your `.env` file or current shell.

### "Configuration ID '...' not found"

Use one of the IDs defined in `UpdateAdminGoogleGroup.config`.

### Database error

Verify `DATABASE_URL` and confirm the SQL query runs successfully and returns an `email` column.

### Google API permission errors

Check all of the following:

- the service account JSON path is correct
- Admin SDK is enabled in the Google Cloud project
- domain-wide delegation is configured
- the impersonated user has sufficient Google Workspace admin permissions

### Config file not found

Run the script from this directory, or update the script to use an absolute config path.