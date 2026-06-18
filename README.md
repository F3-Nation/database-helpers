# database-helpers
Scripts and queries to help DBQs manage the F3 Nation database

## The Database
All F3 Nation data is centralized in a postgres database hosted in GCP. We have two instances, one (f3data) for prod, and one (f3data_nonprod) for staging.

### Who should be connecting directly to the database
Probably not you. ALL of our data is in there. We need to protect it. There are only a few people that have access in case issues arise or changes need to be made that cannot be done via existing apps.

But there's still hope! we have an [API](https://api.f3nation.com/docs#description/introduction) as well as an [BigQuery](https://groups.google.com/u/3/a/f3nation.com/g/f3-data-analytics).

### Endpoints and credentials
It's probably not sure hard to find our database endpoints, but we're not going to broadcast them. Reach out on the F3 Nation Slack to the F3 IT Leadership if you think you need access

## Connecting via Cloud SQL Auth Proxy
Our database does not allow direct connections over the open internet. You must use the [Cloud SQL Auth Proxy](https://cloud.google.com/sql/docs/postgres/sql-proxy) to create a secure tunnel from your machine to Cloud SQL. The proxy handles TLS encryption and IAM-based authorization so you never need to expose the database to a public IP.

### How it works
The Auth Proxy runs as a background service on your machine and listens on `localhost`. Your tools (psql, DBeaver, Python scripts) connect to `localhost` and the proxy forwards traffic securely to Cloud SQL using your GCP credentials. Prod listens on port **5433** and staging on port **5434**.

### Prerequisites
- [Google Cloud CLI (`gcloud`)](https://cloud.google.com/sdk/docs/install) installed
- Your Google account must have the **Cloud SQL Client** role (`roles/cloudsql.client`) on the F3 Nation GCP project. Ask the F3 IT Leadership on Slack if you need access.
- You need the **instance connection names** for prod and staging. They look like `project-id:region:instance-name`. You can find them by running `gcloud sql instances list` or asking a team lead.

### 1. Install the Auth Proxy

**Linux / WSL (amd64):**
Run this from any directory.

```bash
curl -o cloud-sql-proxy https://storage.googleapis.com/cloud-sql-connectors/cloud-sql-proxy/v2.14.3/cloud-sql-proxy.linux.amd64
chmod +x cloud-sql-proxy
sudo mv cloud-sql-proxy /usr/local/bin/
```

**macOS (Apple Silicon):**
```bash
curl -o cloud-sql-proxy https://storage.googleapis.com/cloud-sql-connectors/cloud-sql-proxy/v2.14.3/cloud-sql-proxy.darwin.arm64
chmod +x cloud-sql-proxy
sudo mv cloud-sql-proxy /usr/local/bin/
```

**macOS (Intel):**
```bash
curl -o cloud-sql-proxy https://storage.googleapis.com/cloud-sql-connectors/cloud-sql-proxy/v2.14.3/cloud-sql-proxy.darwin.amd64
chmod +x cloud-sql-proxy
sudo mv cloud-sql-proxy /usr/local/bin/
```

**Windows (PowerShell):**
```powershell
curl -o cloud-sql-proxy.exe https://storage.googleapis.com/cloud-sql-connectors/cloud-sql-proxy/v2.14.3/cloud-sql-proxy.x64.exe
# Create a bin directory and move the binary there:
New-Item -ItemType Directory -Path "$env:USERPROFILE\bin" -Force
Move-Item cloud-sql-proxy.exe "$env:USERPROFILE\bin\cloud-sql-proxy.exe"
# Add it to your user PATH (only need to do this once, restart terminal after):
[Environment]::SetEnvironmentVariable("Path", "$env:USERPROFILE\bin;" + [Environment]::GetEnvironmentVariable("Path", "User"), "User")
```

> **WSL + Windows note:** If you use psql in WSL _and_ DBeaver on Windows, install the proxy in **both** environments. WSL and Windows have separate network stacks, so each needs its own proxy running on its own `localhost`. The Linux install instructions above work inside WSL. Run `gcloud auth application-default login` separately in each environment as well. **IMPORT**: The ports you configure for Windows have to be different than those in WSL. Otherwise they will conflict with each other.

See the [official downloads page](https://cloud.google.com/sql/docs/postgres/sql-proxy#install) for other platforms or the latest version.

### 2. Authenticate with GCP

Log in with your Google account:
```bash
gcloud auth application-default login
```

Or, if using a service account key (CI, shared environments):
```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json
```

### 3. Run the proxy as a background service

You want the proxy running automatically so you don't have to start it manually every time.

**Linux / WSL (systemd):**

```bash
sudo nano /etc/systemd/system/cloud-sql-proxy.service
```

Paste (replacing the connection names):
```ini
[Unit]
Description=Cloud SQL Auth Proxy
After=network.target

[Service]
User=<user name>
ExecStart=/usr/local/bin/cloud-sql-proxy \
  --address 127.0.0.1 \
  'f3data:us-central1:f3data?port=5437' \
  'f3data:us-central1:f3data-nonprod?port=5438'
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

Save and exit nano (`Ctrl+O`, Enter, `Ctrl+X`), then enable and start it:
```bash
systemctl --user daemon-reload
systemctl --user enable --now cloud-sql-proxy
```

Check status:
```bash
systemctl --user status cloud-sql-proxy
```

**Windows (Startup folder script):**

1. Open the Startup folder by pressing `Win+R`, typing `shell:startup`, and hitting Enter.
2. Create a new file called `cloud-sql-proxy.vbs` and paste the following (replacing the connection names and username):
```vbs
Set WshShell = CreateObject("WScript.Shell")
WshShell.Run """C:\Users\YOUR_USERNAME\bin\cloud-sql-proxy.exe"" ""PROJECT:REGION:PROD_INSTANCE?port=5433"" ""PROJECT:REGION:STAGING_INSTANCE?port=5434""", 0, False
```
The `0` makes it run without a visible console window.

To verify it's running, open a terminal and run:
```powershell
Test-NetConnection -ComputerName localhost -Port 5433
```

> **Quick test without the service:** If you just want to test the proxy before setting up the service, you can run it directly in a terminal:
> ```bash
> cloud-sql-proxy \
>   "PROJECT:REGION:PROD_INSTANCE?port=5433" \
>   "PROJECT:REGION:STAGING_INSTANCE?port=5434"
> ```

### 4. Configure your connection settings

These files tell psql, Python scripts, and DBeaver how to reach the database through the proxy.

**`~/.pgpass`** (so psql doesn't prompt for passwords):
```
localhost:5433:*:username:password
localhost:5434:*:username:password
```
Secure it: `chmod 600 ~/.pgpass`

**`~/.pg_service.conf`** (used by psql and SQL scripts):
```ini
[prod]
host=localhost
port=5433
user=username
dbname=f3_prod

[staging]
host=localhost
port=5434
user=username
dbname=f3_staging
```

**`.env.prod` / `.env.staging`** (used by the Python scripts in this repo):
```
PG_HOST=localhost
PG_PORT=5433       # or 5434 for staging
PG_DBNAME=f3_prod  # or f3_staging
PG_USER=username
PG_PASSWORD=password
```

### 5. Test the connection

```bash
psql service=staging
```

If you get a psql prompt, you're connected through the proxy. Type `\q` to exit.

## Interacting
We'll go over 2 ways to interact with the database. DBeaver and psql.

### DBeaver
This is a very useful SQL client. It can connect to many types of databases, including postgres. You can use it for querying and editing data, structure, security, etc.

1. Download this database tool that will give you access to everything: https://dbeaver.io/download/
1. After installed, create a new database connection (click the little blue plug with a +)
1. Select PostgreSQL from the list of database types and hit Next.
1. Set **Host** to `localhost`, **Port** to `5433` (prod) or `5434` (staging), and fill in your **Database**, **Username**, and **Password**. Hit Finish.
1. If you need help getting around, just look on YouTube for how to use DBeaver with Postgres. It's a very common app.

### psql
psql is a scripting environment you can use to write detailed routines. For instance, 3 selects are run, you output something from each to a variable, and then it takes those variables to update data using a 4th call.

#### Environment Setup
This assumes you're running Linux, macOS, or WSL and that you have [psql commandline tool installed](https://www.postgresql.org/download/). If you've already completed the Auth Proxy setup above (steps 1–4), you're good to go. Test it:

`psql service=staging`

This should connect you to the staging database. Type `\q` to exit.

### Running a script
You can feed psql a script and input arguments. And, the script can be interactive (asking you to approve changes, etc.). The `\` you'll see is a line break you can add to make it easier to read.

```
psql service=staging \
     -v old_org=123 -v new_org=456 \
     -v commit=[false|true] \
     -f Scripts/Merge-Aos.sql
```

### Caution
Try and include gates. For example, run a SELECT to get a count. Run the UPDATE, then run SELECT again to make sure you get the count. Then use psql language to prompt for approval before moving on. And ROLLBACK if approval is not given.

```sql
SELECT (COUNT(*) > 0) AS ok
FROM orgs
WHERE id = :'old_org'::int;
\gset

\if :ok
\else
  \echo 'ERROR: old_org does not exist'
  ROLLBACK;
  \quit
\endif
```
