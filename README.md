# database-helpers
Scripts and queries to help DBQs manage the F3 Nation database

## The Database
All F3 Nation data is centralized in a postgres database hosted in GCP. We have two instances, one (f3data) for prod, and one (f3data_nonprod) for staging.

### Who should be connecting directly to the database
Probably not you. ALL of our data is in there. We need to protect it. There are only a few people that have access in case issues arise or changes need to be made that cannot be done via existing apps.

But there's still hope! we have an [API](https://api.f3nation.com/docs#description/introduction) as well as an [BigQuery](https://groups.google.com/u/3/a/f3nation.com/g/f3-data-analytics).

### Endpoints and credentials
It's probably not sure hard to find our database endpoints, but we're not going to broadcast them. Reach out on the F3 Nation Slack to the F3 IT Leadership if you think you need access

## Interacting
We'll go over 2 ways to interact with the database. DBeaver and psql.

### DBeaver
This is a very useful SQL client. It can connect to many types of databases, including postgres. You can use it for querying and editing data, structure, security, etc.

1. Download this database tool that will give you access to everything: https://dbeaver.io/download/
1. After installed, create a new database connection (click the little blue plug with a +)
1. Select PostgreSQL from the list of database types and hit Next.
1. Update the Host, Database, Username, and Password fields accordingly and hit Finish.
1. If you need help getting around, just look on YouTube for how to use DBeaver with Postgres. It's a very common app.

### psql
psql is a scripting environment you can use to write detailed routines. For instance, 3 selects are run, you output something from each to a variable, and then it takes those variables to update data using a 4th call.

#### Environment Setup
This assumes you're running Linux, macOS, or WSL and that you have [psql commandline tool installed](https://www.postgresql.org/download/). This anticpates you running scripts locally on your machine. This is not geared toward deploying to an app.

1. Create a password file.

`nano ~/.pgpass`

2. Add a line for each environment.

```
hostname:port:database:username:password
hostname:port:database:username:password
```
3. Secure your password file.

`chmod 600 ~/.pgpass`

4. Create a connection service config file.

`nano ~/.pg_service.conf`

5. Define each service (environment)

```
[prod]
host=hostname
port=5432
user=username
dbname=database

[staging]
host=hostname
port=5432
user=username
dbname=database

```

6. Test it. Run the following command. It should change the command line from your username and directory to the name of the database you connected to. Make sure the value of the service argument matches one of the services you configured in `.pg_service.conf`.

`psql service=staging`

7. You can exit psql by entering the following.

`\q`

### Running a script
You can feed psql a script and input arguments. And, the script can be interactive (asking you to approve changes, etc.). The `\` you'll see is a line break you can add to make it easier to read.

```
psql service=staging \
     -v old_org=123 -v new_org=456 \
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
