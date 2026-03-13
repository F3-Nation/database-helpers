# You can run these commands in terminal

> This one will export production public schema (no data)

```bash
pg_dump service=prod -n  public --schema-only > schema_dump.sql
```