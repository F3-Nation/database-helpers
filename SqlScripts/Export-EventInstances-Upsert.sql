-- ============================================================
-- Export-EventInstances-Upsert.sql
--
-- Generates SQL upsert statements to restore event_instances rows
-- from one series to another, matching target rows by
-- (series_id, start_date).
--
-- Run this against the SOURCE (backup) database and redirect output
-- to a .sql file, then run that output against TARGET (prod).
--
-- Example (generate):
--   psql service=staging -f SqlScripts/Export-EventInstances-Upsert.sql \
--     -v source_series=52543 \
--     -v target_series=53647 \
--     -v from_date='2026-02-27' \
--     -v to_date='2026-06-19' \
--     > temp/restore_event_instances_52543_to_53647.sql
--
-- Example (apply generated SQL):
--   psql service=prod -f temp/restore_event_instances_52543_to_53647.sql
-- ============================================================

\set ON_ERROR_STOP on
\set QUIET on
\pset footer off
\pset pager off
\pset format unaligned
\pset tuples_only on

------------------------------------------------------------
-- Check for required inputs
------------------------------------------------------------
\set _missing false

\if :{?source_series}
\else
  \echo 'ERROR: Missing required variable source_series'
  \set _missing true
\endif

\if :{?target_series}
\else
  \echo 'ERROR: Missing required variable target_series'
  \set _missing true
\endif

\if :{?from_date}
\else
  \echo 'ERROR: Missing required variable from_date (YYYY-MM-DD)'
  \set _missing true
\endif

\if :{?to_date}
\else
  \echo 'ERROR: Missing required variable to_date (YYYY-MM-DD)'
  \set _missing true
\endif

\if :_missing
  \echo 'Usage:'
  \echo '  psql ... -f SqlScripts/Export-EventInstances-Upsert.sql -v source_series=<int> -v target_series=<int> -v from_date=<YYYY-MM-DD> -v to_date=<YYYY-MM-DD>'
  \quit 1
\endif

------------------------------------------------------------
-- Validate input formats
------------------------------------------------------------
SELECT (:'source_series' ~ '^[0-9]+$') AS source_series_is_int \gset
SELECT (:'target_series' ~ '^[0-9]+$') AS target_series_is_int \gset

\if :source_series_is_int
\else
  \echo 'ERROR: source_series must be a positive integer'
  \quit 1
\endif

\if :target_series_is_int
\else
  \echo 'ERROR: target_series must be a positive integer'
  \quit 1
\endif

SELECT (:'from_date'::date < :'to_date'::date) AS valid_date_range \gset

\if :valid_date_range
\else
  \echo 'ERROR: from_date must be earlier than to_date'
  \quit 1
\endif

SELECT (COUNT(*) > 0) AS has_rows
FROM public.event_instances
WHERE series_id = :'source_series'::int
  AND start_date < :'to_date'::date
  AND start_date > :'from_date'::date
\gset

\if :has_rows
\else
  \echo 'ERROR: Query returned 0 rows for the provided filters.'
  \quit 1
\endif

SELECT data_type AS preblast_ts_data_type
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name = 'event_instances'
  AND column_name = 'preblast_ts'
\gset

SELECT data_type AS backblast_ts_data_type
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name = 'event_instances'
  AND column_name = 'backblast_ts'
\gset

------------------------------------------------------------
-- Emit executable SQL as plain text
------------------------------------------------------------
WITH source_rows AS (
    SELECT
        ei.start_date,
        format(
$fmt$
UPDATE public.event_instances
SET
  "name" = %L,
  description = %L,
  email = %L,
  pax_count = %s,
  fng_count = %s,
  preblast = %L,
  backblast = %L,
  preblast_rich = %L,
  backblast_rich = %L,
  preblast_ts = %s,
  backblast_ts = %s,
  meta = %L::jsonb,
  created = %s,
  updated = %s
WHERE series_id = %s
  AND start_date = %L::date;

INSERT INTO public.event_instances (
  series_id,
  start_date,
  "name",
  description,
  email,
  pax_count,
  fng_count,
  preblast,
  backblast,
  preblast_rich,
  backblast_rich,
  preblast_ts,
  backblast_ts,
  meta,
  created,
  updated
)
SELECT
  %s,
  %L::date,
  %L,
  %L,
  %L,
  %s,
  %s,
  %L,
  %L,
  %L,
  %L,
  %s,
  %s,
  %L::jsonb,
  %s,
  %s
WHERE NOT EXISTS (
  SELECT 1
  FROM public.event_instances ei
  WHERE ei.series_id = %s
    AND ei.start_date = %L::date
);$fmt$,
            ei."name",
            ei.description,
            ei.email,
            COALESCE(ei.pax_count::text, 'NULL'),
            COALESCE(ei.fng_count::text, 'NULL'),
            ei.preblast,
            ei.backblast,
            ei.preblast_rich,
            ei.backblast_rich,
            CASE
              WHEN ei.preblast_ts IS NULL THEN 'NULL'
              WHEN :'preblast_ts_data_type' = 'double precision' THEN
                CASE
                  WHEN ei.preblast_ts::text ~ '^[0-9]+([.][0-9]+)?$' THEN ei.preblast_ts::text
                  ELSE format('extract(epoch from %L::timestamptz)', ei.preblast_ts::text)
                END
              WHEN ei.preblast_ts::text ~ '^[0-9]+([.][0-9]+)?$' THEN format('to_timestamp(%s)', ei.preblast_ts::text)
              ELSE format('%L::timestamptz', ei.preblast_ts)
            END,
            CASE
              WHEN ei.backblast_ts IS NULL THEN 'NULL'
              WHEN :'backblast_ts_data_type' = 'double precision' THEN
                CASE
                  WHEN ei.backblast_ts::text ~ '^[0-9]+([.][0-9]+)?$' THEN ei.backblast_ts::text
                  ELSE format('extract(epoch from %L::timestamptz)', ei.backblast_ts::text)
                END
              WHEN ei.backblast_ts::text ~ '^[0-9]+([.][0-9]+)?$' THEN format('to_timestamp(%s)', ei.backblast_ts::text)
              ELSE format('%L::timestamptz', ei.backblast_ts)
            END,
            ei.meta,
            CASE
              WHEN ei.created IS NULL THEN 'NULL'
              WHEN ei.created::text ~ '^[0-9]+([.][0-9]+)?$' THEN format('to_timestamp(%s)', ei.created::text)
              ELSE format('%L::timestamptz', ei.created)
            END,
            CASE
              WHEN ei.updated IS NULL THEN 'NULL'
              WHEN ei.updated::text ~ '^[0-9]+([.][0-9]+)?$' THEN format('to_timestamp(%s)', ei.updated::text)
              ELSE format('%L::timestamptz', ei.updated)
            END,
            :'target_series'::int,
            ei.start_date,
            :'target_series'::int,
            ei.start_date,
            ei."name",
            ei.description,
            ei.email,
            COALESCE(ei.pax_count::text, 'NULL'),
            COALESCE(ei.fng_count::text, 'NULL'),
            ei.preblast,
            ei.backblast,
            ei.preblast_rich,
            ei.backblast_rich,
            CASE
              WHEN ei.preblast_ts IS NULL THEN 'NULL'
              WHEN :'preblast_ts_data_type' = 'double precision' THEN
                CASE
                  WHEN ei.preblast_ts::text ~ '^[0-9]+([.][0-9]+)?$' THEN ei.preblast_ts::text
                  ELSE format('extract(epoch from %L::timestamptz)', ei.preblast_ts::text)
                END
              WHEN ei.preblast_ts::text ~ '^[0-9]+([.][0-9]+)?$' THEN format('to_timestamp(%s)', ei.preblast_ts::text)
              ELSE format('%L::timestamptz', ei.preblast_ts)
            END,
            CASE
              WHEN ei.backblast_ts IS NULL THEN 'NULL'
              WHEN :'backblast_ts_data_type' = 'double precision' THEN
                CASE
                  WHEN ei.backblast_ts::text ~ '^[0-9]+([.][0-9]+)?$' THEN ei.backblast_ts::text
                  ELSE format('extract(epoch from %L::timestamptz)', ei.backblast_ts::text)
                END
              WHEN ei.backblast_ts::text ~ '^[0-9]+([.][0-9]+)?$' THEN format('to_timestamp(%s)', ei.backblast_ts::text)
              ELSE format('%L::timestamptz', ei.backblast_ts)
            END,
            ei.meta,
            CASE
              WHEN ei.created IS NULL THEN 'NULL'
              WHEN ei.created::text ~ '^[0-9]+([.][0-9]+)?$' THEN format('to_timestamp(%s)', ei.created::text)
              ELSE format('%L::timestamptz', ei.created)
            END,
            CASE
              WHEN ei.updated IS NULL THEN 'NULL'
              WHEN ei.updated::text ~ '^[0-9]+([.][0-9]+)?$' THEN format('to_timestamp(%s)', ei.updated::text)
              ELSE format('%L::timestamptz', ei.updated)
            END,
            :'target_series'::int,
            ei.start_date
        ) AS stmt,
        row_number() OVER (ORDER BY ei.start_date) AS rn
    FROM public.event_instances ei
    WHERE ei.series_id = :'source_series'::int
      AND ei.start_date < :'to_date'::date
      AND ei.start_date > :'from_date'::date
)
SELECT line
FROM (
  SELECT 0 AS ord, '-- Generated from source series ' || :'source_series' || ' to target series ' || :'target_series' || ' on ' || to_char(clock_timestamp(), 'YYYY-MM-DD HH24:MI:SS TZ') AS line
    UNION ALL
  SELECT 1, '-- Source column types: preblast_ts=' || :'preblast_ts_data_type' || ', backblast_ts=' || :'backblast_ts_data_type'
    UNION ALL
  SELECT 2, 'BEGIN;'
    UNION ALL
  SELECT 3, 'SET LOCAL lock_timeout = ''5s'';'
  UNION ALL
  SELECT 4, 'SET LOCAL statement_timeout = ''60s'';'
    UNION ALL
  SELECT 1000 + rn, stmt FROM source_rows
    UNION ALL
    SELECT 999999, 'COMMIT;'
) emitted
ORDER BY ord;