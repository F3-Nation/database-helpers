-- ============================================================
-- Merge-Aos.sql
--
-- Help / Usage
--
-- This script merges two organization (AOs) records by taking an `old_org` ID and a `new_org` ID, then remaps/deletes related rows to consolidate everything under the `new_org`.
--
-- Examples:
--
--    psql service=prod -f SqlScripts/Merge-Aos.sql \
--      -v new_org=123 \
--      -v old_org=456
--
--    psql service=staging -f SqlScripts/Merge-Aos.sql \
--      -v new_org=789 \
--      -v old_org=101
--
-- ============================================================
\set ON_ERROR_STOP on
\set QUIET on
\pset footer off

\echo 'Starting Transaction'
BEGIN;

------------------------------------------------------------
-- 1) Echo inputs and environment
------------------------------------------------------------
\echo 
\echo 'Inputs:'

SELECT
    current_database()      AS database,
    :'old_org'::int         AS old_org_id,
    :'new_org'::int         AS new_org_id;

------------------------------------------------------------
-- 2) Validate old org exists
------------------------------------------------------------
SELECT (COUNT(*) > 0) AS ok
FROM orgs
WHERE id = :'old_org'::int
\gset

\if :ok
  \echo 'OK: old_org exists'
\else
  \echo 'ERROR: old_org does not exist. Rolling back.'
  ROLLBACK;
  \quit
\endif

------------------------------------------------------------
-- 3) Validate new org exists
------------------------------------------------------------
SELECT (COUNT(*) > 0) AS ok
FROM orgs
WHERE id = :'new_org'::int
\gset

\if :ok
  \echo 'OK: new_org exists'
\else
  \echo 'ERROR: new_org does not exist. Rolling back.'
  ROLLBACK;
  \quit
\endif

------------------------------------------------------------
-- 4) Prevent same-id merge
------------------------------------------------------------
SELECT (:'old_org'::int <> :'new_org'::int) AS ok
\gset

\if :ok
  \echo 'OK: old_org and new_org are different'
\else
  \echo 'ERROR: old_org and new_org cannot be the same. Rolling back.'
  ROLLBACK;
  \quit
\endif

------------------------------------------------------------
-- 5) Dry run
------------------------------------------------------------
\echo

SELECT COUNT(*) AS instances_to_update
FROM event_instances
WHERE org_id = :'old_org'::int
\gset
\echo 'Found: ':instances_to_update' event_instances to update.'

SELECT COUNT(*) events_to_update
FROM events
WHERE org_id = :'old_org'::int
\gset
\echo 'Found: ':events_to_update' events to update.'

------------------------------------------------------------
-- 6) Manual confirmation
------------------------------------------------------------
\echo ''
\echo '==================================================='
\echo 'You are about to:'
\echo '  - Update event_instances.org_id'
\echo '  - Update events.org_id'
\echo '  - Delete org_id: ':old_org''
\echo '==================================================='
\echo 'Press ENTER to continue or Ctrl+C to abort and rollback changes.'
\prompt confirm

------------------------------------------------------------
-- 7) Update
------------------------------------------------------------
WITH updated_instances AS (
  UPDATE event_instances
  SET org_id = :'new_org'::int
  WHERE org_id = :'old_org'::int
  RETURNING 1
)
SELECT COUNT(*) AS updated_count
FROM updated_instances
\gset
\echo 'Updated event_instances: ':updated_count

WITH updated_events AS (
  UPDATE events
  SET org_id = :'new_org'::int
  WHERE org_id = :'old_org'::int
  RETURNING 1
)
SELECT COUNT(*) AS updated_count
FROM updated_events
\gset
\echo 'Updated events: ':updated_count

------------------------------------------------------------
-- 8) Safety check
------------------------------------------------------------
SELECT COUNT(*) AS remaining_instance_refs
FROM event_instances
WHERE org_id = :'old_org'::int
\gset
\echo 'Remaining event_instances with old_org: ':remaining_instance_refs

SELECT COUNT(*) AS remaining_event_refs
FROM events
WHERE org_id = :'old_org'::int
\gset
\echo 'Remaining events with old_org: ':remaining_event_refs

------------------------------------------------------------
-- 9) Pre-Delete Commit
------------------------------------------------------------
\echo
\echo 'Committing the merge prior to attempting to delete the old org. Often this will fail due to foreign key constraints, and we want to preserve the updates if so.'

COMMIT;

------------------------------------------------------------
-- 10) Delete old org
------------------------------------------------------------
\echo

DELETE FROM orgs
WHERE id = :'old_org'::int;