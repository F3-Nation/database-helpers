-- ============================================================
-- Merge-EventTypes.sql
--
-- Help / Usage
--
-- This script consolidates event type records by taking a
-- `from` event type ID and an `into` event type ID, then remaps
-- related rows in junction tables to the `into` event type.
--
-- Examples:
--
--    psql service=staging -f SqlScripts/Merge-EventTypes.sql \
--      -v from=123 -v into=456
--
-- ============================================================

\set ON_ERROR_STOP on
\set QUIET on
\pset footer off

------------------------------------------------------------
-- Check for required inputs
------------------------------------------------------------
\set _missing false

\if :{?from}
\else
  \echo 'ERROR: Missing required variable from'
  \set _missing true
\endif

\if :{?into}
\else
  \echo 'ERROR: Missing required variable into'
  \set _missing true
\endif

\if :_missing
  \echo
  \echo 'Usage:'
  \echo '  psql ... -v from=<integer> -v into=<integer>'
  \quit
\endif

------------------------------------------------------------
-- Validate input types (should be integers) and aren't the same value
------------------------------------------------------------
\set _invalid false

-- Check if 'from' is a valid integer
SELECT CASE WHEN :'from' ~ '^\d+$' THEN true ELSE false END AS from_is_integer \gset
\if :from_is_integer
\else
  \echo 'ERROR: from variable must be a positive integer'
  \set _invalid true
\endif

-- Check if 'into' is a valid integer
SELECT CASE WHEN :'into' ~ '^\d+$' THEN true ELSE false END AS into_is_integer \gset
\if :into_is_integer
\else
  \echo 'ERROR: into variable must be a positive integer'
  \set _invalid true
\endif

\if :_invalid
  \quit
\endif

SELECT (:'from'::int <> :'into'::int) AS same_value
\gset

\if :same_value
\else
  \echo 'ERROR: from and into cannot be the same value'
  \quit
\endif
------------------------------------------------------------
-- Set execution mode
------------------------------------------------------------
\if :{?commit}
\else
  \set commit false
\endif

\echo 'Execution mode:'
\if :commit
  \echo ' COMMIT (changes will be permanent)'
\else
  \echo ' DRY RUN (ROLLBACK only)'
  \echo
  \echo 'To commit, use: psql ... -v commit=true'
\endif

------------------------------------------------------------
-- Echo inputs and environment
------------------------------------------------------------
\echo
\echo '==================================================='
\echo 'Inputs:'

SELECT
    current_database()          AS database,
    :'from'                     AS from,
    :'into'                     AS into;

------------------------------------------------------------
-- Validate event types exist
------------------------------------------------------------
\set _not_found false

SELECT COUNT(*) = 1 AS from_matches_one
FROM event_types
WHERE id = :'from'
\gset

\if :from_matches_one
\else
  \echo 'ERROR: Expected exactly 1 event type for from_id = ' :from
  \echo '       Found ' :from_matches_one
  \set _not_found true
\endif

SELECT COUNT(*) = 1 AS into_matches_one
FROM event_types
WHERE id = :'into'
\gset

\if :into_matches_one
\else
  \echo 'ERROR: Expected exactly 1 event type for into_id = ' :into
  \echo '       Found ' :into_matches_one
  \set _not_found true
\endif

\if :_not_found
  \quit
\endif

-- Show event type details
SELECT id, name, description
FROM event_types
WHERE id IN (:'from', :'into')
ORDER BY id;

\echo 'Merging event type ' :from ' into ' :into

------------------------------------------------------------
-- Start Transaction
------------------------------------------------------------
\echo
\echo '==================================================='
\echo 'Starting Transaction'
BEGIN;

------------------------------------------------------------
-- Handle events_x_event_types junction table
------------------------------------------------------------
\echo
\echo '==================================================='
\echo 'Processing events_x_event_types junction table'

-- Find events that have both from and into event types
SELECT COUNT(*) AS events_with_both_types
FROM events_x_event_types exet_from
JOIN events_x_event_types exet_into ON exet_from.event_id = exet_into.event_id
WHERE exet_from.event_type_id = :'from'
  AND exet_into.event_type_id = :'into'
\gset

\echo 'Events with both from and into event types: ' :events_with_both_types

-- Delete the 'from' event type records where both types are present
WITH deleted_both AS (
    DELETE FROM events_x_event_types exet_from
    WHERE exet_from.event_type_id = :'from'
      AND EXISTS (
          SELECT 1
          FROM events_x_event_types exet_into
          WHERE exet_into.event_id = exet_from.event_id
            AND exet_into.event_type_id = :'into'
      )
    RETURNING event_id
)
SELECT COUNT(*) AS deleted_events_both_types
FROM deleted_both
\gset

\echo 'Deleted ' :deleted_events_both_types ' from-type records where both types were present'

-- Update remaining 'from' event type records to 'into'
WITH updated_events AS (
    UPDATE events_x_event_types
    SET event_type_id = :'into'
    WHERE event_type_id = :'from'
    RETURNING event_id
)
SELECT COUNT(*) AS updated_event_records
FROM updated_events
\gset

\echo 'Updated ' :updated_event_records ' event records to new event type ID'

-- Verify no 'from' records remain
SELECT COUNT(*) AS remaining_from_events
FROM events_x_event_types
WHERE event_type_id = :'from'
\gset

\echo 'Remaining from-type records in events_x_event_types (should be 0): ' :remaining_from_events

------------------------------------------------------------
-- Handle event_instances_x_event_types junction table
------------------------------------------------------------
\echo
\echo '==================================================='
\echo 'Processing event_instances_x_event_types junction table'

-- Find event instances that have both from and into event types
SELECT COUNT(*) AS event_instances_with_both_types
FROM event_instances_x_event_types eixet_from
JOIN event_instances_x_event_types eixet_into ON eixet_from.event_instance_id = eixet_into.event_instance_id
WHERE eixet_from.event_type_id = :'from'
  AND eixet_into.event_type_id = :'into'
\gset

\echo 'Event instances with both from and into event types: ' :event_instances_with_both_types

-- Delete the 'from' event type records where both types are present
WITH deleted_both_instances AS (
    DELETE FROM event_instances_x_event_types eixet_from
    WHERE eixet_from.event_type_id = :'from'
      AND EXISTS (
          SELECT 1
          FROM event_instances_x_event_types eixet_into
          WHERE eixet_into.event_instance_id = eixet_from.event_instance_id
            AND eixet_into.event_type_id = :'into'
      )
    RETURNING event_instance_id
)
SELECT COUNT(*) AS deleted_instances_both_types
FROM deleted_both_instances
\gset

\echo 'Deleted ' :deleted_instances_both_types ' from-type records where both types were present'

-- Update remaining 'from' event type records to 'into'
WITH updated_instances AS (
    UPDATE event_instances_x_event_types
    SET event_type_id = :'into'
    WHERE event_type_id = :'from'
    RETURNING event_instance_id
)
SELECT COUNT(*) AS updated_instance_records
FROM updated_instances
\gset

\echo 'Updated ' :updated_instance_records ' event instance records to new event type ID'

-- Verify no 'from' records remain
SELECT COUNT(*) AS remaining_from_instances
FROM event_instances_x_event_types
WHERE event_type_id = :'from'
\gset

\echo 'Remaining from-type records in event_instances_x_event_types (should be 0): ' :remaining_from_instances

------------------------------------------------------------
-- Show summary of changes
------------------------------------------------------------
\echo
\echo '==================================================='
\echo 'Summary of changes:'

SELECT
    :deleted_events_both_types + :updated_event_records AS total_events_affected,
    :deleted_instances_both_types + :updated_instance_records AS total_event_instances_affected,
    :deleted_events_both_types + :deleted_instances_both_types AS total_duplicates_removed,
    :updated_event_records + :updated_instance_records AS total_records_updated;

------------------------------------------------------------
-- Explicit confirmation
------------------------------------------------------------
\if :commit
  \echo
  \echo '==================================================='
  \echo 'You are about to:'
  \echo '  - Merge event type ' :from ' into ' :into
  \echo '  - Remove duplicate event type associations where both were present'
  \echo '  - Update remaining associations to the new event type'
  \echo
  \echo 'Above changes will be committed, then the old event type will be deleted.'
  \echo 'Press ENTER to COMMIT changes or Ctrl+C to ROLLBACK changes and exit'
  \prompt confirm
  \echo 'Committing changes.'
  COMMIT;
\endif

------------------------------------------------------------
-- Delete old event type
------------------------------------------------------------
\echo
\echo '==================================================='
\echo 'Deleting old event type'

\if :commit
  \echo 'Starting transaction'
  BEGIN;
\endif

SELECT COUNT(*) AS all_event_types
FROM event_types
\gset

-- Show what we're about to delete
SELECT id, name, description
FROM event_types
WHERE id = :'from';

DELETE FROM event_types
WHERE id = :'from';

SELECT COUNT(*) AS remaining_from_event_types
FROM event_types
WHERE id = :'from'
\gset

SELECT COUNT(*) AS remaining_all_event_types
FROM event_types
\gset

\echo
\echo 'Total event types before deletion: ' :all_event_types
\echo 'Total event types after deletion: ' :remaining_all_event_types
\echo 'Remaining from event type (should be 0): ' :remaining_from_event_types
\echo

\if :commit
  \echo 'Old event type deleted, but not committed. Do the above numbers look right? You sure?'
  \echo 'Press ENTER to COMMIT changes or Ctrl+C to ROLLBACK deletion of old event type.'
  \echo 'Aborting now will not rollback the merge operations, those changes will remain.'
  \prompt confirm
  \echo 'Committing changes.'
  COMMIT;
\else
  ROLLBACK;
  \echo 'Dry run complete. All changes rolled back.'
  \echo 'To commit, rerun the script with -v commit=true'
\endif