-- ============================================================
-- Merge-Users.sql
--
-- Help / Usage
--
-- This script consolidates user records by taking one or more
-- `old_emails` and a single `new_email`, then remaps/deletes
-- related rows (slack_users, roles, permissions, etc.) to the
-- `new_email` user.
--
-- Examples:
--
--    Pass a comma-separated list of emails. The script will
--    convert it into an array and use `= ANY(array)` in queries.
--
--    psql service=staging -f Scripts/Merge-Users.sql \
--      -v new_email='bob@gmail.com' \
--      -v old_emails='bob@yahoo.com,bob@msn.com'
--
--    psql service=staging -f Scripts/Merge-Users.sql \
--      -v new_email='bob@gmail.com' \
--      -v old_emails='bob@yahoo.com'
--
-- ============================================================

\set ON_ERROR_STOP on
\set QUIET on
\pset footer off

------------------------------------------------------------
-- Check for required inputs
------------------------------------------------------------
\set _missing false

\if :{?old_emails}
\else
  \echo 'ERROR: Missing required variable old_emails'
  \set _missing true
\endif

\if :{?new_email}
\else
  \echo 'ERROR: Missing required variable new_email'
  \set _missing true
\endif

\if :_missing
  \echo
  \echo 'Usage:'
  \echo '  psql ... -v old_emails=''a@x.com,b@y.com'' -v new_email=''c@z.com'''
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

SELECT string_to_array(:'old_emails', ',') AS old_email_array
\gset

SELECT
    current_database()          AS database,
    :'old_emails'          AS old_emails,
    :'new_email'                AS new_email;

SELECT string_to_array(:'old_emails', ',') AS old_email_array
\gset
\echo 'Old emails converted to an array: ':old_email_array

SELECT array_agg(id) AS old_user_ids
FROM users
WHERE email = ANY (:'old_email_array')
\gset

SELECT id AS new_user_id
FROM users
WHERE email = :'new_email'
\gset

\echo 'Found user IDs to merge: ':old_user_ids' -> ':new_user_id

------------------------------------------------------------
-- Start Transaction
------------------------------------------------------------
\echo
\echo '==================================================='
\echo 'Starting Transaction'
BEGIN;

------------------------------------------------------------
-- Delete old slack user
------------------------------------------------------------
\echo
\echo '==================================================='
\echo 'Deleteing slack users for old user IDs'

SELECT su.user_id, su.user_name, ss.workspace_name
FROM slack_users su
left join slack_spaces ss on su.slack_team_id = ss.team_id 
WHERE su.user_id = ANY (:'old_user_ids');

DELETE FROM slack_users
WHERE user_id = ANY (:'old_user_ids');

SELECT COUNT(*) AS deleted_slack_users
FROM slack_users
WHERE user_id = ANY (:'old_user_ids')
\gset
\echo 'Deleted slack users count (should be 0): ':deleted_slack_users

------------------------------------------------------------
-- Update role mappings
------------------------------------------------------------
\echo
\echo '==================================================='
\echo 'Updating role mappings to new user ID'
\echo

\echo 'Old users(s) role mappings:'
select u.f3_name, o."name" as org, o."org_type" , r."name"
from roles_x_users_x_org ruo
left join orgs o on ruo.org_id = o.id
left join users u on ruo.user_id = u.id
left join roles r on ruo.role_id = r.id
WHERE ruo.user_id = ANY (:'old_user_ids');

\echo 'New user role mappings:'
select u.f3_name, o."name" as org, o."org_type" , r."name"
from roles_x_users_x_org ruo
left join orgs o on ruo.org_id = o.id
left join users u on ruo.user_id = u.id
left join roles r on ruo.role_id = r.id
WHERE ruo.user_id = :'new_user_id';

\echo 'Deleting roles for old users that the new user already has'
DELETE FROM roles_x_users_x_org ruo_old
WHERE ruo_old.user_id = ANY (:'old_user_ids')
  AND EXISTS (
      SELECT 1
      FROM roles_x_users_x_org ruo_new
      WHERE ruo_new.user_id = :'new_user_id'
        AND ruo_new.role_id = ruo_old.role_id
        AND ruo_new.org_id  = ruo_old.org_id
  );

\echo 'Updating remaining roles'
UPDATE roles_x_users_x_org
SET user_id = :'new_user_id'
WHERE user_id = ANY (:'old_user_ids');

SELECT COUNT(*) AS remaining_old_role_mappings
FROM roles_x_users_x_org
WHERE user_id = ANY (:'old_user_ids')
\gset
\echo 'Remaining role mappings for old user IDs (should be 0): ':remaining_old_role_mappings

------------------------------------------------------------
-- Update permission mappings
------------------------------------------------------------
\echo
\echo '==================================================='
\echo 'Updating permission mappings to new user ID'

select u.f3_name, o."name" as org, o."org_type" , p."name"
from positions_x_orgs_x_users pou
left join orgs o on pou.org_id = o.id
left join users u on pou.user_id = u.id
left join positions p on pou.position_id = p.id
WHERE pou.user_id = ANY (:'old_user_ids');

UPDATE positions_x_orgs_x_users
SET user_id = :'new_user_id'
WHERE user_id = ANY (:'old_user_ids');

SELECT COUNT(*) AS remaining_old_permission_mappings
FROM positions_x_orgs_x_users
WHERE user_id = ANY (:'old_user_ids')
\gset
\echo 'Remaining permission mappings for old user IDs (should be 0): ':remaining_old_permission_mappings

------------------------------------------------------------
-- Update achievements
------------------------------------------------------------
\echo
\echo '==================================================='
\echo 'Achievement remapping is not currently implemented. There is a unique key constraint on (user_id, achievement_id, award_year, award_period) that must be handled. And we''re not even using achievements yet...'

------------------------------------------------------------
-- Update API Keys
------------------------------------------------------------
\echo
\echo '==================================================='
\echo 'Updating API Keys to new user ID'

select u.f3_name, k."name"
from api_keys k
left join users u on k.owner_id = u.id
WHERE k.owner_id = ANY (:'old_user_ids');

UPDATE api_keys
SET owner_id = :'new_user_id'
WHERE owner_id = ANY (:'old_user_ids');

SELECT COUNT(*) AS remaining_old_permission_mappings
FROM positions_x_orgs_x_users
WHERE user_id = ANY (:'old_user_ids')
\gset
\echo 'Remaining API Key mappings for old user IDs (should be 0): ':remaining_old_permission_mappings

------------------------------------------------------------
-- Update expansion
------------------------------------------------------------
\echo
\echo '==================================================='
\echo 'Expansion remapping is not currently implemented. We''re not currently using it.'

------------------------------------------------------------
-- Update attendance
------------------------------------------------------------
\echo
\echo '==================================================='
\echo 'Updating attendance to new user ID'

-- is_planned flag TRUE
WITH conflicting_events_planned AS (
    SELECT DISTINCT a.event_instance_id
    FROM attendance a
    WHERE a.user_id = :new_user_id
      AND EXISTS (
          SELECT 1
          FROM attendance a2
          WHERE a2.event_instance_id = a.event_instance_id
            AND a2.user_id = ANY (:'old_user_ids'::int[])
      )
    AND a.is_planned = TRUE
),
deleted AS (
    DELETE FROM attendance
    WHERE event_instance_id IN (
        SELECT event_instance_id
        FROM conflicting_events_planned
    )
      AND user_id = ANY (:'old_user_ids'::int[])
    RETURNING 1
)
SELECT COUNT(*) AS attendance_deleted_planned
FROM deleted
\gset
\echo 'Deleted ':attendance_deleted_planned' attendance for old users when old and new users were both mapped and is_planned is TRUE.'

-- is_planned flag FALSE
WITH conflicting_events_notplanned AS (
    SELECT DISTINCT a.event_instance_id
    FROM attendance a
    WHERE a.user_id = :new_user_id
      AND EXISTS (
          SELECT 1
          FROM attendance a2
          WHERE a2.event_instance_id = a.event_instance_id
            AND a2.user_id = ANY (:'old_user_ids'::int[])
      )
    AND a.is_planned = FALSE
),
deleted AS (
    DELETE FROM attendance
    WHERE event_instance_id IN (
        SELECT event_instance_id
        FROM conflicting_events_notplanned
    )
      AND user_id = ANY (:'old_user_ids'::int[])
    RETURNING 1
)
SELECT COUNT(*) AS attendance_deleted_notplanned
FROM deleted
\gset
\echo 'Deleted ':attendance_deleted_notplanned' attendance for old users when old and new users were both mapped and is_planned is FALSE.'

-- update remaining
WITH updated AS (
    UPDATE attendance
    SET user_id = :new_user_id
    WHERE user_id = ANY (:'old_user_ids'::int[])
    RETURNING 1
)
SELECT COUNT(*) AS attendance_updated
FROM updated
\gset
\echo 'Updated ':attendance_updated' attendance records to new user ID.'

SELECT COUNT(*) AS remaining_old_attendance
FROM attendance
WHERE user_id = ANY (:'old_user_ids'::int[])
\gset
\echo 'Remaining attendance records for old user IDs (should be 0): ':remaining_old_attendance

------------------------------------------------------------
-- Explicit confirmation
------------------------------------------------------------
\if :commit
  \echo
  \echo '==================================================='
  \echo 'You are about to:'
  \echo '  - Delete slack users listed above'
  \echo '  - Update roles listed above'
  \echo '  - Update positions listed above'
  \echo '  - Update achievements listed above (NOT IMPLEMENTED)'
  \echo '  - Update API Keys listed above'
  \echo '  - Update expansions listed above (NOT IMPLEMENTED)'
  \echo '  - Update attendance records indicated above'
  \echo
  \echo 'Above changes will be commited, then old users will be deleted. User delete action is taken separately in case unhandled references remain.'
  \echo 'Press ENTER to COMMIT changes or Ctrl+C to ROLLBACK changes and exit'
  \prompt confirm
  \echo 'Committing changes.'
  COMMIT;
\endif

------------------------------------------------------------
-- Delete old users
------------------------------------------------------------
\echo
\echo '==================================================='
\echo 'Deleting old users'

\if :commit
  \echo 'Starting transaction'
  BEGIN;
\endif

SELECT COUNT(*) AS all_users
FROM users
\gset

DELETE FROM users
WHERE id = ANY (:'old_user_ids');

SELECT COUNT(*) AS remaining_old_users
FROM users
WHERE id = ANY (:'old_user_ids')
\gset

SELECT COUNT(*) AS remaining_all_users
FROM users
\gset

\echo
\echo 'Total users before deletion: ':all_users
\echo 'Total users after deletion: ':remaining_all_users
\echo 'Remaining old users (should be 0): ':remaining_old_users
\echo

\if :commit
  \echo 'Old user(s) deleted, but not committed. Do the above number look right? You sure?'
  \echo 'Press ENTER to COMMIT changes or Ctrl+C to ROLLBACK deletion of old users.'
  \echo 'Aborting now will not rollback the merge operations, those changes will remain.'
  \prompt confirm
  \echo 'Committing changes.'
  COMMIT;
\else
  ROLLBACK;
  \echo 'Dry run complete. All changes rolled back.'
  \echo 'To commit, rerun the script with -v commit=true'
\endif