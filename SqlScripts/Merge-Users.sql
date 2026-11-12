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
--    psql service=staging -f SqlScripts/Merge-Users.sql \
--      -v new_email='bob@gmail.com' \
--      -v old_emails='bob@yahoo.com,bob@msn.com'
--
--    psql service=staging -f SqlScripts/Merge-Users.sql \
--      -v new_email='bob@gmail.com' \
--      -v old_emails='bob@yahoo.com'
--
-- Output:
--
--    A MERGE SUMMARY (what moves, what is already there, what is
--    removed / not carried over) is always printed before any
--    changes are made. It is plain text meant to be copied and
--    sent to the user for confirmation.
--
--    Dry runs show only the summary plus a final validation line.
--    Add -v verbose=true to also see the detailed per-table output.
--    Commit runs (-v commit=true) always show the detailed output.
--
-- ============================================================

\set ON_ERROR_STOP on
\set QUIET on
\pset footer off
\pset pager off

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

-- Detailed output: always on for commit runs, opt-in for dry runs
\if :commit
  \set verbose true
\elif :{?verbose}
\else
  \set verbose false
\endif

\echo 'Execution mode:'
\if :commit
  \echo ' COMMIT (changes will be permanent)'
\else
  \echo ' DRY RUN (ROLLBACK only)'
  \echo
  \echo 'To commit, use: psql ... -v commit=true'
  \if :verbose
  \else
    \echo 'For detailed output, use: psql ... -v verbose=true'
  \endif
\endif

------------------------------------------------------------
-- Echo inputs and environment
------------------------------------------------------------
\if :verbose
\echo
\echo '==================================================='
\echo 'Inputs:'

SELECT
    current_database()          AS database,
    :'old_emails'          AS old_emails,
    :'new_email'                AS new_email;
\endif

------------------------------------------------------------
-- Fetch User records for old emails
------------------------------------------------------------

SELECT ARRAY(
  SELECT lower(trim(email_text))
  FROM unnest(string_to_array(:'old_emails', ',')) AS email_text
) AS normalized_old_email_array
\gset

SELECT lower(trim(:'new_email')) AS normalized_new_email
\gset

SELECT array_length(string_to_array(:'old_emails', ','), 1) AS old_email_input_count
\gset

SELECT COUNT(*) AS old_user_match_count
FROM users
WHERE lower(email) = ANY (:'normalized_old_email_array')
\gset

SELECT
  (array_length(string_to_array(:'old_emails', ','), 1)
   = COUNT(*)) AS email_match_ok
FROM users
WHERE lower(email) = ANY (:'normalized_old_email_array')
\gset

\if :email_match_ok
\else
  \echo 'ERROR: Mismatch between old emails input and users found.'
  \echo '  Old emails input : ':old_email_input_count
  \echo '  Users found count  : ':old_user_match_count
  \quit
\endif

SELECT array_agg(id) AS old_user_ids
FROM users
WHERE lower(email) = ANY (:'normalized_old_email_array')
\gset

SELECT array_agg(email ORDER BY id) AS matched_old_emails
FROM users
WHERE lower(email) = ANY (:'normalized_old_email_array')
\gset

------------------------------------------------------------
-- Fetch User record for new email (must be exactly one row)
------------------------------------------------------------

SELECT COUNT(*)=1 AS one_new_user_found
FROM users
WHERE lower(email) = :'normalized_new_email'
\gset

SELECT COUNT(*) AS new_user_count
FROM users
WHERE lower(email) = :'normalized_new_email'
\gset

\if :one_new_user_found
\else
  \echo 'ERROR: Expected exactly 1 user for new_email = ' :new_email
  \echo '       Found ' :new_user_count
  \quit
\endif

SELECT id AS new_user_id
FROM users
WHERE lower(email) = :'normalized_new_email'
\gset

SELECT email AS matched_new_email
FROM users
WHERE lower(email) = :'normalized_new_email'
\gset

\if :verbose
\echo 'Matched old emails: ':matched_old_emails

\echo 'Matched new email: ':matched_new_email

\echo 'Found user IDs to merge: ':old_user_ids' -> ':new_user_id
\endif

------------------------------------------------------------
-- Merge summary (READ-ONLY, computed before any changes)
--
-- Mirrors the match rules used by the changes below so it can be
-- sent to the user for confirmation. The expected counts captured
-- here are compared against the actual results at the end.
------------------------------------------------------------

SELECT
  -- old attendance at events the new user is already on (deleted below)
  (SELECT COUNT(*) FROM attendance a
    WHERE a.user_id = ANY (:'old_user_ids'::int[])
      AND EXISTS (SELECT 1 FROM attendance n
                   WHERE n.user_id = :new_user_id
                     AND n.event_instance_id = a.event_instance_id)) AS exp_att_dup,
  -- old attendance that will be moved to the new user
  (SELECT COUNT(*) FROM attendance a
    WHERE a.user_id = ANY (:'old_user_ids'::int[])
      AND NOT EXISTS (SELECT 1 FROM attendance n
                       WHERE n.user_id = :new_user_id
                         AND n.event_instance_id = a.event_instance_id)) AS exp_att_move,
  -- row counts the new user should have once all changes are applied
  (SELECT COUNT(*) FROM roles_x_users_x_org WHERE user_id = :new_user_id)
  + (SELECT COUNT(*) FROM roles_x_users_x_org o
      WHERE o.user_id = ANY (:'old_user_ids'::int[])
        AND NOT EXISTS (SELECT 1 FROM roles_x_users_x_org n
                         WHERE n.user_id = :new_user_id
                           AND n.role_id = o.role_id
                           AND n.org_id  = o.org_id)) AS exp_new_roles,
  (SELECT COUNT(*) FROM positions_x_orgs_x_users
    WHERE user_id = :new_user_id OR user_id = ANY (:'old_user_ids'::int[])) AS exp_new_positions,
  (SELECT COUNT(*) FROM achievements_x_users WHERE user_id = :new_user_id)
  + (SELECT COUNT(*) FROM achievements_x_users o
      WHERE o.user_id = ANY (:'old_user_ids'::int[])
        AND NOT EXISTS (SELECT 1 FROM achievements_x_users n
                         WHERE n.user_id = :new_user_id
                           AND n.achievement_id = o.achievement_id
                           AND n.award_year     = o.award_year
                           AND n.award_period   = o.award_period)) AS exp_new_achievements,
  (SELECT COUNT(*) FROM api_keys
    WHERE owner_id = :new_user_id OR owner_id = ANY (:'old_user_ids'::int[])) AS exp_new_api_keys,
  (SELECT COUNT(*) FROM attendance WHERE user_id = :new_user_id) AS exp_new_attendance_base
\gset

\pset format unaligned
\pset tuples_only on

\echo
\echo '==================================================='
\echo 'MERGE SUMMARY  (database: ':DBNAME')'
\echo '==================================================='

\echo 'Account being KEPT:'
SELECT '  - ' || COALESCE(u.f3_name, '(no F3 name)') || ' <' || u.email || '>'
       || ' | name: ' || COALESCE(NULLIF(trim(concat_ws(' ', u.first_name, u.last_name)), ''), '-')
       || ' | home region: ' || COALESCE(r.name, '-')
       || ' | phone: ' || COALESCE(NULLIF(u.phone, ''), '-')
FROM users u
LEFT JOIN orgs r ON r.id = u.home_region_id
WHERE u.id = :new_user_id;

\echo 'Account(s) being merged in, then DELETED:'
SELECT '  - ' || COALESCE(u.f3_name, '(no F3 name)') || ' <' || u.email || '>'
       || ' | name: ' || COALESCE(NULLIF(trim(concat_ws(' ', u.first_name, u.last_name)), ''), '-')
       || ' | home region: ' || COALESCE(r.name, '-')
       || ' | phone: ' || COALESCE(NULLIF(u.phone, ''), '-')
FROM users u
LEFT JOIN orgs r ON r.id = u.home_region_id
WHERE u.id = ANY (:'old_user_ids'::int[])
ORDER BY u.id;

\echo
\echo 'MOVING to the kept account:'

SELECT '  Attendance:   ' ||
  CASE WHEN COUNT(*) = 0 THEN 'none'
       ELSE COUNT(*) || ' record(s): '
            || COUNT(*) FILTER (WHERE NOT a.is_planned) || ' attended, '
            || COUNT(*) FILTER (WHERE a.is_planned) || ' planned'
            || ' (' || MIN(ei.start_date) || ' to ' || MAX(ei.start_date) || ')'
  END
FROM attendance a
LEFT JOIN event_instances ei ON ei.id = a.event_instance_id
WHERE a.user_id = ANY (:'old_user_ids'::int[])
  AND NOT EXISTS (SELECT 1 FROM attendance n
                   WHERE n.user_id = :new_user_id
                     AND n.event_instance_id = a.event_instance_id);

SELECT '  Roles:        ' ||
  COALESCE(E'\n    - ' || string_agg(DISTINCT label, E'\n    - ' ORDER BY label), 'none')
FROM (
  SELECT COALESCE(o.name::text, '?') || ' (' || COALESCE(o.org_type::text, '?') || '): ' || COALESCE(r.name::text, '?') AS label
  FROM roles_x_users_x_org ruo
  LEFT JOIN orgs o ON o.id = ruo.org_id
  LEFT JOIN roles r ON r.id = ruo.role_id
  WHERE ruo.user_id = ANY (:'old_user_ids'::int[])
    AND NOT EXISTS (SELECT 1 FROM roles_x_users_x_org n
                     WHERE n.user_id = :new_user_id
                       AND n.role_id = ruo.role_id
                       AND n.org_id  = ruo.org_id)
) x;

SELECT '  Positions:    ' ||
  COALESCE(E'\n    - ' || string_agg(DISTINCT label, E'\n    - ' ORDER BY label), 'none')
FROM (
  SELECT COALESCE(o.name::text, '?') || ' (' || COALESCE(o.org_type::text, '?') || '): ' || COALESCE(p.name::text, '?') AS label
  FROM positions_x_orgs_x_users pou
  LEFT JOIN orgs o ON o.id = pou.org_id
  LEFT JOIN positions p ON p.id = pou.position_id
  WHERE pou.user_id = ANY (:'old_user_ids'::int[])
) x;

SELECT '  Achievements: ' ||
  COALESCE(E'\n    - ' || string_agg(label, E'\n    - ' ORDER BY label), 'none')
FROM (
  SELECT COALESCE(a.name::text, '?')
         || CASE WHEN axu.award_year = -1 THEN ''
                 ELSE ' ' || axu.award_year
                      || COALESCE(' (period ' || NULLIF(array_to_string(ARRAY(
                           SELECT DISTINCT p FROM unnest(array_agg(axu.award_period)) p
                           WHERE p <> -1 ORDER BY p), ', '), '') || ')', '')
            END AS label
  FROM achievements_x_users axu
  LEFT JOIN achievements a ON a.id = axu.achievement_id
  WHERE axu.user_id = ANY (:'old_user_ids'::int[])
    AND NOT EXISTS (SELECT 1 FROM achievements_x_users n
                     WHERE n.user_id = :new_user_id
                       AND n.achievement_id = axu.achievement_id
                       AND n.award_year     = axu.award_year
                       AND n.award_period   = axu.award_period)
  GROUP BY a.name, axu.award_year
) x;

SELECT '  API keys:     ' ||
  COALESCE(E'\n    - ' || string_agg(COALESCE(k.name::text, '(unnamed)'), E'\n    - ' ORDER BY k.name), 'none')
FROM api_keys k
WHERE k.owner_id = ANY (:'old_user_ids'::int[]);

\echo
\echo 'ALREADY on the kept account (duplicates on the old account(s) are removed, nothing is lost):'

SELECT '  Attendance:   ' ||
  CASE WHEN COUNT(*) = 0 THEN 'none'
       ELSE COUNT(*) || ' record(s) at events the kept account is already recorded on'
  END
FROM attendance a
WHERE a.user_id = ANY (:'old_user_ids'::int[])
  AND EXISTS (SELECT 1 FROM attendance n
               WHERE n.user_id = :new_user_id
                 AND n.event_instance_id = a.event_instance_id);

SELECT '  Roles:        ' ||
  COALESCE(E'\n    - ' || string_agg(DISTINCT label, E'\n    - ' ORDER BY label), 'none')
FROM (
  SELECT COALESCE(o.name::text, '?') || ' (' || COALESCE(o.org_type::text, '?') || '): ' || COALESCE(r.name::text, '?') AS label
  FROM roles_x_users_x_org ruo
  LEFT JOIN orgs o ON o.id = ruo.org_id
  LEFT JOIN roles r ON r.id = ruo.role_id
  WHERE ruo.user_id = ANY (:'old_user_ids'::int[])
    AND EXISTS (SELECT 1 FROM roles_x_users_x_org n
                 WHERE n.user_id = :new_user_id
                   AND n.role_id = ruo.role_id
                   AND n.org_id  = ruo.org_id)
) x;

SELECT '  Achievements: ' ||
  COALESCE(E'\n    - ' || string_agg(label, E'\n    - ' ORDER BY label), 'none')
FROM (
  SELECT COALESCE(a.name::text, '?')
         || CASE WHEN axu.award_year = -1 THEN ''
                 ELSE ' ' || axu.award_year
                      || COALESCE(' (period ' || NULLIF(array_to_string(ARRAY(
                           SELECT DISTINCT p FROM unnest(array_agg(axu.award_period)) p
                           WHERE p <> -1 ORDER BY p), ', '), '') || ')', '')
            END AS label
  FROM achievements_x_users axu
  LEFT JOIN achievements a ON a.id = axu.achievement_id
  WHERE axu.user_id = ANY (:'old_user_ids'::int[])
    AND EXISTS (SELECT 1 FROM achievements_x_users n
                 WHERE n.user_id = :new_user_id
                   AND n.achievement_id = axu.achievement_id
                   AND n.award_year     = axu.award_year
                   AND n.award_period   = axu.award_period)
  GROUP BY a.name, axu.award_year
) x;

\echo
\echo 'REMOVED / NOT carried over:'
\echo '  Profile:      F3 name, name, phone, home region, emergency contact and avatar on the'
\echo '                old account(s) are NOT copied. The kept account''s profile is unchanged.'

SELECT '  Slack links:  ' ||
  COALESCE(E'\n    - ' || string_agg(COALESCE(su.user_name::text, '?') || ' in ' || COALESCE(ss.workspace_name::text, su.slack_team_id::text, '?'),
                                     E'\n    - ' ORDER BY ss.workspace_name, su.user_name), 'none')
FROM slack_users su
LEFT JOIN slack_spaces ss ON su.slack_team_id = ss.team_id
WHERE su.user_id = ANY (:'old_user_ids'::int[]);

SELECT '  App logins:   ' ||
  CASE WHEN n = 0 THEN 'none'
       ELSE n || ' login/session record(s) on the old account(s) removed; sign in with the kept email going forward'
  END
FROM (
  SELECT (SELECT COUNT(*) FROM auth.oauth_authorization_codes WHERE user_id = ANY (:'old_user_ids'::int[]))
       + (SELECT COUNT(*) FROM auth.oauth_access_tokens       WHERE user_id = ANY (:'old_user_ids'::int[]))
       + (SELECT COUNT(*) FROM auth.oauth_refresh_tokens      WHERE user_id = ANY (:'old_user_ids'::int[]))
       + (SELECT COUNT(*) FROM auth.better_auth_user          WHERE f3_user_id = ANY (:'old_user_ids'::int[]))
       + (SELECT COUNT(*) FROM auth_sessions                  WHERE user_id = ANY (:'old_user_ids'::int[])) AS n
) x;

SELECT '  Expansions:   ' || COUNT(*) || ' (not migrated)'
FROM expansions_x_users
WHERE user_id = ANY (:'old_user_ids'::int[])
HAVING COUNT(*) > 0;

\echo '==================================================='
\echo 'END MERGE SUMMARY'
\echo '==================================================='

\pset format aligned
\pset tuples_only off

------------------------------------------------------------
-- Start Transaction
------------------------------------------------------------
\if :verbose
\echo
\echo '==================================================='
\echo 'Starting Transaction'
\endif
BEGIN;

------------------------------------------------------------
-- Delete old slack user
------------------------------------------------------------
\if :verbose
\echo
\echo '==================================================='
\echo 'Deleteing slack users for old user IDs'

SELECT su.user_id, su.user_name, ss.workspace_name
FROM slack_users su
left join slack_spaces ss on su.slack_team_id = ss.team_id
WHERE su.user_id = ANY (:'old_user_ids');
\endif

DELETE FROM slack_users
WHERE user_id = ANY (:'old_user_ids');

SELECT COUNT(*) AS deleted_slack_users
FROM slack_users
WHERE user_id = ANY (:'old_user_ids')
\gset
\if :verbose
\echo 'Deleted slack users count (should be 0): ':deleted_slack_users
\endif

------------------------------------------------------------
-- Update role mappings
------------------------------------------------------------
\if :verbose
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
\endif
DELETE FROM roles_x_users_x_org ruo_old
WHERE ruo_old.user_id = ANY (:'old_user_ids')
  AND EXISTS (
      SELECT 1
      FROM roles_x_users_x_org ruo_new
      WHERE ruo_new.user_id = :'new_user_id'
        AND ruo_new.role_id = ruo_old.role_id
        AND ruo_new.org_id  = ruo_old.org_id
  );

\if :verbose
\echo 'Updating remaining roles'
\endif
UPDATE roles_x_users_x_org
SET user_id = :'new_user_id'
WHERE user_id = ANY (:'old_user_ids');

SELECT COUNT(*) AS remaining_old_role_mappings
FROM roles_x_users_x_org
WHERE user_id = ANY (:'old_user_ids')
\gset
\if :verbose
\echo 'Remaining role mappings for old user IDs (should be 0): ':remaining_old_role_mappings
\endif

------------------------------------------------------------
-- Update position mappings
------------------------------------------------------------
\if :verbose
\echo
\echo '==================================================='
\echo 'Updating position mappings to new user ID'

select u.f3_name, o."name" as org, o."org_type" , p."name"
from positions_x_orgs_x_users pou
left join orgs o on pou.org_id = o.id
left join users u on pou.user_id = u.id
left join positions p on pou.position_id = p.id
WHERE pou.user_id = ANY (:'old_user_ids');
\endif

UPDATE positions_x_orgs_x_users
SET user_id = :'new_user_id'
WHERE user_id = ANY (:'old_user_ids');

SELECT COUNT(*) AS remaining_old_position_mappings
FROM positions_x_orgs_x_users
WHERE user_id = ANY (:'old_user_ids')
\gset
\if :verbose
\echo 'Remaining position mappings for old user IDs (should be 0): ':remaining_old_position_mappings
\endif

------------------------------------------------------------
-- Update achievements
------------------------------------------------------------
\if :verbose
\echo
\echo '==================================================='
\echo 'Updating achievement mappings to new user ID'

\echo 'Old user(s) achievement mappings:'
SELECT u.f3_name, a.name, axu.award_year, axu.award_period, axu.date_awarded
FROM achievements_x_users axu
LEFT JOIN users u ON axu.user_id = u.id
LEFT JOIN achievements a ON axu.achievement_id = a.id
WHERE axu.user_id = ANY (:'old_user_ids');

\echo 'New user achievement mappings:'
SELECT u.f3_name, a.name, axu.award_year, axu.award_period, axu.date_awarded
FROM achievements_x_users axu
LEFT JOIN users u ON axu.user_id = u.id
LEFT JOIN achievements a ON axu.achievement_id = a.id
WHERE axu.user_id = :'new_user_id';

\echo 'Deleting achievements for old users that the new user already has'
\endif
DELETE FROM achievements_x_users axu_old
WHERE axu_old.user_id = ANY (:'old_user_ids')
  AND EXISTS (
      SELECT 1
      FROM achievements_x_users axu_new
      WHERE axu_new.user_id = :'new_user_id'
        AND axu_new.achievement_id = axu_old.achievement_id
        AND axu_new.award_year    = axu_old.award_year
        AND axu_new.award_period  = axu_old.award_period
  );

\if :verbose
\echo 'Updating remaining achievements'
\endif
UPDATE achievements_x_users
SET user_id = :'new_user_id'
WHERE user_id = ANY (:'old_user_ids');

SELECT COUNT(*) AS remaining_old_achievement_mappings
FROM achievements_x_users
WHERE user_id = ANY (:'old_user_ids')
\gset
\if :verbose
\echo 'Remaining achievement mappings for old user IDs (should be 0): ':remaining_old_achievement_mappings
\endif

------------------------------------------------------------
-- Update API Keys
------------------------------------------------------------
\if :verbose
\echo
\echo '==================================================='
\echo 'Updating API Keys to new user ID'

select u.f3_name, k."name"
from api_keys k
left join users u on k.owner_id = u.id
WHERE k.owner_id = ANY (:'old_user_ids');
\endif

UPDATE api_keys
SET owner_id = :'new_user_id'
WHERE owner_id = ANY (:'old_user_ids');

SELECT COUNT(*) AS remaining_old_permission_mappings
FROM positions_x_orgs_x_users
WHERE user_id = ANY (:'old_user_ids')
\gset
\if :verbose
\echo 'Remaining API Key mappings for old user IDs (should be 0): ':remaining_old_permission_mappings
\endif

------------------------------------------------------------
-- Update expansion
------------------------------------------------------------
\if :verbose
\echo
\echo '==================================================='
\echo 'Expansion remapping is not currently implemented. We''re not currently using it.'
\endif

------------------------------------------------------------
-- Update attendance
------------------------------------------------------------
\if :verbose
\echo
\echo '==================================================='
\echo 'Updating attendance to new user ID'
\endif

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
\if :verbose
\echo 'Deleted ':attendance_deleted_planned' attendance for old users when old and new users were both mapped and is_planned is TRUE.'
\endif

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
\if :verbose
\echo 'Deleted ':attendance_deleted_notplanned' attendance for old users when old and new users were both mapped and is_planned is FALSE.'
\endif

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
\if :verbose
\echo 'Updated ':attendance_updated' attendance records to new user ID.'
\endif

SELECT COUNT(*) AS remaining_old_attendance
FROM attendance
WHERE user_id = ANY (:'old_user_ids'::int[])
\gset
\if :verbose
\echo 'Remaining attendance records for old user IDs (should be 0): ':remaining_old_attendance
\endif

------------------------------------------------------------
-- Delete auth schema records for old users
------------------------------------------------------------
\if :verbose
\echo
\echo '==================================================='
\echo 'Deleting auth schema records for old user IDs'

\echo 'Old user(s) auth.oauth_authorization_codes:'
SELECT u.f3_name, c.user_id, c.created_at
FROM auth.oauth_authorization_codes c
LEFT JOIN users u ON c.user_id = u.id
WHERE c.user_id = ANY (:'old_user_ids'::int[]);
\endif

DELETE FROM auth.oauth_authorization_codes
WHERE user_id = ANY (:'old_user_ids'::int[]);

SELECT COUNT(*) AS remaining_oauth_auth_codes
FROM auth.oauth_authorization_codes
WHERE user_id = ANY (:'old_user_ids'::int[])
\gset
\if :verbose
\echo 'Remaining auth.oauth_authorization_codes for old user IDs (should be 0): ':remaining_oauth_auth_codes

\echo 'Old user(s) auth.oauth_access_tokens:'
SELECT u.f3_name, t.user_id, t.created_at
FROM auth.oauth_access_tokens t
LEFT JOIN users u ON t.user_id = u.id
WHERE t.user_id = ANY (:'old_user_ids'::int[]);
\endif

DELETE FROM auth.oauth_access_tokens
WHERE user_id = ANY (:'old_user_ids'::int[]);

SELECT COUNT(*) AS remaining_oauth_access_tokens
FROM auth.oauth_access_tokens
WHERE user_id = ANY (:'old_user_ids'::int[])
\gset
\if :verbose
\echo 'Remaining auth.oauth_access_tokens for old user IDs (should be 0): ':remaining_oauth_access_tokens

\echo 'Old user(s) auth.oauth_refresh_tokens:'
SELECT u.f3_name, t.user_id, t.created_at
FROM auth.oauth_refresh_tokens t
LEFT JOIN users u ON t.user_id = u.id
WHERE t.user_id = ANY (:'old_user_ids'::int[]);
\endif

DELETE FROM auth.oauth_refresh_tokens
WHERE user_id = ANY (:'old_user_ids'::int[]);

SELECT COUNT(*) AS remaining_oauth_refresh_tokens
FROM auth.oauth_refresh_tokens
WHERE user_id = ANY (:'old_user_ids'::int[])
\gset
\if :verbose
\echo 'Remaining auth.oauth_refresh_tokens for old user IDs (should be 0): ':remaining_oauth_refresh_tokens
\endif

------------------------------------------------------------
-- Compare actual changes against the merge summary (READ-ONLY)
------------------------------------------------------------
SELECT
  (:attendance_deleted_planned + :attendance_deleted_notplanned = :exp_att_dup
   AND :attendance_updated = :exp_att_move
   AND (SELECT COUNT(*) FROM attendance WHERE user_id = :new_user_id)
       = :exp_new_attendance_base + :exp_att_move
   AND (SELECT COUNT(*) FROM roles_x_users_x_org      WHERE user_id = :new_user_id) = :exp_new_roles
   AND (SELECT COUNT(*) FROM positions_x_orgs_x_users WHERE user_id = :new_user_id) = :exp_new_positions
   AND (SELECT COUNT(*) FROM achievements_x_users     WHERE user_id = :new_user_id) = :exp_new_achievements
   AND (SELECT COUNT(*) FROM api_keys                 WHERE owner_id = :new_user_id) = :exp_new_api_keys
  ) AS summary_matches,
  (:deleted_slack_users + :remaining_old_role_mappings + :remaining_old_position_mappings
   + :remaining_old_achievement_mappings + :remaining_old_attendance
   + :remaining_oauth_auth_codes + :remaining_oauth_access_tokens + :remaining_oauth_refresh_tokens
   + (SELECT COUNT(*) FROM api_keys WHERE owner_id = ANY (:'old_user_ids'::int[]))
  ) = 0 AS old_refs_cleared
\gset

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
  \echo '  - Update achievements listed above'
  \echo '  - Update API Keys listed above'
  \echo '  - Update expansions listed above (NOT IMPLEMENTED)'
  \echo '  - Update attendance records indicated above'
  \echo '  - Delete auth schema records (oauth_authorization_codes, oauth_access_tokens, oauth_refresh_tokens) listed above'
  \echo
  \if :summary_matches
    \echo 'Actual changes match the MERGE SUMMARY.'
  \else
    \echo '!! WARNING: actual changes do NOT match the MERGE SUMMARY. Review the detail above before committing.'
  \endif
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
\if :verbose
\echo
\echo '==================================================='
\echo 'Deleting old users'
\endif

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

SELECT :remaining_old_users = 0 AS old_users_deleted
\gset

\if :verbose
\echo
\echo 'Total users before deletion: ':all_users
\echo 'Total users after deletion: ':remaining_all_users
\echo 'Remaining old users (should be 0): ':remaining_old_users
\echo
\endif

\if :commit
  \echo 'Old user(s) deleted, but not committed. Do the above number look right? You sure?'
  \echo 'Press ENTER to COMMIT changes or Ctrl+C to ROLLBACK deletion of old users.'
  \echo 'Aborting now will not rollback the merge operations, those changes will remain.'
  \prompt confirm
  \echo 'Committing changes.'
  COMMIT;
\else
  ROLLBACK;
  \echo
  \echo 'Validation (dry run):'
  \if :summary_matches
    \echo '  OK   Actual changes matched the MERGE SUMMARY'
  \else
    \echo '  FAIL Actual changes did NOT match the MERGE SUMMARY - rerun with -v verbose=true'
  \endif
  \if :old_refs_cleared
    \echo '  OK   No references to the old user(s) remained after the merge'
  \else
    \echo '  FAIL References to the old user(s) remained - rerun with -v verbose=true'
  \endif
  \if :old_users_deleted
    \echo '  OK   Old user(s) deleted cleanly (':all_users' -> ':remaining_all_users' users)'
  \else
    \echo '  FAIL Old user(s) still present after delete - rerun with -v verbose=true'
  \endif
  \echo
  \echo 'Dry run complete. All changes rolled back.'
  \echo 'To commit, rerun the script with -v commit=true'
\endif
