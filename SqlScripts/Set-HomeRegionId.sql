-- ============================================================
-- Set-HomeRegionId.sql
--
-- Help / Usage
--
-- This script identifies users with valid email address and no 
-- home_region_id, then analyzes their attendance over a specified
-- time period to determine their most-attended region. That region
-- is then set as their home_region_id.
--
-- The script first shows a preview of which users would be affected
-- and which regions would be assigned, then performs the update.
--
-- Examples:
--
--    Dry run (preview only, rolls back changes) - uses default 90 days:
--    psql service=staging -f SqlScripts/Set-HomeRegionId.sql
--
--    Look back 180 days:
--    psql service=staging -f SqlScripts/Set-HomeRegionId.sql -v days_back=180
--
--    Commit changes:
--    psql service=staging -f SqlScripts/Set-HomeRegionId.sql -v commit=true -v days_back=180
--
-- ============================================================

\set ON_ERROR_STOP on
\set QUIET on
\pset footer off

------------------------------------------------------------
-- Set defaults for optional parameters
------------------------------------------------------------
\if :{?days_back}
\else
  \set days_back 90
\endif

\if :{?commit}
\else
  \set commit false
\endif

\echo
\echo '==================================================='
\echo 'Configuration:'
\echo ' Days back: ':days_back' days'
\if :commit
  \echo ' Execution mode: COMMIT (changes will be permanent)'
\else
  \echo ' Execution mode: DRY RUN (ROLLBACK only)'
  \echo ' To commit, use: psql ... -v commit=true'
\endif

\echo
\echo '==================================================='
\echo 'Database connection:'
SELECT current_database() AS database
\gset

\echo 'Database: ':database
\echo 'Lookback period: ':days_back' days'
\echo

------------------------------------------------------------
-- Start transaction
------------------------------------------------------------
BEGIN;

\echo '==================================================='
\echo 'Finding eligible users with no home_region_id and recent activity...'
\echo

------------------------------------------------------------
-- Verify user eligibility with and without lookback period
------------------------------------------------------------
\echo 'Eligibility breakdown:'
\echo

SELECT
    (SELECT COUNT(*) FROM users WHERE home_region_id IS NULL AND email LIKE '%@%') AS users_without_home_region,
    (SELECT COUNT(DISTINCT ae.user_id) 
     FROM attendance_expanded ae
     JOIN event_instance_expanded eie ON ae.event_instance_id = eie.id
     JOIN users u ON u.id = ae.user_id
     WHERE u.home_region_id IS NULL 
       AND u.email LIKE '%@%'
       AND eie.start_date >= CURRENT_DATE - (:days_back::INTEGER || ' days')::INTERVAL
    ) AS with_recent_attendance_in_lookback_period;

\echo

WITH eligible_users AS (
    SELECT DISTINCT ae.user_id
    FROM attendance_expanded ae
    JOIN event_instance_expanded eie ON ae.event_instance_id = eie.id
    JOIN users u ON u.id = ae.user_id
    WHERE u.home_region_id IS NULL
      AND u.email LIKE '%@%'
      AND eie.start_date >= CURRENT_DATE - (:days_back::INTEGER || ' days')::INTERVAL
)
SELECT COUNT(*) AS eligible_user_count
FROM eligible_users
\gset

\echo 'Found ':eligible_user_count' eligible users with recent activity in ':days_back' days'
\echo

------------------------------------------------------------
-- Preview: Show which users would be affected and how
------------------------------------------------------------
\echo '==================================================='
\echo 'PREVIEW: Users to be updated and their assigned regions'
\echo '==================================================='
\echo

WITH eligible_users AS (
    SELECT DISTINCT ae.user_id
    FROM attendance_expanded ae
    JOIN event_instance_expanded eie ON ae.event_instance_id = eie.id
    JOIN users u ON u.id = ae.user_id
    WHERE u.home_region_id IS NULL
      AND u.email LIKE '%@%'
      AND eie.start_date >= CURRENT_DATE - (:days_back::INTEGER || ' days')::INTERVAL
),
recent_attendance AS (
    SELECT
        ae.user_id,
        eie.region_org_id,
        eie.region_name,
        COUNT(*) AS attendance_count,
        MAX(eie.start_date) AS last_attended
    FROM attendance_expanded ae
    JOIN event_instance_expanded eie
        ON ae.event_instance_id = eie.id
    JOIN eligible_users eu
        ON eu.user_id = ae.user_id
    WHERE eie.start_date >= CURRENT_DATE - (:days_back::INTEGER || ' days')::INTERVAL
    GROUP BY
        ae.user_id,
        eie.region_org_id,
        eie.region_name
),
ranked_regions AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY user_id
            ORDER BY attendance_count DESC, last_attended DESC
        ) AS rn
    FROM recent_attendance
)
SELECT
    user_id,
    u.f3_name,
    region_org_id AS proposed_home_region_id,
    region_name,
    attendance_count,
    last_attended
FROM ranked_regions
LEFT JOIN users u ON ranked_regions.user_id = u.id
WHERE rn = 1
ORDER BY user_id;

\echo
\echo 'Preview complete. Ready to apply updates.'
\echo

SELECT COUNT(DISTINCT user_id) AS users_to_update
FROM (
    WITH eligible_users AS (
        SELECT DISTINCT ae.user_id
        FROM attendance_expanded ae
        JOIN event_instance_expanded eie ON ae.event_instance_id = eie.id
        JOIN users u ON u.id = ae.user_id
        WHERE u.home_region_id IS NULL
          AND u.email LIKE '%@%'
          AND eie.start_date >= CURRENT_DATE - (:days_back::INTEGER || ' days')::INTERVAL
    ),
    recent_attendance AS (
        SELECT
            ae.user_id,
            eie.region_org_id,
            COUNT(*) AS attendance_count,
            MAX(eie.start_date) AS last_attended
        FROM attendance_expanded ae
        JOIN event_instance_expanded eie
            ON ae.event_instance_id = eie.id
        JOIN eligible_users eu
            ON eu.user_id = ae.user_id
        WHERE eie.start_date >= CURRENT_DATE - (:days_back::INTEGER || ' days')::INTERVAL
        GROUP BY
            ae.user_id,
            eie.region_org_id
    ),
    ranked_regions AS (
        SELECT
            user_id,
            region_org_id,
            ROW_NUMBER() OVER (
                PARTITION BY user_id
                ORDER BY attendance_count DESC, last_attended DESC
            ) AS rn
        FROM recent_attendance
    )
    SELECT user_id
    FROM ranked_regions
    WHERE rn = 1
) updates
\gset

\echo
\echo 'Total users to be updated: ':users_to_update
\echo

------------------------------------------------------------
-- Apply updates
------------------------------------------------------------
\echo '==================================================='
\echo 'Applying home_region_id updates...'
\echo

SELECT COUNT(*) AS users_before_update
FROM users
WHERE home_region_id IS NOT NULL
\gset

WITH eligible_users AS (
    SELECT DISTINCT ae.user_id
    FROM attendance_expanded ae
    JOIN event_instance_expanded eie ON ae.event_instance_id = eie.id
    JOIN users u ON u.id = ae.user_id
    WHERE u.home_region_id IS NULL
      AND u.email LIKE '%@%'
      AND eie.start_date >= CURRENT_DATE - (:days_back::INTEGER || ' days')::INTERVAL
),
recent_attendance AS (
    SELECT
        ae.user_id,
        eie.region_org_id,
        COUNT(*) AS attendance_count,
        MAX(eie.start_date) AS last_attended
    FROM attendance_expanded ae
    JOIN event_instance_expanded eie
        ON ae.event_instance_id = eie.id
    JOIN eligible_users eu
        ON eu.user_id = ae.user_id
    WHERE eie.start_date >= CURRENT_DATE - (:days_back::INTEGER || ' days')::INTERVAL
    GROUP BY
        ae.user_id,
        eie.region_org_id
),
ranked_regions AS (
    SELECT
        user_id,
        region_org_id,
        ROW_NUMBER() OVER (
            PARTITION BY user_id
            ORDER BY attendance_count DESC, last_attended DESC
        ) AS rn
    FROM recent_attendance
)
UPDATE users u
SET home_region_id = rr.region_org_id
FROM ranked_regions rr
WHERE u.id = rr.user_id
  AND rr.rn = 1;

\echo 'Updates applied to users table.'
\echo

SELECT COUNT(*) AS users_after_update
FROM users
WHERE home_region_id IS NOT NULL
\gset

\echo 'Users with home_region_id before: ':users_before_update
\echo 'Users with home_region_id after: ':users_after_update
\echo 'Net change: ':users_after_update ' - ' :users_before_update' new home regions assigned'
\echo

------------------------------------------------------------
-- Final verification
------------------------------------------------------------
\echo '==================================================='
\echo 'Verification: Checking for any eligible users still without home_region_id'
\echo

SELECT 
    COUNT(*) AS still_unassigned,
    COUNT(*) > 0 AS has_unassigned
FROM users
WHERE home_region_id IS NULL
  AND email LIKE '%@%'
\gset

\if :has_unassigned
  \echo 'WARNING: Found ':still_unassigned' users still without home_region_id'
\else
  \echo 'SUCCESS: All eligible users now have a home_region_id assigned'
\endif

\echo

------------------------------------------------------------
-- Commit or rollback
------------------------------------------------------------
\echo '==================================================='

\if :commit
  \echo 'COMMITTING changes (permanent)'
  COMMIT;
  \echo 'SUCCESS: Changes have been committed.'
\else
  \echo 'DRY RUN: Rolling back all changes'
  ROLLBACK;
  \echo 'Changes rolled back. No data was modified.'
  \echo
  \echo 'To commit these changes, rerun the script with: -v commit=true'
\endif

\echo '=================================================='
\echo 'Script complete.'
\echo '=='================================================
