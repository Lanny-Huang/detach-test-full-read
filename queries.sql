-- @query: cohort_sizes
-- Distinct engagement counts per recipe + low-intent holdout
SELECT
    CASE
        WHEN li.engagement_id IS NOT NULL THEN 'Holdout_LowIntent'
        WHEN t.recipe = 'Holdout' THEN 'Holdout_Full'
        WHEN t.recipe = 'detach messaging' THEN 'Detach'
        WHEN t.recipe = 'unassign messaging' THEN 'Unassign'
    END AS cohort,
    COUNT(DISTINCT t.engagement_id) AS n
FROM cgan_ustax_published.`3_9_detach_test_read` t
LEFT JOIN cgan_ustax_published.lh_low_intent_holdout_ids li
    ON t.engagement_id = li.engagement_id
GROUP BY 1
ORDER BY 1;

-- @query: fs_filings
-- FS filing counts per cohort, with daily breakdown (days since 3/10 message)
SELECT
    CASE
        WHEN li.engagement_id IS NOT NULL THEN 'Holdout_LowIntent'
        WHEN t.recipe = 'Holdout' THEN 'Holdout_Full'
        WHEN t.recipe = 'detach messaging' THEN 'Detach'
        WHEN t.recipe = 'unassign messaging' THEN 'Unassign'
    END AS cohort,
    DATEDIFF(DATE(from_utc_timestamp(e.filing_end_ts, 'US/Pacific')), DATE('2026-03-10')) AS days_since_msg,
    COUNT(DISTINCT t.engagement_id) AS filings
FROM cgan_ustax_published.`3_9_detach_test_read` t
LEFT JOIN cgan_ustax_published.lh_low_intent_holdout_ids li
    ON t.engagement_id = li.engagement_id
INNER JOIN cgan_ustax_published.fs_engagements_master e
    ON t.engagement_id = e.engagement_id
    AND e.tax_year = 2025
    AND e.filing_end_ts IS NOT NULL
GROUP BY 1, 2
ORDER BY 1, 2;

-- @query: filing_windows
-- Cumulative filing counts at 3, 5, 7, 10, 15-day windows from message date (3/10)
SELECT
    cohort,
    SUM(CASE WHEN days_since_msg BETWEEN 0 AND 3 THEN filings ELSE 0 END) AS w_0_3,
    SUM(CASE WHEN days_since_msg BETWEEN 0 AND 5 THEN filings ELSE 0 END) AS w_0_5,
    SUM(CASE WHEN days_since_msg BETWEEN 0 AND 7 THEN filings ELSE 0 END) AS w_0_7,
    SUM(CASE WHEN days_since_msg BETWEEN 0 AND 10 THEN filings ELSE 0 END) AS w_0_10,
    SUM(CASE WHEN days_since_msg >= 0 THEN filings ELSE 0 END) AS w_all
FROM (
    SELECT
        CASE
            WHEN li.engagement_id IS NOT NULL THEN 'Holdout_LowIntent'
            WHEN t.recipe = 'Holdout' THEN 'Holdout_Full'
            WHEN t.recipe = 'detach messaging' THEN 'Detach'
            WHEN t.recipe = 'unassign messaging' THEN 'Unassign'
        END AS cohort,
        DATEDIFF(DATE(from_utc_timestamp(e.filing_end_ts, 'US/Pacific')), DATE('2026-03-10')) AS days_since_msg,
        COUNT(DISTINCT t.engagement_id) AS filings
    FROM cgan_ustax_published.`3_9_detach_test_read` t
    LEFT JOIN cgan_ustax_published.lh_low_intent_holdout_ids li
        ON t.engagement_id = li.engagement_id
    INNER JOIN cgan_ustax_published.fs_engagements_master e
        ON t.engagement_id = e.engagement_id
        AND e.tax_year = 2025
        AND e.filing_end_ts IS NOT NULL
    GROUP BY 1, 2
) sub
GROUP BY cohort
ORDER BY cohort;

-- @query: post_action_filings
-- Filing counts after action date: 3/24 for Detach, 3/19 for Unassign
-- Returns filing counts at 0-1, 0-3, 0-5, 0-7 day windows after each action
SELECT
    cohort,
    SUM(CASE WHEN days_since_action BETWEEN 0 AND 1 THEN 1 ELSE 0 END) AS w_0_1,
    SUM(CASE WHEN days_since_action BETWEEN 0 AND 3 THEN 1 ELSE 0 END) AS w_0_3,
    SUM(CASE WHEN days_since_action BETWEEN 0 AND 5 THEN 1 ELSE 0 END) AS w_0_5,
    SUM(CASE WHEN days_since_action BETWEEN 0 AND 7 THEN 1 ELSE 0 END) AS w_0_7
FROM (
    SELECT
        CASE
            WHEN t.recipe = 'detach messaging' THEN 'Detach'
            WHEN t.recipe = 'unassign messaging' THEN 'Unassign'
        END AS cohort,
        CASE
            WHEN t.recipe = 'detach messaging'
                THEN DATEDIFF(DATE(from_utc_timestamp(e.filing_end_ts, 'US/Pacific')), DATE('2026-03-24'))
            WHEN t.recipe = 'unassign messaging'
                THEN DATEDIFF(DATE(from_utc_timestamp(e.filing_end_ts, 'US/Pacific')), DATE('2026-03-19'))
        END AS days_since_action
    FROM cgan_ustax_published.`3_9_detach_test_read` t
    INNER JOIN cgan_ustax_published.fs_engagements_master e
        ON t.engagement_id = e.engagement_id
        AND e.tax_year = 2025
        AND e.filing_end_ts IS NOT NULL
    WHERE t.recipe IN ('detach messaging', 'unassign messaging')
) sub
WHERE cohort IS NOT NULL AND days_since_action >= 0
GROUP BY cohort
ORDER BY cohort;

-- @query: engagement_states
-- Current engagement state: WIP w/ Expert, WIP No Expert, Completed Filed, Completed Not Filed
SELECT
    CASE
        WHEN li.engagement_id IS NOT NULL THEN 'Holdout_LowIntent'
        WHEN t.recipe = 'Holdout' THEN 'Holdout_Full'
        WHEN t.recipe = 'detach messaging' THEN 'Detach'
        WHEN t.recipe = 'unassign messaging' THEN 'Unassign'
    END AS cohort,
    CASE
        WHEN e.current_engagement_status = 'COMPLETED' AND e.filing_end_ts IS NOT NULL THEN 'Completed_Filed'
        WHEN e.current_engagement_status = 'COMPLETED' AND e.filing_end_ts IS NULL THEN 'Completed_Not_Filed'
        WHEN e.expert_assigned_flag = 1 THEN 'WIP_Expert'
        ELSE 'WIP_No_Expert'
    END AS state,
    COUNT(DISTINCT t.engagement_id) AS cnt
FROM cgan_ustax_published.`3_9_detach_test_read` t
LEFT JOIN cgan_ustax_published.lh_low_intent_holdout_ids li
    ON t.engagement_id = li.engagement_id
LEFT JOIN cgan_ustax_published.fs_engagements_master e
    ON t.engagement_id = e.engagement_id
    AND e.tax_year = 2025
GROUP BY 1, 2
ORDER BY 1, 2;

-- @query: wip_clearing
-- Off-queue (not WIP) rates per cohort
SELECT
    CASE
        WHEN li.engagement_id IS NOT NULL THEN 'Holdout_LowIntent'
        WHEN t.recipe = 'Holdout' THEN 'Holdout_Full'
        WHEN t.recipe = 'detach messaging' THEN 'Detach'
        WHEN t.recipe = 'unassign messaging' THEN 'Unassign'
    END AS cohort,
    COUNT(DISTINCT t.engagement_id) AS total,
    COUNT(DISTINCT CASE
        WHEN e.current_engagement_status IN ('COMPLETED', 'REVOKED')
            OR e.engagement_id IS NULL
        THEN t.engagement_id
    END) AS off_queue
FROM cgan_ustax_published.`3_9_detach_test_read` t
LEFT JOIN cgan_ustax_published.lh_low_intent_holdout_ids li
    ON t.engagement_id = li.engagement_id
LEFT JOIN cgan_ustax_published.fs_engagements_master e
    ON t.engagement_id = e.engagement_id
    AND e.tax_year = 2025
GROUP BY 1
ORDER BY 1;

-- @query: milestones
-- Current milestone distribution per cohort
SELECT
    CASE
        WHEN li.engagement_id IS NOT NULL THEN 'Holdout_LowIntent'
        WHEN t.recipe = 'Holdout' THEN 'Holdout_Full'
        WHEN t.recipe = 'detach messaging' THEN 'Detach'
        WHEN t.recipe = 'unassign messaging' THEN 'Unassign'
    END AS cohort,
    COALESCE(e.main_funnel_milestone, 'Unknown') AS milestone,
    COUNT(DISTINCT t.engagement_id) AS cnt
FROM cgan_ustax_published.`3_9_detach_test_read` t
LEFT JOIN cgan_ustax_published.lh_low_intent_holdout_ids li
    ON t.engagement_id = li.engagement_id
LEFT JOIN cgan_ustax_published.fs_engagements_master e
    ON t.engagement_id = e.engagement_id
    AND e.tax_year = 2025
GROUP BY 1, 2
ORDER BY 1, 2;

-- @query: diwm
-- DIWM/DIY re-engagement: starts/completes per cohort, split by pre/post 3/10 message
SELECT
    CASE
        WHEN li.engagement_id IS NOT NULL THEN 'Holdout_LowIntent'
        WHEN t.recipe = 'Holdout' THEN 'Holdout_Full'
        WHEN t.recipe = 'detach messaging' THEN 'Detach'
        WHEN t.recipe = 'unassign messaging' THEN 'Unassign'
    END AS cohort,
    COUNT(DISTINCT d.auth_id) AS ever_started,
    COUNT(DISTINCT CASE WHEN d.first_start_date_adj >= DATE('2026-03-10') THEN d.auth_id END) AS started_post_msg,
    COUNT(DISTINCT CASE WHEN d.first_completed_date_adj IS NOT NULL THEN d.auth_id END) AS completed,
    COUNT(DISTINCT CASE WHEN d.first_completed_date_adj >= DATE('2026-03-10') THEN d.auth_id END) AS completed_post_msg,
    SUM(CASE WHEN d.first_completed_date_adj >= DATE('2026-03-10') THEN d.total_revenue ELSE 0 END) AS rev_post_msg
FROM cgan_ustax_published.`3_9_detach_test_read` t
LEFT JOIN cgan_ustax_published.lh_low_intent_holdout_ids li
    ON t.engagement_id = li.engagement_id
LEFT JOIN cgan_ustax_published.fs_engagements_master e
    ON t.engagement_id = e.engagement_id
    AND e.tax_year = 2025
LEFT JOIN cgan_ustax_published.diwm_funnel_summary_ty25 d
    ON CAST(e.auth_id AS BIGINT) = d.auth_id
    AND d.diwm_unit = 1
GROUP BY 1
ORDER BY 1;

-- @query: appointments
-- Daily first appointments handled per cohort, days since 3/10 message
SELECT
    CASE
        WHEN li.engagement_id IS NOT NULL THEN 'Holdout_LowIntent'
        WHEN t.recipe = 'Holdout' THEN 'Holdout_Full'
        WHEN t.recipe = 'detach messaging' THEN 'Detach'
        WHEN t.recipe = 'unassign messaging' THEN 'Unassign'
    END AS cohort,
    DATEDIFF(
        DATE(from_utc_timestamp(e.first_appointment_datetime_handled, 'US/Pacific')),
        DATE('2026-03-10')
    ) AS days_since_msg,
    COUNT(DISTINCT t.engagement_id) AS appts
FROM cgan_ustax_published.`3_9_detach_test_read` t
LEFT JOIN cgan_ustax_published.lh_low_intent_holdout_ids li
    ON t.engagement_id = li.engagement_id
INNER JOIN cgan_ustax_published.fs_engagements_master e
    ON t.engagement_id = e.engagement_id
    AND e.tax_year = 2025
    AND e.first_appointment_datetime_handled IS NOT NULL
    AND DATE(from_utc_timestamp(e.first_appointment_datetime_handled, 'US/Pacific')) >= DATE('2026-03-10')
GROUP BY 1, 2
ORDER BY 1, 2;
