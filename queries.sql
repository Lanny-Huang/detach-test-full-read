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

-- @query: engagement_detail
-- One row per engagement with recipe, low-intent flag, auth_id, filing, states
-- Python uses this + local CSV lookups to build the 6-cohort classification
SELECT
    t.engagement_id,
    t.recipe,
    CASE WHEN li.engagement_id IS NOT NULL THEN 1 ELSE 0 END AS low_intent_flag,
    e.auth_id,
    e.current_engagement_status,
    e.expert_assigned_flag,
    e.main_funnel_milestone,
    CASE WHEN e.filing_end_ts IS NOT NULL THEN 1 ELSE 0 END AS fs_filed,
    DATE(from_utc_timestamp(e.filing_end_ts, 'US/Pacific')) AS filing_date,
    DATEDIFF(DATE(from_utc_timestamp(e.filing_end_ts, 'US/Pacific')), DATE('2026-03-10')) AS days_since_msg,
    DATE(from_utc_timestamp(e.first_appointment_datetime_handled, 'US/Pacific')) AS first_appt_date,
    DATEDIFF(
        DATE(from_utc_timestamp(e.first_appointment_datetime_handled, 'US/Pacific')),
        DATE('2026-03-10')
    ) AS appt_days_since_msg
FROM cgan_ustax_published.`3_9_detach_test_read` t
LEFT JOIN cgan_ustax_published.lh_low_intent_holdout_ids li
    ON t.engagement_id = li.engagement_id
LEFT JOIN cgan_ustax_published.fs_engagements_master e
    ON t.engagement_id = e.engagement_id
    AND e.tax_year = 2025;

-- @query: franchise_completion
-- Auth-level franchise completion using product_analytics_master (PAM) as SOT
-- FS completion from engagement-level filing_end_ts; franchise completion from PAM completed_flag
-- Includes completion date for daily curve and days-since-message calculation
SELECT
    t.engagement_id,
    t.recipe,
    CASE WHEN li.engagement_id IS NOT NULL THEN 1 ELSE 0 END AS low_intent_flag,
    e.auth_id,
    CASE WHEN e.filing_end_ts IS NOT NULL THEN 1 ELSE 0 END AS fs_completed,
    COALESCE(p.completed_flag, 0) AS franchise_completed,
    p.start_sku_rollup,
    p.completed_sku,
    p.first_completed_date AS franchise_completed_date,
    DATEDIFF(p.first_completed_date, DATE('2026-03-10')) AS fc_days_since_msg
FROM cgan_ustax_published.`3_9_detach_test_read` t
LEFT JOIN cgan_ustax_published.lh_low_intent_holdout_ids li
    ON t.engagement_id = li.engagement_id
LEFT JOIN cgan_ustax_published.fs_engagements_master e
    ON t.engagement_id = e.engagement_id
    AND e.tax_year = 2025
LEFT JOIN tax_rpt.product_analytics_master p
    ON CAST(e.auth_id AS BIGINT) = p.auth_id
    AND p.tax_year = 2025;

-- @query: diwm
-- DIWM/DIY re-engagement at auth level, split by pre/post 3/10 message
SELECT
    t.engagement_id,
    t.recipe,
    CASE WHEN li.engagement_id IS NOT NULL THEN 1 ELSE 0 END AS low_intent_flag,
    e.auth_id,
    CASE WHEN d.auth_id IS NOT NULL THEN 1 ELSE 0 END AS diwm_started,
    CASE WHEN d.first_start_date_adj >= DATE('2026-03-10') THEN 1 ELSE 0 END AS started_post_msg,
    CASE WHEN d.first_completed_date_adj IS NOT NULL THEN 1 ELSE 0 END AS diwm_completed,
    CASE WHEN d.first_completed_date_adj >= DATE('2026-03-10') THEN 1 ELSE 0 END AS completed_post_msg,
    CASE WHEN d.first_completed_date_adj >= DATE('2026-03-10') THEN d.total_revenue ELSE 0 END AS rev_post_msg
FROM cgan_ustax_published.`3_9_detach_test_read` t
LEFT JOIN cgan_ustax_published.lh_low_intent_holdout_ids li
    ON t.engagement_id = li.engagement_id
LEFT JOIN cgan_ustax_published.fs_engagements_master e
    ON t.engagement_id = e.engagement_id
    AND e.tax_year = 2025
LEFT JOIN cgan_ustax_published.diwm_funnel_summary_ty25 d
    ON CAST(e.auth_id AS BIGINT) = d.auth_id
    AND d.diwm_unit = 1;
