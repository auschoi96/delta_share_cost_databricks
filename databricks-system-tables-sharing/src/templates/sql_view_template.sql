-- =============================================================================
-- SQL VIEW TEMPLATE
-- =============================================================================
-- Use this template to create custom views on top of the monitoring tables.
-- 
-- Instructions:
-- 1. Copy this file and rename it (e.g., my_custom_views.sql)
-- 2. Replace ${catalog} and ${schema} with your catalog/schema names
-- 3. Run the SQL statements after the DLT pipeline has created the base tables
-- =============================================================================

-- =============================================================================
-- TEMPLATE: Basic Aggregation View
-- =============================================================================
-- Use for simple aggregations over a time period

CREATE OR REPLACE VIEW ${catalog}.${schema}.v_template_basic_agg AS
SELECT
    _account_identifier,
    _workspace_identifier,
    workspace_id,
    usage_date,
    -- Add your grouping columns
    SUM(total_cost_usd) AS total_cost_usd,
    SUM(total_dbus) AS total_dbus,
    COUNT(*) AS record_count
FROM ${catalog}.${schema}.cost_by_product  -- Replace with your source table
WHERE usage_date >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY ALL;


-- =============================================================================
-- TEMPLATE: Time Window Comparison View
-- =============================================================================
-- Use for comparing metrics across time periods (week-over-week, month-over-month)

CREATE OR REPLACE VIEW ${catalog}.${schema}.v_template_time_comparison AS
WITH current_period AS (
    SELECT
        workspace_id,
        billing_origin_product,
        SUM(total_cost_usd) AS current_cost
    FROM ${catalog}.${schema}.cost_by_product
    WHERE usage_date >= CURRENT_DATE - INTERVAL 7 DAYS
    GROUP BY ALL
),
previous_period AS (
    SELECT
        workspace_id,
        billing_origin_product,
        SUM(total_cost_usd) AS previous_cost
    FROM ${catalog}.${schema}.cost_by_product
    WHERE usage_date >= CURRENT_DATE - INTERVAL 14 DAYS
      AND usage_date < CURRENT_DATE - INTERVAL 7 DAYS
    GROUP BY ALL
)
SELECT
    COALESCE(c.workspace_id, p.workspace_id) AS workspace_id,
    COALESCE(c.billing_origin_product, p.billing_origin_product) AS billing_origin_product,
    COALESCE(c.current_cost, 0) AS current_period_cost,
    COALESCE(p.previous_cost, 0) AS previous_period_cost,
    COALESCE(c.current_cost, 0) - COALESCE(p.previous_cost, 0) AS cost_change,
    ROUND(
        (COALESCE(c.current_cost, 0) - COALESCE(p.previous_cost, 0)) 
        / NULLIF(p.previous_cost, 0) * 100, 
        2
    ) AS cost_change_pct
FROM current_period c
FULL OUTER JOIN previous_period p
    ON c.workspace_id = p.workspace_id 
    AND c.billing_origin_product = p.billing_origin_product;


-- =============================================================================
-- TEMPLATE: Top N View
-- =============================================================================
-- Use for finding the top consumers by a metric

CREATE OR REPLACE VIEW ${catalog}.${schema}.v_template_top_n AS
WITH ranked AS (
    SELECT
        _account_identifier,
        _workspace_identifier,
        workspace_id,
        job_id,
        job_name,
        run_as,
        SUM(total_cost_usd) AS total_cost_usd,
        ROW_NUMBER() OVER (
            PARTITION BY workspace_id 
            ORDER BY SUM(total_cost_usd) DESC
        ) AS cost_rank
    FROM ${catalog}.${schema}.cost_by_job
    WHERE usage_date >= CURRENT_DATE - INTERVAL 30 DAYS
    GROUP BY ALL
)
SELECT * FROM ranked WHERE cost_rank <= 10;


-- =============================================================================
-- TEMPLATE: Rolling Average View
-- =============================================================================
-- Use for smoothing metrics over a rolling window

CREATE OR REPLACE VIEW ${catalog}.${schema}.v_template_rolling_avg AS
SELECT
    *,
    AVG(total_cost_usd) OVER (
        PARTITION BY workspace_id 
        ORDER BY usage_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS cost_7d_rolling_avg,
    AVG(total_cost_usd) OVER (
        PARTITION BY workspace_id 
        ORDER BY usage_date 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS cost_30d_rolling_avg
FROM (
    SELECT
        _account_identifier,
        _workspace_identifier,
        workspace_id,
        usage_date,
        SUM(total_cost_usd) AS total_cost_usd
    FROM ${catalog}.${schema}.cost_by_product
    GROUP BY ALL
);


-- =============================================================================
-- TEMPLATE: Anomaly Detection View
-- =============================================================================
-- Use for identifying outliers based on standard deviation

CREATE OR REPLACE VIEW ${catalog}.${schema}.v_template_anomalies AS
WITH daily_metrics AS (
    SELECT
        workspace_id,
        usage_date,
        SUM(total_cost_usd) AS daily_cost
    FROM ${catalog}.${schema}.cost_by_product
    WHERE usage_date >= CURRENT_DATE - INTERVAL 90 DAYS
    GROUP BY ALL
),
with_stats AS (
    SELECT
        *,
        AVG(daily_cost) OVER (
            PARTITION BY workspace_id 
            ORDER BY usage_date 
            ROWS BETWEEN 29 PRECEDING AND 1 PRECEDING
        ) AS avg_30d,
        STDDEV(daily_cost) OVER (
            PARTITION BY workspace_id 
            ORDER BY usage_date 
            ROWS BETWEEN 29 PRECEDING AND 1 PRECEDING
        ) AS stddev_30d
    FROM daily_metrics
)
SELECT
    *,
    (daily_cost - avg_30d) / NULLIF(stddev_30d, 0) AS z_score,
    CASE
        WHEN (daily_cost - avg_30d) / NULLIF(stddev_30d, 0) > 2 THEN 'HIGH'
        WHEN (daily_cost - avg_30d) / NULLIF(stddev_30d, 0) < -2 THEN 'LOW'
        ELSE 'NORMAL'
    END AS anomaly_flag
FROM with_stats
WHERE avg_30d IS NOT NULL;


-- =============================================================================
-- TEMPLATE: Join Multiple Sources View
-- =============================================================================
-- Use for combining data from multiple monitoring tables

CREATE OR REPLACE VIEW ${catalog}.${schema}.v_template_combined AS
SELECT
    j.workspace_id,
    j.job_id,
    j.job_name,
    j.run_as,
    -- From job runs
    r.total_runs,
    r.successful_runs,
    r.failed_runs,
    r.avg_duration_seconds,
    -- From billing
    c.total_dbus,
    c.total_cost_usd,
    -- Calculated metrics
    ROUND(c.total_cost_usd / NULLIF(r.total_runs, 0), 2) AS cost_per_run,
    ROUND(r.successful_runs * 100.0 / NULLIF(r.total_runs, 0), 2) AS success_rate_pct
FROM ${catalog}.${schema}.jobs_current j
LEFT JOIN (
    SELECT
        workspace_id,
        job_id,
        SUM(total_runs) AS total_runs,
        SUM(successful_runs) AS successful_runs,
        SUM(failed_runs) AS failed_runs,
        AVG(avg_duration_seconds) AS avg_duration_seconds
    FROM ${catalog}.${schema}.job_runs_summary
    WHERE run_date >= CURRENT_DATE - INTERVAL 30 DAYS
    GROUP BY workspace_id, job_id
) r ON j.workspace_id = r.workspace_id AND j.job_id = r.job_id
LEFT JOIN (
    SELECT
        workspace_id,
        job_id,
        SUM(total_dbus) AS total_dbus,
        SUM(total_cost_usd) AS total_cost_usd
    FROM ${catalog}.${schema}.cost_by_job
    WHERE usage_date >= CURRENT_DATE - INTERVAL 30 DAYS
    GROUP BY workspace_id, job_id
) c ON j.workspace_id = c.workspace_id AND j.job_id = c.job_id;


-- =============================================================================
-- TEMPLATE: Pivot View
-- =============================================================================
-- Use for creating pivot table-style views

CREATE OR REPLACE VIEW ${catalog}.${schema}.v_template_pivot AS
SELECT
    workspace_id,
    usage_date,
    SUM(CASE WHEN billing_origin_product = 'JOBS' THEN total_cost_usd ELSE 0 END) AS jobs_cost,
    SUM(CASE WHEN billing_origin_product = 'SQL' THEN total_cost_usd ELSE 0 END) AS sql_cost,
    SUM(CASE WHEN billing_origin_product = 'DLT' THEN total_cost_usd ELSE 0 END) AS dlt_cost,
    SUM(CASE WHEN billing_origin_product = 'MODEL_SERVING' THEN total_cost_usd ELSE 0 END) AS serving_cost,
    SUM(CASE WHEN billing_origin_product = 'VECTOR_SEARCH' THEN total_cost_usd ELSE 0 END) AS vector_search_cost,
    SUM(CASE WHEN billing_origin_product = 'INTERACTIVE' THEN total_cost_usd ELSE 0 END) AS interactive_cost,
    SUM(total_cost_usd) AS total_cost
FROM ${catalog}.${schema}.cost_by_product
WHERE usage_date >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY workspace_id, usage_date
ORDER BY usage_date DESC;


-- =============================================================================
-- TEMPLATE: Tag-Based Cost Attribution
-- =============================================================================
-- Use for cost allocation based on custom tags

CREATE OR REPLACE VIEW ${catalog}.${schema}.v_template_tag_costs AS
SELECT
    _account_identifier,
    _workspace_identifier,
    workspace_id,
    usage_date,
    custom_tags['cost_center'] AS cost_center,
    custom_tags['team'] AS team,
    custom_tags['project'] AS project,
    custom_tags['environment'] AS environment,
    SUM(usage_quantity) AS total_dbus,
    SUM(estimated_cost_usd) AS total_cost_usd
FROM ${catalog}.${schema}.billing_summary
WHERE usage_date >= CURRENT_DATE - INTERVAL 30 DAYS
  AND custom_tags IS NOT NULL
GROUP BY ALL;
