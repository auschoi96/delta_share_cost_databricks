-- =============================================================================
-- BILLING VIEWS AND QUERIES
-- =============================================================================
-- These views provide useful aggregations for cost analysis and reporting.
-- Run these after the DLT pipeline has populated the base tables.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Real-time Spend Overview (Current Month)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW ${catalog}.${schema}.v_current_month_spend AS
SELECT
    _account_identifier,
    _workspace_identifier,
    workspace_id,
    billing_origin_product,
    is_serverless,
    SUM(usage_quantity) AS total_dbus,
    SUM(estimated_cost_usd) AS total_cost_usd,
    COUNT(*) AS record_count
FROM ${catalog}.${schema}.billing_summary
WHERE 
    MONTH(usage_date) = MONTH(CURRENT_DATE())
    AND YEAR(usage_date) = YEAR(CURRENT_DATE())
GROUP BY ALL;

-- -----------------------------------------------------------------------------
-- 2. Daily Spend Trend (Last 30 Days)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW ${catalog}.${schema}.v_daily_spend_trend AS
SELECT
    _account_identifier,
    _workspace_identifier,
    workspace_id,
    usage_date,
    billing_origin_product,
    SUM(usage_quantity) AS total_dbus,
    SUM(estimated_cost_usd) AS total_cost_usd,
    SUM(SUM(estimated_cost_usd)) OVER (
        PARTITION BY workspace_id 
        ORDER BY usage_date
    ) AS cumulative_cost_usd
FROM ${catalog}.${schema}.billing_summary
WHERE usage_date >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY ALL
ORDER BY usage_date;

-- -----------------------------------------------------------------------------
-- 3. Serverless vs Classic Compute Comparison
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW ${catalog}.${schema}.v_serverless_comparison AS
SELECT
    _account_identifier,
    _workspace_identifier,
    workspace_id,
    DATE_TRUNC('week', usage_date) AS week_start,
    is_serverless,
    billing_origin_product,
    SUM(usage_quantity) AS total_dbus,
    SUM(estimated_cost_usd) AS total_cost_usd,
    COUNT(DISTINCT job_id) AS distinct_jobs
FROM ${catalog}.${schema}.billing_summary
WHERE usage_date >= CURRENT_DATE - INTERVAL 90 DAYS
GROUP BY ALL;

-- -----------------------------------------------------------------------------
-- 4. Top 20 Most Expensive Jobs (Last 30 Days)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW ${catalog}.${schema}.v_top_expensive_jobs AS
WITH job_costs AS (
    SELECT
        _account_identifier,
        _workspace_identifier,
        workspace_id,
        job_id,
        job_name,
        run_as,
        SUM(usage_quantity) AS total_dbus,
        SUM(estimated_cost_usd) AS total_cost_usd,
        COUNT(*) AS billing_records,
        MIN(usage_date) AS first_run,
        MAX(usage_date) AS last_run
    FROM ${catalog}.${schema}.billing_summary
    WHERE 
        job_id IS NOT NULL
        AND usage_date >= CURRENT_DATE - INTERVAL 30 DAYS
    GROUP BY ALL
)
SELECT
    *,
    ROW_NUMBER() OVER (
        PARTITION BY workspace_id 
        ORDER BY total_cost_usd DESC
    ) AS cost_rank
FROM job_costs
QUALIFY cost_rank <= 20;

-- -----------------------------------------------------------------------------
-- 5. Cost by Custom Tags
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW ${catalog}.${schema}.v_cost_by_tags AS
SELECT
    _account_identifier,
    _workspace_identifier,
    workspace_id,
    usage_date,
    EXPLODE(custom_tags) AS (tag_key, tag_value),
    SUM(usage_quantity) AS total_dbus,
    SUM(estimated_cost_usd) AS total_cost_usd
FROM ${catalog}.${schema}.billing_summary
WHERE 
    custom_tags IS NOT NULL 
    AND SIZE(custom_tags) > 0
    AND usage_date >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY ALL;

-- -----------------------------------------------------------------------------
-- 6. Warehouse Cost Analysis
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW ${catalog}.${schema}.v_warehouse_costs AS
SELECT
    _account_identifier,
    _workspace_identifier,
    workspace_id,
    warehouse_id,
    usage_date,
    sku_name,
    SUM(usage_quantity) AS total_dbus,
    SUM(estimated_cost_usd) AS total_cost_usd,
    COUNT(*) AS query_count
FROM ${catalog}.${schema}.billing_summary
WHERE 
    warehouse_id IS NOT NULL
    AND billing_origin_product = 'SQL'
GROUP BY ALL;

-- -----------------------------------------------------------------------------
-- 7. Model Serving Costs
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW ${catalog}.${schema}.v_model_serving_costs AS
SELECT
    _account_identifier,
    _workspace_identifier,
    workspace_id,
    endpoint_name,
    usage_date,
    sku_name,
    SUM(usage_quantity) AS total_dbus,
    SUM(estimated_cost_usd) AS total_cost_usd
FROM ${catalog}.${schema}.billing_summary
WHERE 
    billing_origin_product = 'MODEL_SERVING'
    AND usage_date >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY ALL;

-- -----------------------------------------------------------------------------
-- 8. DLT Pipeline Costs
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW ${catalog}.${schema}.v_dlt_pipeline_costs AS
SELECT
    _account_identifier,
    _workspace_identifier,
    workspace_id,
    dlt_pipeline_id,
    usage_date,
    is_serverless,
    SUM(usage_quantity) AS total_dbus,
    SUM(estimated_cost_usd) AS total_cost_usd
FROM ${catalog}.${schema}.billing_summary
WHERE 
    billing_origin_product = 'DLT'
    AND usage_date >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY ALL;

-- -----------------------------------------------------------------------------
-- 9. Hourly Spend Pattern Analysis
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW ${catalog}.${schema}.v_hourly_spend_pattern AS
SELECT
    _account_identifier,
    _workspace_identifier,
    workspace_id,
    HOUR(usage_start_time) AS hour_of_day,
    DAYOFWEEK(usage_date) AS day_of_week,
    billing_origin_product,
    AVG(estimated_cost_usd) AS avg_hourly_cost,
    SUM(estimated_cost_usd) AS total_cost_usd
FROM ${catalog}.${schema}.billing_summary
WHERE usage_date >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY ALL;

-- -----------------------------------------------------------------------------
-- 10. Notebook Costs (Serverless/Interactive)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW ${catalog}.${schema}.v_notebook_costs AS
SELECT
    _account_identifier,
    _workspace_identifier,
    workspace_id,
    notebook_id,
    notebook_path,
    run_as,
    usage_date,
    SUM(usage_quantity) AS total_dbus,
    SUM(estimated_cost_usd) AS total_cost_usd
FROM ${catalog}.${schema}.billing_summary
WHERE 
    notebook_id IS NOT NULL
    AND billing_origin_product = 'INTERACTIVE'
    AND usage_date >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY ALL;
