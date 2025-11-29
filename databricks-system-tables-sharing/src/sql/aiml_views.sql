-- =============================================================================
-- AI/ML WORKLOADS - SQL VIEWS
-- =============================================================================
-- Additional views for monitoring AI/ML spend and usage.
-- Run these after the DLT pipeline has populated the base tables.
-- =============================================================================


-- =============================================================================
-- 1. MODEL SERVING VIEWS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1.1 Model Serving Cost Summary (Last 30 Days)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW ${catalog}.${schema}.v_model_serving_cost_summary AS
SELECT
    _account_identifier,
    _workspace_identifier,
    workspace_id,
    endpoint_name,
    entity_type,
    entity_name,
    SUM(total_dbus) AS total_dbus,
    SUM(total_cost_usd) AS total_cost_usd,
    COUNT(DISTINCT usage_date) AS active_days,
    MIN(usage_date) AS first_usage,
    MAX(usage_date) AS last_usage
FROM ${catalog}.${schema}.model_serving_costs
WHERE usage_date >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY ALL
ORDER BY total_cost_usd DESC;


-- -----------------------------------------------------------------------------
-- 1.2 Foundation Model Leaderboard (Top Models by Usage)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW ${catalog}.${schema}.v_foundation_model_leaderboard AS
SELECT
    _account_identifier,
    _workspace_identifier,
    entity_name,
    SUM(total_input_tokens) AS total_input_tokens,
    SUM(total_output_tokens) AS total_output_tokens,
    SUM(total_tokens) AS total_tokens,
    SUM(request_count) AS total_requests,
    COUNT(DISTINCT workspace_id) AS workspaces_using,
    COUNT(DISTINCT endpoint_name) AS endpoints_using
FROM ${catalog}.${schema}.foundation_model_usage
WHERE usage_date >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY ALL
ORDER BY total_tokens DESC;


-- -----------------------------------------------------------------------------
-- 1.3 External Model Usage Summary (OpenAI, Anthropic, etc.)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW ${catalog}.${schema}.v_external_model_summary AS
SELECT
    _account_identifier,
    _workspace_identifier,
    workspace_id,
    entity_name,
    endpoint_name,
    SUM(total_input_tokens) AS total_input_tokens,
    SUM(total_output_tokens) AS total_output_tokens,
    SUM(total_tokens) AS total_tokens,
    SUM(request_count) AS total_requests,
    ROUND(SUM(total_output_tokens) / NULLIF(SUM(total_input_tokens), 0), 2) AS output_input_ratio
FROM ${catalog}.${schema}.external_model_usage
WHERE usage_date >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY ALL
ORDER BY total_tokens DESC;


-- -----------------------------------------------------------------------------
-- 1.4 Endpoint Utilization Daily Pattern
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW ${catalog}.${schema}.v_endpoint_daily_pattern AS
SELECT
    workspace_id,
    endpoint_name,
    usage_date,
    DAYOFWEEK(usage_date) AS day_of_week,
    SUM(total_tokens) AS total_tokens,
    SUM(request_count) AS total_requests,
    SUM(total_input_tokens) AS input_tokens,
    SUM(total_output_tokens) AS output_tokens
FROM ${catalog}.${schema}.endpoint_usage_summary
WHERE usage_date >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY ALL
ORDER BY usage_date DESC;


-- -----------------------------------------------------------------------------
-- 1.5 Inactive Endpoints (No Usage in 7 Days)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW ${catalog}.${schema}.v_inactive_endpoints AS
SELECT
    inv.*,
    DATEDIFF(CURRENT_DATE, inv.last_billing_date) AS days_since_last_billing
FROM ${catalog}.${schema}.endpoint_inventory inv
WHERE inv.last_billing_date < CURRENT_DATE - INTERVAL 7 DAYS
   OR inv.last_billing_date IS NULL
ORDER BY days_since_last_billing DESC;


-- =============================================================================
-- 2. MLFLOW VIEWS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 2.1 Experiment Activity Dashboard
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW ${catalog}.${schema}.v_mlflow_experiment_activity AS
SELECT
    _account_identifier,
    _workspace_identifier,
    workspace_id,
    experiment_id,
    name AS experiment_name,
    lifecycle_stage,
    total_runs,
    successful_runs,
    failed_runs,
    ROUND(successful_runs * 100.0 / NULLIF(total_runs, 0), 2) AS success_rate_pct,
    distinct_users,
    first_run,
    last_run,
    DATEDIFF(CURRENT_DATE, last_run) AS days_since_last_run
FROM ${catalog}.${schema}.mlflow_experiments_summary
WHERE total_runs > 0
ORDER BY last_run DESC;


-- -----------------------------------------------------------------------------
-- 2.2 MLflow Daily Activity
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW ${catalog}.${schema}.v_mlflow_daily_activity AS
SELECT
    _account_identifier,
    _workspace_identifier,
    workspace_id,
    run_date,
    SUM(run_count) AS total_runs,
    SUM(CASE WHEN status = 'FINISHED' THEN run_count ELSE 0 END) AS successful_runs,
    SUM(CASE WHEN status = 'FAILED' THEN run_count ELSE 0 END) AS failed_runs,
    SUM(distinct_users) AS distinct_users,
    ROUND(AVG(avg_duration_seconds), 2) AS avg_duration_seconds,
    ROUND(SUM(total_compute_seconds) / 3600, 2) AS total_compute_hours
FROM ${catalog}.${schema}.mlflow_runs_summary
WHERE run_date >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY ALL
ORDER BY run_date DESC;


-- -----------------------------------------------------------------------------
-- 2.3 Top Experimenters (Most Active Experiments)
-- -----------------------------------------------------------------------------
-- Note: Since experiments_latest doesn't include owner information,
-- we track experiments with the most activity instead of individual users
CREATE OR REPLACE VIEW ${catalog}.${schema}.v_top_experimenters AS
SELECT
    _account_identifier,
    _workspace_identifier,
    workspace_id,
    experiment_id,
    name AS experiment_name,
    total_runs,
    successful_runs,
    distinct_users,
    ROUND(successful_runs * 100.0 / NULLIF(total_runs, 0), 2) AS success_rate_pct,
    last_run
FROM ${catalog}.${schema}.mlflow_experiments_summary
WHERE last_run >= CURRENT_DATE - INTERVAL 30 DAYS
  AND total_runs > 0
ORDER BY total_runs DESC
LIMIT 50;


-- =============================================================================
-- 3. VECTOR SEARCH VIEWS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 3.1 Vector Search Cost by Endpoint
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW ${catalog}.${schema}.v_vector_search_by_endpoint AS
SELECT
    _account_identifier,
    _workspace_identifier,
    workspace_id,
    endpoint_name,
    SUM(total_dbus) AS total_dbus,
    SUM(total_cost_usd) AS total_cost_usd,
    COUNT(DISTINCT usage_date) AS active_days,
    MIN(usage_date) AS first_usage,
    MAX(usage_date) AS last_usage
FROM ${catalog}.${schema}.vector_search_costs
WHERE usage_date >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY ALL
ORDER BY total_cost_usd DESC;


-- -----------------------------------------------------------------------------
-- 3.2 Vector Search Daily Trend
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW ${catalog}.${schema}.v_vector_search_daily_trend AS
SELECT
    _account_identifier,
    _workspace_identifier,
    workspace_id,
    usage_date,
    total_dbus,
    total_cost_usd,
    distinct_endpoints,
    SUM(total_cost_usd) OVER (
        PARTITION BY workspace_id
        ORDER BY usage_date
    ) AS cumulative_cost_usd
FROM ${catalog}.${schema}.vector_search_summary
WHERE usage_date >= CURRENT_DATE - INTERVAL 30 DAYS
ORDER BY usage_date DESC;


-- =============================================================================
-- 4. COMBINED AI/ML VIEWS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 4.1 AI/ML Spend by Category (Last 30 Days)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW ${catalog}.${schema}.v_aiml_spend_by_category AS
SELECT
    _account_identifier,
    _workspace_identifier,
    workspace_id,
    aiml_category,
    billing_origin_product,
    SUM(total_dbus) AS total_dbus,
    SUM(total_cost_usd) AS total_cost_usd,
    SUM(distinct_endpoints) AS total_endpoints,
    SUM(distinct_users) AS total_users
FROM ${catalog}.${schema}.aiml_executive_summary
WHERE usage_date >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY ALL
ORDER BY total_cost_usd DESC;


-- -----------------------------------------------------------------------------
-- 4.2 AI/ML Daily Trend
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW ${catalog}.${schema}.v_aiml_daily_trend AS
SELECT
    _account_identifier,
    _workspace_identifier,
    workspace_id,
    usage_date,
    SUM(total_cost_usd) AS total_cost_usd,
    SUM(CASE WHEN aiml_category = 'Inference' THEN total_cost_usd ELSE 0 END) AS inference_cost,
    SUM(CASE WHEN aiml_category = 'Vector Search' THEN total_cost_usd ELSE 0 END) AS vector_search_cost,
    SUM(CASE WHEN aiml_category = 'Feature Serving' THEN total_cost_usd ELSE 0 END) AS feature_serving_cost,
    SUM(CASE WHEN aiml_category = 'Fine-tuning' THEN total_cost_usd ELSE 0 END) AS finetuning_cost
FROM ${catalog}.${schema}.aiml_executive_summary
WHERE usage_date >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY ALL
ORDER BY usage_date DESC;


-- -----------------------------------------------------------------------------
-- 4.3 AI/ML Week-over-Week Comparison
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW ${catalog}.${schema}.v_aiml_wow_comparison AS
WITH current_week AS (
    SELECT
        _account_identifier,
        _workspace_identifier,
        workspace_id,
        aiml_category,
        SUM(total_cost_usd) AS current_cost
    FROM ${catalog}.${schema}.aiml_executive_summary
    WHERE usage_date >= CURRENT_DATE - INTERVAL 7 DAYS
    GROUP BY ALL
),
previous_week AS (
    SELECT
        _account_identifier,
        _workspace_identifier,
        workspace_id,
        aiml_category,
        SUM(total_cost_usd) AS previous_cost
    FROM ${catalog}.${schema}.aiml_executive_summary
    WHERE usage_date >= CURRENT_DATE - INTERVAL 14 DAYS
      AND usage_date < CURRENT_DATE - INTERVAL 7 DAYS
    GROUP BY ALL
)
SELECT
    COALESCE(c._account_identifier, p._account_identifier) AS _account_identifier,
    COALESCE(c._workspace_identifier, p._workspace_identifier) AS _workspace_identifier,
    COALESCE(c.workspace_id, p.workspace_id) AS workspace_id,
    COALESCE(c.aiml_category, p.aiml_category) AS aiml_category,
    COALESCE(c.current_cost, 0) AS this_week_cost,
    COALESCE(p.previous_cost, 0) AS last_week_cost,
    COALESCE(c.current_cost, 0) - COALESCE(p.previous_cost, 0) AS cost_change,
    ROUND(
        (COALESCE(c.current_cost, 0) - COALESCE(p.previous_cost, 0))
        / NULLIF(p.previous_cost, 0) * 100,
        2
    ) AS change_pct
FROM current_week c
FULL OUTER JOIN previous_week p
    ON c.workspace_id = p.workspace_id
    AND c.aiml_category = p.aiml_category;


-- -----------------------------------------------------------------------------
-- 4.4 Complete AI/ML Inventory
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW ${catalog}.${schema}.v_aiml_complete_inventory AS
-- Model Serving Endpoints
SELECT
    _account_identifier,
    _workspace_identifier,
    workspace_id,
    'Model Serving' AS resource_type,
    endpoint_name AS resource_name,
    entity_type AS resource_subtype,
    entity_name AS model_name,
    total_cost_usd_30d AS cost_30d,
    change_time AS last_updated
FROM ${catalog}.${schema}.endpoint_inventory

UNION ALL

-- MLflow Experiments
SELECT
    _account_identifier,
    _workspace_identifier,
    workspace_id,
    'MLflow Experiment' AS resource_type,
    name AS resource_name,
    lifecycle_stage AS resource_subtype,
    NULL AS model_name,
    NULL AS cost_30d,
    last_update_time AS last_updated
FROM ${catalog}.${schema}.mlflow_experiments_summary
WHERE total_runs > 0;
