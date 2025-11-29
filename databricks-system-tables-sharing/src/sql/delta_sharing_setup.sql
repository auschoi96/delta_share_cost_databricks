-- =============================================================================
-- DELTA SHARING SETUP
-- =============================================================================
-- Run these commands to set up Delta Sharing for cross-workspace/cross-account
-- data sharing. Execute on each PROVIDER workspace.
-- =============================================================================

-- =============================================================================
-- STEP 1: CREATE THE SHARE
-- =============================================================================
-- A share is a named collection of tables to share with recipients

CREATE SHARE IF NOT EXISTS workspace_monitoring_share
COMMENT 'System tables monitoring data for cross-workspace visibility';

-- =============================================================================
-- STEP 2: ADD TABLES TO THE SHARE
-- =============================================================================
-- Add all monitoring tables that should be shared
-- Note: Replace ${catalog}.${schema} with actual values (e.g., monitoring.workspace_usage)

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ BILLING & COST TABLES                                                   │
-- └─────────────────────────────────────────────────────────────────────────┘
ALTER SHARE workspace_monitoring_share ADD TABLE ${catalog}.${schema}.billing_usage_bronze;
ALTER SHARE workspace_monitoring_share ADD TABLE ${catalog}.${schema}.list_prices;
ALTER SHARE workspace_monitoring_share ADD TABLE ${catalog}.${schema}.billing_summary;
ALTER SHARE workspace_monitoring_share ADD TABLE ${catalog}.${schema}.cost_by_product;
ALTER SHARE workspace_monitoring_share ADD TABLE ${catalog}.${schema}.cost_by_sku;
ALTER SHARE workspace_monitoring_share ADD TABLE ${catalog}.${schema}.cost_by_job;
ALTER SHARE workspace_monitoring_share ADD TABLE ${catalog}.${schema}.cost_by_user;

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ JOBS & WORKFLOWS TABLES                                                 │
-- └─────────────────────────────────────────────────────────────────────────┘
ALTER SHARE workspace_monitoring_share ADD TABLE ${catalog}.${schema}.jobs_bronze;
ALTER SHARE workspace_monitoring_share ADD TABLE ${catalog}.${schema}.job_runs_bronze;
ALTER SHARE workspace_monitoring_share ADD TABLE ${catalog}.${schema}.jobs_current;
ALTER SHARE workspace_monitoring_share ADD TABLE ${catalog}.${schema}.job_runs_summary;

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ COMPUTE & CLUSTERS TABLES                                               │
-- └─────────────────────────────────────────────────────────────────────────┘
ALTER SHARE workspace_monitoring_share ADD TABLE ${catalog}.${schema}.clusters_bronze;
ALTER SHARE workspace_monitoring_share ADD TABLE ${catalog}.${schema}.clusters_current;
ALTER SHARE workspace_monitoring_share ADD TABLE ${catalog}.${schema}.cluster_utilization;

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ AUDIT & ACTIVITY TABLES                                                 │
-- └─────────────────────────────────────────────────────────────────────────┘
ALTER SHARE workspace_monitoring_share ADD TABLE ${catalog}.${schema}.audit_logs_bronze;
ALTER SHARE workspace_monitoring_share ADD TABLE ${catalog}.${schema}.user_activity_summary;
ALTER SHARE workspace_monitoring_share ADD TABLE ${catalog}.${schema}.data_access_summary;

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ MODEL SERVING TABLES                                                    │
-- └─────────────────────────────────────────────────────────────────────────┘
ALTER SHARE workspace_monitoring_share ADD TABLE ${catalog}.${schema}.served_entities_bronze;
ALTER SHARE workspace_monitoring_share ADD TABLE ${catalog}.${schema}.served_entities_current;
ALTER SHARE workspace_monitoring_share ADD TABLE ${catalog}.${schema}.endpoint_usage_bronze;
ALTER SHARE workspace_monitoring_share ADD TABLE ${catalog}.${schema}.endpoint_usage_summary;
ALTER SHARE workspace_monitoring_share ADD TABLE ${catalog}.${schema}.model_serving_costs;
ALTER SHARE workspace_monitoring_share ADD TABLE ${catalog}.${schema}.foundation_model_usage;
ALTER SHARE workspace_monitoring_share ADD TABLE ${catalog}.${schema}.external_model_usage;
ALTER SHARE workspace_monitoring_share ADD TABLE ${catalog}.${schema}.endpoint_inventory;

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ MLFLOW TABLES                                                           │
-- └─────────────────────────────────────────────────────────────────────────┘
ALTER SHARE workspace_monitoring_share ADD TABLE ${catalog}.${schema}.mlflow_experiments_bronze;
ALTER SHARE workspace_monitoring_share ADD TABLE ${catalog}.${schema}.mlflow_runs_bronze;
ALTER SHARE workspace_monitoring_share ADD TABLE ${catalog}.${schema}.mlflow_metrics_bronze;
ALTER SHARE workspace_monitoring_share ADD TABLE ${catalog}.${schema}.mlflow_experiments_summary;
ALTER SHARE workspace_monitoring_share ADD TABLE ${catalog}.${schema}.mlflow_runs_summary;

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ VECTOR SEARCH TABLES                                                    │
-- └─────────────────────────────────────────────────────────────────────────┘
ALTER SHARE workspace_monitoring_share ADD TABLE ${catalog}.${schema}.vector_search_costs;
ALTER SHARE workspace_monitoring_share ADD TABLE ${catalog}.${schema}.vector_search_summary;

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ EXECUTIVE SUMMARY & METADATA TABLES                                     │
-- └─────────────────────────────────────────────────────────────────────────┘
ALTER SHARE workspace_monitoring_share ADD TABLE ${catalog}.${schema}.workspace_info;
ALTER SHARE workspace_monitoring_share ADD TABLE ${catalog}.${schema}.executive_summary;
ALTER SHARE workspace_monitoring_share ADD TABLE ${catalog}.${schema}.aiml_executive_summary;

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ DAILY ROLLUP TABLES (created by daily_aggregation_job)                  │
-- │ Note: Run after the daily aggregation job has executed at least once    │
-- └─────────────────────────────────────────────────────────────────────────┘
ALTER SHARE workspace_monitoring_share ADD TABLE ${catalog}.${schema}.cost_weekly_rollup;
ALTER SHARE workspace_monitoring_share ADD TABLE ${catalog}.${schema}.cost_monthly_rollup;
ALTER SHARE workspace_monitoring_share ADD TABLE ${catalog}.${schema}.job_success_rate_weekly;
ALTER SHARE workspace_monitoring_share ADD TABLE ${catalog}.${schema}.cost_trend_analysis;

-- =============================================================================
-- STEP 3: VIEW SHARE CONTENTS
-- =============================================================================
-- Verify what's in the share (should show 35+ tables)

SHOW ALL IN SHARE workspace_monitoring_share;

-- =============================================================================
-- STEP 4: CREATE RECIPIENT
-- =============================================================================
-- For Databricks-to-Databricks sharing, you need the recipient's sharing identifier
-- Get this from the RECIPIENT workspace by running:
--   SELECT current_metastore()
-- Then get the sharing identifier from the metastore details

-- Create recipient for the central monitoring workspace
-- Replace <sharing-identifier> with the actual value
CREATE RECIPIENT IF NOT EXISTS central_monitoring_workspace
USING ID '<sharing-identifier-from-central-workspace>'
COMMENT 'Central workspace for consolidated monitoring';

-- =============================================================================
-- STEP 5: GRANT ACCESS TO RECIPIENT
-- =============================================================================
-- Grant SELECT access on the share to the recipient

GRANT SELECT ON SHARE workspace_monitoring_share
TO RECIPIENT central_monitoring_workspace;

-- =============================================================================
-- STEP 6: VERIFY ACCESS
-- =============================================================================
-- Check that the grant is in place

SHOW GRANTS ON SHARE workspace_monitoring_share;

-- =============================================================================
-- ADDITIONAL RECIPIENTS (if sharing with multiple workspaces)
-- =============================================================================
-- You can create additional recipients for other consuming workspaces

-- CREATE RECIPIENT IF NOT EXISTS analytics_workspace
-- USING ID '<sharing-identifier-from-analytics-workspace>'
-- COMMENT 'Analytics workspace';

-- GRANT SELECT ON SHARE workspace_monitoring_share
-- TO RECIPIENT analytics_workspace;


-- =============================================================================
-- =============================================================================
-- RECIPIENT WORKSPACE SETUP (Run on the CENTRAL monitoring workspace)
-- =============================================================================
-- =============================================================================

-- =============================================================================
-- STEP R1: VIEW AVAILABLE PROVIDERS AND SHARES
-- =============================================================================
-- See what providers have shared data with you

SHOW PROVIDERS;

SHOW SHARES IN PROVIDER <provider_name>;

-- =============================================================================
-- STEP R2: CREATE CATALOG FROM SHARE
-- =============================================================================
-- Create a catalog from each provider's share
-- This makes the shared tables queryable in your workspace

CREATE CATALOG IF NOT EXISTS shared_prod_account_a
USING SHARE <provider_name>.workspace_monitoring_share
COMMENT 'Monitoring data from Production Account A';

CREATE CATALOG IF NOT EXISTS shared_prod_account_b
USING SHARE <provider_name_b>.workspace_monitoring_share
COMMENT 'Monitoring data from Production Account B';

-- =============================================================================
-- STEP R3: GRANT ACCESS TO SHARED CATALOGS
-- =============================================================================
-- Grant access to users/groups who need to see the consolidated data

GRANT USE CATALOG ON CATALOG shared_prod_account_a TO `data-team`;
GRANT SELECT ON CATALOG shared_prod_account_a TO `data-team`;

-- =============================================================================
-- STEP R4: CREATE CONSOLIDATED VIEWS
-- =============================================================================
-- Create views that union data from all shared catalogs

CREATE SCHEMA IF NOT EXISTS consolidated.monitoring;

-- Consolidated billing view across all accounts
CREATE OR REPLACE VIEW consolidated.monitoring.all_billing_summary AS
SELECT 'prod_account_a' AS source, * FROM shared_prod_account_a.workspace_usage.billing_summary
UNION ALL
SELECT 'prod_account_b' AS source, * FROM shared_prod_account_b.workspace_usage.billing_summary;

-- Consolidated cost by product
CREATE OR REPLACE VIEW consolidated.monitoring.all_cost_by_product AS
SELECT 'prod_account_a' AS source, * FROM shared_prod_account_a.workspace_usage.cost_by_product
UNION ALL
SELECT 'prod_account_b' AS source, * FROM shared_prod_account_b.workspace_usage.cost_by_product;

-- Consolidated job runs
CREATE OR REPLACE VIEW consolidated.monitoring.all_job_runs AS
SELECT 'prod_account_a' AS source, * FROM shared_prod_account_a.workspace_usage.job_runs_summary
UNION ALL
SELECT 'prod_account_b' AS source, * FROM shared_prod_account_b.workspace_usage.job_runs_summary;

-- Consolidated model serving usage
CREATE OR REPLACE VIEW consolidated.monitoring.all_endpoint_usage AS
SELECT 'prod_account_a' AS source, * FROM shared_prod_account_a.workspace_usage.endpoint_usage_summary
UNION ALL
SELECT 'prod_account_b' AS source, * FROM shared_prod_account_b.workspace_usage.endpoint_usage_summary;

-- Consolidated MLflow experiments
CREATE OR REPLACE VIEW consolidated.monitoring.all_mlflow_experiments AS
SELECT 'prod_account_a' AS source, * FROM shared_prod_account_a.workspace_usage.mlflow_experiments_summary
UNION ALL
SELECT 'prod_account_b' AS source, * FROM shared_prod_account_b.workspace_usage.mlflow_experiments_summary;

-- Consolidated executive summary
CREATE OR REPLACE VIEW consolidated.monitoring.all_executive_summary AS
SELECT 'prod_account_a' AS source, * FROM shared_prod_account_a.workspace_usage.executive_summary
UNION ALL
SELECT 'prod_account_b' AS source, * FROM shared_prod_account_b.workspace_usage.executive_summary;

-- Consolidated AI/ML executive summary
CREATE OR REPLACE VIEW consolidated.monitoring.all_aiml_summary AS
SELECT 'prod_account_a' AS source, * FROM shared_prod_account_a.workspace_usage.aiml_executive_summary
UNION ALL
SELECT 'prod_account_b' AS source, * FROM shared_prod_account_b.workspace_usage.aiml_executive_summary;

-- =============================================================================
-- STEP R5: CREATE AGGREGATED ANALYTICS
-- =============================================================================
-- Create additional analytics on top of consolidated data

-- Total spend by account and month
CREATE OR REPLACE VIEW consolidated.monitoring.monthly_spend_by_account AS
SELECT
    source,
    _account_identifier,
    DATE_TRUNC('month', usage_date) AS month,
    SUM(total_cost_usd) AS total_cost_usd,
    SUM(total_dbus) AS total_dbus
FROM consolidated.monitoring.all_cost_by_product
GROUP BY ALL
ORDER BY month DESC, total_cost_usd DESC;

-- Account comparison dashboard view
CREATE OR REPLACE VIEW consolidated.monitoring.account_comparison AS
SELECT
    source,
    _account_identifier,
    _workspace_identifier,
    billing_origin_product,
    SUM(CASE WHEN usage_date >= CURRENT_DATE - INTERVAL 7 DAYS THEN total_cost_usd ELSE 0 END) AS last_7d_cost,
    SUM(CASE WHEN usage_date >= CURRENT_DATE - INTERVAL 30 DAYS THEN total_cost_usd ELSE 0 END) AS last_30d_cost,
    SUM(total_cost_usd) AS total_cost_usd
FROM consolidated.monitoring.all_cost_by_product
WHERE usage_date >= CURRENT_DATE - INTERVAL 90 DAYS
GROUP BY ALL;

-- Cross-account AI/ML spend comparison
CREATE OR REPLACE VIEW consolidated.monitoring.aiml_spend_comparison AS
SELECT
    source,
    _account_identifier,
    _workspace_identifier,
    summary_date,
    model_serving_cost,
    vector_search_cost,
    total_aiml_cost,
    total_requests
FROM consolidated.monitoring.all_aiml_summary
WHERE summary_date >= CURRENT_DATE - INTERVAL 30 DAYS
ORDER BY summary_date DESC, total_aiml_cost DESC;
