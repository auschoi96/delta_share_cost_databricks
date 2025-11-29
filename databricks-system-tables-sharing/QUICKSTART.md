# Quick Start Deployment Guide

## Overview

This guide walks you through deploying the System Tables Monitoring solution to your Databricks workspaces.

## Prerequisites

1. **Databricks CLI** installed (v0.218.0+)
   ```bash
   # Install or upgrade
   pip install databricks-cli --upgrade
   ```

2. **Account Admin** and **Metastore Admin** privileges

3. **Unity Catalog** enabled on all workspaces

4. **Service Principal** (recommended for production)

---

## Step-by-Step Deployment

### Step 1: Configure Authentication

```bash
# Set up authentication profile
databricks configure --profile my_workspace

# Enter:
# - Workspace URL: https://your-workspace.cloud.databricks.com
# - Authentication method: OAuth (recommended) or PAT
```

### Step 2: Clone and Configure the Bundle

```bash
# Clone the project
git clone <repository-url>
cd databricks-system-tables-sharing

# Create environment-specific configuration
# Edit databricks.yml to set your workspace host
```

### Step 3: Enable System Tables (First Time Only)

Run the enable_system_tables notebook in each workspace:

```bash
# Upload and run the notebook
databricks workspace import \
  src/enable_system_tables.py \
  /Users/$(databricks current-user me | jq -r .userName)/enable_system_tables.py \
  --overwrite
```

Or run it manually via the Databricks UI.

### Step 4: Configure Variables

The bundle uses variables to customize deployment per workspace. Understanding these variables is crucial for proper multi-workspace deployment.

#### Variable Reference

| Variable | Purpose | Example | Impact |
|----------|---------|---------|--------|
| `monitoring_catalog` | Unity Catalog where tables are created | `monitoring`, `shared_data` | All tables created in this catalog |
| `monitoring_schema` | Schema within the catalog | `workspace_usage`, `ws_prod_east` | All tables created in this schema |
| `account_identifier` | Human-readable account name | `prod_us`, `acme_corp` | Added to every row as `_account_identifier` column |
| `workspace_identifier` | Human-readable workspace name | `analytics_prod`, `data_science` | Added to every row as `_workspace_identifier` column |
| `notification_email` | Email for alerts | `alerts@company.com` | Pipeline/job failure notifications |
| `refresh_schedule` | Cron expression for pipeline refresh | `0 */15 * * * ?` | How often data is updated |

#### Configuration File: `databricks.yml`

```yaml
variables:
  # ═══════════════════════════════════════════════════════════════════════════
  # CATALOG AND SCHEMA CONFIGURATION
  # ═══════════════════════════════════════════════════════════════════════════
  # These determine WHERE your monitoring tables are created
  
  monitoring_catalog:
    description: "Unity Catalog for monitoring tables"
    default: "monitoring"
    # Options:
    # - Use a dedicated catalog like "monitoring" or "observability"
    # - Use an existing shared catalog like "shared_data" 
    # - For multi-workspace: consider "monitoring_<account>" pattern
  
  monitoring_schema:
    description: "Schema within the catalog"
    default: "workspace_usage"
    # Options:
    # - Use workspace-specific: "ws_analytics_prod", "ws_data_science"
    # - Use account-specific: "account_prod_us"
    # - Use generic: "workspace_usage" (if single workspace per catalog)
  
  # ═══════════════════════════════════════════════════════════════════════════
  # IDENTIFIER COLUMNS (Critical for Multi-Workspace/Multi-Account)
  # ═══════════════════════════════════════════════════════════════════════════
  # These values are embedded in EVERY ROW of data for cross-workspace analysis
  
  account_identifier:
    description: "Human-readable account identifier"
    default: "default_account"
    # Purpose: Distinguish data when combining multiple accounts
    # Examples: "prod_us_east", "acme_corp", "customer_a"
    # Best practice: Use consistent naming across all deployments
    # This appears in: _account_identifier column in all tables
  
  workspace_identifier:
    description: "Human-readable workspace identifier"
    default: "default_workspace"
    # Purpose: Distinguish data when combining multiple workspaces
    # Examples: "analytics_prod", "ml_research", "data_engineering"
    # Best practice: Match your internal workspace naming convention
    # This appears in: _workspace_identifier column in all tables
  
  # ═══════════════════════════════════════════════════════════════════════════
  # OPERATIONAL SETTINGS
  # ═══════════════════════════════════════════════════════════════════════════
  
  notification_email:
    description: "Email for pipeline and job failure alerts"
    default: "admin@example.com"
    # Receives alerts when:
    # - DLT pipeline update fails
    # - DLT flow (individual table) fails  
    # - Scheduled jobs fail
  
  refresh_schedule:
    description: "Cron schedule for pipeline refresh job"
    default: "0 */15 * * * ?"  # Every 15 minutes
    # Common patterns:
    # - "0 */5 * * * ?"   - Every 5 minutes (near real-time, higher cost)
    # - "0 */15 * * * ?"  - Every 15 minutes (balanced)
    # - "0 0 * * * ?"     - Every hour (cost efficient)
    # - "0 0 */4 * * ?"   - Every 4 hours (minimal cost)
```

#### How Variables Affect Materialized Views

**1. Catalog/Schema Placement**

When you set:
```yaml
monitoring_catalog: "observability"
monitoring_schema: "analytics_ws"
```

All tables are created at: `observability.analytics_ws.<table_name>`

Example paths:
- `observability.analytics_ws.billing_summary`
- `observability.analytics_ws.cost_by_product`
- `observability.analytics_ws.endpoint_inventory`

**2. Row-Level Identifiers**

When you set:
```yaml
account_identifier: "acme_prod"
workspace_identifier: "analytics_east"
```

Every row in every table contains:
```sql
SELECT 
    _account_identifier,    -- "acme_prod"
    _workspace_identifier,  -- "analytics_east"
    *
FROM observability.analytics_ws.billing_summary;
```

This enables cross-workspace queries:
```sql
-- After combining via Delta Sharing
SELECT 
    _account_identifier,
    _workspace_identifier,
    SUM(total_cost_usd) as cost
FROM shared_monitoring.all_billing
GROUP BY 1, 2;
```

**3. Job/Pipeline Tagging**

Jobs and pipelines are automatically tagged with:
```yaml
tags:
  project: "system-tables-monitoring"
  workspace: "${var.workspace_identifier}"   # Your value
  account: "${var.account_identifier}"       # Your value
  environment: "${bundle.target}"            # dev/staging/prod
```

This enables filtering in the Databricks UI and API.

#### Per-Workspace Deployment Example

**Scenario**: Deploy to 3 workspaces across 2 accounts

**Workspace 1: Production Analytics (Account: ACME-Prod)**
```yaml
variables:
  monitoring_catalog: "monitoring"
  monitoring_schema: "acme_prod_analytics"
  account_identifier: "acme_prod"
  workspace_identifier: "analytics"
  notification_email: "platform-team@acme.com"
  refresh_schedule: "0 */15 * * * ?"
```

**Workspace 2: Production Data Science (Account: ACME-Prod)**
```yaml
variables:
  monitoring_catalog: "monitoring"
  monitoring_schema: "acme_prod_datascience"
  account_identifier: "acme_prod"
  workspace_identifier: "data_science"
  notification_email: "platform-team@acme.com"
  refresh_schedule: "0 */15 * * * ?"
```

**Workspace 3: Development (Account: ACME-Dev)**
```yaml
variables:
  monitoring_catalog: "monitoring"
  monitoring_schema: "acme_dev_sandbox"
  account_identifier: "acme_dev"
  workspace_identifier: "sandbox"
  notification_email: "dev-alerts@acme.com"
  refresh_schedule: "0 0 * * * ?"  # Hourly (lower cost for dev)
```

#### Environment Overrides (dev/staging/prod)

Use targets in `databricks.yml` to override variables per environment:

```yaml
targets:
  dev:
    mode: development
    default: true
    workspace:
      host: https://dev-workspace.cloud.databricks.com
    variables:
      monitoring_schema: "dev_monitoring"
      refresh_schedule: "0 0 * * * ?"  # Hourly in dev
  
  prod:
    mode: production
    workspace:
      host: https://prod-workspace.cloud.databricks.com
    variables:
      monitoring_schema: "prod_monitoring"
      refresh_schedule: "0 */15 * * * ?"  # Every 15 min in prod
```

Deploy with:
```bash
databricks bundle deploy -t dev   # Uses dev settings
databricks bundle deploy -t prod  # Uses prod settings
```

### Step 5: Validate the Bundle

```bash
# Validate configuration
databricks bundle validate -t prod

# Expected output: bundle configuration summary
```

### Step 6: Deploy to Workspace

```bash
# Deploy to development
databricks bundle deploy -t dev

# Deploy to production
databricks bundle deploy -t prod
```

### Step 7: Run the Pipeline

```bash
# Initial run
databricks bundle run monitoring_pipeline_job -t prod

# This will:
# 1. Create the catalog and schema (if they don't exist)
# 2. Create streaming tables from system tables
# 3. Create aggregated views
```

### What Gets Created

After deployment, you'll have the following resources in your workspace:

**Unity Catalog Objects** (in `${monitoring_catalog}.${monitoring_schema}`):

| Category | Tables | Description |
|----------|--------|-------------|
| Bronze (Raw) | 10 tables | Direct streams from system tables |
| Silver (Aggregated) | 20+ tables | Transformed and aggregated views |
| Daily Rollups | 4 tables | Weekly/monthly summaries (after daily job runs) |

**Jobs & Pipeline**:

| Resource | Name Pattern | Purpose |
|----------|--------------|---------|
| DLT Pipeline | `[env] System Tables Monitoring - {workspace}` | Processes system table data |
| Pipeline Job | `[env] System Tables Monitoring Refresh - {workspace}` | Triggers pipeline on schedule |
| Aggregation Job | `[env] System Tables Daily Aggregation - {workspace}` | Creates historical rollups |

**Example**: With `workspace_identifier: "analytics"` and target `dev`:
- Pipeline: `[dev] System Tables Monitoring - analytics`
- Jobs visible in the Jobs UI with tags for filtering

**Verifying the Deployment**:

```bash
# Check pipeline status
databricks pipelines list | grep "System Tables"

# Check job status
databricks jobs list | grep "System Tables"

# View tables created
databricks unity-catalog tables list \
  --catalog-name monitoring \
  --schema-name workspace_usage
```

### Step 8: Set Up Delta Sharing

You have **two options** for setting up Delta Sharing:

#### Option A: Using Databricks Asset Bundles (Recommended)

The bundle includes a setup job that programmatically adds all tables to the share.

**Step 1: Configure sharing variables in databricks.yml:**

```yaml
variables:
  # Get this from the RECIPIENT workspace by running:
  # SELECT current_metastore();
  # Then look up the sharing identifier in metastore details
  recipient_sharing_id: "<sharing-identifier-from-central-workspace>"
  recipient_name: "central_monitoring_workspace"
```

**Step 2: Run the setup job (after pipeline has created tables):**

```bash
# First, ensure the pipeline has run at least once
databricks bundle run monitoring_pipeline_job -t prod

# Then run the Delta Sharing setup job
databricks bundle run setup_delta_sharing_job -t prod
```

This job will:
1. Create the share (if it doesn't exist)
2. Add all 39 monitoring tables to the share
3. Create the recipient and grant access (if `recipient_sharing_id` is configured)

**Step 3: Verify the share:**

```sql
-- Check share contents
SHOW ALL IN SHARE <workspace_identifier>_monitoring_share;

-- Check grants
SHOW GRANTS ON SHARE <workspace_identifier>_monitoring_share;
```

#### Option B: Using SQL Commands

If you prefer to run SQL directly, or need more control:

**On the Provider Workspace (source):**
```sql
-- ═══════════════════════════════════════════════════════════════════════════
-- DELTA SHARING SETUP - PROVIDER WORKSPACE
-- Run these commands in the workspace that contains the monitoring tables
-- ═══════════════════════════════════════════════════════════════════════════

-- Step 1: Create the share
CREATE SHARE IF NOT EXISTS workspace_monitoring_share
COMMENT 'System tables monitoring data for cross-workspace sharing';

-- ═══════════════════════════════════════════════════════════════════════════
-- Step 2: Add all monitoring tables to the share
-- ═══════════════════════════════════════════════════════════════════════════

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ BILLING & COST TABLES                                                   │
-- └─────────────────────────────────────────────────────────────────────────┘
ALTER SHARE workspace_monitoring_share ADD TABLE monitoring.workspace_usage.billing_usage_bronze;
ALTER SHARE workspace_monitoring_share ADD TABLE monitoring.workspace_usage.list_prices;
ALTER SHARE workspace_monitoring_share ADD TABLE monitoring.workspace_usage.billing_summary;
ALTER SHARE workspace_monitoring_share ADD TABLE monitoring.workspace_usage.cost_by_product;
ALTER SHARE workspace_monitoring_share ADD TABLE monitoring.workspace_usage.cost_by_sku;
ALTER SHARE workspace_monitoring_share ADD TABLE monitoring.workspace_usage.cost_by_job;
ALTER SHARE workspace_monitoring_share ADD TABLE monitoring.workspace_usage.cost_by_user;

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ JOBS & WORKFLOWS TABLES                                                 │
-- └─────────────────────────────────────────────────────────────────────────┘
ALTER SHARE workspace_monitoring_share ADD TABLE monitoring.workspace_usage.jobs_bronze;
ALTER SHARE workspace_monitoring_share ADD TABLE monitoring.workspace_usage.job_runs_bronze;
ALTER SHARE workspace_monitoring_share ADD TABLE monitoring.workspace_usage.jobs_current;
ALTER SHARE workspace_monitoring_share ADD TABLE monitoring.workspace_usage.job_runs_summary;

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ COMPUTE & CLUSTERS TABLES                                               │
-- └─────────────────────────────────────────────────────────────────────────┘
ALTER SHARE workspace_monitoring_share ADD TABLE monitoring.workspace_usage.clusters_bronze;
ALTER SHARE workspace_monitoring_share ADD TABLE monitoring.workspace_usage.clusters_current;
ALTER SHARE workspace_monitoring_share ADD TABLE monitoring.workspace_usage.cluster_utilization;

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ AUDIT & ACTIVITY TABLES                                                 │
-- └─────────────────────────────────────────────────────────────────────────┘
ALTER SHARE workspace_monitoring_share ADD TABLE monitoring.workspace_usage.audit_logs_bronze;
ALTER SHARE workspace_monitoring_share ADD TABLE monitoring.workspace_usage.user_activity_summary;
ALTER SHARE workspace_monitoring_share ADD TABLE monitoring.workspace_usage.data_access_summary;

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ MODEL SERVING TABLES                                                    │
-- └─────────────────────────────────────────────────────────────────────────┘
ALTER SHARE workspace_monitoring_share ADD TABLE monitoring.workspace_usage.served_entities_bronze;
ALTER SHARE workspace_monitoring_share ADD TABLE monitoring.workspace_usage.served_entities_current;
ALTER SHARE workspace_monitoring_share ADD TABLE monitoring.workspace_usage.endpoint_usage_bronze;
ALTER SHARE workspace_monitoring_share ADD TABLE monitoring.workspace_usage.endpoint_usage_summary;
ALTER SHARE workspace_monitoring_share ADD TABLE monitoring.workspace_usage.model_serving_costs;
ALTER SHARE workspace_monitoring_share ADD TABLE monitoring.workspace_usage.foundation_model_usage;
ALTER SHARE workspace_monitoring_share ADD TABLE monitoring.workspace_usage.external_model_usage;
ALTER SHARE workspace_monitoring_share ADD TABLE monitoring.workspace_usage.endpoint_inventory;

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ MLFLOW TABLES                                                           │
-- └─────────────────────────────────────────────────────────────────────────┘
ALTER SHARE workspace_monitoring_share ADD TABLE monitoring.workspace_usage.mlflow_experiments_bronze;
ALTER SHARE workspace_monitoring_share ADD TABLE monitoring.workspace_usage.mlflow_runs_bronze;
ALTER SHARE workspace_monitoring_share ADD TABLE monitoring.workspace_usage.mlflow_metrics_bronze;
ALTER SHARE workspace_monitoring_share ADD TABLE monitoring.workspace_usage.mlflow_experiments_summary;
ALTER SHARE workspace_monitoring_share ADD TABLE monitoring.workspace_usage.mlflow_runs_summary;

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ VECTOR SEARCH TABLES                                                    │
-- └─────────────────────────────────────────────────────────────────────────┘
ALTER SHARE workspace_monitoring_share ADD TABLE monitoring.workspace_usage.vector_search_costs;
ALTER SHARE workspace_monitoring_share ADD TABLE monitoring.workspace_usage.vector_search_summary;

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ EXECUTIVE SUMMARY & METADATA TABLES                                     │
-- └─────────────────────────────────────────────────────────────────────────┘
ALTER SHARE workspace_monitoring_share ADD TABLE monitoring.workspace_usage.workspace_info;
ALTER SHARE workspace_monitoring_share ADD TABLE monitoring.workspace_usage.executive_summary;
ALTER SHARE workspace_monitoring_share ADD TABLE monitoring.workspace_usage.aiml_executive_summary;

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ DAILY ROLLUP TABLES (created by daily_aggregation_job)                  │
-- └─────────────────────────────────────────────────────────────────────────┘
-- Note: Run after the daily aggregation job has executed at least once
ALTER SHARE workspace_monitoring_share ADD TABLE monitoring.workspace_usage.cost_weekly_rollup;
ALTER SHARE workspace_monitoring_share ADD TABLE monitoring.workspace_usage.cost_monthly_rollup;
ALTER SHARE workspace_monitoring_share ADD TABLE monitoring.workspace_usage.job_success_rate_weekly;
ALTER SHARE workspace_monitoring_share ADD TABLE monitoring.workspace_usage.cost_trend_analysis;

-- ═══════════════════════════════════════════════════════════════════════════
-- Step 3: Create recipient and grant access
-- ═══════════════════════════════════════════════════════════════════════════

-- Get the sharing identifier from the central (recipient) workspace first
-- Run this on the RECIPIENT workspace: SELECT current_metastore();

-- Create recipient (replace <sharing-identifier> with actual value)
CREATE RECIPIENT IF NOT EXISTS central_workspace 
USING ID '<sharing-identifier>'
COMMENT 'Central monitoring workspace for cross-account analytics';

-- Grant access to the share
GRANT SELECT ON SHARE workspace_monitoring_share TO RECIPIENT central_workspace;

-- Verify the share contents
SHOW ALL IN SHARE workspace_monitoring_share;
```

**On the Recipient Workspace (central hub):**
```sql
-- Get sharing identifier (share this with providers)
SELECT current_metastore();

-- After providers share, create catalog from share
CREATE CATALOG shared_prod_ws1 USING SHARE provider_name.workspace_monitoring_share;

-- Create consolidated views
CREATE VIEW consolidated.all_billing AS
SELECT * FROM shared_prod_ws1.workspace_usage.billing_summary
UNION ALL
SELECT * FROM shared_prod_ws2.workspace_usage.billing_summary;
```

---

## Deployment to Multiple Workspaces

For deploying to multiple workspaces in multiple accounts:

### Option A: Using Profiles

```bash
# Configure profiles for each workspace
databricks configure --profile ws_account_a_prod
databricks configure --profile ws_account_b_prod

# Deploy to each
databricks bundle deploy -t prod --profile ws_account_a_prod
databricks bundle deploy -t prod --profile ws_account_b_prod
```

### Option B: Using Service Principal (Recommended for CI/CD)

```bash
# Set environment variables
export DATABRICKS_HOST=https://workspace-a.cloud.databricks.com
export DATABRICKS_CLIENT_ID=<sp-client-id>
export DATABRICKS_CLIENT_SECRET=<sp-secret>

# Deploy
databricks bundle deploy -t prod
```

---

## Monitoring and Verification

### Check Pipeline Status

```bash
# View pipeline runs
databricks pipelines list-updates <pipeline-id>

# Get pipeline details
databricks pipelines get <pipeline-id>
```

### Verify Data in Tables

```sql
-- Check billing data
SELECT COUNT(*) FROM monitoring.workspace_usage.billing_summary;

-- Check latest data timestamp
SELECT MAX(usage_date) FROM monitoring.workspace_usage.billing_summary;

-- Check cost summary
SELECT 
    usage_date,
    billing_origin_product,
    SUM(estimated_cost_usd) as total_cost
FROM monitoring.workspace_usage.billing_summary
WHERE usage_date >= CURRENT_DATE - INTERVAL 7 DAYS
GROUP BY usage_date, billing_origin_product
ORDER BY usage_date DESC;
```

---

## Troubleshooting

### Common Issues

1. **"Table not found" error**
   - Ensure system tables are enabled (run enable_system_tables.py)
   - Check permissions on system schemas

2. **Pipeline fails with "permission denied"**
   - Verify service principal has correct permissions
   - Check Unity Catalog grants

3. **Delta Sharing issues**
   - Verify sharing identifier is correct
   - Ensure Delta Sharing is enabled on metastore
   - Check network connectivity between accounts

### Getting Help

- Review pipeline logs in Databricks UI
- Check audit logs: `SELECT * FROM system.access.audit WHERE action_name LIKE '%pipeline%'`
- Contact Databricks support for system table issues

---

## Schedule Summary

### Job Details

| Job | Default Schedule | Purpose | Compute |
|-----|------------------|---------|---------|
| `monitoring_pipeline_job` | Every 15 min | Refresh DLT pipeline with latest system table data | Serverless (via DLT) |
| `daily_aggregation_job` | Daily 2 AM UTC | Create rollups, trends, and run maintenance | Serverless |

### Customizing Schedules

**Pipeline Refresh Job**: Set via `refresh_schedule` variable (Quartz cron format)

| Frequency | Cron Expression | Use Case |
|-----------|-----------------|----------|
| Every 5 minutes | `0 */5 * * * ?` | Near real-time monitoring, incident response |
| Every 15 minutes | `0 */15 * * * ?` | **Recommended** - balanced freshness and cost |
| Every 30 minutes | `0 */30 * * * ?` | Standard monitoring |
| Hourly | `0 0 * * * ?` | Cost-conscious environments |
| Every 4 hours | `0 0 */4 * * ?` | Dev/test environments |

**Daily Aggregation Job**: Fixed at 2 AM UTC to run after end-of-day data is complete. Modify in `resources/job.yml` if needed:

```yaml
schedule:
  quartz_cron_expression: "0 0 2 * * ?"  # Change time here
  timezone_id: "UTC"  # Or your preferred timezone
```

### Understanding the Data Flow

```
System Tables          DLT Pipeline              Daily Aggregation
(source)               (15-min refresh)          (2 AM daily)
    │                        │                         │
    ▼                        ▼                         ▼
┌─────────────┐        ┌─────────────┐          ┌─────────────┐
│ system.     │        │ Bronze      │          │ Weekly/     │
│ billing.    │───────▶│ Tables      │          │ Monthly     │
│ usage       │        │             │          │ Rollups     │
└─────────────┘        └──────┬──────┘          └─────────────┘
                              │                        ▲
                              ▼                        │
                       ┌─────────────┐                 │
                       │ Aggregated  │─────────────────┘
                       │ Tables      │
                       │ (cost_by_*) │
                       └─────────────┘
```

---

## File Structure

```
databricks-system-tables-sharing/
├── databricks.yml              # Bundle configuration with all variables
├── README.md                   # Full documentation
├── QUICKSTART.md              # This guide
├── resources/
│   ├── pipeline.yml           # DLT pipeline definition
│   ├── job.yml                # Scheduled jobs (refresh + daily aggregation)
│   └── sharing.yml            # Delta Sharing resources & setup job
└── src/
    ├── dlt_monitoring_pipeline.py   # Core DLT (billing, jobs, compute, audit)
    ├── dlt_aiml_monitoring.py       # AI/ML DLT (serving, MLflow, vector)
    ├── daily_aggregations.py        # Daily rollup notebook
    ├── setup_delta_sharing.py       # Delta Sharing setup notebook
    ├── enable_system_tables.py      # System table enablement
    └── sql/
        ├── billing_views.sql        # Additional billing views
        ├── aiml_views.sql           # Additional AI/ML views
        └── delta_sharing_setup.sql  # Manual SQL commands (alternative to job)
```

## Deployed Resources Summary

| Resource Type | Name | Purpose |
|---------------|------|---------|
| DLT Pipeline | `[env] System Tables Monitoring - {workspace}` | Streams & transforms system tables |
| Job | `[env] System Tables Monitoring Refresh - {workspace}` | Triggers pipeline every 15 min |
| Job | `[env] System Tables Daily Aggregation - {workspace}` | Creates rollups at 2 AM |
| Job | `[env] Setup Delta Sharing - {workspace}` | One-time share setup |
| Share | `{workspace}_monitoring_share` | Delta Share for cross-workspace access |
| Schema | `{catalog}.{schema}` | Contains all monitoring tables |
