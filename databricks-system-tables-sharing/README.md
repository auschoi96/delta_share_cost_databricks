# Databricks System Tables - Cross-Account/Cross-Workspace Delta Sharing Solution

## Executive Summary

This solution enables centralized monitoring of Databricks usage, billing, and activity across **multiple workspaces in multiple accounts** by:
1. Creating curated views and Delta tables from Databricks system tables
2. Sharing them via Delta Sharing to a central workspace
3. Using Databricks Asset Bundles for reproducible deployment

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        PROVIDER WORKSPACES (Multiple Accounts)                   │
├─────────────────────────────────────────────────────────────────────────────────┤
│  Account A - Workspace 1          Account B - Workspace 1         Account N...  │
│  ┌─────────────────────────┐     ┌─────────────────────────┐                    │
│  │  system.billing.usage   │     │  system.billing.usage   │                    │
│  │  system.access.audit    │     │  system.access.audit    │                    │
│  │  system.compute.*       │     │  system.compute.*       │                    │
│  │  system.lakeflow.*      │     │  system.lakeflow.*      │                    │
│  │  system.query.history   │     │  system.query.history   │                    │
│  └───────────┬─────────────┘     └───────────┬─────────────┘                    │
│              │ DLT Pipeline                   │ DLT Pipeline                     │
│              ▼                                ▼                                   │
│  ┌─────────────────────────┐     ┌─────────────────────────┐                    │
│  │  monitoring_catalog     │     │  monitoring_catalog     │                    │
│  │  └── workspace_usage    │     │  └── workspace_usage    │                    │
│  │      ├── billing_summary│     │      ├── billing_summary│                    │
│  │      ├── cost_by_*      │     │      ├── cost_by_*      │                    │
│  │      ├── jobs_overview  │     │      ├── jobs_overview  │                    │
│  │      └── activity_*     │     │      └── activity_*     │                    │
│  └───────────┬─────────────┘     └───────────┬─────────────┘                    │
│              │ Delta Sharing                  │ Delta Sharing                    │
└──────────────┼────────────────────────────────┼──────────────────────────────────┘
               │                                │
               ▼                                ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        RECIPIENT WORKSPACE (Central Hub)                         │
├─────────────────────────────────────────────────────────────────────────────────┤
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │  shared_monitoring (catalog from Delta Share)                            │    │
│  │  ├── account_a_ws1                                                       │    │
│  │  │   ├── billing_summary                                                 │    │
│  │  │   ├── cost_by_product                                                 │    │
│  │  │   └── ...                                                             │    │
│  │  ├── account_b_ws1                                                       │    │
│  │  │   ├── billing_summary                                                 │    │
│  │  │   └── ...                                                             │    │
│  │  └── consolidated_views                                                  │    │
│  │      ├── all_workspaces_billing                                          │    │
│  │      ├── all_workspaces_costs                                            │    │
│  │      └── cross_account_dashboard                                         │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## Part 1: System Tables Reference

### Key System Tables for Usage, Activity, and Billing

#### 1. `system.billing.usage` (Global - Primary for Cost Tracking)
**Purpose:** Records all billable usage across your entire account
**Retention:** 365 days (free)
**Supports Streaming:** Yes

| Column | Type | Description |
|--------|------|-------------|
| `record_id` | string | Unique ID for this usage record |
| `account_id` | string | Account ID |
| `workspace_id` | string | Workspace ID |
| `sku_name` | string | SKU name (e.g., JOBS_COMPUTE, SERVERLESS_SQL) |
| `cloud` | string | AWS, AZURE, or GCP |
| `usage_start_time` | timestamp | Start time of usage |
| `usage_end_time` | timestamp | End time of usage |
| `usage_date` | date | Date of usage (for fast aggregation) |
| `usage_quantity` | decimal | Number of units consumed |
| `usage_unit` | string | Unit type (DBU, GPU_HOUR) |
| `usage_metadata` | struct | Contains job_id, cluster_id, warehouse_id, notebook_id, etc. |
| `identity_metadata` | struct | run_as identity, owned_by |
| `custom_tags` | map | User-defined tags for cost attribution |
| `billing_origin_product` | string | JOBS, DLT, SQL, MODEL_SERVING, etc. |
| `product_features` | struct | jobs_tier, sql_tier, is_serverless, is_photon, etc. |
| `record_type` | string | ORIGINAL, RETRACTION, or RESTATEMENT |

#### 2. `system.billing.list_prices` (Global)
**Purpose:** Historical SKU pricing (SCD2 table)
**Retention:** Indefinite

| Column | Type | Description |
|--------|------|-------------|
| `sku_name` | string | SKU identifier |
| `cloud` | string | Cloud provider |
| `currency_code` | string | Currency (USD) |
| `usage_unit` | string | DBU, GPU_HOUR |
| `pricing` | struct | Contains effective_list.default price |
| `price_start_time` | timestamp | Price effective start |
| `price_end_time` | timestamp | Price effective end (null = current) |

#### 3. `system.access.audit` (Regional)
**Purpose:** All audit events for security and compliance
**Retention:** 365 days (free)
**Supports Streaming:** Yes

| Column | Type | Description |
|--------|------|-------------|
| `account_id` | string | Account ID |
| `workspace_id` | string | Workspace ID (0 = account-level) |
| `event_time` | timestamp | When the event occurred |
| `event_date` | date | Date of event |
| `service_name` | string | Service (unityCatalog, databrickssql, etc.) |
| `action_name` | string | Action performed |
| `user_identity` | struct | User email, subject_name |
| `request_params` | map | Action-specific parameters |
| `response` | struct | Response status and result |
| `source_ip_address` | string | Origin IP |

#### 4. `system.compute.clusters` (Regional - SCD2)
**Purpose:** Cluster configuration history
**Retention:** 365 days

| Column | Type | Description |
|--------|------|-------------|
| `account_id` | string | Account ID |
| `workspace_id` | string | Workspace ID |
| `cluster_id` | string | Cluster identifier |
| `cluster_name` | string | User-given name |
| `owned_by` | string | Owner user ID |
| `driver_node_type` | string | Driver instance type |
| `worker_node_type` | string | Worker instance type |
| `num_workers` | int | Fixed worker count |
| `autoscale` | struct | min_workers, max_workers |
| `data_security_mode` | string | Access mode |
| `dbr_version` | string | Databricks Runtime version |
| `change_time` | timestamp | Last change timestamp |
| `delete_time` | timestamp | Deletion time (null if active) |

#### 5. `system.compute.node_timeline` (Regional)
**Purpose:** Node-level resource utilization at minute granularity
**Retention:** 90 days

| Column | Type | Description |
|--------|------|-------------|
| `cluster_id` | string | Cluster ID |
| `start_time` | timestamp | Minute start |
| `end_time` | timestamp | Minute end |
| `driver` | boolean | Is this the driver node |
| `cpu_user_percent` | double | User CPU % |
| `cpu_system_percent` | double | System CPU % |
| `mem_used_percent` | double | Memory utilization % |
| `network_received_bytes` | long | Network in |
| `network_sent_bytes` | long | Network out |

#### 6. `system.lakeflow.jobs` (Regional - SCD2)
**Purpose:** Job definitions and metadata
**Retention:** 365 days

| Column | Type | Description |
|--------|------|-------------|
| `account_id` | string | Account ID |
| `workspace_id` | string | Workspace ID |
| `job_id` | string | Job identifier |
| `name` | string | Job name |
| `creator_id` | string | Creator user ID |
| `run_as` | string | Execution identity |
| `tags` | map | Job tags |
| `schedule` | struct | Cron schedule |
| `change_time` | timestamp | Last modification |
| `delete_time` | timestamp | Deletion time |

#### 7. `system.lakeflow.job_run_timeline` (Regional)
**Purpose:** Job run execution tracking
**Retention:** 365 days

| Column | Type | Description |
|--------|------|-------------|
| `workspace_id` | string | Workspace ID |
| `job_id` | string | Job ID |
| `run_id` | string | Run ID |
| `run_name` | string | Run name |
| `run_type` | string | JOB_RUN or SUBMIT_RUN |
| `period_start_time` | timestamp | Run start |
| `period_end_time` | timestamp | Run end |
| `result_state` | string | SUCCEEDED, FAILED, CANCELED, SKIPPED |
| `termination_code` | string | Termination reason |
| `compute_ids` | array | Clusters used |

#### 8. `system.query.history` (Regional)
**Purpose:** SQL warehouse and serverless query tracking
**Retention:** 365 days

| Column | Type | Description |
|--------|------|-------------|
| `account_id` | string | Account ID |
| `workspace_id` | string | Workspace ID |
| `statement_id` | string | Query statement ID |
| `executed_by` | string | User who ran query |
| `statement_text` | string | SQL text |
| `start_time` | timestamp | Query start |
| `end_time` | timestamp | Query end |
| `duration_ms` | long | Execution duration |
| `warehouse_id` | string | Warehouse ID |
| `rows_produced` | long | Result rows |
| `bytes_scanned` | long | Data scanned |

#### 9. `system.access.workspaces_latest` (Global)
**Purpose:** Workspace metadata (slow-changing dimension)
**Retention:** Indefinite

| Column | Type | Description |
|--------|------|-------------|
| `workspace_id` | string | Workspace ID |
| `workspace_name` | string | Workspace name |
| `cloud` | string | Cloud provider |
| `region` | string | Region |
| `create_time` | timestamp | Creation time |

---

## Part 2: Implementation Plan

### Phase 1: Enable System Tables (Per Workspace)

```python
# Run this in each workspace to enable required system schemas
import requests

# Get workspace URL and token
workspace_url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)

# Get metastore ID
metastore_id = spark.sql("SELECT current_metastore()").collect()[0][0]

# Schemas to enable
schemas_to_enable = ['access', 'compute', 'lakeflow', 'query', 'serving']

for schema in schemas_to_enable:
    response = requests.put(
        f"{workspace_url}/api/2.0/unity-catalog/metastores/{metastore_id}/systemschemas/{schema}",
        headers={"Authorization": f"Bearer {token}"}
    )
    print(f"Enabling {schema}: {response.status_code}")
```

### Phase 2: Deploy Asset Bundle to Each Workspace

The bundle creates:
1. **Streaming Delta Tables** from system tables (for near-real-time data)
2. **Aggregated Views** for cost analysis
3. **Delta Shares** for cross-account access

### Phase 3: Configure Delta Sharing

**On Each Provider Workspace:**
```sql
-- Create the share
CREATE SHARE workspace_monitoring_share;

-- Add tables to share
ALTER SHARE workspace_monitoring_share ADD TABLE monitoring_catalog.workspace_usage.billing_summary;
ALTER SHARE workspace_monitoring_share ADD TABLE monitoring_catalog.workspace_usage.cost_by_product;
ALTER SHARE workspace_monitoring_share ADD TABLE monitoring_catalog.workspace_usage.cost_by_job;
ALTER SHARE workspace_monitoring_share ADD TABLE monitoring_catalog.workspace_usage.job_runs_summary;
ALTER SHARE workspace_monitoring_share ADD TABLE monitoring_catalog.workspace_usage.cluster_utilization;

-- Create recipient (using central workspace's sharing identifier)
CREATE RECIPIENT central_monitoring
USING ID '<sharing-identifier-from-central-workspace>';

-- Grant access
GRANT SELECT ON SHARE workspace_monitoring_share TO RECIPIENT central_monitoring;
```

**On Central Recipient Workspace:**
```sql
-- Create catalog from each provider's share
CREATE CATALOG IF NOT EXISTS shared_account_a_ws1 USING SHARE account_a_provider.workspace_monitoring_share;
CREATE CATALOG IF NOT EXISTS shared_account_b_ws1 USING SHARE account_b_provider.workspace_monitoring_share;

-- Create consolidated views
CREATE VIEW all_workspaces_billing AS
SELECT 'account_a_ws1' as source_workspace, * FROM shared_account_a_ws1.workspace_usage.billing_summary
UNION ALL
SELECT 'account_b_ws1' as source_workspace, * FROM shared_account_b_ws1.workspace_usage.billing_summary;
```

---

## Part 3: AI/ML Workloads Monitoring

This solution includes dedicated monitoring for AI/ML workloads:

### Model Serving
- **Served Entities**: Track all deployed models (custom, foundation, external)
- **Endpoint Usage**: Token counts for each endpoint (requires AI Gateway)
- **Foundation Models**: DBRX, Llama, Mixtral usage via Databricks
- **External Models**: OpenAI, Anthropic, Cohere routed through AI Gateway
- **Cost Attribution**: Full cost breakdown by endpoint and model

### MLflow
- **Experiments**: Track experiment creation and activity
- **Runs**: Monitor training run success rates and durations
- **Metrics**: Training metric timeseries data

### Vector Search
- **Index Costs**: Cost tracking by vector search endpoint
- **Usage Patterns**: Daily usage trends

### Tables Created for AI/ML

| Table | Purpose |
|-------|---------|
| `served_entities_current` | Current model serving configurations |
| `endpoint_usage_summary` | Daily token usage by endpoint |
| `model_serving_costs` | Cost breakdown for model serving |
| `foundation_model_usage` | Foundation model API usage |
| `external_model_usage` | External model (OpenAI, etc.) usage |
| `mlflow_experiments_summary` | Experiment activity metrics |
| `mlflow_runs_summary` | Daily run statistics |
| `vector_search_costs` | Vector search index costs |
| `vector_search_summary` | Daily vector search usage |
| `aiml_executive_summary` | Combined AI/ML spend overview |
| `endpoint_inventory` | Complete endpoint inventory |

---

## Part 4: Templates for Extension

This project includes templates to help you add new tables and customize the solution:

### Template Files

| File | Purpose |
|------|---------|
| `src/templates/dlt_table_template.py` | Template for adding new DLT tables |
| `src/templates/sql_view_template.sql` | Template for creating SQL views |
| `src/templates/delta_share_template.sql` | Template for Delta Sharing setup |
| `config/config_template.yml` | Configuration template for deployments |

### How to Add New Tables

1. **Copy the DLT template**:
   ```bash
   cp src/templates/dlt_table_template.py src/dlt_my_feature.py
   ```

2. **Modify the template** following the patterns for:
   - Streaming tables (for tables that support streaming)
   - Snapshot tables (for tables that don't support streaming)
   - SCD2 current state (for slowly changing dimension tables)
   - Aggregations (for roll-up tables)

3. **Add to pipeline**:
   ```yaml
   # In resources/pipeline.yml
   libraries:
     - notebook:
         path: ../src/dlt_my_feature.py
   ```

4. **Add to Delta Share**:
   ```sql
   ALTER SHARE workspace_monitoring_share 
   ADD TABLE monitoring.workspace_usage.my_new_table;
   ```

See `docs/EXTENDING.md` for detailed instructions.

---

## Part 5: Important Considerations

### Near-Real-Time vs True Real-Time

**Important:** Databricks system tables do NOT support true real-time monitoring. Data is updated throughout the day with some latency (typically minutes to hours). For near-real-time:

1. **Use Streaming Tables** in DLT with `skipChangeCommits=true`
2. **Schedule frequent pipeline refreshes** (every 5-15 minutes)
3. **Use `Trigger.Once` or scheduled triggers** (not continuous)

### Streaming from System Tables

```python
# When streaming from system tables, always use skipChangeCommits
(spark.readStream
    .option("skipChangeCommits", "true")
    .table("system.billing.usage")
    .writeStream
    .trigger(processingTime="5 minutes")
    .toTable("monitoring_catalog.workspace_usage.billing_stream"))
```

### Schema Evolution

System tables may add new columns at any time. Enable schema evolution:
```python
.option("mergeSchema", "true")
```

### Data Retention

| Table Category | Free Retention |
|----------------|----------------|
| Billing | 365 days |
| Audit | 365 days |
| Compute | 365 days (clusters), 90 days (node_timeline) |
| Jobs | 365 days |
| Query History | 365 days |

---

## Project Files

```
databricks-system-tables-sharing/
├── databricks.yml                     # Bundle configuration
├── README.md                          # Full documentation
├── QUICKSTART.md                      # Quick deployment guide
├── config/
│   └── config_template.yml            # Configuration template
├── docs/
│   └── EXTENDING.md                   # Guide for adding new tables
├── resources/
│   ├── pipeline.yml                   # DLT pipeline definition
│   └── job.yml                        # Scheduled jobs
└── src/
    ├── dlt_monitoring_pipeline.py     # Core monitoring (billing, jobs, compute)
    ├── dlt_aiml_monitoring.py         # AI/ML monitoring (serving, MLflow, vector)
    ├── daily_aggregations.py          # Daily rollup notebook
    ├── enable_system_tables.py        # System table enablement
    ├── sql/
    │   ├── billing_views.sql          # Billing analysis views
    │   ├── aiml_views.sql             # AI/ML analysis views
    │   └── delta_sharing_setup.sql    # Delta Sharing commands
    └── templates/
        ├── dlt_table_template.py      # Template for new DLT tables
        ├── sql_view_template.sql      # Template for new SQL views
        └── delta_share_template.sql   # Template for Delta Sharing
```

---

## Jobs and Pipeline Architecture

This solution deploys **one DLT pipeline** and **two scheduled jobs** to each workspace.

### DLT Pipeline: `system_tables_monitoring_pipeline`

The DLT (Delta Live Tables) pipeline is the core data processing engine. It:

1. **Streams data** from Databricks system tables (billing, compute, jobs, audit, serving, MLflow)
2. **Creates bronze tables** that mirror the source system tables with workspace/account identifiers
3. **Transforms and aggregates** data into silver/gold materialized views for analysis
4. **Handles schema evolution** automatically as system tables add new columns

**Configuration:**
- **Mode**: Triggered (not continuous) for cost efficiency
- **Compute**: Serverless (recommended) or classic compute with Photon
- **Target**: Unity Catalog schema specified by `monitoring_catalog.monitoring_schema`

**Source Notebooks:**
- `dlt_monitoring_pipeline.py` - Billing, Jobs, Compute, Audit tables
- `dlt_aiml_monitoring.py` - Model Serving, MLflow, Vector Search tables

### Job 1: `monitoring_pipeline_job`

**Purpose**: Refreshes the DLT pipeline on a regular schedule to keep data current.

| Setting | Value | Description |
|---------|-------|-------------|
| Schedule | Every 15 minutes (configurable) | Set via `refresh_schedule` variable |
| Timeout | 1 hour | Maximum time for pipeline refresh |
| Retries | 2 | Automatic retry on failure |
| Concurrency | 1 | Prevents overlapping runs |

**What it does:**
1. Triggers an incremental refresh of the DLT pipeline
2. Processes new records from all system tables since the last refresh
3. Updates all downstream materialized views automatically

**When to run more frequently:**
- Near-real-time cost monitoring requirements
- Active incident response or debugging
- High-volume environments with rapid changes

**When to run less frequently:**
- Cost optimization (fewer DBUs consumed)
- Lower data freshness requirements
- Development/test environments

### Job 2: `daily_aggregation_job`

**Purpose**: Creates historical rollups and performs maintenance tasks.

| Setting | Value | Description |
|---------|-------|-------------|
| Schedule | Daily at 2 AM UTC | Fixed schedule |
| Timeout | 2 hours | Maximum time for aggregations |
| Compute | Serverless | Uses environment specification |

**What it does:**
1. **Weekly Cost Rollup** (`cost_weekly_rollup`) - Aggregates costs by week for trend analysis
2. **Monthly Cost Rollup** (`cost_monthly_rollup`) - Aggregates costs by month for budgeting
3. **Job Success Rate Analysis** (`job_success_rate_weekly`) - Weekly job reliability metrics
4. **Cost Trend Detection** (`cost_trend_analysis`) - Week-over-week cost change calculations
5. **Table Optimization** - Runs OPTIMIZE on large tables for query performance
6. **Table Vacuuming** - Cleans up old file versions to reduce storage costs

---

## Materialized Views Reference

### Core Monitoring Tables (from `dlt_monitoring_pipeline.py`)

#### Bronze (Raw) Tables

| Table | Source | Description |
|-------|--------|-------------|
| `billing_usage_bronze` | `system.billing.usage` | Raw billing records with usage quantities, SKUs, and metadata |
| `list_prices` | `system.billing.list_prices` | SKU pricing reference (SCD2 with effective dates) |
| `jobs_bronze` | `system.lakeflow.jobs` | Job definitions including schedules, tags, and configurations |
| `job_runs_bronze` | `system.lakeflow.job_run_timeline` | Job execution records with timing and status |
| `clusters_bronze` | `system.compute.clusters` | Cluster configurations (instance types, autoscaling, DBR versions) |
| `audit_logs_bronze` | `system.access.audit` | Security and compliance audit events |

#### Silver/Gold (Aggregated) Tables

| Table | Description | Key Columns |
|-------|-------------|-------------|
| `billing_summary` | **Primary cost table** - Usage records joined with prices to calculate estimated costs | `usage_date`, `sku_name`, `usage_quantity`, `estimated_cost_usd`, `billing_origin_product` |
| `cost_by_product` | Daily costs grouped by product (JOBS, SQL, SERVING, etc.) | `usage_date`, `billing_origin_product`, `total_dbus`, `total_cost_usd` |
| `cost_by_sku` | Daily costs grouped by SKU for detailed analysis | `usage_date`, `sku_name`, `cloud`, `total_dbus`, `total_cost_usd` |
| `cost_by_job` | Costs attributed to specific jobs | `job_id`, `job_name`, `total_dbus`, `total_cost_usd`, `run_count` |
| `cost_by_user` | Costs attributed to users (run_as identity) | `user_identity`, `total_dbus`, `total_cost_usd` |
| `jobs_current` | Current state of all jobs (latest version from SCD2) | `job_id`, `name`, `creator_id`, `schedule`, `delete_time` |
| `job_runs_summary` | Daily job execution statistics | `run_date`, `job_id`, `total_runs`, `successful_runs`, `failed_runs`, `avg_duration_seconds` |
| `clusters_current` | Current state of all clusters | `cluster_id`, `cluster_name`, `driver_node_type`, `worker_node_type`, `autoscale` |
| `cluster_utilization` | Resource utilization metrics (CPU, memory, network) | `cluster_id`, `date`, `avg_cpu_percent`, `avg_memory_percent`, `peak_cpu_percent` |
| `user_activity_summary` | Daily user activity from audit logs | `event_date`, `user_email`, `service_name`, `event_count`, `distinct_actions` |
| `data_access_summary` | Table access patterns for governance | `event_date`, `table_name`, `access_type`, `user_count`, `access_count` |
| `workspace_info` | Workspace metadata for joins | `workspace_id`, `workspace_identifier`, `account_identifier` |
| `executive_summary` | High-level KPIs for dashboards | `summary_date`, `total_cost_usd`, `active_jobs`, `active_clusters`, `active_users` |

### AI/ML Monitoring Tables (from `dlt_aiml_monitoring.py`)

#### Bronze (Raw) Tables

| Table | Source | Description |
|-------|--------|-------------|
| `served_entities_bronze` | `system.serving.served_entities` | Model serving entity configurations (SCD2) |
| `endpoint_usage_bronze` | `system.serving.endpoint_usage` | Per-request token usage for model endpoints |
| `mlflow_experiments_bronze` | `system.mlflow.experiments_latest` | MLflow experiment metadata |
| `mlflow_runs_bronze` | `system.mlflow.runs_latest` | MLflow run records with params and metrics |
| `mlflow_metrics_bronze` | `system.mlflow.run_metrics_history` | Detailed metric history for training runs |

#### Silver/Gold (Aggregated) Tables

| Table | Description | Key Columns |
|-------|-------------|-------------|
| `served_entities_current` | Current model serving configurations (latest from SCD2) | `endpoint_name`, `entity_type`, `entity_name`, `entity_version`, `task` |
| `endpoint_usage_summary` | Daily token usage by endpoint | `usage_date`, `endpoint_name`, `entity_type`, `total_requests`, `total_input_tokens`, `total_output_tokens` |
| `model_serving_costs` | Cost breakdown for model serving from billing | `usage_date`, `endpoint_name`, `total_dbus`, `total_cost_usd` |
| `foundation_model_usage` | Foundation Model API usage (DBRX, Llama, Mixtral) | `usage_date`, `endpoint_name`, `entity_name`, `total_requests`, `total_tokens` |
| `external_model_usage` | External model usage (OpenAI, Anthropic, Cohere) | `usage_date`, `endpoint_name`, `entity_name`, `total_requests`, `total_tokens` |
| `mlflow_experiments_summary` | Experiment activity metrics | `experiment_id`, `name`, `total_runs`, `successful_runs`, `distinct_users`, `last_run` |
| `mlflow_runs_summary` | Daily ML training statistics | `run_date`, `experiment_id`, `status`, `run_count`, `avg_duration_seconds` |
| `vector_search_costs` | Vector Search index costs | `usage_date`, `endpoint_name`, `total_dbus`, `total_cost_usd` |
| `vector_search_summary` | Daily Vector Search usage | `usage_date`, `index_name`, `query_count`, `avg_latency_ms` |
| `aiml_executive_summary` | Combined AI/ML spend overview | `summary_date`, `model_serving_cost`, `vector_search_cost`, `total_aiml_cost`, `total_requests` |
| `endpoint_inventory` | Complete endpoint inventory with costs | `endpoint_name`, `entity_type`, `entity_name`, `created_by`, `total_cost_usd_30d` |

### Daily Aggregation Tables (from `daily_aggregations.py`)

| Table | Description | Refresh |
|-------|-------------|---------|
| `cost_weekly_rollup` | Costs aggregated by week and product | Daily |
| `cost_monthly_rollup` | Costs aggregated by month and product | Daily |
| `job_success_rate_weekly` | Weekly job success rates by job | Daily |
| `cost_trend_analysis` | Week-over-week cost changes | Daily |

---

## Deployment Instructions

### Prerequisites
1. Databricks CLI installed and configured
2. Account admin or metastore admin privileges
3. System tables enabled on each workspace

### Deploy to Each Provider Workspace

```bash
# Clone and navigate to project
cd databricks-system-tables-sharing

# Configure target workspace in databricks.yml or use profile
databricks bundle validate -t dev

# Deploy the bundle
databricks bundle deploy -t dev

# Run the pipeline
databricks bundle run monitoring_pipeline_job -t dev
```

### Set Up Delta Sharing

After deploying to all provider workspaces:

1. Get the sharing identifier from the central (recipient) workspace
2. Create recipients on each provider workspace
3. Grant share access to recipients
4. Create catalogs from shares on the central workspace
5. Build consolidated views for cross-workspace analytics

---

## Estimated Costs

- **System Tables**: billing and list_prices are free; others may incur charges
- **DLT Pipeline**: Serverless or jobs compute costs
- **Delta Sharing**: No additional cost for Databricks-to-Databricks sharing
- **Storage**: Delta tables stored in your cloud storage

---

## Next Steps

1. Review the DLT pipeline notebook (`src/dlt_monitoring_pipeline.py`)
2. Customize aggregation logic for your needs
3. Set up alerting using Databricks SQL alerts
4. Build dashboards using AI/BI dashboards
