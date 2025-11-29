# Databricks System Tables Monitoring

A Databricks Asset Bundle that creates a centralized monitoring solution using system tables and Delta Sharing.

## What This Does

1. **Streams data** from Databricks system tables (billing, compute, jobs, audit, AI/ML)
2. **Creates aggregated tables** for cost analysis, job monitoring, and usage tracking
3. **Enables Delta Sharing** for cross-workspace and cross-account visibility

## Architecture

```
Provider Workspaces (Multiple Accounts)          Central Workspace
┌──────────────────────────────────┐            ┌──────────────────────────┐
│  system.billing.usage            │            │                          │
│  system.lakeflow.jobs            │   Delta    │  shared_monitoring       │
│  system.compute.clusters         │  Sharing   │  ├── workspace_a         │
│  system.access.audit             │ ─────────▶ │  │   └── billing_summary │
│  system.serving.*                │            │  ├── workspace_b         │
│  system.mlflow.*                 │            │  │   └── billing_summary │
│          │                       │            │  └── consolidated_views  │
│          ▼                       │            │                          │
│  DLT Pipeline → Monitoring Tables│            └──────────────────────────┘
└──────────────────────────────────┘
```

## What Gets Deployed

| Resource | Purpose |
|----------|---------|
| DLT Pipeline | Streams system tables and creates aggregated views |
| Pipeline Refresh Job | Triggers pipeline every 15 min (configurable) |
| Daily Aggregation Job | Creates weekly/monthly rollups at 2 AM UTC |
| Delta Sharing Setup Job | One-time job to configure sharing |

## Tables Created

**Billing & Costs**
- `billing_summary` - Usage records with calculated costs
- `cost_by_product` - Daily costs by product (Jobs, SQL, DLT, etc.)
- `cost_by_sku`, `cost_by_job`, `cost_by_user` - Cost attribution

**Jobs & Compute**
- `jobs_current`, `job_runs_summary` - Job definitions and run statistics
- `clusters_current`, `cluster_utilization` - Cluster configs and resource usage

**Audit & Activity**
- `audit_logs_bronze` - Raw audit events
- `user_activity_summary`, `data_access_summary` - Activity analytics

**AI/ML** (from `dlt_aiml_monitoring.py`)
- `served_entities_current`, `endpoint_usage_summary` - Model serving
- `mlflow_experiments_summary`, `mlflow_runs_summary` - MLflow tracking
- `vector_search_costs`, `vector_search_summary` - Vector Search

## Quick Start

```bash
# 1. Clone and configure
cd databricks-system-tables-sharing
# Edit databricks.yml with your workspace settings

# 2. Deploy
databricks bundle deploy -t dev

# 3. Run pipeline
databricks bundle run monitoring_pipeline_job -t dev

# 4. Set up Delta Sharing (after tables exist)
databricks bundle run setup_delta_sharing_job -t dev
```

See [QUICKSTART.md](QUICKSTART.md) for detailed deployment instructions.

## Configuration

Key variables in `databricks.yml`:

| Variable | Purpose |
|----------|---------|
| `monitoring_catalog` | Unity Catalog for tables |
| `monitoring_schema` | Schema within the catalog |
| `account_identifier` | Embedded in rows for cross-account tracking |
| `workspace_identifier` | Embedded in rows for cross-workspace tracking |
| `refresh_schedule` | How often to refresh (default: every 15 min) |

## Project Structure

```
databricks-system-tables-sharing/
├── databricks.yml              # Bundle configuration
├── resources/
│   ├── pipeline.yml            # DLT pipeline definition
│   ├── job.yml                 # Scheduled jobs
│   └── sharing.yml             # Delta Sharing setup job
└── src/
    ├── dlt_monitoring_pipeline.py   # Core DLT (billing, jobs, compute, audit)
    ├── dlt_aiml_monitoring.py       # AI/ML DLT (serving, MLflow, vector)
    ├── daily_aggregations.py        # Daily rollup notebook
    ├── setup_delta_sharing.py       # Delta Sharing setup
    └── enable_system_tables.py      # System table enablement
```

## Documentation

- [QUICKSTART.md](QUICKSTART.md) - Step-by-step deployment guide
- [docs/EXTENDING.md](databricks-system-tables-sharing/docs/EXTENDING.md) - How to add new tables

## Requirements

- Databricks CLI v0.218.0+
- Unity Catalog enabled
- Account Admin or Metastore Admin privileges
- System tables enabled on workspace
