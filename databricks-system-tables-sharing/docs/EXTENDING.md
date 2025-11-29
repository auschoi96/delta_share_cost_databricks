# Extending the System Tables Monitoring Solution

This guide explains how to add new tables, views, and data sources to the monitoring solution.

---

## Table of Contents

1. [Adding New DLT Tables](#adding-new-dlt-tables)
2. [Adding New SQL Views](#adding-new-sql-views)
3. [Adding Tables to Delta Share](#adding-tables-to-delta-share)
4. [Adding New System Table Sources](#adding-new-system-table-sources)
5. [Common Patterns](#common-patterns)
6. [Available System Tables Reference](#available-system-tables-reference)

---

## Adding New DLT Tables

### Step 1: Create a New DLT Notebook

Create a new file in `src/` following this pattern:

```python
# src/dlt_my_extension.py

import dlt
from pyspark.sql import functions as F

# Get configuration
account_identifier = spark.conf.get("account_identifier", "unknown")
workspace_identifier = spark.conf.get("workspace_identifier", "unknown")

@dlt.table(
    name="my_new_table",
    comment="Description of what this table contains",
    table_properties={
        "quality": "silver",  # bronze, silver, or gold
        "delta.enableChangeDataFeed": "true"  # Enable for streaming consumers
    }
)
def my_new_table():
    """
    Docstring explaining the table purpose.
    """
    return (
        spark.table("system.schema.source_table")
        .withColumn("_account_identifier", F.lit(account_identifier))
        .withColumn("_workspace_identifier", F.lit(workspace_identifier))
        # Add your transformations here
    )
```

### Step 2: Add to Pipeline Definition

Edit `resources/pipeline.yml`:

```yaml
libraries:
  - notebook:
      path: ../src/dlt_monitoring_pipeline.py
  - notebook:
      path: ../src/dlt_my_extension.py  # Add your new notebook
```

### Step 3: Add to Delta Share (if needed)

```sql
ALTER SHARE workspace_monitoring_share 
ADD TABLE ${catalog}.${schema}.my_new_table;
```

---

## Adding New SQL Views

### Step 1: Create View Definition

Add to `src/sql/` or create a new file:

```sql
-- src/sql/my_custom_views.sql

CREATE OR REPLACE VIEW ${catalog}.${schema}.v_my_custom_view AS
SELECT
    _account_identifier,
    _workspace_identifier,
    -- Your columns here
FROM ${catalog}.${schema}.source_table
WHERE -- Your filters
GROUP BY ALL;
```

### Step 2: Run Views After Pipeline

Views can be created:
- Manually after pipeline runs
- In the daily_aggregations.py notebook
- In a separate job task

---

## Adding Tables to Delta Share

### Pattern for Adding Tables

```sql
-- 1. Add the table to the share
ALTER SHARE workspace_monitoring_share 
ADD TABLE ${catalog}.${schema}.new_table_name;

-- 2. Verify it was added
SHOW ALL IN SHARE workspace_monitoring_share;

-- 3. On recipient side, refresh the catalog
-- (Data appears automatically, but metadata may need refresh)
```

### Pattern for Adding Views

```sql
-- Views can also be shared (Databricks-to-Databricks only)
ALTER SHARE workspace_monitoring_share 
ADD VIEW ${catalog}.${schema}.v_my_view;
```

---

## Adding New System Table Sources

### Available System Table Schemas

| Schema | Tables | Status |
|--------|--------|--------|
| `system.billing` | usage, list_prices | GA |
| `system.access` | audit, table_lineage, column_lineage | Public Preview |
| `system.compute` | clusters, node_types, node_timeline, warehouse_events | GA/Preview |
| `system.lakeflow` | jobs, job_run_timeline, job_task_run_timeline, pipelines, pipeline_update_timeline | GA |
| `system.query` | history | Preview |
| `system.serving` | served_entities, endpoint_usage | Preview |
| `system.mlflow` | experiments_latest, runs_latest, run_metrics_history | Preview |
| `system.access` | workspaces_latest, audit, table_lineage, column_lineage | GA/Preview |
| `system.marketplace` | listing_access_events, listing_funnel_events | Preview |

### Pattern for Streaming Tables

```python
@dlt.table(name="my_streaming_table")
def my_streaming_table():
    return (
        spark.readStream
        .option("skipChangeCommits", "true")  # REQUIRED for system tables
        .option("maxFilesPerTrigger", 1000)   # Control batch size
        .table("system.schema.table_name")
        .withColumn("_account_identifier", F.lit(account_identifier))
        .withColumn("_workspace_identifier", F.lit(workspace_identifier))
        .withColumn("_ingested_at", F.current_timestamp())
    )
```

### Pattern for SCD2 Tables (Slowly Changing Dimension)

```python
@dlt.table(name="my_current_state")
def my_current_state():
    """Get current state from SCD2 table"""
    source = dlt.read("my_scd2_bronze")
    
    window_spec = Window.partitionBy("workspace_id", "entity_id").orderBy(F.desc("change_time"))
    
    return (
        source
        .withColumn("rn", F.row_number().over(window_spec))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .filter(F.col("delete_time").isNull())  # Exclude deleted
    )
```

### Pattern for Batch/Snapshot Tables

```python
@dlt.table(name="my_snapshot_table")
def my_snapshot_table():
    """For tables that don't support streaming"""
    return (
        spark.table("system.schema.table_name")
        .withColumn("_account_identifier", F.lit(account_identifier))
        .withColumn("_snapshot_time", F.current_timestamp())
    )
```

---

## Common Patterns

### 1. Adding Cost Calculation

```python
# Join with list_prices to get USD costs
def add_cost_calculation(usage_df, prices_df):
    return (
        usage_df
        .join(
            prices_df.select(
                "sku_name", "cloud",
                F.col("pricing.default").alias("price_per_unit"),
                "price_start_time", "price_end_time"
            ),
            on=(
                (usage_df.sku_name == prices_df.sku_name) &
                (usage_df.cloud == prices_df.cloud) &
                (usage_df.usage_start_time >= prices_df.price_start_time) &
                ((prices_df.price_end_time.isNull()) | 
                 (usage_df.usage_end_time <= prices_df.price_end_time))
            ),
            how="left"
        )
        .withColumn("estimated_cost_usd", 
            F.col("usage_quantity") * F.coalesce(F.col("price_per_unit"), F.lit(0))
        )
    )
```

### 2. Filtering by Product

```python
# Common billing_origin_product values
PRODUCT_FILTERS = {
    "jobs": "JOBS",
    "sql": "SQL",
    "dlt": "DLT",
    "serving": "MODEL_SERVING",
    "interactive": "INTERACTIVE",
    "vector_search": "VECTOR_SEARCH",
    "online_tables": "ONLINE_TABLES",
    "monitoring": "LAKEHOUSE_MONITORING"
}

# Usage
jobs_usage = billing_summary.filter(
    F.col("billing_origin_product") == PRODUCT_FILTERS["jobs"]
)
```

### 3. Time-Based Aggregations

```python
# Daily aggregation
daily = df.groupBy(
    "_account_identifier",
    "_workspace_identifier", 
    "workspace_id",
    "usage_date"
).agg(...)

# Weekly aggregation
weekly = df.groupBy(
    "_account_identifier",
    "_workspace_identifier",
    "workspace_id",
    F.date_trunc("week", "usage_date").alias("week_start")
).agg(...)

# Monthly aggregation  
monthly = df.groupBy(
    "_account_identifier",
    "_workspace_identifier",
    "workspace_id",
    F.date_trunc("month", "usage_date").alias("month_start")
).agg(...)
```

### 4. Cross-Workspace Identifier Pattern

Always include these columns for cross-workspace aggregation:

```python
.withColumn("_account_identifier", F.lit(account_identifier))
.withColumn("_workspace_identifier", F.lit(workspace_identifier))
```

---

## Available System Tables Reference

### Billing Tables
- `system.billing.usage` - All billable usage (Global, 365 days)
- `system.billing.list_prices` - SKU pricing history (Global, Indefinite)

### Access/Audit Tables
- `system.access.audit` - Audit events (Regional, 365 days)
- `system.access.table_lineage` - Table read/write events (Regional, 365 days)
- `system.access.column_lineage` - Column-level access (Regional, 365 days)

### Compute Tables
- `system.compute.clusters` - Cluster configs SCD2 (Regional, 365 days)
- `system.compute.node_types` - Available node types (Regional, Indefinite)
- `system.compute.node_timeline` - Node utilization (Regional, 90 days)
- `system.compute.warehouse_events` - SQL warehouse events (Regional, 365 days)

### Jobs/Pipelines Tables
- `system.lakeflow.jobs` - Job definitions SCD2 (Regional, 365 days)
- `system.lakeflow.job_run_timeline` - Job runs (Regional, 365 days)
- `system.lakeflow.job_task_run_timeline` - Task runs (Regional, 365 days)
- `system.lakeflow.pipelines` - Pipeline definitions SCD2 (Regional, 365 days)
- `system.lakeflow.pipeline_update_timeline` - Pipeline updates (Regional, 365 days)

### ML Tables
- `system.mlflow.experiments_latest` - MLflow experiments (Regional, data from Sept 2025+)
- `system.mlflow.runs_latest` - MLflow runs (Regional, data from Sept 2025+)
- `system.mlflow.run_metrics_history` - Training metrics (Regional, data from Sept 2025+)

### Serving Tables
- `system.serving.served_entities` - Served model entities SCD2 (Regional, 365 days)
- `system.serving.endpoint_usage` - Endpoint token usage (Regional, 90 days)

### Query Tables
- `system.query.history` - SQL query history (Regional, 365 days)

### Workspace Metadata
- `system.access.workspaces_latest` - Workspace metadata (Global, Indefinite)

---

## Checklist for Adding New Data

- [ ] Identify the source system table
- [ ] Check if it supports streaming (`skipChangeCommits`)
- [ ] Determine if it's SCD2 (needs current-state extraction)
- [ ] Add identifiers for cross-workspace tracking
- [ ] Create bronze table (raw data)
- [ ] Create silver/gold aggregations as needed
- [ ] Add to Delta Share
- [ ] Update consolidated views on recipient workspace
- [ ] Document in README.md
