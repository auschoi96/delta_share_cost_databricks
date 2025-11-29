# Extending the Monitoring Solution

## Adding a New DLT Table

### 1. Create the Table Definition

Add to an existing DLT notebook or create a new one in `src/`:

```python
import dlt
from pyspark.sql import functions as F

account_identifier = spark.conf.get("account_identifier", "unknown")
workspace_identifier = spark.conf.get("workspace_identifier", "unknown")

@dlt.table(
    name="my_new_table",
    comment="Description of the table"
)
def my_new_table():
    return (
        spark.readStream
        .option("skipChangeCommits", "true")  # Required for system tables
        .table("system.schema.source_table")
        .withColumn("_account_identifier", F.lit(account_identifier))
        .withColumn("_workspace_identifier", F.lit(workspace_identifier))
    )
```

### 2. Add to Pipeline (if new notebook)

Edit `resources/pipeline.yml`:

```yaml
libraries:
  - notebook:
      path: ../src/dlt_monitoring_pipeline.py
  - notebook:
      path: ../src/my_new_notebook.py  # Add here
```

### 3. Add to Delta Share

```sql
ALTER SHARE workspace_monitoring_share
ADD TABLE ${catalog}.${schema}.my_new_table;
```

## Common Patterns

### Streaming Table (for tables that support streaming)

```python
@dlt.table(name="my_streaming_table")
def my_streaming_table():
    return (
        spark.readStream
        .option("skipChangeCommits", "true")
        .table("system.schema.table_name")
        .withColumn("_account_identifier", F.lit(account_identifier))
        .withColumn("_workspace_identifier", F.lit(workspace_identifier))
    )
```

### Snapshot Table (for tables without streaming support)

```python
@dlt.table(name="my_snapshot_table")
def my_snapshot_table():
    return (
        spark.table("system.schema.table_name")
        .withColumn("_account_identifier", F.lit(account_identifier))
        .withColumn("_snapshot_time", F.current_timestamp())
    )
```

### SCD2 Current State (get latest record per entity)

```python
from pyspark.sql.window import Window

@dlt.table(name="entities_current")
def entities_current():
    source = dlt.read("entities_bronze")

    window = Window.partitionBy("workspace_id", "entity_id").orderBy(F.desc("change_time"))

    return (
        source
        .withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .filter(F.col("delete_time").isNull())
    )
```

### Aggregation Table

```python
@dlt.table(name="daily_summary")
def daily_summary():
    return (
        dlt.read("source_table")
        .groupBy(
            "_account_identifier",
            "_workspace_identifier",
            "workspace_id",
            F.date_trunc("day", "event_time").alias("event_date")
        )
        .agg(
            F.count("*").alias("total_count"),
            F.sum("amount").alias("total_amount")
        )
    )
```

## Available System Tables

| Schema | Key Tables | Supports Streaming |
|--------|-----------|-------------------|
| `system.billing` | usage, list_prices | Yes |
| `system.access` | audit, table_lineage | Yes |
| `system.compute` | clusters, node_timeline | Yes |
| `system.lakeflow` | jobs, job_run_timeline | Yes |
| `system.query` | history | Yes |
| `system.serving` | served_entities, endpoint_usage | Yes |
| `system.mlflow` | experiments_latest, runs_latest | No (use snapshot) |

## Checklist

- [ ] Add `_account_identifier` and `_workspace_identifier` columns
- [ ] Use `skipChangeCommits` option for streaming
- [ ] Add table to Delta Share if needed
- [ ] Redeploy bundle: `databricks bundle deploy -t dev`
