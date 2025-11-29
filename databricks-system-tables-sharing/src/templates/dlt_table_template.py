# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Table Template
# MAGIC 
# MAGIC Use this template to add new tables to the monitoring solution.
# MAGIC 
# MAGIC ## Instructions:
# MAGIC 1. Copy this file and rename it (e.g., `dlt_my_feature.py`)
# MAGIC 2. Update the table definitions below
# MAGIC 3. Add the notebook to `resources/pipeline.yml`
# MAGIC 4. Deploy with `databricks bundle deploy`

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC These variables are passed from the pipeline configuration.

# COMMAND ----------

# Get configuration from pipeline
account_identifier = spark.conf.get("account_identifier", "unknown_account")
workspace_identifier = spark.conf.get("workspace_identifier", "unknown_workspace")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Template: Streaming Bronze Table
# MAGIC Use for tables that support streaming (check docs for `Supports streaming: Yes`)

# COMMAND ----------

@dlt.table(
    name="template_bronze_streaming",
    comment="TEMPLATE: Replace with your table description",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "usage_date,workspace_id"  # Adjust columns
    }
)
def template_bronze_streaming():
    """
    TEMPLATE: Stream raw data from a system table.

    Replace 'system.schema.table_name' with your source table.
    """
    return (
        spark.readStream
        .option("skipChangeCommits", "true")  # REQUIRED for system tables
        .option("maxFilesPerTrigger", 1000)  # Adjust batch size as needed
        .table("system.schema.table_name")  # <-- REPLACE THIS
        # Add cross-workspace identifiers
        .withColumn("_account_identifier", F.lit(account_identifier))
        .withColumn("_workspace_identifier", F.lit(workspace_identifier))
        .withColumn("_ingested_at", F.current_timestamp())
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Template: Batch/Snapshot Table
# MAGIC Use for tables that DON'T support streaming

# COMMAND ----------

@dlt.table(
    name="template_snapshot",
    comment="TEMPLATE: Replace with your table description"
)
def template_snapshot():
    """
    TEMPLATE: Load a full snapshot from a system table.

    Use this for tables that don't support streaming.
    """
    return (
        spark.table("system.schema.table_name")  # <-- REPLACE THIS
        .withColumn("_account_identifier", F.lit(account_identifier))
        .withColumn("_workspace_identifier", F.lit(workspace_identifier))
        .withColumn("_snapshot_time", F.current_timestamp())
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Template: SCD2 Current State
# MAGIC Use for Slowly Changing Dimension tables to get the latest version of each record

# COMMAND ----------

@dlt.table(
    name="template_current_state",
    comment="TEMPLATE: Current state from SCD2 table"
)
def template_current_state():
    """
    TEMPLATE: Extract current state from an SCD2 table.

    SCD2 tables include: clusters, jobs, pipelines, served_entities
    """
    source = dlt.read("template_bronze_streaming")  # <-- Reference your bronze table

    # Define the window to get the latest record
    window_spec = Window.partitionBy(
        "workspace_id",
        "entity_id"  # <-- REPLACE with your unique identifier column
    ).orderBy(F.desc("change_time"))  # <-- REPLACE with your timestamp column

    return (
        source
        .withColumn("_row_number", F.row_number().over(window_spec))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
        .filter(F.col("delete_time").isNull())  # Exclude deleted records
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Template: Aggregated Silver/Gold Table
# MAGIC Use for creating aggregations from raw data

# COMMAND ----------

@dlt.table(
    name="template_aggregated",
    comment="TEMPLATE: Aggregated metrics",
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true"  # Enable for downstream streaming
    }
)
def template_aggregated():
    """
    TEMPLATE: Create aggregated metrics from source data.
    """
    source = dlt.read("template_bronze_streaming")  # <-- Reference your source

    return (
        source
        .groupBy(
            "_account_identifier",
            "_workspace_identifier",
            "workspace_id",
            F.date_trunc("day", "timestamp_column").alias("date")  # <-- Adjust
            # Add other grouping columns
        )
        .agg(
            F.count("*").alias("record_count"),
            F.sum("metric_column").alias("total_metric"),  # <-- Adjust
            F.avg("metric_column").alias("avg_metric"),
            F.max("timestamp_column").alias("last_seen")
        )
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Template: Join with Billing for Cost Calculation
# MAGIC Use when you need to add cost information

# COMMAND ----------

@dlt.table(
    name="template_with_costs",
    comment="TEMPLATE: Data enriched with cost information"
)
def template_with_costs():
    """
    TEMPLATE: Join your data with billing to add cost calculations.
    """
    # Read your source data
    source = dlt.read("template_bronze_streaming")

    # Read billing summary (assumes billing_summary table exists)
    billing = dlt.read("billing_summary")

    return (
        source
        .join(
            billing.select(
                "workspace_id",
                "usage_date",
                "job_id",  # <-- Adjust join keys
                "estimated_cost_usd"
            ),
            on=["workspace_id", "job_id"],  # <-- Adjust join keys
            how="left"
        )
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Template: Filtered View
# MAGIC Use for creating focused subsets of data

# COMMAND ----------

@dlt.view(
    name="template_filtered_view",
    comment="TEMPLATE: Filtered view for specific use case"
)
def template_filtered_view():
    """
    TEMPLATE: Create a view that filters or transforms source data.

    Views are lightweight and don't store data.
    Good for: filters, simple transforms, joins for ad-hoc queries
    """
    return (
        dlt.read("template_bronze_streaming")
        .filter(F.col("some_column") == "some_value")  # <-- Adjust filter
        .select(
            "_account_identifier",
            "_workspace_identifier",
            "workspace_id",
            # Select only needed columns
        )
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Common Helper Functions

# COMMAND ----------

def add_workspace_identifiers(df):
    """Add standard cross-workspace tracking columns."""
    return (
        df
        .withColumn("_account_identifier", F.lit(account_identifier))
        .withColumn("_workspace_identifier", F.lit(workspace_identifier))
    )


def extract_current_scd2(df, partition_cols, order_col="change_time", delete_col="delete_time"):
    """Extract current state from SCD2 table."""
    window_spec = Window.partitionBy(*partition_cols).orderBy(F.desc(order_col))

    return (
        df
        .withColumn("_rn", F.row_number().over(window_spec))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .filter(F.col(delete_col).isNull())
    )


def add_time_dimensions(df, timestamp_col):
    """Add common time dimension columns."""
    return (
        df
        .withColumn("date", F.to_date(timestamp_col))
        .withColumn("hour", F.hour(timestamp_col))
        .withColumn("day_of_week", F.dayofweek(timestamp_col))
        .withColumn("week_start", F.date_trunc("week", timestamp_col))
        .withColumn("month_start", F.date_trunc("month", timestamp_col))
    )
