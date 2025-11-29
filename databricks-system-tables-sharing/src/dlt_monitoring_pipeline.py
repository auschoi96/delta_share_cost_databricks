# Databricks notebook source
# MAGIC %md
# MAGIC # System Tables Monitoring Pipeline
# MAGIC 
# MAGIC This DLT pipeline creates streaming tables and views from Databricks system tables
# MAGIC for comprehensive usage, billing, and activity monitoring.
# MAGIC 
# MAGIC ## Tables Created:
# MAGIC - **Streaming Tables**: Real-time data from system tables
# MAGIC - **Materialized Views**: Aggregated data for dashboards and reporting
# MAGIC - **Views**: Lightweight joins for ad-hoc analysis

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Get pipeline configuration parameters
account_identifier = spark.conf.get("account_identifier", "unknown_account")
workspace_identifier = spark.conf.get("workspace_identifier", "unknown_workspace")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Billing & Usage Tables
# MAGIC
# MAGIC These tables capture all billable usage for cost tracking and attribution.

# COMMAND ----------

@dlt.table(
    name="billing_usage_bronze",
    comment="Raw billing usage data streamed from system.billing.usage",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "usage_date,workspace_id"
    }
)
def billing_usage_bronze():
    """
    Stream raw billing data from system tables.
    Includes workspace and account identifiers for cross-workspace aggregation.
    """
    return (
        spark.readStream
        .option("skipChangeCommits", "true")
        .option("maxFilesPerTrigger", 1000)
        .table("system.billing.usage")
        .withColumn("_account_identifier", F.lit(account_identifier))
        .withColumn("_workspace_identifier", F.lit(workspace_identifier))
        .withColumn("_ingested_at", F.current_timestamp())
    )


# COMMAND ----------

@dlt.table(
    name="list_prices",
    comment="SKU pricing reference table from system.billing.list_prices"
)
def list_prices():
    """
    Reference table for SKU pricing. Used to calculate costs from DBU usage.
    """
    return (
        spark.table("system.billing.list_prices")
        .withColumn("_account_identifier", F.lit(account_identifier))
    )


# COMMAND ----------

@dlt.table(
    name="billing_summary",
    comment="Aggregated billing summary with calculated costs - ready for Delta Sharing",
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true"
    }
)
def billing_summary():
    """
    Gold-level billing summary with calculated costs.
    Joins usage with list prices to compute dollar amounts.
    Designed for Delta Sharing to central monitoring workspace.
    """
    usage = dlt.read("billing_usage_bronze")
    prices = dlt.read("list_prices")

    # Filter for ORIGINAL records only (exclude corrections for simplicity)
    usage_filtered = usage.filter(F.col("record_type") == "ORIGINAL")

    # Prepare prices with aliased columns to avoid ambiguity in join
    prices_prepared = (
        prices
        .select(
            F.col("sku_name").alias("price_sku_name"),
            F.col("cloud").alias("price_cloud"),
            F.col("pricing.default").alias("price_per_unit"),
            F.col("price_start_time"),
            F.col("price_end_time")
        )
    )

    # Use DataFrame aliases for clean join syntax
    u = usage_filtered.alias("u")
    p = prices_prepared.alias("p")

    return (
        u.join(
            p,
            on=(
                    (F.col("u.sku_name") == F.col("p.price_sku_name")) &
                    (F.col("u.cloud") == F.col("p.price_cloud")) &
                    (F.col("u.usage_start_time") >= F.col("p.price_start_time")) &
                    (F.col("p.price_end_time").isNull() |
                     (F.col("u.usage_end_time") <= F.col("p.price_end_time")))
            ),
            how="left"
        )
        .withColumn("estimated_cost_usd",
                    F.col("usage_quantity") * F.coalesce(F.col("price_per_unit"), F.lit(0))
                    )
        .select(
            # Identifiers for cross-workspace tracking
            "_account_identifier",
            "_workspace_identifier",
            "account_id",
            "workspace_id",

            # Time dimensions
            "usage_date",
            "usage_start_time",
            "usage_end_time",

            # Usage details
            "sku_name",
            "cloud",
            "billing_origin_product",
            "usage_quantity",
            "usage_unit",
            "estimated_cost_usd",

            # Attribution
            "usage_metadata.job_id",
            "usage_metadata.job_name",
            "usage_metadata.cluster_id",
            "usage_metadata.warehouse_id",
            "usage_metadata.notebook_id",
            "usage_metadata.notebook_path",
            "usage_metadata.dlt_pipeline_id",
            "usage_metadata.endpoint_name",
            "identity_metadata.run_as",

            # Product features
            "product_features.is_serverless",
            "product_features.is_photon",
            "product_features.jobs_tier",
            "product_features.sql_tier",

            # Tags for custom attribution
            "custom_tags",

            "_ingested_at"
        )
    )


# COMMAND ----------

@dlt.table(
    name="cost_by_product",
    comment="Daily costs aggregated by product - for executive dashboards"
)
def cost_by_product():
    """
    Daily cost breakdown by billing origin product.
    Perfect for high-level spend tracking dashboards.
    """
    return (
        dlt.read("billing_summary")
        .groupBy(
            "_account_identifier",
            "_workspace_identifier",
            "workspace_id",
            "usage_date",
            "billing_origin_product",
            "is_serverless"
        )
        .agg(
            F.sum("usage_quantity").alias("total_dbus"),
            F.sum("estimated_cost_usd").alias("total_cost_usd"),
            F.count("*").alias("record_count")
        )
    )


# COMMAND ----------

@dlt.table(
    name="cost_by_sku",
    comment="Daily costs aggregated by SKU - for detailed cost analysis"
)
def cost_by_sku():
    """
    Daily cost breakdown by SKU.
    Useful for understanding specific resource consumption patterns.
    """
    return (
        dlt.read("billing_summary")
        .groupBy(
            "_account_identifier",
            "_workspace_identifier",
            "workspace_id",
            "usage_date",
            "sku_name",
            "cloud",
            "usage_unit"
        )
        .agg(
            F.sum("usage_quantity").alias("total_usage"),
            F.sum("estimated_cost_usd").alias("total_cost_usd"),
            F.count("*").alias("record_count")
        )
    )


# COMMAND ----------

@dlt.table(
    name="cost_by_job",
    comment="Costs aggregated by job - for job cost attribution"
)
def cost_by_job():
    """
    Cost attribution by job.
    Enables team chargebacks and cost optimization by identifying expensive jobs.
    """
    return (
        dlt.read("billing_summary")
        .filter(F.col("job_id").isNotNull())
        .groupBy(
            "_account_identifier",
            "_workspace_identifier",
            "workspace_id",
            "usage_date",
            "job_id",
            "job_name",
            "run_as",
            "billing_origin_product"
        )
        .agg(
            F.sum("usage_quantity").alias("total_dbus"),
            F.sum("estimated_cost_usd").alias("total_cost_usd"),
            F.count("*").alias("run_count")
        )
    )


# COMMAND ----------

@dlt.table(
    name="cost_by_user",
    comment="Costs aggregated by user identity - for user-level attribution"
)
def cost_by_user():
    """
    Cost attribution by user (run_as identity).
    Useful for departmental chargebacks.
    """
    return (
        dlt.read("billing_summary")
        .filter(F.col("run_as").isNotNull())
        .groupBy(
            "_account_identifier",
            "_workspace_identifier",
            "workspace_id",
            "usage_date",
            "run_as",
            "billing_origin_product"
        )
        .agg(
            F.sum("usage_quantity").alias("total_dbus"),
            F.sum("estimated_cost_usd").alias("total_cost_usd"),
            F.countDistinct("job_id").alias("distinct_jobs"),
            F.count("*").alias("record_count")
        )
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Jobs Monitoring Tables

# COMMAND ----------

@dlt.table(
    name="jobs_bronze",
    comment="Raw job definitions from system.lakeflow.jobs (SCD2)",
    table_properties={"quality": "bronze"}
)
def jobs_bronze():
    """
    Stream job definitions. This is an SCD2 table, so we track history.
    """
    return (
        spark.readStream
        .option("skipChangeCommits", "true")
        .table("system.lakeflow.jobs")
        .withColumn("_account_identifier", F.lit(account_identifier))
        .withColumn("_workspace_identifier", F.lit(workspace_identifier))
    )


# COMMAND ----------

@dlt.table(
    name="job_runs_bronze",
    comment="Raw job run timeline from system.lakeflow.job_run_timeline",
    table_properties={"quality": "bronze"}
)
def job_runs_bronze():
    """
    Stream job run events for execution tracking.
    Tracks the start and end times of job runs.

    Note: For runs longer than 1 hour, multiple rows are emitted with the same run_id,
    each representing up to 1 hour of runtime (period_start_time to period_end_time).
    """
    return (
        spark.readStream
        .option("skipChangeCommits", "true")
        .table("system.lakeflow.job_run_timeline")
        .withColumn("_account_identifier", F.lit(account_identifier))
        .withColumn("_workspace_identifier", F.lit(workspace_identifier))
    )


# COMMAND ----------

@dlt.table(
    name="jobs_current",
    comment="Current state of all jobs (latest version only)"
)
def jobs_current():
    """
    Get the most recent version of each job (SCD2 current state).
    """
    jobs = dlt.read("jobs_bronze")

    window_spec = Window.partitionBy("workspace_id", "job_id").orderBy(F.desc("change_time"))

    return (
        jobs
        .withColumn("rn", F.row_number().over(window_spec))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .filter(F.col("delete_time").isNull())  # Exclude deleted jobs
    )


# COMMAND ----------

@dlt.table(
    name="job_runs_summary",
    comment="Job run statistics - for SLA and reliability monitoring"
)
def job_runs_summary():
    """
    Aggregated job run statistics for monitoring job health and SLAs.
    """
    runs = dlt.read("job_runs_bronze").filter(F.col("run_type") == "JOB_RUN")
    jobs = dlt.read("jobs_current")

    # Calculate run duration in seconds
    runs_with_duration = runs.withColumn(
        "duration_seconds",
        F.unix_timestamp("period_end_time") - F.unix_timestamp("period_start_time")
    )

    return (
        runs_with_duration
        .groupBy(
            "_account_identifier",
            "_workspace_identifier",
            "workspace_id",
            "job_id",
            F.date_trunc("day", "period_start_time").alias("run_date")
        )
        .agg(
            F.count("*").alias("total_runs"),
            F.sum(F.when(F.col("result_state") == "SUCCEEDED", 1).otherwise(0)).alias("successful_runs"),
            F.sum(F.when(F.col("result_state") == "FAILED", 1).otherwise(0)).alias("failed_runs"),
            F.sum(F.when(F.col("result_state") == "CANCELED", 1).otherwise(0)).alias("canceled_runs"),
            F.avg("duration_seconds").alias("avg_duration_seconds"),
            F.max("duration_seconds").alias("max_duration_seconds"),
            F.min("duration_seconds").alias("min_duration_seconds"),
            F.expr("percentile_approx(duration_seconds, 0.95)").alias("p95_duration_seconds")
        )
        .join(
            jobs.select("workspace_id", "job_id", "name").alias("job_names"),
            on=["workspace_id", "job_id"],
            how="left"
        )
        .withColumnRenamed("name", "job_name")
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Compute & Cluster Tables

# COMMAND ----------

@dlt.table(
    name="clusters_bronze",
    comment="Raw cluster configurations from system.compute.clusters (SCD2)",
    table_properties={"quality": "bronze"}
)
def clusters_bronze():
    """
    Stream cluster configuration history.
    """
    return (
        spark.readStream
        .option("skipChangeCommits", "true")
        .table("system.compute.clusters")
        .withColumn("_account_identifier", F.lit(account_identifier))
        .withColumn("_workspace_identifier", F.lit(workspace_identifier))
    )


# COMMAND ----------

@dlt.table(
    name="clusters_current",
    comment="Current state of all clusters"
)
def clusters_current():
    """
    Get the most recent version of each cluster.
    """
    clusters = dlt.read("clusters_bronze")

    window_spec = Window.partitionBy("workspace_id", "cluster_id").orderBy(F.desc("change_time"))

    return (
        clusters
        .withColumn("rn", F.row_number().over(window_spec))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )


# COMMAND ----------

@dlt.table(
    name="cluster_utilization",
    comment="Cluster resource utilization from node_timeline - for rightsizing analysis"
)
def cluster_utilization():
    """
    Aggregate cluster utilization metrics for capacity planning and rightsizing.
    Note: node_timeline has 90-day retention, so this table won't have older data.
    """
    return (
        spark.table("system.compute.node_timeline")
        .withColumn("_account_identifier", F.lit(account_identifier))
        .withColumn("_workspace_identifier", F.lit(workspace_identifier))
        .groupBy(
            "_account_identifier",
            "_workspace_identifier",
            "cluster_id",
            F.date_trunc("hour", "start_time").alias("hour")
        )
        .agg(
            F.avg(F.col("cpu_user_percent") + F.col("cpu_system_percent")).alias("avg_cpu_percent"),
            F.max(F.col("cpu_user_percent") + F.col("cpu_system_percent")).alias("max_cpu_percent"),
            F.avg("mem_used_percent").alias("avg_memory_percent"),
            F.max("mem_used_percent").alias("max_memory_percent"),
            F.sum("network_received_bytes").alias("total_network_in_bytes"),
            F.sum("network_sent_bytes").alias("total_network_out_bytes"),
            F.count("*").alias("sample_count")
        )
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Audit & Activity Tables

# COMMAND ----------

@dlt.table(
    name="audit_logs_bronze",
    comment="Raw audit logs from system.access.audit",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "event_date,service_name"
    }
)
def audit_logs_bronze():
    """
    Stream audit events for security and compliance monitoring.
    """
    return (
        spark.readStream
        .option("skipChangeCommits", "true")
        .option("maxFilesPerTrigger", 1000)
        .table("system.access.audit")
        .withColumn("_account_identifier", F.lit(account_identifier))
        .withColumn("_workspace_identifier", F.lit(workspace_identifier))
    )


# COMMAND ----------

@dlt.table(
    name="user_activity_summary",
    comment="Daily user activity summary - for usage and adoption tracking"
)
def user_activity_summary():
    """
    Aggregate user activity by day for adoption and usage analytics.
    """
    return (
        dlt.read("audit_logs_bronze")
        .filter(F.col("user_identity.email").isNotNull())
        .groupBy(
            "_account_identifier",
            "_workspace_identifier",
            "workspace_id",
            "event_date",
            F.col("user_identity.email").alias("user_email"),
            "service_name"
        )
        .agg(
            F.count("*").alias("event_count"),
            F.countDistinct("action_name").alias("distinct_actions"),
            F.min("event_time").alias("first_activity"),
            F.max("event_time").alias("last_activity")
        )
    )


# COMMAND ----------

@dlt.table(
    name="data_access_summary",
    comment="Table access patterns from audit logs - for data governance"
)
def data_access_summary():
    """
    Track Unity Catalog table access patterns.
    """
    return (
        dlt.read("audit_logs_bronze")
        .filter(F.col("service_name") == "unityCatalog")
        .filter(F.col("action_name").isin(["getTable", "createTable", "deleteTable"]))
        .groupBy(
            "_account_identifier",
            "_workspace_identifier",
            "workspace_id",
            "event_date",
            F.col("user_identity.email").alias("user_email"),
            "action_name",
            F.col("request_params.full_name_arg").alias("table_full_name")
        )
        .agg(
            F.count("*").alias("access_count"),
            F.min("event_time").alias("first_access"),
            F.max("event_time").alias("last_access")
        )
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Workspace Metadata

# COMMAND ----------

@dlt.table(
    name="workspace_info",
    comment="Workspace metadata for cross-workspace joins"
)
def workspace_info():
    """
    Reference table for workspace metadata.
    Helps map workspace_id to friendly names in dashboards.

    Note: system.access.workspaces_latest is a slow-changing dimension table
    containing metadata for all workspaces in the account (Global, Indefinite retention).
    """
    return (
        spark.table("system.access.workspaces_latest")
        .withColumn("_account_identifier", F.lit(account_identifier))
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Combined Monitoring View
# MAGIC
# MAGIC This view provides a comprehensive overview suitable for executive dashboards.

# COMMAND ----------

@dlt.table(
    name="executive_summary",
    comment="Combined executive summary for quick insights"
)
def executive_summary():
    """
    High-level metrics for executive dashboards.
    Aggregates key metrics by day for quick consumption.
    """
    costs = dlt.read("cost_by_product")

    return (
        costs
        .groupBy(
            "_account_identifier",
            "_workspace_identifier",
            "workspace_id",
            "usage_date"
        )
        .agg(
            F.sum("total_dbus").alias("total_dbus"),
            F.sum("total_cost_usd").alias("total_cost_usd"),
            F.sum(F.when(F.col("billing_origin_product") == "JOBS", F.col("total_cost_usd")).otherwise(0)).alias(
                "jobs_cost_usd"),
            F.sum(F.when(F.col("billing_origin_product") == "SQL", F.col("total_cost_usd")).otherwise(0)).alias(
                "sql_cost_usd"),
            F.sum(F.when(F.col("billing_origin_product") == "DLT", F.col("total_cost_usd")).otherwise(0)).alias(
                "dlt_cost_usd"),
            F.sum(
                F.when(F.col("billing_origin_product") == "MODEL_SERVING", F.col("total_cost_usd")).otherwise(0)).alias(
                "serving_cost_usd"),
            F.sum(F.when(F.col("billing_origin_product") == "INTERACTIVE", F.col("total_cost_usd")).otherwise(0)).alias(
                "interactive_cost_usd"),
            F.sum(F.when(F.col("is_serverless") == True, F.col("total_cost_usd")).otherwise(0)).alias(
                "serverless_cost_usd"),
            F.sum(F.when(F.col("is_serverless") == False, F.col("total_cost_usd")).otherwise(0)).alias(
                "classic_cost_usd")
        )
    )
