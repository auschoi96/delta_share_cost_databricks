# Databricks notebook source
# MAGIC %md
# MAGIC # Daily Aggregations & Maintenance
# MAGIC 
# MAGIC This notebook runs daily to:
# MAGIC 1. Create weekly/monthly rollup tables
# MAGIC 2. Generate trend analysis
# MAGIC 3. Perform maintenance tasks (optimize, vacuum)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Get parameters from job (defaults match databricks.yml dev target)
dbutils.widgets.text("monitoring_catalog", "demo_catalog")
dbutils.widgets.text("monitoring_schema", "demo_data_example")
dbutils.widgets.text("account_identifier", "unknown_account")
dbutils.widgets.text("workspace_identifier", "unknown_workspace")

monitoring_catalog = dbutils.widgets.get("monitoring_catalog")
monitoring_schema = dbutils.widgets.get("monitoring_schema")
account_identifier = dbutils.widgets.get("account_identifier")
workspace_identifier = dbutils.widgets.get("workspace_identifier")

full_schema = f"{monitoring_catalog}.{monitoring_schema}"

print(f"Working with schema: {full_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Weekly Cost Rollup

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {full_schema}.cost_weekly_rollup AS
SELECT
    _account_identifier,
    _workspace_identifier,
    workspace_id,
    DATE_TRUNC('week', usage_date) AS week_start,
    billing_origin_product,
    SUM(total_dbus) AS total_dbus,
    SUM(total_cost_usd) AS total_cost_usd,
    SUM(record_count) AS record_count
FROM {full_schema}.cost_by_product
GROUP BY ALL
""")

print("Weekly cost rollup created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Monthly Cost Rollup

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {full_schema}.cost_monthly_rollup AS
SELECT
    _account_identifier,
    _workspace_identifier,
    workspace_id,
    DATE_TRUNC('month', usage_date) AS month_start,
    billing_origin_product,
    SUM(total_dbus) AS total_dbus,
    SUM(total_cost_usd) AS total_cost_usd,
    SUM(record_count) AS record_count
FROM {full_schema}.cost_by_product
GROUP BY ALL
""")

print("Monthly cost rollup created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Job Performance Trends

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {full_schema}.job_performance_trends AS
WITH daily_metrics AS (
    SELECT
        _account_identifier,
        _workspace_identifier,
        workspace_id,
        job_id,
        job_name,
        run_date,
        total_runs,
        successful_runs,
        failed_runs,
        avg_duration_seconds,
        p95_duration_seconds,
        ROUND(successful_runs * 100.0 / NULLIF(total_runs, 0), 2) AS success_rate_pct
    FROM {full_schema}.job_runs_summary
    WHERE run_date >= CURRENT_DATE - INTERVAL 30 DAYS
)
SELECT
    *,
    -- 7-day moving averages
    AVG(avg_duration_seconds) OVER (
        PARTITION BY workspace_id, job_id 
        ORDER BY run_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS ma7_avg_duration,
    AVG(success_rate_pct) OVER (
        PARTITION BY workspace_id, job_id 
        ORDER BY run_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS ma7_success_rate
FROM daily_metrics
""")

print("Job performance trends created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Cost Anomaly Detection

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {full_schema}.cost_anomalies AS
WITH daily_costs AS (
    SELECT
        _account_identifier,
        _workspace_identifier,
        workspace_id,
        usage_date,
        SUM(total_cost_usd) AS daily_cost
    FROM {full_schema}.cost_by_product
    WHERE usage_date >= CURRENT_DATE - INTERVAL 60 DAYS
    GROUP BY ALL
),
with_stats AS (
    SELECT
        *,
        AVG(daily_cost) OVER (
            PARTITION BY workspace_id 
            ORDER BY usage_date 
            ROWS BETWEEN 29 PRECEDING AND 1 PRECEDING
        ) AS ma30_cost,
        STDDEV(daily_cost) OVER (
            PARTITION BY workspace_id 
            ORDER BY usage_date 
            ROWS BETWEEN 29 PRECEDING AND 1 PRECEDING
        ) AS stddev30_cost
    FROM daily_costs
)
SELECT
    *,
    ROUND((daily_cost - ma30_cost) / NULLIF(stddev30_cost, 0), 2) AS z_score,
    CASE
        WHEN (daily_cost - ma30_cost) / NULLIF(stddev30_cost, 0) > 2 THEN 'HIGH_SPEND'
        WHEN (daily_cost - ma30_cost) / NULLIF(stddev30_cost, 0) < -2 THEN 'LOW_SPEND'
        ELSE 'NORMAL'
    END AS anomaly_type
FROM with_stats
WHERE ma30_cost IS NOT NULL
""")

print("Cost anomalies table created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Top Consumers Report

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {full_schema}.top_consumers_30d AS
WITH job_costs AS (
    SELECT
        _account_identifier,
        _workspace_identifier,
        workspace_id,
        job_id,
        job_name,
        run_as,
        SUM(total_dbus) AS total_dbus,
        SUM(total_cost_usd) AS total_cost_usd,
        SUM(run_count) AS total_runs
    FROM {full_schema}.cost_by_job
    WHERE usage_date >= CURRENT_DATE - INTERVAL 30 DAYS
    GROUP BY ALL
),
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY workspace_id 
            ORDER BY total_cost_usd DESC
        ) AS cost_rank
    FROM job_costs
)
SELECT * FROM ranked WHERE cost_rank <= 50
""")

print("Top consumers report created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. User Activity Metrics

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {full_schema}.user_activity_30d AS
SELECT
    _account_identifier,
    _workspace_identifier,
    workspace_id,
    user_email,
    COUNT(DISTINCT event_date) AS active_days,
    SUM(event_count) AS total_events,
    MIN(first_activity) AS first_seen,
    MAX(last_activity) AS last_seen,
    COLLECT_SET(service_name) AS services_used
FROM {full_schema}.user_activity_summary
WHERE event_date >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY ALL
""")

print("User activity metrics created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Optimize Tables

# COMMAND ----------

tables_to_optimize = [
    "billing_summary",
    "cost_by_product",
    "cost_by_sku",
    "cost_by_job",
    "job_runs_summary",
    "audit_logs_bronze"
]

for table in tables_to_optimize:
    try:
        spark.sql(f"OPTIMIZE {full_schema}.{table}")
        print(f"Optimized {table}")
    except Exception as e:
        print(f"Could not optimize {table}: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Summary

# COMMAND ----------

print(f"""
Daily Aggregation Complete!
===========================
Schema: {full_schema}
Account: {account_identifier}
Workspace: {workspace_identifier}

Tables Updated:
- cost_weekly_rollup
- cost_monthly_rollup
- job_performance_trends
- cost_anomalies
- top_consumers_30d
- user_activity_30d

Tables Optimized:
- {', '.join(tables_to_optimize)}
""")
