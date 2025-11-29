# Databricks notebook source
# MAGIC %md
# MAGIC # AI/ML Workloads Monitoring Pipeline
# MAGIC 
# MAGIC This extension adds monitoring for:
# MAGIC - **Model Serving Endpoints** (including Foundation Model APIs, External Models)
# MAGIC - **MLflow** (Experiments, Runs, Metrics)
# MAGIC - **Vector Search** (Indexes, Usage)
# MAGIC 
# MAGIC ## Required System Tables:
# MAGIC - `system.serving.served_entities` (SCD2)
# MAGIC - `system.serving.endpoint_usage`
# MAGIC - `system.mlflow.experiments_latest`
# MAGIC - `system.mlflow.runs_latest`
# MAGIC - `system.mlflow.run_metrics_history`
# MAGIC - `system.billing.usage` (filtered for VECTOR_SEARCH, MODEL_SERVING)

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

account_identifier = spark.conf.get("account_identifier", "unknown_account")
workspace_identifier = spark.conf.get("workspace_identifier", "unknown_workspace")


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 1. Model Serving Monitoring
# MAGIC
# MAGIC Track model serving endpoints, foundation models, and external models.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1 Served Entities (Bronze)

# COMMAND ----------

@dlt.table(
    name="served_entities_bronze",
    comment="Raw served entity configurations from system.serving.served_entities (SCD2)",
    table_properties={"quality": "bronze"}
)
def served_entities_bronze():
    """
    Stream served entity (model) configurations.
    This is an SCD2 table tracking all models deployed to endpoints.
    """
    return (
        spark.readStream
        .option("skipChangeCommits", "true")
        .table("system.serving.served_entities")
        .withColumn("_account_identifier", F.lit(account_identifier))
        .withColumn("_workspace_identifier", F.lit(workspace_identifier))
        .withColumn("_ingested_at", F.current_timestamp())
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 Current Served Entities

# COMMAND ----------

@dlt.table(
    name="served_entities_current",
    comment="Current state of all served entities (latest version only)"
)
def served_entities_current():
    """
    Get the current configuration for each served entity.
    Uses ROW_NUMBER to get most recent record per entity.

    Actual columns from system.serving.served_entities:
    served_entity_id, account_id, workspace_id, created_by, endpoint_name, endpoint_id,
    served_entity_name, entity_type, entity_name, entity_version, endpoint_config_version,
    task, external_model_config, foundation_model_config, custom_model_config,
    feature_spec_config, change_time, endpoint_delete_time
    """
    entities = dlt.read("served_entities_bronze")

    # Window to get latest record per entity
    window_spec = Window.partitionBy(
        "workspace_id",
        "served_entity_id"
    ).orderBy(F.col("change_time").desc())

    return (
        entities
        .withColumn("_rn", F.row_number().over(window_spec))
        .filter(F.col("_rn") == 1)
        .filter(F.col("endpoint_delete_time").isNull())  # Correct column name
        .drop("_rn")
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.3 Endpoint Usage (Token Tracking)

# COMMAND ----------

@dlt.table(
    name="endpoint_usage_bronze",
    comment="Token usage per model serving endpoint from system.serving.endpoint_usage",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "served_entity_id,request_time"
    }
)
def endpoint_usage_bronze():
    """
    Stream endpoint usage data including token counts.
    Critical for tracking Foundation Model API and external model costs.

    Note: To capture usage in this table, you must enable
    'Enable AI Gateway features' on your serving endpoint.

    Columns: account_id, workspace_id, client_request_id, databricks_request_id,
    requester_id, status_code, request_time, input_token_count, output_token_count,
    input_character_count, output_character_count, usage_context, is_streaming, served_entity_id
    """
    return (
        spark.readStream
        .option("skipChangeCommits", "true")
        .table("system.serving.endpoint_usage")
        .withColumn("_account_identifier", F.lit(account_identifier))
        .withColumn("_workspace_identifier", F.lit(workspace_identifier))
        .withColumn("_ingested_at", F.current_timestamp())
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.4 Endpoint Usage Summary

# COMMAND ----------

@dlt.table(
    name="endpoint_usage_summary",
    comment="Daily endpoint usage aggregated by endpoint and model - for cost tracking",
    table_properties={"quality": "gold"}
)
def endpoint_usage_summary():
    """
    Aggregate endpoint usage by day for cost analysis.
    Includes token counts for input, output, and calculated total.
    """
    usage = dlt.read("endpoint_usage_bronze")
    entities = dlt.read("served_entities_current")

    # Aggregate usage with explicit column references
    usage_agg = (
        usage
        .groupBy(
            F.col("_account_identifier"),
            F.col("_workspace_identifier"),
            F.col("workspace_id"),
            F.col("served_entity_id"),
            F.to_date(F.col("request_time")).alias("usage_date")
        )
        .agg(
            F.sum(F.col("input_token_count")).alias("total_input_tokens"),
            F.sum(F.col("output_token_count")).alias("total_output_tokens"),
            F.sum(F.col("input_token_count") + F.col("output_token_count")).alias("total_tokens"),
            F.count(F.lit(1)).alias("request_count"),
            F.min(F.col("request_time")).alias("first_request"),
            F.max(F.col("request_time")).alias("last_request")
        )
    )

    # Prepare entity info with aliased columns
    entity_info = entities.select(
        F.col("workspace_id").alias("entity_workspace_id"),
        F.col("served_entity_id").alias("entity_served_entity_id"),
        F.col("endpoint_id"),
        F.col("endpoint_name"),
        F.col("served_entity_name"),
        F.col("entity_type"),
        F.col("entity_name")
    )

    # Join with entity info
    return (
        usage_agg
        .join(
            entity_info,
            on=(
                    (usage_agg.workspace_id == entity_info.entity_workspace_id) &
                    (usage_agg.served_entity_id == entity_info.entity_served_entity_id)
            ),
            how="left"
        )
        .drop("entity_workspace_id", "entity_served_entity_id")
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.5 Model Serving Costs (from Billing)

# COMMAND ----------

@dlt.table(
    name="model_serving_costs",
    comment="Cost breakdown for model serving from billing data",
    table_properties={"quality": "gold"}
)
def model_serving_costs():
    """
    Extract model serving costs from billing data.
    Includes both provisioned throughput and pay-per-token costs.
    """
    billing = dlt.read("billing_summary")
    entities = dlt.read("served_entities_current")

    serving_billing = billing.filter(
        F.col("billing_origin_product") == "MODEL_SERVING"
    )

    # Aggregate billing data
    billing_agg = (
        serving_billing
        .groupBy(
            F.col("_account_identifier"),
            F.col("_workspace_identifier"),
            F.col("workspace_id"),
            F.col("usage_date"),
            F.col("endpoint_name"),
            F.col("sku_name")
        )
        .agg(
            F.sum(F.col("usage_quantity")).alias("total_dbus"),
            F.sum(F.col("estimated_cost_usd")).alias("total_cost_usd"),
            F.count(F.lit(1)).alias("billing_records")
        )
    )

    # Prepare entity info
    entity_info = (
        entities
        .select(
            F.col("workspace_id").alias("entity_workspace_id"),
            F.col("endpoint_name").alias("entity_endpoint_name"),
            F.col("entity_type"),
            F.col("entity_name")
        )
        .dropDuplicates(["entity_workspace_id", "entity_endpoint_name"])
    )

    return (
        billing_agg
        .join(
            entity_info,
            on=(
                    (billing_agg.workspace_id == entity_info.entity_workspace_id) &
                    (billing_agg.endpoint_name == entity_info.entity_endpoint_name)
            ),
            how="left"
        )
        .drop("entity_workspace_id", "entity_endpoint_name")
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.6 Foundation Model API Usage

# COMMAND ----------

@dlt.table(
    name="foundation_model_usage",
    comment="Foundation Model API usage summary"
)
def foundation_model_usage():
    """
    Focused view on Foundation Model API usage.
    Includes models like DBRX, Llama, Mixtral served via Databricks.

    Foundation models can be identified by:
    - entity_type containing 'FOUNDATION'
    - endpoint_name starting with 'databricks-' (pay-per-token system endpoints)
    - entity_name containing known foundation model patterns
    """
    usage = dlt.read("endpoint_usage_summary")

    # Foundation model patterns for pay-per-token endpoints
    foundation_endpoint_patterns = [
        "databricks-meta-llama",
        "databricks-mixtral",
        "databricks-dbrx",
        "databricks-bge",
        "databricks-gte",
        "databricks-mpnet",
        "databricks-claude",
    ]

    # Build filter condition: entity_type contains FOUNDATION OR endpoint matches patterns
    foundation_filter = (
        F.col("entity_type").contains("FOUNDATION") |
        F.col("entity_type").isin(["FOUNDATION_MODEL_API", "FOUNDATION_MODEL"]) |
        # Match pay-per-token system endpoints by name pattern
        F.col("endpoint_name").rlike("^databricks-(meta-llama|mixtral|dbrx|bge|gte|mpnet)")
    )

    return (
        usage
        .filter(foundation_filter)
        .select(
            "_account_identifier",
            "_workspace_identifier",
            "workspace_id",
            "usage_date",
            "endpoint_name",
            "entity_name",
            "entity_type",
            "total_input_tokens",
            "total_output_tokens",
            "total_tokens",
            "request_count"
        )
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.6b Foundation Model Costs (from Billing)
# MAGIC
# MAGIC More reliable tracking via billing data - captures all foundation model usage including pay-per-token.

# COMMAND ----------

@dlt.table(
    name="foundation_model_costs",
    comment="Foundation Model costs from billing - includes pay-per-token usage"
)
def foundation_model_costs():
    """
    Track foundation model costs from billing data.
    This captures ALL foundation model usage including:
    - Pay-per-token endpoints (databricks-meta-llama-*, etc.)
    - Provisioned throughput endpoints
    - Custom fine-tuned foundation models

    Uses product_features.serving_type = 'FOUNDATION_MODEL' from billing.
    """
    billing = dlt.read("billing_summary")

    # Filter for model serving with foundation model serving type
    # Also match by SKU patterns for foundation models
    foundation_skus = [
        "FOUNDATION_MODEL",
        "FOUNDATION_MODEL_TRAINING",
        "PAY_PER_TOKEN",
    ]

    return (
        billing
        .filter(
            (F.col("billing_origin_product") == "MODEL_SERVING") |
            (F.col("billing_origin_product") == "FOUNDATION_MODEL_TRAINING") |
            (F.col("sku_name").contains("FOUNDATION")) |
            (F.col("sku_name").contains("TOKEN")) |
            # Match databricks foundation model endpoint names
            (F.col("endpoint_name").rlike("^databricks-(meta-llama|mixtral|dbrx|bge|gte|mpnet)"))
        )
        .groupBy(
            "_account_identifier",
            "_workspace_identifier",
            "workspace_id",
            "usage_date",
            "endpoint_name",
            "sku_name",
            "billing_origin_product"
        )
        .agg(
            F.sum("usage_quantity").alias("total_dbus"),
            F.sum("estimated_cost_usd").alias("total_cost_usd"),
            F.count("*").alias("billing_records")
        )
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.7 External Model Usage

# COMMAND ----------

@dlt.table(
    name="external_model_usage",
    comment="External model (OpenAI, Anthropic, etc.) usage via AI Gateway"
)
def external_model_usage():
    """
    Track usage of external models routed through AI Gateway.
    Includes OpenAI, Anthropic, Cohere, etc.

    Filter by entity_type = 'EXTERNAL_MODEL' to identify external models.
    The external_model_config struct contains provider info (provider, name, task).
    The entity_name contains the model identifier.
    """
    usage = dlt.read("endpoint_usage_summary")

    return (
        usage
        .filter(F.col("entity_type") == "EXTERNAL_MODEL")
        .select(
            "_account_identifier",
            "_workspace_identifier",
            "workspace_id",
            "usage_date",
            "endpoint_name",
            "entity_name",
            "total_input_tokens",
            "total_output_tokens",
            "total_tokens",
            "request_count"
        )
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 2. MLflow Monitoring
# MAGIC
# MAGIC Track experiments, runs, and training metrics.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 MLflow Experiments (Bronze)

# COMMAND ----------

@dlt.table(
    name="mlflow_experiments_bronze",
    comment="MLflow experiments from system.mlflow.experiments_latest",
    table_properties={"quality": "bronze"}
)
def mlflow_experiments_bronze():
    """
    Stream MLflow experiment definitions.
    Each row represents an experiment created in Databricks-managed MLflow.

    Note: This table uses experiments_latest which records experiment names and soft-deletion events.
    Columns: account_id, _updated_at, deleted_at, experiment_id, workspace_id, name, creation_time
    """
    return (
        spark.readStream
        .option("skipChangeCommits", "true")
        .table("system.mlflow.experiments_latest")
        .withColumn("_account_identifier", F.lit(account_identifier))
        .withColumn("_workspace_identifier", F.lit(workspace_identifier))
        .withColumn("_ingested_at", F.current_timestamp())
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 MLflow Runs (Bronze)

# COMMAND ----------

@dlt.table(
    name="mlflow_runs_bronze",
    comment="MLflow runs from system.mlflow.runs_latest",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "start_time,experiment_id"
    }
)
def mlflow_runs_bronze():
    """
    Stream MLflow run data.
    Each row represents a model training/evaluation run.

    Note: This table uses runs_latest which records run-lifecycle information,
    params, tags, and aggregated stats of min, max, and latest values of all metrics.
    Columns: account_id, update_time, delete_time, workspace_id, run_id, experiment_id,
    created_by, start_time, end_time, run_name, status, params, tags, aggregated_metrics
    """
    return (
        spark.readStream
        .option("skipChangeCommits", "true")
        .table("system.mlflow.runs_latest")
        .withColumn("_account_identifier", F.lit(account_identifier))
        .withColumn("_workspace_identifier", F.lit(workspace_identifier))
        .withColumn("_ingested_at", F.current_timestamp())
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3 MLflow Metrics (Bronze)

# COMMAND ----------

@dlt.table(
    name="mlflow_metrics_bronze",
    comment="MLflow run metrics history from system.mlflow.run_metrics_history",
    table_properties={"quality": "bronze"}
)
def mlflow_metrics_bronze():
    """
    Stream MLflow training metrics.
    Contains metrics logged during model training at different steps/times.

    Note: Columns include account_id, inserted_time, metric_id, workspace_id,
    experiment_id, run_id, metric_name, metric_time, step, metric_value
    """
    return (
        spark.readStream
        .option("skipChangeCommits", "true")
        .table("system.mlflow.run_metrics_history")
        .withColumn("_account_identifier", F.lit(account_identifier))
        .withColumn("_workspace_identifier", F.lit(workspace_identifier))
        .withColumn("_ingested_at", F.current_timestamp())
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.4 MLflow Experiments Summary

# COMMAND ----------

@dlt.table(
    name="mlflow_experiments_summary",
    comment="MLflow experiment activity summary"
)
def mlflow_experiments_summary():
    """
    Aggregate experiment activity for tracking ML development.

    Actual columns from system.mlflow.experiments_latest:
    - account_id, last_updated_time, deleted_time, experiment_id, workspace_id, name, creation_time

    Actual columns from system.mlflow.runs_latest:
    - account_id, update_time, delete_time, workspace_id, run_id, experiment_id,
      created_by, start_time, end_time, run_name, status, params, tags, aggregated_metrics
    """
    experiments = dlt.read("mlflow_experiments_bronze")
    runs = dlt.read("mlflow_runs_bronze")

    # Count runs per experiment - use created_by instead of user_id
    run_stats = (
        runs
        .groupBy(F.col("experiment_id").alias("run_experiment_id"))
        .agg(
            F.count("*").alias("total_runs"),
            F.sum(F.when(F.col("status") == "FINISHED", 1).otherwise(0)).alias("successful_runs"),
            F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0)).alias("failed_runs"),
            F.min("start_time").alias("first_run"),
            F.max("start_time").alias("last_run"),
            F.countDistinct("created_by").alias("distinct_users")  # Use created_by, not user_id
        )
    )

    # Join experiments with run stats
    return (
        experiments
        .join(
            run_stats,
            experiments.experiment_id == run_stats.run_experiment_id,
            how="left"
        )
        .select(
            experiments["_account_identifier"],
            experiments["_workspace_identifier"],
            experiments["workspace_id"],
            experiments["experiment_id"],
            experiments["name"],
            experiments["create_time"].alias("creation_time"),
            experiments["update_time"].alias("last_update_time"),  # Actual column is update_time
            F.when(experiments["delete_time"].isNull(), F.lit("active")).otherwise(F.lit("deleted")).alias(
                "lifecycle_stage"),  # Actual column is delete_time
            F.coalesce(F.col("total_runs"), F.lit(0)).alias("total_runs"),
            F.coalesce(F.col("successful_runs"), F.lit(0)).alias("successful_runs"),
            F.coalesce(F.col("failed_runs"), F.lit(0)).alias("failed_runs"),
            F.col("first_run"),
            F.col("last_run"),
            F.coalesce(F.col("distinct_users"), F.lit(0)).alias("distinct_users")
        )
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.5 MLflow Runs Summary

# COMMAND ----------

@dlt.table(
    name="mlflow_runs_summary",
    comment="Daily MLflow run activity - for tracking ML workload"
)
def mlflow_runs_summary():
    """
    Daily aggregation of MLflow runs for ML workload tracking.

    Actual columns from system.mlflow.runs_latest:
    - account_id, update_time, delete_time, workspace_id, run_id, experiment_id,
      created_by, start_time, end_time, run_name, status, params, tags, aggregated_metrics
    """
    runs = dlt.read("mlflow_runs_bronze")

    return (
        runs
        .withColumn("run_date", F.to_date(F.col("start_time")))
        .groupBy(
            F.col("_account_identifier"),
            F.col("_workspace_identifier"),
            F.col("workspace_id"),
            F.col("run_date"),
            F.col("experiment_id"),
            F.col("status")
        )
        .agg(
            F.count(F.lit(1)).alias("run_count"),
            F.countDistinct(F.col("created_by")).alias("distinct_users"),  # Use created_by, not user_id
            F.avg(
                F.unix_timestamp(F.col("end_time")) - F.unix_timestamp(F.col("start_time"))
            ).alias("avg_duration_seconds"),
            F.sum(
                F.unix_timestamp(F.col("end_time")) - F.unix_timestamp(F.col("start_time"))
            ).alias("total_compute_seconds")
        )
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 3. Vector Search Monitoring
# MAGIC
# MAGIC Track vector search index usage and costs.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 Vector Search Costs (from Billing)

# COMMAND ----------

@dlt.table(
    name="vector_search_costs",
    comment="Vector Search index costs from billing data",
    table_properties={"quality": "gold"}
)
def vector_search_costs():
    """
    Extract Vector Search costs from billing data.
    Includes index creation, maintenance, and query costs.
    """
    billing = dlt.read("billing_summary")

    return (
        billing
        .filter(F.col("billing_origin_product") == "VECTOR_SEARCH")
        .groupBy(
            "_account_identifier",
            "_workspace_identifier",
            "workspace_id",
            "usage_date",
            "endpoint_name",  # Vector search endpoint
            "sku_name"
        )
        .agg(
            F.sum("usage_quantity").alias("total_dbus"),
            F.sum("estimated_cost_usd").alias("total_cost_usd"),
            F.count("*").alias("billing_records")
        )
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 Vector Search Usage Summary

# COMMAND ----------

@dlt.table(
    name="vector_search_summary",
    comment="Vector Search daily usage summary"
)
def vector_search_summary():
    """
    Daily summary of vector search usage across all indexes.
    """
    costs = dlt.read("vector_search_costs")

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
            F.countDistinct("endpoint_name").alias("distinct_endpoints"),
            F.collect_set("endpoint_name").alias("endpoint_names")
        )
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 4. Combined AI/ML Executive View

# COMMAND ----------

@dlt.table(
    name="aiml_executive_summary",
    comment="Combined AI/ML workload summary for executives"
)
def aiml_executive_summary():
    """
    High-level view of all AI/ML spend across:
    - Model Serving (custom models, foundation models, external models)
    - MLflow (experiment/training compute)
    - Vector Search (index operations)
    """
    # Get billing for all AI/ML products
    billing = dlt.read("billing_summary")

    aiml_products = [
        "MODEL_SERVING",
        "VECTOR_SEARCH",
        "ONLINE_TABLES",
        "FOUNDATION_MODEL_TRAINING"
    ]

    return (
        billing
        .filter(F.col("billing_origin_product").isin(aiml_products))
        .groupBy(
            "_account_identifier",
            "_workspace_identifier",
            "workspace_id",
            "usage_date",
            "billing_origin_product"
        )
        .agg(
            F.sum("usage_quantity").alias("total_dbus"),
            F.sum("estimated_cost_usd").alias("total_cost_usd"),
            F.countDistinct("endpoint_name").alias("distinct_endpoints"),
            F.countDistinct("run_as").alias("distinct_users")
        )
        .withColumn("aiml_category",
                    F.when(F.col("billing_origin_product") == "MODEL_SERVING", "Inference")
                    .when(F.col("billing_origin_product") == "VECTOR_SEARCH", "Vector Search")
                    .when(F.col("billing_origin_product") == "ONLINE_TABLES", "Feature Serving")
                    .when(F.col("billing_origin_product") == "FOUNDATION_MODEL_TRAINING", "Fine-tuning")
                    .otherwise("Other AI/ML")
                    )
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 Endpoint Inventory

# COMMAND ----------

@dlt.table(
    name="endpoint_inventory",
    comment="Current inventory of all model serving endpoints"
)
def endpoint_inventory():
    """
    Complete inventory of model serving endpoints with their configurations.
    Useful for governance and capacity planning.
    """
    entities = dlt.read("served_entities_current")
    costs = dlt.read("model_serving_costs")

    # Get total costs per endpoint with explicit columns
    endpoint_costs = (
        costs
        .groupBy(F.col("workspace_id"), F.col("endpoint_name"))
        .agg(
            F.sum(F.col("total_cost_usd")).alias("total_cost_usd_30d"),
            F.max(F.col("usage_date")).alias("last_billing_date")
        )
        .select(
            F.col("workspace_id").alias("cost_workspace_id"),
            F.col("endpoint_name").alias("cost_endpoint_name"),
            F.col("total_cost_usd_30d"),
            F.col("last_billing_date")
        )
    )

    # Prepare entity info
    entity_base = entities.select(
        F.col("_account_identifier"),
        F.col("_workspace_identifier"),
        F.col("workspace_id"),
        F.col("endpoint_id"),
        F.col("endpoint_name"),
        F.col("served_entity_id"),
        F.col("served_entity_name"),
        F.col("entity_type"),
        F.col("entity_name"),
        F.col("entity_version"),
        F.col("task").alias("task_type"),  # Actual column is task
        F.col("endpoint_config_version").alias("config_version"),  # Actual column is endpoint_config_version
        F.col("created_by"),
        F.col("change_time")
    )

    return (
        entity_base
        .join(
            endpoint_costs,
            on=(
                    (entity_base.workspace_id == endpoint_costs.cost_workspace_id) &
                    (entity_base.endpoint_name == endpoint_costs.cost_endpoint_name)
            ),
            how="left"
        )
        .drop("cost_workspace_id", "cost_endpoint_name")
    )
