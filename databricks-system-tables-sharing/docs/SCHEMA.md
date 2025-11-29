# Schema Reference

This document describes all tables created by the System Tables Monitoring solution.

## Table Overview

| Category | Table | Type | Purpose |
|----------|-------|------|---------|
| **Billing** | `billing_usage_bronze` | Streaming | Raw billing records |
| | `list_prices` | Batch | SKU pricing reference |
| | `billing_summary` | Materialized View | Usage with calculated costs |
| | `cost_by_product` | Materialized View | Daily costs by product |
| | `cost_by_sku` | Materialized View | Daily costs by SKU |
| | `cost_by_job` | Materialized View | Costs attributed to jobs |
| | `cost_by_user` | Materialized View | Costs attributed to users |
| **Jobs** | `jobs_bronze` | Streaming | Job definitions (SCD2) |
| | `job_runs_bronze` | Streaming | Job run events |
| | `jobs_current` | Materialized View | Current job configurations |
| | `job_runs_summary` | Materialized View | Daily job statistics |
| **Compute** | `clusters_bronze` | Streaming | Cluster configs (SCD2) |
| | `clusters_current` | Materialized View | Current cluster state |
| | `cluster_utilization` | Materialized View | Resource utilization metrics |
| **Audit** | `audit_logs_bronze` | Streaming | Raw audit events |
| | `user_activity_summary` | Materialized View | Daily user activity |
| | `data_access_summary` | Materialized View | Table access patterns |
| **Model Serving** | `served_entities_bronze` | Streaming | Model configs (SCD2) |
| | `served_entities_current` | Materialized View | Current model deployments |
| | `endpoint_usage_bronze` | Streaming | Token usage per request |
| | `endpoint_usage_summary` | Materialized View | Daily token usage |
| | `model_serving_costs` | Materialized View | Serving costs from billing |
| | `foundation_model_usage` | Materialized View | Foundation model API usage |
| | `foundation_model_costs` | Materialized View | Foundation model costs |
| | `external_model_usage` | Materialized View | External model (OpenAI, etc.) usage |
| | `endpoint_inventory` | Materialized View | Complete endpoint catalog |
| **MLflow** | `mlflow_experiments_bronze` | Streaming | Experiment definitions |
| | `mlflow_runs_bronze` | Streaming | Training runs |
| | `mlflow_metrics_bronze` | Streaming | Training metrics |
| | `mlflow_experiments_summary` | Materialized View | Experiment activity |
| | `mlflow_runs_summary` | Materialized View | Daily run statistics |
| **Vector Search** | `vector_search_costs` | Materialized View | Vector Search costs |
| | `vector_search_summary` | Materialized View | Daily Vector Search usage |
| **Executive** | `workspace_info` | Batch | Workspace metadata |
| | `executive_summary` | Materialized View | High-level cost KPIs |
| | `aiml_executive_summary` | Materialized View | AI/ML spend overview |
| **Daily Rollups** | `cost_weekly_rollup` | Table | Weekly cost aggregation |
| | `cost_monthly_rollup` | Table | Monthly cost aggregation |
| | `job_performance_trends` | Table | Job performance with moving averages |
| | `cost_anomalies` | Table | Spend anomaly detection |
| | `top_consumers_30d` | Table | Top 50 cost-generating jobs |
| | `user_activity_30d` | Table | 30-day user activity summary |

---

## Common Columns

All tables include these identifier columns for cross-workspace analysis:

| Column | Type | Description |
|--------|------|-------------|
| `_account_identifier` | STRING | Human-readable account name (from config) |
| `_workspace_identifier` | STRING | Human-readable workspace name (from config) |

---

## Billing & Cost Tables

### billing_usage_bronze

Raw billing data streamed from `system.billing.usage`.

| Column | Type | Description |
|--------|------|-------------|
| `record_id` | STRING | Unique usage record ID |
| `account_id` | STRING | Databricks account ID |
| `workspace_id` | STRING | Workspace ID |
| `sku_name` | STRING | SKU identifier (e.g., `JOBS_COMPUTE`, `SERVERLESS_SQL`) |
| `cloud` | STRING | Cloud provider (`AWS`, `AZURE`, `GCP`) |
| `usage_start_time` | TIMESTAMP | Start time of usage period |
| `usage_end_time` | TIMESTAMP | End time of usage period |
| `usage_date` | DATE | Date of usage (for partitioning) |
| `usage_quantity` | DECIMAL | Number of units consumed |
| `usage_unit` | STRING | Unit type (`DBU`, `GPU_HOUR`) |
| `usage_metadata` | STRUCT | Contains `job_id`, `cluster_id`, `warehouse_id`, `notebook_id`, `endpoint_name`, etc. |
| `identity_metadata` | STRUCT | Contains `run_as` (execution identity) |
| `custom_tags` | MAP | User-defined cost attribution tags |
| `billing_origin_product` | STRING | Product category (`JOBS`, `SQL`, `DLT`, `MODEL_SERVING`, etc.) |
| `product_features` | STRUCT | Contains `is_serverless`, `is_photon`, `jobs_tier`, `sql_tier` |
| `record_type` | STRING | `ORIGINAL`, `RETRACTION`, or `RESTATEMENT` |
| `_ingested_at` | TIMESTAMP | When the record was ingested |

**Use cases**: Raw data for custom analysis, audit trail, debugging billing discrepancies.

---

### list_prices

SKU pricing reference from `system.billing.list_prices`. SCD2 table with effective dates.

| Column | Type | Description |
|--------|------|-------------|
| `sku_name` | STRING | SKU identifier |
| `cloud` | STRING | Cloud provider |
| `currency_code` | STRING | Currency (typically `USD`) |
| `usage_unit` | STRING | Unit type |
| `pricing` | STRUCT | Contains `default` (list price per unit) |
| `price_start_time` | TIMESTAMP | Price effective start date |
| `price_end_time` | TIMESTAMP | Price effective end date (NULL = current) |

**Use cases**: Cost calculations, historical price analysis, pricing audits.

---

### billing_summary

Gold-level table joining usage with prices to calculate estimated costs.

| Column | Type | Description |
|--------|------|-------------|
| `usage_date` | DATE | Date of usage |
| `usage_start_time` | TIMESTAMP | Start time of usage |
| `usage_end_time` | TIMESTAMP | End time of usage |
| `sku_name` | STRING | SKU identifier |
| `cloud` | STRING | Cloud provider |
| `billing_origin_product` | STRING | Product category |
| `usage_quantity` | DECIMAL | Units consumed |
| `usage_unit` | STRING | Unit type |
| `estimated_cost_usd` | DECIMAL | Calculated cost (usage_quantity × price) |
| `job_id` | STRING | Associated job ID (if applicable) |
| `job_name` | STRING | Job name |
| `cluster_id` | STRING | Associated cluster ID |
| `warehouse_id` | STRING | Associated SQL warehouse ID |
| `notebook_id` | STRING | Associated notebook ID |
| `notebook_path` | STRING | Notebook path |
| `dlt_pipeline_id` | STRING | Associated DLT pipeline ID |
| `endpoint_name` | STRING | Model serving endpoint name |
| `run_as` | STRING | User/service principal that ran the workload |
| `is_serverless` | BOOLEAN | Whether serverless compute was used |
| `is_photon` | BOOLEAN | Whether Photon was enabled |
| `jobs_tier` | STRING | Jobs tier (e.g., `CLASSIC`, `LIGHT`) |
| `sql_tier` | STRING | SQL tier |
| `custom_tags` | MAP | User-defined tags |

**Use cases**: Primary table for cost analysis, dashboards, chargeback reports.

---

### cost_by_product

Daily costs aggregated by billing product.

| Column | Type | Description |
|--------|------|-------------|
| `usage_date` | DATE | Date |
| `billing_origin_product` | STRING | Product (`JOBS`, `SQL`, `DLT`, `MODEL_SERVING`, `INTERACTIVE`, etc.) |
| `is_serverless` | BOOLEAN | Serverless vs. classic compute |
| `total_dbus` | DECIMAL | Total DBUs consumed |
| `total_cost_usd` | DECIMAL | Total estimated cost |
| `record_count` | BIGINT | Number of billing records |

**Use cases**: Executive dashboards, product-level spend tracking, serverless adoption metrics.

---

### cost_by_sku

Daily costs aggregated by SKU for detailed analysis.

| Column | Type | Description |
|--------|------|-------------|
| `usage_date` | DATE | Date |
| `sku_name` | STRING | SKU identifier |
| `cloud` | STRING | Cloud provider |
| `usage_unit` | STRING | Unit type |
| `total_usage` | DECIMAL | Total units consumed |
| `total_cost_usd` | DECIMAL | Total estimated cost |
| `record_count` | BIGINT | Number of billing records |

**Use cases**: Detailed cost breakdown, SKU-level optimization, cloud comparison.

---

### cost_by_job

Costs attributed to specific jobs.

| Column | Type | Description |
|--------|------|-------------|
| `usage_date` | DATE | Date |
| `job_id` | STRING | Job ID |
| `job_name` | STRING | Job name |
| `run_as` | STRING | Execution identity |
| `billing_origin_product` | STRING | Product category |
| `total_dbus` | DECIMAL | Total DBUs |
| `total_cost_usd` | DECIMAL | Total cost |
| `run_count` | BIGINT | Number of runs |

**Use cases**: Job-level chargeback, identifying expensive jobs, optimization targeting.

---

### cost_by_user

Costs attributed to users (run_as identity).

| Column | Type | Description |
|--------|------|-------------|
| `usage_date` | DATE | Date |
| `run_as` | STRING | User or service principal |
| `billing_origin_product` | STRING | Product category |
| `total_dbus` | DECIMAL | Total DBUs |
| `total_cost_usd` | DECIMAL | Total cost |
| `distinct_jobs` | BIGINT | Number of distinct jobs run |
| `record_count` | BIGINT | Number of billing records |

**Use cases**: User-level chargeback, team cost attribution, identifying heavy users.

---

## Jobs & Workflow Tables

### jobs_bronze

Job definitions streamed from `system.lakeflow.jobs`. SCD2 table tracking configuration history.

| Column | Type | Description |
|--------|------|-------------|
| `account_id` | STRING | Account ID |
| `workspace_id` | STRING | Workspace ID |
| `job_id` | STRING | Job ID |
| `name` | STRING | Job name |
| `creator_id` | STRING | User who created the job |
| `run_as` | STRING | Execution identity |
| `tags` | MAP | Job tags |
| `schedule` | STRUCT | Cron schedule configuration |
| `change_time` | TIMESTAMP | When this version was created |
| `delete_time` | TIMESTAMP | When job was deleted (NULL = active) |

**Use cases**: Job inventory, configuration auditing, tracking changes over time.

---

### job_runs_bronze

Job run events streamed from `system.lakeflow.job_run_timeline`.

| Column | Type | Description |
|--------|------|-------------|
| `workspace_id` | STRING | Workspace ID |
| `job_id` | STRING | Job ID |
| `run_id` | STRING | Run ID |
| `run_name` | STRING | Run name |
| `run_type` | STRING | `JOB_RUN` or `SUBMIT_RUN` |
| `period_start_time` | TIMESTAMP | Run start time |
| `period_end_time` | TIMESTAMP | Run end time |
| `result_state` | STRING | `SUCCEEDED`, `FAILED`, `CANCELED`, `SKIPPED` |
| `termination_code` | STRING | Termination reason |
| `compute_ids` | ARRAY | Clusters used |

**Note**: For runs longer than 1 hour, multiple rows are emitted per run_id, each covering up to 1 hour.

**Use cases**: Run history, failure analysis, SLA tracking.

---

### jobs_current

Current state of all jobs (latest version from SCD2).

| Column | Type | Description |
|--------|------|-------------|
| All columns from `jobs_bronze` | | Latest version only, excluding deleted jobs |

**Use cases**: Current job inventory, active job catalog, governance.

---

### job_runs_summary

Daily job execution statistics.

| Column | Type | Description |
|--------|------|-------------|
| `run_date` | DATE | Run date |
| `job_id` | STRING | Job ID |
| `job_name` | STRING | Job name |
| `total_runs` | BIGINT | Total runs |
| `successful_runs` | BIGINT | Successful runs |
| `failed_runs` | BIGINT | Failed runs |
| `canceled_runs` | BIGINT | Canceled runs |
| `avg_duration_seconds` | DOUBLE | Average run duration |
| `max_duration_seconds` | BIGINT | Maximum run duration |
| `min_duration_seconds` | BIGINT | Minimum run duration |
| `p95_duration_seconds` | DOUBLE | 95th percentile duration |

**Use cases**: Job health monitoring, SLA dashboards, performance trending.

---

## Compute & Cluster Tables

### clusters_bronze

Cluster configurations streamed from `system.compute.clusters`. SCD2 table.

| Column | Type | Description |
|--------|------|-------------|
| `account_id` | STRING | Account ID |
| `workspace_id` | STRING | Workspace ID |
| `cluster_id` | STRING | Cluster ID |
| `cluster_name` | STRING | Cluster name |
| `owned_by` | STRING | Owner user ID |
| `driver_node_type` | STRING | Driver instance type |
| `worker_node_type` | STRING | Worker instance type |
| `num_workers` | INT | Fixed worker count (if not autoscaling) |
| `autoscale` | STRUCT | Contains `min_workers`, `max_workers` |
| `data_security_mode` | STRING | Access mode |
| `dbr_version` | STRING | Databricks Runtime version |
| `change_time` | TIMESTAMP | Last change time |
| `delete_time` | TIMESTAMP | Deletion time (NULL = active) |

**Use cases**: Cluster inventory, configuration auditing, DBR version tracking.

---

### clusters_current

Current state of all clusters.

| Column | Type | Description |
|--------|------|-------------|
| All columns from `clusters_bronze` | | Latest version only |

**Use cases**: Current cluster inventory, rightsizing analysis.

---

### cluster_utilization

Hourly cluster resource utilization from `system.compute.node_timeline`.

| Column | Type | Description |
|--------|------|-------------|
| `cluster_id` | STRING | Cluster ID |
| `hour` | TIMESTAMP | Hour (truncated) |
| `avg_cpu_percent` | DOUBLE | Average CPU utilization |
| `max_cpu_percent` | DOUBLE | Peak CPU utilization |
| `avg_memory_percent` | DOUBLE | Average memory utilization |
| `max_memory_percent` | DOUBLE | Peak memory utilization |
| `total_network_in_bytes` | BIGINT | Network bytes received |
| `total_network_out_bytes` | BIGINT | Network bytes sent |
| `sample_count` | BIGINT | Number of samples |

**Note**: Source data (`node_timeline`) has 90-day retention.

**Use cases**: Rightsizing analysis, capacity planning, identifying underutilized clusters.

---

## Audit & Activity Tables

### audit_logs_bronze

Raw audit events streamed from `system.access.audit`.

| Column | Type | Description |
|--------|------|-------------|
| `account_id` | STRING | Account ID |
| `workspace_id` | STRING | Workspace ID (0 = account-level) |
| `event_time` | TIMESTAMP | Event time |
| `event_date` | DATE | Event date |
| `service_name` | STRING | Service (`unityCatalog`, `databrickssql`, `clusters`, etc.) |
| `action_name` | STRING | Action performed |
| `user_identity` | STRUCT | Contains `email`, `subject_name` |
| `request_params` | MAP | Action-specific parameters |
| `response` | STRUCT | Response status and result |
| `source_ip_address` | STRING | Origin IP |

**Use cases**: Security monitoring, compliance auditing, access tracking.

---

### user_activity_summary

Daily user activity aggregated from audit logs.

| Column | Type | Description |
|--------|------|-------------|
| `event_date` | DATE | Date |
| `user_email` | STRING | User email |
| `service_name` | STRING | Service name |
| `event_count` | BIGINT | Number of events |
| `distinct_actions` | BIGINT | Number of distinct action types |
| `first_activity` | TIMESTAMP | First activity of the day |
| `last_activity` | TIMESTAMP | Last activity of the day |

**Use cases**: User adoption tracking, activity dashboards, license utilization.

---

### data_access_summary

Unity Catalog table access patterns.

| Column | Type | Description |
|--------|------|-------------|
| `event_date` | DATE | Date |
| `user_email` | STRING | User email |
| `action_name` | STRING | `getTable`, `createTable`, `deleteTable` |
| `table_full_name` | STRING | Full table name (`catalog.schema.table`) |
| `access_count` | BIGINT | Number of accesses |
| `first_access` | TIMESTAMP | First access time |
| `last_access` | TIMESTAMP | Last access time |

**Use cases**: Data governance, table popularity analysis, access auditing.

---

## Model Serving Tables

### served_entities_bronze

Model serving configurations streamed from `system.serving.served_entities`. SCD2 table.

| Column | Type | Description |
|--------|------|-------------|
| `served_entity_id` | STRING | Unique entity ID |
| `account_id` | STRING | Account ID |
| `workspace_id` | STRING | Workspace ID |
| `created_by` | STRING | Creator |
| `endpoint_name` | STRING | Endpoint name |
| `endpoint_id` | STRING | Endpoint ID |
| `served_entity_name` | STRING | Entity name in endpoint |
| `entity_type` | STRING | `CUSTOM_MODEL`, `FOUNDATION_MODEL_API`, `EXTERNAL_MODEL`, etc. |
| `entity_name` | STRING | Model name or identifier |
| `entity_version` | STRING | Model version |
| `endpoint_config_version` | INT | Configuration version |
| `task` | STRING | Task type (`llm/v1/chat`, `llm/v1/completions`, etc.) |
| `external_model_config` | STRUCT | Config for external models (OpenAI, Anthropic) |
| `foundation_model_config` | STRUCT | Config for foundation models |
| `custom_model_config` | STRUCT | Config for custom models |
| `change_time` | TIMESTAMP | Last change time |
| `endpoint_delete_time` | TIMESTAMP | Deletion time (NULL = active) |
| `_ingested_at` | TIMESTAMP | Ingestion time |

**Use cases**: Model inventory, configuration auditing, deployment tracking.

---

### served_entities_current

Current model deployments (latest version, excluding deleted).

| Column | Type | Description |
|--------|------|-------------|
| All columns from `served_entities_bronze` | | Latest version only |

**Use cases**: Current model inventory, endpoint catalog.

---

### endpoint_usage_bronze

Token usage per request from `system.serving.endpoint_usage`.

| Column | Type | Description |
|--------|------|-------------|
| `account_id` | STRING | Account ID |
| `workspace_id` | STRING | Workspace ID |
| `served_entity_id` | STRING | Served entity ID |
| `client_request_id` | STRING | Client request ID |
| `databricks_request_id` | STRING | Databricks request ID |
| `requester_id` | STRING | User/service making request |
| `status_code` | INT | HTTP status code |
| `request_time` | TIMESTAMP | Request time |
| `input_token_count` | BIGINT | Input tokens |
| `output_token_count` | BIGINT | Output tokens |
| `input_character_count` | BIGINT | Input characters |
| `output_character_count` | BIGINT | Output characters |
| `is_streaming` | BOOLEAN | Streaming request |
| `usage_context` | STRING | Usage context |
| `_ingested_at` | TIMESTAMP | Ingestion time |

**Note**: Requires "Enable AI Gateway features" on the endpoint.

**Use cases**: Token usage analysis, per-request debugging, usage attribution.

---

### endpoint_usage_summary

Daily token usage aggregated by endpoint.

| Column | Type | Description |
|--------|------|-------------|
| `usage_date` | DATE | Date |
| `served_entity_id` | STRING | Served entity ID |
| `endpoint_id` | STRING | Endpoint ID |
| `endpoint_name` | STRING | Endpoint name |
| `served_entity_name` | STRING | Entity name |
| `entity_type` | STRING | Entity type |
| `entity_name` | STRING | Model name |
| `total_input_tokens` | BIGINT | Total input tokens |
| `total_output_tokens` | BIGINT | Total output tokens |
| `total_tokens` | BIGINT | Total tokens (input + output) |
| `request_count` | BIGINT | Number of requests |
| `first_request` | TIMESTAMP | First request time |
| `last_request` | TIMESTAMP | Last request time |

**Use cases**: Token usage dashboards, cost forecasting, usage trending.

---

### model_serving_costs

Model serving costs from billing data.

| Column | Type | Description |
|--------|------|-------------|
| `usage_date` | DATE | Date |
| `endpoint_name` | STRING | Endpoint name |
| `sku_name` | STRING | SKU |
| `entity_type` | STRING | Entity type |
| `entity_name` | STRING | Model name |
| `total_dbus` | DECIMAL | Total DBUs |
| `total_cost_usd` | DECIMAL | Total cost |
| `billing_records` | BIGINT | Number of billing records |

**Use cases**: Model serving cost analysis, endpoint cost comparison.

---

### foundation_model_usage

Foundation Model API usage (DBRX, Llama, Mixtral, etc.).

| Column | Type | Description |
|--------|------|-------------|
| `usage_date` | DATE | Date |
| `endpoint_name` | STRING | Endpoint name (e.g., `databricks-meta-llama-3-70b-instruct`) |
| `entity_name` | STRING | Model name |
| `entity_type` | STRING | `FOUNDATION_MODEL_API` or similar |
| `total_input_tokens` | BIGINT | Input tokens |
| `total_output_tokens` | BIGINT | Output tokens |
| `total_tokens` | BIGINT | Total tokens |
| `request_count` | BIGINT | Number of requests |

**Use cases**: Foundation model adoption, token usage by model.

---

### foundation_model_costs

Foundation model costs from billing (more reliable than token-based tracking).

| Column | Type | Description |
|--------|------|-------------|
| `usage_date` | DATE | Date |
| `endpoint_name` | STRING | Endpoint name |
| `sku_name` | STRING | SKU |
| `billing_origin_product` | STRING | Product category |
| `total_dbus` | DECIMAL | Total DBUs |
| `total_cost_usd` | DECIMAL | Total cost |
| `billing_records` | BIGINT | Number of billing records |

**Use cases**: Foundation model cost tracking, pay-per-token analysis.

---

### external_model_usage

External model usage (OpenAI, Anthropic, Cohere) via AI Gateway.

| Column | Type | Description |
|--------|------|-------------|
| `usage_date` | DATE | Date |
| `endpoint_name` | STRING | Endpoint name |
| `entity_name` | STRING | External model identifier |
| `total_input_tokens` | BIGINT | Input tokens |
| `total_output_tokens` | BIGINT | Output tokens |
| `total_tokens` | BIGINT | Total tokens |
| `request_count` | BIGINT | Number of requests |

**Use cases**: External model usage tracking, AI Gateway analytics.

---

### endpoint_inventory

Complete inventory of model serving endpoints with costs.

| Column | Type | Description |
|--------|------|-------------|
| `endpoint_id` | STRING | Endpoint ID |
| `endpoint_name` | STRING | Endpoint name |
| `served_entity_id` | STRING | Served entity ID |
| `served_entity_name` | STRING | Entity name |
| `entity_type` | STRING | Entity type |
| `entity_name` | STRING | Model name |
| `entity_version` | STRING | Model version |
| `task_type` | STRING | Task (`llm/v1/chat`, etc.) |
| `config_version` | INT | Configuration version |
| `created_by` | STRING | Creator |
| `change_time` | TIMESTAMP | Last change |
| `total_cost_usd_30d` | DECIMAL | Total cost in last 30 days |
| `last_billing_date` | DATE | Last billing date |

**Use cases**: Endpoint governance, capacity planning, cost attribution.

---

## MLflow Tables

### mlflow_experiments_bronze

MLflow experiments from `system.mlflow.experiments_latest`.

| Column | Type | Description |
|--------|------|-------------|
| `account_id` | STRING | Account ID |
| `workspace_id` | STRING | Workspace ID |
| `experiment_id` | STRING | Experiment ID |
| `name` | STRING | Experiment name |
| `create_time` | TIMESTAMP | Creation time |
| `update_time` | TIMESTAMP | Last update time |
| `delete_time` | TIMESTAMP | Deletion time (NULL = active) |
| `_ingested_at` | TIMESTAMP | Ingestion time |

**Use cases**: Experiment inventory, ML project tracking.

---

### mlflow_runs_bronze

MLflow runs from `system.mlflow.runs_latest`.

| Column | Type | Description |
|--------|------|-------------|
| `account_id` | STRING | Account ID |
| `workspace_id` | STRING | Workspace ID |
| `run_id` | STRING | Run ID |
| `experiment_id` | STRING | Parent experiment ID |
| `created_by` | STRING | User who created the run |
| `start_time` | TIMESTAMP | Run start time |
| `end_time` | TIMESTAMP | Run end time |
| `run_name` | STRING | Run name |
| `status` | STRING | `FINISHED`, `FAILED`, `RUNNING`, `KILLED` |
| `params` | MAP | Run parameters |
| `tags` | MAP | Run tags |
| `aggregated_metrics` | MAP | Min/max/latest metrics |
| `update_time` | TIMESTAMP | Last update time |
| `delete_time` | TIMESTAMP | Deletion time |
| `_ingested_at` | TIMESTAMP | Ingestion time |

**Use cases**: Training run tracking, experiment analysis, model lineage.

---

### mlflow_metrics_bronze

MLflow training metrics from `system.mlflow.run_metrics_history`.

| Column | Type | Description |
|--------|------|-------------|
| `account_id` | STRING | Account ID |
| `workspace_id` | STRING | Workspace ID |
| `experiment_id` | STRING | Experiment ID |
| `run_id` | STRING | Run ID |
| `metric_id` | STRING | Metric ID |
| `metric_name` | STRING | Metric name (e.g., `loss`, `accuracy`) |
| `metric_time` | TIMESTAMP | Metric timestamp |
| `step` | BIGINT | Training step |
| `metric_value` | DOUBLE | Metric value |
| `inserted_time` | TIMESTAMP | Insert time |
| `_ingested_at` | TIMESTAMP | Ingestion time |

**Use cases**: Training curve analysis, hyperparameter tuning, model comparison.

---

### mlflow_experiments_summary

Experiment activity summary with run statistics.

| Column | Type | Description |
|--------|------|-------------|
| `experiment_id` | STRING | Experiment ID |
| `name` | STRING | Experiment name |
| `creation_time` | TIMESTAMP | Creation time |
| `last_update_time` | TIMESTAMP | Last update |
| `lifecycle_stage` | STRING | `active` or `deleted` |
| `total_runs` | BIGINT | Total runs |
| `successful_runs` | BIGINT | Successful runs |
| `failed_runs` | BIGINT | Failed runs |
| `first_run` | TIMESTAMP | First run time |
| `last_run` | TIMESTAMP | Most recent run |
| `distinct_users` | BIGINT | Number of distinct users |

**Use cases**: Experiment health monitoring, ML team activity.

---

### mlflow_runs_summary

Daily MLflow run statistics.

| Column | Type | Description |
|--------|------|-------------|
| `run_date` | DATE | Date |
| `experiment_id` | STRING | Experiment ID |
| `status` | STRING | Run status |
| `run_count` | BIGINT | Number of runs |
| `distinct_users` | BIGINT | Distinct users |
| `avg_duration_seconds` | DOUBLE | Average duration |
| `total_compute_seconds` | BIGINT | Total compute time |

**Use cases**: ML workload tracking, training resource usage.

---

## Vector Search Tables

### vector_search_costs

Vector Search costs from billing.

| Column | Type | Description |
|--------|------|-------------|
| `usage_date` | DATE | Date |
| `endpoint_name` | STRING | Vector Search endpoint |
| `sku_name` | STRING | SKU |
| `total_dbus` | DECIMAL | Total DBUs |
| `total_cost_usd` | DECIMAL | Total cost |
| `billing_records` | BIGINT | Number of billing records |

**Use cases**: Vector Search cost tracking.

---

### vector_search_summary

Daily Vector Search usage summary.

| Column | Type | Description |
|--------|------|-------------|
| `usage_date` | DATE | Date |
| `total_dbus` | DECIMAL | Total DBUs |
| `total_cost_usd` | DECIMAL | Total cost |
| `distinct_endpoints` | BIGINT | Number of endpoints |
| `endpoint_names` | ARRAY | List of endpoint names |

**Use cases**: Vector Search adoption, daily usage trends.

---

## Executive & Summary Tables

### workspace_info

Workspace metadata from `system.access.workspaces_latest`.

| Column | Type | Description |
|--------|------|-------------|
| `workspace_id` | STRING | Workspace ID |
| `workspace_name` | STRING | Workspace name |
| `cloud` | STRING | Cloud provider |
| `region` | STRING | Region |
| `create_time` | TIMESTAMP | Creation time |

**Use cases**: Workspace lookup for dashboards, cross-workspace joins.

---

### executive_summary

High-level daily cost KPIs.

| Column | Type | Description |
|--------|------|-------------|
| `usage_date` | DATE | Date |
| `total_dbus` | DECIMAL | Total DBUs |
| `total_cost_usd` | DECIMAL | Total cost |
| `jobs_cost_usd` | DECIMAL | Jobs cost |
| `sql_cost_usd` | DECIMAL | SQL cost |
| `dlt_cost_usd` | DECIMAL | DLT cost |
| `serving_cost_usd` | DECIMAL | Model Serving cost |
| `interactive_cost_usd` | DECIMAL | Interactive (notebooks) cost |
| `serverless_cost_usd` | DECIMAL | Serverless compute cost |
| `classic_cost_usd` | DECIMAL | Classic compute cost |

**Use cases**: Executive dashboards, daily cost summary.

---

### aiml_executive_summary

AI/ML workload cost summary.

| Column | Type | Description |
|--------|------|-------------|
| `usage_date` | DATE | Date |
| `billing_origin_product` | STRING | Product |
| `aiml_category` | STRING | `Inference`, `Vector Search`, `Feature Serving`, `Fine-tuning` |
| `total_dbus` | DECIMAL | Total DBUs |
| `total_cost_usd` | DECIMAL | Total cost |
| `distinct_endpoints` | BIGINT | Number of endpoints |
| `distinct_users` | BIGINT | Number of users |

**Use cases**: AI/ML spend dashboards, category-level tracking.

---

## Daily Rollup Tables

Created by `daily_aggregations.py` job (runs at 2 AM UTC).

### cost_weekly_rollup

Weekly cost aggregation.

| Column | Type | Description |
|--------|------|-------------|
| `week_start` | DATE | Week start (Monday) |
| `billing_origin_product` | STRING | Product |
| `total_dbus` | DECIMAL | Total DBUs |
| `total_cost_usd` | DECIMAL | Total cost |
| `record_count` | BIGINT | Number of records |

---

### cost_monthly_rollup

Monthly cost aggregation.

| Column | Type | Description |
|--------|------|-------------|
| `month_start` | DATE | Month start |
| `billing_origin_product` | STRING | Product |
| `total_dbus` | DECIMAL | Total DBUs |
| `total_cost_usd` | DECIMAL | Total cost |
| `record_count` | BIGINT | Number of records |

---

### job_performance_trends

Job performance with 7-day moving averages.

| Column | Type | Description |
|--------|------|-------------|
| `job_id` | STRING | Job ID |
| `job_name` | STRING | Job name |
| `run_date` | DATE | Date |
| `total_runs` | BIGINT | Total runs |
| `successful_runs` | BIGINT | Successful runs |
| `failed_runs` | BIGINT | Failed runs |
| `avg_duration_seconds` | DOUBLE | Average duration |
| `p95_duration_seconds` | DOUBLE | P95 duration |
| `success_rate_pct` | DOUBLE | Success rate percentage |
| `ma7_avg_duration` | DOUBLE | 7-day moving avg duration |
| `ma7_success_rate` | DOUBLE | 7-day moving avg success rate |

---

### cost_anomalies

Spend anomaly detection using z-scores.

| Column | Type | Description |
|--------|------|-------------|
| `usage_date` | DATE | Date |
| `daily_cost` | DECIMAL | Daily cost |
| `ma30_cost` | DOUBLE | 30-day moving average cost |
| `stddev30_cost` | DOUBLE | 30-day standard deviation |
| `z_score` | DOUBLE | Z-score (deviation from mean) |
| `anomaly_type` | STRING | `HIGH_SPEND`, `LOW_SPEND`, or `NORMAL` |

---

### top_consumers_30d

Top 50 cost-generating jobs per workspace.

| Column | Type | Description |
|--------|------|-------------|
| `job_id` | STRING | Job ID |
| `job_name` | STRING | Job name |
| `run_as` | STRING | Execution identity |
| `total_dbus` | DECIMAL | Total DBUs |
| `total_cost_usd` | DECIMAL | Total cost |
| `total_runs` | BIGINT | Total runs |
| `cost_rank` | INT | Rank by cost (1 = highest) |

---

### user_activity_30d

30-day user activity summary.

| Column | Type | Description |
|--------|------|-------------|
| `user_email` | STRING | User email |
| `active_days` | BIGINT | Days with activity |
| `total_events` | BIGINT | Total events |
| `first_seen` | TIMESTAMP | First activity |
| `last_seen` | TIMESTAMP | Last activity |
| `services_used` | ARRAY | List of services used |
