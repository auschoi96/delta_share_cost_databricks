# Databricks notebook source
# MAGIC %md
# MAGIC # Enable System Tables
# MAGIC 
# MAGIC This notebook enables all required system table schemas for the monitoring solution.
# MAGIC Run this notebook **once** in each workspace before deploying the monitoring bundle.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prerequisites
# MAGIC - You must be an **Account Admin** and **Metastore Admin** to enable system tables
# MAGIC - The workspace must be enabled for Unity Catalog

# COMMAND ----------

import requests
import json

# Get workspace context
workspace_url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)

headers = {"Authorization": f"Bearer {token}"}

print(f"Workspace URL: {workspace_url}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Metastore ID

# COMMAND ----------

# Get current metastore
metastore_id = spark.sql("SELECT current_metastore()").collect()[0][0]
print(f"Current Metastore ID: {metastore_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Available System Schemas

# COMMAND ----------

def get_system_schemas():
    """Get all available system schemas and their states"""
    response = requests.get(
        f"{workspace_url}/api/2.0/unity-catalog/metastores/{metastore_id}/systemschemas",
        headers=headers
    )
    return response.json()

schemas_info = get_system_schemas()
print("Available System Schemas:")
print("-" * 60)

for schema in schemas_info.get('schemas', []):
    print(f"  {schema['schema']:25} : {schema['state']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enable Required Schemas

# COMMAND ----------

# Schemas required for the monitoring solution
required_schemas = [
    'access',    # Audit logs, workspaces_latest
    'billing',   # Usage and list prices
    'compute',   # Cluster info
    'lakeflow',  # Job runs
    'query',     # Query history
    'serving',   # Model serving
    'mlflow',    # MLflow experiments and runs
]

def enable_schema(schema_name):
    """Enable a system schema"""
    response = requests.put(
        f"{workspace_url}/api/2.0/unity-catalog/metastores/{metastore_id}/systemschemas/{schema_name}",
        headers=headers
    )
    return response

print("Enabling System Schemas:")
print("=" * 60)

for schema in required_schemas:
    result = enable_schema(schema)
    status = "✓ Enabled" if result.status_code in [200, 201] else f"⚠ Status: {result.status_code}"
    print(f"  {schema:25} : {status}")
    if result.status_code not in [200, 201]:
        try:
            print(f"    Response: {result.json()}")
        except:
            pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Schema Enablement

# COMMAND ----------

import time
print("Waiting 10 seconds for schemas to be enabled...")
time.sleep(10)

# Check status again
schemas_info = get_system_schemas()
print("\nUpdated System Schema Status:")
print("-" * 60)

for schema in schemas_info.get('schemas', []):
    icon = "✓" if schema['state'] == 'ENABLE_COMPLETED' else "○"
    print(f"  {icon} {schema['schema']:25} : {schema['state']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grant Access to System Schemas
# MAGIC
# MAGIC By default, only account admins can access system tables.
# MAGIC Run these commands to grant access to other users/groups.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Grant access to a group (modify as needed)
# MAGIC -- GRANT USE SCHEMA ON SCHEMA system.billing TO `data-engineering`;
# MAGIC -- GRANT SELECT ON SCHEMA system.billing TO `data-engineering`;
# MAGIC
# MAGIC -- GRANT USE SCHEMA ON SCHEMA system.access TO `data-engineering`;
# MAGIC -- GRANT SELECT ON SCHEMA system.access TO `data-engineering`;
# MAGIC
# MAGIC -- GRANT USE SCHEMA ON SCHEMA system.compute TO `data-engineering`;
# MAGIC -- GRANT SELECT ON SCHEMA system.compute TO `data-engineering`;
# MAGIC
# MAGIC -- GRANT USE SCHEMA ON SCHEMA system.lakeflow TO `data-engineering`;
# MAGIC -- GRANT SELECT ON SCHEMA system.lakeflow TO `data-engineering`;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test System Tables Access

# COMMAND ----------

# Test queries to verify access
print("Testing System Table Access:")
print("=" * 60)

test_tables = [
    ("system.billing.usage", "SELECT COUNT(*) as cnt FROM system.billing.usage WHERE usage_date >= CURRENT_DATE - INTERVAL 7 DAYS"),
    ("system.billing.list_prices", "SELECT COUNT(*) as cnt FROM system.billing.list_prices"),
    ("system.access.audit", "SELECT COUNT(*) as cnt FROM system.access.audit WHERE event_date >= CURRENT_DATE - INTERVAL 1 DAY"),
    ("system.access.workspaces_latest", "SELECT COUNT(*) as cnt FROM system.access.workspaces_latest"),
    ("system.compute.clusters", "SELECT COUNT(*) as cnt FROM system.compute.clusters"),
    ("system.lakeflow.jobs", "SELECT COUNT(*) as cnt FROM system.lakeflow.jobs"),
    ("system.serving.served_entities", "SELECT COUNT(*) as cnt FROM system.serving.served_entities"),
    ("system.mlflow.experiments_latest", "SELECT COUNT(*) as cnt FROM system.mlflow.experiments_latest"),
]

for table_name, query in test_tables:
    try:
        result = spark.sql(query).collect()[0][0]
        print(f"  ✓ {table_name:40} : {result} records")
    except Exception as e:
        print(f"  ✗ {table_name:40} : Error - {str(e)[:50]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("""
╔══════════════════════════════════════════════════════════════╗
║                    SETUP COMPLETE                             ║
╠══════════════════════════════════════════════════════════════╣
║                                                               ║
║  System tables have been enabled for this workspace.          ║
║                                                               ║
║  Next Steps:                                                  ║
║  1. Deploy the monitoring bundle:                             ║
║     > databricks bundle deploy -t prod                        ║
║                                                               ║
║  2. Run the monitoring pipeline:                              ║
║     > databricks bundle run monitoring_pipeline_job -t prod   ║
║                                                               ║
║  3. Set up Delta Sharing using the SQL scripts in:            ║
║     src/sql/delta_sharing_setup.sql                           ║
║                                                               ║
╚══════════════════════════════════════════════════════════════╝
""")
