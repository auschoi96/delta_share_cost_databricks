# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Sharing Setup
# MAGIC 
# MAGIC This notebook programmatically sets up Delta Sharing for the monitoring tables.
# MAGIC 
# MAGIC **Prerequisites:**
# MAGIC - The DLT pipeline must have run at least once (tables must exist)
# MAGIC - You must have permissions to create shares and recipients
# MAGIC 
# MAGIC **What this notebook does:**
# MAGIC 1. Creates the share (if it doesn't exist)
# MAGIC 2. Adds all monitoring tables to the share
# MAGIC 3. Optionally creates a recipient and grants access

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Get parameters from job or widgets (defaults match databricks.yml dev target)
dbutils.widgets.text("monitoring_catalog", "demo_catalog")
dbutils.widgets.text("monitoring_schema", "demo_data_example")
dbutils.widgets.text("share_name", "workspace_monitoring_share")
dbutils.widgets.text("recipient_sharing_id", "")
dbutils.widgets.text("recipient_name", "central_monitoring_workspace")

monitoring_catalog = dbutils.widgets.get("monitoring_catalog")
monitoring_schema = dbutils.widgets.get("monitoring_schema")
share_name = dbutils.widgets.get("share_name")
recipient_sharing_id = dbutils.widgets.get("recipient_sharing_id")
recipient_name = dbutils.widgets.get("recipient_name")

full_schema = f"{monitoring_catalog}.{monitoring_schema}"

print(f"Setting up Delta Sharing:")
print(f"  Schema: {full_schema}")
print(f"  Share: {share_name}")
print(f"  Recipient: {recipient_name if recipient_sharing_id else '(not configured)'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define All Monitoring Tables
# MAGIC 
# MAGIC Complete list of tables to add to the share.

# COMMAND ----------

# All monitoring tables organized by category
MONITORING_TABLES = {
    "billing_cost": [
        "billing_usage_bronze",
        "list_prices",
        "billing_summary",
        "cost_by_product",
        "cost_by_sku",
        "cost_by_job",
        "cost_by_user",
    ],
    "jobs_workflows": [
        "jobs_bronze",
        "job_runs_bronze",
        "jobs_current",
        "job_runs_summary",
    ],
    "compute_clusters": [
        "clusters_bronze",
        "clusters_current",
        "cluster_utilization",
    ],
    "audit_activity": [
        "audit_logs_bronze",
        "user_activity_summary",
        "data_access_summary",
    ],
    "model_serving": [
        "served_entities_bronze",
        "served_entities_current",
        "endpoint_usage_bronze",
        "endpoint_usage_summary",
        "model_serving_costs",
        "foundation_model_usage",
        "foundation_model_costs",
        "external_model_usage",
        "endpoint_inventory",
    ],
    "mlflow": [
        "mlflow_experiments_bronze",
        "mlflow_runs_bronze",
        "mlflow_metrics_bronze",
        "mlflow_experiments_summary",
        "mlflow_runs_summary",
    ],
    "vector_search": [
        "vector_search_costs",
        "vector_search_summary",
    ],
    "executive_metadata": [
        "workspace_info",
        "executive_summary",
        "aiml_executive_summary",
    ],
    "daily_rollups": [
        "cost_weekly_rollup",
        "cost_monthly_rollup",
        "job_success_rate_weekly",
        "cost_trend_analysis",
    ],
}

# Flatten to single list
all_tables = []
for category, tables in MONITORING_TABLES.items():
    all_tables.extend(tables)

print(f"Total tables to share: {len(all_tables)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Share

# COMMAND ----------

try:
    spark.sql(f"""
        CREATE SHARE IF NOT EXISTS {share_name}
        COMMENT 'System tables monitoring data - auto-configured'
    """)
    print(f"✓ Share '{share_name}' ready")
except Exception as e:
    print(f"✗ Error creating share: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Verify Tables Exist

# COMMAND ----------

# Check which tables exist and determine their types
# DLT creates STREAMING TABLEs for readStream sources, MATERIALIZED VIEWs for others
existing_objects = {}  # {table_name: object_type}
missing_tables = []

for table_name in all_tables:
    full_table = f"{full_schema}.{table_name}"
    try:
        # Query information_schema to get the actual object type
        result = spark.sql(f"""
            SELECT table_type
            FROM {monitoring_catalog}.information_schema.tables
            WHERE table_schema = '{monitoring_schema}'
            AND table_name = '{table_name}'
        """).collect()

        if result:
            table_type = result[0]["table_type"]
            # Map information_schema types to Delta Sharing types
            if table_type in ("MANAGED", "EXTERNAL", "STREAMING_TABLE"):
                existing_objects[table_name] = "TABLE"
            elif table_type == "MATERIALIZED_VIEW":
                existing_objects[table_name] = "MATERIALIZED VIEW"
            elif table_type == "VIEW":
                existing_objects[table_name] = "VIEW"
            else:
                existing_objects[table_name] = "TABLE"  # Default fallback
        else:
            missing_tables.append(table_name)
    except Exception as e:
        # Fallback: try to detect type from DESCRIBE EXTENDED
        try:
            desc = spark.sql(f"DESCRIBE EXTENDED {full_table}").collect()
            type_row = [r for r in desc if r["col_name"] == "Type"]
            if type_row:
                obj_type = type_row[0]["data_type"].upper()
                if "STREAMING" in obj_type or "TABLE" in obj_type:
                    existing_objects[table_name] = "TABLE"
                elif "MATERIALIZED" in obj_type:
                    existing_objects[table_name] = "MATERIALIZED VIEW"
                else:
                    existing_objects[table_name] = "TABLE"
            else:
                existing_objects[table_name] = "TABLE"
        except Exception:
            missing_tables.append(table_name)

print(f"Objects found: {len(existing_objects)}/{len(all_tables)}")

# Show breakdown by type
type_counts = {}
for obj_type in existing_objects.values():
    type_counts[obj_type] = type_counts.get(obj_type, 0) + 1
for obj_type, count in type_counts.items():
    print(f"  - {obj_type}: {count}")

if missing_tables:
    print(f"\n⚠ Missing objects (will be skipped):")
    for t in missing_tables:
        print(f"  - {t}")
    print("\nNote: Daily rollup tables are created by the daily_aggregation_job.")
    print("Run that job first if you need those tables in the share.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Add Tables to Share

# COMMAND ----------

# Get current objects in share (tables and materialized views)
try:
    current_share_objects = spark.sql(f"SHOW ALL IN SHARE {share_name}").collect()
    objects_in_share = {row["name"].split(".")[-1] for row in current_share_objects}
except Exception:
    objects_in_share = set()

print(f"Objects already in share: {len(objects_in_share)}")

# Add objects to share using correct syntax based on type
added_count = 0
skipped_count = 0
error_count = 0

for table_name, obj_type in existing_objects.items():
    full_table = f"{full_schema}.{table_name}"

    # Skip if already in share
    if table_name in objects_in_share:
        print(f"  ○ {table_name} (already in share)")
        skipped_count += 1
        continue

    # Skip regular VIEWs - they're not supported in Delta Sharing
    if obj_type == "VIEW":
        print(f"  ○ {table_name} (VIEW - not supported in Delta Sharing)")
        skipped_count += 1
        continue

    try:
        # Use correct ALTER SHARE syntax based on object type
        if obj_type == "MATERIALIZED VIEW":
            spark.sql(f"ALTER SHARE {share_name} ADD MATERIALIZED VIEW {full_table}")
        else:
            spark.sql(f"ALTER SHARE {share_name} ADD TABLE {full_table}")
        print(f"  ✓ {table_name} ({obj_type})")
        added_count += 1
    except Exception as e:
        error_str = str(e)
        if "already exists" in error_str.lower():
            print(f"  ○ {table_name} (already exists)")
            skipped_count += 1
        else:
            print(f"  ✗ {table_name}: {e}")
            error_count += 1

print(f"\nSummary:")
print(f"  Added: {added_count}")
print(f"  Already in share: {skipped_count}")
print(f"  Errors: {error_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create Recipient (Optional)

# COMMAND ----------

if recipient_sharing_id and recipient_sharing_id.strip():
    # Sanitize recipient name - remove any backticks and replace hyphens with underscores
    clean_recipient_name = recipient_name.replace('`', '').replace('-', '_')

    # Validate sharing ID format: should be <cloud>:<region>:<uuid>
    sharing_id_parts = recipient_sharing_id.split(':')
    if len(sharing_id_parts) != 3:
        print(f"⚠ Invalid sharing ID format: '{recipient_sharing_id}'")
        print(f"   Expected format: <cloud>:<region>:<uuid>")
        print(f"   Example: aws:us-east-1:12345678-1234-1234-1234-123456789abc")
        print(f"\n   To get the correct sharing ID, run on the RECIPIENT workspace:")
        print(f"   SELECT current_metastore();")
    else:
        cloud, region, uuid = sharing_id_parts
        print(f"Recipient configuration:")
        print(f"  Name: {clean_recipient_name}")
        print(f"  Sharing ID: {recipient_sharing_id}")
        print(f"    Cloud: {cloud}")
        print(f"    Region: {region}")
        print(f"    Metastore UUID: {uuid}")

        # First check existing recipients
        try:
            recipients_df = spark.sql("SHOW RECIPIENTS")
            print(f"  Available columns: {recipients_df.columns}")
            recipients = recipients_df.collect()

            # Show all existing recipients for debugging
            print(f"  Existing recipients ({len(recipients)}):")
            existing_recipient = None
            existing_databricks_recipients = []
            for r in recipients:
                row_dict = r.asDict()
                recipient_name_value = row_dict.get("recipient") or row_dict.get("name")
                auth_type = row_dict.get("authentication_type", "")
                print(f"    - {recipient_name_value} (auth: {auth_type})")

                # Track Databricks-to-Databricks recipients (they use sharing IDs)
                if auth_type == "DATABRICKS":
                    existing_databricks_recipients.append(recipient_name_value)

                if recipient_name_value == clean_recipient_name:
                    existing_recipient = r

            # If there are existing DATABRICKS recipients, one of them likely has our sharing ID
            if existing_databricks_recipients and not existing_recipient:
                print(f"\n  ⚠ Found {len(existing_databricks_recipients)} existing Databricks-to-Databricks recipient(s).")
                print(f"    The sharing ID may already be linked to one of these.")
                print(f"    Consider using an existing recipient name in your config.")

            if existing_recipient:
                print(f"✓ Recipient '{clean_recipient_name}' already exists (found in SHOW RECIPIENTS)")
                recipient_ready = True
            else:
                # Try to create recipient WITHOUT IF NOT EXISTS to get actual error
                print(f"\nCreating recipient '{clean_recipient_name}'...")
                try:
                    spark.sql(f"""
                        CREATE RECIPIENT `{clean_recipient_name}`
                        USING ID '{recipient_sharing_id}'
                        COMMENT 'Central monitoring workspace - auto-configured'
                    """)
                    print(f"✓ Recipient '{clean_recipient_name}' created successfully")
                    recipient_ready = True
                except Exception as create_error:
                    error_str = str(create_error)
                    error_lower = error_str.lower()

                    # Always print the actual error first for debugging
                    print(f"✗ CREATE RECIPIENT failed.")
                    print(f"   Raw error: {error_str[:500]}")  # First 500 chars

                    # Check if sharing ID is already in use by another recipient
                    if "already exists" in error_lower:
                        print(f"\n   ➜ A recipient with this sharing ID already exists!")
                        print(f"   ➜ Use one of the existing DATABRICKS recipients listed above.")
                        if existing_databricks_recipients:
                            print(f"   ➜ Suggested: Change 'recipient_name' in databricks.yml to: {existing_databricks_recipients[0]}")
                    else:
                        print(f"\nCommon causes:")
                        print(f"  1. Invalid sharing ID - run 'SELECT current_metastore();' on RECIPIENT workspace")
                        print(f"  2. Cross-region sharing not supported - both must be in same cloud region")
                        print(f"  3. Missing CREATE RECIPIENT privilege - contact metastore admin")

                    recipient_ready = False

            # Grant access if recipient is ready
            if recipient_ready:
                try:
                    spark.sql(f"""
                        GRANT SELECT ON SHARE `{share_name}` TO RECIPIENT `{clean_recipient_name}`
                    """)
                    print(f"✓ Granted SELECT on share '{share_name}' to recipient '{clean_recipient_name}'")
                except Exception as grant_error:
                    error_str = str(grant_error)
                    if "already" in error_str.lower():
                        print(f"✓ Recipient already has access to share")
                    else:
                        print(f"✗ Failed to grant access: {grant_error}")

        except Exception as e:
            print(f"⚠ Recipient setup error: {e}")
else:
    print("○ Recipient not configured (recipient_sharing_id is empty)")
    print("  To configure later:")
    print(f"  1. Get sharing ID from recipient workspace: SELECT current_metastore();")
    print(f"  2. Run: CREATE RECIPIENT <name> USING ID '<sharing-id>';")
    print(f"  3. Run: GRANT SELECT ON SHARE `{share_name}` TO RECIPIENT <name>;")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Verify Share Contents

# COMMAND ----------

print(f"\n{'=' * 60}")
print(f"SHARE SUMMARY: {share_name}")
print(f"{'=' * 60}\n")

# Show share contents
share_contents = spark.sql(f"SHOW ALL IN SHARE {share_name}")
display(share_contents)

# COMMAND ----------

# Show grants
print("\nGRANTS ON SHARE:")
try:
    grants = spark.sql(f"SHOW GRANTS ON SHARE {share_name}")
    display(grants)
except Exception as e:
    print(f"Could not retrieve grants: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC ### On the Recipient Workspace:
# MAGIC 
# MAGIC 1. **View available shares:**
# MAGIC    ```sql
# MAGIC    SHOW PROVIDERS;
# MAGIC    SHOW SHARES IN PROVIDER <provider_name>;
# MAGIC    ```
# MAGIC 
# MAGIC 2. **Create catalog from share:**
# MAGIC    ```sql
# MAGIC    CREATE CATALOG shared_<workspace_name>
# MAGIC    USING SHARE <provider_name>.<share_name>;
# MAGIC    ```
# MAGIC 
# MAGIC 3. **Query shared data:**
# MAGIC    ```sql
# MAGIC    SELECT * FROM shared_<workspace_name>.<schema>.billing_summary;
# MAGIC    ```

# COMMAND ----------

print("\n✓ Delta Sharing setup complete!")
print(f"\nShare name: {share_name}")
print(f"Tables shared: {added_count + skipped_count}")
if recipient_sharing_id:
    print(f"Recipient: {recipient_name}")
