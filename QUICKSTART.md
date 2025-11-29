# Quick Start Guide

## Prerequisites

1. **Databricks CLI** v0.218.0+ installed
   ```bash
   pip install databricks-cli --upgrade
   ```

2. **Permissions**: Account Admin or Metastore Admin

3. **Unity Catalog** enabled on your workspace

## Step 1: Configure Authentication

```bash
databricks configure --profile my_workspace
# Enter workspace URL and choose OAuth or PAT authentication
```

## Step 2: Configure the Bundle

Edit `databricks-system-tables-sharing/databricks.yml`:

```yaml
variables:
  # Where tables are created
  monitoring_catalog: "monitoring"
  monitoring_schema: "workspace_usage"

  # Identifiers added to every row (for cross-workspace queries)
  account_identifier: "my_account"
  workspace_identifier: "my_workspace"

  # Alerts
  notification_email: "alerts@company.com"
```

### Multi-Workspace Deployment

For multiple workspaces, use unique identifiers for each:

| Workspace | account_identifier | workspace_identifier |
|-----------|-------------------|---------------------|
| Prod Analytics | `prod_account` | `analytics` |
| Prod Data Science | `prod_account` | `data_science` |
| Dev | `dev_account` | `sandbox` |

## Step 3: Enable System Tables (First Time Only)

Run this in each workspace before deployment:

```bash
databricks workspace import \
  src/enable_system_tables.py \
  /Users/$(databricks current-user me | jq -r .userName)/enable_system_tables.py
```

Or run `src/enable_system_tables.py` manually in the Databricks UI.

## Step 4: Deploy

```bash
cd databricks-system-tables-sharing

# Validate configuration
databricks bundle validate -t dev

# Deploy
databricks bundle deploy -t dev
```

## Step 5: Run the Pipeline

```bash
# Initial run (creates all tables)
databricks bundle run monitoring_pipeline_job -t dev
```

## Step 6: Set Up Delta Sharing (Optional)

For cross-workspace visibility:

### Option A: Using the Setup Job

1. Get the sharing identifier from the recipient workspace:
   ```sql
   SELECT current_metastore();
   ```

2. Configure in `databricks.yml`:
   ```yaml
   variables:
     recipient_sharing_id: "<sharing-identifier>"
     recipient_name: "central_workspace"
   ```

3. Run the setup job:
   ```bash
   databricks bundle run setup_delta_sharing_job -t dev
   ```

### Option B: Using SQL

On the provider workspace:
```sql
-- Create share
CREATE SHARE workspace_monitoring_share;

-- Add tables
ALTER SHARE workspace_monitoring_share ADD TABLE monitoring.workspace_usage.billing_summary;
ALTER SHARE workspace_monitoring_share ADD TABLE monitoring.workspace_usage.cost_by_product;
-- Add other tables as needed

-- Create recipient
CREATE RECIPIENT central_workspace USING ID '<sharing-identifier>';
GRANT SELECT ON SHARE workspace_monitoring_share TO RECIPIENT central_workspace;
```

On the recipient workspace:
```sql
-- Create catalog from share
CREATE CATALOG shared_ws1 USING SHARE provider_name.workspace_monitoring_share;
```

## Verification

### Check Pipeline Status
```bash
databricks pipelines list | grep "System Tables"
```

### Check Tables
```sql
SELECT COUNT(*) FROM monitoring.workspace_usage.billing_summary;
SELECT MAX(usage_date) FROM monitoring.workspace_usage.billing_summary;
```

## Deployed Resources

After deployment, you'll have:

| Resource | Name Pattern |
|----------|--------------|
| DLT Pipeline | `[dev] System Tables Monitoring - {workspace}` |
| Refresh Job | `[dev] System Tables Monitoring Refresh - {workspace}` |
| Daily Job | `[dev] System Tables Daily Aggregation - {workspace}` |
| Sharing Job | `[dev] Setup Delta Sharing - {workspace}` |

## Schedule Reference

| Job | Schedule | Purpose |
|-----|----------|---------|
| Pipeline Refresh | Every 15 min | Keep data fresh |
| Daily Aggregation | 2 AM UTC | Create rollups |

To change the refresh schedule, modify `refresh_schedule` in `databricks.yml`:
- `0 */5 * * * ?` - Every 5 minutes
- `0 */15 * * * ?` - Every 15 minutes (default)
- `0 0 * * * ?` - Hourly

## Troubleshooting

**"Table not found" error**
- Run `enable_system_tables.py` first
- Check Unity Catalog permissions

**Pipeline fails with "permission denied"**
- Verify service principal has correct permissions
- Check Unity Catalog grants on system schemas

**Delta Sharing issues**
- Ensure sharing identifier is correct
- Verify Delta Sharing is enabled on metastore
