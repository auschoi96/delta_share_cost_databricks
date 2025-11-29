-- =============================================================================
-- DELTA SHARING TEMPLATE
-- =============================================================================
-- Use this template to add new tables to Delta Share.
--
-- Instructions:
-- 1. Ensure your table exists and has data
-- 2. Run the appropriate commands on the PROVIDER workspace
-- 3. Refresh the catalog on the RECIPIENT workspace
-- =============================================================================


-- =============================================================================
-- SECTION 1: ADDING TABLES TO AN EXISTING SHARE
-- =============================================================================

-- Template: Add a single table
ALTER SHARE ${share_name} 
ADD TABLE ${catalog}.${schema}.${table_name};

-- Template: Add a table with an alias (useful for renaming)
ALTER SHARE ${share_name} 
ADD TABLE ${catalog}.${schema}.${table_name}
AS ${alias_schema}.${alias_table_name};

-- Template: Add a table with partition filter (share subset of data)
ALTER SHARE ${share_name} 
ADD TABLE ${catalog}.${schema}.${table_name}
PARTITION (workspace_id = '12345');

-- Template: Add multiple tables at once
ALTER SHARE ${share_name} 
ADD TABLE ${catalog}.${schema}.table_1;

ALTER SHARE ${share_name} 
ADD TABLE ${catalog}.${schema}.table_2;

ALTER SHARE ${share_name} 
ADD TABLE ${catalog}.${schema}.table_3;


-- =============================================================================
-- SECTION 2: ADDING VIEWS TO A SHARE (Databricks-to-Databricks only)
-- =============================================================================

-- Template: Add a view
ALTER SHARE ${share_name} 
ADD VIEW ${catalog}.${schema}.${view_name};

-- Note: Views can only be shared in Databricks-to-Databricks sharing


-- =============================================================================
-- SECTION 3: ADDING AN ENTIRE SCHEMA
-- =============================================================================

-- Template: Add all objects in a schema
-- This shares all current AND future tables in the schema
ALTER SHARE ${share_name} 
ADD SCHEMA ${catalog}.${schema};


-- =============================================================================
-- SECTION 4: REMOVING TABLES FROM A SHARE
-- =============================================================================

-- Template: Remove a table
ALTER SHARE ${share_name} 
REMOVE TABLE ${catalog}.${schema}.${table_name};

-- Template: Remove a schema
ALTER SHARE ${share_name} 
REMOVE SCHEMA ${catalog}.${schema};


-- =============================================================================
-- SECTION 5: MANAGING RECIPIENTS
-- =============================================================================

-- Template: Create a new recipient (Databricks-to-Databricks)
CREATE RECIPIENT IF NOT EXISTS ${recipient_name}
USING ID '${sharing_identifier}'
COMMENT 'Description of this recipient';

-- Template: Create a recipient with IP access list
CREATE RECIPIENT IF NOT EXISTS ${recipient_name}
USING ID '${sharing_identifier}'
PROPERTIES (
    'allowed_ip_addresses' = '10.0.0.0/8,192.168.1.0/24'
)
COMMENT 'Recipient with IP restrictions';

-- Template: Grant access to a share
GRANT SELECT ON SHARE ${share_name} 
TO RECIPIENT ${recipient_name};

-- Template: Revoke access
REVOKE SELECT ON SHARE ${share_name} 
FROM RECIPIENT ${recipient_name};


-- =============================================================================
-- SECTION 6: VERIFICATION COMMANDS
-- =============================================================================

-- View share contents
SHOW ALL IN SHARE ${share_name};

-- View share grants
SHOW GRANTS ON SHARE ${share_name};

-- View all shares
SHOW SHARES;

-- View all recipients
SHOW RECIPIENTS;

-- Get share details
DESCRIBE SHARE ${share_name};

-- Get recipient details
DESCRIBE RECIPIENT ${recipient_name};


-- =============================================================================
-- SECTION 7: COMPLETE EXAMPLE - ADDING NEW AI/ML TABLES
-- =============================================================================

-- Step 1: Add the new AI/ML monitoring tables to the share
ALTER SHARE workspace_monitoring_share 
ADD TABLE monitoring.workspace_usage.model_serving_costs;

ALTER SHARE workspace_monitoring_share 
ADD TABLE monitoring.workspace_usage.endpoint_usage_summary;

ALTER SHARE workspace_monitoring_share 
ADD TABLE monitoring.workspace_usage.foundation_model_usage;

ALTER SHARE workspace_monitoring_share 
ADD TABLE monitoring.workspace_usage.vector_search_costs;

ALTER SHARE workspace_monitoring_share 
ADD TABLE monitoring.workspace_usage.mlflow_experiments_summary;

ALTER SHARE workspace_monitoring_share 
ADD TABLE monitoring.workspace_usage.mlflow_runs_summary;

ALTER SHARE workspace_monitoring_share 
ADD TABLE monitoring.workspace_usage.aiml_executive_summary;

ALTER SHARE workspace_monitoring_share 
ADD TABLE monitoring.workspace_usage.endpoint_inventory;

-- Step 2: Verify the tables were added
SHOW ALL IN SHARE workspace_monitoring_share;


-- =============================================================================
-- SECTION 8: RECIPIENT SIDE - ACCESSING NEW SHARED TABLES
-- =============================================================================
-- Run these commands on the RECIPIENT (central) workspace

-- Refresh catalog metadata (new tables appear automatically, but may be cached)
-- Simply query the catalog to refresh:
SELECT * FROM shared_account_a.workspace_usage.model_serving_costs LIMIT 1;

-- Create consolidated views for new tables
CREATE OR REPLACE VIEW consolidated.monitoring.all_model_serving_costs AS
SELECT 'account_a' AS source, * FROM shared_account_a.workspace_usage.model_serving_costs
UNION ALL
SELECT 'account_b' AS source, * FROM shared_account_b.workspace_usage.model_serving_costs;

CREATE OR REPLACE VIEW consolidated.monitoring.all_aiml_summary AS
SELECT 'account_a' AS source, * FROM shared_account_a.workspace_usage.aiml_executive_summary
UNION ALL
SELECT 'account_b' AS source, * FROM shared_account_b.workspace_usage.aiml_executive_summary;


-- =============================================================================
-- SECTION 9: TROUBLESHOOTING
-- =============================================================================

-- Check if a table can be shared (must be managed Delta table)
DESCRIBE EXTENDED ${catalog}.${schema}.${table_name};

-- Check share history
DESCRIBE HISTORY SHARE ${share_name};

-- Check recipient activation status
SELECT * FROM system.access.audit 
WHERE action_name LIKE '%recipient%' 
ORDER BY event_time DESC 
LIMIT 100;

-- Debug sharing issues
-- 1. Verify table exists: DESCRIBE TABLE ${catalog}.${schema}.${table_name}
-- 2. Verify share exists: DESCRIBE SHARE ${share_name}
-- 3. Verify recipient exists: DESCRIBE RECIPIENT ${recipient_name}
-- 4. Check grants: SHOW GRANTS ON SHARE ${share_name}
