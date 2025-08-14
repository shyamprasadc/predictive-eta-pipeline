-- Roles and Security Policies for Predictive ETA Calculator Pipeline
-- Run this script after snowflake_ddl.sql to set up proper security

-- =============================================================================
-- 1. CREATE CUSTOM ROLES
-- =============================================================================

-- Role for ETL operations (Airflow, dbt)
CREATE ROLE IF NOT EXISTS PREDICTIVE_ETA_ETL_ROLE
COMMENT = 'Role for ETL operations including data ingestion and transformation';

-- Role for read-only analytics access
CREATE ROLE IF NOT EXISTS PREDICTIVE_ETA_ANALYTICS_ROLE
COMMENT = 'Role for analytics and reporting with read-only access';

-- Role for API service access (read-only to serving tables)
CREATE ROLE IF NOT EXISTS PREDICTIVE_ETA_API_ROLE
COMMENT = 'Role for API services with read access to serving tables';

-- Role for data quality monitoring
CREATE ROLE IF NOT EXISTS PREDICTIVE_ETA_MONITOR_ROLE
COMMENT = 'Role for monitoring data quality and pipeline health';

-- =============================================================================
-- 2. ROLE HIERARCHY
-- =============================================================================

-- Grant roles to SYSADMIN for management
GRANT ROLE PREDICTIVE_ETA_ETL_ROLE TO ROLE SYSADMIN;
GRANT ROLE PREDICTIVE_ETA_ANALYTICS_ROLE TO ROLE SYSADMIN;
GRANT ROLE PREDICTIVE_ETA_API_ROLE TO ROLE SYSADMIN;
GRANT ROLE PREDICTIVE_ETA_MONITOR_ROLE TO ROLE SYSADMIN;

-- Set up role hierarchy
GRANT ROLE PREDICTIVE_ETA_API_ROLE TO ROLE PREDICTIVE_ETA_ANALYTICS_ROLE;
GRANT ROLE PREDICTIVE_ETA_MONITOR_ROLE TO ROLE PREDICTIVE_ETA_ANALYTICS_ROLE;

-- =============================================================================
-- 3. WAREHOUSE PERMISSIONS
-- =============================================================================

-- ETL role needs full warehouse access
GRANT USAGE, OPERATE, MONITOR ON WAREHOUSE PREDICTIVE_ETA_WH TO ROLE PREDICTIVE_ETA_ETL_ROLE;

-- Analytics role needs usage and monitor
GRANT USAGE, MONITOR ON WAREHOUSE PREDICTIVE_ETA_WH TO ROLE PREDICTIVE_ETA_ANALYTICS_ROLE;

-- API role needs limited warehouse access
GRANT USAGE ON WAREHOUSE PREDICTIVE_ETA_WH TO ROLE PREDICTIVE_ETA_API_ROLE;

-- Monitor role needs monitor access
GRANT MONITOR ON WAREHOUSE PREDICTIVE_ETA_WH TO ROLE PREDICTIVE_ETA_MONITOR_ROLE;

-- =============================================================================
-- 4. DATABASE AND SCHEMA PERMISSIONS
-- =============================================================================

-- All roles need database usage
GRANT USAGE ON DATABASE PREDICTIVE_ETA TO ROLE PREDICTIVE_ETA_ETL_ROLE;
GRANT USAGE ON DATABASE PREDICTIVE_ETA TO ROLE PREDICTIVE_ETA_ANALYTICS_ROLE;
GRANT USAGE ON DATABASE PREDICTIVE_ETA TO ROLE PREDICTIVE_ETA_API_ROLE;
GRANT USAGE ON DATABASE PREDICTIVE_ETA TO ROLE PREDICTIVE_ETA_MONITOR_ROLE;

-- Schema permissions for ETL role (full access)
GRANT USAGE, CREATE TABLE, CREATE VIEW ON SCHEMA PREDICTIVE_ETA.RAW TO ROLE PREDICTIVE_ETA_ETL_ROLE;
GRANT USAGE, CREATE TABLE, CREATE VIEW ON SCHEMA PREDICTIVE_ETA.CORE TO ROLE PREDICTIVE_ETA_ETL_ROLE;
GRANT USAGE, CREATE TABLE, CREATE VIEW ON SCHEMA PREDICTIVE_ETA.STAGING TO ROLE PREDICTIVE_ETA_ETL_ROLE;
GRANT USAGE, CREATE TABLE, CREATE VIEW ON SCHEMA PREDICTIVE_ETA.MARTS TO ROLE PREDICTIVE_ETA_ETL_ROLE;

-- Schema permissions for analytics role (read-only)
GRANT USAGE ON SCHEMA PREDICTIVE_ETA.CORE TO ROLE PREDICTIVE_ETA_ANALYTICS_ROLE;
GRANT USAGE ON SCHEMA PREDICTIVE_ETA.MARTS TO ROLE PREDICTIVE_ETA_ANALYTICS_ROLE;

-- Schema permissions for API role (limited read-only)
GRANT USAGE ON SCHEMA PREDICTIVE_ETA.CORE TO ROLE PREDICTIVE_ETA_API_ROLE;

-- Schema permissions for monitor role (monitoring access)
GRANT USAGE ON SCHEMA PREDICTIVE_ETA.RAW TO ROLE PREDICTIVE_ETA_MONITOR_ROLE;
GRANT USAGE ON SCHEMA PREDICTIVE_ETA.CORE TO ROLE PREDICTIVE_ETA_MONITOR_ROLE;

-- =============================================================================
-- 5. TABLE PERMISSIONS
-- =============================================================================

-- ETL role permissions (full access to all tables)
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA PREDICTIVE_ETA.RAW TO ROLE PREDICTIVE_ETA_ETL_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA PREDICTIVE_ETA.CORE TO ROLE PREDICTIVE_ETA_ETL_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON FUTURE TABLES IN SCHEMA PREDICTIVE_ETA.RAW TO ROLE PREDICTIVE_ETA_ETL_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON FUTURE TABLES IN SCHEMA PREDICTIVE_ETA.CORE TO ROLE PREDICTIVE_ETA_ETL_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON FUTURE TABLES IN SCHEMA PREDICTIVE_ETA.STAGING TO ROLE PREDICTIVE_ETA_ETL_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON FUTURE TABLES IN SCHEMA PREDICTIVE_ETA.MARTS TO ROLE PREDICTIVE_ETA_ETL_ROLE;

-- Analytics role permissions (read-only to processed data)
GRANT SELECT ON ALL TABLES IN SCHEMA PREDICTIVE_ETA.CORE TO ROLE PREDICTIVE_ETA_ANALYTICS_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA PREDICTIVE_ETA.MARTS TO ROLE PREDICTIVE_ETA_ANALYTICS_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA PREDICTIVE_ETA.CORE TO ROLE PREDICTIVE_ETA_ANALYTICS_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA PREDICTIVE_ETA.MARTS TO ROLE PREDICTIVE_ETA_ANALYTICS_ROLE;

-- API role permissions (read-only to serving tables)
GRANT SELECT ON TABLE PREDICTIVE_ETA.CORE.ETA_SLABS TO ROLE PREDICTIVE_ETA_API_ROLE;
GRANT SELECT ON TABLE PREDICTIVE_ETA.CORE.H3_LOOKUP TO ROLE PREDICTIVE_ETA_API_ROLE;

-- Monitor role permissions (read-only for monitoring)
GRANT SELECT ON ALL TABLES IN SCHEMA PREDICTIVE_ETA.CORE TO ROLE PREDICTIVE_ETA_MONITOR_ROLE;
GRANT SELECT ON TABLE PREDICTIVE_ETA.RAW.ROUTES_RAW TO ROLE PREDICTIVE_ETA_MONITOR_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA PREDICTIVE_ETA.CORE TO ROLE PREDICTIVE_ETA_MONITOR_ROLE;

-- =============================================================================
-- 6. VIEW PERMISSIONS
-- =============================================================================

-- All analytical roles can access views
GRANT SELECT ON ALL VIEWS IN SCHEMA PREDICTIVE_ETA.CORE TO ROLE PREDICTIVE_ETA_ANALYTICS_ROLE;
GRANT SELECT ON ALL VIEWS IN SCHEMA PREDICTIVE_ETA.CORE TO ROLE PREDICTIVE_ETA_MONITOR_ROLE;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA PREDICTIVE_ETA.CORE TO ROLE PREDICTIVE_ETA_ANALYTICS_ROLE;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA PREDICTIVE_ETA.CORE TO ROLE PREDICTIVE_ETA_MONITOR_ROLE;

-- ETL role can create and manage views
GRANT SELECT ON ALL VIEWS IN SCHEMA PREDICTIVE_ETA.CORE TO ROLE PREDICTIVE_ETA_ETL_ROLE;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA PREDICTIVE_ETA.CORE TO ROLE PREDICTIVE_ETA_ETL_ROLE;

-- API role has limited view access
GRANT SELECT ON VIEW PREDICTIVE_ETA.CORE.VW_ETA_WITH_GEOGRAPHY TO ROLE PREDICTIVE_ETA_API_ROLE;

-- =============================================================================
-- 7. STAGE AND FILE FORMAT PERMISSIONS
-- =============================================================================

-- ETL role needs full stage access
GRANT USAGE ON ALL STAGES IN SCHEMA PREDICTIVE_ETA.RAW TO ROLE PREDICTIVE_ETA_ETL_ROLE;
GRANT USAGE ON ALL STAGES IN SCHEMA PREDICTIVE_ETA.CORE TO ROLE PREDICTIVE_ETA_ETL_ROLE;
GRANT USAGE ON ALL FILE FORMATS IN SCHEMA PREDICTIVE_ETA.RAW TO ROLE PREDICTIVE_ETA_ETL_ROLE;

-- =============================================================================
-- 8. FUNCTION AND PROCEDURE PERMISSIONS (for future use)
-- =============================================================================

-- ETL role can execute stored procedures/functions
GRANT USAGE ON FUTURE FUNCTIONS IN SCHEMA PREDICTIVE_ETA.CORE TO ROLE PREDICTIVE_ETA_ETL_ROLE;
GRANT USAGE ON FUTURE PROCEDURES IN SCHEMA PREDICTIVE_ETA.CORE TO ROLE PREDICTIVE_ETA_ETL_ROLE;

-- =============================================================================
-- 9. CREATE SERVICE USERS
-- =============================================================================

-- ETL service user (for Airflow/dbt)
CREATE USER IF NOT EXISTS PREDICTIVE_ETA_ETL_USER
PASSWORD = 'CHANGE_ME_STRONG_PASSWORD_123!'
DEFAULT_ROLE = 'PREDICTIVE_ETA_ETL_ROLE'
DEFAULT_WAREHOUSE = 'PREDICTIVE_ETA_WH'
DEFAULT_NAMESPACE = 'PREDICTIVE_ETA.CORE'
MUST_CHANGE_PASSWORD = TRUE
COMMENT = 'Service user for ETL operations (Airflow, dbt)';

-- API service user
CREATE USER IF NOT EXISTS PREDICTIVE_ETA_API_USER
PASSWORD = 'CHANGE_ME_API_PASSWORD_456!'
DEFAULT_ROLE = 'PREDICTIVE_ETA_API_ROLE'
DEFAULT_WAREHOUSE = 'PREDICTIVE_ETA_WH'
DEFAULT_NAMESPACE = 'PREDICTIVE_ETA.CORE'
MUST_CHANGE_PASSWORD = TRUE
COMMENT = 'Service user for API access to serving tables';

-- Analytics service user
CREATE USER IF NOT EXISTS PREDICTIVE_ETA_ANALYTICS_USER
PASSWORD = 'CHANGE_ME_ANALYTICS_PASSWORD_789!'
DEFAULT_ROLE = 'PREDICTIVE_ETA_ANALYTICS_ROLE'
DEFAULT_WAREHOUSE = 'PREDICTIVE_ETA_WH'
DEFAULT_NAMESPACE = 'PREDICTIVE_ETA.MARTS'
MUST_CHANGE_PASSWORD = TRUE
COMMENT = 'Service user for analytics and reporting';

-- =============================================================================
-- 10. ASSIGN ROLES TO USERS
-- =============================================================================

GRANT ROLE PREDICTIVE_ETA_ETL_ROLE TO USER PREDICTIVE_ETA_ETL_USER;
GRANT ROLE PREDICTIVE_ETA_API_ROLE TO USER PREDICTIVE_ETA_API_USER;
GRANT ROLE PREDICTIVE_ETA_ANALYTICS_ROLE TO USER PREDICTIVE_ETA_ANALYTICS_USER;

-- =============================================================================
-- 11. CREATE RESOURCE MONITORS (Optional)
-- =============================================================================

-- Resource monitor for warehouse usage
CREATE OR REPLACE RESOURCE MONITOR PREDICTIVE_ETA_MONITOR
WITH 
    CREDIT_QUOTA = 1000                    -- Monthly credit limit
    FREQUENCY = 'MONTHLY'
    START_TIMESTAMP = 'IMMEDIATELY'
    TRIGGERS 
        ON 80 PERCENT DO NOTIFY             -- Notify at 80%
        ON 95 PERCENT DO SUSPEND            -- Suspend at 95%
        ON 100 PERCENT DO SUSPEND_IMMEDIATE -- Immediately suspend at 100%
COMMENT = 'Resource monitor for Predictive ETA warehouse usage';

-- Apply monitor to warehouse
ALTER WAREHOUSE PREDICTIVE_ETA_WH SET RESOURCE_MONITOR = 'PREDICTIVE_ETA_MONITOR';

-- =============================================================================
-- 12. CREATE ROW ACCESS POLICIES (Optional - for multi-tenant setup)
-- =============================================================================

-- Example row access policy for city-based access control
-- Uncomment and customize if you need multi-city access control

/*
CREATE OR REPLACE ROW ACCESS POLICY PREDICTIVE_ETA.CORE.CITY_ACCESS_POLICY AS (city STRING) RETURNS BOOLEAN ->
    CASE 
        WHEN CURRENT_ROLE() = 'PREDICTIVE_ETA_ETL_ROLE' THEN TRUE  -- ETL can access all cities
        WHEN CURRENT_ROLE() = 'PREDICTIVE_ETA_DUBAI_ROLE' AND city = 'Dubai' THEN TRUE
        WHEN CURRENT_ROLE() = 'PREDICTIVE_ETA_NYC_ROLE' AND city = 'New York' THEN TRUE
        ELSE FALSE
    END;

-- Apply policy to tables
ALTER TABLE PREDICTIVE_ETA.CORE.H3_LOOKUP ADD ROW ACCESS POLICY PREDICTIVE_ETA.CORE.CITY_ACCESS_POLICY ON (city);
*/

-- =============================================================================
-- 13. CREATE NETWORK POLICIES (Optional - for IP restrictions)
-- =============================================================================

-- Example network policy for API access
-- Uncomment and customize with your IP ranges

/*
CREATE OR REPLACE NETWORK POLICY PREDICTIVE_ETA_API_NETWORK_POLICY
ALLOWED_IP_LIST = (
    '10.0.0.0/8',          -- Internal network
    '192.168.1.0/24',      -- Office network
    '203.0.113.0/24'       -- API server network
)
BLOCKED_IP_LIST = ()
COMMENT = 'Network policy for API user access';

-- Apply to API user
ALTER USER PREDICTIVE_ETA_API_USER SET NETWORK_POLICY = 'PREDICTIVE_ETA_API_NETWORK_POLICY';
*/

-- =============================================================================
-- 14. VERIFICATION QUERIES
-- =============================================================================

-- Verify role assignments
SELECT 
    'Role Grants' as check_type,
    granted_to_type,
    grantee_name,
    role,
    granted_by
FROM INFORMATION_SCHEMA.APPLICABLE_ROLES 
WHERE role LIKE 'PREDICTIVE_ETA%'
ORDER BY grantee_name, role;

-- Verify table permissions
SELECT 
    'Table Permissions' as check_type,
    privilege,
    granted_on,
    name as object_name,
    grantee_name
FROM INFORMATION_SCHEMA.TABLE_PRIVILEGES 
WHERE grantee_name LIKE 'PREDICTIVE_ETA%'
ORDER BY grantee_name, name;

-- Verify warehouse permissions
SELECT 
    'Warehouse Permissions' as check_type,
    privilege,
    granted_on,
    name as warehouse_name,
    grantee_name
FROM INFORMATION_SCHEMA.USAGE_PRIVILEGES 
WHERE name = 'PREDICTIVE_ETA_WH'
  AND grantee_name LIKE 'PREDICTIVE_ETA%'
ORDER BY grantee_name;

-- =============================================================================
-- SECURITY RECOMMENDATIONS
-- =============================================================================

/*
SECURITY BEST PRACTICES:

1. PASSWORD MANAGEMENT:
   - Change default passwords immediately
   - Use strong, unique passwords for each service user
   - Consider using key pair authentication for service accounts
   - Rotate passwords regularly

2. ACCESS CONTROL:
   - Follow principle of least privilege
   - Regularly audit user access and permissions
   - Remove unused users and roles
   - Use specific roles rather than SYSADMIN for applications

3. NETWORK SECURITY:
   - Implement network policies to restrict IP access
   - Use VPN or private endpoints for production access
   - Monitor login attempts and unusual access patterns

4. MONITORING:
   - Set up resource monitors to control costs
   - Monitor data access patterns
   - Set up alerts for unusual activity
   - Regular security audits

5. DATA PROTECTION:
   - Consider column-level encryption for sensitive data
   - Implement data masking for non-production environments
   - Use row-level security for multi-tenant scenarios
   - Regular backup verification

6. COMPLIANCE:
   - Document access controls and procedures
   - Regular permission reviews
   - Audit trail maintenance
   - Data retention policies
*/

SELECT 'Security setup completed. Please review and implement additional security measures as needed.' as status;
