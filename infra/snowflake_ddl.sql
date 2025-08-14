-- Snowflake DDL for Predictive ETA Calculator Pipeline
-- Run this script to set up the complete database infrastructure

-- =============================================================================
-- 1. CREATE WAREHOUSE
-- =============================================================================

CREATE WAREHOUSE IF NOT EXISTS PREDICTIVE_ETA_WH
WITH
    WAREHOUSE_SIZE = 'MEDIUM'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 3
    SCALING_POLICY = 'STANDARD'
    INITIALLY_SUSPENDED = TRUE
COMMENT = 'Warehouse for Predictive ETA Calculator ETL operations';

-- =============================================================================
-- 2. CREATE DATABASE AND SCHEMAS
-- =============================================================================

CREATE DATABASE IF NOT EXISTS PREDICTIVE_ETA
COMMENT = 'Database for Predictive ETA Calculator system';

USE DATABASE PREDICTIVE_ETA;

-- Raw data schema for ingested data
CREATE SCHEMA IF NOT EXISTS RAW
COMMENT = 'Raw data from routing providers before processing';

-- Core schema for processed and aggregated data
CREATE SCHEMA IF NOT EXISTS CORE
COMMENT = 'Core business data and aggregations';

-- Staging schema for dbt intermediate models
CREATE SCHEMA IF NOT EXISTS STAGING
COMMENT = 'Staging models and intermediate transformations';

-- Marts schema for dbt analytics models
CREATE SCHEMA IF NOT EXISTS MARTS
COMMENT = 'Business mart models and fact/dimension tables';

-- Test results schema
CREATE SCHEMA IF NOT EXISTS TEST_RESULTS
COMMENT = 'dbt test results and data quality reports';

-- =============================================================================
-- 3. CREATE FILE FORMATS
-- =============================================================================

-- CSV file format for data loading
CREATE OR REPLACE FILE FORMAT RAW.CSV_FORMAT
TYPE = 'CSV'
COMPRESSION = 'AUTO'
FIELD_DELIMITER = ','
RECORD_DELIMITER = '\n'
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
TRIM_SPACE = TRUE
ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
ESCAPE = 'NONE'
ESCAPE_UNENCLOSED_FIELD = '\134'
DATE_FORMAT = 'AUTO'
TIMESTAMP_FORMAT = 'AUTO'
NULL_IF = ('\\N', 'NULL', 'null', '')
COMMENT = 'CSV file format for bulk data loading';

-- JSON file format for structured data
CREATE OR REPLACE FILE FORMAT RAW.JSON_FORMAT
TYPE = 'JSON'
COMPRESSION = 'AUTO'
ENABLE_OCTAL = FALSE
ALLOW_DUPLICATE = FALSE
STRIP_OUTER_ARRAY = TRUE
STRIP_NULL_VALUES = FALSE
IGNORE_UTF8_ERRORS = FALSE
COMMENT = 'JSON file format for API responses and metadata';

-- =============================================================================
-- 4. CREATE STAGES
-- =============================================================================

-- Internal stage for data loading
CREATE OR REPLACE STAGE RAW.PREDICTIVE_ETA_STAGE
FILE_FORMAT = RAW.CSV_FORMAT
COMMENT = 'Internal stage for ETL data loading operations';

-- Routes raw data stage
CREATE OR REPLACE STAGE RAW.ROUTES_RAW_STAGE
FILE_FORMAT = RAW.CSV_FORMAT
COMMENT = 'Stage for loading raw routing data';

-- ETA slabs stage
CREATE OR REPLACE STAGE CORE.ETA_SLABS_STAGE
FILE_FORMAT = RAW.CSV_FORMAT
COMMENT = 'Stage for loading ETA slab aggregations';

-- =============================================================================
-- 5. CREATE TABLES - RAW SCHEMA
-- =============================================================================

-- Raw routes data from routing providers
CREATE TABLE IF NOT EXISTS RAW.ROUTES_RAW (
    from_hex STRING NOT NULL,
    to_hex STRING NOT NULL,
    provider STRING NOT NULL,
    distance_m FLOAT NOT NULL,
    duration_s FLOAT NOT NULL,
    depart_ts TIMESTAMP_TZ,
    weekday STRING,
    hour INTEGER,
    ingested_at TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    request_id STRING NOT NULL,
    batch_id STRING,
    traffic_duration_s FLOAT,
    
    -- Constraints
    CONSTRAINT chk_routes_distance CHECK (distance_m >= 0),
    CONSTRAINT chk_routes_duration CHECK (duration_s > 0),
    CONSTRAINT chk_routes_hour CHECK (hour BETWEEN 0 AND 23),
    CONSTRAINT chk_routes_provider CHECK (provider IN ('osrm', 'google', 'here')),
    CONSTRAINT chk_routes_weekday CHECK (weekday IN ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday')),
    CONSTRAINT chk_routes_different_hexes CHECK (from_hex != to_hex)
)
CLUSTER BY (from_hex, to_hex, depart_ts)
COMMENT = 'Raw routing data ingested from OSRM, Google Maps, and HERE APIs';

-- =============================================================================
-- 6. CREATE TABLES - CORE SCHEMA
-- =============================================================================

-- H3 hexagonal grid lookup
CREATE TABLE IF NOT EXISTS CORE.H3_LOOKUP (
    hex_id STRING NOT NULL PRIMARY KEY,
    city STRING NOT NULL,
    resolution INTEGER NOT NULL,
    centroid_lat DOUBLE NOT NULL,
    centroid_lng DOUBLE NOT NULL,
    created_at TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    
    -- Constraints
    CONSTRAINT chk_h3_resolution CHECK (resolution BETWEEN 0 AND 15),
    CONSTRAINT chk_h3_lat CHECK (centroid_lat BETWEEN -90 AND 90),
    CONSTRAINT chk_h3_lng CHECK (centroid_lng BETWEEN -180 AND 180)
)
CLUSTER BY (city, resolution)
COMMENT = 'H3 hexagonal grid lookup with geographic information';

-- Hourly ETA aggregations (intermediate)
CREATE TABLE IF NOT EXISTS CORE.ETA_HOURLY (
    from_hex STRING NOT NULL,
    to_hex STRING NOT NULL,
    weekday STRING NOT NULL,
    hour INTEGER NOT NULL,
    min_eta_s INTEGER NOT NULL,
    max_eta_s INTEGER NOT NULL,
    avg_eta_s FLOAT NOT NULL,
    median_eta_s FLOAT,
    provider_pref STRING,
    sample_count INTEGER NOT NULL,
    updated_at TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    
    -- Constraints
    CONSTRAINT pk_eta_hourly PRIMARY KEY (from_hex, to_hex, weekday, hour),
    CONSTRAINT chk_eta_hourly_hour CHECK (hour BETWEEN 0 AND 23),
    CONSTRAINT chk_eta_hourly_weekday CHECK (weekday IN ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday')),
    CONSTRAINT chk_eta_hourly_min_max CHECK (min_eta_s <= max_eta_s),
    CONSTRAINT chk_eta_hourly_avg CHECK (avg_eta_s BETWEEN min_eta_s AND max_eta_s),
    CONSTRAINT chk_eta_hourly_samples CHECK (sample_count > 0),
    CONSTRAINT chk_eta_hourly_different_hexes CHECK (from_hex != to_hex)
)
CLUSTER BY (from_hex, to_hex, weekday)
COMMENT = 'Hourly ETA aggregations for intermediate processing';

-- Final ETA slabs for serving (production table)
CREATE TABLE IF NOT EXISTS CORE.ETA_SLABS (
    from_hex STRING NOT NULL,
    to_hex STRING NOT NULL,
    weekday STRING NOT NULL,
    slab STRING NOT NULL,
    min_eta_s INTEGER NOT NULL,
    max_eta_s INTEGER NOT NULL,
    rain_eta_s INTEGER,
    provider_pref STRING,
    sample_count INTEGER NOT NULL,
    updated_at TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    
    -- Constraints
    CONSTRAINT pk_eta_slabs PRIMARY KEY (from_hex, to_hex, weekday, slab),
    CONSTRAINT chk_eta_slabs_weekday CHECK (weekday IN ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday')),
    CONSTRAINT chk_eta_slabs_slab CHECK (slab IN ('0-4', '4-8', '8-12', '12-16', '16-20', '20-24')),
    CONSTRAINT chk_eta_slabs_min_max CHECK (min_eta_s <= max_eta_s),
    CONSTRAINT chk_eta_slabs_rain CHECK (rain_eta_s IS NULL OR rain_eta_s >= max_eta_s),
    CONSTRAINT chk_eta_slabs_samples CHECK (sample_count > 0),
    CONSTRAINT chk_eta_slabs_different_hexes CHECK (from_hex != to_hex)
)
CLUSTER BY (from_hex, to_hex, weekday, slab)
COMMENT = 'Final ETA aggregations by time slabs for serving applications';

-- Processing state tracking
CREATE TABLE IF NOT EXISTS CORE.PROCESSING_STATE (
    job_id STRING NOT NULL PRIMARY KEY,
    job_type STRING NOT NULL,
    status STRING NOT NULL,
    start_time TIMESTAMP_TZ NOT NULL,
    end_time TIMESTAMP_TZ,
    processed_records INTEGER DEFAULT 0,
    failed_records INTEGER DEFAULT 0,
    metadata VARIANT,
    error_message STRING,
    updated_at TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    
    -- Constraints
    CONSTRAINT chk_processing_status CHECK (status IN ('pending', 'running', 'completed', 'failed', 'cancelled'))
)
CLUSTER BY (job_type, start_time)
COMMENT = 'ETL processing job state tracking';

-- Backfill job definitions
CREATE TABLE IF NOT EXISTS CORE.BACKFILL_JOBS (
    job_id STRING NOT NULL PRIMARY KEY,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    hex_pairs VARIANT,
    providers VARIANT,
    status STRING NOT NULL,
    created_at TIMESTAMP_TZ NOT NULL,
    updated_at TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    
    -- Constraints
    CONSTRAINT chk_backfill_status CHECK (status IN ('pending', 'running', 'completed', 'failed', 'cancelled')),
    CONSTRAINT chk_backfill_dates CHECK (start_date <= end_date)
)
CLUSTER BY (created_at)
COMMENT = 'Backfill job definitions and tracking';

-- Watermarks for incremental processing
CREATE TABLE IF NOT EXISTS CORE.WATERMARKS (
    watermark_name STRING NOT NULL PRIMARY KEY,
    watermark_value TIMESTAMP_TZ NOT NULL,
    metadata VARIANT,
    updated_at TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Watermarks for incremental data processing';

-- =============================================================================
-- 7. CREATE VIEWS FOR EASY ACCESS
-- =============================================================================

-- View for latest ETA data with geographic context
CREATE OR REPLACE VIEW CORE.VW_ETA_WITH_GEOGRAPHY AS
SELECT 
    e.from_hex,
    e.to_hex,
    e.weekday,
    e.slab,
    e.min_eta_s,
    e.max_eta_s,
    e.rain_eta_s,
    e.provider_pref,
    e.sample_count,
    e.updated_at,
    h_from.city as from_city,
    h_from.centroid_lat as from_lat,
    h_from.centroid_lng as from_lng,
    h_to.centroid_lat as to_lat,
    h_to.centroid_lng as to_lng,
    -- Calculate straight-line distance for reference
    ST_DISTANCE(
        ST_POINT(h_from.centroid_lng, h_from.centroid_lat),
        ST_POINT(h_to.centroid_lng, h_to.centroid_lat)
    ) / 1000.0 as straight_line_distance_km
FROM CORE.ETA_SLABS e
LEFT JOIN CORE.H3_LOOKUP h_from ON e.from_hex = h_from.hex_id
LEFT JOIN CORE.H3_LOOKUP h_to ON e.to_hex = h_to.hex_id;

-- View for data quality monitoring
CREATE OR REPLACE VIEW CORE.VW_DATA_QUALITY_SUMMARY AS
SELECT 
    DATE(updated_at) as update_date,
    COUNT(*) as total_eta_records,
    COUNT(DISTINCT from_hex) as unique_from_hexes,
    COUNT(DISTINCT to_hex) as unique_to_hexes,
    SUM(sample_count) as total_samples,
    AVG(sample_count) as avg_samples_per_record,
    MIN(min_eta_s) as fastest_eta_s,
    MAX(max_eta_s) as slowest_eta_s,
    AVG((min_eta_s + max_eta_s) / 2.0) as avg_eta_s,
    COUNT(CASE WHEN rain_eta_s IS NOT NULL THEN 1 END) as records_with_rain_adjustment
FROM CORE.ETA_SLABS
GROUP BY DATE(updated_at)
ORDER BY update_date DESC;

-- =============================================================================
-- 8. CREATE MATERIALIZED VIEWS FOR PERFORMANCE
-- =============================================================================

-- Materialized view for aggregated metrics by hex
CREATE OR REPLACE MATERIALIZED VIEW CORE.MV_HEX_PERFORMANCE_SUMMARY AS
SELECT 
    from_hex,
    COUNT(DISTINCT to_hex) as reachable_destinations,
    AVG(min_eta_s) as avg_min_eta_s,
    AVG(max_eta_s) as avg_max_eta_s,
    SUM(sample_count) as total_samples,
    MAX(updated_at) as last_updated
FROM CORE.ETA_SLABS
GROUP BY from_hex;

-- =============================================================================
-- 9. CREATE INDEXES AND OPTIMIZATIONS
-- =============================================================================

-- Search optimization for common query patterns
ALTER TABLE RAW.ROUTES_RAW ADD SEARCH OPTIMIZATION;
ALTER TABLE CORE.ETA_SLABS ADD SEARCH OPTIMIZATION;
ALTER TABLE CORE.H3_LOOKUP ADD SEARCH OPTIMIZATION;

-- =============================================================================
-- 10. CREATE STREAMS FOR CHANGE DATA CAPTURE (Optional)
-- =============================================================================

-- Stream on routes_raw for real-time processing
CREATE OR REPLACE STREAM RAW.ROUTES_RAW_STREAM ON TABLE RAW.ROUTES_RAW
COMMENT = 'Stream for capturing changes to raw routes data';

-- Stream on eta_slabs for downstream systems
CREATE OR REPLACE STREAM CORE.ETA_SLABS_STREAM ON TABLE CORE.ETA_SLABS
COMMENT = 'Stream for capturing changes to ETA slabs for downstream consumption';

-- =============================================================================
-- 11. CREATE TASKS FOR AUTOMATED MAINTENANCE (Optional)
-- =============================================================================

-- Task to clean up old processing states
CREATE OR REPLACE TASK CORE.CLEANUP_PROCESSING_STATE
WAREHOUSE = PREDICTIVE_ETA_WH
SCHEDULE = 'USING CRON 0 2 * * 0'  -- Weekly on Sunday at 2 AM
AS
DELETE FROM CORE.PROCESSING_STATE 
WHERE start_time < DATEADD('day', -30, CURRENT_DATE())
  AND status IN ('completed', 'failed', 'cancelled');

-- Task to refresh materialized views
CREATE OR REPLACE TASK CORE.REFRESH_MATERIALIZED_VIEWS
WAREHOUSE = PREDICTIVE_ETA_WH
SCHEDULE = 'USING CRON 0 4 * * *'  -- Daily at 4 AM
AS
ALTER MATERIALIZED VIEW CORE.MV_HEX_PERFORMANCE_SUMMARY REFRESH;

-- =============================================================================
-- 12. GRANT INITIAL PERMISSIONS
-- =============================================================================

-- Grant usage on warehouse
GRANT USAGE ON WAREHOUSE PREDICTIVE_ETA_WH TO ROLE SYSADMIN;
GRANT OPERATE ON WAREHOUSE PREDICTIVE_ETA_WH TO ROLE SYSADMIN;

-- Grant database and schema permissions
GRANT USAGE ON DATABASE PREDICTIVE_ETA TO ROLE SYSADMIN;
GRANT USAGE ON ALL SCHEMAS IN DATABASE PREDICTIVE_ETA TO ROLE SYSADMIN;
GRANT CREATE SCHEMA ON DATABASE PREDICTIVE_ETA TO ROLE SYSADMIN;

-- Grant table permissions
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA RAW TO ROLE SYSADMIN;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA CORE TO ROLE SYSADMIN;
GRANT SELECT ON ALL VIEWS IN SCHEMA CORE TO ROLE SYSADMIN;

-- Grant stage permissions
GRANT USAGE ON ALL STAGES IN SCHEMA RAW TO ROLE SYSADMIN;
GRANT USAGE ON ALL STAGES IN SCHEMA CORE TO ROLE SYSADMIN;

-- Grant file format permissions
GRANT USAGE ON ALL FILE FORMATS IN SCHEMA RAW TO ROLE SYSADMIN;

-- Grant future permissions
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA RAW TO ROLE SYSADMIN;
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA CORE TO ROLE SYSADMIN;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA CORE TO ROLE SYSADMIN;

-- =============================================================================
-- SCRIPT COMPLETE
-- =============================================================================

-- Verify setup
SELECT 'Snowflake infrastructure setup completed successfully' as status;

-- Show created objects summary
SELECT 
    'Tables created: ' || COUNT(*) as summary
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA IN ('RAW', 'CORE')
  AND TABLE_TYPE = 'BASE TABLE';

SELECT 
    'Views created: ' || COUNT(*) as summary  
FROM INFORMATION_SCHEMA.VIEWS
WHERE TABLE_SCHEMA = 'CORE';

SELECT 
    'Stages created: ' || COUNT(*) as summary
FROM INFORMATION_SCHEMA.STAGES 
WHERE STAGE_SCHEMA IN ('RAW', 'CORE');
