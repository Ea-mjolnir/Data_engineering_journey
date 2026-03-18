-- ============================================================================
-- Data Warehouse Schema Setup
-- Creates staging and warehouse schemas
-- ============================================================================

-- Drop schemas if they exist (for development)
DROP SCHEMA IF EXISTS staging CASCADE;
DROP SCHEMA IF EXISTS warehouse CASCADE;

-- Create schemas
CREATE SCHEMA staging;
CREATE SCHEMA warehouse;

-- Grant permissions
GRANT ALL ON SCHEMA staging TO data_engineer;
GRANT ALL ON SCHEMA warehouse TO data_engineer;

-- Set search path (so we don't have to prefix table names)
ALTER DATABASE ecommerce_warehouse SET search_path TO warehouse, staging, public;

-- Verify
SELECT schema_name 
FROM information_schema.schemata 
WHERE schema_name IN ('staging', 'warehouse');
