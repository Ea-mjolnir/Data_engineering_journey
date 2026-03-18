-- ============================================================================
-- WAREHOUSE SCHEMA INSPECTION
-- Run this to check all warehouse tables before ETL
-- ============================================================================

-- Set output formatting
\pset border 2
\pset format wrapped
\pset tuples_only off
\pset pager off

-- ============================================================================
-- 1. LIST ALL TABLES IN WAREHOUSE
-- ============================================================================
SELECT '============================================================================' as "LINE";
SELECT '1. ALL TABLES IN WAREHOUSE SCHEMA' as "WAREHOUSE TABLES";
SELECT '============================================================================' as "LINE";

SELECT 
    table_name,
    table_type
FROM information_schema.tables 
WHERE table_schema = 'warehouse' 
ORDER BY 
    CASE 
        WHEN table_name LIKE 'dim_%' THEN 1
        WHEN table_name LIKE 'fact_%' THEN 2
        ELSE 3
    END,
    table_name;

-- ============================================================================
-- 2. DIMENSION TABLES STRUCTURE
-- ============================================================================
SELECT '============================================================================' as "LINE";
SELECT '2. DIMENSION TABLES STRUCTURE' as "DIMENSION TABLES";
SELECT '============================================================================' as "LINE";

-- dim_date
SELECT '📅 dim_date - Date Dimension' as "TABLE";
SELECT 
    ordinal_position as pos,
    column_name,
    data_type,
    is_nullable,
    column_default
FROM information_schema.columns 
WHERE table_schema = 'warehouse' 
    AND table_name = 'dim_date'
ORDER BY ordinal_position;

-- dim_customer
SELECT '👥 dim_customer - Customer Dimension' as "TABLE";
SELECT 
    ordinal_position as pos,
    column_name,
    data_type,
    is_nullable,
    column_default
FROM information_schema.columns 
WHERE table_schema = 'warehouse' 
    AND table_name = 'dim_customer'
ORDER BY ordinal_position;

-- dim_product
SELECT '📦 dim_product - Product Dimension' as "TABLE";
SELECT 
    ordinal_position as pos,
    column_name,
    data_type,
    is_nullable,
    column_default
FROM information_schema.columns 
WHERE table_schema = 'warehouse' 
    AND table_name = 'dim_product'
ORDER BY ordinal_position;

-- dim_store
SELECT '🏪 dim_store - Store Dimension' as "TABLE";
SELECT 
    ordinal_position as pos,
    column_name,
    data_type,
    is_nullable,
    column_default
FROM information_schema.columns 
WHERE table_schema = 'warehouse' 
    AND table_name = 'dim_store'
ORDER BY ordinal_position;

-- dim_payment
SELECT '💳 dim_payment - Payment Dimension' as "TABLE";
SELECT 
    ordinal_position as pos,
    column_name,
    data_type,
    is_nullable,
    column_default
FROM information_schema.columns 
WHERE table_schema = 'warehouse' 
    AND table_name = 'dim_payment'
ORDER BY ordinal_position;

-- ============================================================================
-- 3. FACT TABLES STRUCTURE
-- ============================================================================
SELECT '============================================================================' as "LINE";
SELECT '3. FACT TABLES STRUCTURE' as "FACT TABLES";
SELECT '============================================================================' as "LINE";

-- fact_sales
SELECT '💰 fact_sales - Sales Fact' as "TABLE";
SELECT 
    ordinal_position as pos,
    column_name,
    data_type,
    is_nullable,
    column_default
FROM information_schema.columns 
WHERE table_schema = 'warehouse' 
    AND table_name = 'fact_sales'
ORDER BY ordinal_position;

-- fact_daily_sales_summary
SELECT '📊 fact_daily_sales_summary - Daily Sales Summary' as "TABLE";
SELECT 
    ordinal_position as pos,
    column_name,
    data_type,
    is_nullable,
    column_default
FROM information_schema.columns 
WHERE table_schema = 'warehouse' 
    AND table_name = 'fact_daily_sales_summary'
ORDER BY ordinal_position;

-- fact_inventory
SELECT '📦 fact_inventory - Inventory Fact' as "TABLE";
SELECT 
    ordinal_position as pos,
    column_name,
    data_type,
    is_nullable,
    column_default
FROM information_schema.columns 
WHERE table_schema = 'warehouse' 
    AND table_name = 'fact_inventory'
ORDER BY ordinal_position;

-- fact_table_metadata
SELECT '📋 fact_table_metadata - Metadata Tracking' as "TABLE";
SELECT 
    ordinal_position as pos,
    column_name,
    data_type,
    is_nullable,
    column_default
FROM information_schema.columns 
WHERE table_schema = 'warehouse' 
    AND table_name = 'fact_table_metadata'
ORDER BY ordinal_position;

-- ============================================================================
-- 4. VIEWS
-- ============================================================================
SELECT '============================================================================' as "LINE";
SELECT '4. VIEWS' as "VIEWS";
SELECT '============================================================================' as "LINE";

SELECT 
    table_name as view_name
FROM information_schema.views
WHERE table_schema = 'warehouse';

-- Show view definitions
SELECT 'v_sales_analysis definition:' as "VIEW";
SELECT view_definition 
FROM information_schema.views
WHERE table_schema = 'warehouse' AND table_name = 'v_sales_analysis';

-- ============================================================================
-- 5. FOREIGN KEY RELATIONSHIPS
-- ============================================================================
SELECT '============================================================================' as "LINE";
SELECT '5. FOREIGN KEY RELATIONSHIPS' as "FOREIGN KEYS";
SELECT '============================================================================' as "LINE";

SELECT
    tc.table_name,
    kcu.column_name,
    ccu.table_name AS foreign_table_name,
    ccu.column_name AS foreign_column_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
    ON tc.constraint_name = kcu.constraint_name
JOIN information_schema.constraint_column_usage ccu
    ON ccu.constraint_name = tc.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY'
    AND tc.table_schema = 'warehouse'
ORDER BY tc.table_name, kcu.ordinal_position;

-- ============================================================================
-- 6. PRIMARY KEYS
-- ============================================================================
SELECT '============================================================================' as "LINE";
SELECT '6. PRIMARY KEYS' as "PRIMARY KEYS";
SELECT '============================================================================' as "LINE";

SELECT
    tc.table_name,
    kcu.column_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
    ON tc.constraint_name = kcu.constraint_name
WHERE tc.constraint_type = 'PRIMARY KEY'
    AND tc.table_schema = 'warehouse'
ORDER BY tc.table_name, kcu.ordinal_position;

-- ============================================================================
-- 7. INDEXES
-- ============================================================================
SELECT '============================================================================' as "LINE";
SELECT '7. INDEXES' as "INDEXES";
SELECT '============================================================================' as "LINE";

SELECT
    tablename as table_name,
    indexname as index_name
FROM pg_indexes
WHERE schemaname = 'warehouse'
ORDER BY tablename, indexname;

-- ============================================================================
-- 8. CURRENT DATA COUNTS
-- ============================================================================
SELECT '============================================================================' as "LINE";
SELECT '8. CURRENT DATA COUNTS' as "DATA COUNTS";
SELECT '============================================================================' as "LINE";

SELECT 'dim_date' as table_name, COUNT(*) as row_count FROM warehouse.dim_date
UNION ALL
SELECT 'dim_customer', COUNT(*) FROM warehouse.dim_customer
UNION ALL
SELECT 'dim_product', COUNT(*) FROM warehouse.dim_product
UNION ALL
SELECT 'dim_store', COUNT(*) FROM warehouse.dim_store
UNION ALL
SELECT 'dim_payment', COUNT(*) FROM warehouse.dim_payment
UNION ALL
SELECT 'fact_sales', COUNT(*) FROM warehouse.fact_sales
UNION ALL
SELECT 'fact_daily_sales_summary', COUNT(*) FROM warehouse.fact_daily_sales_summary
UNION ALL
SELECT 'fact_inventory', COUNT(*) FROM warehouse.fact_inventory
UNION ALL
SELECT 'fact_table_metadata', COUNT(*) FROM warehouse.fact_table_metadata
ORDER BY table_name;

-- ============================================================================
-- 9. SEQUENCES
-- ============================================================================
SELECT '============================================================================' as "LINE";
SELECT '9. SEQUENCES' as "SEQUENCES";
SELECT '============================================================================' as "LINE";

SELECT
    sequence_name,
    data_type
FROM information_schema.sequences
WHERE sequence_schema = 'warehouse';

-- ============================================================================
-- 10. COLUMN COUNT SUMMARY
-- ============================================================================
SELECT '============================================================================' as "LINE";
SELECT '10. COLUMN COUNT SUMMARY' as "SUMMARY";
SELECT '============================================================================' as "LINE";

SELECT 
    table_name,
    COUNT(*) as column_count
FROM information_schema.columns 
WHERE table_schema = 'warehouse'
GROUP BY table_name
ORDER BY 
    CASE 
        WHEN table_name LIKE 'dim_%' THEN 1
        WHEN table_name LIKE 'fact_%' THEN 2
        ELSE 3
    END,
    table_name;

-- ============================================================================
-- 11. CHECK FOR NULLABLE COLUMNS THAT SHOULDN'T BE NULL
-- ============================================================================
SELECT '============================================================================' as "LINE";
SELECT '11. REQUIRED COLUMNS (NOT NULL)' as "REQUIRED FIELDS";
SELECT '============================================================================' as "LINE";

SELECT 
    table_name,
    column_name,
    data_type
FROM information_schema.columns 
WHERE table_schema = 'warehouse' 
    AND is_nullable = 'NO'
    AND column_default IS NULL
ORDER BY table_name, ordinal_position;

-- ============================================================================
-- 12. STAGING VS WAREHOUSE COMPARISON
-- ============================================================================
SELECT '============================================================================' as "LINE";
SELECT '12. STAGING VS WAREHOUSE COMPARISON' as "MAPPING CHECK";
SELECT '============================================================================' as "LINE";

-- Check if staging schema exists
SELECT 
    CASE WHEN COUNT(*) > 0 THEN '✅ Staging schema exists' 
         ELSE '❌ Staging schema missing' END as staging_status
FROM information_schema.schemata 
WHERE schema_name = 'staging';

-- List staging tables
SELECT table_name as staging_tables
FROM information_schema.tables 
WHERE table_schema = 'staging' 
    AND table_type = 'BASE TABLE'
ORDER BY table_name;

-- Count rows in staging tables
SELECT 'staging_customers' as table_name, COUNT(*) as row_count FROM staging.staging_customers
UNION ALL
SELECT 'staging_products', COUNT(*) FROM staging.staging_products
UNION ALL
SELECT 'staging_orders', COUNT(*) FROM staging.staging_orders
UNION ALL
SELECT 'staging_order_items', COUNT(*) FROM staging.staging_order_items
UNION ALL
SELECT 'staging_payments', COUNT(*) FROM staging.staging_payments
UNION ALL
SELECT 'staging_stores', COUNT(*) FROM staging.staging_stores
ORDER BY table_name;

SELECT '============================================================================' as "LINE";
SELECT 'INSPECTION COMPLETE' as "DONE";
SELECT '============================================================================' as "LINE";
