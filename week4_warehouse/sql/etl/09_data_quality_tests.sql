-- ============================================================================
-- ETL 09: Data Quality Tests
-- Runs comprehensive quality checks after ETL completion
-- FAILURE = Stop pipeline, data is not safe to use
-- ============================================================================

SET search_path TO warehouse;

-- Create temp table for results
DROP TABLE IF EXISTS quality_check_results;
CREATE TEMP TABLE quality_check_results (
    check_name text,
    severity text,  -- 'CRITICAL' or 'WARNING'
    status text,    -- 'PASS' or 'FAIL'
    violations bigint,
    threshold bigint
);

-- ============================================================================
-- CRITICAL CHECKS (MUST PASS - otherwise data is unusable)
-- ============================================================================

-- Check 1: No NULL foreign keys
INSERT INTO quality_check_results
SELECT 
    'NULL foreign keys' as check_name,
    'CRITICAL' as severity,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as status,
    COUNT(*) as violations,
    0 as threshold
FROM fact_sales 
WHERE customer_key IS NULL OR product_key IS NULL OR date_key IS NULL;

-- Check 2: No orphan records
INSERT INTO quality_check_results
SELECT 
    'Orphan customers' as check_name,
    'CRITICAL' as severity,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as status,
    COUNT(*) as violations,
    0 as threshold
FROM fact_sales f
LEFT JOIN dim_customer c ON f.customer_key = c.customer_key
WHERE c.customer_key IS NULL;

-- Check 3: No duplicates in fact table
INSERT INTO quality_check_results
SELECT 
    'Duplicate order lines' as check_name,
    'CRITICAL' as severity,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as status,
    COUNT(*) as violations,
    0 as threshold
FROM (
    SELECT order_id, order_line_number
    FROM fact_sales
    GROUP BY order_id, order_line_number
    HAVING COUNT(*) > 1
) dupes;

-- Check 4: No negative quantities
INSERT INTO quality_check_results
SELECT 
    'Negative quantities' as check_name,
    'CRITICAL' as severity,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as status,
    COUNT(*) as violations,
    0 as threshold
FROM fact_sales WHERE quantity < 0;

-- ============================================================================
-- WARNING CHECKS (Should investigate, but data still usable)
-- ============================================================================

-- Check 5: Unusual profit margins
INSERT INTO quality_check_results
SELECT 
    'Extreme profit margins (< -50% or > 100%)' as check_name,
    'WARNING' as severity,
    CASE WHEN COUNT(*) < 100 THEN 'PASS' ELSE 'WARN' END as status,
    COUNT(*) as violations,
    100 as threshold
FROM fact_sales WHERE profit_margin < -50 OR profit_margin > 100;

-- Check 6: Future dates
INSERT INTO quality_check_results
SELECT 
    'Future order dates' as check_name,
    'WARNING' as severity,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARN' END as status,
    COUNT(*) as violations,
    0 as threshold
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_id
WHERE d.date > CURRENT_DATE;

-- Check 7: Data freshness
INSERT INTO quality_check_results
SELECT 
    'Stale data (>24 hours old)' as check_name,
    'WARNING' as severity,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARN' END as status,
    COUNT(*) as violations,
    0 as threshold
FROM fact_table_metadata
WHERE last_refresh < NOW() - INTERVAL '24 hours'
AND table_name IN ('fact_sales', 'fact_daily_sales_summary');

-- ============================================================================
-- RESULTS SUMMARY
-- ============================================================================

-- Show all results
SELECT '📊 DATA QUALITY RESULTS' as section;
SELECT * FROM quality_check_results ORDER BY severity, status;

-- Count failures by severity
SELECT '📈 QUALITY SUMMARY' as section;
SELECT 
    severity,
    COUNT(*) as total_checks,
    SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) as failed,
    SUM(CASE WHEN status = 'WARN' THEN 1 ELSE 0 END) as warnings
FROM quality_check_results
GROUP BY severity;

-- Return exit code (1 if any CRITICAL failures)
SELECT 
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM quality_check_results 
            WHERE severity = 'CRITICAL' AND status = 'FAIL'
        ) 
        THEN 1 
        ELSE 0 
    END as should_fail_pipeline;
