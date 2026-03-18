-- ============================================================================
-- DATA QUALITY FRAMEWORK - 6 PILLARS
-- Tests: COMPLETENESS, ACCURACY, CONSISTENCY, VALIDITY, UNIQUENESS, TIMELINESS
-- ============================================================================

SET search_path TO warehouse;

-- Create temp table for results
DROP TABLE IF EXISTS quality_results;
CREATE TEMP TABLE quality_results (
    pillar text,
    test_name text,
    status text,
    violations bigint,
    expected bigint
);

-- ============================================================================
-- PILLAR 1: COMPLETENESS
-- ============================================================================
INSERT INTO quality_results 
SELECT 'COMPLETENESS', 'NULL customer keys',
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
       COUNT(*), 0
FROM fact_sales WHERE customer_key IS NULL;

INSERT INTO quality_results 
SELECT 'COMPLETENESS', 'NULL product keys',
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
       COUNT(*), 0
FROM fact_sales WHERE product_key IS NULL;

INSERT INTO quality_results 
SELECT 'COMPLETENESS', 'NULL date keys',
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
       COUNT(*), 0
FROM fact_sales WHERE date_key IS NULL;

INSERT INTO quality_results 
SELECT 'COMPLETENESS', 'Customers missing email',
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
       COUNT(*), 0
FROM dim_customer WHERE email IS NULL AND is_current = true;

INSERT INTO quality_results 
SELECT 'COMPLETENESS', 'Orders without items',
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
       COUNT(*), 0
FROM staging.staging_orders o
LEFT JOIN staging.staging_order_items i ON o.order_id = i.order_id
WHERE i.order_id IS NULL;

-- ============================================================================
-- PILLAR 2: ACCURACY
-- ============================================================================
INSERT INTO quality_results 
SELECT 'ACCURACY', 'Negative quantities',
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
       COUNT(*), 0
FROM fact_sales WHERE quantity < 0;

INSERT INTO quality_results 
SELECT 'ACCURACY', 'Negative profit margins',
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
       COUNT(*), 0
FROM fact_sales WHERE profit_margin < 0;

INSERT INTO quality_results 
SELECT 'ACCURACY', 'Cost exceeds price',
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
       COUNT(*), 0
FROM fact_sales WHERE unit_cost > unit_price;

INSERT INTO quality_results 
SELECT 'ACCURACY', 'Gross revenue mismatch',
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
       COUNT(*), 0
FROM fact_sales WHERE ABS(gross_revenue - (quantity * unit_price)) > 0.01;

-- ============================================================================
-- PILLAR 3: CONSISTENCY
-- ============================================================================
INSERT INTO quality_results 
SELECT 'CONSISTENCY', 'Orphan customers',
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
       COUNT(*), 0
FROM fact_sales f LEFT JOIN dim_customer c ON f.customer_key = c.customer_key
WHERE c.customer_key IS NULL;

INSERT INTO quality_results 
SELECT 'CONSISTENCY', 'Orphan products',
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
       COUNT(*), 0
FROM fact_sales f LEFT JOIN dim_product p ON f.product_key = p.product_key
WHERE p.product_key IS NULL;

INSERT INTO quality_results 
SELECT 'CONSISTENCY', 'Orphan stores',
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
       COUNT(*), 0
FROM fact_sales f LEFT JOIN dim_store s ON f.store_key = s.store_key
WHERE f.store_key IS NOT NULL AND s.store_key IS NULL;

-- ============================================================================
-- PILLAR 4: VALIDITY
-- ============================================================================
INSERT INTO quality_results 
SELECT 'VALIDITY', 'Invalid email format',
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
       COUNT(*), 0
FROM dim_customer WHERE email NOT LIKE '%@%.%' AND email IS NOT NULL;

INSERT INTO quality_results 
SELECT 'VALIDITY', 'Future dates',
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
       COUNT(*), 0
FROM fact_sales f JOIN dim_date d ON f.date_key = d.date_id
WHERE d.date > CURRENT_DATE;

INSERT INTO quality_results 
SELECT 'VALIDITY', 'Profit margin out of range',
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
       COUNT(*), 0
FROM fact_sales WHERE profit_margin > 100 OR profit_margin < -100;

-- ============================================================================
-- PILLAR 5: UNIQUENESS
-- ============================================================================
INSERT INTO quality_results 
SELECT 'UNIQUENESS', 'Duplicate order lines',
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
       COUNT(*), 0
FROM (
    SELECT order_id, order_line_number
    FROM fact_sales
    GROUP BY order_id, order_line_number
    HAVING COUNT(*) > 1
) dupes;

INSERT INTO quality_results 
SELECT 'UNIQUENESS', 'Duplicate current customers',
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
       COUNT(*), 0
FROM (
    SELECT customer_id
    FROM dim_customer
    WHERE is_current = true
    GROUP BY customer_id
    HAVING COUNT(*) > 1
) dupes;

-- ============================================================================
-- PILLAR 6: TIMELINESS
-- ============================================================================
INSERT INTO quality_results 
SELECT 'TIMELINESS', 'Stale data (>24 hours)',
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARN' END,
       COUNT(*), 0
FROM fact_table_metadata
WHERE last_refresh < NOW() - INTERVAL '24 hours'
AND table_name IN ('fact_sales', 'fact_daily_sales_summary');

INSERT INTO quality_results 
SELECT 'TIMELINESS', 'Unprocessed staging data',
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARN' END,
       COUNT(*), 0
FROM staging.staging_orders
WHERE load_status != 'WAREHOUSE_LOADED'
AND loaded_at < NOW() - INTERVAL '24 hours';

-- ============================================================================
-- RESULTS SUMMARY
-- ============================================================================
SELECT '📊 DATA QUALITY RESULTS BY PILLAR' as section;
SELECT pillar, 
       COUNT(*) as tests,
       SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) as passed,
       SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) as failed,
       SUM(CASE WHEN status = 'WARN' THEN 1 ELSE 0 END) as warnings
FROM quality_results
GROUP BY pillar
ORDER BY pillar;

SELECT '📋 DETAILED RESULTS' as section;
SELECT pillar, test_name, status, violations, expected
FROM quality_results
ORDER BY pillar, 
         CASE status 
            WHEN 'FAIL' THEN 1 
            WHEN 'WARN' THEN 2 
            ELSE 3 
         END;

SELECT '📈 OVERALL SUMMARY' as section;
SELECT 
    COUNT(*) as total_tests,
    SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) as passed,
    SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) as failed,
    SUM(CASE WHEN status = 'WARN' THEN 1 ELSE 0 END) as warnings,
    ROUND(100.0 * SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) / COUNT(*), 2) as pass_rate
FROM quality_results;
