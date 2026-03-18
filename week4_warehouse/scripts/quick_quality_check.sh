#!/bin/bash
# scripts/quick_quality_check.sh

echo "🔍 Quick Data Quality Check"
echo "============================"

# Run just the critical tests
psql -U data_engineer -d ecommerce_warehouse -h localhost <<EOF
SET search_path TO warehouse;

SELECT '📊 CRITICAL DATA QUALITY CHECKS' as section;

SELECT '1. NULL foreign keys:' as check,
       CASE WHEN COUNT(*) = 0 THEN '✅ PASS' ELSE '❌ FAIL' END as status,
       COUNT(*) as violations
FROM fact_sales WHERE customer_key IS NULL OR product_key IS NULL OR date_key IS NULL;

SELECT '2. Negative quantities:' as check,
       CASE WHEN COUNT(*) = 0 THEN '✅ PASS' ELSE '❌ FAIL' END as status,
       COUNT(*) as violations
FROM fact_sales WHERE quantity < 0;

SELECT '3. Invalid profit margins:' as check,
       CASE WHEN COUNT(*) = 0 THEN '✅ PASS' ELSE '❌ FAIL' END as status,
       COUNT(*) as violations
FROM fact_sales WHERE profit_margin < -100 OR profit_margin > 100;

SELECT '4. Orphan records:' as check,
       CASE WHEN COUNT(*) = 0 THEN '✅ PASS' ELSE '❌ FAIL' END as status,
       COUNT(*) as violations
FROM fact_sales f LEFT JOIN dim_customer c ON f.customer_key = c.customer_key
WHERE c.customer_key IS NULL;

SELECT '5. Duplicate orders:' as check,
       CASE WHEN COUNT(*) = 0 THEN '✅ PASS' ELSE '❌ FAIL' END as status,
       COUNT(*) as violations
FROM (
    SELECT order_id, order_line_number, COUNT(*)
    FROM fact_sales
    GROUP BY order_id, order_line_number
    HAVING COUNT(*) > 1
) dupes;

SELECT '6. Data freshness:' as check,
       CASE WHEN COUNT(*) = 0 THEN '✅ PASS' ELSE '⚠️  WARNING' END as status,
       COUNT(*) as tables_stale
FROM fact_table_metadata
WHERE last_refresh < NOW() - INTERVAL '24 hours';
EOF
