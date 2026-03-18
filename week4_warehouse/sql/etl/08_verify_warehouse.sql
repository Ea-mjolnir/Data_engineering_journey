-- ============================================================================
-- ETL 08: Verify Warehouse Load
-- ============================================================================
SET search_path TO warehouse;
SELECT '📊 FINAL WAREHOUSE COUNTS' as section;
SELECT 'dim_customer', COUNT(*) FROM dim_customer
UNION ALL
SELECT 'dim_product', COUNT(*) FROM dim_product
UNION ALL
SELECT 'dim_store', COUNT(*) FROM dim_store
UNION ALL
SELECT 'dim_date', COUNT(*) FROM dim_date
UNION ALL
SELECT 'dim_payment', COUNT(*) FROM dim_payment
UNION ALL
SELECT 'fact_sales', COUNT(*) FROM fact_sales
UNION ALL
SELECT 'fact_daily_sales_summary', COUNT(*) FROM fact_daily_sales_summary
ORDER BY 1;
