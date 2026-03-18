#!/bin/bash
# scripts/reset_warehouse_for_testing.sh

echo "🧹 Resetting Warehouse for Clean Test"
echo "======================================"

# Load environment variables
source ../.env

export PGPASSWORD="$DB_PASSWORD"

# Truncate all warehouse tables in correct order (fact tables first, then dimensions)
psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" << EOF
-- Disable triggers temporarily to avoid foreign key issues
SET session_replication_role = 'replica';

-- Truncate fact tables first
TRUNCATE TABLE warehouse.fact_sales CASCADE;
TRUNCATE TABLE warehouse.fact_daily_sales_summary CASCADE;
TRUNCATE TABLE warehouse.fact_inventory CASCADE;

-- Then dimension tables
TRUNCATE TABLE warehouse.dim_customer CASCADE;
TRUNCATE TABLE warehouse.dim_product CASCADE;
TRUNCATE TABLE warehouse.dim_store CASCADE;
TRUNCATE TABLE warehouse.dim_date CASCADE;
-- dim_payment stays (it's pre-populated)

-- Reset sequences to start from 1
ALTER SEQUENCE warehouse.dim_customer_customer_key_seq RESTART WITH 1;
ALTER SEQUENCE warehouse.dim_product_product_key_seq RESTART WITH 1;
ALTER SEQUENCE warehouse.dim_store_store_key_seq RESTART WITH 1;
ALTER SEQUENCE warehouse.fact_sales_sale_key_seq RESTART WITH 1;
ALTER SEQUENCE warehouse.fact_daily_sales_summary_summary_key_seq RESTART WITH 1;

-- Re-enable triggers
SET session_replication_role = 'origin';

-- Show empty tables
SELECT '📊 Warehouse is now empty:' as status;
SELECT 'dim_customer', COUNT(*) FROM warehouse.dim_customer
UNION ALL
SELECT 'dim_product', COUNT(*) FROM warehouse.dim_product
UNION ALL
SELECT 'dim_store', COUNT(*) FROM warehouse.dim_store
UNION ALL
SELECT 'dim_date', COUNT(*) FROM warehouse.dim_date
UNION ALL
SELECT 'dim_payment', COUNT(*) FROM warehouse.dim_payment
UNION ALL
SELECT 'fact_sales', COUNT(*) FROM warehouse.fact_sales;
EOF

unset PGPASSWORD

echo ""
echo "✅ Warehouse reset complete! Ready for fresh ETL test."
