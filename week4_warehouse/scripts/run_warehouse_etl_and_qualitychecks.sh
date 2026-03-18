#!/bin/bash

################################################################################
# COMPLETE ETL PIPELINE WITH DATA QUALITY FRAMEWORK
# This script runs all ETL steps AND the full 6-pillar data quality framework
# SINGLE SCRIPT - Everything in one place!
################################################################################

set -e  # Exit on error
set -u  # Exit on undefined variable

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}🏭 COMPLETE ETL + DATA QUALITY PIPELINE${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"

# Load environment variables
if [ -f ../.env ]; then
    echo -e "${YELLOW}📦 Loading configuration from ../.env${NC}"
    source ../.env
elif [ -f .env ]; then
    echo -e "${YELLOW}📦 Loading configuration from .env${NC}"
    source .env
fi

# Get script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
ETL_DIR="$PROJECT_ROOT/sql/etl"
LOG_DIR="$PROJECT_ROOT/logs"
REPORT_DIR="$PROJECT_ROOT/reports/data_quality"
mkdir -p "$ETL_DIR" "$LOG_DIR" "$REPORT_DIR"

LOG_FILE="$LOG_DIR/etl_$(date +%Y%m%d_%H%M%S).log"
QUALITY_LOG="$REPORT_DIR/quality_$(date +%Y%m%d_%H%M%S).log"
echo -e "${CYAN}📝 ETL Log file: $LOG_FILE${NC}"
echo -e "${CYAN}📊 Quality Log file: $QUALITY_LOG${NC}"

# Function to run SQL file with timing
run_sql() {
    local step=$1
    local file=$2
    local description=$3
    local timeout_seconds=${4:-300}
    
    echo -e "\n${YELLOW}▶️  Step $step: $description${NC}" | tee -a "$LOG_FILE"
    echo -e "${BLUE}--------------------------------------------------${NC}" | tee -a "$LOG_FILE"
    
    step_start=$(date +%s)
    
    export PGPASSWORD="$DB_PASSWORD"
    
    if [ $timeout_seconds -eq 0 ]; then
        psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -v ON_ERROR_STOP=1 -f "$file" 2>&1 | tee -a "$LOG_FILE"
        local exit_code=${PIPESTATUS[0]}
    else
        timeout $timeout_seconds psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -v ON_ERROR_STOP=1 -f "$file" 2>&1 | tee -a "$LOG_FILE"
        local exit_code=${PIPESTATUS[0]}
    fi
    
    unset PGPASSWORD
    step_end=$(date +%s)
    step_duration=$((step_end - step_start))
    
    if [ $exit_code -eq 0 ]; then
        echo -e "${GREEN}✅ Completed in ${step_duration}s${NC}" | tee -a "$LOG_FILE"
    else
        echo -e "${RED}❌ Failed at step $step${NC}" | tee -a "$LOG_FILE"
        exit $exit_code
    fi
}

# ============================================================================
# CREATE ALL ETL SCRIPTS (Steps 1-8)
# ============================================================================
echo -e "\n${YELLOW}📝 Creating ETL scripts...${NC}"

# 01_load_dim_dates.sql
cat > "$ETL_DIR/01_load_dim_dates.sql" << 'EOF'
-- ============================================================================
-- ETL 01: Load Date Dimension
-- ============================================================================
SET search_path TO warehouse;
INSERT INTO dim_date (
    date_id, date, day_of_month, day_of_week, day_name,
    day_of_year, week_of_year, week_start_date, week_end_date,
    month, month_name, month_abbr, quarter, quarter_name,
    year, is_weekend, is_last_day_of_month,
    is_last_day_of_quarter, is_last_day_of_year
)
SELECT DISTINCT
    TO_CHAR(d::DATE, 'YYYYMMDD')::INTEGER,
    d::DATE,
    EXTRACT(DAY FROM d)::INTEGER,
    EXTRACT(DOW FROM d)::INTEGER,
    TO_CHAR(d, 'Day'),
    EXTRACT(DOY FROM d)::INTEGER,
    EXTRACT(WEEK FROM d)::INTEGER,
    DATE_TRUNC('week', d)::DATE,
    (DATE_TRUNC('week', d) + INTERVAL '6 days')::DATE,
    EXTRACT(MONTH FROM d)::INTEGER,
    TO_CHAR(d, 'Month'),
    TO_CHAR(d, 'Mon'),
    EXTRACT(QUARTER FROM d)::INTEGER,
    'Q' || EXTRACT(QUARTER FROM d)::TEXT,
    EXTRACT(YEAR FROM d)::INTEGER,
    CASE WHEN EXTRACT(DOW FROM d) IN (0, 6) THEN TRUE ELSE FALSE END,
    (d = (DATE_TRUNC('month', d) + INTERVAL '1 month - 1 day')::DATE),
    (d = (DATE_TRUNC('quarter', d) + INTERVAL '3 months - 1 day')::DATE),
    (d = (DATE_TRUNC('year', d) + INTERVAL '1 year - 1 day')::DATE)
FROM (
    SELECT order_date::DATE as d FROM staging.staging_orders
    UNION
    SELECT registration_date FROM staging.staging_customers WHERE registration_date IS NOT NULL
) dates
WHERE d IS NOT NULL
AND NOT EXISTS (SELECT 1 FROM dim_date dd WHERE dd.date = d::DATE);
SELECT '📅 Date dimension: ' || COUNT(*) || ' rows' as status FROM dim_date;
EOF

# 02_load_dim_stores.sql - FIXED
cat > "$ETL_DIR/02_load_dim_stores.sql" << 'EOF'
-- ============================================================================
-- ETL 02: Load Store Dimension
-- FIXED: No ON CONFLICT
-- ============================================================================
SET search_path TO warehouse;

-- First, check if stores already exist
DO $$
DECLARE
    v_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO v_count FROM dim_store;
    
    IF v_count = 0 THEN
        -- Insert all stores (first time load)
        INSERT INTO dim_store (
            store_id, store_name, store_type, address, city, state,
            country, postal_code, latitude, longitude, region, district,
            square_footage, opening_date, manager_name, phone, is_active
        )
        SELECT 
            store_id, store_name, store_type, address, city, state,
            country, postal_code, latitude, longitude, region, district,
            square_footage, opening_date, manager_name, phone, is_active
        FROM staging.staging_stores;
        
        RAISE NOTICE 'Inserted % stores', (SELECT COUNT(*) FROM dim_store);
    ELSE
        -- Update existing stores
        UPDATE dim_store d
        SET 
            store_name = s.store_name,
            store_type = s.store_type,
            address = s.address,
            city = s.city,
            state = s.state,
            country = s.country,
            postal_code = s.postal_code,
            latitude = s.latitude,
            longitude = s.longitude,
            region = s.region,
            district = s.district,
            square_footage = s.square_footage,
            opening_date = s.opening_date,
            manager_name = s.manager_name,
            phone = s.phone,
            is_active = s.is_active,
            updated_at = CURRENT_TIMESTAMP
        FROM staging.staging_stores s
        WHERE d.store_id = s.store_id;
        
        -- Insert any new stores
        INSERT INTO dim_store (
            store_id, store_name, store_type, address, city, state,
            country, postal_code, latitude, longitude, region, district,
            square_footage, opening_date, manager_name, phone, is_active
        )
        SELECT 
            store_id, store_name, store_type, address, city, state,
            country, postal_code, latitude, longitude, region, district,
            square_footage, opening_date, manager_name, phone, is_active
        FROM staging.staging_stores s
        WHERE NOT EXISTS (
            SELECT 1 FROM dim_store d WHERE d.store_id = s.store_id
        );
        
        RAISE NOTICE 'Updated stores, total now: %', (SELECT COUNT(*) FROM dim_store);
    END IF;
END $$;

SELECT '🏪 Store dimension: ' || COUNT(*) || ' rows' as status FROM dim_store;
EOF

# 03_load_dim_customers.sql
cat > "$ETL_DIR/03_load_dim_customers.sql" << 'EOF'
-- ============================================================================
-- ETL 03: Load Customer Dimension (SCD Type 2)
-- ============================================================================
SET search_path TO warehouse;
DO $$
DECLARE
    v_updated_count INTEGER := 0;
    v_inserted_count INTEGER := 0;
BEGIN
    WITH changed_customers AS (
        SELECT s.customer_id
        FROM staging.staging_customers s
        JOIN dim_customer d ON s.customer_id = d.customer_id AND d.is_current = true
        WHERE COALESCE(s.first_name,'') <> COALESCE(d.first_name,'')
           OR COALESCE(s.last_name,'') <> COALESCE(d.last_name,'')
           OR COALESCE(s.email,'') <> COALESCE(d.email,'')
    )
    UPDATE dim_customer d
    SET is_current = false, end_date = CURRENT_DATE - 1
    FROM changed_customers c
    WHERE d.customer_id = c.customer_id AND d.is_current = true;
    GET DIAGNOSTICS v_updated_count = ROW_COUNT;
    
    INSERT INTO dim_customer (
        customer_id, first_name, last_name, full_name, email, phone,
        address_line1, address_line2, city, state, country, postal_code,
        customer_segment, registration_date, is_active, effective_date,
        end_date, is_current, source_system
    )
    SELECT 
        customer_id, first_name, last_name,
        first_name || ' ' || last_name, email, phone,
        address_line1, address_line2, city, state, country, postal_code,
        customer_segment, registration_date, is_active,
        CURRENT_DATE, NULL, true, 'STAGING'
    FROM staging.staging_customers s
    WHERE NOT EXISTS (
        SELECT 1 FROM dim_customer d 
        WHERE d.customer_id = s.customer_id AND d.is_current = true
    );
    GET DIAGNOSTICS v_inserted_count = ROW_COUNT;
    RAISE NOTICE 'Customers: % updated, % inserted', v_updated_count, v_inserted_count;
END $$;
SELECT '👥 Customer dimension: ' || COUNT(*) || ' rows' as status FROM dim_customer;
EOF

# 04_load_dim_products.sql
cat > "$ETL_DIR/04_load_dim_products.sql" << 'EOF'
-- ============================================================================
-- ETL 04: Load Product Dimension (SCD Type 2)
-- ============================================================================
SET search_path TO warehouse;
DO $$
DECLARE
    v_updated_count INTEGER := 0;
    v_inserted_count INTEGER := 0;
BEGIN
    WITH changed_products AS (
        SELECT s.product_id
        FROM staging.staging_products s
        JOIN dim_product d ON s.product_id = d.product_id AND d.is_current = true
        WHERE COALESCE(s.product_name,'') <> COALESCE(d.product_name,'')
           OR s.unit_price <> d.unit_price
    )
    UPDATE dim_product d
    SET is_current = false, end_date = CURRENT_DATE - 1
    FROM changed_products c
    WHERE d.product_id = c.product_id AND d.is_current = true;
    GET DIAGNOSTICS v_updated_count = ROW_COUNT;
    
    INSERT INTO dim_product (
        product_id, product_name, product_description, sku, barcode,
        category, subcategory, brand, unit_cost, unit_price, msrp,
        supplier_name, color, size, weight_kg, is_active,
        effective_date, end_date, is_current
    )
    SELECT 
        product_id, product_name, product_description, sku, barcode,
        category, subcategory, brand, unit_cost, unit_price, msrp,
        supplier_name, color, size, weight_kg, is_active,
        CURRENT_DATE, NULL, true
    FROM staging.staging_products s
    WHERE NOT EXISTS (
        SELECT 1 FROM dim_product d 
        WHERE d.product_id = s.product_id AND d.is_current = true
    );
    GET DIAGNOSTICS v_inserted_count = ROW_COUNT;
    RAISE NOTICE 'Products: % updated, % inserted', v_updated_count, v_inserted_count;
END $$;
SELECT '📦 Product dimension: ' || COUNT(*) || ' rows' as status FROM dim_product;
EOF

# 05_load_fact_sales.sql
cat > "$ETL_DIR/05_load_fact_sales.sql" << 'EOF'
-- ============================================================================
-- ETL 05: Load Fact Sales Table
-- ============================================================================
SET search_path TO warehouse;
CREATE INDEX IF NOT EXISTS idx_temp_orders_customer ON staging.staging_orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_temp_order_items_product ON staging.staging_order_items(product_id);
CREATE INDEX IF NOT EXISTS idx_temp_order_items_order ON staging.staging_order_items(order_id);
ANALYZE staging.staging_orders;
ANALYZE staging.staging_order_items;

DO $$
DECLARE
    v_batch_id INTEGER;
    v_inserted_count INTEGER := 0;
BEGIN
    v_batch_id := floor(extract(epoch from now()))::integer;
    
    INSERT INTO fact_sales (
        order_id, order_line_number, invoice_number, date_key,
        customer_key, product_key, store_key, payment_key,
        quantity, unit_price, unit_cost, discount_amount, tax_amount,
        shipping_amount, discount_percent, order_status, transaction_type,
        source_system, batch_id
    )
    SELECT 
        o.order_id, oi.line_number, o.invoice_number,
        TO_CHAR(o.order_date::DATE, 'YYYYMMDD')::INTEGER,
        c.customer_key, p.product_key, s.store_key, pay.payment_key,
        oi.quantity, oi.unit_price, p.unit_cost,
        oi.discount_amount, oi.tax_amount, o.shipping_amount,
        oi.discount_percent, o.order_status,
        CASE WHEN o.order_status = 'Returned' THEN 'Return' ELSE 'Sale' END,
        'STAGING', v_batch_id
    FROM staging.staging_orders o
    JOIN staging.staging_order_items oi ON o.order_id = oi.order_id
    JOIN dim_customer c ON o.customer_id = c.customer_id AND c.is_current = true
    JOIN dim_product p ON oi.product_id = p.product_id AND p.is_current = true
    LEFT JOIN dim_store s ON o.store_id = s.store_id
    LEFT JOIN dim_payment pay ON o.payment_method = pay.payment_method
    WHERE NOT EXISTS (
        SELECT 1 FROM fact_sales f 
        WHERE f.order_id = o.order_id AND f.order_line_number = oi.line_number
    );
    
    GET DIAGNOSTICS v_inserted_count = ROW_COUNT;
    RAISE NOTICE 'Fact sales: % rows inserted', v_inserted_count;
END $$;

DROP INDEX IF EXISTS staging.idx_temp_orders_customer;
DROP INDEX IF EXISTS staging.idx_temp_order_items_product;
DROP INDEX IF EXISTS staging.idx_temp_order_items_order;
SELECT '💰 Fact sales: ' || COUNT(*) || ' rows' as status FROM fact_sales;
EOF

# 06_load_fact_daily_summary.sql
cat > "$ETL_DIR/06_load_fact_daily_summary.sql" << 'EOF'
-- ============================================================================
-- ETL 06: Load Fact Daily Sales Summary
-- ============================================================================
SET search_path TO warehouse;
TRUNCATE TABLE fact_daily_sales_summary;
INSERT INTO fact_daily_sales_summary (
    date_key, store_key, product_key, total_orders, total_line_items,
    total_quantity, total_customers, new_customers, total_gross_revenue,
    total_net_revenue, total_discounts, total_tax, total_shipping,
    total_cost, total_profit
)
SELECT 
    date_key, store_key, product_key,
    COUNT(DISTINCT order_id), COUNT(*), SUM(quantity),
    COUNT(DISTINCT customer_key), 0,
    SUM(gross_revenue), SUM(net_revenue), SUM(discount_amount),
    SUM(tax_amount), SUM(shipping_amount), SUM(total_cost), SUM(gross_profit)
FROM fact_sales
GROUP BY date_key, store_key, product_key;
SELECT '📊 Daily summary: ' || COUNT(*) || ' rows' as status FROM fact_daily_sales_summary;
EOF

# 07_update_fact_metadata.sql - FIXED (removed ON CONFLICT)
cat > "$ETL_DIR/07_update_fact_metadata.sql" << 'EOF'
-- ============================================================================
-- ETL 07: Update Fact Table Metadata
-- FIXED: Removed ON CONFLICT, using upsert logic
-- ============================================================================
SET search_path TO warehouse;

-- Update fact_sales metadata
DO $$
DECLARE
    v_count INTEGER;
    v_fact_sales_rows BIGINT;
    v_fact_sales_min INTEGER;
    v_fact_sales_max INTEGER;
    v_daily_summary_rows BIGINT;
    v_daily_summary_min INTEGER;
    v_daily_summary_max INTEGER;
BEGIN
    -- Get fact_sales stats
    SELECT COUNT(*), MIN(date_key), MAX(date_key) 
    INTO v_fact_sales_rows, v_fact_sales_min, v_fact_sales_max
    FROM fact_sales;
    
    -- Get daily summary stats
    SELECT COUNT(*), MIN(date_key), MAX(date_key) 
    INTO v_daily_summary_rows, v_daily_summary_min, v_daily_summary_max
    FROM fact_daily_sales_summary;
    
    -- Check if fact_sales metadata exists
    SELECT COUNT(*) INTO v_count FROM fact_table_metadata WHERE table_name = 'fact_sales';
    
    IF v_count = 0 THEN
        INSERT INTO fact_table_metadata (table_name, grain_description, row_count, min_date_key, max_date_key, last_refresh, refresh_status)
        VALUES ('fact_sales', 'Order line item grain', v_fact_sales_rows, v_fact_sales_min, v_fact_sales_max, CURRENT_TIMESTAMP, 'COMPLETED');
    ELSE
        UPDATE fact_table_metadata 
        SET row_count = v_fact_sales_rows,
            min_date_key = v_fact_sales_min,
            max_date_key = v_fact_sales_max,
            last_refresh = CURRENT_TIMESTAMP,
            refresh_status = 'COMPLETED'
        WHERE table_name = 'fact_sales';
    END IF;
    
    -- Check if daily summary metadata exists
    SELECT COUNT(*) INTO v_count FROM fact_table_metadata WHERE table_name = 'fact_daily_sales_summary';
    
    IF v_count = 0 THEN
        INSERT INTO fact_table_metadata (table_name, grain_description, row_count, min_date_key, max_date_key, last_refresh, refresh_status)
        VALUES ('fact_daily_sales_summary', 'Daily aggregated by store/product', v_daily_summary_rows, v_daily_summary_min, v_daily_summary_max, CURRENT_TIMESTAMP, 'COMPLETED');
    ELSE
        UPDATE fact_table_metadata 
        SET row_count = v_daily_summary_rows,
            min_date_key = v_daily_summary_min,
            max_date_key = v_daily_summary_max,
            last_refresh = CURRENT_TIMESTAMP,
            refresh_status = 'COMPLETED'
        WHERE table_name = 'fact_daily_sales_summary';
    END IF;
END $$;

SELECT '📋 Metadata updated' as status;
SELECT * FROM fact_table_metadata;
EOF

# 08_verify_warehouse.sql
cat > "$ETL_DIR/08_verify_warehouse.sql" << 'EOF'
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
EOF

# ============================================================================
# CREATE DATA QUALITY FRAMEWORK SCRIPT (Step 9)
# ============================================================================
cat > "$ETL_DIR/09_data_quality_framework.sql" << 'EOF'
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
EOF

# ============================================================================
# RUN ETL PIPELINE (Steps 1-8)
# ============================================================================
echo -e "\n${YELLOW}🚀 Starting ETL Pipeline...${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"

run_sql "1" "$ETL_DIR/01_load_dim_dates.sql" "Loading additional dates" 60
run_sql "2" "$ETL_DIR/02_load_dim_stores.sql" "Loading store dimension" 60
run_sql "3" "$ETL_DIR/03_load_dim_customers.sql" "Loading customer dimension (SCD Type 2)" 120
run_sql "4" "$ETL_DIR/04_load_dim_products.sql" "Loading product dimension (SCD Type 2)" 120
run_sql "5" "$ETL_DIR/05_load_fact_sales.sql" "Loading fact sales (118K rows)" 0
run_sql "6" "$ETL_DIR/06_load_fact_daily_summary.sql" "Loading daily sales summary" 120
run_sql "7" "$ETL_DIR/07_update_fact_metadata.sql" "Updating metadata" 30
run_sql "8" "$ETL_DIR/08_verify_warehouse.sql" "Basic verification" 60

# ============================================================================
# RUN DATA QUALITY FRAMEWORK (Step 9)
# ============================================================================
echo -e "\n${YELLOW}▶️  Step 9: Running Complete Data Quality Framework (6 Pillars)${NC}"
echo -e "${BLUE}--------------------------------------------------${NC}" | tee -a "$LOG_FILE"

export PGPASSWORD="$DB_PASSWORD"
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -f "$ETL_DIR/09_data_quality_framework.sql" | tee -a "$QUALITY_LOG"
DQ_EXIT_CODE=${PIPESTATUS[0]}
unset PGPASSWORD

if [ $DQ_EXIT_CODE -ne 0 ]; then
    echo -e "\n${RED}❌ DATA QUALITY FRAMEWORK FAILED - Pipeline stopped${NC}" | tee -a "$LOG_FILE"
    echo -e "${YELLOW}📊 Check quality report: $QUALITY_LOG${NC}" | tee -a "$LOG_FILE"
    exit 1
fi

# ============================================================================
# FINAL SUMMARY
# ============================================================================
echo -e "\n${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}✅ ETL + DATA QUALITY PIPELINE COMPLETE!${NC}"
echo -e "${CYAN}📝 ETL Log: $LOG_FILE${NC}"
echo -e "${CYAN}📊 Quality Log: $QUALITY_LOG${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"

# Show final status
echo -e "\n${YELLOW}📊 Final Warehouse Status:${NC}"
export PGPASSWORD="$DB_PASSWORD"
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -P pager=off << EOF
SELECT '📊 WAREHOUSE SUMMARY' as section;
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
SELECT 'fact_sales', COUNT(*) FROM warehouse.fact_sales
UNION ALL
SELECT 'fact_daily_sales_summary', COUNT(*) FROM warehouse.fact_daily_sales_summary
ORDER BY 1;
EOF
unset PGPASSWORD

# Show quality summary from the log
echo -e "\n${YELLOW}📊 Data Quality Summary (last 15 lines):${NC}"
tail -15 "$QUALITY_LOG"

echo -e "\n${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}✅ COMPLETE PIPELINE EXECUTION FINISHED!${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
