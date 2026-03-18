#!/bin/bash

################################################################################
# Master ETL Script - Load Warehouse from Staging
# This script runs all ETL steps in the correct order
# Based on warehouse schema inspection from 2026-03-04
# OPTIMIZED: Fixed Step 7 metadata update (removed ON CONFLICT)
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
echo -e "${GREEN}🏭 ETL Pipeline: Staging → Warehouse${NC}"
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
mkdir -p "$ETL_DIR" "$LOG_DIR"

LOG_FILE="$LOG_DIR/etl_$(date +%Y%m%d_%H%M%S).log"
echo -e "${CYAN}📝 Log file: $LOG_FILE${NC}"

# Function to run SQL file with timing and optional timeout
run_sql() {
    local step=$1
    local file=$2
    local description=$3
    local timeout_seconds=${4:-300}  # Default 5 minute timeout
    
    echo -e "\n${YELLOW}▶️  Step $step: $description${NC}"
    echo -e "${BLUE}--------------------------------------------------${NC}" | tee -a "$LOG_FILE"
    
    if [ $timeout_seconds -eq 0 ]; then
        echo -e "${CYAN}⏱️  No timeout (will run until complete)${NC}" | tee -a "$LOG_FILE"
    else
        echo -e "${CYAN}⏱️  Timeout: ${timeout_seconds}s${NC}" | tee -a "$LOG_FILE"
    fi
    
    step_start=$(date +%s)
    
    export PGPASSWORD="$DB_PASSWORD"
    
    if [ $timeout_seconds -eq 0 ]; then
        # Run without timeout
        psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -v ON_ERROR_STOP=1 -f "$file" 2>&1 | tee -a "$LOG_FILE"
        local exit_code=${PIPESTATUS[0]}
    else
        # Run with timeout
        timeout $timeout_seconds psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -v ON_ERROR_STOP=1 -f "$file" 2>&1 | tee -a "$LOG_FILE"
        local exit_code=${PIPESTATUS[0]}
    fi
    
    unset PGPASSWORD
    
    step_end=$(date +%s)
    step_duration=$((step_end - step_start))
    
    if [ $exit_code -eq 0 ]; then
        echo -e "${GREEN}✅ Completed in ${step_duration}s${NC}" | tee -a "$LOG_FILE"
    elif [ $exit_code -eq 124 ]; then
        echo -e "${RED}❌ Step $step timed out after ${timeout_seconds}s${NC}" | tee -a "$LOG_FILE"
        exit $exit_code
    else
        echo -e "${RED}❌ Failed at step $step with exit code $exit_code${NC}" | tee -a "$LOG_FILE"
        exit $exit_code
    fi
}

# ============================================================================
# CREATE ALL ETL SCRIPTS
# ============================================================================

echo -e "\n${YELLOW}📝 Creating ETL scripts...${NC}"

# 01_load_dim_dates.sql - Only insert missing dates
cat > "$ETL_DIR/01_load_dim_dates.sql" << 'EOF'
-- ============================================================================
-- ETL 01: Load Date Dimension
-- Adds any missing dates from staging data
-- ============================================================================

SET search_path TO warehouse;

-- Insert missing dates from orders
INSERT INTO dim_date (
    date_id,
    date,
    day_of_month,
    day_of_week,
    day_name,
    day_of_year,
    week_of_year,
    week_start_date,
    week_end_date,
    month,
    month_name,
    month_abbr,
    quarter,
    quarter_name,
    year,
    is_weekend,
    is_last_day_of_month,
    is_last_day_of_quarter,
    is_last_day_of_year
)
SELECT DISTINCT
    TO_CHAR(d::DATE, 'YYYYMMDD')::INTEGER as date_id,
    d::DATE as date,
    EXTRACT(DAY FROM d)::INTEGER as day_of_month,
    EXTRACT(DOW FROM d)::INTEGER as day_of_week,
    TO_CHAR(d, 'Day') as day_name,
    EXTRACT(DOY FROM d)::INTEGER as day_of_year,
    EXTRACT(WEEK FROM d)::INTEGER as week_of_year,
    DATE_TRUNC('week', d)::DATE as week_start_date,
    (DATE_TRUNC('week', d) + INTERVAL '6 days')::DATE as week_end_date,
    EXTRACT(MONTH FROM d)::INTEGER as month,
    TO_CHAR(d, 'Month') as month_name,
    TO_CHAR(d, 'Mon') as month_abbr,
    EXTRACT(QUARTER FROM d)::INTEGER as quarter,
    'Q' || EXTRACT(QUARTER FROM d)::TEXT as quarter_name,
    EXTRACT(YEAR FROM d)::INTEGER as year,
    CASE WHEN EXTRACT(DOW FROM d) IN (0, 6) THEN TRUE ELSE FALSE END as is_weekend,
    (d = (DATE_TRUNC('month', d) + INTERVAL '1 month - 1 day')::DATE) as is_last_day_of_month,
    (d = (DATE_TRUNC('quarter', d) + INTERVAL '3 months - 1 day')::DATE) as is_last_day_of_quarter,
    (d = (DATE_TRUNC('year', d) + INTERVAL '1 year - 1 day')::DATE) as is_last_day_of_year
FROM (
    SELECT order_date::DATE as d FROM staging.staging_orders
    UNION
    SELECT registration_date FROM staging.staging_customers WHERE registration_date IS NOT NULL
    UNION
    SELECT payment_date FROM staging.staging_payments WHERE payment_date IS NOT NULL
    UNION
    SELECT opening_date FROM staging.staging_stores WHERE opening_date IS NOT NULL
) dates
WHERE d IS NOT NULL
AND NOT EXISTS (
    SELECT 1 FROM dim_date dd WHERE dd.date = d::DATE
)
ORDER BY d;

SELECT '📅 Date dimension: ' || COUNT(*) || ' total rows' as status FROM dim_date;
EOF

# 02_load_dim_stores.sql - Load store dimension
cat > "$ETL_DIR/02_load_dim_stores.sql" << 'EOF'
-- ============================================================================
-- ETL 02: Load Store Dimension
-- Simple dimension load (no SCD for stores)
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
            store_id,
            store_name,
            store_type,
            address,
            city,
            state,
            country,
            postal_code,
            latitude,
            longitude,
            region,
            district,
            square_footage,
            opening_date,
            manager_name,
            phone,
            is_active
        )
        SELECT 
            s.store_id,
            s.store_name,
            s.store_type,
            s.address,
            s.city,
            s.state,
            s.country,
            s.postal_code,
            s.latitude,
            s.longitude,
            s.region,
            s.district,
            s.square_footage,
            s.opening_date,
            s.manager_name,
            s.phone,
            s.is_active
        FROM staging.staging_stores s;
        
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
            store_id,
            store_name,
            store_type,
            address,
            city,
            state,
            country,
            postal_code,
            latitude,
            longitude,
            region,
            district,
            square_footage,
            opening_date,
            manager_name,
            phone,
            is_active
        )
        SELECT 
            s.store_id,
            s.store_name,
            s.store_type,
            s.address,
            s.city,
            s.state,
            s.country,
            s.postal_code,
            s.latitude,
            s.longitude,
            s.region,
            s.district,
            s.square_footage,
            s.opening_date,
            s.manager_name,
            s.phone,
            s.is_active
        FROM staging.staging_stores s
        WHERE NOT EXISTS (
            SELECT 1 FROM dim_store d WHERE d.store_id = s.store_id
        );
        
        RAISE NOTICE 'Updated stores, total now: %', (SELECT COUNT(*) FROM dim_store);
    END IF;
END $$;

SELECT '🏪 Store dimension: ' || COUNT(*) || ' rows' as status FROM dim_store;
EOF

# 03_load_dim_customers.sql - SCD Type 2 customer dimension
cat > "$ETL_DIR/03_load_dim_customers.sql" << 'EOF'
-- ============================================================================
-- ETL 03: Load Customer Dimension (SCD Type 2)
-- Handles historical tracking of customer changes
-- ============================================================================

SET search_path TO warehouse;

DO $$
DECLARE
    v_batch_id INTEGER;
    v_updated_count INTEGER := 0;
    v_inserted_count INTEGER := 0;
BEGIN
    -- Get batch ID
    v_batch_id := floor(extract(epoch from now()))::integer;
    
    -- Step 1: Mark existing customers as not current if they've changed
    WITH changed_customers AS (
        SELECT 
            s.customer_id,
            s.first_name,
            s.last_name,
            s.email,
            s.phone,
            s.address_line1,
            s.address_line2,
            s.city,
            s.state,
            s.country,
            s.postal_code,
            s.customer_segment,
            s.is_active
        FROM staging.staging_customers s
        JOIN dim_customer d ON s.customer_id = d.customer_id AND d.is_current = true
        WHERE 
            COALESCE(s.first_name, '') <> COALESCE(d.first_name, '')
            OR COALESCE(s.last_name, '') <> COALESCE(d.last_name, '')
            OR COALESCE(s.email, '') <> COALESCE(d.email, '')
            OR COALESCE(s.phone, '') <> COALESCE(d.phone, '')
            OR COALESCE(s.address_line1, '') <> COALESCE(d.address_line1, '')
            OR COALESCE(s.city, '') <> COALESCE(d.city, '')
            OR COALESCE(s.state, '') <> COALESCE(d.state, '')
            OR COALESCE(s.country, '') <> COALESCE(d.country, '')
            OR COALESCE(s.postal_code, '') <> COALESCE(d.postal_code, '')
            OR COALESCE(s.customer_segment, '') <> COALESCE(d.customer_segment, '')
            OR COALESCE(s.is_active, true) <> COALESCE(d.is_active, true)
    )
    UPDATE dim_customer d
    SET 
        is_current = false,
        end_date = CURRENT_DATE - 1
    FROM changed_customers c
    WHERE d.customer_id = c.customer_id AND d.is_current = true;
    
    GET DIAGNOSTICS v_updated_count = ROW_COUNT;
    
    -- Step 2: Insert new customers (including changed ones as new versions)
    WITH new_customers AS (
        INSERT INTO dim_customer (
            customer_id,
            first_name,
            last_name,
            full_name,
            email,
            phone,
            address_line1,
            address_line2,
            city,
            state,
            country,
            postal_code,
            customer_segment,
            registration_date,
            is_active,
            effective_date,
            end_date,
            is_current,
            source_system
        )
        SELECT 
            s.customer_id,
            s.first_name,
            s.last_name,
            s.first_name || ' ' || s.last_name as full_name,
            s.email,
            s.phone,
            s.address_line1,
            s.address_line2,
            s.city,
            s.state,
            s.country,
            s.postal_code,
            s.customer_segment,
            s.registration_date,
            s.is_active,
            CURRENT_DATE as effective_date,
            NULL as end_date,
            true as is_current,
            'STAGING' as source_system
        FROM staging.staging_customers s
        WHERE NOT EXISTS (
            SELECT 1 FROM dim_customer d 
            WHERE d.customer_id = s.customer_id 
            AND d.is_current = true
            AND COALESCE(d.first_name, '') = COALESCE(s.first_name, '')
            AND COALESCE(d.last_name, '') = COALESCE(s.last_name, '')
            AND COALESCE(d.email, '') = COALESCE(s.email, '')
            AND COALESCE(d.address_line1, '') = COALESCE(s.address_line1, '')
        )
        RETURNING 1
    )
    SELECT COUNT(*) INTO v_inserted_count FROM new_customers;
    
    -- Log results
    RAISE NOTICE 'Customers: % updated (closed), % inserted', v_updated_count, v_inserted_count;
END $$;

SELECT '👥 Customer dimension: ' || COUNT(*) || ' total rows, ' || 
       SUM(CASE WHEN is_current THEN 1 ELSE 0 END) || ' current' as status 
FROM dim_customer;
EOF

# 04_load_dim_products.sql - SCD Type 2 product dimension
cat > "$ETL_DIR/04_load_dim_products.sql" << 'EOF'
-- ============================================================================
-- ETL 04: Load Product Dimension (SCD Type 2)
-- Handles historical tracking of product changes
-- ============================================================================

SET search_path TO warehouse;

DO $$
DECLARE
    v_batch_id INTEGER;
    v_updated_count INTEGER := 0;
    v_inserted_count INTEGER := 0;
BEGIN
    -- Get batch ID
    v_batch_id := floor(extract(epoch from now()))::integer;
    
    -- Step 1: Mark existing products as not current if they've changed
    WITH changed_products AS (
        SELECT 
            s.product_id,
            s.product_name,
            s.category,
            s.subcategory,
            s.brand,
            s.unit_price,
            s.unit_cost,
            s.supplier_name,
            s.is_active
        FROM staging.staging_products s
        JOIN dim_product d ON s.product_id = d.product_id AND d.is_current = true
        WHERE 
            COALESCE(s.product_name, '') <> COALESCE(d.product_name, '')
            OR COALESCE(s.category, '') <> COALESCE(d.category, '')
            OR COALESCE(s.brand, '') <> COALESCE(d.brand, '')
            OR s.unit_price <> d.unit_price
            OR s.unit_cost <> d.unit_cost
            OR COALESCE(s.supplier_name, '') <> COALESCE(d.supplier_name, '')
            OR COALESCE(s.is_active, true) <> COALESCE(d.is_active, true)
    )
    UPDATE dim_product d
    SET 
        is_current = false,
        end_date = CURRENT_DATE - 1
    FROM changed_products c
    WHERE d.product_id = c.product_id AND d.is_current = true;
    
    GET DIAGNOSTICS v_updated_count = ROW_COUNT;
    
    -- Step 2: Insert new products (including changed ones as new versions)
    WITH new_products AS (
        INSERT INTO dim_product (
            product_id,
            product_name,
            product_description,
            sku,
            barcode,
            category,
            subcategory,
            brand,
            unit_cost,
            unit_price,
            msrp,
            supplier_name,
            color,
            size,
            weight_kg,
            is_active,
            effective_date,
            end_date,
            is_current
        )
        SELECT 
            s.product_id,
            s.product_name,
            s.product_description,
            s.sku,
            s.barcode,
            s.category,
            s.subcategory,
            s.brand,
            s.unit_cost,
            s.unit_price,
            s.msrp,
            s.supplier_name,
            s.color,
            s.size,
            s.weight_kg,
            s.is_active,
            CURRENT_DATE as effective_date,
            NULL as end_date,
            true as is_current
        FROM staging.staging_products s
        WHERE NOT EXISTS (
            SELECT 1 FROM dim_product d 
            WHERE d.product_id = s.product_id 
            AND d.is_current = true
            AND d.product_name = s.product_name
            AND d.unit_price = s.unit_price
        )
        RETURNING 1
    )
    SELECT COUNT(*) INTO v_inserted_count FROM new_products;
    
    -- Log results
    RAISE NOTICE 'Products: % updated (closed), % inserted', v_updated_count, v_inserted_count;
END $$;

SELECT '📦 Product dimension: ' || COUNT(*) || ' total rows, ' || 
       SUM(CASE WHEN is_current THEN 1 ELSE 0 END) || ' current' as status 
FROM dim_product;
EOF

# 05_load_fact_sales.sql - Main fact table (OPTIMIZED for performance)
cat > "$ETL_DIR/05_load_fact_sales.sql" << 'EOF'
-- ============================================================================
-- ETL 05: Load Fact Sales Table
-- Transforms staging orders + order_items into fact_sales
-- OPTIMIZED: Added indexes and batch processing for better performance
-- FIXED: Removed transaction_timestamp column (doesn't exist in table)
-- ============================================================================

SET search_path TO warehouse;

-- Create temporary indexes to speed up the INSERT
CREATE INDEX IF NOT EXISTS idx_temp_orders_customer ON staging.staging_orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_temp_order_items_product ON staging.staging_order_items(product_id);
CREATE INDEX IF NOT EXISTS idx_temp_order_items_order ON staging.staging_order_items(order_id);

-- Analyze to update statistics
ANALYZE staging.staging_orders;
ANALYZE staging.staging_order_items;
ANALYZE dim_customer;
ANALYZE dim_product;

DO $$
DECLARE
    v_batch_id INTEGER;
    v_inserted_count INTEGER := 0;
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
BEGIN
    -- Get batch ID
    v_batch_id := floor(extract(epoch from now()))::integer;
    v_start_time := clock_timestamp();
    
    RAISE NOTICE 'Starting fact_sales load at %', v_start_time;
    
    -- Insert into fact_sales
    WITH inserted AS (
        INSERT INTO fact_sales (
            order_id,
            order_line_number,
            invoice_number,
            date_key,
            customer_key,
            product_key,
            store_key,
            payment_key,
            quantity,
            unit_price,
            unit_cost,
            discount_amount,
            tax_amount,
            shipping_amount,
            discount_percent,
            order_status,
            transaction_type,
            source_system,
            batch_id
        )
        SELECT 
            -- Degenerate dimensions
            o.order_id,
            oi.line_number,
            o.invoice_number,
            
            -- Date key
            TO_CHAR(o.order_date::DATE, 'YYYYMMDD')::INTEGER as date_key,
            
            -- Customer key (current version)
            c.customer_key,
            
            -- Product key (current version)
            p.product_key,
            
            -- Store key
            s.store_key,
            
            -- Payment key
            pay.payment_key,
            
            -- Measures
            oi.quantity,
            oi.unit_price,
            p.unit_cost,
            oi.discount_amount,
            oi.tax_amount,
            o.shipping_amount,
            oi.discount_percent,
            
            -- Metadata
            o.order_status,
            CASE 
                WHEN o.order_status = 'Returned' THEN 'Return'
                ELSE 'Sale'
            END as transaction_type,
            'STAGING' as source_system,
            v_batch_id as batch_id

        FROM staging.staging_orders o
        JOIN staging.staging_order_items oi ON o.order_id = oi.order_id
        JOIN dim_customer c ON o.customer_id = c.customer_id AND c.is_current = true
        JOIN dim_product p ON oi.product_id = p.product_id AND p.is_current = true
        LEFT JOIN dim_store s ON o.store_id = s.store_id
        LEFT JOIN dim_payment pay ON o.payment_method = pay.payment_method
        WHERE NOT EXISTS (
            SELECT 1 FROM fact_sales f 
            WHERE f.order_id = o.order_id 
            AND f.order_line_number = oi.line_number
        )
        RETURNING 1
    )
    SELECT COUNT(*) INTO v_inserted_count FROM inserted;
    
    v_end_time := clock_timestamp();
    
    -- Log results
    RAISE NOTICE 'Fact sales: % rows inserted in % seconds', 
        v_inserted_count, 
        EXTRACT(EPOCH FROM (v_end_time - v_start_time));
    
    -- Update staging metadata
    UPDATE staging.staging_orders 
    SET load_status = 'WAREHOUSE_LOADED' 
    WHERE order_id IN (
        SELECT DISTINCT order_id FROM fact_sales 
        WHERE batch_id = v_batch_id
    );
    
    UPDATE staging.staging_order_items 
    SET load_status = 'WAREHOUSE_LOADED' 
    WHERE order_id IN (
        SELECT DISTINCT order_id FROM fact_sales 
        WHERE batch_id = v_batch_id
    );
END $$;

-- Drop temporary indexes
DROP INDEX IF EXISTS staging.idx_temp_orders_customer;
DROP INDEX IF EXISTS staging.idx_temp_order_items_product;
DROP INDEX IF EXISTS staging.idx_temp_order_items_order;

-- Show results
SELECT '💰 Fact sales: ' || COUNT(*) || ' total rows' as status FROM fact_sales;
SELECT '📈 Revenue: $' || ROUND(SUM(net_revenue)::numeric, 2) as total_revenue FROM fact_sales;
EOF

# 06_load_fact_daily_summary.sql - Aggregated fact table
cat > "$ETL_DIR/06_load_fact_daily_summary.sql" << 'EOF'
-- ============================================================================
-- ETL 06: Load Fact Daily Sales Summary
-- Pre-aggregates daily sales for faster reporting
-- ============================================================================

SET search_path TO warehouse;

-- Refresh the daily summary (simple truncate and reload)
TRUNCATE TABLE fact_daily_sales_summary;

INSERT INTO fact_daily_sales_summary (
    date_key,
    store_key,
    product_key,
    total_orders,
    total_line_items,
    total_quantity,
    total_customers,
    new_customers,
    total_gross_revenue,
    total_net_revenue,
    total_discounts,
    total_tax,
    total_shipping,
    total_cost,
    total_profit
)
SELECT 
    f.date_key,
    f.store_key,
    f.product_key,
    COUNT(DISTINCT f.order_id) as total_orders,
    COUNT(*) as total_line_items,
    SUM(f.quantity) as total_quantity,
    COUNT(DISTINCT f.customer_key) as total_customers,
    0 as new_customers,
    SUM(f.gross_revenue) as total_gross_revenue,
    SUM(f.net_revenue) as total_net_revenue,
    SUM(f.discount_amount) as total_discounts,
    SUM(f.tax_amount) as total_tax,
    SUM(f.shipping_amount) as total_shipping,
    SUM(f.total_cost) as total_cost,
    SUM(f.gross_profit) as total_profit
FROM fact_sales f
GROUP BY f.date_key, f.store_key, f.product_key;

SELECT '📊 Daily summary: ' || COUNT(*) || ' rows' as status FROM fact_daily_sales_summary;
EOF

# 07_update_fact_metadata.sql - Update metadata tracking (FIXED - removed ON CONFLICT)
cat > "$ETL_DIR/07_update_fact_metadata.sql" << 'EOF'
-- ============================================================================
-- ETL 07: Update Fact Table Metadata
-- Tracks ETL runs and table statistics
-- FIXED: Removed ON CONFLICT since no unique constraint on table_name
-- ============================================================================

SET search_path TO warehouse;

-- First, check if metadata already exists
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
        -- Insert new record
        INSERT INTO fact_table_metadata (
            table_name,
            grain_description,
            row_count,
            min_date_key,
            max_date_key,
            last_refresh,
            refresh_status
        ) VALUES (
            'fact_sales',
            'Order line item grain',
            v_fact_sales_rows,
            v_fact_sales_min,
            v_fact_sales_max,
            CURRENT_TIMESTAMP,
            'COMPLETED'
        );
    ELSE
        -- Update existing record
        UPDATE fact_table_metadata SET
            row_count = v_fact_sales_rows,
            min_date_key = v_fact_sales_min,
            max_date_key = v_fact_sales_max,
            last_refresh = CURRENT_TIMESTAMP,
            refresh_status = 'COMPLETED'
        WHERE table_name = 'fact_sales';
    END IF;
    
    -- Check if daily summary metadata exists
    SELECT COUNT(*) INTO v_count FROM fact_table_metadata WHERE table_name = 'fact_daily_sales_summary';
    
    IF v_count = 0 THEN
        -- Insert new record
        INSERT INTO fact_table_metadata (
            table_name,
            grain_description,
            row_count,
            min_date_key,
            max_date_key,
            last_refresh,
            refresh_status
        ) VALUES (
            'fact_daily_sales_summary',
            'Daily aggregated by store/product',
            v_daily_summary_rows,
            v_daily_summary_min,
            v_daily_summary_max,
            CURRENT_TIMESTAMP,
            'COMPLETED'
        );
    ELSE
        -- Update existing record
        UPDATE fact_table_metadata SET
            row_count = v_daily_summary_rows,
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

# 08_verify_warehouse.sql - Final verification
cat > "$ETL_DIR/08_verify_warehouse.sql" << 'EOF'
-- ============================================================================
-- ETL 08: Verify Warehouse Load
-- Checks all tables and foreign key relationships
-- ============================================================================

SET search_path TO warehouse;

-- Table counts
SELECT '📊 FINAL WAREHOUSE COUNTS' as section;
SELECT 
    'dim_customer' as table_name, 
    COUNT(*) as row_count,
    SUM(CASE WHEN is_current THEN 1 ELSE 0 END) as current_version
FROM dim_customer
UNION ALL
SELECT 'dim_product', COUNT(*), SUM(CASE WHEN is_current THEN 1 ELSE 0 END)
FROM dim_product
UNION ALL
SELECT 'dim_store', COUNT(*), COUNT(*)
FROM dim_store
UNION ALL
SELECT 'dim_date', COUNT(*), COUNT(*)
FROM dim_date
UNION ALL
SELECT 'dim_payment', COUNT(*), COUNT(*)
FROM dim_payment
UNION ALL
SELECT 'fact_sales', COUNT(*), COUNT(*)
FROM fact_sales
UNION ALL
SELECT 'fact_daily_sales_summary', COUNT(*), COUNT(*)
FROM fact_daily_sales_summary
ORDER BY table_name;

-- Revenue summary
SELECT '💰 REVENUE SUMMARY' as section;
SELECT 
    'Total Revenue: $' || ROUND(SUM(net_revenue)::numeric, 2) as total_revenue,
    'Total Profit: $' || ROUND(SUM(gross_profit)::numeric, 2) as total_profit,
    'Avg Margin: ' || ROUND(AVG(profit_margin)::numeric, 2) || '%' as avg_margin
FROM fact_sales
WHERE profit_margin > 0;

-- Foreign key integrity check
SELECT '🔗 FOREIGN KEY INTEGRITY' as section;
SELECT 
    'fact_sales - customer_key' as check_name,
    COUNT(*) as invalid_count
FROM fact_sales f
LEFT JOIN dim_customer c ON f.customer_key = c.customer_key
WHERE c.customer_key IS NULL
UNION ALL
SELECT 'fact_sales - product_key', COUNT(*)
FROM fact_sales f
LEFT JOIN dim_product p ON f.product_key = p.product_key
WHERE p.product_key IS NULL
UNION ALL
SELECT 'fact_sales - date_key', COUNT(*)
FROM fact_sales f
LEFT JOIN dim_date d ON f.date_key = d.date_id
WHERE d.date_id IS NULL
UNION ALL
SELECT 'fact_sales - store_key', COUNT(*)
FROM fact_sales f
LEFT JOIN dim_store s ON f.store_key = s.store_key
WHERE f.store_key IS NOT NULL AND s.store_key IS NULL;

-- Sample data from view
SELECT '👁️  Sample from v_sales_analysis' as section;
SELECT 
    date,
    customer_name,
    product_name,
    category,
    store_name,
    quantity,
    gross_revenue,
    profit_margin
FROM v_sales_analysis
LIMIT 10;
EOF

echo -e "${GREEN}✅ All ETL scripts created in $ETL_DIR${NC}"

# ============================================================================
# RUN ETL PIPELINE
# ============================================================================

echo -e "\n${YELLOW}🚀 Starting ETL Pipeline...${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"

# Run steps in order
run_sql "1" "$ETL_DIR/01_load_dim_dates.sql" "Loading additional dates" 60
run_sql "2" "$ETL_DIR/02_load_dim_stores.sql" "Loading store dimension" 60
run_sql "3" "$ETL_DIR/03_load_dim_customers.sql" "Loading customer dimension (SCD Type 2)" 120
run_sql "4" "$ETL_DIR/04_load_dim_products.sql" "Loading product dimension (SCD Type 2)" 120
run_sql "5" "$ETL_DIR/05_load_fact_sales.sql" "Loading fact sales (118K rows)" 0  # No timeout
run_sql "6" "$ETL_DIR/06_load_fact_daily_summary.sql" "Loading daily sales summary" 120
run_sql "7" "$ETL_DIR/07_update_fact_metadata.sql" "Updating metadata" 30
run_sql "8" "$ETL_DIR/08_verify_warehouse.sql" "Final verification" 60

echo -e "\n${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}✅ ETL Pipeline Complete!${NC}"
echo -e "${CYAN}📝 Log file: $LOG_FILE${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"

# Show final summary
echo -e "\n${YELLOW}📊 Final Warehouse Status:${NC}"
export PGPASSWORD="$DB_PASSWORD"
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -P pager=off << EOF
SELECT '📊 WAREHOUSE LOAD SUMMARY' as section;
SELECT 
    'dim_customer' as table_name, 
    COUNT(*) as row_count,
    SUM(CASE WHEN is_current THEN 1 ELSE 0 END) as current_records
FROM warehouse.dim_customer
UNION ALL
SELECT 'dim_product', COUNT(*), SUM(CASE WHEN is_current THEN 1 ELSE 0 END)
FROM warehouse.dim_product
UNION ALL
SELECT 'dim_store', COUNT(*), COUNT(*)
FROM warehouse.dim_store
UNION ALL
SELECT 'dim_date', COUNT(*), COUNT(*)
FROM warehouse.dim_date
UNION ALL
SELECT 'dim_payment', COUNT(*), COUNT(*)
FROM warehouse.dim_payment
UNION ALL
SELECT 'fact_sales', COUNT(*), COUNT(*)
FROM warehouse.fact_sales
UNION ALL
SELECT 'fact_daily_sales_summary', COUNT(*), COUNT(*)
FROM warehouse.fact_daily_sales_summary
ORDER BY table_name;

SELECT '💰 TOTAL REVENUE: $' || COALESCE(ROUND(SUM(net_revenue)::numeric, 2), 0) as revenue
FROM warehouse.fact_sales;
EOF
unset PGPASSWORD

echo -e "\n${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}✅ ETL Pipeline Execution Complete!${NC}"
echo -e "${YELLOW}📁 Check $LOG_FILE for detailed logs${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
