#!/bin/bash

################################################################################
# Warehouse Schema Inspector
# This script inspects all warehouse tables and saves structure to a file
# Run this BEFORE ETL to ensure alignment with staging data
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
echo -e "${GREEN}🔍 Warehouse Schema Inspector${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"

# Default connection parameters (will be overridden by .env if present)
DB_HOST="localhost"
DB_PORT="5432"
DB_USER="data_engineer"
DB_NAME="ecommerce_warehouse"
DB_PASSWORD=""

# Load environment variables if .env exists
if [ -f ../.env ]; then
    echo -e "${YELLOW}📦 Loading configuration from ../.env${NC}"
    source ../.env
elif [ -f .env ]; then
    echo -e "${YELLOW}📦 Loading configuration from .env${NC}"
    source .env
else
    echo -e "${YELLOW}⚠️  No .env file found, using default connection parameters${NC}"
fi

# Prompt for password if not set in .env
if [ -z "$DB_PASSWORD" ]; then
    echo -e "${YELLOW}🔑 Database password required${NC}"
    read -s -p "Enter password for user $DB_USER: " DB_PASSWORD
    echo ""
fi

# Get script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
OUTPUT_DIR="$PROJECT_ROOT/docs"
mkdir -p "$OUTPUT_DIR"

OUTPUT_FILE="$OUTPUT_DIR/warehouse_schema_inspection_$(date +%Y%m%d_%H%M%S).txt"
echo -e "${CYAN}📝 Output will be saved to: $OUTPUT_FILE${NC}"

# Create the inspection SQL file
echo -e "\n${YELLOW}📝 Creating inspection SQL script...${NC}"

cat > "$PROJECT_ROOT/sql/inspect_warehouse.sql" << 'EOF'
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
EOF

echo -e "${GREEN}✅ Inspection SQL created at sql/inspect_warehouse.sql${NC}"

# Run the inspection
echo -e "\n${YELLOW}🔍 Running warehouse inspection...${NC}"
echo -e "${BLUE}--------------------------------------------------${NC}"

# Use PGPASSWORD environment variable for password
export PGPASSWORD="$DB_PASSWORD"

# Run psql with connection parameters
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -f "$PROJECT_ROOT/sql/inspect_warehouse.sql" > "$OUTPUT_FILE"
PSQL_EXIT_CODE=$?

# Clear password from environment
unset PGPASSWORD

if [ $PSQL_EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}✅ Inspection complete!${NC}"
    echo -e "${CYAN}📄 Output saved to: $OUTPUT_FILE${NC}"
    
    # Show summary from the output
    echo -e "\n${YELLOW}📊 Quick Summary:${NC}"
    echo -e "${CYAN}Here are the first 20 lines of the inspection:${NC}"
    head -20 "$OUTPUT_FILE"
    
else
    echo -e "${RED}❌ Inspection failed with exit code: $PSQL_EXIT_CODE${NC}"
    echo -e "${YELLOW}Check the error messages above for details.${NC}"
    exit 1
fi

# Create a simplified ETL mapping document
echo -e "\n${YELLOW}📝 Creating ETL mapping document...${NC}"
MAPPING_FILE="$OUTPUT_DIR/etl_mapping_$(date +%Y%m%d_%H%M%S).txt"

cat > "$MAPPING_FILE" << EOF
=============================================================================
ETL MAPPING: Staging → Warehouse
=============================================================================
Generated: $(date)

This document maps staging columns to warehouse columns for ETL development.
Based on warehouse schema inspection.

=============================================================================
DIMENSION MAPPINGS
=============================================================================

1. staging_customers → dim_customer
   ---------------------------------
   Source (staging)              → Target (warehouse)
   --------------------------------------------------
   customer_id                   → customer_id
   first_name                    → first_name
   last_name                     → last_name
   email                         → email
   phone                         → phone
   address_line1                 → address_line1
   address_line2                 → address_line2
   city                          → city
   state                         → state
   country                       → country
   postal_code                   → postal_code
   registration_date             → registration_date
   customer_segment              → customer_segment
   is_active                     → is_active

2. staging_products → dim_product
   ------------------------------
   product_id                    → product_id
   product_name                  → product_name
   product_description           → product_description
   sku                           → sku
   barcode                       → barcode
   category                      → category
   subcategory                   → subcategory
   brand                         → brand
   unit_price                    → unit_price
   unit_cost                     → unit_cost
   msrp                          → msrp
   supplier_name                 → supplier_name
   color                         → color
   size                          → size
   weight_kg                     → weight_kg
   is_active                     → is_active

3. staging_stores → dim_store
   --------------------------
   store_id                      → store_id
   store_name                    → store_name
   store_type                    → store_type
   address                       → address
   city                          → city
   state                         → state
   country                       → country
   postal_code                   → postal_code
   latitude                      → latitude
   longitude                     → longitude
   region                        → region
   district                      → district
   square_footage                → square_footage
   opening_date                  → opening_date
   manager_name                  → manager_name
   phone                         → phone
   is_active                     → is_active

4. staging_orders + staging_order_items → fact_sales
   -------------------------------------------------
   staging_orders                → fact_sales
   -----------------------------   -----------
   order_id                      → order_id
   order_date                    → date_key (via dim_date)
   customer_id                   → customer_key (lookup)
   store_id                      → store_key (lookup)
   payment_method                → payment_key (lookup)
   order_status                  → order_status
   created_at                    → transaction_timestamp
   
   staging_order_items           → fact_sales
   ------------------------        -----------
   line_number                   → order_line_number
   product_id                    → product_key (lookup)
   quantity                      → quantity
   unit_price                    → unit_price
   discount_percent              → discount_percent
   discount_amount               → discount_amount
   tax_amount                    → tax_amount
   line_total                    → gross_revenue

=============================================================================
NOTES FOR ETL DEVELOPMENT
=============================================================================
1. Always load dimensions BEFORE facts
2. Use LOOKUPs to get surrogate keys
3. Handle SCD Type 2 for customers and products
4. Set default values for missing references
5. Use batch_id for tracking ETL runs

=============================================================================
EOF

echo -e "${GREEN}✅ ETL mapping document created at: $MAPPING_FILE${NC}"
echo -e "\n${YELLOW}📋 Next steps:${NC}"
echo -e "   1. Review the inspection output to understand warehouse structure"
echo -e "   2. Check the ETL mapping document for column alignment"
echo -e "   3. Run the ETL pipeline with proper column mappings"

echo -e "\n${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}✅ Inspection complete!${NC}"
echo -e "${YELLOW}📁 Output files:${NC}"
echo -e "   • Schema inspection: ${CYAN}$OUTPUT_FILE${NC}"
echo -e "   • ETL mapping: ${CYAN}$MAPPING_FILE${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
