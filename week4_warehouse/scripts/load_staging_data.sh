#!/bin/bash

################################################################################
# Load Data into Staging Tables Script - DYNAMIC BATCH LOADER
# This script loads CSV files from a specified batch folder into staging tables
# Usage: ./load_staging.sh [batch_folder_name]
# Example: ./load_staging.sh batch_20260305_113746
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
echo -e "${GREEN}📥 Dynamic Batch Loader - Loading Data into Staging Tables${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"

# Load environment variables
if [ -f ../.env ]; then
    echo -e "${YELLOW}📦 Loading configuration from .env${NC}"
    source ../.env
else
    echo -e "${RED}❌ .env file not found!${NC}"
    exit 1
fi

# Get script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BATCHES_DIR="$PROJECT_ROOT/data"

# ============================================================================
# BATCH SELECTION
# ============================================================================

# If batch folder is provided as argument, use it
if [ $# -eq 1 ]; then
    BATCH_FOLDER="$1"
    DATA_DIR="$BATCHES_DIR/$BATCH_FOLDER"
    echo -e "${CYAN}📂 Using provided batch: ${BATCH_FOLDER}${NC}"
else
    # List available batches
    echo -e "\n${YELLOW}📋 Available batches:${NC}"
    
    # Find all batch folders and sort by date (newest first)
    mapfile -t BATCHES < <(ls -d "$BATCHES_DIR"/batch_* 2>/dev/null | sort -r | head -5 | xargs -n1 basename)
    
    if [ ${#BATCHES[@]} -eq 0 ]; then
        echo -e "${RED}❌ No batch folders found in $BATCHES_DIR${NC}"
        echo -e "${YELLOW}Please run data generator first or specify a batch folder.${NC}"
        exit 1
    fi
    
    # Display menu
    echo -e "${CYAN}Select a batch to load:${NC}"
    for i in "${!BATCHES[@]}"; do
        # Get batch size and file count
        BATCH_SIZE=$(du -sh "$BATCHES_DIR/${BATCHES[$i]}" 2>/dev/null | cut -f1)
        FILE_COUNT=$(ls -1 "$BATCHES_DIR/${BATCHES[$i]}"/*.csv 2>/dev/null | wc -l)
        echo -e "  ${GREEN}$((i+1))${NC}) ${BATCHES[$i]} ${CYAN}(size: $BATCH_SIZE, files: $FILE_COUNT)${NC}"
    done
    echo -e "  ${GREEN}q${NC}) Quit"
    
    # Get user choice
    echo -e "\n${YELLOW}Enter choice (1-${#BATCHES[@]} or q):${NC} "
    read -r choice
    
    if [[ "$choice" == "q" ]]; then
        echo -e "${RED}❌ Load cancelled${NC}"
        exit 0
    fi
    
    if ! [[ "$choice" =~ ^[0-9]+$ ]] || [ "$choice" -lt 1 ] || [ "$choice" -gt ${#BATCHES[@]} ]; then
        echo -e "${RED}❌ Invalid choice${NC}"
        exit 1
    fi
    
    BATCH_FOLDER="${BATCHES[$((choice-1))]}"
    DATA_DIR="$BATCHES_DIR/$BATCH_FOLDER"
fi

echo -e "${GREEN}✅ Selected batch: ${BATCH_FOLDER}${NC}"
echo -e "${CYAN}📂 Data directory: ${DATA_DIR}${NC}"

# ============================================================================
# CHECK FOR CSV FILES
# ============================================================================
echo -e "\n${YELLOW}🔍 Checking for CSV files in batch folder...${NC}"
MISSING_FILES=0
REQUIRED_FILES=("customers.csv" "products.csv" "orders.csv" "order_items.csv" "payments.csv" "stores.csv")

for file in "${REQUIRED_FILES[@]}"; do
    if [ -f "$DATA_DIR/$file" ]; then
        SIZE=$(du -h "$DATA_DIR/$file" | cut -f1)
        RECORDS=$(($(wc -l < "$DATA_DIR/$file") - 1))  # Subtract header
        echo -e "${GREEN}  ✅ Found $file ${CYAN}($SIZE, $RECORDS records)${NC}"
        
        # Show first line of each CSV to verify columns
        echo -e "${CYAN}     CSV Columns: $(head -1 "$DATA_DIR/$file")${NC}"
    else
        echo -e "${RED}  ❌ Missing $file${NC}"
        MISSING_FILES=$((MISSING_FILES + 1))
    fi
done

if [ $MISSING_FILES -gt 0 ]; then
    echo -e "${RED}❌ $MISSING_FILES required files missing from batch ${BATCH_FOLDER}${NC}"
    exit 1
fi

# ============================================================================
# CREATE LOAD SQL SCRIPT
# ============================================================================
echo -e "\n${YELLOW}📝 Creating staging load SQL script for batch ${BATCH_FOLDER}...${NC}"

mkdir -p "$PROJECT_ROOT/sql/queries"

# Get absolute path for COPY command - escape single quotes for SQL
DATA_PATH="$DATA_DIR"
ESCAPED_PATH=$(echo "$DATA_PATH" | sed "s/'/''/g")

cat > "$PROJECT_ROOT/sql/queries/06_load_staging.sql" << EOF
-- ============================================================================
-- Load data from CSV files into staging tables
-- BATCH: ${BATCH_FOLDER}
-- Generated: $(date)
-- ============================================================================

SET search_path TO staging;

-- Clear staging tables (in correct order to handle dependencies)
TRUNCATE TABLE staging_order_items CASCADE;
TRUNCATE TABLE staging_payments CASCADE;
TRUNCATE TABLE staging_orders CASCADE;
TRUNCATE TABLE staging_products CASCADE;
TRUNCATE TABLE staging_customers CASCADE;
TRUNCATE TABLE staging_stores CASCADE;

-- Get current batch_id
DO \$\$
DECLARE
    current_batch INTEGER;
BEGIN
    current_batch := floor(extract(epoch from now()))::integer;
    CREATE TEMP TABLE current_batch_id AS SELECT current_batch as batch_id;
END \$\$;

-- ============================================================================
-- LOAD STORES - ALL ON ONE LINE (no line breaks)
-- ============================================================================
\COPY staging_stores(store_id, store_name, store_type, address, city, state, country, postal_code, latitude, longitude, region, district, square_footage, opening_date, manager_name, phone, is_active) FROM '${ESCAPED_PATH}/stores.csv' WITH (FORMAT csv, HEADER true);

-- ============================================================================
-- LOAD CUSTOMERS - ALL ON ONE LINE
-- ============================================================================
\COPY staging_customers(customer_id, first_name, last_name, email, phone, address_line1, address_line2, city, state, country, postal_code, registration_date, customer_segment, is_active) FROM '${ESCAPED_PATH}/customers.csv' WITH (FORMAT csv, HEADER true);

-- ============================================================================
-- LOAD PRODUCTS - ALL ON ONE LINE
-- ============================================================================
\COPY staging_products(product_id, product_name, product_description, sku, barcode, category, subcategory, brand, unit_price, unit_cost, msrp, supplier_name, color, size, weight_kg, is_active) FROM '${ESCAPED_PATH}/products.csv' WITH (FORMAT csv, HEADER true);

-- ============================================================================
-- LOAD ORDERS - ALL ON ONE LINE
-- ============================================================================
\COPY staging_orders(order_id, invoice_number, order_date, customer_id, store_id, subtotal, tax_amount, shipping_amount, total_amount, order_status, payment_method, shipping_method, created_at, updated_at) FROM '${ESCAPED_PATH}/orders.csv' WITH (FORMAT csv, HEADER true);

-- ============================================================================
-- LOAD ORDER ITEMS - ALL ON ONE LINE
-- ============================================================================
\COPY staging_order_items(order_id, line_number, product_id, quantity, unit_price, discount_percent, discount_amount, tax_percent, tax_amount, line_total) FROM '${ESCAPED_PATH}/order_items.csv' WITH (FORMAT csv, HEADER true);

-- ============================================================================
-- LOAD PAYMENTS - ALL ON ONE LINE
-- ============================================================================
\COPY staging_payments(payment_id, order_id, payment_date, payment_method, payment_amount, transaction_id, payment_status) FROM '${ESCAPED_PATH}/payments.csv' WITH (FORMAT csv, HEADER true);

-- ============================================================================
-- UPDATE METADATA COLUMNS
-- ============================================================================
UPDATE staging_stores SET 
    loaded_at = CURRENT_TIMESTAMP,
    source_file = '${BATCH_FOLDER}/stores.csv',
    load_status = 'COMPLETED',
    batch_id = (SELECT batch_id FROM current_batch_id)
WHERE load_status IS NULL OR load_status = 'NEW';

UPDATE staging_customers SET 
    loaded_at = CURRENT_TIMESTAMP,
    source_file = '${BATCH_FOLDER}/customers.csv',
    load_status = 'COMPLETED',
    batch_id = (SELECT batch_id FROM current_batch_id)
WHERE load_status IS NULL OR load_status = 'NEW';

UPDATE staging_products SET 
    loaded_at = CURRENT_TIMESTAMP,
    source_file = '${BATCH_FOLDER}/products.csv',
    load_status = 'COMPLETED',
    batch_id = (SELECT batch_id FROM current_batch_id)
WHERE load_status IS NULL OR load_status = 'NEW';

UPDATE staging_orders SET 
    loaded_at = CURRENT_TIMESTAMP,
    source_file = '${BATCH_FOLDER}/orders.csv',
    load_status = 'COMPLETED',
    batch_id = (SELECT batch_id FROM current_batch_id)
WHERE load_status IS NULL OR load_status = 'NEW';

UPDATE staging_order_items SET 
    loaded_at = CURRENT_TIMESTAMP,
    source_file = '${BATCH_FOLDER}/order_items.csv',
    load_status = 'COMPLETED',
    batch_id = (SELECT batch_id FROM current_batch_id)
WHERE load_status IS NULL OR load_status = 'NEW';

UPDATE staging_payments SET 
    loaded_at = CURRENT_TIMESTAMP,
    source_file = '${BATCH_FOLDER}/payments.csv',
    load_status = 'COMPLETED',
    batch_id = (SELECT batch_id FROM current_batch_id)
WHERE load_status IS NULL OR load_status = 'NEW';

-- Drop temp table
DROP TABLE current_batch_id;

-- ============================================================================
-- VERIFICATION
-- ============================================================================
SELECT '📊 LOAD SUMMARY - BATCH ${BATCH_FOLDER}' as info;

SELECT 'staging_stores' as table_name, COUNT(*) as row_count, COUNT(DISTINCT batch_id) as batches, MIN(loaded_at) as earliest, MAX(loaded_at) as latest FROM staging_stores
UNION ALL
SELECT 'staging_customers', COUNT(*), COUNT(DISTINCT batch_id), MIN(loaded_at), MAX(loaded_at) FROM staging_customers
UNION ALL
SELECT 'staging_products', COUNT(*), COUNT(DISTINCT batch_id), MIN(loaded_at), MAX(loaded_at) FROM staging_products
UNION ALL
SELECT 'staging_orders', COUNT(*), COUNT(DISTINCT batch_id), MIN(loaded_at), MAX(loaded_at) FROM staging_orders
UNION ALL
SELECT 'staging_order_items', COUNT(*), COUNT(DISTINCT batch_id), MIN(loaded_at), MAX(loaded_at) FROM staging_order_items
UNION ALL
SELECT 'staging_payments', COUNT(*), COUNT(DISTINCT batch_id), MIN(loaded_at), MAX(loaded_at) FROM staging_payments
ORDER BY table_name;

-- Show staging status view
SELECT '📊 STAGING STATUS:' as info;
SELECT * FROM v_staging_status;
EOF

echo -e "${GREEN}✅ SQL script created at sql/queries/06_load_staging.sql${NC}"
echo -e "${CYAN}   Using data from batch: ${BATCH_FOLDER}${NC}"

# ============================================================================
# CONFIRM LOAD
# ============================================================================
echo -e "\n${YELLOW}📋 Ready to load batch ${BATCH_FOLDER} into staging tables:${NC}"
echo -e "   • stores.csv"
echo -e "   • customers.csv"
echo -e "   • products.csv"
echo -e "   • orders.csv"
echo -e "   • order_items.csv"
echo -e "   • payments.csv"

echo -e "\n${YELLOW}⚠️  This will TRUNCATE existing staging tables before loading! Continue? (y/n)${NC}"
read -r response
if [[ ! "$response" =~ ^[Yy]$ ]]; then
    echo -e "${RED}❌ Load cancelled${NC}"
    exit 0
fi

# ============================================================================
# RUN THE LOAD
# ============================================================================
echo -e "\n${YELLOW}⚙️  Loading batch ${BATCH_FOLDER} into staging tables...${NC}"
echo -e "${BLUE}--------------------------------------------------${NC}"

export PGPASSWORD=$DB_PASSWORD
psql -U $DB_USER -d $DB_NAME -h $DB_HOST -v ON_ERROR_STOP=1 -f "$PROJECT_ROOT/sql/queries/06_load_staging.sql"
PSQL_EXIT_CODE=$?
unset PGPASSWORD

# ============================================================================
# CHECK RESULT
# ============================================================================
if [ $PSQL_EXIT_CODE -eq 0 ]; then
    echo -e "${BLUE}--------------------------------------------------${NC}"
    echo -e "${GREEN}✅ Batch ${BATCH_FOLDER} loaded into staging tables successfully!${NC}"
    
    # Show verification
    echo -e "\n${YELLOW}🔍 Quick verification from v_staging_status:${NC}"
    export PGPASSWORD=$DB_PASSWORD
    psql -U $DB_USER -d $DB_NAME -h $DB_HOST -P pager=off -c "
        SELECT * FROM staging.v_staging_status
        ORDER BY table_name;"
    unset PGPASSWORD
    
    # Log the load
    echo "$(date): Loaded batch ${BATCH_FOLDER}" >> "$PROJECT_ROOT/logs/batch_load_history.log"
    
else
    echo -e "${RED}❌ Failed to load batch ${BATCH_FOLDER} into staging tables (exit code: $PSQL_EXIT_CODE)${NC}"
    exit 1
fi

echo -e "\n${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}✅ Batch load complete!${NC}"
echo -e "${YELLOW}📊 Data from ${BATCH_FOLDER} is now in staging tables${NC}"
echo -e "${YELLOW}Next step: Run ETL to move from staging to warehouse${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
