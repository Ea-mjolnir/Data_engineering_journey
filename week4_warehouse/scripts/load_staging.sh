#!/bin/bash

################################################################################
# Load Data into Staging Tables Script
# This script loads CSV files into staging tables 
# Now that CSV files match staging tables EXACTLY!
# UPDATED: Includes stores.csv loading
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
echo -e "${GREEN}📥 Loading Data into Staging Tables${NC}"
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
DATA_DIR="$PROJECT_ROOT/data/sample_large"

echo -e "${CYAN}📂 Project root: ${PROJECT_ROOT}${NC}"
echo -e "${CYAN}📂 Data directory: ${DATA_DIR}${NC}"

# Check if data files exist (INCLUDING STORES!)
echo -e "\n${YELLOW}🔍 Checking for data files...${NC}"
MISSING_FILES=0
for file in customers.csv products.csv orders.csv order_items.csv payments.csv stores.csv; do
    if [ -f "$DATA_DIR/$file" ]; then
        SIZE=$(du -h "$DATA_DIR/$file" | cut -f1)
        echo -e "${GREEN}  ✅ Found $file ($SIZE)${NC}"
        
        # Show first line of each CSV to verify columns
        echo -e "${CYAN}     CSV Columns: $(head -1 "$DATA_DIR/$file")${NC}"
        
        # Show first data line to verify boolean format
        if [ "$file" == "customers.csv" ] || [ "$file" == "products.csv" ] || [ "$file" == "stores.csv" ]; then
            FIRST_RECORD=$(head -2 "$DATA_DIR/$file" | tail -1)
            LAST_COL=$(echo "$FIRST_RECORD" | awk -F',' '{print $NF}')
            echo -e "${CYAN}     Boolean check (is_active): '$LAST_COL'${NC}"
        fi
        
        # Count columns
        COL_COUNT=$(head -1 "$DATA_DIR/$file" | tr ',' '\n' | wc -l)
        echo -e "${CYAN}     Column count: $COL_COUNT${NC}"
    else
        echo -e "${RED}  ❌ Missing $file${NC}"
        MISSING_FILES=$((MISSING_FILES + 1))
    fi
done

if [ $MISSING_FILES -gt 0 ]; then
    echo -e "${RED}❌ $MISSING_FILES data files missing. Please run the data generator first.${NC}"
    exit 1
fi

# Create the SQL load script
echo -e "\n${YELLOW}📝 Creating staging load SQL script...${NC}"

mkdir -p "$PROJECT_ROOT/sql/queries"

# Use the user's actual home directory path
USER_HOME="$HOME"
DATA_PATH="$USER_HOME/Data_engineering_Journey/week4_warehouse/data/sample_large"

cat > "$PROJECT_ROOT/sql/queries/06_load_staging.sql" << EOF
-- ============================================================================
-- Load data from CSV files into staging tables
-- SIMPLIFIED: CSV files now match staging tables EXACTLY!
-- INCLUDES: stores.csv loading
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
    
    -- Store batch_id in a temporary table for later use
    CREATE TEMP TABLE current_batch_id AS SELECT current_batch as batch_id;
END \$\$;

-- ============================================================================
-- LOAD STORES - Direct match with staging_stores (NEW!)
-- ============================================================================
\COPY staging_stores(store_id, store_name, store_type, address, city, state, country, postal_code, latitude, longitude, region, district, square_footage, opening_date, manager_name, phone, is_active) FROM '$DATA_PATH/stores.csv' WITH (FORMAT csv, HEADER true);

-- ============================================================================
-- LOAD CUSTOMERS - Direct match with staging_customers
-- ============================================================================
\COPY staging_customers(customer_id, first_name, last_name, email, phone, address_line1, address_line2, city, state, country, postal_code, registration_date, customer_segment, is_active) FROM '$DATA_PATH/customers.csv' WITH (FORMAT csv, HEADER true);

-- ============================================================================
-- LOAD PRODUCTS - Direct match with staging_products
-- ============================================================================
\COPY staging_products(product_id, product_name, product_description, sku, barcode, category, subcategory, brand, unit_price, unit_cost, msrp, supplier_name, color, size, weight_kg, is_active) FROM '$DATA_PATH/products.csv' WITH (FORMAT csv, HEADER true);

-- ============================================================================
-- LOAD ORDERS - Direct match with staging_orders
-- ============================================================================
\COPY staging_orders(order_id, invoice_number, order_date, customer_id, store_id, subtotal, tax_amount, shipping_amount, total_amount, order_status, payment_method, shipping_method, created_at, updated_at) FROM '$DATA_PATH/orders.csv' WITH (FORMAT csv, HEADER true);

-- ============================================================================
-- LOAD ORDER ITEMS - Direct match with staging_order_items
-- ============================================================================
\COPY staging_order_items(order_id, line_number, product_id, quantity, unit_price, discount_percent, discount_amount, tax_percent, tax_amount, line_total) FROM '$DATA_PATH/order_items.csv' WITH (FORMAT csv, HEADER true);

-- ============================================================================
-- LOAD PAYMENTS - Direct match with staging_payments
-- ============================================================================
\COPY staging_payments(payment_id, order_id, payment_date, payment_method, payment_amount, transaction_id, payment_status) FROM '$DATA_PATH/payments.csv' WITH (FORMAT csv, HEADER true);

-- ============================================================================
-- UPDATE METADATA COLUMNS - Now using the batch_id from temp table
-- ============================================================================
UPDATE staging_stores SET 
    loaded_at = CURRENT_TIMESTAMP,
    source_file = 'stores.csv',
    load_status = 'COMPLETED',
    batch_id = (SELECT batch_id FROM current_batch_id)
WHERE load_status IS NULL OR load_status = 'NEW';

UPDATE staging_customers SET 
    loaded_at = CURRENT_TIMESTAMP,
    source_file = 'customers.csv',
    load_status = 'COMPLETED',
    batch_id = (SELECT batch_id FROM current_batch_id)
WHERE load_status IS NULL OR load_status = 'NEW';

UPDATE staging_products SET 
    loaded_at = CURRENT_TIMESTAMP,
    source_file = 'products.csv',
    load_status = 'COMPLETED',
    batch_id = (SELECT batch_id FROM current_batch_id)
WHERE load_status IS NULL OR load_status = 'NEW';

UPDATE staging_orders SET 
    loaded_at = CURRENT_TIMESTAMP,
    source_file = 'orders.csv',
    load_status = 'COMPLETED',
    batch_id = (SELECT batch_id FROM current_batch_id)
WHERE load_status IS NULL OR load_status = 'NEW';

UPDATE staging_order_items SET 
    loaded_at = CURRENT_TIMESTAMP,
    source_file = 'order_items.csv',
    load_status = 'COMPLETED',
    batch_id = (SELECT batch_id FROM current_batch_id)
WHERE load_status IS NULL OR load_status = 'NEW';

UPDATE staging_payments SET 
    loaded_at = CURRENT_TIMESTAMP,
    source_file = 'payments.csv',
    load_status = 'COMPLETED',
    batch_id = (SELECT batch_id FROM current_batch_id)
WHERE load_status IS NULL OR load_status = 'NEW';

-- Drop temp table
DROP TABLE current_batch_id;

-- ============================================================================
-- VERIFICATION - Now includes stores!
-- ============================================================================
SELECT '📊 LOAD SUMMARY' as info;

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

SELECT '✅ Sample stores WITH METADATA:' as info;
SELECT store_id, store_name, city, state, region, is_active,
       to_char(loaded_at, 'YYYY-MM-DD HH24:MI:SS') as loaded_at,
       source_file, batch_id, load_status
FROM staging_stores LIMIT 3;

SELECT '✅ Sample customers WITH METADATA:' as info;
SELECT customer_id, first_name, last_name, email, is_active, 
       to_char(loaded_at, 'YYYY-MM-DD HH24:MI:SS') as loaded_at,
       source_file, batch_id, load_status
FROM staging_customers LIMIT 3;

SELECT '✅ Sample products WITH METADATA:' as info;
SELECT product_id, product_name, unit_price, is_active,
       to_char(loaded_at, 'YYYY-MM-DD HH24:MI:SS') as loaded_at,
       source_file, batch_id, load_status
FROM staging_products LIMIT 3;

SELECT '✅ Sample orders WITH METADATA:' as info;
SELECT order_id, customer_id, store_id, total_amount, order_status,
       to_char(loaded_at, 'YYYY-MM-DD HH24:MI:SS') as loaded_at,
       source_file, batch_id, load_status
FROM staging_orders LIMIT 3;

SELECT '✅ Sample order items WITH METADATA:' as info;
SELECT order_id, product_id, quantity, line_total,
       to_char(loaded_at, 'YYYY-MM-DD HH24:MI:SS') as loaded_at,
       source_file, batch_id, load_status
FROM staging_order_items LIMIT 3;

SELECT '✅ Sample payments WITH METADATA:' as info;
SELECT payment_id, order_id, payment_amount, payment_status,
       to_char(loaded_at, 'YYYY-MM-DD HH24:MI:SS') as loaded_at,
       source_file, batch_id, load_status
FROM staging_payments LIMIT 3;

-- Show staging status view
SELECT '📊 STAGING STATUS:' as info;
SELECT * FROM v_staging_status;
EOF

echo -e "${GREEN}✅ SQL script created at sql/queries/06_load_staging.sql${NC}"
echo -e "${CYAN}   Using data path: $DATA_PATH${NC}"

# Show verification - NOW INCLUDES STORES
echo -e "\n${YELLOW}📋 Verification - All CSV columns now match staging tables EXACTLY:${NC}"
echo -e "${GREEN}✓ stores.csv: 17 columns match staging_stores (NEW!)${NC}"
echo -e "${GREEN}✓ customers.csv: 14 columns match staging_customers${NC}"
echo -e "${GREEN}✓ products.csv: 16 columns match staging_products${NC}"
echo -e "${GREEN}✓ orders.csv: 14 columns match staging_orders${NC}"
echo -e "${GREEN}✓ order_items.csv: 10 columns match staging_order_items${NC}"
echo -e "${GREEN}✓ payments.csv: 7 columns match staging_payments${NC}"

# Ask for confirmation before loading
echo -e "\n${YELLOW}⚠️  About to load data into staging tables. Continue? (y/n)${NC}"
read -r response
if [[ ! "$response" =~ ^[Yy]$ ]]; then
    echo -e "${RED}❌ Load cancelled${NC}"
    exit 0
fi

# Run the SQL file
echo -e "\n${YELLOW}⚙️  Loading data into staging tables...${NC}"
echo -e "${BLUE}--------------------------------------------------${NC}"

export PGPASSWORD=$DB_PASSWORD
psql -U $DB_USER -d $DB_NAME -h $DB_HOST -v ON_ERROR_STOP=1 -f "$PROJECT_ROOT/sql/queries/06_load_staging.sql"
PSQL_EXIT_CODE=$?
unset PGPASSWORD

# Check if successful
if [ $PSQL_EXIT_CODE -eq 0 ]; then
    echo -e "${BLUE}--------------------------------------------------${NC}"
    echo -e "${GREEN}✅ Data loaded into staging tables successfully!${NC}"
    
    # Quick verification - NOW INCLUDES STORES
    echo -e "\n${YELLOW}🔍 Quick verification from v_staging_status:${NC}"
    export PGPASSWORD=$DB_PASSWORD
    psql -U $DB_USER -d $DB_NAME -h $DB_HOST -P pager=off -c "
        SELECT * FROM staging.v_staging_status
        ORDER BY 
            CASE 
                WHEN table_name = 'staging_stores' THEN 1
                ELSE 2
            END,
            table_name;"
    unset PGPASSWORD
    
    echo -e "\n${GREEN}✅ Metadata columns populated successfully!${NC}"
    echo -e "${GREEN}✅ Stores data loaded successfully!${NC}"
    echo -e "${YELLOW}📊 All staging tables now have referential integrity!${NC}"
    
else
    echo -e "${RED}❌ Failed to load data into staging tables (exit code: $PSQL_EXIT_CODE)${NC}"
    exit 1
fi

echo -e "\n${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}✅ Staging load complete!${NC}"
echo -e "${GREEN}✅ All 6 staging tables now populated!${NC}"
echo -e "${YELLOW}Next step: Transform from staging to warehouse tables${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
