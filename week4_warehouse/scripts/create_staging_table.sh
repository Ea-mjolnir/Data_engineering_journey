#!/bin/bash

################################################################################
# Create All Staging Tables Script
# This script creates staging tables for raw data ingestion
# Staging tables are temporary landing zones before transformation
################################################################################

set -e  # Exit on error
set -u  # Exit on undefined variable

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}🏗️  Creating All Staging Tables${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"

# Load environment variables
if [ -f ../.env ]; then
    echo -e "${YELLOW}📦 Loading configuration from .env${NC}"
    source ../.env
else
    echo -e "${RED}❌ .env file not found!${NC}"
    exit 1
fi

# Create the SQL file
echo -e "\n${YELLOW}📝 Creating staging tables SQL file...${NC}"

mkdir -p ../sql/ddl

cat > ../sql/ddl/05_create_staging_tables.sql << 'EOF'
-- ============================================================================
-- Staging Tables Creation
-- Temporary landing zone for source data before transformation
-- All tables have minimal constraints for maximum flexibility during load
-- ============================================================================

-- Create staging schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS staging;

SET search_path TO staging;

-- ────────────────────────────────────────────────────────────────────────────
-- STAGING_CUSTOMERS - Raw customer data from source
-- ────────────────────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS staging_customers CASCADE;

CREATE TABLE staging_customers (
    -- Business keys
    customer_id         INTEGER,
    first_name          VARCHAR(50),
    last_name           VARCHAR(50),
    email               VARCHAR(100),
    phone               VARCHAR(20),
    
    -- Address information
    address_line1       VARCHAR(200),
    address_line2       VARCHAR(200),
    city                VARCHAR(50),
    state               VARCHAR(50),
    country             VARCHAR(50),
    postal_code         VARCHAR(20),
    
    -- Customer attributes
    registration_date   DATE,
    customer_segment    VARCHAR(20),
    is_active           BOOLEAN,
    
    -- Staging metadata (critical for tracking and debugging)
    loaded_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file         VARCHAR(200),
    batch_id            INTEGER,
    load_status         VARCHAR(20) DEFAULT 'NEW',  -- NEW, PROCESSING, ERROR, COMPLETED
    error_message       TEXT,                        -- Captures any loading errors
    
    -- Index for faster querying in staging
    CONSTRAINT idx_staging_customers_id UNIQUE (customer_id, batch_id)
);

-- Indexes for staging queries
CREATE INDEX idx_staging_customers_email ON staging_customers(email);
CREATE INDEX idx_staging_customers_load_status ON staging_customers(load_status);

COMMENT ON TABLE staging_customers IS 'Raw customer data landing zone';
COMMENT ON COLUMN staging_customers.loaded_at IS 'Timestamp when data was loaded into staging';
COMMENT ON COLUMN staging_customers.batch_id IS 'Batch identifier for ETL runs';
COMMENT ON COLUMN staging_customers.load_status IS 'Status of the staging record (NEW, PROCESSING, ERROR, COMPLETED)';

-- ────────────────────────────────────────────────────────────────────────────
-- STAGING_PRODUCTS - Raw product data from source
-- ────────────────────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS staging_products CASCADE;

CREATE TABLE staging_products (
    -- Business keys
    product_id          INTEGER,
    product_name        VARCHAR(200),
    product_description TEXT,
    sku                 VARCHAR(50),
    barcode             VARCHAR(50),
    
    -- Categorization
    category            VARCHAR(50),
    subcategory         VARCHAR(50),
    brand               VARCHAR(100),
    
    -- Pricing and costs
    unit_price          DECIMAL(10,2),
    unit_cost           DECIMAL(10,2),
    msrp                DECIMAL(10,2),
    
    -- Product attributes
    supplier_name       VARCHAR(100),
    color               VARCHAR(30),
    size                VARCHAR(20),
    weight_kg           DECIMAL(8,2),
    is_active           BOOLEAN,
    
    -- Staging metadata
    loaded_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file         VARCHAR(200),
    batch_id            INTEGER,
    load_status         VARCHAR(20) DEFAULT 'NEW',
    error_message       TEXT,
    
    CONSTRAINT idx_staging_products_id UNIQUE (product_id, batch_id)
);

CREATE INDEX idx_staging_products_category ON staging_products(category);
CREATE INDEX idx_staging_products_brand ON staging_products(brand);
CREATE INDEX idx_staging_products_load_status ON staging_products(load_status);

COMMENT ON TABLE staging_products IS 'Raw product data landing zone';

-- ────────────────────────────────────────────────────────────────────────────
-- STAGING_STORES - Raw store data from source
-- ────────────────────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS staging_stores CASCADE;

CREATE TABLE staging_stores (
    -- Business keys
    store_id            INTEGER,
    store_name          VARCHAR(100),
    store_type          VARCHAR(30),
    
    -- Location
    address             VARCHAR(200),
    city                VARCHAR(50),
    state               VARCHAR(50),
    country             VARCHAR(50),
    postal_code         VARCHAR(20),
    latitude            DECIMAL(9,6),
    longitude           DECIMAL(9,6),
    
    -- Store attributes
    region              VARCHAR(50),
    district            VARCHAR(50),
    square_footage      INTEGER,
    opening_date        DATE,
    manager_name        VARCHAR(100),
    phone               VARCHAR(20),
    is_active           BOOLEAN,
    
    -- Staging metadata
    loaded_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file         VARCHAR(200),
    batch_id            INTEGER,
    load_status         VARCHAR(20) DEFAULT 'NEW',
    error_message       TEXT,
    
    CONSTRAINT idx_staging_stores_id UNIQUE (store_id, batch_id)
);

CREATE INDEX idx_staging_stores_region ON staging_stores(region);
CREATE INDEX idx_staging_stores_load_status ON staging_stores(load_status);

COMMENT ON TABLE staging_stores IS 'Raw store data landing zone';

-- ────────────────────────────────────────────────────────────────────────────
-- STAGING_ORDERS - Raw order header data from source
-- ────────────────────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS staging_orders CASCADE;

CREATE TABLE staging_orders (
    -- Order identifiers
    order_id            INTEGER,
    invoice_number      VARCHAR(50),
    order_date          DATE,
    
    -- Foreign keys (raw IDs)
    customer_id         INTEGER,
    store_id            INTEGER,
    
    -- Order totals (from source)
    subtotal            DECIMAL(10,2),
    tax_amount          DECIMAL(10,2),
    shipping_amount     DECIMAL(10,2),
    total_amount        DECIMAL(10,2),
    
    -- Order attributes
    order_status        VARCHAR(20),
    payment_method      VARCHAR(50),
    shipping_method     VARCHAR(50),
    
    -- Timestamps
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP,
    
    -- Staging metadata
    loaded_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file         VARCHAR(200),
    batch_id            INTEGER,
    load_status         VARCHAR(20) DEFAULT 'NEW',
    error_message       TEXT,
    
    CONSTRAINT idx_staging_orders_id UNIQUE (order_id, batch_id)
);

CREATE INDEX idx_staging_orders_customer ON staging_orders(customer_id);
CREATE INDEX idx_staging_orders_date ON staging_orders(order_date);
CREATE INDEX idx_staging_orders_status ON staging_orders(order_status);
CREATE INDEX idx_staging_orders_load_status ON staging_orders(load_status);

COMMENT ON TABLE staging_orders IS 'Raw order header data landing zone';

-- ────────────────────────────────────────────────────────────────────────────
-- STAGING_ORDER_ITEMS - Raw order line item data from source
-- ────────────────────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS staging_order_items CASCADE;

CREATE TABLE staging_order_items (
    -- Composite key
    order_id            INTEGER,
    line_number         INTEGER,
    
    -- Product reference
    product_id          INTEGER,
    
    -- Line item details
    quantity            INTEGER,
    unit_price          DECIMAL(10,2),
    discount_percent    DECIMAL(5,2),
    discount_amount     DECIMAL(10,2),
    tax_percent         DECIMAL(5,2),
    tax_amount          DECIMAL(10,2),
    line_total          DECIMAL(10,2),
    
    -- Staging metadata
    loaded_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file         VARCHAR(200),
    batch_id            INTEGER,
    load_status         VARCHAR(20) DEFAULT 'NEW',
    error_message       TEXT,
    
    CONSTRAINT idx_staging_order_items_id UNIQUE (order_id, line_number, batch_id)
);

CREATE INDEX idx_staging_order_items_product ON staging_order_items(product_id);
CREATE INDEX idx_staging_order_items_load_status ON staging_order_items(load_status);

COMMENT ON TABLE staging_order_items IS 'Raw order line item data landing zone';

-- ────────────────────────────────────────────────────────────────────────────
-- STAGING_PAYMENTS - Raw payment transaction data from source
-- ────────────────────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS staging_payments CASCADE;

CREATE TABLE staging_payments (
    -- Payment identifiers
    payment_id          INTEGER,
    order_id            INTEGER,
    payment_date        DATE,
    
    -- Payment details
    payment_method      VARCHAR(50),
    payment_amount      DECIMAL(10,2),
    transaction_id      VARCHAR(100),
    payment_status      VARCHAR(20),
    
    -- Staging metadata
    loaded_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file         VARCHAR(200),
    batch_id            INTEGER,
    load_status         VARCHAR(20) DEFAULT 'NEW',
    error_message       TEXT,
    
    CONSTRAINT idx_staging_payments_id UNIQUE (payment_id, batch_id)
);

CREATE INDEX idx_staging_payments_order ON staging_payments(order_id);
CREATE INDEX idx_staging_payments_load_status ON staging_payments(load_status);

COMMENT ON TABLE staging_payments IS 'Raw payment transaction data landing zone';

-- ────────────────────────────────────────────────────────────────────────────
-- STAGING_ERROR_LOG - Track any data quality issues
-- ────────────────────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS staging_error_log CASCADE;

CREATE TABLE staging_error_log (
    error_id            BIGSERIAL PRIMARY KEY,
    table_name          VARCHAR(50) NOT NULL,
    batch_id            INTEGER,
    source_file         VARCHAR(200),
    record_data         JSONB,           -- Store the problematic record
    error_type          VARCHAR(50),      -- 'NULL_REQUIRED', 'TYPE_MISMATCH', 'FK_VIOLATION', etc.
    error_message       TEXT,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_error_log_batch ON staging_error_log(batch_id);
CREATE INDEX idx_error_log_table ON staging_error_log(table_name);

COMMENT ON TABLE staging_error_log IS 'Logs data quality errors from staging loads';

-- ────────────────────────────────────────────────────────────────────────────
-- Create a view to monitor staging status
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW v_staging_status AS
SELECT 
    'staging_customers' as table_name,
    COUNT(*) as total_records,
    COUNT(CASE WHEN load_status = 'NEW' THEN 1 END) as pending,
    COUNT(CASE WHEN load_status = 'ERROR' THEN 1 END) as errors,
    COUNT(CASE WHEN load_status = 'COMPLETED' THEN 1 END) as completed,
    MAX(loaded_at) as last_load
FROM staging_customers
UNION ALL
SELECT 
    'staging_products',
    COUNT(*),
    COUNT(CASE WHEN load_status = 'NEW' THEN 1 END),
    COUNT(CASE WHEN load_status = 'ERROR' THEN 1 END),
    COUNT(CASE WHEN load_status = 'COMPLETED' THEN 1 END),
    MAX(loaded_at)
FROM staging_products
UNION ALL
SELECT 
    'staging_stores',
    COUNT(*),
    COUNT(CASE WHEN load_status = 'NEW' THEN 1 END),
    COUNT(CASE WHEN load_status = 'ERROR' THEN 1 END),
    COUNT(CASE WHEN load_status = 'COMPLETED' THEN 1 END),
    MAX(loaded_at)
FROM staging_stores
UNION ALL
SELECT 
    'staging_orders',
    COUNT(*),
    COUNT(CASE WHEN load_status = 'NEW' THEN 1 END),
    COUNT(CASE WHEN load_status = 'ERROR' THEN 1 END),
    COUNT(CASE WHEN load_status = 'COMPLETED' THEN 1 END),
    MAX(loaded_at)
FROM staging_orders
UNION ALL
SELECT 
    'staging_order_items',
    COUNT(*),
    COUNT(CASE WHEN load_status = 'NEW' THEN 1 END),
    COUNT(CASE WHEN load_status = 'ERROR' THEN 1 END),
    COUNT(CASE WHEN load_status = 'COMPLETED' THEN 1 END),
    MAX(loaded_at)
FROM staging_order_items
UNION ALL
SELECT 
    'staging_payments',
    COUNT(*),
    COUNT(CASE WHEN load_status = 'NEW' THEN 1 END),
    COUNT(CASE WHEN load_status = 'ERROR' THEN 1 END),
    COUNT(CASE WHEN load_status = 'COMPLETED' THEN 1 END),
    MAX(loaded_at)
FROM staging_payments
ORDER BY table_name;

COMMENT ON VIEW v_staging_status IS 'Monitor staging tables load status';

-- ────────────────────────────────────────────────────────────────────────────
-- Verify tables were created
-- ────────────────────────────────────────────────────────────────────────────
\pset pager off
SELECT 
    'STAGING TABLES' as table_type,
    table_name,
    (SELECT COUNT(*) FROM information_schema.columns 
     WHERE table_schema = 'staging' AND table_name = t.table_name) as column_count
FROM information_schema.tables t
WHERE table_schema = 'staging'
  AND table_type = 'BASE TABLE'
ORDER BY table_name;

-- Show staging status view
SELECT * FROM v_staging_status;
EOF

echo -e "${GREEN}✅ SQL file created at ../sql/ddl/05_create_staging_tables.sql${NC}"

# Run the SQL file
echo -e "\n${YELLOW}⚙️  Creating staging tables in database...${NC}"
echo -e "${BLUE}--------------------------------------------------${NC}"

export PGPASSWORD=$DB_PASSWORD
psql -U $DB_USER -d $DB_NAME -h $DB_HOST -f ../sql/ddl/05_create_staging_tables.sql
PSQL_EXIT_CODE=$?
unset PGPASSWORD

# Check if successful
if [ $PSQL_EXIT_CODE -eq 0 ]; then
    echo -e "${BLUE}--------------------------------------------------${NC}"
    echo -e "${GREEN}✅ Staging tables created successfully!${NC}"
    
    # Show the tables
    echo -e "\n${YELLOW}📊 Staging tables in database:${NC}"
    export PGPASSWORD=$DB_PASSWORD
    psql -U $DB_USER -d $DB_NAME -h $DB_HOST -P pager=off -c "
        SELECT 
            table_name,
            (SELECT COUNT(*) FROM information_schema.columns 
             WHERE table_schema = 'staging' AND table_name = t.table_name) as columns
        FROM information_schema.tables t
        WHERE table_schema = 'staging' 
          AND table_type = 'BASE TABLE'
        ORDER BY table_name;"
    unset PGPASSWORD
    
    # Show summary
    echo -e "\n${YELLOW}📈 Staging tables created:${NC}"
    echo -e "  • staging_customers - Raw customer data"
    echo -e "  • staging_products - Raw product data"
    echo -e "  • staging_stores - Raw store data"
    echo -e "  • staging_orders - Raw order headers"
    echo -e "  • staging_order_items - Raw order line items"
    echo -e "  • staging_payments - Raw payment transactions"
    echo -e "  • staging_error_log - Data quality error tracking"
    echo -e "  • v_staging_status - Monitoring view"
    
else
    echo -e "${RED}❌ Failed to create staging tables (exit code: $PSQL_EXIT_CODE)${NC}"
    exit 1
fi

echo -e "\n${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}✅ All staging tables ready for data ingestion!${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
