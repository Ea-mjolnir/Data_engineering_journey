#!/bin/bash

################################################################################
# Create All Fact Tables Script
# This script creates all fact tables for the warehouse
# Transaction grain - one row per order line item
################################################################################

set -e  # Exit on error

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}🏗️  Creating All Fact Tables${NC}"
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
echo -e "\n${YELLOW}📝 Creating fact tables SQL file...${NC}"

mkdir -p ../sql/ddl

cat > ../sql/ddl/04_create_fact_tables.sql << 'EOF'
-- ============================================================================
-- Fact Tables Creation
-- Star Schema for E-Commerce Analytics
-- Transaction grain - one row per order line item
-- ============================================================================

SET search_path TO warehouse;

-- ────────────────────────────────────────────────────────────────────────────
-- FACT_SALES - Main sales fact table
-- ────────────────────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS fact_sales CASCADE;

CREATE TABLE fact_sales (
    -- Surrogate key for the fact
    sale_key            BIGSERIAL PRIMARY KEY,
    
    -- Degenerate dimensions (business keys stored in fact)
    order_id            INTEGER NOT NULL,
    order_line_number   INTEGER NOT NULL,
    invoice_number      VARCHAR(50),
    
    -- Foreign keys to dimensions (surrogate keys)
    date_key            INTEGER NOT NULL REFERENCES dim_date(date_id),
    customer_key        INTEGER NOT NULL REFERENCES dim_customer(customer_key),
    product_key         INTEGER NOT NULL REFERENCES dim_product(product_key),
    store_key           INTEGER REFERENCES dim_store(store_key),
    payment_key         INTEGER REFERENCES dim_payment(payment_key),
    
    -- Measures (Additive - can be summed across all dimensions)
    quantity            INTEGER NOT NULL CHECK (quantity > 0),
    unit_price          DECIMAL(10,2) NOT NULL,
    unit_cost           DECIMAL(10,2) NOT NULL,
    discount_amount     DECIMAL(10,2) DEFAULT 0,
    tax_amount          DECIMAL(10,2) DEFAULT 0,
    shipping_amount     DECIMAL(10,2) DEFAULT 0,
    
    -- Calculated measures (derived from above)
    gross_revenue       DECIMAL(10,2) GENERATED ALWAYS AS (quantity * unit_price) STORED,
    net_revenue         DECIMAL(10,2) GENERATED ALWAYS AS (
        quantity * unit_price - discount_amount
    ) STORED,
    total_cost          DECIMAL(10,2) GENERATED ALWAYS AS (quantity * unit_cost) STORED,
    gross_profit        DECIMAL(10,2) GENERATED ALWAYS AS (
        quantity * unit_price - discount_amount - quantity * unit_cost
    ) STORED,
    profit_margin       DECIMAL(5,2) GENERATED ALWAYS AS (
        CASE 
            WHEN (quantity * unit_price - discount_amount) > 0 
            THEN ((quantity * unit_price - discount_amount - quantity * unit_cost) / 
                  (quantity * unit_price - discount_amount) * 100)
            ELSE 0 
        END
    ) STORED,
    
    -- Non-additive measures
    discount_percent    DECIMAL(5,2),
    
    -- Transaction metadata
    order_status        VARCHAR(20),        -- 'Completed', 'Pending', 'Cancelled', 'Returned'
    transaction_type    VARCHAR(20),        -- 'Sale', 'Return', 'Exchange'
    return_reason       VARCHAR(100),       -- If transaction_type = 'Return'
    
    -- Audit columns (matching your dimension tables pattern)
    source_system       VARCHAR(50) DEFAULT 'POS',
    batch_id            INTEGER,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Data quality constraints
    CONSTRAINT check_valid_discount CHECK (discount_amount >= 0),
    CONSTRAINT check_valid_profit_margin CHECK (profit_margin BETWEEN -100 AND 100),
    CONSTRAINT check_valid_tax CHECK (tax_amount >= 0),
    CONSTRAINT check_valid_shipping CHECK (shipping_amount >= 0)
);

-- Indexes for query performance
CREATE INDEX idx_fact_sales_date ON fact_sales(date_key);
CREATE INDEX idx_fact_sales_customer ON fact_sales(customer_key);
CREATE INDEX idx_fact_sales_product ON fact_sales(product_key);
CREATE INDEX idx_fact_sales_store ON fact_sales(store_key);
CREATE INDEX idx_fact_sales_payment ON fact_sales(payment_key);
CREATE INDEX idx_fact_sales_order ON fact_sales(order_id);
CREATE INDEX idx_fact_sales_status ON fact_sales(order_status);
CREATE INDEX idx_fact_sales_batch ON fact_sales(batch_id);

-- Composite indexes for common query patterns
CREATE INDEX idx_fact_sales_date_customer ON fact_sales(date_key, customer_key);
CREATE INDEX idx_fact_sales_date_product ON fact_sales(date_key, product_key);
CREATE INDEX idx_fact_sales_date_store ON fact_sales(date_key, store_key);
CREATE INDEX idx_fact_sales_customer_product ON fact_sales(customer_key, product_key);

-- Comments
COMMENT ON TABLE fact_sales IS 'Sales transactions at order line item grain';
COMMENT ON COLUMN fact_sales.sale_key IS 'Surrogate key for the fact record';
COMMENT ON COLUMN fact_sales.order_id IS 'Degenerate dimension - business order number';
COMMENT ON COLUMN fact_sales.date_key IS 'FK to dim_date - when transaction occurred';
COMMENT ON COLUMN fact_sales.customer_key IS 'FK to dim_customer - who bought';
COMMENT ON COLUMN fact_sales.product_key IS 'FK to dim_product - what was bought';
COMMENT ON COLUMN fact_sales.quantity IS 'Number of units sold (additive)';
COMMENT ON COLUMN fact_sales.gross_revenue IS 'Revenue before discounts (additive)';
COMMENT ON COLUMN fact_sales.net_revenue IS 'Revenue after discounts (additive)';
COMMENT ON COLUMN fact_sales.gross_profit IS 'Profit before expenses (additive)';
COMMENT ON COLUMN fact_sales.profit_margin IS 'Profit percentage (non-additive)';

-- ────────────────────────────────────────────────────────────────────────────
-- FACT_DAILY_SALES_SUMMARY - Aggregated snapshot for fast reporting
-- Grain: One row per day per store per product (if applicable)
-- ────────────────────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS fact_daily_sales_summary CASCADE;

CREATE TABLE fact_daily_sales_summary (
    summary_key         BIGSERIAL PRIMARY KEY,
    
    -- Dimension keys at the summary grain
    date_key            INTEGER NOT NULL REFERENCES dim_date(date_id),
    store_key           INTEGER REFERENCES dim_store(store_key),
    product_key         INTEGER REFERENCES dim_product(product_key),
    
    -- Aggregated measures
    total_orders        INTEGER NOT NULL,
    total_line_items    INTEGER NOT NULL,
    total_quantity      INTEGER NOT NULL,
    total_customers     INTEGER NOT NULL,       -- Distinct count
    new_customers       INTEGER DEFAULT 0,      -- First-time purchasers
    
    -- Financial aggregates
    total_gross_revenue DECIMAL(12,2) NOT NULL,
    total_net_revenue   DECIMAL(12,2) NOT NULL,
    total_discounts     DECIMAL(12,2) DEFAULT 0,
    total_tax           DECIMAL(12,2) DEFAULT 0,
    total_shipping      DECIMAL(12,2) DEFAULT 0,
    total_cost          DECIMAL(12,2) NOT NULL,
    total_profit        DECIMAL(12,2) NOT NULL,
    
    -- Calculated metrics
    avg_order_value     DECIMAL(10,2) GENERATED ALWAYS AS (
        CASE WHEN total_orders > 0 
        THEN total_net_revenue / total_orders 
        ELSE 0 END
    ) STORED,
    avg_items_per_order DECIMAL(8,2) GENERATED ALWAYS AS (
        CASE WHEN total_orders > 0 
        THEN total_quantity::DECIMAL / total_orders 
        ELSE 0 END
    ) STORED,
    profit_margin_pct   DECIMAL(5,2) GENERATED ALWAYS AS (
        CASE WHEN total_net_revenue > 0 
        THEN (total_profit / total_net_revenue * 100)
        ELSE 0 END
    ) STORED,
    
    -- Audit
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Unique constraint to prevent duplicates
    UNIQUE(date_key, store_key, product_key)
);

-- Indexes for summary table
CREATE INDEX idx_daily_summary_date ON fact_daily_sales_summary(date_key);
CREATE INDEX idx_daily_summary_store ON fact_daily_sales_summary(store_key);
CREATE INDEX idx_daily_summary_product ON fact_daily_sales_summary(product_key);

COMMENT ON TABLE fact_daily_sales_summary IS 'Pre-aggregated daily sales for fast reporting';

-- ────────────────────────────────────────────────────────────────────────────
-- FACT_INVENTORY - Inventory snapshot fact table (periodic snapshot)
-- Grain: One row per day per product per store
-- ────────────────────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS fact_inventory CASCADE;

CREATE TABLE fact_inventory (
    inventory_key       BIGSERIAL PRIMARY KEY,
    
    -- Dimension keys
    date_key            INTEGER NOT NULL REFERENCES dim_date(date_id),
    product_key         INTEGER NOT NULL REFERENCES dim_product(product_key),
    store_key           INTEGER NOT NULL REFERENCES dim_store(store_key),
    
    -- Inventory measures
    quantity_on_hand    INTEGER NOT NULL,
    quantity_reserved   INTEGER DEFAULT 0,
    quantity_available  INTEGER GENERATED ALWAYS AS (quantity_on_hand - quantity_reserved) STORED,
    reorder_point       INTEGER,
    reorder_quantity    INTEGER,
    
    -- Inventory value
    unit_cost           DECIMAL(10,2) NOT NULL,
    total_inventory_value DECIMAL(12,2) GENERATED ALWAYS AS (quantity_on_hand * unit_cost) STORED,
    
    -- Inventory metrics
    days_on_hand        INTEGER,              -- Estimated days until stockout
    turnover_rate       DECIMAL(8,2),         -- Inventory turnover
    
    -- Audit
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Unique constraint for the snapshot grain
    UNIQUE(date_key, product_key, store_key)
);

CREATE INDEX idx_inventory_date ON fact_inventory(date_key);
CREATE INDEX idx_inventory_product ON fact_inventory(product_key);
CREATE INDEX idx_inventory_store ON fact_inventory(store_key);

COMMENT ON TABLE fact_inventory IS 'Daily inventory snapshot fact table';

-- ────────────────────────────────────────────────────────────────────────────
-- FACT_TABLE_METADATA - Track fact table statistics (matching your pattern)
-- ────────────────────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS fact_table_metadata CASCADE;

CREATE TABLE fact_table_metadata (
    metadata_key        SERIAL PRIMARY KEY,
    table_name          VARCHAR(50) NOT NULL,
    grain_description   TEXT,
    row_count           BIGINT,
    min_date_key        INTEGER,
    max_date_key        INTEGER,
    last_refresh        TIMESTAMP,
    refresh_status      VARCHAR(20),
    refresh_duration_seconds INTEGER,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert initial records
INSERT INTO fact_table_metadata (table_name, grain_description) VALUES
    ('fact_sales', 'Order line item grain - one row per product in an order'),
    ('fact_daily_sales_summary', 'Daily aggregated sales by store and product'),
    ('fact_inventory', 'Daily inventory snapshot by product and store');

-- ────────────────────────────────────────────────────────────────────────────
-- Create a view for easy sales analysis (optional but useful)
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW v_sales_analysis AS
SELECT 
    d.date,
    d.year,
    d.month_name,
    d.quarter_name,
    c.customer_id,
    c.full_name as customer_name,
    c.customer_segment,
    p.product_id,
    p.product_name,
    p.category,
    p.brand,
    s.store_name,
    s.region,
    pm.payment_method,
    fs.quantity,
    fs.unit_price,
    fs.discount_amount,
    fs.gross_revenue,
    fs.net_revenue,
    fs.gross_profit,
    fs.profit_margin,
    fs.order_status,
    fs.transaction_type
FROM fact_sales fs
JOIN dim_date d ON fs.date_key = d.date_id
JOIN dim_customer c ON fs.customer_key = c.customer_key
JOIN dim_product p ON fs.product_key = p.product_key
LEFT JOIN dim_store s ON fs.store_key = s.store_key
LEFT JOIN dim_payment pm ON fs.payment_key = pm.payment_key
WHERE c.is_current = true  -- Get only current customer records
  AND p.is_current = true;  -- Get only current product records

COMMENT ON VIEW v_sales_analysis IS 'Sales analysis view joining all dimensions for easy querying';

-- ────────────────────────────────────────────────────────────────────────────
-- Verify tables were created (with \pset to avoid pager)
-- ────────────────────────────────────────────────────────────────────────────
\pset pager off
SELECT 
    'FACT TABLES' as table_type,
    table_name,
    (SELECT COUNT(*) FROM information_schema.columns 
     WHERE table_schema = 'warehouse' AND table_name = t.table_name) as column_count
FROM information_schema.tables t
WHERE table_schema = 'warehouse'
  AND table_type = 'BASE TABLE'
  AND table_name LIKE 'fact_%'
ORDER BY table_name;

-- Update statistics
ANALYZE fact_sales;
ANALYZE fact_daily_sales_summary;
ANALYZE fact_inventory;
EOF

echo -e "${GREEN}✅ SQL file created at ../sql/ddl/04_create_fact_tables.sql${NC}"

# Run the SQL file
echo -e "\n${YELLOW}⚙️  Creating fact tables in database...${NC}"
echo -e "${BLUE}--------------------------------------------------${NC}"

export PGPASSWORD=$DB_PASSWORD
psql -U $DB_USER -d $DB_NAME -h $DB_HOST -f ../sql/ddl/04_create_fact_tables.sql
PSQL_EXIT_CODE=$?
unset PGPASSWORD

# Check if successful
if [ $PSQL_EXIT_CODE -eq 0 ]; then
    echo -e "${BLUE}--------------------------------------------------${NC}"
    echo -e "${GREEN}✅ Fact tables created successfully!${NC}"
    
    # Show the tables (with \pset pager off to prevent hanging)
    echo -e "\n${YELLOW}📊 Fact tables in warehouse schema:${NC}"
    export PGPASSWORD=$DB_PASSWORD
    psql -U $DB_USER -d $DB_NAME -h $DB_HOST -P pager=off -c "
        SELECT 
            table_name,
            (SELECT COUNT(*) FROM information_schema.columns 
             WHERE table_schema = 'warehouse' AND table_name = t.table_name) as columns
        FROM information_schema.tables t
        WHERE table_schema = 'warehouse' 
          AND table_type = 'BASE TABLE' 
          AND table_name LIKE 'fact_%'
        ORDER BY table_name;"
    unset PGPASSWORD
    
    # Show summary
    echo -e "\n${YELLOW}📈 Fact tables created:${NC}"
    echo -e "  • fact_sales - Transaction grain (order line items)"
    echo -e "  • fact_daily_sales_summary - Daily aggregated sales"
    echo -e "  • fact_inventory - Daily inventory snapshots"
    echo -e "  • fact_table_metadata - Tracking table statistics"
    echo -e "  • v_sales_analysis - Analysis view joining all dimensions"
    
else
    echo -e "${RED}❌ Failed to create fact tables (exit code: $PSQL_EXIT_CODE)${NC}"
    exit 1
fi

echo -e "\n${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}✅ All fact tables ready!${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
