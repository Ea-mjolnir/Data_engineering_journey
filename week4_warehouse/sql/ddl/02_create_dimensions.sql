-- ============================================================================
-- Dimension Tables Creation
-- Star Schema for E-Commerce Analytics
-- ============================================================================

SET search_path TO warehouse;

-- ────────────────────────────────────────────────────────────────────────────
-- DIM_DATE - Date Dimension
-- ────────────────────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS dim_date CASCADE;

CREATE TABLE dim_date (
    date_id         INTEGER PRIMARY KEY,
    date            DATE NOT NULL UNIQUE,
    day_of_month    INTEGER NOT NULL,
    day_of_week     INTEGER NOT NULL,
    day_name        VARCHAR(10) NOT NULL,
    day_of_year     INTEGER NOT NULL,
    week_of_year    INTEGER NOT NULL,
    week_start_date DATE NOT NULL,
    week_end_date   DATE NOT NULL,
    month           INTEGER NOT NULL,
    month_name      VARCHAR(10) NOT NULL,
    month_abbr      VARCHAR(3) NOT NULL,
    quarter         INTEGER NOT NULL,
    quarter_name    VARCHAR(2) NOT NULL,
    year            INTEGER NOT NULL,
    fiscal_year     INTEGER,
    fiscal_quarter  INTEGER,
    is_weekend      BOOLEAN NOT NULL,
    is_holiday      BOOLEAN DEFAULT FALSE,
    holiday_name    VARCHAR(50),
    is_last_day_of_month BOOLEAN NOT NULL,
    is_last_day_of_quarter BOOLEAN NOT NULL,
    is_last_day_of_year BOOLEAN NOT NULL,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_date_date ON dim_date(date);
CREATE INDEX idx_dim_date_year_month ON dim_date(year, month);
CREATE INDEX idx_dim_date_quarter ON dim_date(year, quarter);

COMMENT ON TABLE dim_date IS 'Date dimension for time-based analysis';

-- ────────────────────────────────────────────────────────────────────────────
-- DIM_CUSTOMER - Customer Dimension
-- ────────────────────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS dim_customer CASCADE;

CREATE TABLE dim_customer (
    customer_key        SERIAL PRIMARY KEY,
    customer_id         INTEGER NOT NULL,
    first_name          VARCHAR(50),
    last_name           VARCHAR(50),
    full_name           VARCHAR(100),
    email               VARCHAR(100),
    phone               VARCHAR(20),
    address_line1       VARCHAR(200),
    address_line2       VARCHAR(200),
    city                VARCHAR(50),
    state               VARCHAR(50),
    country             VARCHAR(50),
    postal_code         VARCHAR(20),
    customer_segment    VARCHAR(20),
    registration_date   DATE,
    first_purchase_date DATE,
    last_purchase_date  DATE,
    is_active           BOOLEAN DEFAULT TRUE,
    effective_date      DATE NOT NULL,
    end_date            DATE,
    is_current          BOOLEAN DEFAULT TRUE,
    source_system       VARCHAR(50),
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_customer_id ON dim_customer(customer_id);
CREATE INDEX idx_dim_customer_email ON dim_customer(email);
CREATE INDEX idx_dim_customer_segment ON dim_customer(customer_segment);
CREATE INDEX idx_dim_customer_is_current ON dim_customer(is_current);

COMMENT ON TABLE dim_customer IS 'Customer dimension with SCD Type 2 tracking';

-- ────────────────────────────────────────────────────────────────────────────
-- DIM_PRODUCT - Product Dimension
-- ────────────────────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS dim_product CASCADE;

CREATE TABLE dim_product (
    product_key         SERIAL PRIMARY KEY,
    product_id          INTEGER NOT NULL,
    product_name        VARCHAR(200) NOT NULL,
    product_description TEXT,
    sku                 VARCHAR(50),
    barcode             VARCHAR(50),
    category            VARCHAR(50),
    subcategory         VARCHAR(50),
    brand               VARCHAR(100),
    unit_cost           DECIMAL(10,2),
    unit_price          DECIMAL(10,2),
    msrp                DECIMAL(10,2),
    color               VARCHAR(30),
    size                VARCHAR(20),
    weight_kg           DECIMAL(8,2),
    supplier_name       VARCHAR(100),
    is_active           BOOLEAN DEFAULT TRUE,
    discontinued_date   DATE,
    effective_date      DATE NOT NULL,
    end_date            DATE,
    is_current          BOOLEAN DEFAULT TRUE,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_product_id ON dim_product(product_id);
CREATE INDEX idx_dim_product_category ON dim_product(category);
CREATE INDEX idx_dim_product_brand ON dim_product(brand);
CREATE INDEX idx_dim_product_is_current ON dim_product(is_current);

COMMENT ON TABLE dim_product IS 'Product dimension with SCD Type 2 tracking';

-- ────────────────────────────────────────────────────────────────────────────
-- DIM_STORE - Store Dimension
-- ────────────────────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS dim_store CASCADE;

CREATE TABLE dim_store (
    store_key           SERIAL PRIMARY KEY,
    store_id            INTEGER NOT NULL,
    store_name          VARCHAR(100) NOT NULL,
    store_type          VARCHAR(30),
    address             VARCHAR(200),
    city                VARCHAR(50),
    state               VARCHAR(50),
    country             VARCHAR(50),
    postal_code         VARCHAR(20),
    latitude            DECIMAL(9,6),
    longitude           DECIMAL(9,6),
    region              VARCHAR(50),
    district            VARCHAR(50),
    square_footage      INTEGER,
    opening_date        DATE,
    manager_name        VARCHAR(100),
    phone               VARCHAR(20),
    is_active           BOOLEAN DEFAULT TRUE,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_store_id ON dim_store(store_id);
CREATE INDEX idx_dim_store_region ON dim_store(region);
CREATE INDEX idx_dim_store_city ON dim_store(city);

COMMENT ON TABLE dim_store IS 'Store/location dimension';

-- ────────────────────────────────────────────────────────────────────────────
-- DIM_PAYMENT - Payment Dimension (pre-populated)
-- ────────────────────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS dim_payment CASCADE;

CREATE TABLE dim_payment (
    payment_key         SERIAL PRIMARY KEY,
    payment_method      VARCHAR(30) NOT NULL UNIQUE,
    payment_type        VARCHAR(20),
    payment_category    VARCHAR(20),
    processing_fee_pct  DECIMAL(5,2),
    is_digital          BOOLEAN DEFAULT FALSE,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO dim_payment (payment_method, payment_type, payment_category, processing_fee_pct, is_digital) VALUES
    ('Credit Card', 'Credit', 'Card', 2.9, FALSE),
    ('Debit Card', 'Debit', 'Card', 1.5, FALSE),
    ('PayPal', 'Digital', 'Digital', 2.9, TRUE),
    ('Apple Pay', 'Digital', 'Digital', 2.9, TRUE),
    ('Google Pay', 'Digital', 'Digital', 2.9, TRUE),
    ('Cash', 'Cash', 'Cash', 0.0, FALSE),
    ('Bank Transfer', 'Transfer', 'Transfer', 0.5, TRUE),
    ('Check', 'Check', 'Check', 0.0, FALSE);

COMMENT ON TABLE dim_payment IS 'Payment method dimension (pre-populated)';

-- ────────────────────────────────────────────────────────────────────────────
-- Verify tables were created
-- ────────────────────────────────────────────────────────────────────────────
SELECT 
    table_name,
    (SELECT COUNT(*) FROM information_schema.columns 
     WHERE table_schema = 'warehouse' AND table_name = t.table_name) as column_count
FROM information_schema.tables t
WHERE table_schema = 'warehouse'
  AND table_type = 'BASE TABLE'
ORDER BY table_name;
