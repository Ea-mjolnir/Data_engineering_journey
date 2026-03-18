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
DO $$
DECLARE
    current_batch INTEGER;
BEGIN
    current_batch := floor(extract(epoch from now()))::integer;
    
    -- Store batch_id in a temporary table for later use
    CREATE TEMP TABLE current_batch_id AS SELECT current_batch as batch_id;
END $$;

-- ============================================================================
-- LOAD STORES - Direct match with staging_stores (NEW!)
-- ============================================================================
\COPY staging_stores(store_id, store_name, store_type, address, city, state, country, postal_code, latitude, longitude, region, district, square_footage, opening_date, manager_name, phone, is_active) FROM '/home/odinsbeard/Data_engineering_Journey/week4_warehouse/data/sample_large/stores.csv' WITH (FORMAT csv, HEADER true);

-- ============================================================================
-- LOAD CUSTOMERS - Direct match with staging_customers
-- ============================================================================
\COPY staging_customers(customer_id, first_name, last_name, email, phone, address_line1, address_line2, city, state, country, postal_code, registration_date, customer_segment, is_active) FROM '/home/odinsbeard/Data_engineering_Journey/week4_warehouse/data/sample_large/customers.csv' WITH (FORMAT csv, HEADER true);

-- ============================================================================
-- LOAD PRODUCTS - Direct match with staging_products
-- ============================================================================
\COPY staging_products(product_id, product_name, product_description, sku, barcode, category, subcategory, brand, unit_price, unit_cost, msrp, supplier_name, color, size, weight_kg, is_active) FROM '/home/odinsbeard/Data_engineering_Journey/week4_warehouse/data/sample_large/products.csv' WITH (FORMAT csv, HEADER true);

-- ============================================================================
-- LOAD ORDERS - Direct match with staging_orders
-- ============================================================================
\COPY staging_orders(order_id, invoice_number, order_date, customer_id, store_id, subtotal, tax_amount, shipping_amount, total_amount, order_status, payment_method, shipping_method, created_at, updated_at) FROM '/home/odinsbeard/Data_engineering_Journey/week4_warehouse/data/sample_large/orders.csv' WITH (FORMAT csv, HEADER true);

-- ============================================================================
-- LOAD ORDER ITEMS - Direct match with staging_order_items
-- ============================================================================
\COPY staging_order_items(order_id, line_number, product_id, quantity, unit_price, discount_percent, discount_amount, tax_percent, tax_amount, line_total) FROM '/home/odinsbeard/Data_engineering_Journey/week4_warehouse/data/sample_large/order_items.csv' WITH (FORMAT csv, HEADER true);

-- ============================================================================
-- LOAD PAYMENTS - Direct match with staging_payments
-- ============================================================================
\COPY staging_payments(payment_id, order_id, payment_date, payment_method, payment_amount, transaction_id, payment_status) FROM '/home/odinsbeard/Data_engineering_Journey/week4_warehouse/data/sample_large/payments.csv' WITH (FORMAT csv, HEADER true);

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
