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
