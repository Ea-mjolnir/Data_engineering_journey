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
