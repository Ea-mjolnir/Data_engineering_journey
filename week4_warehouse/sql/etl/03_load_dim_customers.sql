-- ============================================================================
-- ETL 03: Load Customer Dimension (SCD Type 2)
-- ============================================================================
SET search_path TO warehouse;
DO $$
DECLARE
    v_updated_count INTEGER := 0;
    v_inserted_count INTEGER := 0;
BEGIN
    WITH changed_customers AS (
        SELECT s.customer_id
        FROM staging.staging_customers s
        JOIN dim_customer d ON s.customer_id = d.customer_id AND d.is_current = true
        WHERE COALESCE(s.first_name,'') <> COALESCE(d.first_name,'')
           OR COALESCE(s.last_name,'') <> COALESCE(d.last_name,'')
           OR COALESCE(s.email,'') <> COALESCE(d.email,'')
    )
    UPDATE dim_customer d
    SET is_current = false, end_date = CURRENT_DATE - 1
    FROM changed_customers c
    WHERE d.customer_id = c.customer_id AND d.is_current = true;
    GET DIAGNOSTICS v_updated_count = ROW_COUNT;
    
    INSERT INTO dim_customer (
        customer_id, first_name, last_name, full_name, email, phone,
        address_line1, address_line2, city, state, country, postal_code,
        customer_segment, registration_date, is_active, effective_date,
        end_date, is_current, source_system
    )
    SELECT 
        customer_id, first_name, last_name,
        first_name || ' ' || last_name, email, phone,
        address_line1, address_line2, city, state, country, postal_code,
        customer_segment, registration_date, is_active,
        CURRENT_DATE, NULL, true, 'STAGING'
    FROM staging.staging_customers s
    WHERE NOT EXISTS (
        SELECT 1 FROM dim_customer d 
        WHERE d.customer_id = s.customer_id AND d.is_current = true
    );
    GET DIAGNOSTICS v_inserted_count = ROW_COUNT;
    RAISE NOTICE 'Customers: % updated, % inserted', v_updated_count, v_inserted_count;
END $$;
SELECT '👥 Customer dimension: ' || COUNT(*) || ' rows' as status FROM dim_customer;
