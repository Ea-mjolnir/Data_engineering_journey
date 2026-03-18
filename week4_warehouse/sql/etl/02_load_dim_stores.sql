-- ============================================================================
-- ETL 02: Load Store Dimension
-- FIXED: No ON CONFLICT
-- ============================================================================
SET search_path TO warehouse;

-- First, check if stores already exist
DO $$
DECLARE
    v_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO v_count FROM dim_store;
    
    IF v_count = 0 THEN
        -- Insert all stores (first time load)
        INSERT INTO dim_store (
            store_id, store_name, store_type, address, city, state,
            country, postal_code, latitude, longitude, region, district,
            square_footage, opening_date, manager_name, phone, is_active
        )
        SELECT 
            store_id, store_name, store_type, address, city, state,
            country, postal_code, latitude, longitude, region, district,
            square_footage, opening_date, manager_name, phone, is_active
        FROM staging.staging_stores;
        
        RAISE NOTICE 'Inserted % stores', (SELECT COUNT(*) FROM dim_store);
    ELSE
        -- Update existing stores
        UPDATE dim_store d
        SET 
            store_name = s.store_name,
            store_type = s.store_type,
            address = s.address,
            city = s.city,
            state = s.state,
            country = s.country,
            postal_code = s.postal_code,
            latitude = s.latitude,
            longitude = s.longitude,
            region = s.region,
            district = s.district,
            square_footage = s.square_footage,
            opening_date = s.opening_date,
            manager_name = s.manager_name,
            phone = s.phone,
            is_active = s.is_active,
            updated_at = CURRENT_TIMESTAMP
        FROM staging.staging_stores s
        WHERE d.store_id = s.store_id;
        
        -- Insert any new stores
        INSERT INTO dim_store (
            store_id, store_name, store_type, address, city, state,
            country, postal_code, latitude, longitude, region, district,
            square_footage, opening_date, manager_name, phone, is_active
        )
        SELECT 
            store_id, store_name, store_type, address, city, state,
            country, postal_code, latitude, longitude, region, district,
            square_footage, opening_date, manager_name, phone, is_active
        FROM staging.staging_stores s
        WHERE NOT EXISTS (
            SELECT 1 FROM dim_store d WHERE d.store_id = s.store_id
        );
        
        RAISE NOTICE 'Updated stores, total now: %', (SELECT COUNT(*) FROM dim_store);
    END IF;
END $$;

SELECT '🏪 Store dimension: ' || COUNT(*) || ' rows' as status FROM dim_store;
