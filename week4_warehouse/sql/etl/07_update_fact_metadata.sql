-- ============================================================================
-- ETL 07: Update Fact Table Metadata
-- FIXED: Removed ON CONFLICT, using upsert logic
-- ============================================================================
SET search_path TO warehouse;

-- Update fact_sales metadata
DO $$
DECLARE
    v_count INTEGER;
    v_fact_sales_rows BIGINT;
    v_fact_sales_min INTEGER;
    v_fact_sales_max INTEGER;
    v_daily_summary_rows BIGINT;
    v_daily_summary_min INTEGER;
    v_daily_summary_max INTEGER;
BEGIN
    -- Get fact_sales stats
    SELECT COUNT(*), MIN(date_key), MAX(date_key) 
    INTO v_fact_sales_rows, v_fact_sales_min, v_fact_sales_max
    FROM fact_sales;
    
    -- Get daily summary stats
    SELECT COUNT(*), MIN(date_key), MAX(date_key) 
    INTO v_daily_summary_rows, v_daily_summary_min, v_daily_summary_max
    FROM fact_daily_sales_summary;
    
    -- Check if fact_sales metadata exists
    SELECT COUNT(*) INTO v_count FROM fact_table_metadata WHERE table_name = 'fact_sales';
    
    IF v_count = 0 THEN
        INSERT INTO fact_table_metadata (table_name, grain_description, row_count, min_date_key, max_date_key, last_refresh, refresh_status)
        VALUES ('fact_sales', 'Order line item grain', v_fact_sales_rows, v_fact_sales_min, v_fact_sales_max, CURRENT_TIMESTAMP, 'COMPLETED');
    ELSE
        UPDATE fact_table_metadata 
        SET row_count = v_fact_sales_rows,
            min_date_key = v_fact_sales_min,
            max_date_key = v_fact_sales_max,
            last_refresh = CURRENT_TIMESTAMP,
            refresh_status = 'COMPLETED'
        WHERE table_name = 'fact_sales';
    END IF;
    
    -- Check if daily summary metadata exists
    SELECT COUNT(*) INTO v_count FROM fact_table_metadata WHERE table_name = 'fact_daily_sales_summary';
    
    IF v_count = 0 THEN
        INSERT INTO fact_table_metadata (table_name, grain_description, row_count, min_date_key, max_date_key, last_refresh, refresh_status)
        VALUES ('fact_daily_sales_summary', 'Daily aggregated by store/product', v_daily_summary_rows, v_daily_summary_min, v_daily_summary_max, CURRENT_TIMESTAMP, 'COMPLETED');
    ELSE
        UPDATE fact_table_metadata 
        SET row_count = v_daily_summary_rows,
            min_date_key = v_daily_summary_min,
            max_date_key = v_daily_summary_max,
            last_refresh = CURRENT_TIMESTAMP,
            refresh_status = 'COMPLETED'
        WHERE table_name = 'fact_daily_sales_summary';
    END IF;
END $$;

SELECT '📋 Metadata updated' as status;
SELECT * FROM fact_table_metadata;
