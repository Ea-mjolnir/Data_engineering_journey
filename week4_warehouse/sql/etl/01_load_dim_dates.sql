-- ============================================================================
-- ETL 01: Load Date Dimension
-- ============================================================================
SET search_path TO warehouse;
INSERT INTO dim_date (
    date_id, date, day_of_month, day_of_week, day_name,
    day_of_year, week_of_year, week_start_date, week_end_date,
    month, month_name, month_abbr, quarter, quarter_name,
    year, is_weekend, is_last_day_of_month,
    is_last_day_of_quarter, is_last_day_of_year
)
SELECT DISTINCT
    TO_CHAR(d::DATE, 'YYYYMMDD')::INTEGER,
    d::DATE,
    EXTRACT(DAY FROM d)::INTEGER,
    EXTRACT(DOW FROM d)::INTEGER,
    TO_CHAR(d, 'Day'),
    EXTRACT(DOY FROM d)::INTEGER,
    EXTRACT(WEEK FROM d)::INTEGER,
    DATE_TRUNC('week', d)::DATE,
    (DATE_TRUNC('week', d) + INTERVAL '6 days')::DATE,
    EXTRACT(MONTH FROM d)::INTEGER,
    TO_CHAR(d, 'Month'),
    TO_CHAR(d, 'Mon'),
    EXTRACT(QUARTER FROM d)::INTEGER,
    'Q' || EXTRACT(QUARTER FROM d)::TEXT,
    EXTRACT(YEAR FROM d)::INTEGER,
    CASE WHEN EXTRACT(DOW FROM d) IN (0, 6) THEN TRUE ELSE FALSE END,
    (d = (DATE_TRUNC('month', d) + INTERVAL '1 month - 1 day')::DATE),
    (d = (DATE_TRUNC('quarter', d) + INTERVAL '3 months - 1 day')::DATE),
    (d = (DATE_TRUNC('year', d) + INTERVAL '1 year - 1 day')::DATE)
FROM (
    SELECT order_date::DATE as d FROM staging.staging_orders
    UNION
    SELECT registration_date FROM staging.staging_customers WHERE registration_date IS NOT NULL
) dates
WHERE d IS NOT NULL
AND NOT EXISTS (SELECT 1 FROM dim_date dd WHERE dd.date = d::DATE);
SELECT '📅 Date dimension: ' || COUNT(*) || ' rows' as status FROM dim_date;
