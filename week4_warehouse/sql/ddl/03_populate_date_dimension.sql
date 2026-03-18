-- ============================================================================
-- Populate Date Dimension
-- Generates dates from 2020-01-01 to 2030-12-31 (11 years)
-- ============================================================================

SET search_path TO warehouse;

-- First, clear existing data if any (optional)
TRUNCATE TABLE dim_date RESTART IDENTITY CASCADE;

-- Function to populate date dimension (FIXED: changed variable name)
CREATE OR REPLACE FUNCTION populate_date_dimension(
    start_date DATE,
    end_date DATE
) RETURNS VOID AS $$
DECLARE
    curr_date DATE := start_date;  -- Changed from 'current_date' to 'curr_date'
BEGIN
    WHILE curr_date <= end_date LOOP
        INSERT INTO dim_date (
            date_id,
            date,
            day_of_month,
            day_of_week,
            day_name,
            day_of_year,
            week_of_year,
            week_start_date,
            week_end_date,
            month,
            month_name,
            month_abbr,
            quarter,
            quarter_name,
            year,
            is_weekend,
            is_last_day_of_month,
            is_last_day_of_quarter,
            is_last_day_of_year
        ) VALUES (
            TO_CHAR(curr_date, 'YYYYMMDD')::INTEGER,
            curr_date,
            EXTRACT(DAY FROM curr_date)::INTEGER,
            EXTRACT(ISODOW FROM curr_date)::INTEGER,
            INITCAP(TO_CHAR(curr_date, 'Day')),
            EXTRACT(DOY FROM curr_date)::INTEGER,
            EXTRACT(WEEK FROM curr_date)::INTEGER,
            (DATE_TRUNC('week', curr_date))::DATE,
            (DATE_TRUNC('week', curr_date) + INTERVAL '6 days')::DATE,
            EXTRACT(MONTH FROM curr_date)::INTEGER,
            INITCAP(TO_CHAR(curr_date, 'Month')),
            TO_CHAR(curr_date, 'Mon'),
            EXTRACT(QUARTER FROM curr_date)::INTEGER,
            'Q' || EXTRACT(QUARTER FROM curr_date)::TEXT,
            EXTRACT(YEAR FROM curr_date)::INTEGER,
            EXTRACT(ISODOW FROM curr_date) IN (6, 7),
            curr_date = (DATE_TRUNC('month', curr_date) + INTERVAL '1 month - 1 day')::DATE,
            curr_date = (DATE_TRUNC('quarter', curr_date) + INTERVAL '3 months - 1 day')::DATE,
            curr_date = (DATE_TRUNC('year', curr_date) + INTERVAL '1 year - 1 day')::DATE
        );
        
        curr_date := curr_date + INTERVAL '1 day';  -- Fixed variable name
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Populate from 2020 to 2030 (11 years of dates)
SELECT populate_date_dimension('2020-01-01'::DATE, '2030-12-31'::DATE);

-- Add US holidays
UPDATE dim_date SET is_holiday = TRUE, holiday_name = 'New Year''s Day'
WHERE month = 1 AND day_of_month = 1;

UPDATE dim_date SET is_holiday = TRUE, holiday_name = 'Martin Luther King Jr. Day'
WHERE month = 1 AND day_of_week = 1 AND day_of_month BETWEEN 15 AND 21;

UPDATE dim_date SET is_holiday = TRUE, holiday_name = 'Presidents'' Day'
WHERE month = 2 AND day_of_week = 1 AND day_of_month BETWEEN 15 AND 21;

UPDATE dim_date SET is_holiday = TRUE, holiday_name = 'Memorial Day'
WHERE month = 5 AND day_of_week = 1 AND day_of_month BETWEEN 25 AND 31;

UPDATE dim_date SET is_holiday = TRUE, holiday_name = 'Juneteenth'
WHERE month = 6 AND day_of_month = 19;

UPDATE dim_date SET is_holiday = TRUE, holiday_name = 'Independence Day'
WHERE month = 7 AND day_of_month = 4;

UPDATE dim_date SET is_holiday = TRUE, holiday_name = 'Labor Day'
WHERE month = 9 AND day_of_week = 1 AND day_of_month <= 7;

UPDATE dim_date SET is_holiday = TRUE, holiday_name = 'Columbus Day'
WHERE month = 10 AND day_of_week = 1 AND day_of_month BETWEEN 8 AND 14;

UPDATE dim_date SET is_holiday = TRUE, holiday_name = 'Veterans Day'
WHERE month = 11 AND day_of_month = 11;

UPDATE dim_date SET is_holiday = TRUE, holiday_name = 'Thanksgiving'
WHERE month = 11 AND day_of_week = 4 AND day_of_month BETWEEN 22 AND 28;

UPDATE dim_date SET is_holiday = TRUE, holiday_name = 'Christmas'
WHERE month = 12 AND day_of_month = 25;

UPDATE dim_date SET is_holiday = TRUE, holiday_name = 'New Year''s Eve'
WHERE month = 12 AND day_of_month = 31;

-- Add fiscal year (if fiscal year starts in February)
UPDATE dim_date SET 
    fiscal_year = CASE 
        WHEN month >= 2 THEN year 
        ELSE year - 1 
    END,
    fiscal_quarter = CASE 
        WHEN month >= 2 THEN 
            CASE 
                WHEN month BETWEEN 2 AND 4 THEN 1
                WHEN month BETWEEN 5 AND 7 THEN 2
                WHEN month BETWEEN 8 AND 10 THEN 3
                ELSE 4
            END
        ELSE -- January is in previous year's Q4
            4
    END;

-- Verification
SELECT 
    COUNT(*) as total_dates,
    MIN(date) as earliest_date,
    MAX(date) as latest_date,
    COUNT(*) FILTER (WHERE is_weekend) as weekend_days,
    COUNT(*) FILTER (WHERE is_holiday) as holidays
FROM dim_date;

-- Show sample
SELECT 
    date,
    day_name,
    month_name,
    year,
    quarter_name,
    CASE WHEN is_weekend THEN '🏁 Weekend' ELSE '💼 Weekday' END as day_type,
    CASE WHEN is_holiday THEN '🎉 ' || holiday_name ELSE '' END as holiday
FROM dim_date 
WHERE year = 2024 AND month = 12 
ORDER BY date
LIMIT 15;
