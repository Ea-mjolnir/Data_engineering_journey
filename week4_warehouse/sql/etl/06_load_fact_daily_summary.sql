-- ============================================================================
-- ETL 06: Load Fact Daily Sales Summary
-- ============================================================================
SET search_path TO warehouse;
TRUNCATE TABLE fact_daily_sales_summary;
INSERT INTO fact_daily_sales_summary (
    date_key, store_key, product_key, total_orders, total_line_items,
    total_quantity, total_customers, new_customers, total_gross_revenue,
    total_net_revenue, total_discounts, total_tax, total_shipping,
    total_cost, total_profit
)
SELECT 
    date_key, store_key, product_key,
    COUNT(DISTINCT order_id), COUNT(*), SUM(quantity),
    COUNT(DISTINCT customer_key), 0,
    SUM(gross_revenue), SUM(net_revenue), SUM(discount_amount),
    SUM(tax_amount), SUM(shipping_amount), SUM(total_cost), SUM(gross_profit)
FROM fact_sales
GROUP BY date_key, store_key, product_key;
SELECT '📊 Daily summary: ' || COUNT(*) || ' rows' as status FROM fact_daily_sales_summary;
