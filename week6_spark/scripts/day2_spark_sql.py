#!/usr/bin/env python3
"""
DAY 2: Spark SQL - Query 20 Million Rows with Pure SQL!
You already know SQL - now use it with Spark!
"""

from pyspark.sql import SparkSession
import time
import os

# ============================================================================
# STEP 1: CREATE SPARK SESSION (same as Day 1)
# ============================================================================
print("=" * 70)
print("📊 DAY 2: SPARK SQL - Query with Pure SQL!")
print("=" * 70)

spark = SparkSession.builder \
    .appName("Day2_Spark_SQL") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.ui.port", "4040") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print(f"\n✅ Spark session created")
print(f"   • Version: {spark.version}")
print(f"   • Web UI: http://localhost:4040")

# ============================================================================
# STEP 2: LOAD ALL DATA (same as Day 1)
# ============================================================================
print("\n" + "=" * 70)
print("📥 STEP 2: LOADING ALL DATA")
print("=" * 70)

data_dir = "/home/odinsbeard/Data_engineering_Journey/week6_spark/data/input"

# Read ALL sales files
print("\n📁 Reading ALL sales files...")
df_sales = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(os.path.join(data_dir, "sales_*.csv"))

# Read ALL product files
df_products = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(os.path.join(data_dir, "products_*.csv"))

# Read ALL user files
df_users = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(os.path.join(data_dir, "users_*.csv"))

print(f"\n✅ Data loaded:")
print(f"   • Sales: {df_sales.count():,} rows")
print(f"   • Products: {df_products.count():,} rows")
print(f"   • Users: {df_users.count():,} rows")

# ============================================================================
# STEP 3: REGISTER AS TEMPORARY VIEWS (THE MAGIC OF SPARK SQL!)
# ============================================================================
print("\n" + "=" * 70)
print("✨ STEP 3: REGISTERING TABLES FOR SQL")
print("=" * 70)

# This is the key step - makes DataFrames queryable with SQL!
df_sales.createOrReplaceTempView("sales")
df_products.createOrReplaceTempView("products")
df_users.createOrReplaceTempView("users")

print("\n✅ Tables registered:")
print("   • sales   → SELECT * FROM sales")
print("   • products → SELECT * FROM products")
print("   • users   → SELECT * FROM users")
print("\n💡 Now you can use PURE SQL on 20 million rows!")

# ============================================================================
# STEP 4: BASIC SQL QUERIES (Exactly like PostgreSQL!)
# ============================================================================
print("\n" + "=" * 70)
print("🔍 STEP 4: BASIC SQL QUERIES")
print("=" * 70)

# Query 1: Simple aggregation (same as SQL!)
print("\n📊 Query 1: Total sales by country")
start = time.time()

result1 = spark.sql("""
    SELECT 
        country,
        COUNT(*) as order_count,
        SUM(final_amount) as total_revenue,
        AVG(final_amount) as avg_order_value
    FROM sales
    GROUP BY country
    ORDER BY total_revenue DESC
""")

result1.show()
print(f"⏱️  Time: {time.time()-start:.2f} seconds")

# Query 2: Filtering (WHERE clause)
print("\n📊 Query 2: High-value orders (>$500)")
start = time.time()

result2 = spark.sql("""
    SELECT 
        sale_id,
        user_id,
        final_amount,
        payment_method,
        country
    FROM sales
    WHERE final_amount > 500
    ORDER BY final_amount DESC
    LIMIT 10
""")

result2.show()
print(f"⏱️  Time: {time.time()-start:.2f} seconds")

# ============================================================================
# STEP 5: SQL JOINS (Combine tables like a pro!)
# ============================================================================
print("\n" + "=" * 70)
print("🤝 STEP 5: SQL JOINS")
print("=" * 70)

# Query 3: Join sales with products
print("\n📊 Query 3: Top products by revenue")
start = time.time()

result3 = spark.sql("""
    SELECT 
        p.product_name,
        p.category,
        COUNT(*) as times_sold,
        SUM(s.final_amount) as total_revenue
    FROM sales s
    JOIN products p ON s.product_id = p.product_id
    GROUP BY p.product_name, p.category
    ORDER BY total_revenue DESC
    LIMIT 10
""")

result3.show(truncate=False)
print(f"⏱️  Time: {time.time()-start:.2f} seconds")

# Query 4: Join sales with users (customer analysis)
print("\n📊 Query 4: Top customers by spending")
start = time.time()

result4 = spark.sql("""
    SELECT 
        u.user_id,
        CONCAT(u.first_name, ' ', u.last_name) as customer_name,
        u.country,
        COUNT(*) as purchase_count,
        SUM(s.final_amount) as total_spent,
        AVG(s.final_amount) as avg_purchase
    FROM sales s
    JOIN users u ON s.user_id = u.user_id
    GROUP BY u.user_id, u.first_name, u.last_name, u.country
    ORDER BY total_spent DESC
    LIMIT 10
""")

result4.show(truncate=False)
print(f"⏱️  Time: {time.time()-start:.2f} seconds")

# ============================================================================
# STEP 6: ADVANCED SQL (Window Functions, Subqueries)
# ============================================================================
print("\n" + "=" * 70)
print("🎯 STEP 6: ADVANCED SQL")
print("=" * 70)

# Query 5: Window Function - Rank products within category
print("\n📊 Query 5: Top 3 products per category (Window Function)")
start = time.time()

result5 = spark.sql("""
    WITH product_sales AS (
        SELECT 
            p.category,
            p.product_name,
            SUM(s.final_amount) as revenue,
            RANK() OVER (PARTITION BY p.category ORDER BY SUM(s.final_amount) DESC) as rank
        FROM sales s
        JOIN products p ON s.product_id = p.product_id
        GROUP BY p.category, p.product_name
    )
    SELECT category, product_name, revenue, rank
    FROM product_sales
    WHERE rank <= 3
    ORDER BY category, rank
""")

result5.show(truncate=False)
print(f"⏱️  Time: {time.time()-start:.2f} seconds")

# Query 6: Subquery - Customers above average spending
print("\n📊 Query 6: Customers spending above average")
start = time.time()

result6 = spark.sql("""
    WITH customer_totals AS (
        SELECT 
            u.user_id,
            CONCAT(u.first_name, ' ', u.last_name) as name,
            SUM(s.final_amount) as total_spent
        FROM sales s
        JOIN users u ON s.user_id = u.user_id
        GROUP BY u.user_id, u.first_name, u.last_name
    )
    SELECT 
        user_id,
        name,
        total_spent,
        (SELECT AVG(total_spent) FROM customer_totals) as overall_avg,
        total_spent - (SELECT AVG(total_spent) FROM customer_totals) as above_average
    FROM customer_totals
    WHERE total_spent > (SELECT AVG(total_spent) FROM customer_totals)
    ORDER BY total_spent DESC
    LIMIT 10
""")

result6.show(truncate=False)
print(f"⏱️  Time: {time.time()-start:.2f} seconds")

# ============================================================================
# STEP 7: DATE-BASED ANALYTICS
# ============================================================================
print("\n" + "=" * 70)
print("📅 STEP 7: DATE-BASED ANALYTICS")
print("=" * 70)

# Query 7: Monthly sales trend
print("\n📊 Query 7: Monthly sales trend")
start = time.time()

result7 = spark.sql("""
    SELECT 
        YEAR(sale_date) as year,
        MONTH(sale_date) as month,
        COUNT(*) as orders,
        SUM(final_amount) as revenue,
        AVG(final_amount) as avg_order
    FROM sales
    GROUP BY YEAR(sale_date), MONTH(sale_date)
    ORDER BY year, month
""")

result7.show(50)  # Show up to 50 rows
print(f"⏱️  Time: {time.time()-start:.2f} seconds")

# Query 8: Customer segmentation with CASE WHEN
print("\n📊 Query 8: Customer segmentation")
start = time.time()

result8 = spark.sql("""
    WITH customer_stats AS (
        SELECT 
            u.user_id,
            CONCAT(u.first_name, ' ', u.last_name) as name,
            u.country,
            COUNT(*) as order_count,
            SUM(s.final_amount) as total_spent,
            AVG(s.final_amount) as avg_order
        FROM sales s
        JOIN users u ON s.user_id = u.user_id
        GROUP BY u.user_id, u.first_name, u.last_name, u.country
    )
    SELECT 
        CASE 
            WHEN total_spent > 1000 THEN 'VIP'
            WHEN total_spent > 500 THEN 'GOLD'
            WHEN total_spent > 100 THEN 'SILVER'
            ELSE 'BRONZE'
        END as segment,
        COUNT(*) as customer_count,
        AVG(total_spent) as avg_spent,
        SUM(total_spent) as total_revenue
    FROM customer_stats
    GROUP BY 
        CASE 
            WHEN total_spent > 1000 THEN 'VIP'
            WHEN total_spent > 500 THEN 'GOLD'
            WHEN total_spent > 100 THEN 'SILVER'
            ELSE 'BRONZE'
        END
    ORDER BY avg_spent DESC
""")

result8.show()
print(f"⏱️  Time: {time.time()-start:.2f} seconds")

# ============================================================================
# STEP 8: COMPARE SPARK SQL vs DATAFRAME API
# ============================================================================
print("\n" + "=" * 70)
print("⚖️ STEP 8: SPARK SQL vs DATAFRAME API")
print("=" * 70)

print("\n✅ BOTH APPROACHES WORK THE SAME!")
print("   • DataFrame API: df.groupBy('country').agg(sum('final_amount'))")
print("   • Spark SQL:     SELECT country, SUM(final_amount) FROM sales GROUP BY country")
print("\n💡 Use whichever you're more comfortable with!")

# ============================================================================
# STEP 8: COMPARE SPARK SQL vs DATAFRAME API
# ============================================================================
print("\n" + "=" * 70)
print("⚖️ STEP 8: SPARK SQL vs DATAFRAME API")
print("=" * 70)

print("\n✅ BOTH APPROACHES WORK THE SAME!")
print("   • DataFrame API: df.groupBy('country').agg(sum('final_amount'))")
print("   • Spark SQL:     SELECT country, SUM(final_amount) FROM sales GROUP BY country")
print("\n💡 Use whichever you're more comfortable with!")

# ============================================================================
# STEP 9: SUMMARY
# ============================================================================
print("\n" + "=" * 70)
print("📊 DAY 2 SUMMARY")
print("=" * 70)

print(f"""
✅ WHAT YOU LEARNED TODAY:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
1. createOrReplaceTempView() - Register DataFrames as SQL tables
2. spark.sql() - Run pure SQL queries on big data
3. SQL JOINs - Combine tables exactly like PostgreSQL
4. Window Functions - RANK() OVER(PARTITION BY...)
5. Subqueries - Nest queries for complex logic
6. CASE WHEN - Conditional logic in SQL
7. Date functions - YEAR(), MONTH() for time analysis

🎯 KEY INSIGHT:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
If you know SQL, you already know Spark SQL!
The same queries that work on PostgreSQL work on 20 million rows!
""")

# ============================================================================
# STEP 10: CLEAN UP
# ============================================================================
print("\n🧹 Cleaning up...")
spark.stop()
print("✅ Spark session stopped")

print("\n" + "=" * 70)
print("🎉 DAY 2 COMPLETE! Ready for Day 3?")
print("=" * 70)
