#!/usr/bin/env python3
"""
Medallion Architecture (Bronze → Silver → Gold) with Delta Lake
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta import *

# ============================================================================
# SPARK SESSION
# ============================================================================

builder = SparkSession.builder \
    .appName("MedallionArchitecture") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("✅ Spark with Delta Lake created")

# ============================================================================
# BRONZE LAYER - Raw data, exactly as ingested
# ============================================================================

print("\n" + "=" * 70)
print("🥉 BRONZE LAYER - Raw Data")
print("=" * 70)

# Read source CSV data
df_orders = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("data/bronze/orders.csv")

df_items = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("data/bronze/order_items.csv")

df_customers = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("data/bronze/customers.csv")

df_products = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("data/bronze/products.csv")

# Write to Bronze Delta tables (with partitioning)
df_orders.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("order_date") \
    .save("data/bronze/orders")

df_items.write \
    .format("delta") \
    .mode("overwrite") \
    .save("data/bronze/order_items")

df_customers.write \
    .format("delta") \
    .mode("overwrite") \
    .save("data/bronze/customers")

df_products.write \
    .format("delta") \
    .mode("overwrite") \
    .save("data/bronze/products")

print("✅ Bronze layer created with Delta format")

# Show Bronze stats
print("\n📊 Bronze Layer Statistics:")
print(f"Orders: {df_orders.count():,} rows")
print(f"Order Items: {df_items.count():,} rows")
print(f"Customers: {df_customers.count():,} rows")
print(f"Products: {df_products.count():,} rows")

# ============================================================================
# SILVER LAYER - Cleaned, validated, enriched
# ============================================================================

print("\n" + "=" * 70)
print("🥈 SILVER LAYER - Cleaned & Enriched")
print("=" * 70)

# Read from Bronze
bronze_orders = spark.read.format("delta").load("data/bronze/orders")
bronze_items = spark.read.format("delta").load("data/bronze/order_items")
bronze_customers = spark.read.format("delta").load("data/bronze/customers")
bronze_products = spark.read.format("delta").load("data/bronze/products")

# Clean customers - only active, with full name
silver_customers = bronze_customers \
    .filter(col("is_active") == True) \
    .withColumn("full_name", concat(col("first_name"), lit(" "), col("last_name"))) \
    .drop("first_name", "last_name")

# Clean products - add margin
silver_products = bronze_products \
    .withColumn("profit_margin", round((col("price") - col("cost")) / col("price") * 100, 2))

# Enrich order items with product details
silver_items = bronze_items \
    .join(bronze_products.select("product_id", "product_name", "category"), "product_id") \
    .withColumn("revenue", col("quantity") * col("unit_price"))

# Enrich orders with customer and item details
order_totals = silver_items \
    .groupBy("order_id") \
    .agg(
        sum("revenue").alias("actual_total"),
        count("*").alias("line_items")
    )

silver_orders = bronze_orders \
    .join(order_totals, "order_id", "left") \
    .withColumn("data_quality", 
                when(col("total_amount") == col("actual_total"), "good")
                .otherwise("bad"))

# Write Silver Delta tables
silver_customers.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema","true")\
    .save("data/silver/customers")

silver_products.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema","true")\
    .save("data/silver/products")

silver_orders.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema","true")\
    .partitionBy("order_date") \
    .save("data/silver/orders")

silver_items.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema","true")\
    .save("data/silver/order_items")

print("✅ Silver layer created with cleaned data")

print("\n📊 Silver Layer Statistics:")
print(f"Active Customers: {silver_customers.count():,}")
print(f"Products with margin: {silver_products.count():,}")
quality_check = silver_orders.groupBy("data_quality").count().collect()
for row in quality_check:
    print(f"Orders - {row['data_quality']}: {row['count']:,}")

# ============================================================================
# GOLD LAYER - Business-ready aggregates
# ============================================================================

print("\n" + "=" * 70)
print("🥇 GOLD LAYER - Business Aggregates")
print("=" * 70)

# Customer 360 view
customer_orders = silver_orders \
    .groupBy("customer_id") \
    .agg(
        count("order_id").alias("total_orders"),
        sum("actual_total").alias("lifetime_value"),
        avg("actual_total").alias("avg_order_value"),
        min("order_date").alias("first_order"),
        max("order_date").alias("last_order")
    )

gold_customer_360 = customer_orders \
    .join(silver_customers, "customer_id") \
    .select("customer_id", "full_name", "email", "city", "state",
            "total_orders", "lifetime_value", "avg_order_value",
            "first_order", "last_order")

gold_customer_360.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("data/gold/customer_360")

print("✅ Customer 360 created")

# Daily sales summary
gold_daily_sales = silver_orders \
    .groupBy("order_date") \
    .agg(
        count("order_id").alias("order_count"),
        sum("actual_total").alias("revenue"),
        avg("actual_total").alias("avg_order_value")
    ) \
    .orderBy("order_date")

gold_daily_sales.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("data/gold/daily_sales")

print("✅ Daily sales summary created")

# Product performance
gold_product_performance = silver_items \
    .groupBy("product_id", "product_name", "category") \
    .agg(
        sum("quantity").alias("units_sold"),
        sum("revenue").alias("total_revenue"),
        avg("unit_price").alias("avg_price")
    ) \
    .orderBy(desc("total_revenue"))

gold_product_performance.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("data/gold/product_performance")


print("✅ Product performance created")

# Show Gold samples
print("\n📊 Gold Layer Samples:")
print("\n🏆 Top 5 Customers:")
gold_customer_360.orderBy(desc("lifetime_value")).show(5, truncate=False)

print("\n📈 Daily Sales (last 10 days):")
gold_daily_sales.orderBy(desc("order_date")).show(10)

print("\n🔥 Top 5 Products:")
gold_product_performance.show(5, truncate=False)

# ============================================================================
# DEMO: TIME TRAVEL ON SILVER LAYER
# ============================================================================

print("\n" + "=" * 70)
print("⏰ DEMO: Time Travel on Silver Layer")
print("=" * 70)

# Make some changes to silver layer
print("\n📝 Making changes to silver customers...")
from delta.tables import DeltaTable

# Update a customer's city
delta_customers = DeltaTable.forPath(spark, "data/silver/customers")
delta_customers.update(
    condition="customer_id = 1",
    set={"city": "'NEW CITY'"}
)
print("✅ Updated customer 1's city")

# Wait a moment
import time
time.sleep(2)

# Show current data
current = spark.read.format("delta").load("data/silver/customers") \
    .filter("customer_id = 1") \
    .select("customer_id", "full_name", "city")
print("\n📍 Current data:")
current.show()

# Time travel back to previous version
previous = spark.read.format("delta") \
    .option("versionAsOf", 0) \
    .load("data/silver/customers") \
    .filter("customer_id = 1") \
    .select("customer_id", "full_name", "city")
print("\n⏪ Previous version (before update):")
previous.show()

# Show version history
history = DeltaTable.forPath(spark, "data/silver/customers").history()
history.select("version", "timestamp", "operation").show(truncate=False)

# ============================================================================
# SUMMARY
# ============================================================================

print("\n" + "=" * 70)
print("🎉 MEDALLION ARCHITECTURE COMPLETE!")
print("=" * 70)
print("""
✅ Bronze Layer: Raw Delta tables (ACID, partitioned)
✅ Silver Layer: Cleaned, validated, enriched
✅ Gold Layer: Business aggregates, customer 360
✅ Time Travel: Query any historical version
✅ ACID Guarantees: All operations are atomic

Your Lakehouse is ready for production!
""")

spark.stop()
