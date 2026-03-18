#!/usr/bin/env python3
"""
Delta Lake Basics - ACID transactions, time travel, and more!
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, countDistinct, count, avg, min, max, desc, when, concat, lit
from delta import *

# ============================================================================
# CREATE SPARK SESSION WITH DELTA LAKE SUPPORT
# ============================================================================

builder = SparkSession.builder \
    .appName("DeltaLakeBasics") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("✅ Spark with Delta Lake created")

# ============================================================================
# READ CSV DATA AND WRITE AS DELTA
# ============================================================================

print("\n📥 Reading CSV data...")
df_customers = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("data/bronze/customers.csv")

df_orders = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("data/bronze/orders.csv")

df_order_items = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("data/bronze/order_items.csv")

df_products = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("data/bronze/products.csv")

print(f"✅ Loaded: {df_customers.count():,} customers")
print(f"✅ Loaded: {df_products.count():,} products")
print(f"✅ Loaded: {df_orders.count():,} orders")
print(f"✅ Loaded: {df_order_items.count():,} order items")

# ============================================================================
# WRITE TO DELTA FORMAT (BRONZE LAYER)
# ============================================================================

print("\n💾 Writing to Delta format (Bronze layer)...")

# Write with Delta format - this enables ACID transactions!
df_customers.write \
    .mode("overwrite") \
    .format("delta") \
    .save("data/bronze/customers_delta")

df_products.write \
    .mode("overwrite") \
    .format("delta") \
    .save("data/bronze/products_delta")

df_orders.write \
    .mode("overwrite") \
    .format("delta") \
    .save("data/bronze/orders_delta")

df_order_items.write \
    .mode("overwrite") \
    .format("delta") \
    .save("data/bronze/order_items_delta")

print("✅ Bronze Delta tables created")

# ============================================================================
# READ DELTA TABLES (SAME AS READING PARQUET)
# ============================================================================

print("\n📖 Reading Delta tables...")
bronze_customers = spark.read.format("delta").load("data/bronze/customers_delta")
bronze_orders = spark.read.format("delta").load("data/bronze/orders_delta")

print(f"✅ Bronze customers: {bronze_customers.count():,} rows")
bronze_customers.show(5, truncate=False)

# ============================================================================
# DEMO 1: ACID TRANSACTIONS
# ============================================================================

print("\n" + "=" * 70)
print("🔬 DEMO 1: ACID TRANSACTIONS")
print("=" * 70)

# Create a test table
test_data = spark.range(0, 10)
test_data.write.format("delta").mode("overwrite").save("data/demo/acid_test")

print("✅ Created test table with 10 rows")

# Simulate a multi-step transaction
from pyspark.sql.functions import col

try:
    # Start transaction (Delta automatically handles this!)
    print("\n🔄 Running ACID transaction...")
    
    # Step 1: Delete some rows
    spark.read.format("delta").load("data/demo/acid_test") \
        .filter(col("id") < 3) \
        .write.format("delta").mode("overwrite").save("data/demo/acid_test")
    
    # Step 2: Insert new rows
    new_data = spark.range(10, 15)
    new_data.write.format("delta").mode("append").save("data/demo/acid_test")
    
    # Step 3: Update some rows - THIS IS THE MAGIC!
    # With ACID, all steps succeed OR none do
    print("✅ Transaction committed successfully!")
    
except Exception as e:
    print(f"❌ Transaction failed: {e}")
    # If any step fails, NO changes are applied!

# Check final result
final_count = spark.read.format("delta").load("data/demo/acid_test").count()
print(f"\n📊 Final row count: {final_count} (should be 12 if transaction succeeded)")

# ============================================================================
# DEMO 2: TIME TRAVEL (Query historical data)
# ============================================================================

print("\n" + "=" * 70)
print("⏰ DEMO 2: TIME TRAVEL")
print("=" * 70)

# Create a table with history
history_data = spark.range(0, 100)
history_data.write.format("delta").mode("overwrite").save("data/demo/time_travel")

print("✅ Created table with 100 rows")
print("⏳ Waiting 2 seconds...")
import time
time.sleep(2)

# First change
spark.range(100, 150).write.format("delta").mode("append").save("data/demo/time_travel")
print("✅ Appended 50 rows (total 150)")

time.sleep(2)

# Second change
spark.read.format("delta").load("data/demo/time_travel") \
    .filter("id < 50") \
    .write.format("delta").mode("overwrite").save("data/demo/time_travel")
print("✅ Deleted first 50 rows (total 100)")

# Show current version
current_count = spark.read.format("delta").load("data/demo/time_travel").count()
print(f"\n📊 Current row count: {current_count}")

# TIME TRAVEL - Go back to previous versions!
print("\n⏪ TIME TRAVEL - Going back to version 1...")
version_1 = spark.read.format("delta") \
    .option("versionAsOf", 1) \
    .load("data/demo/time_travel")
print(f"📊 Version 1 count: {version_1.count()} (should be 100)")

print("⏪ TIME TRAVEL - Going back to version 0...")
version_0 = spark.read.format("delta") \
    .option("versionAsOf", 0) \
    .load("data/demo/time_travel")
print(f"📊 Version 0 count: {version_0.count()} (should be 100)")

# View history
print("\n📜 Table History:")
from delta.tables import DeltaTable
delta_table = DeltaTable.forPath(spark, "data/demo/time_travel")
history = delta_table.history().select("version", "timestamp", "operation", "operationMetrics")
history.show(truncate=False)

# ============================================================================
# DEMO 3: UPSERT (MERGE) - Like CDC updates!
# ============================================================================

print("\n" + "=" * 70)
print("🔄 DEMO 3: UPSERT (MERGE) - CDC-style updates")
print("=" * 70)

# Create target table
target_data = spark.range(0, 10) \
    .withColumn("value", col("id") * 10)
target_data.write.format("delta").mode("overwrite").save("data/demo/merge_target")

print("✅ Target table (10 rows):")
target_data.show()

# Create source table with updates and inserts
source_data = spark.createDataFrame([
    (5, 55),   # Update: id 5, new value 55
    (10, 100), # Insert: new id 10
    (11, 110), # Insert: new id 11
], ["id", "value"])

print("\n📦 Source data (2 updates, 2 inserts):")
source_data.show()

# Perform MERGE (UPSERT) operation
from delta.tables import DeltaTable

delta_target = DeltaTable.forPath(spark, "data/demo/merge_target")

delta_target.alias("target") \
    .merge(
        source_data.alias("source"),
        "target.id = source.id"
    ) \
    .whenMatchedUpdate(set={
        "value": "source.value"
    }) \
    .whenNotMatchedInsert(values={
        "id": "source.id",
        "value": "source.value"
    }) \
    .execute()

print("\n✅ After MERGE operation:")
result = spark.read.format("delta").load("data/demo/merge_target")
result.orderBy("id").show()

print("🎉 Notice: id 5 updated, ids 10 and 11 inserted!")

# ============================================================================
# CREATE SILVER LAYER (Cleaned, validated data)
# ============================================================================

print("\n" + "=" * 70)
print("✨ CREATING SILVER LAYER (Cleaned data)")
print("=" * 70)

# Clean customers - remove inactive, standardize emails
silver_customers = bronze_customers \
    .filter("is_active = true") \
    .withColumn("email", col("email").cast("string"))

silver_customers.write \
    .format("delta") \
    .mode("overwrite") \
    .save("data/silver/customers")

print(f"✅ Silver customers: {silver_customers.count():,} rows (active only)")

# ============================================================================
# CREATE GOLD LAYER (Aggregated, business-ready)
# ============================================================================

print("\n" + "=" * 70)
print("🏆 CREATING GOLD LAYER (Business aggregates)")
print("=" * 70)

# Join orders with order_items to get total per order
silver_orders = spark.read.format("delta").load("data/bronze/orders_delta")
silver_items = spark.read.format("delta").load("data/bronze/order_items_delta")


# Import aggregation functions (if not already imported)
from pyspark.sql.functions import sum, countDistinct

# Calculate daily sales
daily_sales = silver_items \
    .join(silver_orders, "order_id") \
    .groupBy("order_date") \
    .agg(
        sum("line_total").alias("total_revenue"),
        countDistinct("order_id").alias("order_count"),
        sum("quantity").alias("total_items")
    ) \
    .orderBy("order_date")

daily_sales.write \
    .format("delta") \
    .mode("overwrite") \
    .save("data/gold/daily_sales")

print("✅ Gold daily sales created")
daily_sales.show(10)

# ============================================================================
# SUMMARY
# ============================================================================

print("\n" + "=" * 70)
print("🎉 DELTA LAKE BASICS COMPLETE!")
print("=" * 70)
print("""
✅ Bronze Layer: Raw Delta tables with ACID
✅ ACID Transactions: All-or-nothing guarantees
✅ Time Travel: Query historical versions
✅ UPSERT/MERGE: CDC-style updates
✅ Silver Layer: Cleaned, validated data
✅ Gold Layer: Business-ready aggregates

Next: Run 02_medallion_architecture.py for full Lakehouse!
""")

spark.stop()
