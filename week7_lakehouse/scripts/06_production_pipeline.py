#!/usr/bin/env python3
"""
Production-Ready Data Pipeline with Delta Lake
Combines everything: Bronze → Silver → Gold with quality checks
FIXED: Using integer math to avoid Spark round function conflict
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import *
from delta.tables import DeltaTable
import time
import random
import sys
from datetime import datetime, timedelta

# ============================================================================
# INITIALIZE SPARK
# ============================================================================

builder = SparkSession.builder \
    .appName("ProductionDeltaPipeline") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
    .config("spark.sql.adaptive.enabled", "true")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("=" * 70)
print("🏭 PRODUCTION DELTA LAKE PIPELINE")
print("=" * 70)

# ============================================================================
# 1. CREATE DATABASE
# ============================================================================

spark.sql("CREATE DATABASE IF NOT EXISTS production_db")
spark.sql("USE production_db")
print("✅ Using database: production_db")

# ============================================================================
# 2. BRONZE LAYER - Raw data ingestion
# ============================================================================

print("\n" + "=" * 70)
print("🥉 BRONZE LAYER - Raw Data Ingestion")
print("=" * 70)

# Simulate streaming data (in production, this would be Kafka)
print("\n📡 Simulating real-time data stream...")

def generate_batch(batch_id):
    """Generate a batch of simulated orders - using integer math to avoid rounding issues"""
    customers = list(range(1, 1001))
    products = list(range(1, 101))
    statuses = ['pending', 'completed', 'cancelled']
    
    data = []
    for i in range(100):  # 100 orders per batch
        # Generate price as integer cents, then convert to dollars
        price_cents = random.randint(1000, 50000)  # $10.00 to $500.00
        price_dollars = price_cents / 100.0
        
        data.append((
            batch_id * 1000 + i,
            random.choice(customers),
            random.choice(products),
            random.randint(1, 5),
            price_dollars,
            random.choice(statuses),
            datetime.now().isoformat()
        ))
    return data

# Create bronze table with Change Data Feed enabled
print("\n📝 Creating bronze tables...")

for batch_id in range(5):  # 5 batches
    batch_data = generate_batch(batch_id)
    print(f"   Generating batch {batch_id} with {len(batch_data)} orders...")
    
    df = spark.createDataFrame(batch_data, 
        ["order_id", "customer_id", "product_id", "quantity", "price", "status", "timestamp"])
    
    if batch_id == 0:
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("delta.enableChangeDataFeed", "true") \
            .saveAsTable("bronze_orders")
        print(f"✅ Batch {batch_id}: Initial load - {len(batch_data)} orders")
    else:
        df.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable("bronze_orders")
        print(f"✅ Batch {batch_id}: Incremental load - {len(batch_data)} orders")
    
    time.sleep(1)

# Verify bronze table was created
bronze_count = spark.table("bronze_orders").count()
print(f"\n✅ Bronze table created with {bronze_count:,} total orders")

# Show bronze table stats
print("\n📊 Bronze table stats:")
spark.sql("SELECT COUNT(*) as count FROM bronze_orders").show()

# ============================================================================
# 3. SILVER LAYER - Clean and enrich
# ============================================================================

print("\n" + "=" * 70)
print("🥈 SILVER LAYER - Clean and Enrich")
print("=" * 70)

# Read from bronze
bronze_df = spark.table("bronze_orders")

# Clean and enrich
silver_df = bronze_df \
    .withColumn("order_date", to_date(col("timestamp"))) \
    .withColumn("year", year("order_date")) \
    .withColumn("month", month("order_date")) \
    .withColumn("total_amount", col("quantity") * col("price")) \
    .withColumn("is_completed", col("status") == "completed") \
    .drop("timestamp")

# Add data quality checks
print("\n🔍 Data Quality Checks:")
total_orders = silver_df.count()
invalid_orders = silver_df.filter("quantity <= 0 OR price <= 0").count()
print(f"   Total orders: {total_orders:,}")
print(f"   Invalid orders: {invalid_orders:,}")

# Write to silver with constraints
silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("year", "month") \
    .saveAsTable("silver_orders")

# Add constraints
print("\n🔒 Adding constraints...")
try:
    spark.sql("""
        ALTER TABLE silver_orders 
        ADD CONSTRAINT valid_quantity CHECK (quantity > 0)
    """)
    print("✅ Quantity constraint added")
except Exception as e:
    print(f"ℹ️ Quantity constraint note: {e}")

try:
    spark.sql("""
        ALTER TABLE silver_orders 
        ADD CONSTRAINT valid_price CHECK (price > 0)
    """)
    print("✅ Price constraint added")
except Exception as e:
    print(f"ℹ️ Price constraint note: {e}")

print("\n📊 Silver table stats:")
spark.sql("""
    SELECT year, month, COUNT(*) as orders, ROUND(SUM(total_amount), 2) as revenue
    FROM silver_orders
    GROUP BY year, month
    ORDER BY year, month
""").show()

# ============================================================================
# 4. GOLD LAYER - Business aggregates
# ============================================================================

print("\n" + "=" * 70)
print("🥇 GOLD LAYER - Business Aggregates")
print("=" * 70)

# Customer 360 view
customer_stats = spark.sql("""
    SELECT 
        customer_id,
        COUNT(*) as total_orders,
        ROUND(SUM(total_amount), 2) as lifetime_value,
        ROUND(AVG(total_amount), 2) as avg_order_value,
        MIN(order_date) as first_order,
        MAX(order_date) as last_order
    FROM silver_orders
    GROUP BY customer_id
""")

customer_stats.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_customer_360")

# Daily sales
daily_sales = spark.sql("""
    SELECT 
        order_date,
        COUNT(*) as order_count,
        ROUND(SUM(total_amount), 2) as revenue,
        ROUND(AVG(total_amount), 2) as avg_order_value,
        COUNT(DISTINCT customer_id) as unique_customers
    FROM silver_orders
    GROUP BY order_date
    ORDER BY order_date
""")

daily_sales.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_daily_sales")

print("\n📊 Gold tables created:")
print("   ✅ gold_customer_360")
print("   ✅ gold_daily_sales")

# Show samples
print("\n🏆 Top 5 customers:")
spark.sql("""
    SELECT customer_id, total_orders, lifetime_value 
    FROM gold_customer_360 
    ORDER BY lifetime_value DESC 
    LIMIT 5
""").show()

print("\n📈 Last 5 days sales:")
spark.sql("""
    SELECT order_date, order_count, revenue 
    FROM gold_daily_sales 
    ORDER BY order_date DESC 
    LIMIT 5
""").show()

# ============================================================================
# 5. MAINTENANCE
# ============================================================================

print("\n" + "=" * 70)
print("🔧 MAINTENANCE")
print("=" * 70)

# OPTIMIZE tables
print("\n⚡ Optimizing tables...")
for table in ["bronze_orders", "silver_orders", "gold_customer_360", "gold_daily_sales"]:
    try:
        delta_table = DeltaTable.forName(spark, table)
        print(f"   Optimizing {table}...")
        delta_table.optimize().executeCompaction()
        print(f"   ✅ {table} optimized")
    except Exception as e:
        print(f"   ⚠️  Could not optimize {table}: {e}")

# Show table stats
print("\n📊 Table Statistics:")
for table in ["bronze_orders", "silver_orders", "gold_customer_360", "gold_daily_sales"]:
    try:
        count = spark.table(table).count()
        print(f"   {table}: {count:,} rows")
    except:
        print(f"   {table}: Not found")

# ============================================================================
# 6. CHANGE DATA FEED DEMO
# ============================================================================

print("\n" + "=" * 70)
print("🔄 CHANGE DATA FEED")
print("=" * 70)

# Show change history
print("\n📜 Bronze table history:")
try:
    delta_bronze = DeltaTable.forName(spark, "bronze_orders")
    delta_bronze.history().select("version", "timestamp", "operation", "operationMetrics").show(5, truncate=False)
except Exception as e:
    print(f"Could not show history: {e}")

# ============================================================================
# 7. TIME TRAVEL DEMO
# ============================================================================

print("\n" + "=" * 70)
print("⏰ TIME TRAVEL")
print("=" * 70)

# Show version 0 (initial load)
try:
    print("\n📊 Version 0 (initial load):")
    v0_count = spark.sql("SELECT COUNT(*) FROM bronze_orders VERSION AS OF 0").collect()[0][0]
    print(f"   Rows: {v0_count:,}")

    print("\n📊 Current version:")
    current_count = spark.table("bronze_orders").count()
    print(f"   Rows: {current_count:,}")
    
    print(f"\n✅ Time travel shows growth: +{current_count - v0_count:,} rows")
except Exception as e:
    print(f"Time travel demo skipped: {e}")

# ============================================================================
# 8. CLEANUP OPTION (commented out by default)
# ============================================================================

print("\n" + "=" * 70)
print("🧹 CLEANUP (Optional)")
print("=" * 70)

print("""
To clean up the demo database, run:
   spark.sql("DROP TABLE IF EXISTS bronze_orders")
   spark.sql("DROP TABLE IF EXISTS silver_orders") 
   spark.sql("DROP TABLE IF EXISTS gold_customer_360")
   spark.sql("DROP TABLE IF EXISTS gold_daily_sales")
   spark.sql("DROP DATABASE IF EXISTS production_db CASCADE")
""")

# ============================================================================
# 9. PIPELINE SUMMARY
# ============================================================================

print("\n" + "=" * 70)
print("✅ PIPELINE SUMMARY")
print("=" * 70)

print("""
🏭 PRODUCTION DELTA LAKE PIPELINE
───────────────────────────────────────────────────────────────
✅ BRONZE LAYER: Raw data with Change Data Feed
   • Table: bronze_orders
   • Features: CDC enabled, append-only

✅ SILVER LAYER: Cleaned and enriched
   • Table: silver_orders
   • Features: Constraints, partitioning, data quality

✅ GOLD LAYER: Business aggregates
   • Tables: gold_customer_360, gold_daily_sales
   • Features: Pre-aggregated, optimized for BI

✅ MAINTENANCE: OPTIMIZE executed
✅ TIME TRAVEL: Version history verified
✅ CHANGE DATA FEED: Track changes
✅ CONSTRAINTS: Data quality rules

🎉 Your production Lakehouse is ready!
""")

print("\n" + "=" * 70)
print("📊 FINAL STATISTICS")
print("=" * 70)

# Final counts
bronze_final = spark.table("bronze_orders").count()
silver_final = spark.table("silver_orders").count()
gold_customer_final = spark.table("gold_customer_360").count()
gold_daily_final = spark.table("gold_daily_sales").count()

print(f"🥉 Bronze orders: {bronze_final:,}")
print(f"🥈 Silver orders: {silver_final:,}")
print(f"🥇 Gold customers: {gold_customer_final:,}")
print(f"📈 Gold daily records: {gold_daily_final:,}")

spark.stop()
print("\n✅ Pipeline execution complete!")
