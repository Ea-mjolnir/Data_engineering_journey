#!/usr/bin/env python3
"""
DAY 1: REAL-WORLD SPARK - Processing ALL Historical Data
FIXED: Correct way to track input files
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, max, min, desc, year, month, dayofmonth, to_date, input_file_name,when
import time
import os
from glob import glob

# ============================================================================
# STEP 1: Create Spark Session - Configured for REAL workloads
# ============================================================================
print("=" * 70)
print("🏭 REAL-WORLD SPARK: PROCESSING ALL HISTORICAL DATA")
print("=" * 70)

spark = SparkSession.builder \
    .appName("RealWorld_Spark") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
    .config("spark.sql.files.maxPartitionBytes", "128MB") \
    .config("spark.sql.files.openCostInBytes", "32MB") \
    .config("spark.ui.port", "4040") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print(f"\n✅ Spark session created")
print(f"   • Version: {spark.version}")
print(f"   • Master: {spark.sparkContext.master}")
print(f"   • Web UI: http://localhost:4040")

# ============================================================================
# STEP 2: REAL-WORLD - Read ALL historical files
# ============================================================================
print("\n" + "=" * 70)
print("📥 STEP 2: READING ALL HISTORICAL FILES")
print("=" * 70)

data_dir = "/home/odinsbeard/Data_engineering_Journey/week6_spark/data/input"

# Find ALL sales files
sales_files = glob(os.path.join(data_dir, "sales_*.csv"))
print(f"\n📁 Found {len(sales_files)} sales files:")

# Show all files with sizes
total_size_mb = 0
for f in sales_files:
    size_mb = os.path.getsize(f) / (1024 * 1024)
    total_size_mb += size_mb
    print(f"   • {os.path.basename(f)} ({size_mb:.1f} MB)")
print(f"\n📊 TOTAL DATA SIZE: {total_size_mb:.1f} MB ({total_size_mb/1024:.1f} GB)")

# REAL-WORLD: Read ALL files at once!
print("\n🚀 Reading ALL files in parallel...")
start_load = time.time()

df_all_sales = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(os.path.join(data_dir, "sales_*.csv"))  # Wildcard = ALL sales files!

end_load = time.time()
print(f"✅ Schema inferred from ALL files ({end_load-start_load:.2f} seconds)")
df_all_sales.printSchema()

# ============================================================================
# STEP 3: REAL-WORLD - Understand Your Data Volume
# ============================================================================
print("\n" + "=" * 70)
print("📊 STEP 3: UNDERSTANDING DATA VOLUME")
print("=" * 70)

# Count total rows across ALL files
print("\n🔍 Counting total rows across ALL files...")
start = time.time()
total_rows = df_all_sales.count()
end = time.time()
print(f"   • TOTAL ROWS: {total_rows:,}")
print(f"   • Time: {end-start:.2f} seconds")

# CORRECT WAY: Add input file name as a column using input_file_name()
print("\n📈 Adding source file information to DataFrame...")
df_with_source = df_all_sales.withColumn("source_file", input_file_name())

# Now show data distribution across files
print("\n📊 Data distribution across files:")
file_stats = df_with_source.groupBy("source_file").count() \
    .withColumn("filename", col("source_file")) \
    .drop("source_file") \
    .orderBy("count")

# Show results nicely
file_stats.show(truncate=False)

# Calculate statistics
print("\n📈 File Statistics:")
total_files = file_stats.count()
min_rows = file_stats.agg({"count": "min"}).collect()[0][0]
max_rows = file_stats.agg({"count": "max"}).collect()[0][0]
avg_rows = file_stats.agg({"count": "avg"}).collect()[0][0]

print(f"   • Total files: {total_files}")
print(f"   • Min rows/file: {min_rows:,}")
print(f"   • Max rows/file: {max_rows:,}")
print(f"   • Avg rows/file: {avg_rows:,.0f}")

# ============================================================================
# STEP 4: REAL-WORLD - Optimize with Partitioning (SIMPLIFIED)
# ============================================================================
print("\n" + "=" * 70)
print("🔧 STEP 4: OPTIMIZING FOR REAL WORKLOADS")
print("=" * 70)

# Get partition count
initial_partitions = df_all_sales.rdd.getNumPartitions()
print(f"\n📊 Initial partitions: {initial_partitions}")

# Get row count directly from Spark (guaranteed to be integer)
row_count = df_all_sales.count()
print(f"   • Total rows: {row_count:,}")
print(f"   • Initial rows/partition: {row_count // initial_partitions:,}")

# Calculate target partitions (aim for 500k rows per partition)
target_partitions = row_count // 500000
if target_partitions < 8:
    target_partitions = 8
    
print(f"\n🔄 Repartitioning to {target_partitions} partitions...")

# Repartition
df_optimized = df_all_sales.repartition(target_partitions)

# Show results
new_parts = df_optimized.rdd.getNumPartitions()
print(f"   • New partitions: {new_parts}")
print(f"   • New rows/partition: {row_count // new_parts:,}")

# ============================================================================
# STEP 5: REAL-WORLD - Complex Analytics on ALL Data
# ============================================================================
print("\n" + "=" * 70)
print("📈 STEP 5: REAL ANALYTICS ON HISTORICAL DATA")
print("=" * 70)

# Query 1: Year-over-Year Growth (REAL business metric!)
print("\n📊 Query 1: Year-over-Year Sales Growth")
start = time.time()

# Parse date and extract year
df_with_date = df_optimized.withColumn("sale_date", to_date(col("sale_date"))) \
                           .withColumn("year", year("sale_date"))

yearly_sales = df_with_date.groupBy("year").agg(
    count("*").alias("total_orders"),
    sum("final_amount").alias("total_revenue"),
    avg("final_amount").alias("avg_order_value")
).orderBy("year")

yearly_sales.show()
end = time.time()
print(f"⏱️  Execution time: {end-start:.2f} seconds")

# Query 2: Top Products by Revenue (with joins to products)
print("\n📊 Query 2: Top 10 Products by Revenue")
start = time.time()

# Read ALL product files
df_products = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(os.path.join(data_dir, "products_*.csv"))

# Join sales with products
product_performance = df_optimized.groupBy("product_id").agg(
    sum("final_amount").alias("revenue"),
    count("*").alias("times_sold")
).join(df_products, "product_id") \
 .select("product_name", "category", "revenue", "times_sold") \
 .orderBy(desc("revenue")) \
 .limit(10)

product_performance.show(truncate=False)
end = time.time()
print(f"⏱️  Execution time: {end-start:.2f} seconds")

# Query 3: Customer Segmentation (RFM Analysis style)
print("\n📊 Query 3: Customer Segmentation")
start = time.time()

# Read ALL user files
df_users = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(os.path.join(data_dir, "users_*.csv"))

# Calculate customer metrics
customer_metrics = df_optimized.groupBy("user_id").agg(
    count("*").alias("purchase_count"),
    sum("final_amount").alias("total_spent"),
    avg("final_amount").alias("avg_purchase")
).join(df_users, "user_id")

# Segment customers
segmented = customer_metrics.withColumn("segment",
    when(col("total_spent") > 1000, "VIP") \
    .when(col("total_spent") > 500, "GOLD") \
    .when(col("total_spent") > 100, "SILVER") \
    .otherwise("BRONZE")
)

segment_summary = segmented.groupBy("segment").agg(
    count("*").alias("customer_count"),
    avg("total_spent").alias("avg_spent"),
    sum("total_spent").alias("total_revenue")
).orderBy(desc("avg_spent"))

segment_summary.show()
end = time.time()
print(f"⏱️  Execution time: {end-start:.2f} seconds")

# ============================================================================
# STEP 6: REAL-WORLD - Export Results
# ============================================================================
print("\n" + "=" * 70)
print("💾 STEP 6: EXPORTING RESULTS")
print("=" * 70)

output_dir = "/home/odinsbeard/Data_engineering_Journey/week6_spark/data/output"
os.makedirs(output_dir, exist_ok=True)

# Save aggregated results (partitioned for efficiency)
print("\n📁 Saving yearly sales report...")
yearly_sales.coalesce(1) \
    .write \
    .option("header", "true") \
    .mode("overwrite") \
    .csv(os.path.join(output_dir, "yearly_sales_report"))

print(f"✅ Results saved to: {output_dir}/yearly_sales_report")

# ============================================================================
# STEP 7: REAL-WORLD - Performance Metrics
# ============================================================================
print("\n" + "=" * 70)
print("📊 STEP 7: PRODUCTION METRICS")
print("=" * 70)

print(f"""
📈 JOB SUMMARY:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
• Total files processed: {len(sales_files)}
• Total rows processed: {total_rows:,}
• Total data size: {total_size_mb/1024:.2f} GB
• Average file size: {total_size_mb/len(sales_files):.1f} MB
• Partitions used: {target_partitions}
• Processing completed: {time.strftime('%Y-%m-%d %H:%M:%S')}

💡 KEY TAKEAWAYS:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
✓ Spark reads ALL files in parallel using wildcards
✓ Use input_file_name() to track source files
✓ 20 million rows processed in seconds!
✓ Partitioning is CRITICAL for performance
✓ Joins work seamlessly across datasets
""")

# ============================================================================
# STEP 8: Clean Up
# ============================================================================
print("\n🧹 Cleaning up...")
spark.stop()
print("✅ Spark session stopped")

print("\n" + "=" * 70)
print("🎉 REAL-WORLD SPARK EXERCISE COMPLETE!")
print(f"✅ Successfully processed {total_rows:,} rows across {len(sales_files)} files")
print("=" * 70)
