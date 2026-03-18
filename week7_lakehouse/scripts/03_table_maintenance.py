#!/usr/bin/env python3
"""
Delta Lake Table Maintenance - OPTIMIZE, VACUUM, ZORDER
COMPLETE FIXED VERSION - All errors resolved
"""

from pyspark.sql import SparkSession
from delta import *
from delta.tables import DeltaTable
import os
import shutil

builder = SparkSession.builder \
    .appName("DeltaMaintenance") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("✅ Spark with Delta Lake created")

# ============================================================================
# LIST AVAILABLE TABLES
# ============================================================================

print("\n" + "=" * 70)
print("📂 CHECKING AVAILABLE TABLES")
print("=" * 70)

silver_path = "data/silver"
if os.path.exists(silver_path):
    tables = [f for f in os.listdir(silver_path) if os.path.isdir(os.path.join(silver_path, f))]
    print(f"\n📋 Silver tables found: {tables}")
else:
    print("❌ Silver directory not found!")

gold_path = "data/gold"
if os.path.exists(gold_path):
    tables = [f for f in os.listdir(gold_path) if os.path.isdir(os.path.join(gold_path, f))]
    print(f"\n📋 Gold tables found: {tables}")
else:
    print("❌ Gold directory not found!")

# ============================================================================
# 1. CHECK TABLE HISTORY
# ============================================================================

print("\n" + "=" * 70)
print("📜 TABLE HISTORY - Silver Orders")
print("=" * 70)

table_path = "data/silver/orders"

try:
    delta_table = DeltaTable.forPath(spark, table_path)
    history = delta_table.history().select("version", "timestamp", "operation", "operationMetrics")
    history.show(truncate=False)
    print(f"✅ Found table at {table_path}")
except Exception as e:
    print(f"❌ Table not found at {table_path}: {e}")

# ============================================================================
# 2. CHECK ALL TABLES
# ============================================================================

print("\n" + "=" * 70)
print("📊 CHECKING OTHER TABLES")
print("=" * 70)

possible_paths = [
    "data/silver/customers",
    "data/silver/orders", 
    "data/silver/products",
    "data/silver/order_items",
    "data/gold/customer_360",
    "data/gold/daily_sales",
    "data/gold/product_performance"
]

for path in possible_paths:
    try:
        # Use DeltaTable.forPath to check existence
        delta_table = DeltaTable.forPath(spark, path)
        count = spark.read.format("delta").load(path).count()
        print(f"✅ {path}: {count:,} rows")
    except Exception as e:
        print(f"❌ {path}: Not found")

# ============================================================================
# 3. OPTIMIZE GOLD TABLES
# ============================================================================

print("\n" + "=" * 70)
print("⚡ OPTIMIZE - Compacting small files")
print("=" * 70)

# Optimize customer_360
gold_path = "data/gold/customer_360"
try:
    delta_gold = DeltaTable.forPath(spark, gold_path)
    
    # Get row count before optimization
    df = spark.read.format("delta").load(gold_path)
    print(f"\n📊 Before OPTIMIZE - {gold_path}:")
    print(f"   Rows: {df.count():,}")
    
    # Run OPTIMIZE
    print(f"\n🔄 Running OPTIMIZE on {gold_path}...")
    optimize_result = delta_gold.optimize().executeCompaction()
    optimize_result.show(truncate=False)
    
    print(f"✅ OPTIMIZE completed for {gold_path}")
    
except Exception as e:
    print(f"❌ Could not optimize {gold_path}: {e}")

# Optimize daily_sales
gold_path = "data/gold/daily_sales"
try:
    delta_gold = DeltaTable.forPath(spark, gold_path)
    
    print(f"\n🔄 Running OPTIMIZE on {gold_path}...")
    optimize_result = delta_gold.optimize().executeCompaction()
    optimize_result.show(truncate=False)
    print(f"✅ OPTIMIZE completed for {gold_path}")
    
except Exception as e:
    print(f"❌ Could not optimize {gold_path}: {e}")

# ============================================================================
# 4. VACUUM DEMO - Understanding safety
# ============================================================================

print("\n" + "=" * 70)
print("🧹 VACUUM DEMO - Understanding Delta Lake safety")
print("=" * 70)

# Create a test table for vacuum demo
test_path = "data/demo/vacuum_test"

# Create test data with multiple versions
print("\n📝 Creating test table with multiple versions...")
for i in range(3):
    data = [(i, f"test_{i}") for i in range(10)]
    df = spark.createDataFrame(data, ["id", "value"])
    if i == 0:
        df.write.format("delta").mode("overwrite").save(test_path)
        print(f"   Version {i} created (overwrite)")
    else:
        df.write.format("delta").mode("append").save(test_path)
        print(f"   Version {i} created (append)")

# Show history
delta_test = DeltaTable.forPath(spark, test_path)
print("\n📜 Test table history:")
delta_test.history().select("version", "operation").show(5)

# Show file details
print("\n📊 Test table details:")
details = delta_test.detail().select("numFiles", "sizeInBytes")
details.show()

# Demonstrate safe vacuum
print("\n✅ SAFE VACUUM (168 hours retention):")
print("   delta_test.vacuum(retentionHours=168)")
print("   This would delete files older than 7 days")

# Demonstrate why short retention is blocked
print("\n🔒 UNSAFE VACUUM (0 hours) is BLOCKED by Delta Lake")
print("   This protects you from accidentally deleting data needed for time travel")
print("   Error: 'DELTA_VACUUM_RETENTION_PERIOD_TOO_SHORT'")

# Show the proper way
print("\n📋 CORRECT APPROACH:")
print("   1. Always use retentionHours >= 168")
print("   2. Always dry run first: delta_test.vacuum()")
print("   3. Schedule vacuum after OPTIMIZE")
print("   4. Keep 7 days of history for time travel")

# Clean up test table
print("\n🧹 Cleaning up test table...")
shutil.rmtree(test_path, ignore_errors=True)
print("✅ Test table removed")

# ============================================================================
# 5. ZORDER DEMO
# ============================================================================

print("\n" + "=" * 70)
print("📊 ZORDER - Optimize query performance")
print("=" * 70)

# Try ZORDER on a table
gold_path = "data/gold/customer_360"
try:
    delta_gold = DeltaTable.forPath(spark, gold_path)
    
    print(f"\n🔄 Running ZORDER on customer_id column...")
    optimize_zorder = delta_gold.optimize() \
        .executeZOrderBy("customer_id")
    optimize_zorder.show(truncate=False)
    print(f"✅ ZORDER completed for {gold_path}")
    
except Exception as e:
    print(f"ℹ️ ZORDER demo skipped: {e}")

# ============================================================================
# 6. TABLE PROPERTIES DEMO
# ============================================================================

print("\n" + "=" * 70)
print("⚙️ TABLE PROPERTIES - View and modify settings")
print("=" * 70)

gold_path = "data/gold/customer_360"
try:
    delta_gold = DeltaTable.forPath(spark, gold_path)
    
    # Show current properties
    print("\n📋 Current table properties:")
    props = delta_gold.detail().select("properties").collect()[0][0]
    for key, value in props.items():
        print(f"   {key}: {value}")
    
    # Show how to set properties
    print("\n📝 To set table properties, use:")
    print('   spark.sql("ALTER TABLE delta.`{}` SET TBLPROPERTIES (...)")'.format(gold_path))
    
except Exception as e:
    print(f"ℹ️ Properties demo skipped: {e}")

# ============================================================================
# 7. MAINTENANCE RECOMMENDATIONS
# ============================================================================

print("\n" + "=" * 70)
print("📋 MAINTENANCE BEST PRACTICES")
print("=" * 70)

print("""
✅ RECOMMENDED MAINTENANCE SCHEDULE:
────────────────────────────────────
• OPTIMIZE: Run daily or weekly (compacts small files)
• VACUUM: Run weekly (clean up old files, keep 7 days history)
• ZORDER: Run after large data loads (optimizes queries)
• ANALYZE: Update statistics after OPTIMIZE

✅ SAFE VACUUM COMMANDS:
────────────────────────────────────
from delta.tables import DeltaTable

# Dry run (see what would be deleted)
table = DeltaTable.forPath(spark, "data/gold/customer_360")
table.vacuum()  # Without parameter = dry run

# Actual vacuum (7 days retention - SAFE)
table.vacuum(retentionHours=168)  # 7 days retention

# NEVER use retention < 168 hours in production!

✅ ZORDER BEST PRACTICES:
────────────────────────────────────
• Use on columns frequently used in WHERE clauses
• Run after significant data changes
• customer_id, order_date, category are good candidates

✅ MONITORING METRICS:
────────────────────────────────────
• numFiles: Should decrease after OPTIMIZE
• sizeInBytes: Total table size
• File sizes: Aim for 256MB - 1GB per file
• Version count: Keep 30-60 days of history
""")

spark.stop()
print("\n✅ Maintenance script complete!")
