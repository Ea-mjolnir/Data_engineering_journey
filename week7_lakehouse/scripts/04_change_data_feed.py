#!/usr/bin/env python3
"""
Delta Lake Change Data Feed - Track changes between versions
"""

from pyspark.sql import SparkSession
from delta import *
from delta.tables import DeltaTable

builder = SparkSession.builder \
    .appName("DeltaChangeDataFeed") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.changeDataFeed.enabled", "true")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("✅ Spark with Delta Lake created")

# ============================================================================
# CREATE TABLE WITH CHANGE DATA FEED ENABLED
# ============================================================================

print("\n" + "=" * 70)
print("📝 CREATING TABLE WITH CHANGE DATA FEED")
print("=" * 70)

# Create test data
data = [(1, "Product A", 100), (2, "Product B", 200), (3, "Product C", 300)]
df = spark.createDataFrame(data, ["id", "name", "price"])

# Enable change data feed when creating table
df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("delta.enableChangeDataFeed", "true") \
    .save("data/demo/cdf_test")

print("✅ Table created with Change Data Feed enabled")

# ============================================================================
# MAKE CHANGES
# ============================================================================

print("\n" + "=" * 70)
print("🔄 MAKING CHANGES")
print("=" * 70)

delta_table = DeltaTable.forPath(spark, "data/demo/cdf_test")

# Version 0: Initial data (already written)

# Version 1: Update
print("\n📝 Version 1: Update product 1")
delta_table.update(
    condition="id = 1",
    set={"price": "150"}
)

# Version 2: Insert
print("📝 Version 2: Insert new product")
new_data = [(4, "Product D", 400)]
df_new = spark.createDataFrame(new_data, ["id", "name", "price"])
df_new.write \
    .format("delta") \
    .mode("append") \
    .save("data/demo/cdf_test")

# Version 3: Delete
print("📝 Version 3: Delete product 2")
delta_table.delete("id = 2")

# ============================================================================
# READ CHANGE DATA FEED
# ============================================================================

print("\n" + "=" * 70)
print("📊 READING CHANGE DATA FEED")
print("=" * 70)

# Read changes between versions
print("\n🔄 Changes from version 0 to 3:")
changes = spark.read \
    .format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", "0") \
    .option("endingVersion", "3") \
    .load("data/demo/cdf_test")

changes.show(truncate=False)

# Explain change types
print("\n📋 Change Types:")
print("  - insert: new row added")
print("  - update_preimage: old values before update")
print("  - update_postimage: new values after update")
print("  - delete: row deleted")

# Read only latest changes
print("\n🔄 Changes from version 2 to current:")
latest = spark.read \
    .format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", "2") \
    .load("data/demo/cdf_test")

latest.show(truncate=False)

# ============================================================================
# CDC-STYLE PROCESSING
# ============================================================================

print("\n" + "=" * 70)
print("🔄 CDC-STYLE PROCESSING")
print("=" * 70)

# Simulate processing changes incrementally
last_processed_version = 0
current_version = delta_table.history().agg({"version": "max"}).collect()[0][0]

print(f"\n📊 Last processed version: {last_processed_version}")
print(f"📊 Current version: {current_version}")

# Process new changes
new_changes = spark.read \
    .format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", last_processed_version + 1) \
    .load("data/demo/cdf_test")

print(f"\n🔄 Processing {new_changes.count()} new changes:")
new_changes.groupBy("_change_type").count().show()

# Separate by operation type
inserts = new_changes.filter("_change_type = 'insert'")
updates = new_changes.filter("_change_type = 'update_postimage'")
deletes = new_changes.filter("_change_type = 'delete'")

print(f"\n✅ Inserts: {inserts.count()}")
print(f"✅ Updates: {updates.count()}")
print(f"✅ Deletes: {deletes.count()}")

spark.stop()
print("\n✅ Change Data Feed demo complete!")
