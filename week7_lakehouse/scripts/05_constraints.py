#!/usr/bin/env python3
"""
Delta Lake Constraints - Add data quality rules
FIXED: Using metastore tables instead of path-based tables
"""

from pyspark.sql import SparkSession
from delta import *
from delta.tables import DeltaTable
from pyspark.sql.functions import col

builder = SparkSession.builder \
    .appName("DeltaConstraints") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("✅ Spark with Delta Lake created")

# ============================================================================
# CREATE DATABASE AND TABLE IN METASTORE
# ============================================================================

print("\n" + "=" * 70)
print("🔒 CREATING TABLE WITH CONSTRAINTS")
print("=" * 70)

# Create a database in the metastore
spark.sql("CREATE DATABASE IF NOT EXISTS demo_db")
spark.sql("USE demo_db")
print("✅ Using database: demo_db")

# Create sample data
data = [
    (1, "John", 25, "john@email.com"),
    (2, "Jane", 30, "jane@email.com"),
    (3, "Bob", 35, "bob@email.com")
]
df = spark.createDataFrame(data, ["id", "name", "age", "email"])

# Write as Delta table (registered in metastore)
df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("constraints_demo")

print("✅ Table 'constraints_demo' created in metastore")

# ============================================================================
# ADD CONSTRAINTS USING SQL (now works with metastore tables)
# ============================================================================

print("\n" + "=" * 70)
print("➕ ADDING CONSTRAINTS")
print("=" * 70)

# Add NOT NULL constraint
print("\n📌 Adding NOT NULL constraint on 'name'...")
try:
    spark.sql("""
        ALTER TABLE constraints_demo 
        ALTER COLUMN name SET NOT NULL
    """)
    print("✅ NOT NULL constraint added")
except Exception as e:
    print(f"❌ Failed: {e}")

# Add CHECK constraint
print("\n📌 Adding CHECK constraint (age >= 0)...")
try:
    spark.sql("""
        ALTER TABLE constraints_demo
        ADD CONSTRAINT age_positive CHECK (age >= 0)
    """)
    print("✅ CHECK constraint added")
except Exception as e:
    print(f"❌ Failed: {e}")

# Show table properties (which include constraints)
print("\n📋 Current table properties:")
props = spark.sql("SHOW TBLPROPERTIES constraints_demo") \
    .filter("key like '%constraint%'")
props.show(truncate=False)

# ============================================================================
# TEST CONSTRAINTS - Valid data
# ============================================================================

print("\n" + "=" * 70)
print("✅ TESTING VALID DATA")
print("=" * 70)

# This should work
valid_data = [(4, "Alice", 28, "alice@email.com")]
df_valid = spark.createDataFrame(valid_data, ["id", "name", "age", "email"])
df_valid.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("constraints_demo")

print("✅ Valid data inserted successfully")
spark.table("constraints_demo").show()

# ============================================================================
# TEST CONSTRAINTS - Invalid data (should fail)
# ============================================================================

print("\n" + "=" * 70)
print("❌ TESTING INVALID DATA")
print("=" * 70)

# Try to insert NULL name (violates NOT NULL)
try:
    invalid_data1 = [(5, None, 32, "null@email.com")]
    df_invalid1 = spark.createDataFrame(invalid_data1, ["id", "name", "age", "email"])
    df_invalid1.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("constraints_demo")
    print("❌ This should have failed!")
except Exception as e:
    print("✅ NOT NULL constraint worked!")
    print(f"   Error: {str(e)[:100]}...")

# Try to insert negative age (violates CHECK)
try:
    invalid_data2 = [(6, "Charlie", -5, "charlie@email.com")]
    df_invalid2 = spark.createDataFrame(invalid_data2, ["id", "name", "age", "email"])
    df_invalid2.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("constraints_demo")
    print("❌ This should have failed!")
except Exception as e:
    print("✅ CHECK constraint worked!")
    print(f"   Error: {str(e)[:100]}...")

# ============================================================================
# VIEW ALL CONSTRAINTS
# ============================================================================

print("\n" + "=" * 70)
print("📋 ALL CONSTRAINTS")
print("=" * 70)

# Show all table properties
print("\n📊 Table properties:")
props = spark.sql("SHOW TBLPROPERTIES constraints_demo")
props.show(50, truncate=False)

# Show Delta table history
print("\n📜 Table history:")
delta_table = DeltaTable.forName(spark, "constraints_demo")
delta_table.history().select("version", "timestamp", "operation").show()

# Final data
print("\n📊 Final data:")
spark.table("constraints_demo").show()

# ============================================================================
# ALTERNATIVE: Path-based tables with constraints via options
# ============================================================================

print("\n" + "=" * 70)
print("🔄 ALTERNATIVE: Path-based tables")
print("=" * 70)

print("""
Path-based tables don't support ALTER TABLE commands.
For path-based tables, use schema validation during write:

# Option 1: Use .option("mergeSchema", "false") to prevent schema changes
df.write.option("mergeSchema", "false").format("delta").save(path)

# Option 2: Use DataFrame transformations to validate data before write
df.filter(col("age") >= 0).filter(col("name").isNotNull()).write...

# Option 3: Create a view and use CHECK constraints in queries
df.createOrReplaceTempView("temp_view")
spark.sql("SELECT * FROM temp_view WHERE age >= 0 AND name IS NOT NULL")
""")

# ============================================================================
# CLEAN UP
# ============================================================================

print("\n" + "=" * 70)
print("🧹 CLEAN UP")
print("=" * 70)

print("\n📝 To clean up the demo table, run:")
print("   spark.sql(\"DROP TABLE IF EXISTS demo_db.constraints_demo\")")
print("   spark.sql(\"DROP DATABASE IF EXISTS demo_db CASCADE\")")

# ============================================================================
# CONSTRAINTS BEST PRACTICES
# ============================================================================

print("\n" + "=" * 70)
print("📋 CONSTRAINTS BEST PRACTICES")
print("=" * 70)

print("""
✅ DELTA LAKE CONSTRAINTS:
────────────────────────────────────
• NOT NULL: ALTER TABLE ... ALTER COLUMN ... SET NOT NULL
• CHECK: ALTER TABLE ... ADD CONSTRAINT ... CHECK (...)
• UNIQUE: Not directly supported (use pre-processing)

✅ REQUIREMENTS:
────────────────────────────────────
• Table must be registered in metastore (not path-based)
• Use saveAsTable() instead of save(path)
• Create a database first: CREATE DATABASE IF NOT EXISTS db_name

✅ EXAMPLES:
────────────────────────────────────
# Create table in metastore
df.write.format("delta").saveAsTable("my_table")

# Add constraints
ALTER TABLE my_table ALTER COLUMN name SET NOT NULL
ALTER TABLE my_table ADD CONSTRAINT age_check CHECK (age >= 0)

✅ PATH-BASED TABLES ALTERNATIVE:
────────────────────────────────────
Use DataFrame transformations for validation:
df.filter(col("age") >= 0).filter(col("name").isNotNull()).write...
""")

spark.stop()
print("\n✅ Constraints demo complete!")
