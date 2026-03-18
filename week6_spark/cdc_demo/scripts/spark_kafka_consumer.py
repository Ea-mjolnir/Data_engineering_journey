#!/usr/bin/env python3
"""
Spark Streaming Consumer for Kafka CDC Messages
Reads real-time changes from PostgreSQL via Debezium and Kafka
FULL VERSION with Kafka connector JARs
"""

import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# ============================================================================
# CONFIGURATION
# ============================================================================

KAFKA_BOOTSTRAP_SERVERS = "localhost:9093"  # Note: using 9093 (mapped port)
KAFKA_TOPIC = "dbserver1.public.orders"

# PostgreSQL target configuration (for writing results)
PG_CONFIG = {
    "url": "jdbc:postgresql://localhost:5434/orderdb",  # Note: using 5434
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

# Path to PostgreSQL JDBC driver
BASE_DIR = "/home/odinsbeard/Data_engineering_Journey/week6_spark"
JAR_PATH = f"{BASE_DIR}/jars/postgresql-42.7.1.jar"
CHECKPOINT_DIR = f"{BASE_DIR}/cdc_demo/checkpoints"

os.makedirs(CHECKPOINT_DIR, exist_ok=True)

# ============================================================================
# INITIALIZE SPARK SESSION WITH KAFKA JARS
# ============================================================================

# Path to Kafka connector JARs
KAFKA_JARS = [
    f"{BASE_DIR}/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar",
    f"{BASE_DIR}/jars/kafka-clients-3.5.0.jar",
    f"{BASE_DIR}/jars/commons-pool2-2.11.1.jar",
    f"{BASE_DIR}/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar"
]

# Combine all JARs (PostgreSQL + Kafka)
all_jars = [JAR_PATH] + KAFKA_JARS
jars_list = ",".join(all_jars)

print(f"📦 Loading JARs: {jars_list}")

spark = SparkSession.builder \
    .appName("Spark Kafka CDC Consumer") \
    .config("spark.jars", jars_list) \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("✅ Spark session created with Kafka connector")

# ============================================================================
# READ FROM KAFKA STREAM
# ============================================================================
print(f"\n📡 Connecting to Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
print(f"📡 Subscribing to topic: {KAFKA_TOPIC}")

# Read stream from Kafka
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

print("✅ Kafka stream created")

# ============================================================================
# PARSE KAFKA MESSAGES (CDC JSON)
# ============================================================================

# Define schema for CDC message payload
# This matches the Debezium CDC message structure
cdc_schema = StructType([
    StructField("op", StringType()),                    # c=insert, u=update, d=delete
    StructField("before", StringType()),                # JSON string of before state
    StructField("after", StringType()),                 # JSON string of after state
    StructField("source", StringType()),                # Source metadata
    StructField("ts_ms", LongType())                    # Timestamp
])

# Parse the Kafka value (which is the CDC message)
# The value is in JSON format, stored as bytes
parsed_stream = kafka_stream \
    .selectExpr("CAST(value AS STRING) as json_value") \
    .select(from_json("json_value", cdc_schema).alias("data")) \
    .select("data.*")

# Parse the 'after' field which contains the actual order data
order_schema = StructType([
    StructField("id", IntegerType()),
    StructField("customer_name", StringType()),
    StructField("product", StringType()),
    StructField("quantity", IntegerType()),
    StructField("price", DoubleType()),
    StructField("order_date", StringType())
])

# Extract order data from the 'after' field
# Only care about inserts and updates (c and u)
orders_stream = parsed_stream \
    .filter(col("op").isin(["c", "u"])) \
    .select(
        col("op").alias("operation"),
        from_json(col("after"), order_schema).alias("order_data")
    ) \
    .select(
        "operation",
        "order_data.*"
    )

# ============================================================================
# PROCESSING FUNCTIONS
# ============================================================================

def process_batch(df, epoch_id):
    """Process each micro-batch of CDC messages"""
    count = df.count()
    
    if count == 0:
        print(f"\n⏳ Batch {epoch_id}: No new messages")
        return
    
    print(f"\n📦 Processing batch {epoch_id} with {count} messages")
    
    # Show the data
    print("\n📊 CDC Events in this batch:")
    df.show(truncate=False)
    
    # Separate operations
    inserts = df.filter(col("operation") == "c")
    updates = df.filter(col("operation") == "u")
    
    if inserts.count() > 0:
        print(f"\n✅ {inserts.count()} INSERT operations:")
        inserts.show(truncate=False)
        
        # Write inserts to PostgreSQL
        try:
            inserts.write \
                .mode("append") \
                .format("jdbc") \
                .option("url", PG_CONFIG["url"]) \
                .option("dbtable", "orders_replica") \
                .option("user", PG_CONFIG["user"]) \
                .option("password", PG_CONFIG["password"]) \
                .option("driver", PG_CONFIG["driver"]) \
                .save()
            print(f"💾 {inserts.count()} inserts saved to PostgreSQL table 'orders_replica'")
        except Exception as e:
            print(f"❌ Error writing to PostgreSQL: {e}")
            # Create table if it doesn't exist
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS orders_replica (
                id INTEGER PRIMARY KEY,
                customer_name VARCHAR(100),
                product VARCHAR(100),
                quantity INTEGER,
                price DECIMAL(10,2),
                order_date TIMESTAMP
            )
            """
            # Note: You'd need a separate connection to create the table
            print("💡 Table 'orders_replica' may not exist - create it manually in PostgreSQL")
    
    if updates.count() > 0:
        print(f"\n🔄 {updates.count()} UPDATE operations:")
        updates.show(truncate=False)
        # For updates, you'd typically upsert or handle differently

# ============================================================================
# START STREAMING QUERY
# ============================================================================
print("\n" + "=" * 70)
print("🚀 STARTING STREAMING PROCESSING")
print("=" * 70)

query = orders_stream.writeStream \
    .foreachBatch(process_batch) \
    .trigger(processingTime="10 seconds") \
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/orders_processor") \
    .start()

print("\n✅ Streaming query started")
print("📡 Listening for CDC messages...")
print("⏱️  Processing batches every 10 seconds")
print("\nPress Ctrl+C to stop\n")

# Keep the stream running
try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\n\n🛑 Stopping stream...")
    query.stop()
    print("✅ Stream stopped")
finally:
    spark.stop()
    print("✅ Spark session stopped")
