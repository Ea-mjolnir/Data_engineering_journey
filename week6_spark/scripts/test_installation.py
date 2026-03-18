#!/usr/bin/env python3
"""
Quick test to verify Spark installation
"""

from pyspark.sql import SparkSession
import sys
import os

def test_spark():
    """Simple test to verify Spark is working"""
    
    print("🚀 Creating Spark session...")
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName("InstallationTest") \
        .master("local[*]") \
        .getOrCreate()
    
    print(f"✅ Spark version: {spark.version}")
    print(f"✅ Python version: {sys.version}")
    print(f"✅ Spark UI: http://localhost:4040")
    
    # Create simple DataFrame
    data = [("Spark", "works!"), ("Installation", "successful")]
    df = spark.createDataFrame(data, ["test", "result"])
    
    # Perform simple operation
    count = df.count()
    print(f"✅ Test DataFrame created with {count} rows")
    
    # Show result
    df.show()
    
    # Stop Spark
    spark.stop()
    print("✅ Spark session stopped")
    return True

if __name__ == "__main__":
    success = test_spark()
    sys.exit(0 if success else 1)
