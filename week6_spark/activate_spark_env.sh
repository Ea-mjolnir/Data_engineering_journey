#!/bin/bash
# Activate Spark environment

# Activate Python virtual environment
source "/home/odinsbeard/Data_engineering_Journey/week6_spark/venv/bin/activate"

# Set Spark environment variables
export SPARK_HOME="/home/odinsbeard/Data_engineering_Journey/week6_spark/spark"
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
export PYSPARK_PYTHON="/home/odinsbeard/Data_engineering_Journey/week6_spark/venv/bin/python"
export PYSPARK_DRIVER_PYTHON="/home/odinsbeard/Data_engineering_Journey/week6_spark/venv/bin/python"
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

echo "✅ Spark environment activated"
echo "   Python: $(which python)"
echo "   Spark: $SPARK_HOME"
