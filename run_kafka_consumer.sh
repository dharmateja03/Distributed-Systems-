#!/bin/bash

# Check if required Python packages are installed
if ! pip list | grep -q pyspark; then
    echo "Installing PySpark..."
    pip install pyspark pandas matplotlib
fi

# Run the Kafka consumer with Spark streaming
echo "Starting Kafka consumer with Spark streaming..."

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,org.postgresql:postgresql:42.2.5 kafka_streaming.py
