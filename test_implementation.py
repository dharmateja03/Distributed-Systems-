#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Test script for Apache Spark Stream Processing Enhancement
"""

import os
import time
import shutil
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, TimestampType, StringType, DoubleType, LongType

# Get event schema
def get_event_schema():
    """Return schema for financial market events"""
    return StructType([
        StructField("event_time", TimestampType(), False),
        StructField("symbol", StringType(), False),
        StructField("price", DoubleType(), False),
        StructField("volume", LongType(), False),
        StructField("user_id", StringType(), False),
        StructField("trade_type", StringType(), False)
    ])

def generate_test_data(spark, num_batches=5, events_per_batch=100):
    """Generate test data for streaming simulation"""
    print("Generating test data for streaming simulation...")
    
    schema = get_event_schema()
    
    # Create test directory
    test_dir = "test_data"
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)
    os.makedirs(test_dir)
    
    # Symbols and trade types for random data
    symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]
    trade_types = ["BUY", "SELL"]
    
    # Generate data in batches to simulate streaming
    for batch in range(num_batches):
        import random
        import datetime
        
        data = []
        base_time = datetime.datetime.now() - datetime.timedelta(minutes=num_batches - batch)
        
        for i in range(events_per_batch):
            # Generate random event
            event_time = base_time + datetime.timedelta(seconds=random.randint(0, 59))
            
            # Add late events
            if random.random() < 0.1:  # 10% of events are late
                event_time = event_time - datetime.timedelta(minutes=random.randint(1, 5))
            
            symbol = random.choice(symbols)
            price = round(random.uniform(100.0, 1000.0), 2)
            volume = random.randint(10, 1000)
            user_id = f"user_{random.randint(1, 20)}"
            trade_type = random.choice(trade_types)
            
            data.append((event_time, symbol, price, volume, user_id, trade_type))
        
        # Create DataFrame
        df = spark.createDataFrame(data, schema)
        
        # Write batch
        df.write.json(f"{test_dir}/batch_{batch}")
        
        print(f"Generated batch {batch} with {events_per_batch} events")
    
    return test_dir

# Simplified version of the AdaptiveWatermarkedWindowAggregation class for testing
class SimpleAdaptiveWindow:
    def __init__(self, spark, input_df, event_time_col="event_time", 
                 window_duration="1 minute", sliding_duration="30 seconds"):
        self.spark = spark
        self.input_df = input_df
        self.event_time_col = event_time_col
        self.window_duration = window_duration
        self.sliding_duration = sliding_duration
        
    def process(self):
        """Process data with adaptive watermarked windows"""
        # Add watermark for handling late data
        df_with_watermark = self.input_df.withWatermark(self.event_time_col, "5 minutes")
        
        # Create window aggregations
        result = (df_with_watermark
            .groupBy(F.window(F.col(self.event_time_col), self.window_duration, self.sliding_duration))
            .agg(
                F.count("*").alias("event_count"),
                F.sum("volume").alias("total_volume"),
                F.avg("price").alias("avg_price"),
                F.max("price").alias("max_price"),
                F.min("price").alias("min_price")
            )
            .withColumn("watermark_delay", F.lit("5 minutes"))
            .withColumn("window_start", F.col("window.start"))
            .withColumn("window_end", F.col("window.end"))
            .drop("window")
        )
        
        return result

def test_streaming_query():
    """Test a simple streaming query"""
    print("\n=== Testing Simple Streaming Query ===")
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName("StreamingTest") \
        .config("spark.sql.shuffle.partitions", "4") \
        .master("local[*]") \
        .getOrCreate()
    
    # Generate test data
    test_dir = generate_test_data(spark)
    
    # Create a streaming DataFrame
    streaming_df = spark.readStream \
        .schema(get_event_schema()) \
        .option("maxFilesPerTrigger", "1") \
        .json(test_dir)
    
    # Process with a simple window aggregation
    window_agg = SimpleAdaptiveWindow(
        spark, streaming_df,
        window_duration="1 minute",
        sliding_duration="30 seconds"
    )
    result_df = window_agg.process()
    
    # Set up checkpointing
    checkpoint_dir = "test_checkpoint"
    if os.path.exists(checkpoint_dir):
        shutil.rmtree(checkpoint_dir)
    os.makedirs(checkpoint_dir)
    
    # Start a streaming query to the console
    print("Starting streaming query...")
    query = result_df.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .option("checkpointLocation", checkpoint_dir) \
        .start()
    
    # Let the query run for a short time
    print("Query running. Waiting to process data...")
    query.awaitTermination(timeout=20)  # Wait for 20 seconds
    
    # Stop the query
    query.stop()
    print("Query stopped")
    
    print("✓ Streaming query test completed")
    
    # Clean up
    spark.stop()

if __name__ == "__main__":
    print("Starting tests for Apache Spark Stream Processing Enhancement...")
    
    # Run a simplified test first
    try:
        test_streaming_query()
        print("\nTest completed successfully.")
    except Exception as e:
        print(f"\nTest failed with exception: {e}")
        import traceback
        traceback.print_exc()
    
    # Clean up test data
    print("Cleaning up test files...")
    if os.path.exists("test_data"):
        shutil.rmtree("test_data")
    if os.path.exists("test_checkpoint"):
        shutil.rmtree("test_checkpoint")