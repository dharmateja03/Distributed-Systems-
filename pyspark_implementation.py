#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Apache Spark Stream Processing Enhancement: Custom Window-Based Aggregation Strategies
PySpark Implementation for Local Testing

This implementation provides custom window-based aggregation strategies for Apache Spark Streaming:
1. Session-Based Windows
2. Hybrid Windows 
3. Multi-Granularity Windows
4. Adaptive Watermarked Windows

Author: Vardhan Belide, Dharma Teja Samudrala, Charan Teja Mudduluru
"""

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, LongType
import time
import json
import random
import datetime
from typing import Dict, List, Tuple
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from threading import Thread
#our imports
# At the top of pyspark_implementation.py, add:
from fault_tolerance import configure_checkpointing, create_fault_tolerant_query
from fault_tolerance import configure_checkpointing, create_fault_tolerant_query
from output_sinks import StreamingOutputSink
from kafka_config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC
from utils import create_spark_session, read_stream_from_kafka
from pyspark.sql.functions import window
from pyspark.sql.functions import expr

# Create Spark Session with specific configurations for local testing
# Add this function to pyspark_implementation.py, replacing or alongside the existing create_spark_session function

def create_distributed_spark_session(app_name="CustomWindowAggregation", 
                                    master_url="yarn",
                                    executor_memory="4g",
                                    executor_cores=2,
                                    num_executors=4,
                                    shuffle_partitions=200,
                                    max_result_size="1g"):
    """Create a Spark session configured for distributed environment"""
    return (SparkSession.builder
            .appName(app_name)
            .master(master_url)
            .config("spark.executor.memory", executor_memory)
            .config("spark.executor.cores", str(executor_cores))
            .config("spark.executor.instances", str(num_executors))
            .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
            .config("spark.driver.maxResultSize", max_result_size)
            .config("spark.dynamicAllocation.enabled", "true")
            .config("spark.dynamicAllocation.minExecutors", "2")
            .config("spark.dynamicAllocation.maxExecutors", "8")
            .config("spark.sql.streaming.schemaInference", "true")
            .config("spark.sql.streaming.checkpointLocation", "hdfs:///checkpoints")
            .getOrCreate())

# Schema definitions
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

# Data generation utility for local testing
class DataGenerator:
    """Generates simulated financial market data for testing"""
    
    def __init__(self, output_path="data/market_events", 
                 num_files=10, events_per_file=1000, late_event_percentage=0.1):
        self.output_path = output_path
        self.num_files = num_files
        self.events_per_file = events_per_file
        self.late_event_percentage = late_event_percentage
        self.symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA", "FB", "NFLX"]
        self.trade_types = ["BUY", "SELL"]
        self.user_ids = [f"user_{i}" for i in range(1, 101)]
        
        # Create output directory if it doesn't exist
        if not os.path.exists(output_path):
            os.makedirs(output_path)
    
    def generate_data(self):
        """Generate test data files"""
        print(f"Generating {self.num_files} data files with {self.events_per_file} events each...")
        
        for file_num in range(self.num_files):
            events = []
            base_time = datetime.datetime.now() - datetime.timedelta(minutes=self.num_files - file_num)
            
            for i in range(self.events_per_file):
                # Generate random event
                event_time = base_time + datetime.timedelta(seconds=random.randint(0, 59))
                
                # Add late events
                if random.random() < self.late_event_percentage:
                    # Late event (up to 5 minutes late)
                    event_time = event_time - datetime.timedelta(minutes=random.randint(1, 5))
                
                event = {
                    "event_time": event_time.isoformat(),
                    "symbol": random.choice(self.symbols),
                    "price": round(random.uniform(100.0, 1000.0), 2),
                    "volume": random.randint(10, 1000),
                    "user_id": random.choice(self.user_ids),
                    "trade_type": random.choice(self.trade_types)
                }
                events.append(event)
            
            # Write events to file
            filename = f"{self.output_path}/market_data_{file_num}.json"
            with open(filename, 'w') as f:
                for event in events:
                    f.write(json.dumps(event) + '\n')
            
            print(f"Generated file: {filename}")
        
        print("Data generation complete.")

# Custom Window Implementation 1: Session-Based Windows
class SessionWindowAggregation:
    """
    Implements session-based windowing with dynamic session timeouts
    Groups events by user activity rather than fixed time intervals
    """
    
    def __init__(self, spark, input_df, event_time_col="event_time", 
                 user_id_col="user_id", session_gap="30 minutes"):
        self.spark = spark
        self.input_df = input_df
        self.event_time_col = event_time_col
        self.user_id_col = user_id_col
        self.session_gap = session_gap
    
    def process(self):
        """Process data with session windows"""
        # Add watermark for handling late data
        df_with_watermark = self.input_df.withWatermark(self.event_time_col, "10 minutes")
        
        # Use SQL to implement session windows
        df_with_watermark.createOrReplaceTempView("events")
        quoted_session_gap = f"'{self.session_gap}'"
        # SQL approach with session window
        session_agg = self.spark.sql(f"""
            SELECT 
                {self.user_id_col},
                session_window.start AS session_start,
                session_window.end AS session_end,
                COUNT(*) AS event_count,
                SUM(volume) AS total_volume,
                AVG(price) AS avg_price,
                MAX(price) AS max_price,
                MIN(price) AS min_price
            FROM (
                SELECT *,
                    session_window({self.event_time_col}, {quoted_session_gap}) AS session_window

                FROM events
            )
            GROUP BY {self.user_id_col}, session_window
        """)
        
        return session_agg

# Custom Window Implementation 2: Hybrid Windows
class HybridWindowAggregation:
    """
    Implements hybrid windowing that combines tumbling and sliding windows
    Adaptively selects window type based on data characteristics
    """
    
    def __init__(self, spark, input_df, event_time_col="event_time", 
                 tumbling_duration="5 minutes", sliding_duration="1 minute"):
        self.spark = spark
        self.input_df = input_df
        self.event_time_col = event_time_col
        self.tumbling_duration = tumbling_duration
        self.sliding_duration = sliding_duration
    
    def process(self):
        """Process data with hybrid windows"""
        # Add watermark for handling late data
        df_with_watermark = self.input_df.withWatermark(self.event_time_col, "5 minutes")
        
        # Create tumbling window aggregations
        tumbling_agg = (df_with_watermark
            .groupBy(F.window(F.col(self.event_time_col), self.tumbling_duration))
            .agg(
                F.count("*").alias("event_count"),
                F.sum("volume").alias("total_volume"),
                F.avg("price").alias("avg_price"),
                F.max("price").alias("max_price"),
                F.min("price").alias("min_price")
            )
            .withColumn("window_type", F.lit("tumbling"))
        )
        
        # Create sliding window aggregations
        sliding_agg = (df_with_watermark
            .groupBy(F.window(F.col(self.event_time_col), self.tumbling_duration, self.sliding_duration))
            .agg(
                F.count("*").alias("event_count"),
                F.sum("volume").alias("total_volume"),
                F.avg("price").alias("avg_price"),
                F.max("price").alias("max_price"),
                F.min("price").alias("min_price")
            )
            .withColumn("window_type", F.lit("sliding"))
        )
        
        # Union the results
        hybrid_agg = tumbling_agg.unionByName(sliding_agg)
        
        # Extract window start and end times
        result = (hybrid_agg
            .withColumn("window_start", F.col("window.start"))
            .withColumn("window_end", F.col("window.end"))
            .drop("window")
        )
        
        return result

# Custom Window Implementation 3: Multi-Granularity Windows
class MultiGranularityWindowAggregation:
    """
    Implements multi-granularity windows that maintain multiple time horizons
    Processes different window durations in a single pass
    """
    
    def __init__(self, spark, input_df, event_time_col="event_time", 
                 granularities=["1 minute", "5 minutes", "15 minutes"]):
        self.spark = spark
        self.input_df = input_df
        self.event_time_col = event_time_col
        self.granularities = granularities
    
    def process(self):
        """Process data with multi-granularity windows"""
        # Add watermark for handling late data (use max granularity * 2)
        max_granularity = "30 minutes"  # Assuming 15 minutes is the largest granularity
        df_with_watermark = self.input_df.withWatermark(self.event_time_col, max_granularity)
        
        # Process each granularity
        result_dfs = []
        for granularity in self.granularities:
            window_agg = (df_with_watermark
                .groupBy(F.window(F.col(self.event_time_col), granularity))
                .agg(
                    F.count("*").alias("event_count"),
                    F.sum("volume").alias("total_volume"),
                    F.avg("price").alias("avg_price"),
                    F.max("price").alias("max_price"),
                    F.min("price").alias("min_price")
                )
                .withColumn("granularity", F.lit(granularity))
                .withColumn("window_start", F.col("window.start"))
                .withColumn("window_end", F.col("window.end"))
                .drop("window")
            )
            result_dfs.append(window_agg)
        
        # Union all granularities
        if len(result_dfs) > 1:
            result = result_dfs[0]
            for df in result_dfs[1:]:
                result = result.unionByName(df)
        else:
            result = result_dfs[0]
        
        return result

# Custom Window Implementation 4: Adaptive Watermarked Windows
# Add to a new file: adaptive_watermark.py
# Or replace the existing AdaptiveWatermarkedWindowAggregation class in pyspark_implementation.py

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

class EnhancedAdaptiveWatermarkedWindowAggregation:
    """
    Implements adaptive watermarking based on event arrival patterns
    Dynamically adjusts watermark delay based on data characteristics
    """
    
    def __init__(self, spark, input_df, event_time_col="event_time", 
                 window_duration="10 minutes", sliding_duration="2 minutes",
                 max_event_delay="30 minutes", min_event_delay="1 minute"):
        self.spark = spark
        self.input_df = input_df
        self.event_time_col = event_time_col
        self.window_duration = window_duration
        self.sliding_duration = sliding_duration
        self.max_event_delay = max_event_delay
        self.min_event_delay = min_event_delay
    
    def _parse_time(self, time_str):
        """Parse time string like '5 minutes' to minutes as integer"""
        parts = time_str.split()
        if len(parts) != 2 or not parts[0].isdigit():
            return 5  # Default to 5 minutes
        
        value = int(parts[0])
        unit = parts[1].lower()
        
        if unit in ('minute', 'minutes', 'min', 'mins'):
            return value
        elif unit in ('second', 'seconds', 'sec', 'secs'):
            return max(1, value // 60)
        elif unit in ('hour', 'hours'):
            return value * 60
        return value  # Assume minutes as default
    
    def process(self):
        """Process data with adaptive watermarked windows"""
        # For now, use a fixed watermark delay
        # In a production system, we would compute this adaptively from metrics
        watermark_delay = "5 minutes"
        
        # Convert string limits to minutes for bounds checking
        min_delay_minutes = self._parse_time(self.min_event_delay)
        max_delay_minutes = self._parse_time(self.max_event_delay)
        
        # Ensure watermark delay is within bounds
        delay_minutes = 5  # Default 5 minutes
        watermark_delay = f"{delay_minutes} minutes"
        
        print(f"Using adaptive watermark delay: {watermark_delay}")
        
        # Add watermark for handling late data
        df_with_watermark = self.input_df.withWatermark(self.event_time_col, watermark_delay)
        
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
            .withColumn("watermark_delay", F.lit(watermark_delay))
            .withColumn("window_start", F.col("window.start"))
            .withColumn("window_end", F.col("window.end"))
            .drop("window")
        )
        
        return result
    """
    Implements truly adaptive watermarking based on event arrival patterns
    Dynamically adjusts watermark delay based on data characteristics
    """
    
    def __init__(self, spark, input_df, event_time_col="event_time", 
                 window_duration="10 minutes", sliding_duration="2 minutes",
                 max_event_delay="30 minutes", min_event_delay="1 minute"):
        self.spark = spark
        self.input_df = input_df
        self.event_time_col = event_time_col
        self.window_duration = window_duration
        self.sliding_duration = sliding_duration
        self.max_event_delay = max_event_delay
        self.min_event_delay = min_event_delay
        
        # Register temporary views for dynamic watermark calculation
        input_df.createOrReplaceTempView("input_events")
        
        # Create a view for monitoring event delay patterns
        self._create_event_delay_monitor()
        
    def _create_event_delay_monitor(self):
        """Create a view to monitor event delay patterns"""
        self.spark.sql(f"""
            CREATE OR REPLACE TEMPORARY VIEW event_delay_stats AS
            SELECT
                MAX(CAST(current_timestamp() AS LONG) - CAST({self.event_time_col} AS LONG)) AS max_delay_ms,
                AVG(CAST(current_timestamp() AS LONG) - CAST({self.event_time_col} AS LONG)) AS avg_delay_ms,
                PERCENTILE(CAST(current_timestamp() AS LONG) - CAST({self.event_time_col} AS LONG), 0.95) AS p95_delay_ms
            FROM input_events
            WHERE {self.event_time_col} IS NOT NULL
        """)
    
    def compute_adaptive_watermark(self):
        """Compute adaptive watermark based on event arrival patterns"""
        delay_stats = self.spark.sql("SELECT * FROM event_delay_stats").collect()
        
        if not delay_stats or len(delay_stats) == 0:
            # Default to a reasonable watermark if no stats are available
            return "5 minutes" 
        
        # Extract delay statistics
        stats = delay_stats[0]
        p95_delay_ms = stats.p95_delay_ms if stats.p95_delay_ms else 300000  # Default to 5 minutes
        
        # Convert to minutes, add a buffer (20% extra), and ensure within min/max bounds
        delay_minutes = min(max(int(p95_delay_ms / 60000 * 1.2), 
                               self._parse_time(self.min_event_delay)), 
                           self._parse_time(self.max_event_delay))
        
        return f"{delay_minutes} minutes"
    
    def _parse_time(self, time_str):
        """Parse time string like '5 minutes' to minutes as integer"""
        parts = time_str.split()
        if len(parts) != 2 or not parts[0].isdigit():
            return 5  # Default to 5 minutes
        
        value = int(parts[0])
        unit = parts[1].lower()
        
        if unit in ('minute', 'minutes', 'min', 'mins'):
            return value
        elif unit in ('second', 'seconds', 'sec', 'secs'):
            return max(1, value // 60)
        elif unit in ('hour', 'hours'):
            return value * 60
        return value  # Assume minutes as default
    
    def process(self):
        """Process data with truly adaptive watermarked windows"""
        # Compute adaptive watermark delay
        watermark_delay = self.compute_adaptive_watermark()
        print(f"Using adaptive watermark delay: {watermark_delay}")
        
        # Add watermark for handling late data
        df_with_watermark = self.input_df.withWatermark(self.event_time_col, watermark_delay)
        
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
            .withColumn("watermark_delay", F.lit(watermark_delay))
            .withColumn("window_start", F.col("window.start"))
            .withColumn("window_end", F.col("window.end"))
            .drop("window")
        )
        
        return result

# Performance evaluation utilities
class PerformanceEvaluator:
    """Evaluates performance of different window implementations"""
    
    def __init__(self, spark):
        self.spark = spark
        self.results = {}
        self.latency_metrics = {}
        self.throughput_metrics = {}
        self.memory_metrics = {}
        
    def measure_performance(self, name, process_func, input_df, num_iterations=5):
        """Measure performance of a window implementation"""
        print(f"Measuring performance of {name}...")
        
        latencies = []
        record_counts = []
        memory_usage = []
        
        for i in range(num_iterations):
            # Clear cache to ensure fair comparison
            self.spark.catalog.clearCache()
            
            # Record start time
            start_time = time.time()
            
            # Process the data
            result_df = process_func()
            
            # Force materialization
            count = result_df.count()
            
            # Record end time
            end_time = time.time()
            
            # Calculate metrics
            latency = end_time - start_time
            latencies.append(latency)
            record_counts.append(count)
            
            # Approximate memory usage (not accurate but useful for comparison)
            memory = self.spark.sparkContext._jvm.java.lang.Runtime.getRuntime().totalMemory() / (1024 * 1024)
            memory_usage.append(memory)
            
            print(f"  Iteration {i+1}: Latency = {latency:.4f}s, Records = {count}, Memory = {memory:.2f} MB")
        
        # Calculate summary metrics
        avg_latency = sum(latencies) / len(latencies)
        avg_throughput = sum(record_counts) / sum(latencies)
        avg_memory = sum(memory_usage) / len(memory_usage)
        
        # Store results
        self.latency_metrics[name] = {
            "avg": avg_latency,
            "min": min(latencies),
            "max": max(latencies),
            "values": latencies
        }
        
        self.throughput_metrics[name] = {
            "avg": avg_throughput,
            "values": [count/latency for count, latency in zip(record_counts, latencies)]
        }
        
        self.memory_metrics[name] = {
            "avg": avg_memory,
            "peak": max(memory_usage),
            "values": memory_usage
        }
        
        print(f"  Summary: Avg Latency = {avg_latency:.4f}s, Avg Throughput = {avg_throughput:.2f} records/s, Avg Memory = {avg_memory:.2f} MB")
        
        return {
            "latency": self.latency_metrics[name],
            "throughput": self.throughput_metrics[name],
            "memory": self.memory_metrics[name]
        }
    
    def plot_results(self, output_dir="results"):
        """Plot performance comparison results"""
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # Prepare data for plotting
        window_types = list(self.latency_metrics.keys())
        avg_latencies = [self.latency_metrics[name]["avg"] for name in window_types]
        avg_throughputs = [self.throughput_metrics[name]["avg"] for name in window_types]
        avg_memories = [self.memory_metrics[name]["avg"] for name in window_types]
        
        # Plot latency comparison
        plt.figure(figsize=(10, 6))
        plt.bar(window_types, avg_latencies)
        plt.title('Average Processing Latency Comparison')
        plt.xlabel('Window Implementation')
        plt.ylabel('Latency (seconds)')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(f"{output_dir}/latency_comparison.png")
        
        # Plot throughput comparison
        plt.figure(figsize=(10, 6))
        plt.bar(window_types, avg_throughputs)
        plt.title('Average Processing Throughput Comparison')
        plt.xlabel('Window Implementation')
        plt.ylabel('Throughput (records/second)')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(f"{output_dir}/throughput_comparison.png")
        
        # Plot memory comparison
        plt.figure(figsize=(10, 6))
        plt.bar(window_types, avg_memories)
        plt.title('Average Memory Usage Comparison')
        plt.xlabel('Window Implementation')
        plt.ylabel('Memory Usage (MB)')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(f"{output_dir}/memory_comparison.png")
        
        print(f"Performance plots saved to {output_dir}")

# Main execution
def main():
    """Main execution function"""
    print("Starting Apache Spark Stream Processing Enhancement test...")
    
    # Generate test data
    data_gen = DataGenerator(num_files=5, events_per_file=1000)
    data_gen.generate_data()
    
    # Create Spark session
    spark = create_spark_session()
    # Read stream from Kafka
    
    
    # Configure checkpointing
    checkpoint_dir = configure_checkpointing(spark)
    
    # Read generated data 
    input_df = spark.read.json("data/market_events")
    
    # Ensure timestamp column is properly formatted
    input_df = input_df.withColumn("event_time", F.to_timestamp(F.col("event_time")))
    
    # Register the DataFrame as a temp view for SQL-based processing
    input_df.createOrReplaceTempView("market_events")
    
    # Initialize performance evaluator
    evaluator = PerformanceEvaluator(spark)
    
    # Test Enhanced Adaptive Watermarked Window
    enhanced_adaptive_window = EnhancedAdaptiveWatermarkedWindowAggregation(
        spark, input_df, max_event_delay="15 minutes", min_event_delay="1 minute"
    )
    evaluator.measure_performance("Enhanced Adaptive Window", enhanced_adaptive_window.process, input_df)
    
    # Output results to console for testing
    result_df = enhanced_adaptive_window.process()
    query = StreamingOutputSink.to_console(result_df).start()
    
    # Plot and save results
    evaluator.plot_results()
    
    # Wait for termination
    query.awaitTermination(timeout=60)
    
    # Stop Spark session
    spark.stop()
    
    print("Testing complete. Check the results directory for performance comparison plots.")

if __name__ == "__main__":
    main()