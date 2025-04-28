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

# Create Spark Session with specific configurations for local testing
def create_spark_session(app_name="CustomWindowAggregation"):
    """Create and configure a Spark session for local testing"""
    return (SparkSession.builder
            .appName(app_name)
            .config("spark.sql.shuffle.partitions", "8")  # Reduce for local testing
            .config("spark.sql.streaming.schemaInference", "true")
            .config("spark.sql.streaming.checkpointLocation", "checkpoint")
            .master("local[*]")  # Use all available cores
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
class AdaptiveWatermarkedWindowAggregation:
    """
    Implements adaptive watermarking based on event arrival patterns
    Dynamically adjusts watermark delay based on data characteristics
    """
    
    def __init__(self, spark, input_df, event_time_col="event_time", 
                 window_duration="10 minutes", sliding_duration="2 minutes"):
        self.spark = spark
        self.input_df = input_df
        self.event_time_col = event_time_col
        self.window_duration = window_duration
        self.sliding_duration = sliding_duration
        # For local testing, we'll use a static watermark
        # In production, we would compute this dynamically
        self.watermark_delay = "5 minutes"
    
    def process(self):
        """Process data with adaptive watermarked windows"""
        # Add watermark for handling late data
        df_with_watermark = self.input_df.withWatermark(self.event_time_col, self.watermark_delay)
        
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
            .withColumn("watermark_delay", F.lit(self.watermark_delay))
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
    
    # Read generated data 
    input_df = spark.read.json("data/market_events")
    
    # Ensure timestamp column is properly formatted
    input_df = input_df.withColumn("event_time", F.to_timestamp(F.col("event_time")))
    
    # Register the DataFrame as a temp view for SQL-based processing
    input_df.createOrReplaceTempView("market_events")
    
    # Initialize performance evaluator
    evaluator = PerformanceEvaluator(spark)
    
    # Test Standard Window (as baseline)
    standard_window = lambda: (
        input_df
        .withWatermark("event_time", "5 minutes")
        .groupBy(F.window(F.col("event_time"), "5 minutes"))
        .agg(
            F.count("*").alias("event_count"),
            F.sum("volume").alias("total_volume"),
            F.avg("price").alias("avg_price")
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .drop("window")
    )
    evaluator.measure_performance("Standard Window", standard_window, input_df)
    
    # Test Session-Based Window
    session_window = SessionWindowAggregation(spark, input_df)
    evaluator.measure_performance("Session-Based Window", session_window.process, input_df)
    
    # Test Hybrid Window
    hybrid_window = HybridWindowAggregation(spark, input_df)
    evaluator.measure_performance("Hybrid Window", hybrid_window.process, input_df)
    
    # Test Multi-Granularity Window
    multi_granularity_window = MultiGranularityWindowAggregation(spark, input_df)
    evaluator.measure_performance("Multi-Granularity Window", multi_granularity_window.process, input_df)
    
    # Test Adaptive Watermarked Window
    adaptive_window = AdaptiveWatermarkedWindowAggregation(spark, input_df)
    evaluator.measure_performance("Adaptive Watermarked Window", adaptive_window.process, input_df)
    
    # Plot and save results
    evaluator.plot_results()
    
    # Stop Spark session
    spark.stop()
    
    print("Testing complete. Check the results directory for performance comparison plots.")

if __name__ == "__main__":
    main()