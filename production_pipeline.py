#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Apache Spark Stream Processing Enhancement: Production Implementation
This module integrates all the custom window-based aggregation strategies
with fault tolerance and monitoring capabilities.
"""

import os
import time
import json
import datetime
import random
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, LongType
import matplotlib.pyplot as plt
import pandas as pd
import threading

# Create Spark Session for distributed environment
def create_distributed_spark_session(app_name="CustomWindowAggregation", 
                                    master_url="yarn",  # Use "local[*]" for local testing
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

# Create Spark Session for local testing
def create_local_spark_session(app_name="CustomWindowAggregation"):
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

# Fault Tolerance Utilities
def configure_checkpointing(spark, checkpoint_dir="checkpoint", checkpoint_interval="1 minute"):
    """Configure proper checkpointing for fault tolerance"""
    # Ensure checkpoint directory exists
    if not os.path.exists(checkpoint_dir):
        os.makedirs(checkpoint_dir)
    
    # Configure checkpointing for the Spark session
    spark.conf.set("spark.sql.streaming.checkpointLocation", checkpoint_dir)
    
    return checkpoint_dir

def create_fault_tolerant_query(result_df, output_mode="update", checkpoint_dir=None, 
                              query_name=None, trigger_interval="10 seconds"):
    """Create a streaming query with proper fault tolerance configuration"""
    if checkpoint_dir is None:
        checkpoint_dir = "checkpoint/" + (query_name or "default_query")
    
    # Configure the streaming query with checkpointing
    return (result_df.writeStream
            .outputMode(output_mode)
            .option("checkpointLocation", checkpoint_dir)
            .trigger(processingTime=trigger_interval)
            .queryName(query_name or "default_query"))

# Enhanced Adaptive Watermarked Windows Implementation
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

# Session-Based Windows Implementation
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

# Hybrid Windows Implementation
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

# Multi-Granularity Windows Implementation
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

# Performance monitoring and metrics collection
class PerformanceMonitor:
    """Monitors and collects performance metrics for streaming jobs"""
    
    def __init__(self, spark, metrics_path="metrics"):
        self.spark = spark
        self.metrics_path = metrics_path
        self.metrics = {
            "latency": [],
            "throughput": [],
            "memory": [],
            "watermark_delays": []
        }
        
        # Create metrics directory if it doesn't exist
        if not os.path.exists(metrics_path):
            os.makedirs(metrics_path)
    
    def record_metrics(self, window_type, processing_time, event_count, memory_usage=0.0, watermark_delay="unknown"):
        """Record performance metrics"""
        timestamp = datetime.datetime.now().isoformat()
        
        # Store metrics
        self.metrics["latency"].append((timestamp, window_type, processing_time))
        self.metrics["throughput"].append((timestamp, window_type, event_count / max(0.001, processing_time)))
        self.metrics["memory"].append((timestamp, window_type, memory_usage))
        self.metrics["watermark_delays"].append((timestamp, window_type, watermark_delay))
        
        # Log metrics
        print(f"[{timestamp}] {window_type}: Latency={processing_time:.4f}s, " +
              f"Throughput={event_count/max(0.001, processing_time):.2f} events/s, " +
              f"Memory={memory_usage:.2f}MB, Watermark={watermark_delay}")
    
    def save_metrics(self):
        """Save collected metrics to files"""
        # Convert to DataFrames
        for metric_name, metric_data in self.metrics.items():
            if not metric_data:
                continue
                
            # Create DataFrame
            if metric_name == "watermark_delays":
                df = pd.DataFrame(metric_data, columns=["timestamp", "window_type", "watermark_delay"])
            else:
                df = pd.DataFrame(metric_data, columns=["timestamp", "window_type", "value"])
            
            # Save to CSV
            filename = f"{self.metrics_path}/{metric_name}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            df.to_csv(filename, index=False)
            print(f"Saved {metric_name} metrics to {filename}")
    
    def plot_metrics(self):
        """Plot collected metrics"""
        # Plot latency
        if self.metrics["latency"]:
            plt.figure(figsize=(10, 6))
            df = pd.DataFrame(self.metrics["latency"], columns=["timestamp", "window_type", "value"])
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            
            for window_type, group in df.groupby("window_type"):
                plt.plot(group["timestamp"], group["value"], label=window_type)
            
            plt.title("Processing Latency")
            plt.xlabel("Time")
            plt.ylabel("Latency (seconds)")
            plt.legend()
            plt.grid(True)
            plt.savefig(f"{self.metrics_path}/latency_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
        
        # Plot throughput
        if self.metrics["throughput"]:
            plt.figure(figsize=(10, 6))
            df = pd.DataFrame(self.metrics["throughput"], columns=["timestamp", "window_type", "value"])
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            
            for window_type, group in df.groupby("window_type"):
                plt.plot(group["timestamp"], group["value"], label=window_type)
            
            plt.title("Processing Throughput")
            plt.xlabel("Time")
            plt.ylabel("Throughput (events/second)")
            plt.legend()
            plt.grid(True)
            plt.savefig(f"{self.metrics_path}/throughput_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.png")

# Main Production Pipeline
def run_production_pipeline(mode="local", duration_seconds=None):
    """
    Run the production pipeline with all enhanced components
    
    Args:
        mode: 'local' for local testing, 'distributed' for cluster deployment
        duration_seconds: If provided, run for this many seconds, otherwise run indefinitely
    """
    print(f"Starting Apache Spark Stream Processing Enhancement in {mode} mode...")
    
    # Create appropriate Spark session
    if mode == "local":
        spark = create_local_spark_session()
    else:
        spark = create_distributed_spark_session()
    
    # Configure checkpointing
    checkpoint_dir = configure_checkpointing(spark, "checkpoint")
    
    # Set up performance monitoring
    monitor = PerformanceMonitor(spark)
    
    # Set up data source
    if mode == "local":
        # Generate test data for local mode
        data_gen = DataGenerator(num_files=10, events_per_file=1000)
        data_gen.generate_data()
        
        # Create input DataFrame from local files
        input_df = spark.readStream \
            .schema(get_event_schema()) \
            .json("data/market_events")
    else:
        # Use Kafka for distributed mode (placeholder - to be implemented)
        # This will be replaced with actual Kafka integration
        print("PLACEHOLDER: Kafka integration would be implemented here")
        
        # For now, use the same file-based approach for demonstration
        data_gen = DataGenerator(num_files=10, events_per_file=1000)
        data_gen.generate_data()
        
        input_df = spark.readStream \
            .schema(get_event_schema()) \
            .json("data/market_events")
    
    # Ensure timestamp column is properly formatted
    input_df = input_df.withColumn("event_time", F.to_timestamp(F.col("event_time")))
    
    # Create instances of all window implementations
    enhanced_adaptive_window = EnhancedAdaptiveWatermarkedWindowAggregation(
        spark, input_df,
        window_duration="5 minutes",
        sliding_duration="1 minute",
        max_event_delay="15 minutes",
        min_event_delay="30 seconds"
    )
    
    session_window = SessionWindowAggregation(
        spark, input_df,
        session_gap="10 minutes"
    )
    
    hybrid_window = HybridWindowAggregation(
        spark, input_df,
        tumbling_duration="5 minutes",
        sliding_duration="1 minute"
    )
    
    multi_granularity_window = MultiGranularityWindowAggregation(
        spark, input_df,
        granularities=["1 minute", "5 minutes", "15 minutes"]
    )
    
    # Process with all window implementations
    # print("Starting adaptive watermarked window processing...")
    # adaptive_start_time = time.time()
    # adaptive_result = enhanced_adaptive_window.process()
    
    print("Starting session window processing...")
    session_start_time = time.time()
    session_result = session_window.process()
    
    print("Starting hybrid window processing...")
    hybrid_start_time = time.time()
    hybrid_result = hybrid_window.process()
    
    print("Starting multi-granularity window processing...")
    multi_start_time = time.time()
    multi_result = multi_granularity_window.process()
    
    # Create fault-tolerant queries with console output for visualization
    # adaptive_query = create_fault_tolerant_query(
    #     adaptive_result,
    #     output_mode="update",
    #     checkpoint_dir=f"{checkpoint_dir}/adaptive",
    #     query_name="adaptive_window"
    # ).format("console") \
    #  .option("truncate", "false") \
    #  .start()
    
    # For session window query
    session_query = session_result.writeStream \
        .outputMode("append").format("console") \
    .option("truncate", "false") \
    .option("checkpointLocation", f"{checkpoint_dir}/session") \
    .start()
    
    hybrid_query = create_fault_tolerant_query(
        hybrid_result,
        output_mode="update",
        checkpoint_dir=f"{checkpoint_dir}/hybrid",
        query_name="hybrid_window"
    ).format("console") \
     .option("truncate", "false") \
     .start()
    
    multi_query = create_fault_tolerant_query(
        multi_result,
        output_mode="update",
        checkpoint_dir=f"{checkpoint_dir}/multi",
        query_name="multi_granularity_window"
    ).format("console") \
     .option("truncate", "false") \
     .start()
    
    # Set up performance metrics collection
    def collect_metrics(interval=10):
        """Periodically collect performance metrics"""
        while True:
            try:
                # Record metrics for each window implementation
                
                
                monitor.record_metrics(
                    "Session Window",
                    time.time() - session_start_time,
                    session_query.lastProgress["numInputRows"] if session_query.lastProgress else 0,
                    spark.sparkContext._jvm.java.lang.Runtime.getRuntime().totalMemory() / (1024 * 1024),
                    "10 minutes"  # Fixed value for this implementation
                )
                
                monitor.record_metrics(
                    "Hybrid Window",
                    time.time() - hybrid_start_time,
                    hybrid_query.lastProgress["numInputRows"] if hybrid_query.lastProgress else 0,
                    spark.sparkContext._jvm.java.lang.Runtime.getRuntime().totalMemory() / (1024 * 1024),
                    "5 minutes"  # Fixed value for this implementation
                )
                
                monitor.record_metrics(
                    "Multi-Granularity Window",
                    time.time() - multi_start_time,
                    multi_query.lastProgress["numInputRows"] if multi_query.lastProgress else 0,
                    spark.sparkContext._jvm.java.lang.Runtime.getRuntime().totalMemory() / (1024 * 1024),
                    "30 minutes"  # Fixed value for this implementation
                )

            #     monitor.record_metrics(
            #     "Adaptive Window",
            #     time.time() - adaptive_start_time,
            #     adaptive_query.lastProgress["numInputRows"] if adaptive_query.lastProgress else 0,
            #     spark.sparkContext._jvm.java.lang.Runtime.getRuntime().totalMemory() / (1024 * 1024),
            #     "5 minutes"  # Use the fixed value instead of accessing schema
            # )
            except Exception as e:
                print(f"Error collecting metrics: {e}")
            
            # Sleep before next collection
            time.sleep(interval)
    
    # Start metrics collection in a background thread
    metrics_thread = threading.Thread(target=collect_metrics, daemon=True)
    metrics_thread.start()
    
    try:
        # Run for specified duration or indefinitely
        if duration_seconds:
            print(f"Running for {duration_seconds} seconds...")
            time.sleep(duration_seconds)
            print("Run duration completed.")
        else:
            print("Running indefinitely. Press Ctrl+C to stop.")
            spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("Interrupted by user.")
    finally:
        # Stop all queries
        print("Stopping queries...")
        # adaptive_query.stop()
        session_query.stop()
        hybrid_query.stop()
        multi_query.stop()
        
        # Save and plot metrics
        monitor.save_metrics()
        monitor.plot_metrics()
        
        # Stop Spark session
        spark.stop()
        
        print("Processing completed.")

# Entry point for execution
if __name__ == "__main__":
    import sys
    
    # Parse command line arguments
    mode = "local"
    duration = None
    
    if len(sys.argv) > 1:
        mode = sys.argv[1]
    
    if len(sys.argv) > 2:
        try:
            duration = int(sys.argv[2])
        except ValueError:
            print(f"Invalid duration: {sys.argv[2]}. Using default (run indefinitely).")
    
    # Run the pipeline
    run_production_pipeline(mode=mode, duration_seconds=duration)