from pyspark.sql import SparkSession
from pyspark.sql.functions import window, session_window, col, avg, count, expr
from kafka_config import kafka_bootstrap_servers, input_topic
from fault_tolerance import with_fault_tolerance
from output_sinks import StreamingOutputSink


# Initialize Spark Session
spark = SparkSession.builder \
    .appName("CustomWindowAggregations") \
    .getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions", 4)

# Read data from Kafka
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", input_topic) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false")\
    .load()

# Assuming incoming value is in JSON format and has event_time and key fields
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StringType, TimestampType

schema = StructType() \
    .add("event_time", TimestampType()) \
    .add("symbol", StringType()) \
    .add("price", StringType()) \
    .add("volume", StringType()) \
    .add("user_id", StringType()) \
    .add("trade_type", StringType())


stream_df = raw_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", schema).alias("data")) \
    .select("data.*")
stream_df = stream_df.withColumn("price", col("price").cast("double"))



# --------------------------------------
# Custom Window Functions
# --------------------------------------

def process_session_window(df):
    session_gap = "2 minutes"
    result = df.withWatermark("event_time", "5 minutes") \
        .groupBy(window("event_time", session_gap), col("symbol")) \
        .agg(
            count("*").alias("event_count"),
            avg("price").alias("avg_price")
        ) \
        .selectExpr(
            "window.start AS window_start",
            "window.end AS window_end",
            "symbol",
            "event_count",
            "avg_price"
        )
    return result



def process_hybrid_window(df):
    window_duration = "2 minutes"
    slide_duration = "1 minute"
    
    result = df.withWatermark("event_time", "5 minutes") \
        .groupBy(window("event_time", window_duration, slide_duration), col("symbol")) \
        .agg(
            count("*").alias("event_count"),
            avg("price").alias("avg_price")
        ) \
        .selectExpr(
            "window.start AS window_start",
            "window.end AS window_end",
            "symbol",
            "event_count",
            "avg_price"
        )
    return result



def process_multi_granularity_window(df):
    granularities = ["1 minute", "5 minutes", "15 minutes"]
    
    union_df = None
    for granularity in granularities:
        temp = df.withWatermark("event_time", "5 minutes") \
            .groupBy(window("event_time", granularity), col("symbol")) \
            .agg(
                count("*").alias("event_count"),
                avg("price").alias("avg_price")
            ) \
            .selectExpr(
                "window.start AS window_start",
                "window.end AS window_end",
                "symbol",
                "event_count",
                "avg_price"
            ) \
            .withColumn("granularity", expr(f"'{granularity}'"))
        
        union_df = temp if union_df is None else union_df.union(temp)
    
    return union_df


def process_adaptive_watermarked_window(df):
    result = df.withWatermark("event_time", "5 minutes") \
        .groupBy(session_window("event_time", "2 minutes"), col("symbol")) \
        .agg(
            count("*").alias("event_count"),
            avg("price").alias("avg_price")
        ) \
        .selectExpr(
            "session_window.start AS window_start",
            "session_window.end AS window_end",
            "symbol",
            "event_count",
            "avg_price"
        )
    return result



# --------------------------------------
# Choose which window function to run
# --------------------------------------

# Process each window type separately
session_df = process_session_window(stream_df)
hybrid_df = process_hybrid_window(stream_df)
multi_granularity_df = process_multi_granularity_window(stream_df)
adaptive_df = process_adaptive_watermarked_window(stream_df)

# Write outputs to PostgreSQL separately
with_fault_tolerance(session_df, lambda df: StreamingOutputSink.write_to_postgres(df, window_type="session"))
with_fault_tolerance(hybrid_df, lambda df: StreamingOutputSink.write_to_postgres(df, window_type="hybrid"))
with_fault_tolerance(multi_granularity_df, lambda df: StreamingOutputSink.write_to_postgres(df, window_type="multi_granularity"))
with_fault_tolerance(adaptive_df, lambda df: StreamingOutputSink.write_to_postgres(df, window_type="adaptive"))

# Start the streaming queries
spark.streams.awaitAnyTermination()
# --------------------------------------

