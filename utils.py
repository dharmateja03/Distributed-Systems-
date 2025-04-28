# utils.py

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from pyspark.sql.functions import from_json, col

def create_spark_session(app_name="KafkaSparkStreamingApp"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.streaming.backpressure.enabled", "true") \
        .getOrCreate()
    return spark

def read_stream_from_kafka(spark, kafka_bootstrap_servers, kafka_topic):
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "latest")  # or "earliest"
        .load()
    )

    value_df = kafka_df.selectExpr("CAST(value AS STRING) as message")

    schema = StructType([
        StructField("event_time", TimestampType(), True),
        StructField("symbol", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("volume", IntegerType(), True)
    ])

    parsed_df = value_df.select(from_json(col("message"), schema).alias("data")).select("data.*")

    return parsed_df
