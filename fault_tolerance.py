

from pyspark.sql import SparkSession
import os

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