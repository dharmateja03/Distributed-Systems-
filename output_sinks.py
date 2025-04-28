# Add to a new file: output_sinks.py

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

class StreamingOutputSink:
    """Implements various output sinks for Spark Streaming results"""
    
    @staticmethod
    def to_console(result_df, output_mode="update", query_name="console_output"):
        """Output results to console (for debugging)"""
        return (result_df.writeStream
                .outputMode(output_mode)
                .format("console")
                .option("truncate", "false")
                .queryName(query_name))
    
    @staticmethod
    def to_memory(result_df, table_name, output_mode="update", query_name="memory_output"):
        """Output results to in-memory table"""
        return (result_df.writeStream
                .outputMode(output_mode)
                .format("memory")
                .queryName(query_name)
                .option("queryName", table_name))
    
    @staticmethod
    def to_delta_lake(result_df, path, checkpoint_path=None, output_mode="append", 
                     query_name="delta_output", partition_by=None):
        """Output results to Delta Lake"""
        writer = (result_df.writeStream
                 .outputMode(output_mode)
                 .format("delta")
                 .option("checkpointLocation", checkpoint_path or f"checkpoint/{query_name}")
                 .queryName(query_name))
        
        if partition_by:
            writer = writer.partitionBy(partition_by)
            
        return writer.start(path)
    
    @staticmethod
    def to_jdbc(result_df, url, table, user, password, driver="org.postgresql.Driver",
               checkpoint_path=None, query_name="jdbc_output", output_mode="append",
               batch_size=1000):
        """Output results to a JDBC database (e.g., PostgreSQL)"""
        return (result_df.writeStream
                .outputMode(output_mode)
                .foreachBatch(lambda batch_df, batch_id: 
                             batch_df.write
                             .format("jdbc")
                             .option("url", url)
                             .option("dbtable", table)
                             .option("user", user)
                             .option("password", password)
                             .option("driver", driver)
                             .option("batchsize", batch_size)
                             .mode("append")
                             .save())
                .option("checkpointLocation", checkpoint_path or f"checkpoint/{query_name}")
                .queryName(query_name))
    
    @staticmethod
    def to_kafka(result_df, bootstrap_servers, topic, 
                checkpoint_path=None, query_name="kafka_output",
                key_col=None, value_cols=None):
        """Output results to Kafka topic"""
        # If specific columns are not provided, convert entire row to JSON
        if value_cols is None:
            df_to_write = result_df.withColumn("value", 
                                              F.to_json(F.struct(*result_df.columns)).cast("string"))
        else:
            df_to_write = result_df.withColumn("value", 
                                              F.to_json(F.struct(*value_cols)).cast("string"))
        
        # Add key column if specified
        if key_col:
            df_to_write = df_to_write.withColumn("key", F.col(key_col).cast("string"))
        
        return (df_to_write.writeStream
                .outputMode("append")
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrap_servers)
                .option("topic", topic)
                .option("checkpointLocation", checkpoint_path or f"checkpoint/{query_name}")
                .queryName(query_name))