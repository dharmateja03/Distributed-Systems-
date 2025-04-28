import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

class StreamingOutputSink:
    """Implements various output sinks for Spark Streaming results"""

    @staticmethod
    def write_to_local_parquet(df, window_type="default"):
        """
        Writes the processed DataFrame to local file storage (parquet).
        """
        output_dir = f"/tmp/stream_output/{window_type}"
        os.makedirs(output_dir, exist_ok=True)

        query = df.writeStream \
            .format("parquet") \
            .outputMode("append") \
            .option("path", output_dir) \
            .option("checkpointLocation", f"/tmp/stream_checkpoints/{window_type}") \
            .trigger(processingTime='30 seconds') \
            .start()

        return query

    @staticmethod
    def write_to_postgres(df, window_type="default"):
        """
        Writes the processed DataFrame to a PostgreSQL table.
        """
        jdbc_url = "jdbc:postgresql://localhost:5432/streamingdb"

        table_name = f"{window_type}_aggregations"
        user = "postgres"
        password = "your_password"
        driver = "org.postgresql.Driver"
        checkpoint_path = f"/tmp/stream_checkpoints/postgres/{window_type}"

        query = df.writeStream \
            .outputMode("append") \
            .foreachBatch(
                lambda batch_df, batch_id: batch_df.write \
                    .format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("dbtable", table_name) \
                    .option("user", user) \
                    .option("password", password) \
                    .option("driver", driver) \
                    .mode("append") \
                    .save()
            ) \
            .option("checkpointLocation", checkpoint_path) \
            .trigger(processingTime="10 seconds") \
            .start()

        return query

    @staticmethod
    def write_to_delta_lake(df, window_type="default"):
        """
        Writes the processed DataFrame to a Delta Lake table.
        """
        output_dir = f"/tmp/stream_output/delta/{window_type}"
        checkpoint_path = f"/tmp/stream_checkpoints/delta/{window_type}"

        query = df.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("path", output_dir) \
            .option("checkpointLocation", checkpoint_path) \
            .start()

        return query
