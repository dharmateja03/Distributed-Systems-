# Apache Spark Stream Processing Enhancement
## Custom Window-Based Aggregation Strategies - PySpark Implementation

This repository contains a PySpark implementation of custom window-based aggregation strategies for 
Apache Spark Streaming. The implementation focuses on enhancing standard Spark windowing mechanisms 
with more adaptive and efficient alternatives for real-time analytics.

## Features

The implementation includes four custom window-based aggregation strategies:

1. **Session-Based Windows**: Dynamically group events based on user activity patterns rather than fixed time intervals
2. **Hybrid Windows**: Combine tumbling and sliding windows with adaptive behavior
3. **Multi-Granularity Windows**: Maintain multiple time-based aggregations simultaneously
4. **Adaptive Watermarked Windows**: Dynamically adjust watermarks based on data characteristics

## Requirements

- Python 3.6+
- Apache Spark 3.0+
- PySpark
- Pandas
- NumPy
- Matplotlib

## Local Setup and Testing

### Installation

1. Clone this repository:
   ```
   git clone https://github.com/yourusername/spark-custom-windows.git
   cd spark-custom-windows
   ```

2. Install required dependencies:
   ```
   pip install pyspark pandas numpy matplotlib
   ```

### Running the Test Implementation

1. Make the run script executable:
   ```
   chmod +x run-test.sh
   ```

2. Run the test implementation:
   ```
   ./run-test.sh
   ```

   Alternatively, you can run the Python script directly:
   ```
   python pyspark_implementation.py
   ```

3. Check the results:
   - Performance charts will be saved in the `results` directory
   - Test data will be generated in the `data/market_events` directory

## Implementation Details

### Data Generation

The implementation includes a data generator that creates simulated financial market data for testing. 
This allows you to run the tests without requiring an external data source or Kafka setup.

### Performance Evaluation

The test implementation measures and compares:
- Processing latency
- Throughput (records per second)
- Memory usage

Results are displayed in the console and saved as charts in the `results` directory.

### Customization

You can customize various parameters in the implementation:

- Modify data generation parameters in the `DataGenerator` class
- Adjust window durations and other settings in each window implementation class
- Change performance testing parameters in the `PerformanceEvaluator` class

## Extending to a Production Environment

For production use, consider the following extensions:

1. Replace the file-based data source with Kafka integration:
   ```python
   df = spark.readStream.format("kafka") \
       .option("kafka.bootstrap.servers", "host:port") \
       .option("subscribe", "topic_name") \
       .load()
   ```

2. Add proper output sinks:
   ```python
   query = result_df.writeStream \
       .format("console" or "memory" or "delta" or "jdbc") \
       .outputMode("update") \
       .start()
   ```

3. Implement proper checkpointing and fault tolerance:
   ```python
   query = result_df.writeStream \
       .option("checkpointLocation", "/path/to/checkpoint") \
       .start()
   ```

4. Dynamically compute adaptive watermarks based on data characteristics

## Performance Tuning

For better performance on your local machine:

1. Adjust Spark configuration parameters in `create_spark_session()`:
   - Increase/decrease `spark.sql.shuffle.partitions` based on your machine's cores
   - Adjust memory settings if needed

2. For larger datasets, consider:
   - Increasing executor memory: `.config("spark.executor.memory", "4g")`
   - Using more partitions: `.config("spark.sql.shuffle.partitions", "20")`

## Contributing

Feel free to submit issues or pull requests for improvements or bug fixes.

## License

This project is licensed under the MIT License - see the LICENSE file for details.