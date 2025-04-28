# Add to a new file: adaptive_watermark.py
# Or replace the existing AdaptiveWatermarkedWindowAggregation class in pyspark_implementation.py

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

class EnhancedAdaptiveWatermarkedWindowAggregation:
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