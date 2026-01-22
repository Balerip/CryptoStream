from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, avg, max, min, first, last, 
    count, sum, stddev, current_timestamp, when, lit,
    coalesce, trim, upper, unix_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

spark = SparkSession.builder \
    .appName("CryptoStream") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("product_id", StringType()),
    StructField("price", DoubleType()),
    StructField("bid", DoubleType()),
    StructField("ask", DoubleType()),
    StructField("volume_24h", DoubleType()),
    StructField("event_time", TimestampType())
])

# ============================================================================
# STEP 1: READ FROM KAFKA
# ============================================================================
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "crypto_ticks") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .option("maxOffsetsPerTrigger", "10000") \
    .load()

# ============================================================================
# STEP 2: PARSE JSON
# ============================================================================
parsed_df = raw_df \
    .selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), schema).alias("data")) \
    .select("data.*")

# Add processing timestamp
parsed_df = parsed_df.withColumn("ingestion_time", current_timestamp())

# ============================================================================
# STEP 3: STREAMING-SAFE DATA CLEANING
# ============================================================================

# 3.1 Remove NULL values (SAFE for streaming)
cleaned_df = parsed_df \
    .filter(col("product_id").isNotNull()) \
    .filter(col("price").isNotNull()) \
    .filter(col("event_time").isNotNull()) \
    .filter(col("bid").isNotNull()) \
    .filter(col("ask").isNotNull())

# 3.2 Validate numeric ranges (SAFE for streaming)
cleaned_df = cleaned_df \
    .filter(col("price") > 0) \
    .filter(col("bid") > 0) \
    .filter(col("ask") > 0) \
    .filter(col("volume_24h") >= 0)

# 3.3 Validate bid-ask relationship (SAFE for streaming)
cleaned_df = cleaned_df.filter(col("bid") <= col("ask"))

# 3.4 Normalize product_id (SAFE for streaming)
cleaned_df = cleaned_df \
    .withColumn("product_id", upper(trim(col("product_id"))))

# ============================================================================
# STEP 4: DEDUPLICATION (SAFE for streaming)
# ============================================================================
dedup_df = cleaned_df.dropDuplicates([
    "product_id", 
    "event_time", 
    "price"
])

# ============================================================================
# STEP 5: DATA ENRICHMENT (SAFE for streaming)
# ============================================================================

# 5.1 Add derived fields
enriched_df = dedup_df \
    .withColumn("spread", col("ask") - col("bid")) \
    .withColumn("spread_pct", (col("spread") / col("price")) * 100) \
    .withColumn("mid_price", (col("bid") + col("ask")) / 2) \
    .withColumn("spread_bps", (col("spread") / col("price")) * 10000)

# 5.2 Add latency tracking
enriched_df = enriched_df \
    .withColumn("latency_seconds", 
                unix_timestamp("ingestion_time") - unix_timestamp("event_time"))

# 5.3 Flag suspicious spreads (simple filter, no windows)
enriched_df = enriched_df \
    .withColumn("is_suspicious_spread", col("spread_pct") > 10)

# 5.4 Add quality score (simple rule-based, no windows)
enriched_df = enriched_df \
    .withColumn("quality_score", 
                when((col("latency_seconds") < 5) & (col("spread_pct") < 1.0), 100)
                .when((col("latency_seconds") < 10) & (col("spread_pct") < 5.0), 80)
                .otherwise(50))

# ============================================================================
# STEP 6: FILTER FINAL RECORDS
# ============================================================================
final_df = enriched_df \
    .filter(col("quality_score") >= 50) \
    .filter(col("is_suspicious_spread") == False)

# ============================================================================
# STEP 7: SIMPLE OUTLIER DETECTION (Time-based, streaming-safe)
# ============================================================================
# Use time-based aggregation to detect outliers
# This replaces the row-based rolling window

outlier_detection = final_df \
    .withWatermark("event_time", "30 seconds") \
    .groupBy(
        col("product_id"),
        window(col("event_time"), "2 minutes", "30 seconds")
    ) \
    .agg(
        avg("price").alias("window_avg_price"),
        stddev("price").alias("window_std_price"),
        max("price").alias("window_max_price"),
        min("price").alias("window_min_price")
    )

# Print outlier stats
outlier_query = outlier_detection \
    .select(
        "product_id",
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "window_avg_price",
        "window_std_price",
        "window_max_price",
        "window_min_price"
    ) \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .option("checkpointLocation", "/tmp/checkpoint/outliers") \
    .queryName("outlier_detection") \
    .trigger(processingTime="10 seconds") \
    .start()

# ============================================================================
# MAIN QUERY: Enhanced Rolling Average
# ============================================================================
main_query = final_df \
    .withWatermark("event_time", "10 seconds") \
    .groupBy(
        col("product_id"),
        window(col("event_time"), "30 seconds", "10 seconds")
    ) \
    .agg(
        avg("price").alias("avg_price"),
        min("price").alias("min_price"),
        max("price").alias("max_price"),
        count("*").alias("tick_count"),
        stddev("price").alias("price_volatility"),
        avg("spread").alias("avg_spread"),
        avg("spread_pct").alias("avg_spread_pct"),
        sum("volume_24h").alias("total_volume"),
        avg("quality_score").alias("avg_quality_score"),
        avg("latency_seconds").alias("avg_latency")
    ) \
    .select(
        "product_id",
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "avg_price",
        "min_price",
        "max_price",
        "tick_count",
        "price_volatility",
        "avg_spread",
        "avg_spread_pct",
        "total_volume",
        "avg_quality_score",
        "avg_latency"
    ) \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .option("checkpointLocation", "/tmp/checkpoint/main") \
    .trigger(processingTime="5 seconds") \
    .queryName("main_query") \
    .start()

# ============================================================================
# DATA QUALITY MONITORING
# ============================================================================
quality_query = final_df \
    .withWatermark("event_time", "30 seconds") \
    .groupBy(
        col("product_id"),
        window(col("event_time"), "1 minute")
    ) \
    .agg(
        count("*").alias("total_records"),
        avg("quality_score").alias("avg_quality"),
        avg("latency_seconds").alias("avg_latency"),
        sum(when(col("spread_pct") > 5, 1).otherwise(0)).alias("high_spread_count"),
        sum(when(col("quality_score") < 80, 1).otherwise(0)).alias("low_quality_count")
    ) \
    .select(
        "product_id",
        col("window.start").alias("minute_start"),
        "total_records",
        "avg_quality",
        "avg_latency",
        "high_spread_count",
        "low_quality_count"
    ) \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .option("checkpointLocation", "/tmp/checkpoint/quality") \
    .queryName("quality_metrics") \
    .trigger(processingTime="15 seconds") \
    .start()

# ============================================================================
# SUMMARY
# ============================================================================
print("\n" + "="*70)
print("🚀 STREAMING PIPELINE ACTIVE")
print("="*70)
print("\n✅ PREPROCESSING STEPS:")
print("   1. JSON Parsing")
print("   2. NULL Filtering")
print("   3. Numeric Validation (price > 0, bid <= ask)")
print("   4. Deduplication")
print("   5. Data Enrichment (spread, mid_price, quality_score)")
print("   6. Latency Tracking")
print("   7. Suspicious Spread Detection (>10%)")
print("\n📊 ACTIVE QUERIES:")
print("   • main_query: 30-sec rolling avg with enhanced metrics")
print("   • outlier_detection: 2-min windows for anomaly detection")
print("   • quality_metrics: Per-minute data quality monitoring")
print("\n⚠️  REMOVED (Not streaming-compatible):")
print("   ✗ Row-based rolling windows (Z-score outlier detection)")
print("   ✗ Lag functions without time windows")
print("\n💡 All preprocessing is now STREAMING-SAFE!")
print("="*70)
print("\nPress Ctrl+C to stop...\n")

# Wait for termination
try:
    main_query.awaitTermination()
except KeyboardInterrupt:
    print("\n\n🛑 Stopping all queries...")
    for q in spark.streams.active:
        q.stop()
    print("✅ All queries stopped successfully\n")