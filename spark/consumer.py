from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    from_json, col, window, avg, max, min, first, last,
    count, sum, stddev, current_timestamp, when, lit,
    coalesce, trim, upper, unix_timestamp, lag
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# ============================================
# SPARK SESSION
# ============================================
spark = SparkSession.builder \
    .appName("CryptoStream_FullMetrics") \
    .config("spark.streaming.backpressure.enabled", "true")\
    .config("spark.streaming.backpressure.initialRate", "100")\
    .config("spark.streaming.kafka.maxRatePerPartition", "500")\
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ============================================
# SCHEMA FOR KAFKA JSON
# ============================================
schema = StructType([
    StructField("product_id", StringType()),
    StructField("price", DoubleType()),
    StructField("bid", DoubleType()),
    StructField("ask", DoubleType()),
    StructField("volume_24h", DoubleType()),
    StructField("event_time", TimestampType())
])

# ============================================
# 1. READ FROM KAFKA
# ============================================
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "crypto_ticks") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .option("maxOffsetsPerTrigger", "10000") \
    .load()

# ============================================
# 2. PARSE JSON
# ============================================
parsed_df = df.selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), schema).alias("d")) \
    .select("d.*") \
    .withColumn("ingestion_time", current_timestamp())

# ============================================
# 3. CLEAN & VALIDATE DATA
# ============================================
cleaned_df = parsed_df \
    .filter(col("product_id").isNotNull()) \
    .filter(col("price").isNotNull()) \
    .filter(col("event_time").isNotNull()) \
    .filter(col("bid").isNotNull()) \
    .filter(col("ask").isNotNull()) \
    .filter(col("price") > 0) \
    .filter(col("bid") > 0) \
    .filter(col("ask") > 0) \
    .filter(col("volume_24h") >= 0) \
    .filter(col("bid") <= col("ask")) \
    .withColumn("product_id", upper(trim(col("product_id"))))

# ============================================
# 4. DEDUPLICATION
# ============================================
dedup_df = cleaned_df.dropDuplicates(["product_id", "event_time", "price"])

# ============================================
# 5. ENRICH DATA
# ============================================
enriched_df = dedup_df \
    .withColumn("spread", col("ask") - col("bid")) \
    .withColumn("spread_pct", (col("spread") / col("price")) * 100) \
    .withColumn("mid_price", (col("bid") + col("ask")) / 2) \
    .withColumn("latency_seconds",
                unix_timestamp("ingestion_time") - unix_timestamp("event_time")) \
    .withColumn("quality_score",
                when((col("latency_seconds") < 5) & (col("spread_pct") < 1), 100)
                .when((col("latency_seconds") < 10) & (col("spread_pct") < 5), 80)
                .otherwise(50)) \
    .withColumn("is_suspicious_spread", col("spread_pct") > 10)

# ============================================
# 6. FINAL STREAM FILTERING
# ============================================
final_df = enriched_df \
    .filter(col("quality_score") >= 50) \
    .filter(col("is_suspicious_spread") == False)

# ============================================
# 7. WATERMARK
# ============================================
base_stream = final_df.withWatermark("event_time", "5 minutes")

# ============================================
# FUNCTION TO AGGREGATE METRICS PER WINDOW
# ============================================
def compute_window_agg(df, window_duration):
    agg_df = df.groupBy(
        col("product_id"),
        window(col("event_time"), window_duration)
    ).agg(
        first("price").alias("open_price"),
        last("price").alias("close_price"),
        max("price").alias("high_price"),
        min("price").alias("low_price"),
        sum("volume_24h").alias("total_volume"),
        avg("price").alias("avg_price"),
        stddev("price").alias("volatility"),
        avg((col("ask") - col("bid")) / col("mid_price") * 10000).alias("spread_bps"),
        (sum(col("price") * col("volume_24h")) / sum("volume_24h")).alias("vwap")
    )

    # Window spec for lag-based metrics per product
    windowSpec = Window.partitionBy("product_id").orderBy("window")

    # Momentum & % Change
    agg_df = agg_df.withColumn("close_prev", lag("close_price").over(windowSpec)) \
                   .withColumn("momentum", col("close_price") - col("close_prev")) \
                   .withColumn("pct_change", ((col("close_price") - col("close_prev")) / col("close_prev")) * 100)

    # RSI 14-window
    agg_df = agg_df.withColumn("gain", when(col("momentum") > 0, col("momentum")).otherwise(0)) \
                   .withColumn("loss", when(col("momentum") < 0, -col("momentum")).otherwise(0))

    rsi_window = Window.partitionBy("product_id").orderBy("window").rowsBetween(-13, 0)
    agg_df = agg_df.withColumn("avg_gain", avg("gain").over(rsi_window)) \
                   .withColumn("avg_loss", avg("loss").over(rsi_window)) \
                   .withColumn("RSI", 100 - 100 / (1 + col("avg_gain") / col("avg_loss")))

    return agg_df

# ============================================
# COMPUTE 1m, 5m, 15m METRICS
# ============================================
agg_1m = compute_window_agg(base_stream, "1 minute")
agg_5m = compute_window_agg(base_stream, "5 minutes")
agg_15m = compute_window_agg(base_stream, "15 minutes")

# ============================================
# WRITE STREAMS TO CONSOLE
# ============================================
query_1m = agg_1m.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .option("checkpointLocation", "/tmp/checkpoint/agg_1m_full_metrics") \
    .trigger(processingTime="2 seconds") \
    .start()

query_5m = agg_5m.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .option("checkpointLocation", "/tmp/checkpoint/agg_5m_full_metrics") \
    .trigger(processingTime="5 seconds") \
    .start()

query_15m = agg_15m.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .option("checkpointLocation", "/tmp/checkpoint/agg_15m_full_metrics") \
    .trigger(processingTime="10 seconds") \
    .start()

# ============================================
# STREAMING PIPELINE ACTIVE
# ============================================
print("\n🚀 Crypto Streaming Pipeline Started")
print("   • 1m → live updates every 2s")
print("   • 5m → rolling trend every 5s")
print("   • 15m → intermediate live values every 10s\n")

query_1m.awaitTermination()