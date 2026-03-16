from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, avg, max, min, first, last, 
    count, sum, stddev, current_timestamp, when, lit,
    coalesce, trim, upper, unix_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

spark = SparkSession.builder \
    .appName("CryptoStream_1") \
    .config("spark.streaming.backpressure.enabled", "true")\
    .config("spark.streaming.backpressure.initialRate", "100")\
    .config("spark.streaming.kafka.maxRatePerPartition", "500")\
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
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

# STEP 1: READ FROM KAFKA


df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "crypto_ticks") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .option("maxOffsetsPerTrigger", "10000") \
    .load() 

# STEP 2: PARSE JOSN
parsed_df=df.selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), schema).alias("d")) \
    .select("d.*")
    
# Add processing timestamp
parsed_df = parsed_df.withColumn("ingestion_time", current_timestamp())

# STEP 3: STREAMING-SAFE DATA CLEANING
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

# STEP 4: DEDUPLICATION (SAFE for streaming)

dedup_df = cleaned_df.dropDuplicates([
    "product_id", 
    "event_time", 
    "price"
])
# STEP 5: DATA ENRICHMENT (SAFE for streaming)
# 5.1 Add derived fields
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

# =====================================================
# STEP 6: FINAL BASE STREAM
# =====================================================
final_df = enriched_df \
    .filter(col("quality_score") >= 50) \
    .filter(col("is_suspicious_spread") == False)

# =====================================================
# STEP 7: BASE STREAM WITH WATERMARK
# =====================================================
# Watermark ensures late data is handled but state doesn't grow indefinitely
base_stream = final_df.withWatermark("event_time", "5 minutes")

# =====================================================
# 1-MINUTE WINDOW AGGREGATION (LIVE)
# =====================================================
agg_1m = base_stream.groupBy(
    col("product_id"),
    window(col("event_time"), "1 minute")
).agg(
    avg("price").alias("avg_price"),
    min("price").alias("min_price"),
    max("price").alias("max_price"),
    count("*").alias("tick_count"),
    sum("volume_24h").alias("total_volume")
)

query_1m = agg_1m.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .option("checkpointLocation", "/tmp/checkpoint/agg_1m") \
    .trigger(processingTime="2 seconds") \
    .queryName("agg_1m") \
    .start()

# =====================================================
# 5-MINUTE WINDOW AGGREGATION (LIVE TREND)
# =====================================================
agg_5m = base_stream.groupBy(
    col("product_id"),
    window(col("event_time"), "5 minutes")
).agg(
    avg("price").alias("avg_price"),
    min("price").alias("min_price"),
    max("price").alias("max_price"),
    count("*").alias("tick_count"),
    sum("volume_24h").alias("total_volume")
)

query_5m = agg_5m.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .option("checkpointLocation", "/tmp/checkpoint/agg_5m") \
    .trigger(processingTime="5 seconds") \
    .queryName("agg_5m") \
    .start()

# =====================================================
# 15-MINUTE WINDOW AGGREGATION (LIVE INTERMEDIATE)
# =====================================================
agg_15m = base_stream.groupBy(
    col("product_id"),
    window(col("event_time"), "15 minutes")
).agg(
    avg("price").alias("avg_price"),
    min("price").alias("min_price"),
    max("price").alias("max_price"),
    count("*").alias("tick_count"),
    sum("volume_24h").alias("total_volume")
)

query_15m = agg_15m.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .option("checkpointLocation", "/tmp/checkpoint/agg_15m") \
    .trigger(processingTime="10 seconds") \
    .queryName("agg_15m") \
    .start()

# =====================================================
# STREAMING PIPELINE ACTIVE
# =====================================================
print("\n🚀 Crypto Streaming Pipeline Started")
print("   • 1m → live updates every 2s")
print("   • 5m → rolling trend every 5s")
print("   • 15m → intermediate live values every 10s\n")

query.awaitTermination()