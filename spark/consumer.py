from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, avg, max, min, sum, count,
    current_timestamp, when, unix_timestamp,
    trim, upper, date_format, concat_ws
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, TimestampType
)

# =====================================================
# SPARK SESSION
# =====================================================
spark = SparkSession.builder \
    .appName("CryptoStream-RealTime-Elastic") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# =====================================================
# KAFKA SCHEMA
# =====================================================
schema = StructType([
    StructField("product_id", StringType()),
    StructField("price", DoubleType()),
    StructField("bid", DoubleType()),
    StructField("ask", DoubleType()),
    StructField("volume_24h", DoubleType()),
    StructField("event_time", TimestampType())
])

# =====================================================
# READ FROM KAFKA
# =====================================================
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "crypto_ticks") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# =====================================================
# PARSE JSON
# =====================================================
parsed_df = raw_df.selectExpr("CAST(value AS STRING) AS json_string") \
    .select(from_json(col("json_string"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("ingestion_time", current_timestamp())

# =====================================================
# CLEANING
# =====================================================
cleaned_df = parsed_df \
    .filter(col("product_id").isNotNull()) \
    .filter(col("price").isNotNull()) \
    .filter(col("bid").isNotNull()) \
    .filter(col("ask").isNotNull()) \
    .filter(col("event_time").isNotNull()) \
    .filter(col("price") > 0) \
    .filter(col("bid") > 0) \
    .filter(col("ask") > 0) \
    .filter(col("volume_24h") >= 0) \
    .filter(col("bid") <= col("ask")) \
    .withColumn("product_id", upper(trim(col("product_id"))))

# =====================================================
# DEDUPLICATION
# =====================================================
dedup_df = cleaned_df.dropDuplicates(["product_id", "event_time", "price"])

# =====================================================
# ENRICHMENT
# =====================================================
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
# FINAL STREAM
# =====================================================
final_df = enriched_df \
    .filter(col("quality_score") >= 50) \
    .filter(col("is_suspicious_spread") == False)

base_stream = final_df.withWatermark("event_time", "5 minutes")

# =====================================================
# WRITE TO ELASTICSEARCH
# =====================================================
def write_to_es(batch_df, batch_id, index_name):
    batch_df.write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.nodes", "elasticsearch") \
        .option("es.port", "9200") \
        .option("es.nodes.wan.only", "true") \
        .option("es.mapping.id", "doc_id") \
        .mode("append") \
        .save(index_name)

# =====================================================
# WINDOW AGG FUNCTION
# =====================================================
def create_window_agg(stream_df, window_duration, index_name, checkpoint_dir, trigger_sec):

    agg_df = stream_df.groupBy(
        col("product_id"),
        window(col("event_time"), window_duration)
    ).agg(
        avg("price").alias("avg_price"),
        min("price").alias("min_price"),
        max("price").alias("max_price"),
        count("*").alias("tick_count"),
        sum("volume_24h").alias("total_volume")
    )

    # Convert timestamps to ISO format (critical for ES date detection)
    agg_df_es = agg_df \
        .withColumn(
            "window_start_ts",
            date_format(col("window.start"), "yyyy-MM-dd'T'HH:mm:ss")
        ) \
        .withColumn(
            "window_end_ts",
            date_format(col("window.end"), "yyyy-MM-dd'T'HH:mm:ss")
        ) \
        .drop("window")

    # Unique document id (prevents overwriting)
    agg_df_es = agg_df_es.withColumn(
        "doc_id",
        concat_ws("_", col("product_id"), col("window_start_ts"))
    )

    query = agg_df_es.writeStream \
        .foreachBatch(lambda df, batch_id: write_to_es(df, batch_id, index_name)) \
        .outputMode("update") \
        .option("checkpointLocation", checkpoint_dir) \
        .trigger(processingTime=trigger_sec) \
        .start()

    return query

# =====================================================
# START STREAMS
# =====================================================
query_1m = create_window_agg(
    base_stream, "1 minute",
    "crypto_agg_1m",
    "/tmp/checkpoint/agg_1m",
    "2 seconds"
)

query_5m = create_window_agg(
    base_stream, "5 minutes",
    "crypto_agg_5m",
    "/tmp/checkpoint/agg_5m",
    "5 seconds"
)

query_15m = create_window_agg(
    base_stream, "15 minutes",
    "crypto_agg_15m",
    "/tmp/checkpoint/agg_15m",
    "10 seconds"
)

print("\n Crypto Streaming Pipeline Started → Elasticsearch")
print("   • 1m → index: crypto_agg_1m")
print("   • 5m → index: crypto_agg_5m")
print("   • 15m → index: crypto_agg_15m\n")

spark.streams.awaitAnyTermination()
