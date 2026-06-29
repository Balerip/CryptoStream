from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    col,
    window,
    avg,
    max,
    min,
    sum,
    count,
    current_timestamp,
    when,
    unix_timestamp,
    trim,
    upper,
    date_format,
    concat_ws,
    year,
    month,
    dayofmonth,  # ← for S3 partition columns
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# =====================================================
# SPARK SESSION
# =====================================================
spark = (
    SparkSession.builder.appName("CryptoStream-RealTime-Elastic")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.EnvironmentVariableCredentialsProvider",
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# =====================================================
# S3 BASE PATH
# =====================================================
S3_BASE = "s3a://crypto-data-pk/aggregates"  # s3a:// — required for Spark/Hadoop

# =====================================================
# KAFKA SCHEMA
# =====================================================
schema = StructType([
    StructField("product_id",  StringType()),
    StructField("price",       DoubleType()),
    StructField("bid",         DoubleType()),
    StructField("ask",         DoubleType()),
    StructField("volume_24h",  DoubleType()),
    StructField("open_24h",    DoubleType()),   # ← add
    StructField("high_24h",    DoubleType()),   # ← add
    StructField("low_24h",     DoubleType()),   # ← add
    StructField("event_time",  TimestampType()),
])

# =====================================================
# READ FROM KAFKA
# =====================================================
raw_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "crypto_ticks")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

# =====================================================
# PARSE JSON
# =====================================================
parsed_df = (
    raw_df.selectExpr("CAST(value AS STRING) AS json_string")
    .select(from_json(col("json_string"), schema).alias("data"))
    .select("data.*")
    .withColumn("ingestion_time", current_timestamp())
)

# =====================================================
# CLEANING
# =====================================================
cleaned_df = (
    parsed_df.filter(col("product_id").isNotNull())
    .filter(col("price").isNotNull())
    .filter(col("bid").isNotNull())
    .filter(col("ask").isNotNull())
    .filter(col("event_time").isNotNull())
    .filter(col("price") > 0)
    .filter(col("bid") > 0)
    .filter(col("ask") > 0)
    .filter(col("volume_24h") >= 0)
    .filter(col("bid") <= col("ask"))
    .withColumn("product_id", upper(trim(col("product_id"))))
)

# =====================================================
# DEDUPLICATION
# =====================================================
dedup_df = cleaned_df \
    .withWatermark("event_time", "1 minutes") \
    .dropDuplicates(["product_id", "event_time", "price"])

# =====================================================
# ENRICHMENT
# =====================================================
enriched_df = (
    dedup_df.withColumn("spread", col("ask") - col("bid"))
    .withColumn("spread_pct", (col("spread") / col("price")) * 100)
    .withColumn("mid_price", (col("bid") + col("ask")) / 2)
    .withColumn("latency_seconds", unix_timestamp("ingestion_time") - unix_timestamp("event_time"))
    .withColumn(
        "quality_score",
        when((col("latency_seconds") < 5) & (col("spread_pct") < 1), 100)
        .when((col("latency_seconds") < 10) & (col("spread_pct") < 5), 80)
        .when((col("latency_seconds") < 30) & (col("spread_pct") < 10), 50)
        .otherwise(0))
    .withColumn("is_suspicious_spread", col("spread_pct") > 10)
)

# =====================================================
# FINAL STREAM
# =====================================================
final_df = enriched_df.filter(col("quality_score") >= 50).filter(
    col("is_suspicious_spread") == False
)

base_stream_1m = final_df.withWatermark("event_time", "1 minutes")
base_stream_5m = final_df.withWatermark("event_time", "6 minutes")
base_stream_15m = final_df.withWatermark("event_time", "11 minutes")


# =====================================================
# WRITE TO ELASTICSEARCH
# =====================================================
def write_to_es(batch_df, batch_id, index_name):
    batch_df.write.format("org.elasticsearch.spark.sql").option("es.nodes", "elasticsearch").option(
        "es.port", "9200"
    ).option("es.nodes.wan.only", "true").option("es.mapping.id", "doc_id").mode("append").save(
        index_name
    )


# =====================================================
# WRITE TO S3 (Parquet, Hive-partitioned by date)
# =====================================================
# WRITE TO S3 — Delta instead of Parquet
#
# Key changes from original:
# 1. format("delta") replaces .parquet()
# 2. Removed coalesce(1) — Delta manages file compaction
#    via OPTIMIZE command; coalesce hurts parallelism
# 3. Removed isEmpty() check — Delta handles empty
#    micro-batches as no-ops internally, no extra Spark job
# 4. replaceWhere used during backfill (see backfill.py);
#    normal streaming runs use append
#
# Partition layout on S3:
#   s3://crypto-data-pk/aggregates/window=1m/year=2026/month=06/day=27/
#     part-00000-<uuid>.snappy.parquet  ← Delta still uses parquet files
#     _delta_log/                       ← transaction log (ACID guarantee)
def write_to_s3(batch_df, batch_id, window_label):
    (
      batch_df
        .write.format("delta")
        .mode("append")
        .partitionBy("year", "month", "day")
        .save(f"{S3_BASE}/window={window_label}/")
    )


# =====================================================
# WINDOW AGG + DUAL SINK FUNCTION
# =====================================================
def create_window_agg(
    stream_df, window_duration, es_index, checkpoint_dir, trigger_sec, window_label
):
    """
    Aggregates over `window_duration`, then writes each micro-batch to
    both Elasticsearch (hot path) and S3 Parquet (cold path).

    Two separate foreachBatch queries share the same aggregated DataFrame
    but write to independent sinks — ES for sub-second Kibana queries,
    S3 for long-term analytics and historical backtesting.
    """
    agg_df = stream_df.groupBy(col("product_id"), window(col("event_time"), window_duration)).agg(
        avg("price").alias("avg_price"),
        min("price").alias("min_price"),
        max("price").alias("max_price"),
        count("*").alias("tick_count"),
        sum("volume_24h").alias("total_volume"),
    )

    agg_df_es = (
    agg_df
    .withColumn("window_start_ts", date_format(col("window.start"), "yyyy-MM-dd'T'HH:mm:ss"))
    .withColumn("window_end_ts",   date_format(col("window.end"),   "yyyy-MM-dd'T'HH:mm:ss"))
    .withColumn("year",  year(col("window.start")))
    .withColumn("month", month(col("window.start")))
    .withColumn("day",   dayofmonth(col("window.start")))
    .drop("window")
    .withColumn("doc_id", concat_ws("_", col("product_id"), col("window_start_ts")))
)

    # ── Single query, dual sink ──────────────────────────────────────────
    # Write to both ES and S3 in one foreachBatch — halves query count
    # from 6 to 3, significantly reducing memory pressure
    def write_both(df, bid):
        write_to_es(df, bid, es_index)
        write_to_s3(df, bid, window_label)

    query = (
        agg_df_es.writeStream.foreachBatch(write_both)
        .outputMode("update")
        .option("checkpointLocation", checkpoint_dir)
        .trigger(processingTime=trigger_sec)
        .start()
    )

    return query


# =====================================================
# START STREAMS
# =====================================================
q1m = create_window_agg(
    base_stream_1m, "1 minute", "crypto_agg_1m", "/tmp/checkpoints/agg_1m", "2 seconds", "1m"
)
q5m = create_window_agg(
    base_stream_5m, "5 minutes", "crypto_agg_5m", "/tmp/checkpoints/agg_5m", "5 seconds", "5m"
)
q15m = create_window_agg(
    base_stream_15m, "15 minutes", "crypto_agg_15m", "/tmp/checkpoints/agg_15m", "10 seconds", "15m"
)

print("\n Crypto Streaming Pipeline Started")
print("   ES  → crypto_agg_1m / 5m / 15m")
print(f"  S3  → {S3_BASE}/window=1m|5m|15m/\n")

spark.streams.awaitAnyTermination()
