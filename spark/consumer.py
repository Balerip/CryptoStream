from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

spark = SparkSession.builder.appName("CryptoStream").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("product_id", StringType()),
    StructField("price", DoubleType()),
    StructField("bid", DoubleType()),
    StructField("ask", DoubleType()),
    StructField("volume_24h", DoubleType()),
    StructField("event_time", TimestampType())
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "crypto_ticks") \
    .option("startingOffsets", "latest") \
    .load() \
    .selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("d")) \
    .select("d.*")

# 30-second rolling average price
query = df \
    .withWatermark("event_time", "10 seconds")\
    .groupBy(
        col("product_id"),
        window(col("event_time"), "30 seconds", "10 seconds")
    ) \
    .agg(avg("price").alias("avg_price")) \
    .select("product_id", "window.start", "window.end", "avg_price") \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()