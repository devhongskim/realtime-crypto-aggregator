"""
PySpark Structured Streaming job.

Reads raw trade events from Kafka (`raw_trades`), validates and deduplicates
them in-memory (logical "Silver" step -- see ADR-008, not materialized to
S3), computes 10-second tumbling-window VWAP/volume/high/low aggregates on
event time with a 30-second watermark (ADR-003, ADR-004), and writes
finalized windows to S3 as Hive-partitioned Parquet via foreachBatch.

Run with (adjust versions to match your installed Spark/Scala/Hadoop):

    spark-submit \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,\\
org.apache.hadoop:hadoop-aws:3.3.4,\\
com.amazonaws:aws-java-sdk-bundle:1.12.262 \\
        spark_job/streaming_job.py

Required environment variables: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
AWS_BUCKET, AWS_REGION, KAFKA_BOOTSTRAP_SERVERS (defaults to localhost:9092).
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, avg, count, sum as spark_sum,
    max as spark_max, min as spark_min, current_timestamp, to_timestamp,
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
)

# --- Configuration -----------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = "raw_trades"

AWS_BUCKET = os.environ["AWS_BUCKET"]  # fail fast if not set -- no silent misconfiguration
AWS_REGION = os.getenv("AWS_REGION", "us-east-2")

S3_BRONZE_PATH = f"s3a://{AWS_BUCKET}/bronze/trades/"
S3_GOLD_PATH = f"s3a://{AWS_BUCKET}/gold/trades/"
CHECKPOINT_LOCATION = "./checkpoint/streaming_job/"  # gitignored, disposable -- see README

ALLOWED_SYMBOLS = ["BTC-USD", "ETH-USD"]

WINDOW_DURATION = "10 seconds"
WATERMARK_DELAY = "30 seconds"


# --- Schema --------------------------------------------------------------
# Matches the envelope shape produced by producer/coinbase_producer.py:
# {"ingested_at": ..., "source": ..., "trade": {trade_id, product_id, price, size, side, time}}
# NOTE: verify field names against real captured traffic once the producer
# has actually run -- Coinbase's schema is inferred from docs, not yet
# confirmed against live payloads. Adjust here if real events differ.

TRADE_SCHEMA = StructType([
    StructField("trade_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("price", StringType(), True),   # Coinbase sends prices as strings; cast below
    StructField("size", StringType(), True),
    StructField("side", StringType(), True),
    StructField("time", StringType(), True),    # ISO8601 event timestamp
])

ENVELOPE_SCHEMA = StructType([
    StructField("ingested_at", StringType(), True),
    StructField("source", StringType(), True),
    StructField("trade", TRADE_SCHEMA, True),
])


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("crypto-trade-aggregator")
        .master("local[*]")
        .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
        .config("spark.hadoop.fs.s3a.endpoint.region", AWS_REGION)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def read_raw_trades(spark: SparkSession):
    """Read from Kafka and parse the JSON envelope into structured columns."""
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed = raw.select(
        col("key").cast("string").alias("kafka_key"),
        from_json(col("value").cast("string"), ENVELOPE_SCHEMA).alias("envelope"),
        col("timestamp").alias("kafka_ingest_time"),
    )

    return parsed.select(
        col("envelope.trade.trade_id").alias("trade_id"),
        col("envelope.trade.product_id").alias("symbol"),
        col("envelope.trade.price").cast(DoubleType()).alias("price"),
        col("envelope.trade.size").cast(DoubleType()).alias("size"),
        to_timestamp(col("envelope.trade.time")).alias("event_time"),
        col("envelope.ingested_at").alias("producer_ingested_at"),
        col("kafka_ingest_time"),
    )


def validate_and_dedupe(df):
    """
    Logical Streaming Silver step (ADR-008) -- schema/null validation,
    symbol allow-listing, and trade_id deduplication, all in-memory.
    Not written to S3 as its own layer; see ADR-008 for why.
    """
    validated = (
        df
        .filter(col("trade_id").isNotNull())
        .filter(col("symbol").isNotNull())
        .filter(col("price").isNotNull() & (col("price") > 0))
        .filter(col("size").isNotNull() & (col("size") > 0))
        .filter(col("event_time").isNotNull())
        .filter(col("symbol").isin(ALLOWED_SYMBOLS))
    )

    # Watermark must be applied before dropDuplicatesWithinWatermark so Spark
    # knows how long to retain dedup state before safely discarding it.
    deduped = (
        validated
        .withWatermark("event_time", WATERMARK_DELAY)
        .dropDuplicatesWithinWatermark(["trade_id"])
    )

    return deduped


def write_streaming_bronze(df):
    """
    Persist the raw parsed events (pre-validation) as Streaming Bronze --
    durable once this write completes; Kafka is the buffer before that
    point (see ADR-008's corrected framing of Bronze durability).
    """
    return (
        df.writeStream
        .format("parquet")
        .option("path", S3_BRONZE_PATH)
        .option("checkpointLocation", CHECKPOINT_LOCATION + "bronze/")
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )


def compute_windowed_gold(df):
    """10-second tumbling window aggregation on event time (ADR-003)."""
    return (
        df
        .groupBy(
            window(col("event_time"), WINDOW_DURATION),
            col("symbol"),
        )
        .agg(
            (spark_sum(col("price") * col("size")) / spark_sum(col("size"))).alias("vwap"),
            count("*").alias("trade_count"),
            spark_sum("size").alias("total_volume"),
            spark_max("price").alias("high"),
            spark_min("price").alias("low"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("symbol"),
            col("vwap"),
            col("trade_count"),
            col("total_volume"),
            col("high"),
            col("low"),
            current_timestamp().alias("written_at"),  # supports Scenario 7 latency measurement
        )
    )


def write_batch_to_gold_s3(batch_df, batch_id: int) -> None:
    """
    foreachBatch sink -- lets Spark's own DataFrameWriter own the commit
    (rather than hand-rolled file I/O), sidestepping the S3 commit-protocol
    edge cases noted in the README's Implementation Notes. Hive-style
    partitioning by window_start/symbol makes this directly Athena-friendly.
    """
    (
        batch_df.write
        .mode("append")
        .partitionBy("window_start", "symbol")
        .parquet(S3_GOLD_PATH)
    )


def main() -> None:
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Attempting to connect to Kafka stream...")
    raw_trades = read_raw_trades(spark)
    print("Successfully connected to Kafka!")

    # Streaming Bronze: durable raw layer, written independently of validation.
    bronze_query = write_streaming_bronze(raw_trades)

    # Logical Silver -> windowed Gold -> S3.
    validated = validate_and_dedupe(raw_trades)
    gold = compute_windowed_gold(validated)

    gold_query = (
        gold.writeStream
        .foreachBatch(write_batch_to_gold_s3)
        .option("checkpointLocation", CHECKPOINT_LOCATION + "gold/")
        .outputMode("append")  # only emits windows once the watermark has finalized them
        .trigger(processingTime="10 seconds")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()