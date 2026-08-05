-- Athena table definitions for the streaming pipeline's S3 output.
-- Run these in the Athena query editor (adjust the S3 bucket name to match
-- your AWS_BUCKET). Both tables are external, Hive-partitioned, and point
-- directly at the Parquet files the Spark job writes -- no data copy needed.

-- Streaming Gold: windowed aggregates. This is the table the "Analytics &
-- Observability" queries and the Grafana dashboard are expected to read from.
CREATE EXTERNAL TABLE IF NOT EXISTS streaming_gold_trades (
    vwap DOUBLE,
    trade_count BIGINT,
    total_volume DOUBLE,
    high DOUBLE,
    low DOUBLE,
    written_at TIMESTAMP,
    window_end TIMESTAMP
)
PARTITIONED BY (
    window_start TIMESTAMP,
    symbol STRING
)
STORED AS PARQUET
LOCATION 's3://YOUR_BUCKET_NAME/streaming/gold/trades/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

-- Streaming Bronze: raw parsed events, pre-validation. Kept queryable mainly
-- for debugging Gold output (see ADR-008's accepted debuggability tradeoff)
-- -- e.g. "was a given window's VWAP wrong because of bad raw input, or a
-- bug in the aggregation logic" is answerable by comparing this table
-- against Gold, without needing a persisted Silver layer.
CREATE EXTERNAL TABLE IF NOT EXISTS streaming_bronze_trades (
    trade_id STRING,
    symbol STRING,
    price DOUBLE,
    size DOUBLE,
    event_time TIMESTAMP,
    producer_ingested_at STRING,
    kafka_ingest_time TIMESTAMP
)
STORED AS PARQUET
LOCATION 's3://YOUR_BUCKET_NAME/streaming/bronze/trades/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

-- After creating streaming_gold_trades, register its partitions:
-- MSCK REPAIR TABLE streaming_gold_trades;

-- Example query: partition pruning demonstration (see README's Analytics &
-- Observability section) -- compare the "Data scanned" stat in the Athena
-- console for this query against a version with the WHERE clause removed.
-- SELECT symbol, window_start, vwap, trade_count
-- FROM streaming_gold_trades
-- WHERE symbol = 'BTC-USD'
--   AND window_start >= TIMESTAMP '2026-08-01 00:00:00'
-- ORDER BY window_start DESC
-- LIMIT 100;