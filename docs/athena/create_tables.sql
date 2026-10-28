-- Athena table definitions for the streaming pipeline's S3 output.
-- Replace YOUR_BUCKET_NAME with your AWS_BUCKET (it appears 3 times below).
-- LOCATION must match the paths the Spark job writes to:
--   s3a://{AWS_BUCKET}/gold/trades/   and   s3a://{AWS_BUCKET}/bronze/trades/

-- Streaming Gold: windowed aggregates, Hive-partitioned as
--   gold/trades/dt=YYYY-MM-DD/symbol=BTC-USD/part-....parquet
-- Uses partition projection, so new partitions written by the streaming job
-- are queryable immediately -- no MSCK REPAIR TABLE needed.
CREATE EXTERNAL TABLE IF NOT EXISTS streaming_gold_trades (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    vwap DOUBLE,
    trade_count BIGINT,
    total_volume DOUBLE,
    high DOUBLE,
    low DOUBLE,
    written_at TIMESTAMP
)
PARTITIONED BY (
    dt STRING,
    symbol STRING
)
STORED AS PARQUET
LOCATION 's3://hongskim-crypto-stream/gold/trades/'
TBLPROPERTIES (
    'parquet.compression'        = 'SNAPPY',
    'projection.enabled'         = 'true',
    'projection.dt.type'         = 'date',
    'projection.dt.format'       = 'yyyy-MM-dd',
    'projection.dt.range'        = '2026-08-01,NOW',
    'projection.symbol.type'     = 'enum',
    'projection.symbol.values'   = 'BTC-USD,ETH-USD',
    'storage.location.template' = 's3://hongskim-crypto-stream/gold/trades/dt=${dt}/symbol=${symbol}/'
);

-- Streaming Bronze: raw parsed events, pre-validation. Kept queryable mainly
-- for debugging Gold output (see ADR-008's accepted debuggability tradeoff).
-- (Athena skips Spark's _spark_metadata folder because it starts with "_".)
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
LOCATION 's3://hongskim-crypto-stream/bronze/trades/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

-- Partition pruning demonstration: filter on the partition columns (dt,
-- symbol) to prune, then on window_start for the exact time range. Compare
-- "Data scanned" against the same query with the WHERE clause removed.
-- SELECT symbol, window_start, vwap, trade_count
-- FROM streaming_gold_trades
-- WHERE symbol = 'BTC-USD'
--   AND dt = '2026-08-16'
--   AND window_start >= TIMESTAMP '2026-08-16 22:00:00'
-- ORDER BY window_start DESC
-- LIMIT 100;
