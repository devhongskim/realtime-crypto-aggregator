"""
Unit tests for the Spark transformations in spark_job/streaming_job.py.

Timestamps are always compared as strings (CAST ... AS STRING under the
session's UTC timezone), never as Python datetimes, so results don't depend
on the timezone of the machine running the tests.
"""

import json
from datetime import datetime, timezone

import pytest
from pyspark.sql.functions import col
from pyspark.sql.types import (
    DoubleType, StringType, StructField, StructType, TimestampType, BinaryType,
)

import streaming_job as job

PARSED_SCHEMA = StructType([
    StructField("trade_id", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("size", DoubleType(), True),
    StructField("event_time", TimestampType(), True),
    StructField("producer_ingested_at", StringType(), True),
    StructField("kafka_ingest_time", TimestampType(), True),
])


def utc(hh, mm, ss, day=28):
    """A timezone-aware UTC datetime on 2026-10-<day>."""
    return datetime(2026, 10, day, hh, mm, ss, tzinfo=timezone.utc)


def trade(trade_id, symbol, price, size, event_time):
    return (trade_id, symbol, price, size, event_time, None, None)


def gold_rows(df):
    """Collect Gold output as plain dicts with timestamps rendered as UTC strings."""
    out = df.select(
        col("window_start").cast("string").alias("window_start"),
        col("window_end").cast("string").alias("window_end"),
        "dt", "symbol", "vwap", "trade_count", "total_volume", "high", "low",
    ).orderBy("window_start", "symbol")
    return [r.asDict() for r in out.collect()]


# --- parse_trades ------------------------------------------------------------

RAW_SCHEMA = StructType([
    StructField("key", BinaryType(), True),
    StructField("value", BinaryType(), True),
    StructField("timestamp", TimestampType(), True),
])


def kafka_record(envelope, key=b"BTC-USD"):
    value = envelope if isinstance(envelope, bytes) else json.dumps(envelope).encode()
    return (key, value, utc(0, 0, 2))


def test_parse_trades_casts_coinbase_payload_to_typed_columns(spark):
    envelope = {
        "ingested_at": "2026-10-28T00:00:01.500000+00:00",
        "source": "coinbase_advanced_trade_ws",
        "trade": {
            "trade_id": "123", "product_id": "BTC-USD",
            "price": "67000.50", "size": "0.015",  # Coinbase sends strings
            "side": "BUY", "time": "2026-10-28T00:00:01.123456Z",
        },
    }
    raw = spark.createDataFrame([kafka_record(envelope)], RAW_SCHEMA)

    row = (
        job.parse_trades(raw)
        .select("trade_id", "symbol", "price", "size",
                col("event_time").cast("string").alias("event_time"))
        .first()
    )

    assert row.trade_id == "123"
    assert row.symbol == "BTC-USD"
    assert row.price == pytest.approx(67000.50)
    assert row.size == pytest.approx(0.015)
    assert row.event_time == "2026-10-28 00:00:01.123456"


def test_malformed_message_parses_to_nulls_and_is_removed_by_validation(spark):
    raw = spark.createDataFrame([kafka_record(b"{not valid json")], RAW_SCHEMA)

    parsed = job.parse_trades(raw)
    row = parsed.first()

    assert row.trade_id is None and row.event_time is None  # no exception raised
    assert job.validate_trades(parsed).count() == 0


# --- validate_trades ---------------------------------------------------------

def test_validation_keeps_only_well_formed_allow_listed_trades(spark):
    t = utc(0, 0, 1)
    rows = [
        trade("good", "BTC-USD", 100.0, 1.0, t),
        trade(None, "BTC-USD", 100.0, 1.0, t),      # missing trade_id
        trade("2", None, 100.0, 1.0, t),            # missing symbol
        trade("3", "BTC-USD", None, 1.0, t),        # missing price
        trade("4", "BTC-USD", 0.0, 1.0, t),         # non-positive price
        trade("5", "BTC-USD", 100.0, -2.0, t),      # non-positive size
        trade("6", "BTC-USD", 100.0, 1.0, None),    # missing event time
        trade("7", "DOGE-USD", 0.1, 50.0, t),       # symbol not allow-listed
    ]
    df = spark.createDataFrame(rows, PARSED_SCHEMA)

    result = [r.trade_id for r in job.validate_trades(df).collect()]

    assert result == ["good"]


# --- compute_windowed_gold ---------------------------------------------------

def test_windowed_aggregates_vwap_volume_high_low_per_symbol(spark):
    rows = [
        trade("1", "BTC-USD", 100.0, 1.0, utc(0, 0, 1)),
        trade("2", "BTC-USD", 200.0, 3.0, utc(0, 0, 9)),
        trade("3", "ETH-USD", 2500.0, 2.0, utc(0, 0, 5)),
    ]
    df = spark.createDataFrame(rows, PARSED_SCHEMA)

    result = gold_rows(job.compute_windowed_gold(df))

    assert len(result) == 2
    btc, eth = result
    assert btc["symbol"] == "BTC-USD"
    # VWAP = sum(price * size) / sum(size) = (100*1 + 200*3) / 4 = 175
    assert btc["vwap"] == pytest.approx(175.0)
    assert btc["trade_count"] == 2
    assert btc["total_volume"] == pytest.approx(4.0)
    assert (btc["high"], btc["low"]) == (200.0, 100.0)
    assert eth["symbol"] == "ETH-USD"
    assert eth["vwap"] == pytest.approx(2500.0)


def test_windows_are_10s_tumbling_with_utc_dt_partition(spark):
    rows = [
        trade("a", "BTC-USD", 100.0, 1.0, utc(23, 59, 55, day=27)),  # just before midnight UTC
        trade("b", "BTC-USD", 100.0, 1.0, utc(0, 0, 0)),             # exactly on a boundary
        trade("c", "BTC-USD", 100.0, 1.0, utc(0, 0, 10)),            # next window's start
    ]
    df = spark.createDataFrame(rows, PARSED_SCHEMA)

    result = [(r["window_start"], r["window_end"], r["dt"], r["trade_count"])
              for r in gold_rows(job.compute_windowed_gold(df))]

    assert result == [
        ("2026-10-27 23:59:50", "2026-10-28 00:00:00", "2026-10-27", 1),
        ("2026-10-28 00:00:00", "2026-10-28 00:00:10", "2026-10-28", 1),  # start inclusive
        ("2026-10-28 00:00:10", "2026-10-28 00:00:20", "2026-10-28", 1),
    ]


# --- end-to-end streaming behaviour (watermark + dedup state) ----------------

def test_streaming_watermark_drops_late_events_and_keeps_in_bound_ones(spark, tmp_path):
    """
    Runs validate_and_dedupe -> compute_windowed_gold as a real Structured
    Streaming query, one micro-batch per input file, to verify the 30-second
    watermark (ADR-004) and trade_id deduplication (within and across batches;
    dropDuplicatesWithinWatermark only runs on streaming DataFrames):

      batch 1: trades at 00:00:01 and 00:00:12, plus a duplicate of the first
      batch 2: trade at 00:00:45            -> watermark advances to 00:00:15
      batch 3: a late trade at 00:00:05     -> older than the watermark: dropped
               a late trade at 00:00:16     -> within the watermark: counted
               a replay of the 00:00:45 trade (same trade_id): deduplicated
      batch 4: trade at 00:01:30            -> watermark advances to 00:01:00
      batch 5: trade at 00:01:31            -> finalizes windows ending <= 00:01:00
    """
    input_dir = tmp_path / "input"
    input_dir.mkdir()

    def write_batch(n, trades):
        lines = [
            json.dumps({"trade_id": tid, "symbol": "BTC-USD", "price": 100.0,
                        "size": 1.0, "event_time": f"2026-10-28T{t}Z"})
            for tid, t in trades
        ]
        (input_dir / f"batch_{n:02d}.json").write_text("\n".join(lines) + "\n")

    stream = (
        spark.readStream
        .schema(PARSED_SCHEMA)
        .option("maxFilesPerTrigger", 1)
        .json(str(input_dir))
    )
    gold = job.compute_windowed_gold(job.validate_and_dedupe(stream))
    query = (
        gold.writeStream
        .format("memory")
        .queryName("gold_watermark_test")
        .outputMode("append")
        .option("checkpointLocation", str(tmp_path / "checkpoint"))
        .start()
    )
    try:
        batches = [
            [("t1", "00:00:01"), ("t2", "00:00:12"), ("t1", "00:00:01")],
            [("t3", "00:00:45")],
            [("late_dropped", "00:00:05"), ("late_kept", "00:00:16"), ("t3", "00:00:45")],
            [("t4", "00:01:30")],
            [("t5", "00:01:31")],
        ]
        for n, batch in enumerate(batches, start=1):
            write_batch(n, batch)
            query.processAllAvailable()
    finally:
        query.stop()

    result = {
        r["window_start"]: r["trade_count"]
        for r in gold_rows(spark.table("gold_watermark_test"))
    }

    assert result == {
        "2026-10-28 00:00:00": 1,  # t1 once (duplicate removed); late_dropped arrived after the watermark passed
        "2026-10-28 00:00:10": 2,  # t2 + late_kept (arrived late, but within the watermark)
        "2026-10-28 00:00:40": 1,  # t3 counted once despite the replay
        # 00:01:30 window not emitted yet: still within the watermark, as designed
    }
