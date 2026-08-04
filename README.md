# Real-Time Crypto Trade Aggregator

A streaming data pipeline that ingests live cryptocurrency trade data, computes rolling windowed analytics, and writes queryable output to a data lake — the streaming counterpart to [stock-data-pipeline](https://github.com/devhongskim/stock-data-pipeline), which covers the batch side of the same underlying problem.

## 📚 Additional Documentation

- **[ADR.md](ADR.md)** — Architecture Decision Records documenting the reasoning and tradeoffs behind every major technology choice in this project.
- **[docs/validation.md](docs/validation.md)** — Planned validation scenarios for confirming the pipeline actually meets its requirements (restart-safety, watermark behavior, ordering) once built.

## Why this project exists
[stock-data-pipeline](https://github.com/devhongskim/stock-data-pipeline) processes one clean, complete daily snapshot, with the luxury of retries and full reprocessing whenever needed. This project answers the opposite problem: data never stops arriving, there's no "end of batch" to wait for, and a late or out-of-order event has to be handled *while the stream keeps moving*. Windowed aggregation, watermarking, and restart-safe checkpointing are the concepts this project exists to demonstrate — the deliberate other half of a complete data engineering skill set.

## Requirements

| # | Requirement | Why it matters |
|---|---|---|
| 1 | Ingest live trade data continuously | Core premise of the project — no daily batch window to hide behind |
| 2 | Preserve ordering within each trading pair | Rolling metrics (VWAP, high/low) are only correct if trades for a given symbol are processed in the order they occurred |
| 3 | Continue processing after consumer restarts | A crash or redeploy must not silently drop data or double-count it |
| 4 | Produce 10-second rolling analytics | The concrete, demonstrable output — proves windowing works, not just ingestion |
| 5 | Support late-arriving events | Network jitter is guaranteed at this granularity; the system must have an explicit, bounded policy for "how late is too late" |
| 6 | Write analytics in a format suitable for downstream querying | The pipeline's value is in what's queryable afterward, not just in having processed something |

Every architectural decision in this project traces back to one of these six requirements — see [ADR.md](ADR.md) for the full reasoning behind each choice.

## Architecture (current scope)

```
Coinbase WS → Kafka → Spark Structured Streaming → S3 (streaming/trades/)
```

## Architecture (planned expansion — analytics layer)

```
S3 (streaming/trades/) → Athena → Power BI / dashboard
```

## Architecture (final, unified — batch + streaming converge)

Two independently-built pipelines — one batch, one streaming — converging on a shared query layer and a shared BI layer. This is a realistic "lakehouse" pattern: once an organization has both batch and streaming sources, both typically end up feeding the same warehouse/query layer rather than living as permanently separate systems.

```
                    Batch Side
Polygon API
     │
     ▼
Bronze (S3)
     │
     ▼
Silver (DuckDB + S3)
     │
     ▼
Gold (DuckDB + S3)
     │
     ▼
Athena ──────────────┐
                      │
                Streaming Side          │
Coinbase WS                             │
     │                                  │
     ▼                                  │
Kafka                                   │
     │                                  │
     ▼                                  │
Spark Structured Streaming              │
     │                                  │
     ▼                                  │
Streaming Bronze (S3)                   │
     │                                  │
     ▼                                  │
Streaming Silver (validated events)     │
     │                                  │
     ▼                                  │
Streaming Gold (windowed metrics)       │
     │                                  │
     ▼                                  │
Athena ──────────────────────────────────┘
                      │
                      ▼
                  Power BI
```

**Streaming Bronze** — raw trade events landed as-is from Kafka, pre-validation (mirrors the batch project's Bronze: immutable, minimally transformed).
**Streaming Silver** — validated and deduplicated trade events (schema/null checks, duplicate `trade_id` removal) prior to windowing — conceptually mirrors the batch project's `validate_bronze_data` gate, kept intentionally thin since streaming validation has to happen inline rather than as a separate batch step.
**Streaming Gold** — windowed rolling analytics (VWAP, trade count, volume, high/low per 10-second window) — the business-ready output (mirrors the batch project's Gold).

## Components

**1. Data source — Coinbase Advanced Trade public WebSocket** (`wss://advanced-trade-ws.coinbase.com`)
No API key, no auth required for most channels — genuinely live trade data for pairs like `BTC-USD`, `ETH-USD`. A real production-grade data source, not a toy simulation.

**2. Producer** — a lightweight Python service (`websockets` client) that subscribes to the market-trades channel and republishes each trade event onto a Kafka topic (`raw_trades`), keeping the ingestion boundary intentionally thin (see [ADR-007](ADR.md#adr-007-why-kafka-as-an-ingestion-boundary-instead-of-spark-reading-coinbase-directly)).

**3. Kafka** — local via Docker Compose (KRaft mode, no Zookeeper). `raw_trades` topic partitioned by symbol to preserve per-symbol ordering (see [ADR-001](ADR.md#adr-001-why-kafka-instead-of-redis-streams)).

**4. Stream processor — PySpark Structured Streaming**, local mode (`local[*]`, no cluster):
- Reads from Kafka via the `kafka` source connector
- Parses trade events (symbol, price, size, trade timestamp)
- 10-second **tumbling window** aggregation on **event time** (see [ADR-003](ADR.md#adr-003-why-event-time-windows-not-processing-time)), computing VWAP, trade count, total volume, high/low
- 30-second **watermark** bounds how late an event can arrive and still count (see [ADR-004](ADR.md#adr-004-why-watermark--30-seconds))
- Writes completed windows to S3 as Parquet via `foreachBatch`, checkpointed for restart-safety

**5. Sink** — same S3 bucket as the batch project, new prefix: `streaming/trades/window_start=<timestamp>/symbol=<symbol>/`.

## ⚠️ Implementation Notes (heads-up for later)

- **Coinbase subscription payload**: Unlike the deprecated legacy feed, the Advanced Trade WebSocket requires an explicit JSON `subscribe` message specifying `product_ids` (e.g. `["BTC-USD", "ETH-USD"]`) and `channel: "market_trades"` after the connection opens — trades won't start flowing just by connecting. Confirm the exact current message schema against Coinbase's live docs when writing the producer, since API details can shift independently of this README.
- **PySpark's Kafka connector isn't bundled by default**: `SparkSession` needs the `spark-sql-kafka-0-10` package passed explicitly at startup (e.g. via `.config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:<version>")`, matched to the installed Spark/Scala version) — omitting this produces a `ClassNotFoundException` at runtime, not at job submission, which can be a confusing first debugging session if it isn't expected.
- **S3 output commits inside `foreachBatch`**: S3 has been strongly read-after-write consistent since late 2020, so "eventual consistency" isn't the actual risk today — but Spark's classic file-commit protocols (`FileOutputCommitter` v1/v2) were originally designed around HDFS's atomic rename, which S3 doesn't have (S3 "renames" are really copy+delete). Under concurrent writers or task retries this can still produce partial or duplicate output files. Mitigation: let Spark's own DataFrameWriter handle commits inside `foreachBatch` rather than hand-rolling file I/O, and consider an S3-aware committer (or a transactional format like Delta Lake) if this becomes a real problem in practice rather than a theoretical one.
- **Local checkpoint directory is fragile across schema/logic changes**: Spark Structured Streaming checkpoints encode state tied to the query's current schema and structure — changing the windowing logic or event schema between runs can make an existing local checkpoint (e.g. `./checkpoint/`) incompatible, causing a hard startup error rather than a graceful migration. Add `checkpoint/` to `.gitignore` immediately, and treat it as disposable: wipe it whenever windowing/schema logic changes during development.

## Tech Stack
| Category | Technology |
| :--- | :--- |
| **Language** | Python |
| **Data Source** | Coinbase Advanced Trade WebSocket |
| **Message Broker** | Apache Kafka (KRaft mode, via Docker Compose) |
| **Stream Processing** | PySpark Structured Streaming |
| **Storage** | AWS S3 (Parquet, Hive-partitioned) |
| **Query Layer (planned)** | AWS Athena |
| **BI (planned)** | Power BI |

## Scaling Considerations
- **Current**: single/small symbol set, `local[*]` Spark, single Kafka broker — appropriate for portfolio scale.
- **At scale**: multiple Kafka partitions per symbol for parallelism; a real Spark cluster (EMR/Databricks) instead of local mode; Kafka Connect or a managed sink instead of hand-rolled `foreachBatch` writes.

## 🚀 Roadmap

### Phase 1 — Ingestion
- [ ] Kafka infrastructure (Docker Compose, KRaft mode)
- [ ] Coinbase producer
- [ ] End-to-end event flow validation

### Phase 2 — Stream Processing
- [ ] Windowed aggregations
- [ ] Watermark handling
- [ ] Checkpoint recovery testing

### Phase 3 — Analytics
- [ ] Athena tables
- [ ] Power BI dashboard

### Phase 4 — Production Hardening
- [ ] Automated tests
- [ ] CI pipeline
