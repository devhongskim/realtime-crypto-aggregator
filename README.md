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
Coinbase WS → Kafka → Spark Structured Streaming → S3 (bronze/trades/, gold/trades/)
```

## Architecture (planned expansion — analytics layer)

```
S3 (bronze/trades/, gold/trades/) → Athena → Grafana (pipeline observability + trend dashboards)
```

## Architecture (final, unified — batch + streaming converge)

Two independently-built pipelines — one batch, one streaming — each with its own dedicated S3 bucket, converging on a shared query layer (Athena) and a shared BI layer (Grafana). This is a realistic "lakehouse" pattern: Athena can query and join tables across separate S3 buckets without issue, so convergence happens at the catalog/query layer, not by forcing both pipelines into shared storage -- which mirrors how most real organizations actually separate storage by domain/team while unifying access at the query layer.

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
[validate + dedupe — logical step,       │
 not materialized separately, see ADR-008]│
     │                                  │
     ▼                                  │
Streaming Gold (windowed metrics)       │
     │                                  │
     ▼                                  │
Athena ──────────────────────────────────┘
                      │
                      ▼
                  Grafana
```

**Streaming Bronze** — raw trade events landed in S3 via Kafka, which durably buffers events between ingestion and persistence (mirrors the batch project's Bronze: immutable once written, minimally transformed).
**Validation & deduplication** — schema/null checks and duplicate `trade_id` removal, applied as an in-memory transformation within the same Spark job — a logical step, not a separately materialized S3 layer (see [ADR-008](ADR.md#adr-008-why-streaming-silver-is-not-materialized-separately)).
**Streaming Gold** — windowed rolling analytics (VWAP, trade count, volume, high/low per 10-second window) — the business-ready output (mirrors the batch project's Gold).

## Components

**1. Data source — Coinbase Advanced Trade public WebSocket** (`wss://advanced-trade-ws.coinbase.com`)
No API key, no auth required for most channels — genuinely live trade data for pairs like `BTC-USD`, `ETH-USD`. A real production-grade data source, not a toy simulation.

**2. Producer** — a lightweight Python service (`websockets` client) that subscribes to the market-trades channel and republishes each trade event onto a Kafka topic (`raw_trades`), keeping the ingestion boundary intentionally thin (see [ADR-007](ADR.md#adr-007-why-kafka-as-an-ingestion-boundary-instead-of-spark-reading-coinbase-directly)).

**3. Kafka** — local via Docker Compose (KRaft mode, no Zookeeper). `raw_trades` topic partitioned by symbol to preserve per-symbol ordering (see [ADR-001](ADR.md#adr-001-why-kafka-instead-of-redis-streams)).

**4. Stream processor — PySpark Structured Streaming**, local mode (`local[*]`, no cluster):
- Reads from Kafka via the `kafka` source connector
- Parses trade events (symbol, price, size, trade timestamp)
- Validates and deduplicates events in-memory (schema/null checks, duplicate `trade_id` removal) — a logical transformation step, not a separately materialized S3 layer (see [ADR-008](ADR.md#adr-008-why-streaming-silver-is-not-materialized-separately))
- 10-second **tumbling window** aggregation on **event time** (see [ADR-003](ADR.md#adr-003-why-event-time-windows-not-processing-time)), computing VWAP, trade count, total volume, high/low
- 30-second **watermark** bounds how late an event can arrive and still count (see [ADR-004](ADR.md#adr-004-why-watermark--30-seconds))
- Writes completed windows to S3 as Parquet via `foreachBatch`, checkpointed for restart-safety

**5. Sink** — a dedicated S3 bucket for this project (`hongskim-crypto-stream`, provisioned via Terraform -- see below), separate from the batch project's bucket. Convergence between the two pipelines happens at the query layer (Athena, which can join tables across different S3 buckets without issue), not at the storage layer -- this keeps blast radius isolated per project and mirrors how most organizations actually separate storage by domain/team, unifying only at the catalog/query layer. Paths: `bronze/trades/` (raw events) and `gold/trades/dt=<yyyy-MM-dd>/symbol=<symbol>/` (windowed aggregates, Hive-partitioned by UTC date and symbol, see [ADR-009](ADR.md#adr-009-why-partition-gold-by-day-instead-of-by-window-timestamp)).

## ⚠️ Implementation Notes (heads-up for later)

- **Coinbase subscription payload**: Unlike the deprecated legacy feed, the Advanced Trade WebSocket requires an explicit JSON `subscribe` message specifying `product_ids` (e.g. `["BTC-USD", "ETH-USD"]`) and `channel: "market_trades"` after the connection opens — trades won't start flowing just by connecting. Confirm the exact current message schema against Coinbase's live docs when writing the producer, since API details can shift independently of this README.
- **PySpark's Kafka connector isn't bundled by default**: `SparkSession` needs the `spark-sql-kafka-0-10` package passed explicitly at startup (e.g. via `.config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:<version>")`, matched to the installed Spark/Scala version) — omitting this produces a `ClassNotFoundException` at runtime, not at job submission, which can be a confusing first debugging session if it isn't expected.
- **S3 output commits inside `foreachBatch`**: S3 has been strongly read-after-write consistent since late 2020, so "eventual consistency" isn't the actual risk today — but Spark's classic file-commit protocols (`FileOutputCommitter` v1/v2) were originally designed around HDFS's atomic rename, which S3 doesn't have (S3 "renames" are really copy+delete). Under concurrent writers or task retries this can still produce partial or duplicate output files. Mitigation: let Spark's own DataFrameWriter handle commits inside `foreachBatch` rather than hand-rolling file I/O, and consider an S3-aware committer (or a transactional format like Delta Lake) if this becomes a real problem in practice rather than a theoretical one.
- **Local checkpoint directory is fragile across schema/logic changes**: Spark Structured Streaming checkpoints encode state tied to the query's current schema and structure — changing the windowing logic or event schema between runs can make an existing local checkpoint (e.g. `./checkpoint/`) incompatible, causing a hard startup error rather than a graceful migration. Add `checkpoint/` to `.gitignore` immediately, and treat it as disposable: wipe it whenever windowing/schema logic changes during development.
- **Partition values must be path-safe**: Spark URL-escapes characters like `:` when it builds partition directories, so partitioning by a raw timestamp string (`2026-08-07 18:36:10`) produces paths like `22%3A25%3A50` that Athena can't map back to a timestamp. This project originally hit exactly that, plus a second problem: one partition per 10-second window is far too granular. **The fix:** Gold is partitioned by a colon-free UTC date (`dt=yyyy-MM-dd`) and `symbol`, with the full `window_start` kept as a regular `TIMESTAMP` column, and registered via Athena partition projection. See [ADR-009](ADR.md#adr-009-why-partition-gold-by-day-instead-of-by-window-timestamp) for the full reasoning.

## Tech Stack
| Category | Technology |
| :--- | :--- |
| **Language** | Python |
| **Data Source** | Coinbase Advanced Trade WebSocket |
| **Message Broker** | Apache Kafka (KRaft mode, via Docker Compose) |
| **Stream Processing** | PySpark Structured Streaming |
| **Storage** | AWS S3 (Parquet, Hive-partitioned) |
| **Query Layer** | AWS Athena (partition projection) |
| **Observability / BI (planned)** | Grafana |

## Scaling Considerations
- **Current**: single/small symbol set, `local[*]` Spark, single Kafka broker — appropriate for portfolio scale.
- **At scale**: multiple Kafka partitions per symbol for parallelism; a real Spark cluster (EMR/Databricks) instead of local mode; Kafka Connect or a managed sink instead of hand-rolled `foreachBatch` writes; an S3-aware committer (Hadoop S3A committers) or a table format like Delta Lake/Iceberg instead of `FileOutputCommitter`'s copy-based commits; a daily compaction job to merge each `dt=` partition's many small micro-batch files.

## 📦 How to Run (Phase 0 — Provision Infrastructure)

Unlike the batch project's bucket (imported from pre-existing infrastructure), this bucket doesn't exist yet -- Terraform creates it from scratch:

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars   # confirm bucket_name is available/unique
terraform init
terraform plan
terraform apply
```

## 📦 How to Run (Phase 1 — Ingestion)

Only ingestion is built so far (Kafka + producer) — Spark processing, Athena, and Grafana come in later phases.

1. **Start Kafka** (KRaft mode, single broker, via Docker Compose):
   ```bash
   docker compose up -d
   ```
   This also auto-creates the `raw_trades` topic (3 partitions) via the `kafka-init` one-shot container.

2. **Install producer dependencies**:
   ```bash
   cd producer
   pip install -r requirements.txt
   ```

3. **Configure environment**:
   ```bash
   cp .env.example .env
   ```
   Default values work out of the box for local Kafka.

4. **Run the producer**:
   ```bash
   python coinbase_producer.py
   ```
   It connects to Coinbase's public `market_trades` channel for `BTC-USD` and `ETH-USD`, and publishes each trade event onto the `raw_trades` Kafka topic, keyed by symbol.

5. **Verify events are flowing** (from inside the Kafka container, or via any local Kafka CLI):
   ```bash
   docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic raw_trades --from-beginning
   ```

6. **Set AWS credentials for the Spark job** (in `.env`, alongside the Kafka config):
   ```
   AWS_ACCESS_KEY_ID=...
   AWS_SECRET_ACCESS_KEY=...
   AWS_BUCKET=hongskim-crypto-stream
   AWS_REGION=us-east-2
   ```

## 📦 How to Run (Phase 2 — Stream Processing)

With Kafka and the producer running (Phase 1), start the Spark job in a separate terminal:

```bash
cd spark_job
pip install -r requirements.txt
export $(cat ../.env | xargs)   # or otherwise load the same env vars used by the producer

spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
    streaming_job.py
```

Adjust the `--packages` versions to match your installed Spark/Hadoop if they differ from 3.5.1/3.3.4. Streaming Bronze lands continuously in `s3://<bucket>/bronze/trades/`; Streaming Gold (windowed aggregates) lands in `s3://<bucket>/gold/trades/` once each 10-second window clears the 30-second watermark.

## 📦 How to Run (Phase 3 — Analytics)

1. In the Athena query editor, run each `CREATE EXTERNAL TABLE` statement in `athena/create_tables.sql` separately (replace `YOUR_BUCKET_NAME` first, since it appears in both `LOCATION`s and the gold table's `storage.location.template`).
2. No partition registration step is needed: `streaming_gold_trades` uses partition projection, so new `dt=`/`symbol=` partitions are queryable as soon as Spark writes them (no `MSCK REPAIR TABLE`). Always filter on `dt` (UTC date) to get pruning:
   ```sql
   SELECT * FROM streaming_gold_trades WHERE dt = '2026-10-28' AND symbol = 'BTC-USD' LIMIT 10;
   ```
3. Grafana dashboard setup: not yet built (see Roadmap).

## Analytics & Observability (planned)

This project isn't a crypto-trading analysis tool — the trade data is a realistic, free, genuinely live payload chosen to demonstrate streaming DE fundamentals, not a domain the author has trading opinions about. Accordingly, Phase 3's deliverables are deliberately framed around **proving the pipeline works and is observable**, not around generating market insights:

- **Athena — partition pruning, demonstrated, not just claimed**: a query filtering on the partition columns `dt` and `symbol`, with the data-scanned/cost difference shown against an unfiltered equivalent — proof the Hive-style partitioning decision (see [ADR-009](ADR.md#adr-009-why-partition-gold-by-day-instead-of-by-window-timestamp)) actually pays off, not just a diagram claiming it does.
- **Athena — a query joining the batch and streaming Gold outputs** in one query, the concrete payoff of the "unified lakehouse" architecture this project was designed around from the start, rather than leaving that convergence as a diagram that was never actually exercised.
- **Grafana — pipeline health panels**, not trading charts:
  - Watermark-dropped-event count over time (makes [ADR-004](ADR.md#adr-004-why-watermark--30-seconds)'s tradeoff measurable, not just theoretical)
  - End-to-end lag (event time → S3 write time)
  - Records processed per window / Kafka consumer lag
  - A rolling VWAP/volume chart per symbol, included as proof the windowed computation itself is correct — not framed as a trading signal

## 🚀 Roadmap

### Phase 1 — Ingestion
- [x] Kafka infrastructure (Docker Compose, KRaft mode)
- [x] Coinbase producer
- [x] End-to-end event flow validation

### Phase 2 — Stream Processing
- [x] Windowed aggregations
- [x] Watermark handling
- [x] Checkpoint recovery testing

### Phase 3 — Analytics & Observability
- [x] Athena tables over `bronze/trades/` and `gold/trades/`
- [x] Athena partition projection on `dt`/`symbol` (no `MSCK REPAIR` needed)
- [x] Athena query demonstrating partition pruning (with scanned-data comparison)
- [x] Athena query joining batch + streaming Gold outputs
- [ ] Grafana dashboard: pipeline health panels (watermark drops, end-to-end lag, consumer lag)
- [ ] Grafana panel: rolling VWAP/volume (correctness proof, not a trading signal)

### Phase 4 — Production Hardening
- [ ] Automated tests
- [ ] CI pipeline

## Proof of Work: Athena Partition Pruning

Streaming Gold is Hive-partitioned by UTC date and symbol (`gold/trades/dt=.../symbol=.../`) and registered via Athena partition projection (see [ADR-009](ADR.md#adr-009-why-partition-gold-by-day-instead-of-by-window-timestamp)). Filtering on the partition columns lets Athena skip non-matching S3 folders entirely:

| Query | Data scanned |
|---|---|
| No partition filter | 4.62 KB |
| `WHERE dt = '2026-10-28' AND symbol = 'BTC-USD'` | **2.15 KB (−53%)** |
<img width="2039" height="639" alt="athena_unpruned_query" src="https://github.com/user-attachments/assets/9ae4200c-483e-49c9-90aa-ff9cf5b89dfd" />
<img width="2041" height="646" alt="athena_pruned_query" src="https://github.com/user-attachments/assets/eba20c10-12b0-4c71-9dce-565564bdc156" />
Measured on a single day of data, where the entire saving comes from skipping the other symbol's folder. The gap widens with every additional day of history: a one-day query against 30 days of data reads roughly 1/60th of the table.

## Proof of Work: Batch + Streaming Join in One Athena Query

The payoff of the "unified lakehouse" design: one Athena query reading from **two independently built pipelines in two separate S3 buckets**. Streaming Gold (`hongskim-crypto-stream`, this project) and batch Gold from [stock-data-pipeline](https://github.com/devhongskim/stock-data-pipeline) (`hongskim-stock-pipeline`), each registered as its own partition-projected table and converging only at the query layer.

The two datasets don't share dates yet (batch history covers Jul 1 – Aug 7, 2026; streaming data starts Oct 28), so instead of a date-matched join the query compares **BTC's move so far today** against **each stock's typical trading day**, using identical metric definitions on both sides: return = (close − open) / open, volatility = (high − low) / open. Both sides filter on their partition columns, so each table reads only the folders it needs.

- **Streaming side**: today's 10-second BTC-USD windows reduced to one summary row (first/last window VWAP as open/close proxies, overall high/low)
- **Batch side**: daily metrics averaged per ticker across 20–27 trading days
- **`CROSS JOIN`**: attaches the single BTC summary row to every ticker. There is no shared key, by design.

Full query: [`athena/batch_streaming_join.sql`](athena/batch_streaming_join.sql)

<!-- TODO: replace src with the GitHub upload link for the screenshot -->
<img alt="crypto_and_stock_join" src="CRYPTO_STOCK_JOIN_SCREENSHOT_URL" />

| Ticker | Trading days | Avg daily return % | Avg intraday volatility % | Max intraday volatility % | BTC vol ÷ stock avg vol |
|---|---|---|---|---|---|
| INTC | 20 | −0.87 | 6.99 | 10.17 | 0.03 |
| AMD | 27 | −0.37 | 6.18 | 9.68 | 0.03 |
| TSLA | 27 | −0.53 | 3.92 | 10.06 | 0.05 |
| AVGO | 20 | 0.25 | 3.61 | 5.34 | 0.06 |
| NVDA | 27 | 0.38 | 3.47 | 6.39 | 0.06 |
| META | 20 | −0.08 | 3.34 | 6.79 | 0.06 |
| GOOGL | 20 | 0.18 | 2.85 | 7.23 | 0.07 |
| MSFT | 27 | 0.54 | 2.81 | 5.99 | 0.07 |
| AMZN | 20 | −0.10 | 2.60 | 4.28 | 0.08 |
| AAPL | 27 | 0.68 | 2.40 | 5.35 | 0.09 |

BTC-USD (streaming): **+0.07% return, 0.21% volatility** across 42 windows from 2026-10-28 00:21 to 01:23 UTC. Query stats: 645 ms run time, **15.82 KB scanned across both buckets**.

**Reading the results honestly**: the BTC side covers about one hour of streaming data while each stock figure is a full trading session, so the tiny volatility ratios reflect the time spans, not a real claim that BTC is calmer than tech stocks. The point here is the mechanics: cross-bucket, cross-pipeline, partition-pruned SQL with consistent metric definitions. Once both pipelines run on the same trading days, the same tables support a true same-session, date-matched comparison (BTC over 09:30–16:00 New York time vs. each stock's open-to-close return), drafted in the same `.sql` file.

**A data-quality finding the join surfaced**: five tickers (INTC, AVGO, META, GOOGL, AMZN) have only 20 usable trading days against 27 for the others in the same date range, and 2026-07-10 has rows with null metrics. That's a batch-side gap to investigate, exactly the kind of issue that only becomes visible once the two pipelines' outputs are queried side by side.
