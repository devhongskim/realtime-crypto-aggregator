# Architecture Decision Records

Architecture Decision Records document the major engineering decisions made during the design of this project, including the context, alternatives considered, rationale, and accepted tradeoffs. Each ADR traces back to one or more of the requirements listed in [README.md](README.md#requirements).

---

## ADR-001: Why Kafka instead of Redis Streams?

**Context**: Requirement 2 (ordering) and 3 (restart-safe continuation) both need a durable, ordered log — not just a queue.

**Decision**: Kafka.

**Rationale**: Kafka partitions guarantee ordering *within* a partition — assuming a single producer per partition and preserving producer ordering (true for this project, since there is exactly one producer: the Coinbase WebSocket subscriber) — so partitioning `raw_trades` by symbol directly satisfies Requirement 2 without extra application logic. Kafka's append-only log allows consumers to replay historical events simply by resetting offsets, which is what satisfies Requirement 3: a restarted consumer resumes from its last committed offset rather than needing its own bespoke checkpoint mechanism. Kafka has become the de facto standard for durable event streaming and integrates directly with the broader streaming ecosystem, including Spark Structured Streaming.

**Tradeoff accepted**: Kafka has meaningfully more operational overhead than Redis (ZooKeeper/KRaft, broker config, topic/partition management) — justified here by the stronger ordering/replay guarantees and ecosystem fit, not chosen for its own sake.

---

## ADR-002: Why PySpark Structured Streaming instead of Flink?

**Context**: Requirement 4 (windowed analytics) and 5 (late data) need a stream processing engine with native event-time windowing and watermarking.

**Decision**: PySpark Structured Streaming.

**Rationale**: Both Flink and Spark Structured Streaming support event-time windows and watermarks natively — this isn't a capability gap. Spark Structured Streaming naturally complements the existing batch pipeline, allowing the project to reuse the same execution engine, DataFrame APIs, and Python ecosystem across both batch and streaming workloads. It also runs in local mode (`local[*]`) with zero cluster setup, which matters for a project developed and run locally.

Spark does technically offer a Continuous Processing mode as a lower-latency alternative to micro-batching, closer to Flink's per-event model — but it remains experimental, supports a limited set of sinks, and has materially weaker checkpointing/fault-tolerance guarantees, which directly conflicts with Requirement 3 (restart-safe continuation). Given a 10-second window requirement, Spark's standard micro-batch model comfortably meets the latency bar without taking on either Continuous Processing's fault-tolerance tradeoffs or Flink's more complex state-management model. Flink still has a genuine edge for true sub-second, per-event latency requirements — not the case here.

**Tradeoff accepted**: Micro-batch processing means "real-time" here is closer to "near-real-time, batch interval-bound" than Flink's native per-event model — an accepted tradeoff given the 10-second window target, not a limitation being glossed over.

---

## ADR-003: Why event-time windows (not processing-time)?

**Context**: Requirement 5 (late data support) is only meaningful if windows are defined by when the trade *actually happened*, not when Spark happened to receive it.

**Decision**: Event-time windowing, keyed on the trade's own timestamp field from the Coinbase payload.

**Rationale**: Processing-time windows would silently produce different results depending on network conditions and consumer lag — the same set of trades could land in different windows on a re-run, which breaks reproducibility. Event-time windowing makes the aggregation a pure function of the data itself, independent of transport latency, consumer lag, or temporary backpressure.

**Tradeoff accepted**: Requires a watermark (see ADR-004) to bound how long Spark waits for late data before finalizing a window — event-time correctness isn't free, it trades immediacy for correctness.

---

## ADR-004: Why watermark = 30 seconds?

**Context**: Requirement 5 needs an explicit, bounded answer to "how late can an event be and still count?"

**Decision**: 30-second watermark.

**Rationale**: This is a deliberately chosen tradeoff, not a default: 30 seconds is approximately three window intervals, giving meaningful slack for realistic WebSocket network jitter without indefinitely delaying window finalization (which would blow up state size and delay output). This value was selected as an operational tuning parameter rather than derived from a business requirement, and should be treated as tunable — tightening it trades completeness for lower latency-to-finalized-result, while loosening it does the reverse.

**Tradeoff accepted**: Trades arriving more than 30 seconds late are dropped from their window entirely — an explicit, bounded data-loss policy, chosen deliberately over unbounded state growth. The chosen watermark should ultimately be informed by observed lateness distributions once the pipeline is running against real traffic, rather than treated as a fixed value decided once and never revisited.

---

## ADR-005: Why Parquet instead of JSON?

**Context**: Requirement 6 (queryable downstream format) and the Athena query layer.

**Decision**: Parquet.

**Rationale**: Columnar storage means Athena only scans the columns a query actually needs, versus JSON requiring a full-row scan and parse for every query. Columnar encoding and compression substantially reduce storage footprint and network I/O compared to row-oriented JSON documents. Parquet also carries schema/type information natively, avoiding the "is this field a string or a number" ambiguity JSON has. This mirrors the batch pipeline's existing Silver/Gold format choice — same reasoning applies to both projects.

**Tradeoff accepted**: Parquet files aren't human-readable by just opening them in a text editor (unlike JSON) — a minor debugging inconvenience traded for real query performance, compression, and cost benefits at read time.

---

## ADR-006: Why S3 instead of PostgreSQL?

**Context**: Requirement 6, plus the desire to converge batch and streaming outputs into one shared query layer (Athena).

**Decision**: S3 (Parquet), queried via Athena, rather than a relational database.

**Rationale**: S3 decouples storage from compute, allowing multiple analytical engines (Athena today, Spark or DuckDB later) to operate over the same immutable Parquet datasets without migrating data into a database first. This is what lets the batch and streaming sides converge on the same query layer without needing to also converge on the same database schema or instance — each side writes its own Parquet layout and both remain queryable together.

**Tradeoff accepted**: Athena has higher per-query latency than a warehouse-native query engine or an indexed Postgres table for point lookups — acceptable here since this is an analytical/BI query pattern (aggregates, scans), not a low-latency transactional lookup pattern.

---

## ADR-007: Why Kafka as an ingestion boundary, instead of Spark reading Coinbase directly?

**Context**: Live market data needs to support reliable ingestion, and potentially multiple downstream consumers, without the data source and the processing engine being tightly coupled to each other's availability.

**Decision**: Introduce Kafka between the WebSocket producer and Spark, rather than having Spark consume the WebSocket directly.

**Rationale**: This decouples the producer from its consumers — the producer's only job is reliably capturing the WebSocket feed and appending to Kafka, and it doesn't need to know or care what (if anything) is consuming downstream. This buys several properties at once:
- The producer survives a Spark restart (Kafka simply buffers events until Spark reconnects)
- Spark survives a producer restart (it resumes from its last committed offset, no data replay logic needed in the producer itself)
- Multiple consumers could subscribe to the same topic in the future without touching the producer at all
- Kafka absorbs backpressure — if Spark falls behind processing, events queue durably in Kafka rather than overwhelming Spark directly or getting dropped at the source

**Tradeoff accepted**: An entire extra piece of infrastructure (Kafka itself) for what could, in a smaller system, be a direct WebSocket-to-Spark connection — justified here because the decoupling properties above are exactly what a message broker exists to provide, and this project is explicitly meant to demonstrate that pattern.

---

## ADR-008: Why Streaming Silver is not materialized separately

**Context**: The batch pipeline materializes Bronze, Silver, and Gold as distinct, separately stored layers — Silver specifically holding schema-validated, cleaned data ahead of the Gold aggregation step. The natural instinct was to mirror that exact structure here, with a `Streaming Silver` prefix in S3 holding validated/deduplicated trade events.

**Decision**: Streaming Silver (validation: schema/null checks, `trade_id` deduplication) exists as a logical transformation step — applied in-memory, inside the same Spark Structured Streaming job, between reading from Kafka and windowing into Gold — rather than as its own physical storage layer written out to S3.

**Rationale**: Streaming Bronze represents the durable raw event layer once successfully written to S3; Kafka provides the replay buffer between ingestion and persistence — Bronze's durability depends on Spark successfully completing that write, not on the data having existed in Kafka alone. Given that, the validation/dedup logic applied to produce "Silver" is deterministic given a fixed Bronze input — meaning "what Silver would have looked like" can always be reconstructed on demand by replaying Bronze through the same filtering logic again. Persisting it separately would add S3 write volume and an extra prefix to manage, without adding any information that isn't already recoverable from Bronze plus the (version-controlled) validation code itself. This is closer to a **Kappa architecture** pattern (one durable raw log, with downstream views treated as recomputable rather than permanently materialized) than the batch pipeline's stricter medallion pattern, and the two are allowed to differ, since the batch and streaming sides face genuinely different constraints — the batch pipeline persists Silver partly because Postgres/downstream consumers historically read from it directly, a concern that doesn't apply here.

**Tradeoff accepted**: Debuggability is the real cost — if Gold output ever looks wrong, there's no separately stored intermediate artifact to inspect; diagnosing "was the raw data bad, or did the aggregation logic misbehave" requires re-running the validation/dedup logic against Bronze on demand rather than querying an already-materialized Silver table. Accepted here because that debugging scenario is expected to be occasional, and the logic being replayed is simple (schema checks and a `dropDuplicates` call) rather than expensive or slow to reconstruct.

---

## ADR-009: Why partition Gold by day instead of by window timestamp?

**Context**: Requirement 6 (queryable downstream format), and specifically making Streaming Gold work with Athena. The first layout partitioned Gold by each 10-second window's start time (`window_folder=2026-08-16 22:25:50/symbol=BTC-USD/`). Two problems surfaced once real data landed in S3:
- **URL-escaped partition paths**: Spark escapes `:` when building partition directories, so the physical paths became `window_folder=2026-08-16 22%3A25%3A50/`. Athena couldn't map those back to a `TIMESTAMP` partition, and an `injected` partition projection only works with an exact, escaped-string equality filter on every query, so range filters like `window_start >= TIMESTAMP '...'` were impossible. Workarounds piled up: a flat table, plus a view that regex-extracted the timestamp from `"$path"` and un-escaped `%3A` by hand.
- **Partition granularity**: one partition per 10-second window means 8,640 partitions per symbol per day (17,280/day for two symbols), each holding a single row. Even with a colon-free format, that layout maximizes S3 listing and partition-metadata overhead while giving almost no pruning benefit, since nobody queries a single 10-second window.

**Decision**: Partition Gold by `dt` (the window's UTC date, `yyyy-MM-dd`) and `symbol`, producing `gold/trades/dt=2026-10-28/symbol=BTC-USD/`. `window_start` and `window_end` stay in the Parquet data as regular `TIMESTAMP` columns. Partitions are registered with **Athena partition projection** (`dt` as a date range, `symbol` as an enum) instead of `MSCK REPAIR TABLE` or a Glue crawler, and the Spark session is pinned to UTC.

**Rationale**: Partition granularity should follow the query pattern, not the window size. Queries here look at a day (or range of days) for a symbol, so `dt` + `symbol` prunes to exactly the folders a query needs, and `window_start` then filters precisely within them. Measured on the first day of data: **4.62 KB scanned without partition filters → 2.15 KB with them (−53%)**. That early saving comes entirely from skipping the other symbol, and it grows with every additional day of history.

Partition projection fits a streaming writer specifically: new partitions appear continuously, and `MSCK REPAIR TABLE` would have to be re-run every time one does, listing the entire prefix each run (slower as history grows). A Glue crawler adds cost and schedule lag. Projection computes partition locations from table properties at query time, so data is queryable the moment Spark commits it, with zero partition maintenance. UTC avoids daylight-saving gaps/overlaps and keeps `dt` independent of the machine the job happens to run on.

**Tradeoff accepted**:
- **Pruning requires an explicit `dt` filter**. Athena doesn't derive `dt` from a `window_start` predicate, so a query filtering only on `window_start` scans every day. Queries need both predicates. Because `dt` is in UTC, evening trades in US time zones land in the next day's partition.
- **Projection config must stay in sync with the job**. Adding a symbol to `ALLOWED_SYMBOLS` without also updating `projection.symbol.values` makes the new symbol silently invisible to Athena: no error, just missing rows.
- **Small files remain**. Each 10-second micro-batch still writes a new small Parquet file per partition (hundreds per symbol per day). Daily partitions make this manageable, and a compaction job per `dt` is the at-scale fix, not yet built.
- **Data written under the old layout isn't queryable by this table**. It was moved to `gold/trades_legacy/` and is out of scope for now (a Glue crawler over that prefix is a possible later exploration).

---

## ADR-010: Why a self-hosted Grafana, provisioned as code, querying Athena directly?

**Context**: The Analytics & Observability goals call for proof that the pipeline is healthy and its tradeoffs are measurable (not just diagrammed), using the Athena query layer that already serves the batch/streaming convergence.

**Decision**: Grafana OSS (pinned to 11.3.0) runs as another service in the existing Docker Compose stack, with the official Athena data-source plugin querying `streaming_gold_trades` directly. The data source and the dashboard are **provisioned from version-controlled files** (`grafana/provisioning/`, `grafana/dashboards/crypto_pipeline_health.json`), not clicked together in the UI. Grafana authenticates as a **dedicated IAM user** (`grafana-athena-reader`) whose policy can only run Athena queries, read the Glue catalog, read `gold/`, and write its own query results under `athena-results/`.

**Rationale**:
- **Reuses the query layer instead of adding a metrics stack.** Every Gold row already carries `window_end` and `written_at`, so freshness, end-to-end latency, window completeness, and throughput are plain SQL over data the pipeline already produces. No Prometheus, exporters, or extra storage needed to get the core health signals.
- **Dashboards as code.** A fresh `docker compose up` recreates the data source and dashboard identically on any machine. The dashboard is reviewable in git diffs, and no credentials are committed (provisioning files read keys from environment variables sourced from the gitignored `.env`).
- **Least privilege, separate from the writer.** The Spark job's keys can write to the bucket; Grafana's can't. If the dashboard's credentials leaked, the blast radius is read access to Gold and write access to a scratch results prefix, not the ability to modify or delete pipeline data.
- **Partition pruning carried into every panel.** Each panel query filters `dt` using the dashboard's time range (`$__rawTimeFrom('yyyy-MM-dd')`), so refreshes read only the day folders in view (see [ADR-009](#adr-009-why-partition-gold-by-day-instead-of-by-window-timestamp)).
- **Alternatives considered**: Amazon Managed Grafana (same tool, but adds per-user licensing and IAM Identity Center setup for a single-developer project); QuickSight (built for business BI reporting, not operational time-series monitoring); CloudWatch dashboards (would require publishing custom metrics from the job first).

**Observed result**: median end-to-end latency (window close → S3 write) of **~40 s**, which matches the design: the 30 s watermark (ADR-004) plus the 10 s trigger interval. ADR-004's latency cost is now a measured number, not a claim. The dashboard also made two less obvious behaviors visible: startup catch-up (latency sloping down to ~40 s as Spark works through the backlog Kafka buffered while the job was off, Requirement 3), and a ~2-minute ingestion gap during which the last window before the gap waited ~3.3 min to be written. Event-time watermarks only advance when new events arrive, so windows can't close during a gap in the input, which is the correct, designed behavior but is invisible without this kind of monitoring.

**Tradeoff accepted**:
- **Athena isn't a low-latency metrics store.** Each panel refresh is a full Athena query (seconds of latency, billed with a 10 MB minimum per query). At a 1-minute refresh with 8 panels that's roughly 2 cents an hour while the dashboard is open, which is fine for this project but the wrong pattern for second-by-second operational alerting.
- **Only what reaches Gold is observable.** Watermark-dropped events and Kafka consumer lag never land in S3, so they can't be charted this way. Those require exporting Spark's `StreamingQueryProgress` metrics and Kafka consumer-group offsets to a metrics backend (e.g. Prometheus), and are deferred.
- **Freshness is measured at the S3 sink, not upstream.** "Seconds since last Gold write" going red says the pipeline stalled somewhere, not where (producer, Kafka, or Spark).
- **Static access keys in a local `.env`.** Acceptable for a local, single-developer setup; a deployed Grafana would use an assumed IAM role instead of long-lived keys.

---

New ADRs are added as further implementation decisions are actually made — a decision isn't recorded until it's been made, rather than speculated on in advance.