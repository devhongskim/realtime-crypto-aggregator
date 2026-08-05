# Validation Plan

This document lays out specific operational scenarios that will be executed manually once the pipeline is implemented, to confirm it actually satisfies the [requirements](../README.md#requirements) it was designed against — rather than assuming correctness from the architecture alone.

**Status: not yet executed.** This is a plan, not a report — each scenario below will be updated with actual observed results (including anything that *doesn't* work as expected) once the corresponding component exists and has been run for real.

---

## Scenario 1 — Spark restart resumes cleanly, without duplicating windows

**Validates**: Requirement 3 (continue processing after consumer restarts), and the checkpointing approach underlying [ADR-007](../ADR.md#adr-007-why-kafka-as-an-ingestion-boundary-instead-of-spark-reading-coinbase-directly).

**Steps**:
1. Start the producer and the Spark Structured Streaming job; let several windows complete and land in S3.
2. Kill the Spark job (not the producer) mid-stream.
3. Observe: the producer keeps running and Kafka offsets continue advancing while Spark is down.
4. Restart the Spark job.
5. Observe: Spark resumes from its last committed checkpoint rather than reprocessing from the beginning of the topic.
6. Inspect the resulting S3 output for the affected time range.

**Expected outcome**: No duplicate window files for periods that were already completed before the kill; no gap in windows either, once Spark catches up on the backlog Kafka retained while it was down.

**Actual result**: *(to be filled in once executed)*

---

## Scenario 2 — Late-arriving event within the watermark is still included

**Validates**: Requirement 5 (support late-arriving events), and the watermark tradeoff documented in [ADR-004](../ADR.md#adr-004-why-watermark--30-seconds).

**Steps**:
1. Identify or construct a trade event whose event-time timestamp places it in an already-elapsed 10-second window.
2. Inject it so it arrives ~15 seconds after that window's wall-clock end (within the 30-second watermark).
3. Observe whether it's included in that window's final aggregate once the window is written to S3.

**Expected outcome**: Included — 15 seconds is within the 30-second watermark threshold, so the window shouldn't have been finalized yet when the event arrives.

**Actual result**: *(to be filled in once executed)*

---

## Scenario 3 — Late-arriving event beyond the watermark is correctly dropped

**Validates**: The other half of Requirement 5 and ADR-004 — confirming the watermark's bound is actually enforced, not just its "accept late data" half.

**Steps**:
1. Same setup as Scenario 2, but inject the event more than 30 seconds after the window's wall-clock end.

**Expected outcome**: The late event should not appear in the finalized aggregate output after the watermark threshold has passed — its trade shouldn't be reflected in that window's count, volume, or price metrics in S3.

**Actual result**: *(to be filled in once executed)*

---

## Scenario 4 — Ordering is preserved per symbol under concurrent trade activity

**Validates**: Requirement 2 (preserve ordering within each trading pair), and the single-producer-per-partition assumption in [ADR-001](../ADR.md#adr-001-why-kafka-instead-of-redis-streams).

**Steps**:
1. During a period of active trading on at least two symbols (e.g. `BTC-USD` and `ETH-USD`), capture the raw sequence of trades as received by the producer.
2. Compare against the order those same trades appear within their Kafka partition.

**Expected outcome**: Per-symbol order matches — trades for a given symbol appear in the same relative order in Kafka as they were received from Coinbase.

**Actual result**: *(to be filled in once executed)*

---

## Scenario 5 — Duplicate trade events are correctly filtered

**Validates**: The deduplication logic underlying [ADR-008](../ADR.md#adr-008-why-streaming-silver-is-not-materialized-separately), and general data-quality correctness (Requirement 6's usefulness depends on Gold not double-counting a re-delivered event).

**Steps**:
1. Simulate or capture a WebSocket reconnect scenario where Coinbase (or the producer, if reconnect logic causes a replay) redelivers a trade event already seen, carrying the same `trade_id`.
2. Let it flow through Kafka into the Spark job's in-memory validation/dedup step.
3. Inspect the resulting window's aggregate in S3.

**Expected outcome**: The duplicate is dropped before windowing — trade count and volume in the affected window reflect the unique trade only once, not twice.

**Actual result**: *(to be filled in once executed)*

---

## Scenario 6 — Streaming "Silver" is deterministic and recomputable from Bronze

**Validates**: The central premise of [ADR-008](../ADR.md#adr-008-why-streaming-silver-is-not-materialized-separately) — that not persisting Silver is safe specifically *because* re-running the same validation/dedup logic against the same Bronze input always produces the same result.

**Steps**:
1. Take a captured slice of Streaming Bronze data (a set of raw Kafka messages, including at least one intentionally malformed/null event and one intentional duplicate `trade_id`).
2. Run it through the validation/dedup logic twice, independently (e.g. in two separate Spark job runs, or a standalone unit test invoking the same filter function used in the streaming job).
3. Compare the two resulting "Silver" outputs.

**Expected outcome**: Identical results both times — same records kept, same records rejected, same duplicates removed. Confirms the recomputability assumption ADR-008 is actually built on, rather than leaving it as an untested claim.

**Actual result**: *(to be filled in once executed)*

---

## Scenario 7 — End-to-end latency measurement

**Validates**: The project's "real-time" framing, and the end-to-end lag metric promised in the [Analytics & Observability](../README.md#analytics--observability-planned) Grafana panels — without this, "real-time" is just an unmeasured claim.

**Steps**:
1. For a sample of trade events, capture four timestamps: the event's own trade timestamp (from Coinbase), the time it lands in Kafka, the time Spark processes it, and the time its window is written to S3.
2. Compute end-to-end latency as `S3 write timestamp − event timestamp` for each sampled event/window.
3. Repeat across a range of load conditions if possible (quiet periods vs. high trade volume) to see whether latency stays roughly stable or degrades under load.

**Expected outcome**: A concrete, reproducible latency number (e.g. "typically N seconds end-to-end"), not just an assumption that the system is fast. Given the 10-second window and 30-second watermark, latency in roughly the same range (single-digit to low double-digit seconds) would be a reasonable outcome; a much larger number would indicate a bottleneck worth investigating rather than a "real-time" system.

**Actual result**: *(to be filled in once executed)*