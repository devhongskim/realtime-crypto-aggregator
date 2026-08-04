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

**Expected outcome**: Excluded from the aggregate — the window should have already been finalized and written by the time this event arrives, and Spark should not reopen or amend a finalized window.

**Actual result**: *(to be filled in once executed)*

---

## Scenario 4 — Ordering is preserved per symbol under concurrent trade activity

**Validates**: Requirement 2 (preserve ordering within each trading pair), and the single-producer-per-partition assumption in [ADR-001](../ADR.md#adr-001-why-kafka-instead-of-redis-streams).

**Steps**:
1. During a period of active trading on at least two symbols (e.g. `BTC-USD` and `ETH-USD`), capture the raw sequence of trades as received by the producer.
2. Compare against the order those same trades appear within their Kafka partition.

**Expected outcome**: Per-symbol order matches — trades for a given symbol appear in the same relative order in Kafka as they were received from Coinbase.

**Actual result**: *(to be filled in once executed)*
