"""Unit tests for the pure logic in producer/coinbase_producer.py (no network, no Kafka)."""

import logging
from datetime import datetime

from kafka.errors import KafkaError

import coinbase_producer as producer_mod


class FakeProducer:
    """Records send() calls instead of talking to Kafka."""

    def __init__(self, fail=False):
        self.sent = []
        self.fail = fail

    def send(self, topic, key=None, value=None):
        if self.fail:
            raise KafkaError("broker unavailable")
        self.sent.append((topic, key, value))


# --- extract_trades ----------------------------------------------------------

def test_extract_trades_flattens_all_events_in_order():
    message = {
        "channel": "market_trades",
        "events": [
            {"type": "update", "trades": [{"trade_id": "1"}, {"trade_id": "2"}]},
            {"type": "update", "trades": [{"trade_id": "3"}]},
        ],
    }
    assert [t["trade_id"] for t in producer_mod.extract_trades(message)] == ["1", "2", "3"]


def test_extract_trades_ignores_non_trade_messages():
    subscription_ack = {"channel": "subscriptions", "events": [{"subscriptions": {}}]}
    assert producer_mod.extract_trades(subscription_ack) == []


def test_extract_trades_handles_missing_events():
    assert producer_mod.extract_trades({"channel": "market_trades"}) == []


# --- publish_trade -----------------------------------------------------------

def test_publish_trade_keys_by_symbol_and_wraps_in_envelope():
    fake = FakeProducer()
    trade = {"trade_id": "1", "product_id": "ETH-USD", "price": "2500.00"}

    producer_mod.publish_trade(fake, trade)

    assert len(fake.sent) == 1
    topic, key, envelope = fake.sent[0]
    assert topic == "raw_trades"
    assert key == "ETH-USD"  # same key -> same partition -> per-symbol ordering (ADR-001)
    assert envelope["source"] == "coinbase_advanced_trade_ws"
    assert envelope["trade"] == trade  # passed through untouched (ADR-008)
    assert datetime.fromisoformat(envelope["ingested_at"]).tzinfo is not None  # UTC-aware


def test_publish_trade_without_product_id_uses_fallback_key():
    fake = FakeProducer()
    producer_mod.publish_trade(fake, {"trade_id": "1"})
    assert fake.sent[0][1] == "UNKNOWN"


def test_publish_trade_logs_kafka_errors_instead_of_crashing(caplog):
    with caplog.at_level(logging.ERROR):
        producer_mod.publish_trade(FakeProducer(fail=True), {"product_id": "BTC-USD"})
    assert "Failed to publish trade for BTC-USD" in caplog.text
