"""
Coinbase Advanced Trade WebSocket -> Kafka producer.

Subscribes to the public `market_trades` channel for a small set of trading
pairs and republishes each individual trade event onto the `raw_trades` Kafka
topic, keyed by symbol so that Kafka's per-partition ordering guarantee keeps
trades for a given symbol in order (see ADR-001).

This producer is intentionally thin: it does no validation, deduplication, or
transformation of any kind (see ADR-008 -- that logic lives downstream, in the
Spark job, not here). Its only job is reliably capturing the feed and getting
it durably onto Kafka.
"""

import os
import json
import asyncio
import logging
from datetime import datetime, timezone

import websockets
from kafka import KafkaProducer
from kafka.errors import KafkaError
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

COINBASE_WS_URL = "wss://advanced-trade-ws.coinbase.com"
PRODUCT_IDS = ["BTC-USD", "ETH-USD"]  # add "DOGE-USD" later once this is proven out
KAFKA_TOPIC = "raw_trades"
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Coinbase disconnects clients that don't send a subscribe message within a
# few seconds of connecting -- subscribe immediately on connect, no delay.
SUBSCRIBE_MESSAGE = {
    "type": "subscribe",
    "product_ids": PRODUCT_IDS,
    "channel": "market_trades",
}

RECONNECT_DELAY_SECONDS = 5


def build_kafka_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        # acks="all" -- correctness over raw throughput for this project;
        # trade volume here is low enough that this isn't a real bottleneck.
        acks="all",
        retries=5,
    )


def publish_trade(producer: KafkaProducer, trade: dict) -> None:
    """
    Publish a single trade event, keyed by symbol so Kafka's partitioner
    consistently routes every event for a given symbol to the same partition
    -- this is what makes per-symbol ordering (Requirement 2 / ADR-001) hold.
    """
    symbol = trade.get("product_id", "UNKNOWN")

    # Stamp our own ingestion-time alongside Coinbase's event time, so the
    # eventual end-to-end latency measurement (Scenario 7) has both ends
    # of the clock available downstream without re-deriving it later.
    envelope = {
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "source": "coinbase_advanced_trade_ws",
        "trade": trade,
    }

    try:
        producer.send(KAFKA_TOPIC, key=symbol, value=envelope)
    except KafkaError as e:
        logger.error(f"Failed to publish trade for {symbol}: {e}")


def extract_trades(message: dict) -> list:
    """
    Return the individual trade dicts contained in one Coinbase WebSocket
    message. market_trades messages carry one or more trade events inside an
    "events" list; anything else (e.g. the initial subscriptions confirmation)
    yields no trades. Pure function, so it's unit-testable without a socket.
    """
    if message.get("channel") != "market_trades":
        return []
    return [
        trade
        for event in message.get("events", [])
        for trade in event.get("trades", [])
    ]


async def stream_trades(producer: KafkaProducer) -> None:
    """
    Maintain the WebSocket connection, resubscribing and reconnecting on any
    drop -- Coinbase connections do get closed periodically, and per ADR-007
    the producer is expected to survive this independently of Kafka/Spark.
    """
    while True:
        try:
            async with websockets.connect(COINBASE_WS_URL) as ws:
                await ws.send(json.dumps(SUBSCRIBE_MESSAGE))
                logger.info(f"Subscribed to market_trades for {PRODUCT_IDS}")

                async for raw_message in ws:
                    message = json.loads(raw_message)
                    for trade in extract_trades(message):
                        publish_trade(producer, trade)

        except (websockets.ConnectionClosed, OSError) as e:
            logger.warning(
                f"WebSocket connection lost ({e}); "
                f"reconnecting in {RECONNECT_DELAY_SECONDS}s"
            )
            await asyncio.sleep(RECONNECT_DELAY_SECONDS)


def main() -> None:
    producer = build_kafka_producer()
    logger.info(f"Kafka producer connected to {KAFKA_BOOTSTRAP_SERVERS}")
    try:
        asyncio.run(stream_trades(producer))
    except KeyboardInterrupt:
        logger.info("Shutting down producer.")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
