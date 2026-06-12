"""
Background Kafka consumer that bridges the blocking poll loop
to async WebSocket broadcast queues via asyncio.run_coroutine_threadsafe.

Topics consumed (all forwarded to connected dashboard clients):
  - transactions
  - risk-scores
  - fraud-alerts
  - agent-decisions
"""
import asyncio
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING

from confluent_kafka import Consumer, KafkaError

logger = logging.getLogger(__name__)

# Shared list of per-client queues; the WebSocket endpoint manages lifecycle
BROADCAST_QUEUES: list[asyncio.Queue] = []

TOPICS = ["transactions", "risk-scores", "fraud-alerts", "agent-decisions"]


def _build_consumer() -> Consumer:
    bootstrap = os.environ["CONFLUENT_BOOTSTRAP_SERVERS"]
    conf: dict = {
        "bootstrap.servers":  bootstrap,
        "group.id":           "sentinel-dashboard-v1",
        "auto.offset.reset":  "latest",
        "enable.auto.commit": True,
    }
    if "confluent.cloud" in bootstrap:
        conf.update({
            "sasl.mechanism":    "PLAIN",
            "security.protocol": "SASL_SSL",
            "sasl.username":     os.environ["CONFLUENT_API_KEY"],
            "sasl.password":     os.environ["CONFLUENT_API_SECRET"],
        })
    return Consumer(conf)


def _poll_loop(loop: asyncio.AbstractEventLoop) -> None:
    """Blocking Kafka poll loop — runs in a ThreadPoolExecutor thread."""
    consumer = _build_consumer()
    consumer.subscribe(TOPICS)
    logger.info("Kafka dashboard consumer started, subscribed to %s", TOPICS)

    while True:
        msg = consumer.poll(timeout=0.1)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                logger.error("Kafka consumer error: %s", msg.error())
            continue

        try:
            value_bytes = msg.value()
            if value_bytes is None:
                continue
            value = json.loads(value_bytes.decode("utf-8"))
            event = {
                "topic":     msg.topic(),
                "partition": msg.partition(),
                "offset":    msg.offset(),
                "key":       msg.key().decode("utf-8") if msg.key() else None,
                "value":     value,
            }
            # Broadcast to all connected WebSocket clients
            for queue in list(BROADCAST_QUEUES):
                asyncio.run_coroutine_threadsafe(
                    queue.put(event), loop
                )
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            logger.debug("Skipping non-JSON message: %s", exc)
        except Exception as exc:
            logger.error("Error processing Kafka message: %s", exc)


_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="kafka-consumer")


async def start_consumer_loop() -> None:
    """Launch the blocking Kafka consumer in a background thread."""
    loop = asyncio.get_event_loop()
    loop.run_in_executor(_executor, _poll_loop, loop)
    logger.info("Kafka consumer thread started")
