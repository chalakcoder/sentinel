"""
Background Kafka consumer that bridges the blocking poll loop
to async WebSocket broadcast queues via asyncio.run_coroutine_threadsafe.

All pipeline topics carry Schema Registry Avro, so values are decoded with
a generic AvroDeserializer (it resolves the writer schema from the message's
schema ID). Plain-JSON messages fall back to json.loads.

Topics consumed (all forwarded to connected dashboard clients):
  - transactions
  - risk-scores
  - fraud-alerts
"""
import asyncio
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor

from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

logger = logging.getLogger(__name__)

# Shared list of per-client queues; the WebSocket endpoint manages lifecycle
BROADCAST_QUEUES: list[asyncio.Queue] = []

TOPICS = ["transactions", "risk-scores", "fraud-alerts"]


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


def _build_avro_deserializer() -> AvroDeserializer:
    sr_conf: dict = {"url": os.environ["CONFLUENT_SCHEMA_REGISTRY_URL"]}
    sr_key = os.environ.get("CONFLUENT_SCHEMA_REGISTRY_API_KEY", "")
    sr_sec = os.environ.get("CONFLUENT_SCHEMA_REGISTRY_API_SECRET", "")
    if sr_key:
        sr_conf["basic.auth.user.info"] = f"{sr_key}:{sr_sec}"
    # No schema string: resolves the writer schema from each message's schema ID
    return AvroDeserializer(SchemaRegistryClient(sr_conf))


def _decode_value(raw: bytes, topic: str, avro_deserializer: AvroDeserializer):
    try:
        value = avro_deserializer(raw, SerializationContext(topic, MessageField.VALUE))
        if value is not None:
            # Round-trip through JSON to make datetimes/decimals WS-serializable
            return json.loads(json.dumps(value, default=str))
    except Exception:
        pass
    try:
        return json.loads(raw.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None


def _poll_loop(loop: asyncio.AbstractEventLoop) -> None:
    """Blocking Kafka poll loop — runs in a ThreadPoolExecutor thread."""
    consumer = _build_consumer()
    avro_deserializer = _build_avro_deserializer()
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

        raw = msg.value()
        if raw is None:
            continue

        value = _decode_value(raw, msg.topic(), avro_deserializer)
        if value is None:
            continue

        event = {
            "topic":     msg.topic(),
            "partition": msg.partition(),
            "offset":    msg.offset(),
            "key":       msg.key().decode("utf-8") if msg.key() else None,
            "value":     value,
        }
        for queue in list(BROADCAST_QUEUES):
            asyncio.run_coroutine_threadsafe(queue.put(event), loop)


_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="kafka-consumer")


async def start_consumer_loop() -> None:
    """Launch the blocking Kafka consumer in a background thread."""
    loop = asyncio.get_event_loop()
    loop.run_in_executor(_executor, _poll_loop, loop)
    logger.info("Kafka consumer thread started")
