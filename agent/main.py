#!/usr/bin/env python3
"""
Sentinel Agent Service — main entrypoint.

Consumes fraud alerts from the 'fraud-alerts' Kafka topic,
runs each alert through the LangGraph investigation graph,
and produces decisions to 'agent-decisions' and 'audit-trail'.

Usage:
    python agent/main.py

Ensure vector store is initialized first:
    python agent/vector_store.py
"""
import asyncio
import json
import logging
import os
import time

import structlog
from confluent_kafka import Consumer, Producer, KafkaError
from dotenv import load_dotenv

from agent.graph import FRAUD_GRAPH

load_dotenv()
logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
log = structlog.get_logger()


def _build_consumer() -> Consumer:
    bootstrap = os.environ["CONFLUENT_BOOTSTRAP_SERVERS"]
    conf: dict = {
        "bootstrap.servers":  bootstrap,
        "group.id":           "sentinel-agent-v1",
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


def _build_producer() -> Producer:
    bootstrap = os.environ["CONFLUENT_BOOTSTRAP_SERVERS"]
    conf: dict = {"bootstrap.servers": bootstrap}
    if "confluent.cloud" in bootstrap:
        conf.update({
            "sasl.mechanism":    "PLAIN",
            "security.protocol": "SASL_SSL",
            "sasl.username":     os.environ["CONFLUENT_API_KEY"],
            "sasl.password":     os.environ["CONFLUENT_API_SECRET"],
        })
    return Producer(conf)


def _produce(producer: Producer, topic: str, key: str, payload: dict) -> None:
    producer.produce(
        topic=topic,
        key=key,
        value=json.dumps(payload, default=str).encode(),
    )
    producer.flush(timeout=5)


async def process_alert(alert: dict, producer: Producer) -> None:
    transaction_id = alert.get("transaction_id", "unknown")
    risk_score     = float(alert.get("risk_score", 0))

    log.info("processing_alert", transaction_id=transaction_id, risk_score=f"{risk_score:.2f}")

    initial_state = {
        "alert":               alert,
        "transaction_history": "",
        "similar_cases":       "",
        "risk_assessment":     "",
        "action_taken":        "",
        "explanation":         "",
        "agent_steps":         [],
        "decision":            {},
        "start_time":          time.time(),
    }

    result   = await FRAUD_GRAPH.ainvoke(initial_state)
    decision = result["decision"]

    log.info(
        "decision_made",
        action=decision.get("action"),
        confidence=f"{decision.get('confidence', 0):.0%}",
        latency_ms=decision.get("latency_ms"),
        transaction_id=transaction_id,
    )

    # Produce decision to agent-decisions topic (consumed by dashboard + MongoDB sink)
    _produce(producer, "agent-decisions", transaction_id, decision)

    # Produce full audit record
    _produce(producer, "audit-trail", transaction_id, {
        **decision,
        "alert":      alert,
        "audit_type": "AGENT_DECISION",
    })


def main() -> None:
    consumer = _build_consumer()
    producer = _build_producer()
    consumer.subscribe(["fraud-alerts"])

    log.info("agent_service_started", topic="fraud-alerts",
             model=os.environ.get("WATSONX_MODEL_ID", "ibm/granite-13b-instruct-v2"))

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.error("consumer_error", error=str(msg.error()))
                continue

            try:
                alert = json.loads(msg.value().decode("utf-8"))
                loop.run_until_complete(process_alert(alert, producer))
            except json.JSONDecodeError as exc:
                log.error("json_decode_error", error=str(exc))
            except Exception as exc:
                log.error("processing_error", error=str(exc), exc_info=True)

    except KeyboardInterrupt:
        log.info("agent_service_stopped")
    finally:
        consumer.close()
        loop.close()


if __name__ == "__main__":
    main()
