#!/usr/bin/env python3
"""
Realistic UPI/card transaction generator for India.

Normal rate : ~100 transactions/second
Fraud inject: every FRAUD_INJECTION_INTERVAL_SECONDS seconds, one of three patterns:
  - VELOCITY   : 20 rapid small transactions to the same ECOMMERCE merchant
  - GEO_JUMP   : Single large ATM transaction > 800 km from home city
  - AMOUNT_ANOMALY: Single transaction 12-20x the user's average amount

Usage:
    python producer/transaction_generator.py
"""
import uuid
import time
import math
import random
import os
import sys
from enum import Enum

import structlog
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
from dotenv import load_dotenv

load_dotenv()
log = structlog.get_logger()

# ── Indian cities with (lat, lon) ──────────────────────────────────────────────
INDIAN_CITIES = {
    "Mumbai":    (19.0760, 72.8777),
    "Delhi":     (28.6139, 77.2090),
    "Bangalore": (12.9716, 77.5946),
    "Chennai":   (13.0827, 80.2707),
    "Hyderabad": (17.3850, 78.4867),
    "Kolkata":   (22.5726, 88.3639),
    "Pune":      (18.5204, 73.8567),
    "Ahmedabad": (23.0225, 72.5714),
    "Jaipur":    (26.9124, 75.7873),
    "Surat":     (21.1702, 72.8311),
}

MERCHANT_CATEGORIES = [
    "GROCERY", "FUEL", "RESTAURANT", "ECOMMERCE", "UTILITY",
    "ATM", "TRAVEL", "ENTERTAINMENT", "MEDICAL", "EDUCATION",
    "RECHARGE", "OTHER",
]
CATEGORY_WEIGHTS = [15, 8, 12, 20, 10, 5, 8, 7, 5, 4, 4, 2]

TRANSACTION_TYPES = ["UPI", "CREDIT_CARD", "DEBIT_CARD", "NEFT", "IMPS"]
TXN_TYPE_WEIGHTS  = [45, 25, 20, 5, 5]

rng = random.Random(None)  # seeded per-process


def _build_users(n: int = 500) -> dict:
    local_rng = random.Random(42)
    cities = list(INDIAN_CITIES.keys())
    return {
        f"user_{i:04d}": {
            "city":       local_rng.choice(cities),
            "avg_amount": local_rng.uniform(200, 5000),
            "std_amount": local_rng.uniform(100, 2000),
        }
        for i in range(n)
    }


USERS = _build_users()
USER_IDS = list(USERS.keys())


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2)
    return R * 2 * math.asin(math.sqrt(a))


def _make_normal_txn(user_id: str) -> dict:
    user   = USERS[user_id]
    city   = user["city"]
    lat, lon = INDIAN_CITIES[city]
    lat += rng.gauss(0, 0.05)
    lon += rng.gauss(0, 0.05)
    amount = max(10.0, rng.gauss(user["avg_amount"], user["std_amount"]))
    category = rng.choices(MERCHANT_CATEGORIES, weights=CATEGORY_WEIGHTS)[0]
    return {
        "transaction_id":    str(uuid.uuid4()),
        "user_id":           user_id,
        "card_number":       f"4{user_id[-4:]}XXXXXXXX{rng.randint(1000, 9999)}",
        "amount":            round(amount, 2),
        "currency":          "INR",
        "merchant_id":       f"MID_{rng.randint(10000, 99999)}",
        "merchant_name":     f"Merchant_{rng.randint(1, 1000)}",
        "merchant_category": category,
        "transaction_type":  rng.choices(TRANSACTION_TYPES, weights=TXN_TYPE_WEIGHTS)[0],
        "latitude":          round(lat, 6),
        "longitude":         round(lon, 6),
        "city":              city,
        "device_id":         f"DEV_{user_id}_{rng.randint(1, 3)}",
        "ip_address":        f"192.168.{rng.randint(1, 254)}.{rng.randint(1, 254)}",
        "timestamp":         int(time.time() * 1000),
        "is_international":  False,
    }


class FraudPattern(Enum):
    VELOCITY       = "velocity_fraud"
    GEO_JUMP       = "geo_jump"
    AMOUNT_ANOMALY = "amount_anomaly"


def _inject_velocity(user_id: str) -> list[dict]:
    """20 rapid small UPI transactions to the same ECOMMERCE merchant."""
    base = _make_normal_txn(user_id)
    txns = []
    for i in range(20):
        t = dict(base)
        t["transaction_id"]    = str(uuid.uuid4())
        t["amount"]            = round(rng.uniform(1.0, 99.0), 2)
        t["merchant_id"]       = base["merchant_id"]   # same merchant
        t["merchant_category"] = "ECOMMERCE"
        t["transaction_type"]  = "UPI"
        t["timestamp"]         = int(time.time() * 1000) + i * 3_000
        txns.append(t)
    return txns


def _inject_geo_jump(user_id: str) -> list[dict]:
    """Single large ATM transaction in a city > 800 km from home."""
    user      = USERS[user_id]
    home_city = user["city"]
    home_lat, home_lon = INDIAN_CITIES[home_city]

    far_cities = [
        c for c in INDIAN_CITIES
        if _haversine_km(home_lat, home_lon, *INDIAN_CITIES[c]) > 800
    ]
    if not far_cities:
        far_cities = [c for c in INDIAN_CITIES if c != home_city]

    fraud_city = rng.choice(far_cities)
    lat, lon   = INDIAN_CITIES[fraud_city]

    txn = _make_normal_txn(user_id)
    txn["transaction_id"]    = str(uuid.uuid4())
    txn["city"]              = fraud_city
    txn["latitude"]          = round(lat + rng.gauss(0, 0.02), 6)
    txn["longitude"]         = round(lon + rng.gauss(0, 0.02), 6)
    txn["amount"]            = round(rng.uniform(5_000, 50_000), 2)
    txn["merchant_category"] = "ATM"
    txn["transaction_type"]  = "DEBIT_CARD"
    return [txn]


def _inject_amount_anomaly(user_id: str) -> list[dict]:
    """Single ECOMMERCE transaction 12-20x the user's average spend."""
    user = USERS[user_id]
    txn  = _make_normal_txn(user_id)
    txn["transaction_id"]    = str(uuid.uuid4())
    txn["amount"]            = round(user["avg_amount"] * rng.uniform(12, 20), 2)
    txn["merchant_category"] = "ECOMMERCE"
    return [txn]


def _build_producer() -> Producer:
    bootstrap = os.environ["CONFLUENT_BOOTSTRAP_SERVERS"]
    conf: dict = {"bootstrap.servers": bootstrap}

    # Add SASL only for Confluent Cloud (not local KRaft broker)
    if "confluent.cloud" in bootstrap:
        conf.update({
            "sasl.mechanism":   "PLAIN",
            "security.protocol":"SASL_SSL",
            "sasl.username":    os.environ["CONFLUENT_API_KEY"],
            "sasl.password":    os.environ["CONFLUENT_API_SECRET"],
        })

    return Producer(conf)


def _build_serializer(producer_conf: dict) -> AvroSerializer:
    sr_url = os.environ["CONFLUENT_SCHEMA_REGISTRY_URL"]
    sr_conf: dict = {"url": sr_url}
    sr_key = os.environ.get("CONFLUENT_SCHEMA_REGISTRY_API_KEY", "")
    sr_sec = os.environ.get("CONFLUENT_SCHEMA_REGISTRY_API_SECRET", "")
    if sr_key:
        sr_conf["basic.auth.user.info"] = f"{sr_key}:{sr_sec}"

    schema_registry = SchemaRegistryClient(sr_conf)
    schema_str      = open("schemas/transaction.avsc").read()
    return AvroSerializer(
        schema_registry,
        schema_str,
        conf={"auto.register.schemas": True},
    )


def _delivery_report(err, msg):
    if err:
        log.error("delivery_failed", topic=msg.topic(), error=str(err))


def main() -> None:
    producer    = _build_producer()
    serializer  = _build_serializer({})
    ctx         = SerializationContext("transactions", MessageField.VALUE)

    fraud_interval = float(os.environ.get("FRAUD_INJECTION_INTERVAL_SECONDS", 30))
    last_fraud_at  = 0.0
    txn_count      = 0
    fraud_patterns = list(FraudPattern)

    log.info("transaction_generator_started", rate_per_second=100, fraud_interval=fraud_interval)

    while True:
        batch_start = time.time()

        # Normal batch: 10 transactions per 100ms ≈ 100/s
        batch_users = rng.choices(USER_IDS, k=10)
        for user_id in batch_users:
            txn = _make_normal_txn(user_id)
            producer.produce(
                topic="transactions",
                key=txn["user_id"],
                value=serializer(txn, ctx),
                on_delivery=_delivery_report,
            )
            txn_count += 1

        # Fraud injection
        now = time.time()
        if now - last_fraud_at > fraud_interval:
            pattern = rng.choice(fraud_patterns)
            victim  = rng.choice(USER_IDS)
            log.warning("fraud_injecting", pattern=pattern.value, user=victim)

            if pattern == FraudPattern.VELOCITY:
                fraud_txns = _inject_velocity(victim)
            elif pattern == FraudPattern.GEO_JUMP:
                fraud_txns = _inject_geo_jump(victim)
            else:
                fraud_txns = _inject_amount_anomaly(victim)

            for txn in fraud_txns:
                producer.produce(
                    topic="transactions",
                    key=txn["user_id"],
                    value=serializer(txn, ctx),
                    on_delivery=_delivery_report,
                )

            last_fraud_at = now
            log.warning("fraud_injected",
                        pattern=pattern.value, count=len(fraud_txns), victim=victim)

        producer.poll(0)

        elapsed    = time.time() - batch_start
        sleep_time = max(0.0, 0.1 - elapsed)
        time.sleep(sleep_time)

        if txn_count % 1000 == 0:
            log.info("generator_heartbeat", total_sent=txn_count)


if __name__ == "__main__":
    main()
