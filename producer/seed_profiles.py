#!/usr/bin/env python3
"""
Seed the customer-profiles Kafka topic with 500 synthetic Indian customer profiles.

The topic acts as a versioned/compacted changelog keyed by user_id.
Flink uses it as a temporal table for stream-stream joins.

Usage:
    python producer/seed_profiles.py
"""
import os
import time
import random
from pathlib import Path

import structlog
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
from dotenv import load_dotenv

load_dotenv()
log = structlog.get_logger()

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
    "ATM", "TRAVEL", "ENTERTAINMENT", "MEDICAL", "EDUCATION", "RECHARGE", "OTHER",
]

FIRST_NAMES = [
    "Aarav", "Vivaan", "Aditya", "Vihaan", "Arjun", "Sai", "Reyansh", "Ayaan",
    "Krishna", "Ishaan", "Priya", "Diya", "Ananya", "Kavya", "Meera", "Isha",
    "Rohan", "Rahul", "Neha", "Pooja", "Amit", "Riya", "Vikram", "Sunita",
]
LAST_NAMES = [
    "Sharma", "Verma", "Patel", "Gupta", "Singh", "Kumar", "Rao",
    "Mehta", "Joshi", "Nair", "Reddy", "Iyer", "Bose", "Das",
]

rng = random.Random(42)


def generate_profile(user_id: str) -> dict:
    city    = rng.choice(list(INDIAN_CITIES.keys()))
    lat, lon = INDIAN_CITIES[city]
    avg_amt  = rng.uniform(200, 5_000)

    return {
        "user_id":           user_id,
        "name":              f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}",
        "phone":             f"+91{rng.randint(7_000_000_000, 9_999_999_999)}",
        "email":             f"{user_id}@example.com",
        "home_city":         city,
        "home_latitude":     round(lat  + rng.gauss(0, 0.1), 6),
        "home_longitude":    round(lon  + rng.gauss(0, 0.1), 6),
        "avg_txn_amount":    round(avg_amt, 2),
        "std_txn_amount":    round(avg_amt * rng.uniform(0.2, 0.5), 2),
        "typical_merchants": rng.choices(MERCHANT_CATEGORIES, k=rng.randint(3, 6)),
        "risk_tier":         rng.choices(["LOW", "MEDIUM", "HIGH"], weights=[70, 25, 5])[0],
        "account_age_days":  rng.randint(30, 3_650),
        "updated_at":        int(time.time() * 1000),
    }


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


def main() -> None:
    bootstrap = os.environ["CONFLUENT_BOOTSTRAP_SERVERS"]
    sr_url    = os.environ["CONFLUENT_SCHEMA_REGISTRY_URL"]
    sr_conf: dict = {"url": sr_url}
    sr_key = os.environ.get("CONFLUENT_SCHEMA_REGISTRY_API_KEY", "")
    sr_sec = os.environ.get("CONFLUENT_SCHEMA_REGISTRY_API_SECRET", "")
    if sr_key:
        sr_conf["basic.auth.user.info"] = f"{sr_key}:{sr_sec}"

    schema_registry = SchemaRegistryClient(sr_conf)
    schema_str      = open("schemas/customer_profile.avsc").read()
    serializer      = AvroSerializer(
        schema_registry,
        schema_str,
        conf={"auto.register.schemas": True},
    )
    ctx      = SerializationContext("customer-profiles", MessageField.VALUE)
    producer = _build_producer()

    log.info("seeding_profiles", count=500)
    for i in range(500):
        user_id = f"user_{i:04d}"
        profile = generate_profile(user_id)
        producer.produce(
            topic="customer-profiles",
            key=user_id,
            value=serializer(profile, ctx),
        )
        if i % 100 == 0:
            producer.flush()
            log.info("seeded", count=i)

    producer.flush()
    log.info("seeding_complete", total=500)


if __name__ == "__main__":
    main()
