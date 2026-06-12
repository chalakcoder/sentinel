#!/usr/bin/env python3
"""
Schema Registry governance bootstrap.

1. Registers the producer-owned Avro schemas (transactions, customer-profiles)
   with BACKWARD compatibility.
2. Creates PII / PCI_DSS / GDPR tags in the Confluent tag catalog.
3. Applies field-level tags to all subjects — including the Flink-derived
   subjects (enriched-transactions, risk-scores, fraud-alerts), which are
   auto-registered by Flink when the SQL jobs start.

Run once before starting the Flink jobs, then run AGAIN after the Flink
jobs are running to tag the Flink-derived subjects.

Usage:
    python governance/register_schemas.py
"""
import os
import sys
import json
import requests
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

SCHEMA_REGISTRY_URL = os.environ["CONFLUENT_SCHEMA_REGISTRY_URL"].rstrip("/")
SR_API_KEY = os.environ.get("CONFLUENT_SCHEMA_REGISTRY_API_KEY", "")
SR_API_SECRET = os.environ.get("CONFLUENT_SCHEMA_REGISTRY_API_SECRET", "")

# Use basic auth only if credentials are provided (Confluent Cloud requires them)
AUTH = (SR_API_KEY, SR_API_SECRET) if SR_API_KEY else None

# Producer-owned subjects: registered from hand-written .avsc files.
# (Flink sink subjects — enriched-transactions, risk-scores, fraud-alerts —
#  are auto-registered by Flink from the table DDL; we only tag those.)
TOPIC_SCHEMA_MAP = {
    "transactions-value":          "transaction.avsc",
    "customer-profiles-value":     "customer_profile.avsc",
}

# PII fields per subject for governance tagging
PII_FIELDS = {
    # producer-owned
    "transactions-value":          ["user_id", "card_number", "latitude", "longitude", "ip_address"],
    "customer-profiles-value":     ["user_id", "name", "phone", "email", "home_latitude", "home_longitude"],
    # Flink-derived (exist only after the Flink jobs have started)
    "enriched-transactions-value": ["user_id", "card_number", "latitude", "longitude"],
    "risk-scores-value":           ["user_id"],
    "fraud-alerts-value":          ["user_id"],
}

PCI_FIELDS = {
    "transactions-value":          ["card_number"],
    "enriched-transactions-value": ["card_number"],
}

SCHEMA_DIR = Path(__file__).parent.parent / "schemas"


def _request(method: str, path: str, **kwargs) -> requests.Response:
    url = f"{SCHEMA_REGISTRY_URL}{path}"
    kwargs.setdefault("headers", {"Content-Type": "application/vnd.schemaregistry.v1+json"})
    if AUTH:
        kwargs["auth"] = AUTH
    return getattr(requests, method)(url, **kwargs)


def subject_exists(subject: str) -> bool:
    return _request("get", f"/subjects/{subject}/versions/latest").status_code == 200


def set_compatibility(subject: str, compatibility: str = "BACKWARD") -> None:
    resp = _request("put", f"/config/{subject}",
                    json={"compatibility": compatibility})
    if resp.status_code not in (200, 204):
        print(f"  [WARN] Could not set compatibility for {subject}: {resp.text}")
    else:
        print(f"  Compatibility set to {compatibility} for {subject}")


def register_schema(subject: str, schema_file: str) -> int:
    schema_str = (SCHEMA_DIR / schema_file).read_text()
    json.loads(schema_str)  # validate JSON before sending
    resp = _request("post", f"/subjects/{subject}/versions",
                    json={"schemaType": "AVRO", "schema": schema_str})
    if resp.status_code in (200, 201):
        schema_id = resp.json()["id"]
        print(f"  Registered {subject} -> schema ID {schema_id}")
        return schema_id
    elif resp.status_code == 409:
        resp2 = _request("get", f"/subjects/{subject}/versions/latest")
        schema_id = resp2.json().get("id", -1)
        print(f"  Already registered {subject} (ID {schema_id})")
        return schema_id
    else:
        print(f"  [ERROR] Failed to register {subject}: {resp.text}", file=sys.stderr)
        return -1


def create_tag(tag_name: str) -> None:
    resp = _request("post", "/catalog/v1/types/tagdefs",
                    json=[{
                        "name": tag_name,
                        "entityTypes": ["cf_field", "cf_topic"],
                        "attributeDefs": [
                            {"name": "description", "typeName": "string", "isOptional": True}
                        ]
                    }])
    if resp.status_code in (200, 201, 409):
        print(f"  Tag '{tag_name}' ready")
    else:
        print(f"  [WARN] Could not create tag '{tag_name}': {resp.text}")


def apply_field_tag(subject: str, version: int, field: str, tag: str) -> None:
    entity_name = f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions/{version}#{field}"
    resp = _request("post", "/catalog/v1/entity/tags",
                    json=[{
                        "typeName": tag,
                        "entityType": "cf_field",
                        "entityName": entity_name
                    }])
    if resp.status_code in (200, 201, 204):
        print(f"    Tagged {subject}#{field} with [{tag}]")
    else:
        print(f"    [WARN] Could not tag {subject}#{field}: {resp.text}")


def tag_subject_fields(field_map: dict, tag: str) -> list[str]:
    """Apply a tag to every (subject, field) pair; skip subjects not yet registered."""
    skipped = []
    for subject, fields in field_map.items():
        if not subject_exists(subject):
            skipped.append(subject)
            continue
        resp = _request("get", f"/subjects/{subject}/versions/latest")
        version = resp.json().get("version", 1)
        for field in fields:
            apply_field_tag(subject, version, field, tag)
    return skipped


def main() -> None:
    print("=== Sentinel Schema Registry Governance Bootstrap ===\n")

    print("1. Creating governance tags...")
    for tag in ["PII", "PCI_DSS", "GDPR"]:
        create_tag(tag)

    print("\n2. Registering producer-owned Avro schemas...")
    for subject, schema_file in TOPIC_SCHEMA_MAP.items():
        set_compatibility(subject)
        register_schema(subject, schema_file)

    print("\n3. Applying PII tags to sensitive fields...")
    skipped = tag_subject_fields(PII_FIELDS, "PII")

    print("\n4. Applying PCI_DSS tags to card data fields...")
    skipped += tag_subject_fields(PCI_FIELDS, "PCI_DSS")

    print("\n=== Governance bootstrap complete ===")
    if skipped:
        print("\n[NOTE] These subjects are not registered yet (created by Flink "
              "when the SQL jobs start):")
        for s in sorted(set(skipped)):
            print(f"  - {s}")
        print("Re-run this script after starting the Flink jobs to tag them.")


if __name__ == "__main__":
    main()
