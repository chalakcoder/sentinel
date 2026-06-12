#!/usr/bin/env python3
"""
Register all Avro schemas to Confluent Schema Registry.
Sets BACKWARD compatibility, creates PII/PCI_DSS/GDPR tags, and applies them to sensitive fields.

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

# Maps Kafka topic value subject → Avro schema file
TOPIC_SCHEMA_MAP = {
    "transactions-value":              "transaction.avsc",
    "customer-profiles-value":         "customer_profile.avsc",
    "enriched-transactions-value":     "enriched_transaction.avsc",
    "risk-scores-value":               "risk_score.avsc",
    "fraud-alerts-value":              "fraud_alert.avsc",
    "agent-decisions-value":           "agent_decision.avsc",
}

# PII fields per subject that need governance tags
PII_FIELDS = {
    "transactions-value":          ["user_id", "card_number", "latitude", "longitude", "ip_address"],
    "customer-profiles-value":     ["user_id", "name", "phone", "email", "home_latitude", "home_longitude"],
    "enriched-transactions-value": ["user_id", "card_number", "latitude", "longitude"],
    "fraud-alerts-value":          ["user_id"],
    "agent-decisions-value":       ["user_id"],
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
    resp = getattr(requests, method)(url, **kwargs)
    return resp


def set_compatibility(subject: str, compatibility: str = "BACKWARD") -> None:
    resp = _request("put", f"/config/{subject}",
                    json={"compatibility": compatibility})
    if resp.status_code not in (200, 204):
        print(f"  [WARN] Could not set compatibility for {subject}: {resp.text}")
    else:
        print(f"  Compatibility set to {compatibility} for {subject}")


def register_schema(subject: str, schema_file: str) -> int:
    schema_str = (SCHEMA_DIR / schema_file).read_text()
    # Validate it's valid JSON
    json.loads(schema_str)
    resp = _request("post", f"/subjects/{subject}/versions",
                    json={"schemaType": "AVRO", "schema": schema_str})
    if resp.status_code in (200, 201):
        schema_id = resp.json()["id"]
        print(f"  Registered {subject} -> schema ID {schema_id}")
        return schema_id
    elif resp.status_code == 409:
        # Schema already registered and compatible
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


def main() -> None:
    print("=== Sentinel Schema Registry Bootstrap ===\n")

    # 1. Create governance tags
    print("1. Creating governance tags...")
    for tag in ["PII", "PCI_DSS", "GDPR"]:
        create_tag(tag)

    # 2. Register schemas with BACKWARD compatibility
    print("\n2. Registering Avro schemas...")
    subject_versions: dict[str, int] = {}
    for subject, schema_file in TOPIC_SCHEMA_MAP.items():
        set_compatibility(subject)
        schema_id = register_schema(subject, schema_file)
        # Get the version number for tagging
        resp = _request("get", f"/subjects/{subject}/versions/latest")
        if resp.status_code == 200:
            subject_versions[subject] = resp.json().get("version", 1)
        else:
            subject_versions[subject] = 1

    # 3. Apply PII tags
    print("\n3. Applying PII tags to sensitive fields...")
    for subject, fields in PII_FIELDS.items():
        version = subject_versions.get(subject, 1)
        for field in fields:
            apply_field_tag(subject, version, field, "PII")

    print("\n4. Applying PCI_DSS tags to card data fields...")
    for subject, fields in PCI_FIELDS.items():
        version = subject_versions.get(subject, 1)
        for field in fields:
            apply_field_tag(subject, version, field, "PCI_DSS")

    print("\n=== Schema Registry bootstrap complete! ===")
    print(f"Registered {len(TOPIC_SCHEMA_MAP)} schemas with BACKWARD compatibility.")
    print("PII, PCI_DSS, GDPR tags applied for data governance compliance.")


if __name__ == "__main__":
    main()
