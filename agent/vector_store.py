#!/usr/bin/env python3
"""
Initialize ChromaDB with seed fraud case embeddings for similarity search.

Run once before starting the agent service.

Usage:
    python agent/vector_store.py
"""
import os
from dotenv import load_dotenv

load_dotenv()

SEED_CASES = [
    {
        "id":         "CASE-001",
        "text":       "Velocity fraud: 18 UPI transactions in 4 minutes to different ECOMMERCE merchants in Mumbai. "
                      "All amounts under ₹99. Classic card testing pattern to verify stolen card details.",
        "fraud_type": "VELOCITY",
        "outcome":    "BLOCKED",
        "city":       "Mumbai",
    },
    {
        "id":         "CASE-002",
        "text":       "Geo-jump fraud: ATM withdrawal of ₹45,000 in Delhi while customer's last transaction "
                      "was in Bangalore 2 hours ago. Physical distance of 2,150 km makes simultaneous presence impossible.",
        "fraud_type": "GEO_JUMP",
        "outcome":    "BLOCKED",
        "city":       "Delhi",
    },
    {
        "id":         "CASE-003",
        "text":       "Amount anomaly: ECOMMERCE purchase of ₹89,000, customer's 30-day average is ₹1,200. "
                      "74x normal spend. First transaction with this merchant. Account takeover suspected.",
        "fraud_type": "AMOUNT_ANOMALY",
        "outcome":    "BLOCKED",
        "city":       "Hyderabad",
    },
    {
        "id":         "CASE-004",
        "text":       "Suspicious TRAVEL booking ₹35,000 from a new city, customer normally uses GROCERY and RESTAURANT. "
                      "Single transaction, no velocity. Flagged for human review — customer confirmed legitimate.",
        "fraud_type": "AMOUNT_ANOMALY",
        "outcome":    "FALSE_POSITIVE",
        "city":       "Chennai",
    },
    {
        "id":         "CASE-005",
        "text":       "Rapid RECHARGE transactions: 12 mobile recharges in 10 minutes totaling ₹6,000. "
                      "Classic SIM swap fraud precursor — attacker testing new SIM with small top-ups before larger theft.",
        "fraud_type": "VELOCITY",
        "outcome":    "BLOCKED",
        "city":       "Pune",
    },
    {
        "id":         "CASE-006",
        "text":       "ATM withdrawal of ₹50,000 in Kolkata for a customer who lives in Ahmedabad. "
                      "Customer was known to travel for business — called to confirm, transaction was legitimate.",
        "fraud_type": "GEO_JUMP",
        "outcome":    "FALSE_POSITIVE",
        "city":       "Kolkata",
    },
    {
        "id":         "CASE-007",
        "text":       "20 ECOMMERCE transactions in 15 minutes ranging ₹1 to ₹49. All to different merchants. "
                      "Card number used on multiple platforms simultaneously — confirmed stolen card data from breach.",
        "fraud_type": "VELOCITY",
        "outcome":    "BLOCKED",
        "city":       "Bangalore",
    },
    {
        "id":         "CASE-008",
        "text":       "New account (15 days old) made a ₹75,000 TRAVEL booking. "
                      "Account age + high amount anomaly (15x average) triggered alert. "
                      "Investigation revealed synthetic identity fraud.",
        "fraud_type": "AMOUNT_ANOMALY",
        "outcome":    "BLOCKED",
        "city":       "Surat",
    },
]


def initialize_vector_store() -> None:
    import chromadb
    from sentence_transformers import SentenceTransformer

    host       = os.environ.get("CHROMA_HOST", "localhost")
    port       = int(os.environ.get("CHROMA_PORT", 8000))
    collection = os.environ.get("CHROMA_COLLECTION", "fraud_cases")

    print(f"Connecting to ChromaDB at {host}:{port}...")
    client = chromadb.HttpClient(host=host, port=port)

    # Reset collection if exists
    try:
        client.delete_collection(collection)
        print(f"Deleted existing collection '{collection}'")
    except Exception:
        pass

    col = client.create_collection(
        name=collection,
        metadata={"hnsw:space": "cosine"},
    )

    print("Loading sentence transformer model...")
    embedder   = SentenceTransformer("all-MiniLM-L6-v2")
    texts      = [c["text"] for c in SEED_CASES]
    embeddings = embedder.encode(texts).tolist()

    col.add(
        ids=[c["id"] for c in SEED_CASES],
        embeddings=embeddings,
        documents=texts,
        metadatas=[
            {
                "fraud_type": c["fraud_type"],
                "outcome":    c["outcome"],
                "city":       c["city"],
            }
            for c in SEED_CASES
        ],
    )

    print(f"Initialized ChromaDB collection '{collection}' with {len(SEED_CASES)} seed cases.")
    print("Vector store is ready for similarity search.")


if __name__ == "__main__":
    initialize_vector_store()
