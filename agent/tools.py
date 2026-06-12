"""
LangChain tools for the Sentinel fraud triage agent.

Each tool is a pure async function (or sync for Kafka-only operations)
decorated with @tool. They use lazy singleton clients to avoid
re-connecting on every agent invocation.
"""
import os
import json
import uuid
import time
import logging
from typing import Optional

from langchain.tools import tool
from confluent_kafka import Producer
from motor.motor_asyncio import AsyncIOMotorClient
import chromadb
from sentence_transformers import SentenceTransformer
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

# ── Lazy singletons ───────────────────────────────────────────
_producer:      Optional[Producer]             = None
_mongo_client:  Optional[AsyncIOMotorClient]   = None
_chroma_client: Optional[chromadb.HttpClient]  = None
_embedder:      Optional[SentenceTransformer]  = None


def _get_producer() -> Producer:
    global _producer
    if _producer is None:
        bootstrap = os.environ["CONFLUENT_BOOTSTRAP_SERVERS"]
        conf: dict = {"bootstrap.servers": bootstrap}
        if "confluent.cloud" in bootstrap:
            conf.update({
                "sasl.mechanism":    "PLAIN",
                "security.protocol": "SASL_SSL",
                "sasl.username":     os.environ["CONFLUENT_API_KEY"],
                "sasl.password":     os.environ["CONFLUENT_API_SECRET"],
            })
        _producer = Producer(conf)
    return _producer


def _get_mongo() -> AsyncIOMotorClient:
    global _mongo_client
    if _mongo_client is None:
        _mongo_client = AsyncIOMotorClient(os.environ["MONGODB_URI"])
    return _mongo_client


def _get_chroma() -> chromadb.HttpClient:
    global _chroma_client
    if _chroma_client is None:
        _chroma_client = chromadb.HttpClient(
            host=os.environ.get("CHROMA_HOST", "localhost"),
            port=int(os.environ.get("CHROMA_PORT", 8000)),
        )
    return _chroma_client


def _get_embedder() -> SentenceTransformer:
    global _embedder
    if _embedder is None:
        _embedder = SentenceTransformer("all-MiniLM-L6-v2")
    return _embedder


def _produce(topic: str, key: str, payload: dict) -> None:
    producer = _get_producer()
    producer.produce(
        topic=topic,
        key=key,
        value=json.dumps(payload, default=str).encode(),
    )
    producer.flush(timeout=5)


# ── Tools ─────────────────────────────────────────────────────

@tool
async def get_transaction_history(user_id: str) -> str:
    """
    Retrieve the last 30 transactions for a customer from MongoDB.

    Returns a JSON string with:
    - transaction_count: number of recent transactions found
    - total_spend: sum of amounts in those transactions
    - top_category: most frequent merchant category
    - recent_transactions: last 10 transactions with amount, category, city, risk_score
    """
    try:
        db   = _get_mongo()[os.environ.get("MONGODB_DATABASE", "sentinel")]
        docs = await db.fraud_alerts.find(
            {"user_id": user_id},
            {"transaction_id": 1, "amount": 1, "merchant_category": 1,
             "city": 1, "created_at": 1, "risk_score": 1, "_id": 0},
        ).sort("created_at", -1).limit(30).to_list(30)

        if not docs:
            return json.dumps({
                "user_id": user_id,
                "note": "No flagged transaction history — user may be a first-time alert",
                "transaction_count": 0,
                "total_spend": 0,
                "top_category": "UNKNOWN",
                "recent_transactions": [],
            })

        total    = sum(d.get("amount", 0) for d in docs)
        cats     = [d.get("merchant_category") for d in docs if d.get("merchant_category")]
        top_cat  = max(set(cats), key=cats.count) if cats else "UNKNOWN"

        return json.dumps({
            "user_id":              user_id,
            "transaction_count":    len(docs),
            "total_spend":          round(total, 2),
            "top_category":         top_cat,
            "recent_transactions":  docs[:10],
        }, default=str)

    except Exception as exc:
        logger.error("get_transaction_history error: %s", exc)
        return json.dumps({"error": str(exc), "user_id": user_id})


@tool
async def search_similar_fraud(features_description: str) -> str:
    """
    Search the fraud case vector store for historical cases similar to the current alert.

    Args:
        features_description: Natural language description of the fraud signals,
            e.g. 'velocity fraud, 18 rapid UPI transactions under ₹99, ECOMMERCE, Mumbai'

    Returns JSON with top 3 similar historical fraud cases and their outcomes.
    """
    try:
        embedder   = _get_embedder()
        chroma     = _get_chroma()
        embedding  = embedder.encode([features_description])[0].tolist()
        collection = chroma.get_collection(
            os.environ.get("CHROMA_COLLECTION", "fraud_cases")
        )
        results = collection.query(
            query_embeddings=[embedding],
            n_results=3,
            include=["documents", "metadatas", "distances"],
        )
        cases = [
            {
                "case_description": doc,
                "similarity_score":  round(1.0 - dist, 3),
                "fraud_type":        meta.get("fraud_type"),
                "outcome":           meta.get("outcome"),
            }
            for doc, dist, meta in zip(
                results["documents"][0],
                results["distances"][0],
                results["metadatas"][0],
            )
        ]
        return json.dumps({"similar_cases": cases})

    except Exception as exc:
        logger.warning("search_similar_fraud error: %s", exc)
        return json.dumps({"similar_cases": [], "error": str(exc)})


@tool
def block_transaction(transaction_id: str) -> str:
    """
    Immediately block a transaction by producing to the blocked-transactions topic.

    Use this when you are confident the transaction is fraudulent.

    Args:
        transaction_id: UUID of the transaction to block.

    Returns JSON confirmation with block record ID.
    """
    block_id = str(uuid.uuid4())
    _produce("blocked-transactions", transaction_id, {
        "transaction_id": transaction_id,
        "block_id":       block_id,
        "blocked_at":     int(time.time() * 1000),
        "reason":         "FRAUD_DETECTED_BY_SENTINEL_AGENT",
        "blocked_by":     "sentinel-agent-v1",
    })
    logger.info("Blocked transaction %s (block_id=%s)", transaction_id, block_id)
    return json.dumps({"status": "BLOCKED", "transaction_id": transaction_id, "block_id": block_id})


@tool
def send_notification(user_id: str, message: str) -> str:
    """
    Send an SMS notification to the customer.

    Args:
        user_id: The customer's user ID.
        message: Plain-language message to send (keep under 160 chars for SMS).
                 Should mention what happened and what to do next.

    Returns JSON confirmation.
    """
    notification_id = str(uuid.uuid4())
    _produce("notifications", user_id, {
        "notification_id": notification_id,
        "user_id":         user_id,
        "message":         message,
        "channel":         "SMS",
        "sent_at":         int(time.time() * 1000),
    })
    logger.info("Sent notification to %s", user_id)
    return json.dumps({"status": "SENT", "user_id": user_id, "notification_id": notification_id})


@tool
async def file_fraud_case(
    transaction_id: str,
    user_id: str,
    fraud_type: str,
    description: str,
    risk_score: float,
) -> str:
    """
    File an official fraud case in MongoDB and produce to the fraud-cases topic.

    Args:
        transaction_id: The transaction being reported.
        user_id:        The affected customer.
        fraud_type:     One of VELOCITY, GEO_JUMP, AMOUNT_ANOMALY, SIM_SWAP, OTHER.
        description:    Detailed description of the fraud pattern (2-4 sentences).
        risk_score:     ML model risk score (0.0-1.0).

    Returns JSON with the assigned case ID.
    """
    case_id = f"CASE-{int(time.time())}-{transaction_id[:8].upper()}"
    case    = {
        "case_id":        case_id,
        "transaction_id": transaction_id,
        "user_id":        user_id,
        "fraud_type":     fraud_type,
        "description":    description,
        "risk_score":     risk_score,
        "status":         "OPEN",
        "filed_at":       int(time.time() * 1000),
    }

    try:
        db = _get_mongo()[os.environ.get("MONGODB_DATABASE", "sentinel")]
        await db.fraud_cases.insert_one({**case})
    except Exception as exc:
        logger.error("MongoDB insert failed: %s", exc)

    _produce("fraud-cases", case_id, case)
    logger.info("Filed fraud case %s for %s", case_id, transaction_id)
    return json.dumps({"case_id": case_id, "status": "FILED"})


ALL_TOOLS = [
    get_transaction_history,
    search_similar_fraud,
    block_transaction,
    send_notification,
    file_fraud_case,
]
