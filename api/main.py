"""
Sentinel FastAPI backend.

Endpoints:
  GET  /health                 → liveness probe
  WS   /ws/events              → real-time event stream for dashboard
  GET  /api/fraud-alerts       → recent fraud alerts from MongoDB
  GET  /api/agent-decisions    → recent agent decisions from MongoDB
  GET  /api/metrics            → aggregated KPI metrics

Run:
    uvicorn api.main:app --host 0.0.0.0 --port 8080
"""
import asyncio
import logging
import os
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

from api.kafka_consumer import start_consumer_loop, BROADCAST_QUEUES

load_dotenv()
logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
log = structlog.get_logger()


def _get_db():
    client = AsyncIOMotorClient(os.environ["MONGODB_URI"])
    return client[os.environ.get("MONGODB_DATABASE", "sentinel")]


@asynccontextmanager
async def lifespan(app: FastAPI):
    await start_consumer_loop()
    log.info("sentinel_api_started")
    yield
    log.info("sentinel_api_stopping")


app = FastAPI(title="Sentinel API", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── WebSocket: real-time event broadcast ──────────────────────

@app.websocket("/ws/events")
async def websocket_events(websocket: WebSocket) -> None:
    await websocket.accept()
    queue: asyncio.Queue = asyncio.Queue(maxsize=1000)
    BROADCAST_QUEUES.append(queue)
    log.info("ws_client_connected", clients=len(BROADCAST_QUEUES))

    try:
        while True:
            # Wait up to 30 seconds for a new event; send a keepalive ping if idle
            try:
                event = await asyncio.wait_for(queue.get(), timeout=30.0)
                await websocket.send_json(event)
            except asyncio.TimeoutError:
                await websocket.send_json({"type": "ping"})
    except WebSocketDisconnect:
        log.info("ws_client_disconnected")
    except Exception as exc:
        log.error("ws_error", error=str(exc))
    finally:
        if queue in BROADCAST_QUEUES:
            BROADCAST_QUEUES.remove(queue)


# ── REST endpoints ────────────────────────────────────────────

@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "service": "sentinel-api"}


@app.get("/api/fraud-alerts")
async def get_fraud_alerts(limit: int = 20) -> dict:
    db   = _get_db()
    docs = await (
        db.fraud_alerts
        .find({}, {"_id": 0})
        .sort("created_at", -1)
        .limit(limit)
        .to_list(limit)
    )
    return {"alerts": docs, "count": len(docs)}


@app.get("/api/agent-decisions")
async def get_agent_decisions(limit: int = 20) -> dict:
    db   = _get_db()
    docs = await (
        db.agent_decisions
        .find({}, {"_id": 0})
        .sort("decided_at", -1)
        .limit(limit)
        .to_list(limit)
    )
    return {"decisions": docs, "count": len(docs)}


@app.get("/api/metrics")
async def get_metrics() -> dict:
    db = _get_db()

    total_alerts   = await db.fraud_alerts.count_documents({})
    total_decisions = await db.agent_decisions.count_documents({})
    blocked        = await db.agent_decisions.count_documents({"action": "BLOCK"})
    flagged        = await db.agent_decisions.count_documents({"action": "FLAG"})
    approved       = await db.agent_decisions.count_documents({"action": "APPROVE"})

    pipeline = [{"$group": {"_id": None, "avg_latency": {"$avg": "$latency_ms"}}}]
    lat_result = await db.agent_decisions.aggregate(pipeline).to_list(1)
    avg_latency = round(lat_result[0]["avg_latency"], 0) if lat_result else 0

    return {
        "total_alerts":     total_alerts,
        "total_decisions":  total_decisions,
        "blocked":          blocked,
        "flagged":          flagged,
        "approved":         approved,
        "avg_latency_ms":   avg_latency,
        "block_rate":       round(blocked / total_decisions, 3) if total_decisions else 0,
    }


@app.get("/api/fraud-cases")
async def get_fraud_cases(limit: int = 20) -> dict:
    db   = _get_db()
    docs = await (
        db.fraud_cases
        .find({}, {"_id": 0})
        .sort("filed_at", -1)
        .limit(limit)
        .to_list(limit)
    )
    return {"cases": docs, "count": len(docs)}
