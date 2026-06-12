"""
Sentinel FastAPI backend.

Endpoints:
  GET  /health                 → liveness probe
  WS   /ws/events              → real-time event stream for dashboard
  GET  /api/fraud-alerts       → recent fraud alerts from MongoDB
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
    """Recent fraud alerts — written to MongoDB by the Confluent MongoDB Atlas Sink connector."""
    db   = _get_db()
    docs = await (
        db.fraud_alerts
        .find({}, {"_id": 0})
        .sort("created_at", -1)
        .limit(limit)
        .to_list(limit)
    )
    return {"alerts": docs, "count": len(docs)}


@app.get("/api/metrics")
async def get_metrics() -> dict:
    db = _get_db()

    total_alerts    = await db.fraud_alerts.count_documents({})
    critical_alerts = await db.fraud_alerts.count_documents({"risk_score": {"$gte": 0.9}})
    velocity_alerts = await db.fraud_alerts.count_documents({"velocity_flag": True})
    geo_jump_alerts = await db.fraud_alerts.count_documents({"geo_jump_flag": True})

    pipeline = [{"$group": {"_id": None, "avg_risk": {"$avg": "$risk_score"}}}]
    avg_result = await db.fraud_alerts.aggregate(pipeline).to_list(1)
    avg_risk   = round(avg_result[0]["avg_risk"], 3) if avg_result else 0

    return {
        "total_alerts":    total_alerts,
        "critical_alerts": critical_alerts,
        "velocity_alerts": velocity_alerts,
        "geo_jump_alerts": geo_jump_alerts,
        "avg_risk_score":  avg_risk,
    }
