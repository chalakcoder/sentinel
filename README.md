# Sentinel — Real-Time UPI Fraud Detection on Confluent

> Catches UPI/card fraud in **< 2 seconds** instead of next-day batch review.

Built on **Confluent Connectors + Stream Processing + Governance**.

| Capability | How Sentinel uses it |
|------------|----------------------|
| **Connectors** | Fully managed **MongoDB Atlas Sink** sinks `fraud-alerts` into Atlas for case management and dashboard queries |
| **Stream Processing** | Flink SQL: rolling OVER windows (5-min / 1-hour per card), temporal table join with customer profiles, Haversine geo-deviation, weighted risk scoring, alert filtering — 4 SQL jobs |
| **Governance** | 5 Avro schemas in Schema Registry with BACKWARD compatibility, field-level **PII / PCI_DSS / GDPR tags** via the Confluent Tags API, and data contracts with quality rules and SLAs |

---

## Architecture

```
Python Generator (100 txn/s, fraud injected every 30s)
        │
        ▼
[transactions] ──── Avro schema + PII tags (Schema Registry / Governance)
        │
        ▼
Flink SQL 02 — feature engineering
   • OVER windows: txn count/sum/avg per card (5 min + 1 hour)
   • Temporal join: customer_profiles FOR SYSTEM_TIME AS OF
   • Geo deviation (km from home city), amount-vs-average ratio
        │
        ▼
[enriched-transactions]
        │
        ▼
Flink SQL 03 — weighted rule-based risk scoring (0.0 – 1.0)
        │
        ▼
[risk-scores]
        │
        ▼
Flink SQL 04 — filter risk_score ≥ 0.7, attach alert reasons
        │
        ▼
[fraud-alerts] ────────────────► MongoDB Atlas Sink Connector ──► Atlas
        │                                                    (case management)
        ▼
FastAPI WebSocket ──► React Dashboard (live feed, alerts, KPIs)
```

---

## Setup — step by step

### 0. Prerequisites

- **Confluent Cloud** account — free signup with credits: [confluent.cloud](https://confluent.cloud)
- **MongoDB Atlas** account — free M0 cluster: [cloud.mongodb.com](https://cloud.mongodb.com)
- **Python 3.11+** and **Node 20+** locally

### 1. Confluent Cloud: cluster + Flink

1. Create an **Environment** (note the `env-xxxxx` ID).
2. Create a **Basic Kafka cluster** (any region; note the `lkc-xxxxx` ID).
3. Create a **Flink compute pool** in the same region (Environment → Flink → Create compute pool).
4. Create API keys:
   - **Kafka API key**: Cluster → API Keys → Create key
   - **Schema Registry API key**: Environment → Schema Registry → API credentials

### 2. Create the Kafka topics

In Confluent Cloud UI (Cluster → Topics → Create topic), or with the Confluent CLI:

```bash
confluent kafka topic create transactions          --partitions 6
confluent kafka topic create customer-profiles     --partitions 3 --config cleanup.policy=compact
confluent kafka topic create enriched-transactions --partitions 6
confluent kafka topic create risk-scores           --partitions 6
confluent kafka topic create fraud-alerts          --partitions 3
```

### 3. Local configuration

```bash
git clone <this-repo> && cd sentinel
cp .env.example .env
# Edit .env — fill in the Confluent + MongoDB values from steps 1 and 5
pip install -r requirements.txt
```

### 4. Governance: register schemas + PII tags

```bash
python governance/register_schemas.py
```

This registers the producer-owned Avro schemas (`transactions`, `customer-profiles`) with BACKWARD compatibility, creates **PII / PCI_DSS / GDPR** tags, and applies them to sensitive fields (`card_number`, `user_id`, lat/long, phone, email).

The three downstream subjects (`enriched-transactions`, `risk-scores`, `fraud-alerts`) are auto-registered by Flink when the SQL jobs start — **re-run this script after step 8** to tag those too (the script tells you which subjects it skipped).

**Verify**: Confluent Cloud → Environment → Schema Registry → open `transactions-value` → fields show PII tags. See `governance/data_contracts.yaml` for the data quality rules and SLAs.

### 5. MongoDB Atlas

1. Create a free **M0 cluster**.
2. Database Access → create a user (read/write).
3. Network Access → allow access from anywhere (`0.0.0.0/0`) — needed for the Confluent connector.
4. Copy the connection string into `MONGODB_URI` in `.env`.

### 6. Deploy the MongoDB Atlas Sink Connector

Confluent Cloud → Cluster → **Connectors** → search **"MongoDB Atlas Sink"** → configure:

| Setting | Value |
|---------|-------|
| Topics | `fraud-alerts` |
| Kafka credentials | your Kafka API key/secret |
| Connection host | your Atlas host (e.g. `cluster0.abcde.mongodb.net`) |
| Database | `sentinel` |
| Collection | `fraud_alerts` |
| Input message format | AVRO |

(`connectors/mongodb_sink_fraud_alerts.json` documents the full config for reference.)

**Verify**: connector status shows **Running**.

### 7. Seed customer profiles

```bash
python producer/seed_profiles.py
```

**Verify**: `customer-profiles` topic shows 500 messages in the Confluent UI.

### 8. Run the Flink SQL jobs

Confluent Cloud → Environment → **Flink** → open a SQL workspace → run **in order**:

1. `flink/01_create_tables.sql` — table definitions for all 5 topics
2. `flink/02_feature_engineering.sql` — windows + temporal join (long-running INSERT)
3. `flink/03_risk_scoring.sql` — weighted risk scoring (long-running INSERT)
4. `flink/04_fraud_filter.sql` — fraud alert filter (long-running INSERT)

Each INSERT statement becomes a continuously running Flink job — leave them running.

**Verify**: Flink → Statements shows 3 running statements.

### 9. Start the transaction generator

```bash
python producer/transaction_generator.py
```

Generates ~100 realistic Indian UPI/card transactions per second across 10 cities, injecting one of three fraud patterns every 30 seconds:
- **Velocity** — 20 rapid sub-₹99 UPI payments to one merchant
- **Geo-jump** — large ATM withdrawal >800 km from home city
- **Amount anomaly** — purchase 12–20× the customer's average

**Verify**: messages flowing into `transactions`, then `enriched-transactions`, `risk-scores`, and (every ~30s) `fraud-alerts`. Check MongoDB Atlas → `sentinel.fraud_alerts` is filling up.

### 10. Start the API + dashboard

```bash
# Terminal 1
uvicorn api.main:app --host 0.0.0.0 --port 8080

# Terminal 2
cd dashboard && npm install && npm start
```

Open **http://localhost:3000**:
- Live color-coded risk feed and risk-score chart with the 70% alert threshold line
- Fraud alert panel showing amounts, reasons (velocity / geo-jump / amount anomaly), and distance-from-home
- KPI tiles fed by MongoDB (via the connector) and the live Kafka stream

### Local development (no Confluent Cloud)

`docker-compose up -d broker schema-registry mongodb` gives you local Kafka + Schema Registry + MongoDB for testing producers and the API. Flink SQL itself runs only in Confluent Cloud.

---

## Project structure

```
sentinel/
├── schemas/          # 5 Avro schemas (transaction, profile, enriched, score, alert)
├── governance/       # register_schemas.py (PII tags) + data_contracts.yaml
├── producer/         # transaction_generator.py + seed_profiles.py
├── connectors/       # MongoDB Atlas Sink config reference
├── flink/            # 4 Flink SQL jobs (tables, features, scoring, filter)
├── api/              # FastAPI + WebSocket bridge (Kafka → dashboard)
└── dashboard/        # React live dashboard
```
