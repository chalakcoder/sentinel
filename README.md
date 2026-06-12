# Sentinel — Real-Time Fraud & Anomaly Triage Agent

> **Confluent AI Day India 2025 · June 25, Taj MG Road, Bengaluru**
>
> Catches UPI/card fraud in **< 2 seconds** instead of next-day batch review.

---

## Architecture

```
Python Generator (100 txn/s + fraud injection)
        │
        ▼
[transactions] Kafka topic  ←── Avro schema + Schema Registry governance
        │
        ▼
Flink SQL — 02_feature_engineering.sql
  • OVER windows: txn_count_5min, txn_sum_5min, txn_avg_5min
  • Temporal join: LEFT JOIN customer_profiles FOR SYSTEM_TIME AS OF
  • Haversine geo deviation from customer home
  • velocity_flag, geo_jump_flag derived signals
        │
        ▼
[enriched-transactions]
        │
        ▼
Flink SQL — 03_risk_scoring.sql
  • ML_PREDICT('sentinel-fraud-model-v1', ...)   ← ONNX model in Confluent Model Registry
  • Fallback: rule-based weighted scoring
        │
        ▼
[risk-scores]
        │
        ▼
Flink SQL — 04_fraud_filter.sql (risk_score >= 0.7)
        │
        ▼
[fraud-alerts]  ──────────────────────────────────────────────┐
        │                                                       │
        ▼                                                       ▼
LangGraph Agent (IBM WatsonX granite-13b-instruct-v2)    MongoDB Atlas Sink Connector
  fetch_context → assess_risk                                  (Confluent managed connector)
    ├─[BLOCK/FLAG]→ investigate → take_action → explain            │
    └─[APPROVE]──────────────────────────────────┐                 ▼
                                                  ▼           MongoDB Atlas
                                           write_decision     sentinel.fraud_alerts
                                                  │           sentinel.agent_decisions
                                                  ▼
                                        [agent-decisions] + [audit-trail]
                                                  │
                                                  ▼
                                         FastAPI + WebSocket
                                                  │
                                                  ▼
                                        React Dashboard (live)
```

---

## Hackathon Judging Coverage

| Prize | Criterion | How Sentinel covers it |
|-------|-----------|------------------------|
| 1st | Connectors | MongoDB Atlas Sink (platinum sponsor) sinks `fraud-alerts` + `agent-decisions` |
| 1st | Stream Processing | Flink SQL feature engineering: OVER windows, temporal joins, geo computation |
| 1st | Governance | 6 Avro schemas, BACKWARD compat, PII/PCI_DSS/GDPR tags via `register_schemas.py` |
| 2nd | Flink SQL | 4 SQL scripts, 2 OVER windows, temporal table join, ML_PREDICT inference |
| 2nd | AI Model Inference | ONNX GradientBoosting model registered in Confluent Model Registry |
| 3rd | Creativity | LangGraph agent with IBM WatsonX reasoning, vector search for similar cases |

---

## Quick Start

### 1. Prerequisites
- Confluent Cloud account (free tier OK) + Flink compute pool
- MongoDB Atlas cluster (M0 free tier)
- IBM WatsonX account (cloud.ibm.com)
- Python 3.11+, Node 20+

### 2. Configuration
```bash
cp .env.example .env
# Fill in CONFLUENT_*, MONGODB_URI, WATSONX_* credentials
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
cd dashboard && npm install && cd ..
```

### 4. Governance bootstrap (register schemas + PII tags)
```bash
python governance/register_schemas.py
```

### 5. Train and export the fraud detection model
```bash
python model/train_model.py
# Outputs: model/fraud_model.pkl + model/fraud_model.onnx
# Upload fraud_model.onnx to Confluent Cloud:
#   Stream Processing → Models → Create → Upload ONNX
#   Name the model: sentinel-fraud-model-v1
```

### 6. Seed customer profiles
```bash
python producer/seed_profiles.py
```

### 7. Run Flink SQL jobs (Confluent Cloud UI — in order)
```
flink/01_create_tables.sql
flink/02_feature_engineering.sql
flink/03_risk_scoring.sql
flink/04_fraud_filter.sql
```

### 8. Initialize vector store
```bash
python agent/vector_store.py
```

### 9. Start the agent service
```bash
python agent/main.py
```

### 10. Start the API + dashboard
```bash
# Terminal 1
uvicorn api.main:app --port 8080

# Terminal 2
cd dashboard && npm start
```

### 11. Start injecting transactions
```bash
python producer/transaction_generator.py
```

Open **http://localhost:3000** — fraud appears every ~30 seconds, blocked in < 2 seconds.

---

## Project Structure

```
sentinel/
├── schemas/          # 6 Avro schemas (Schema Registry)
├── governance/       # Schema registration + data contracts
├── producer/         # Transaction generator + profile seeder
├── connectors/       # MongoDB Atlas Sink connector configs
├── flink/            # 4 Flink SQL scripts
├── model/            # Fraud detection model (sklearn → ONNX)
├── agent/            # LangGraph agent + IBM WatsonX
├── api/              # FastAPI + WebSocket backend
└── dashboard/        # React live dashboard
```

---

## Business Impact

UPI fraud in India costs **₹1,500+ crore per year** (RBI estimates).
Sentinel reduces detection latency from **next-day batch** to **< 2 seconds**,
enabling real-time blocking before money leaves the account.

At 100 transactions/second with ~1% fraud rate, Sentinel processes **86,400 fraud events/day**
with average agent latency under **2 seconds** end-to-end.