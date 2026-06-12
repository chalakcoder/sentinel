-- ============================================================
-- 01_create_tables.sql
-- Register all Kafka topics as Flink SQL tables in Confluent Cloud.
--
-- Run in: Confluent Cloud → Flink SQL workspace
-- Requirements: Topics must exist and schemas must be registered
--               in Schema Registry before running this script.
-- ============================================================

-- ── Source: raw transactions ──────────────────────────────────
CREATE TABLE transactions (
    transaction_id     STRING,
    user_id            STRING,
    card_number        STRING,
    amount             DOUBLE,
    currency           STRING,
    merchant_id        STRING,
    merchant_name      STRING,
    merchant_category  STRING,
    transaction_type   STRING,
    latitude           DOUBLE,
    longitude          DOUBLE,
    city               STRING,
    device_id          STRING,
    ip_address         STRING,
    `timestamp`        TIMESTAMP(3),
    is_international   BOOLEAN,
    WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '5' SECOND
) WITH (
    'connector'                                          = 'confluent',
    'kafka.topic'                                        = 'transactions',
    'value.format'                                       = 'avro-confluent',
    'value.avro-confluent.schema-registry.subject'       = 'transactions-value',
    'scan.startup.mode'                                  = 'latest-offset'
);

-- ── Source: customer profiles (versioned changelog, keyed by user_id) ─────────
-- Used as a temporal table in the feature engineering join.
-- Seed this topic BEFORE starting the feature engineering job.
CREATE TABLE customer_profiles (
    user_id            STRING,
    name               STRING,
    phone              STRING,
    email              STRING,
    home_city          STRING,
    home_latitude      DOUBLE,
    home_longitude     DOUBLE,
    avg_txn_amount     DOUBLE,
    std_txn_amount     DOUBLE,
    risk_tier          STRING,
    account_age_days   INT,
    updated_at         TIMESTAMP(3),
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector'                                          = 'confluent',
    'kafka.topic'                                        = 'customer-profiles',
    'value.format'                                       = 'avro-confluent',
    'value.avro-confluent.schema-registry.subject'       = 'customer-profiles-value',
    'scan.startup.mode'                                  = 'earliest-offset'
);

-- ── Sink: enriched transactions ───────────────────────────────
CREATE TABLE enriched_transactions (
    transaction_id     STRING,
    user_id            STRING,
    card_number        STRING,
    amount             DOUBLE,
    merchant_category  STRING,
    city               STRING,
    latitude           DOUBLE,
    longitude          DOUBLE,
    `timestamp`        TIMESTAMP(3),
    txn_count_5min     INT,
    txn_sum_5min       DOUBLE,
    txn_avg_5min       DOUBLE,
    txn_count_1hour    INT,
    txn_sum_1hour      DOUBLE,
    geo_deviation_km   DOUBLE,
    amount_ratio       DOUBLE,
    merchant_cat_risk  DOUBLE,
    home_city          STRING,
    customer_risk_tier STRING,
    account_age_days   INT,
    velocity_flag      BOOLEAN,
    geo_jump_flag      BOOLEAN
) WITH (
    'connector'    = 'confluent',
    'kafka.topic'  = 'enriched-transactions',
    'value.format' = 'avro-confluent',
    'value.avro-confluent.schema-registry.subject' = 'enriched-transactions-value'
);

-- ── Sink: risk scores ─────────────────────────────────────────
CREATE TABLE risk_scores (
    transaction_id  STRING,
    user_id         STRING,
    amount          DOUBLE,
    merchant_category STRING,
    city            STRING,
    risk_score      DOUBLE,
    risk_label      STRING,
    model_version   STRING,
    txn_count_5min  INT,
    txn_sum_5min    DOUBLE,
    geo_deviation_km DOUBLE,
    amount_ratio    DOUBLE,
    velocity_flag   BOOLEAN,
    geo_jump_flag   BOOLEAN,
    home_city       STRING,
    customer_risk_tier STRING,
    account_age_days INT,
    scored_at       TIMESTAMP(3)
) WITH (
    'connector'    = 'confluent',
    'kafka.topic'  = 'risk-scores',
    'value.format' = 'avro-confluent'
);

-- ── Sink: fraud alerts (high-risk only) ───────────────────────
CREATE TABLE fraud_alerts (
    alert_id           STRING,
    transaction_id     STRING,
    user_id            STRING,
    amount             DOUBLE,
    merchant_category  STRING,
    city               STRING,
    risk_score         DOUBLE,
    alert_reasons      ARRAY<STRING>,
    txn_count_5min     INT,
    txn_sum_5min       DOUBLE,
    geo_deviation_km   DOUBLE,
    amount_ratio       DOUBLE,
    velocity_flag      BOOLEAN,
    geo_jump_flag      BOOLEAN,
    home_city          STRING,
    customer_risk_tier STRING,
    account_age_days   INT,
    created_at         TIMESTAMP(3),
    status             STRING
) WITH (
    'connector'    = 'confluent',
    'kafka.topic'  = 'fraud-alerts',
    'value.format' = 'json-registry'
);

-- ── Sink: agent decisions ─────────────────────────────────────
CREATE TABLE agent_decisions (
    decision_id      STRING,
    alert_id         STRING,
    transaction_id   STRING,
    user_id          STRING,
    action           STRING,
    confidence       DOUBLE,
    reasoning        STRING,
    latency_ms       BIGINT,
    llm_model        STRING,
    decided_at       TIMESTAMP(3)
) WITH (
    'connector'    = 'confluent',
    'kafka.topic'  = 'agent-decisions',
    'value.format' = 'json-registry'
);
