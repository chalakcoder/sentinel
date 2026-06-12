-- ============================================================
-- 03_risk_scoring.sql
-- ML-powered risk scoring using Flink's ML_PREDICT function.
--
-- PRIMARY PATH: ML_PREDICT calls the ONNX model registered in
--   Confluent Model Registry as 'sentinel-fraud-model-v1'.
--   To register: Confluent Cloud → Stream Processing → Models →
--   Upload fraud_model.onnx from model/fraud_model.onnx
--
-- FALLBACK PATH (rule-based): Uncomment the second block if
--   ML_PREDICT is unavailable in your Confluent environment.
--   The rule-based scorer produces equivalent risk distributions.
--
-- Run AFTER: 02_feature_engineering.sql
-- Run BEFORE: 04_fraud_filter.sql
-- ============================================================

-- ── PRIMARY: ML_PREDICT ──────────────────────────────────────
-- Model input features (order must match train_model.py FEATURE_NAMES):
--   [txn_count_5min, txn_sum_5min, geo_deviation_km, amount_ratio,
--    merchant_cat_risk, velocity_flag, geo_jump_flag, account_age_days]
INSERT INTO risk_scores
SELECT
    transaction_id,
    user_id,
    amount,
    merchant_category,
    city,
    CAST(prediction[1] AS DOUBLE)                      AS risk_score,
    CASE
        WHEN CAST(prediction[1] AS DOUBLE) >= 0.90 THEN 'CRITICAL'
        WHEN CAST(prediction[1] AS DOUBLE) >= 0.70 THEN 'HIGH'
        WHEN CAST(prediction[1] AS DOUBLE) >= 0.40 THEN 'MEDIUM'
        ELSE 'LOW'
    END                                                 AS risk_label,
    'sentinel-fraud-model-v1'                           AS model_version,
    txn_count_5min,
    txn_sum_5min,
    geo_deviation_km,
    amount_ratio,
    velocity_flag,
    geo_jump_flag,
    home_city,
    customer_risk_tier,
    account_age_days,
    CURRENT_TIMESTAMP                                   AS scored_at
FROM (
    SELECT
        *,
        ML_PREDICT(
            'sentinel-fraud-model-v1',
            CAST(txn_count_5min    AS FLOAT),
            CAST(txn_sum_5min      AS FLOAT),
            CAST(geo_deviation_km  AS FLOAT),
            CAST(amount_ratio      AS FLOAT),
            CAST(merchant_cat_risk AS FLOAT),
            CAST(velocity_flag  AS INT),
            CAST(geo_jump_flag  AS INT),
            CAST(account_age_days  AS FLOAT)
        ) AS prediction
    FROM enriched_transactions
);


-- ============================================================
-- FALLBACK: Rule-based scoring (uncomment if ML_PREDICT unavailable)
-- Comment out the ML_PREDICT block above before enabling this.
-- ============================================================
/*
INSERT INTO risk_scores
SELECT
    transaction_id,
    user_id,
    amount,
    merchant_category,
    city,
    -- Weighted additive rule-based score, capped at 1.0
    LEAST(1.0,
        -- Velocity component: many rapid transactions
        (CASE WHEN velocity_flag  THEN 0.35 ELSE 0.0 END) +
        -- Geo-jump component: transaction far from home
        (CASE WHEN geo_jump_flag  THEN 0.40 ELSE 0.0 END) +
        -- Amount anomaly component
        (CASE
            WHEN amount_ratio > 15 THEN 0.35
            WHEN amount_ratio > 10 THEN 0.25
            WHEN amount_ratio > 5  THEN 0.15
            WHEN amount_ratio > 3  THEN 0.08
            ELSE 0.0
         END) +
        -- Merchant category inherent risk
        (merchant_cat_risk * 0.15) +
        -- High frequency in 5 min
        (CASE
            WHEN txn_count_5min > 15 THEN 0.30
            WHEN txn_count_5min > 10 THEN 0.20
            WHEN txn_count_5min > 5  THEN 0.10
            ELSE 0.0
         END) +
        -- New account risk factor
        (CASE WHEN account_age_days < 30 THEN 0.10 ELSE 0.0 END)
    )                                                   AS risk_score,
    CASE
        WHEN LEAST(1.0, ...) >= 0.90 THEN 'CRITICAL'
        WHEN LEAST(1.0, ...) >= 0.70 THEN 'HIGH'
        WHEN LEAST(1.0, ...) >= 0.40 THEN 'MEDIUM'
        ELSE 'LOW'
    END                                                 AS risk_label,
    'rule-based-v1'                                     AS model_version,
    txn_count_5min,
    txn_sum_5min,
    geo_deviation_km,
    amount_ratio,
    velocity_flag,
    geo_jump_flag,
    home_city,
    customer_risk_tier,
    account_age_days,
    CURRENT_TIMESTAMP                                   AS scored_at
FROM enriched_transactions;
*/
