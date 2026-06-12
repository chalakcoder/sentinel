-- ============================================================
-- 03_risk_scoring.sql
-- Real-time rule-based risk scoring in Flink SQL.
--
-- Weighted additive score over the engineered features from
-- 02_feature_engineering.sql, capped at 1.0:
--   • velocity_flag    → +0.35  (rapid repeated transactions)
--   • geo_jump_flag    → +0.40  (transaction far from home city)
--   • amount_ratio     → up to +0.35 (spend vs customer average)
--   • merchant risk    → up to +0.15 (category inherent risk)
--   • txn frequency    → up to +0.30 (count in 5-min window)
--   • new account      → +0.10  (account < 30 days old)
--
-- Run AFTER: 02_feature_engineering.sql
-- Run BEFORE: 04_fraud_filter.sql
-- ============================================================

INSERT INTO risk_scores
SELECT
    transaction_id,
    user_id,
    amount,
    merchant_category,
    city,
    risk_score,
    CASE
        WHEN risk_score >= 0.90 THEN 'CRITICAL'
        WHEN risk_score >= 0.70 THEN 'HIGH'
        WHEN risk_score >= 0.40 THEN 'MEDIUM'
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
FROM (
    SELECT
        *,
        LEAST(1.0,
            (CASE WHEN velocity_flag THEN 0.35 ELSE 0.0 END) +
            (CASE WHEN geo_jump_flag THEN 0.40 ELSE 0.0 END) +
            (CASE
                WHEN amount_ratio > 15 THEN 0.35
                WHEN amount_ratio > 10 THEN 0.25
                WHEN amount_ratio > 5  THEN 0.15
                WHEN amount_ratio > 3  THEN 0.08
                ELSE 0.0
             END) +
            (merchant_cat_risk * 0.15) +
            (CASE
                WHEN txn_count_5min > 15 THEN 0.30
                WHEN txn_count_5min > 10 THEN 0.20
                WHEN txn_count_5min > 5  THEN 0.10
                ELSE 0.0
             END) +
            (CASE WHEN account_age_days < 30 THEN 0.10 ELSE 0.0 END)
        ) AS risk_score
    FROM enriched_transactions
);
