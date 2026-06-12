-- ============================================================
-- 04_fraud_filter.sql
-- Filter high-risk transactions into the fraud-alerts topic.
-- Enriches alerts with the reasons that triggered the alert.
--
-- Threshold: risk_score >= 0.7 (configurable via FRAUD_RISK_THRESHOLD)
-- Run AFTER: 03_risk_scoring.sql
-- ============================================================

INSERT INTO fraud_alerts
SELECT
    -- UUID for this specific alert instance
    CAST(UUID() AS STRING)                              AS alert_id,
    transaction_id,
    user_id,
    amount,
    merchant_category,
    city,
    risk_score,

    -- Build reason array: only non-null entries become alert reasons
    FILTER(
        ARRAY[
            CASE WHEN velocity_flag         THEN 'VELOCITY_FRAUD'   END,
            CASE WHEN geo_jump_flag         THEN 'GEO_JUMP'         END,
            CASE WHEN amount_ratio > 10     THEN 'AMOUNT_ANOMALY'   END,
            CASE WHEN txn_count_5min > 15   THEN 'HIGH_FREQUENCY'   END,
            CASE WHEN risk_score >= 0.90    THEN 'CRITICAL_SCORE'   END,
            CASE WHEN merchant_category = 'ATM' AND geo_jump_flag
                 THEN 'ATM_GEO_JUMP' END
        ],
        x -> x IS NOT NULL
    )                                                   AS alert_reasons,

    txn_count_5min,
    txn_sum_5min,
    geo_deviation_km,
    amount_ratio,
    velocity_flag,
    geo_jump_flag,
    home_city,
    customer_risk_tier,
    account_age_days,
    scored_at                                           AS created_at,
    'PENDING'                                           AS status

FROM risk_scores
WHERE risk_score >= 0.7;
