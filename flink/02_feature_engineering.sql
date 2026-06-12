-- ============================================================
-- 02_feature_engineering.sql
-- Real-time feature engineering using:
--   • OVER windows: rolling 5-min and 1-hour aggregates per card
--   • Temporal table join: enrich with latest customer profile
--   • Haversine geo deviation from customer home
--   • Derived boolean fraud signal flags
--
-- Run AFTER: 01_create_tables.sql and customer-profiles seeding
-- Run BEFORE: 03_risk_scoring.sql
-- ============================================================

-- ── Merchant category static risk lookup ─────────────────────
-- Higher value = higher inherent fraud risk for that merchant type
CREATE TEMPORARY VIEW merchant_risk AS
SELECT * FROM (VALUES
    ('GROCERY',       0.10),
    ('FUEL',          0.20),
    ('RESTAURANT',    0.20),
    ('ECOMMERCE',     0.60),
    ('UTILITY',       0.10),
    ('ATM',           0.80),
    ('TRAVEL',        0.50),
    ('ENTERTAINMENT', 0.40),
    ('MEDICAL',       0.15),
    ('EDUCATION',     0.10),
    ('RECHARGE',      0.30),
    ('OTHER',         0.35)
) AS t(category, cat_risk);

-- ── Main enrichment pipeline ──────────────────────────────────
INSERT INTO enriched_transactions
SELECT
    t.transaction_id,
    t.user_id,
    t.card_number,
    t.amount,
    t.merchant_category,
    t.city,
    t.latitude,
    t.longitude,
    t.`timestamp`,

    -- Rolling window features: transaction velocity per card
    CAST(COUNT(t.transaction_id) OVER w5 AS INT)       AS txn_count_5min,
    COALESCE(SUM(t.amount)       OVER w5, t.amount)    AS txn_sum_5min,
    COALESCE(AVG(t.amount)       OVER w5, t.amount)    AS txn_avg_5min,
    CAST(COUNT(t.transaction_id) OVER w1h AS INT)      AS txn_count_1hour,
    COALESCE(SUM(t.amount)       OVER w1h, t.amount)   AS txn_sum_1hour,

    -- Haversine distance approximation: degrees × 111 km per degree
    -- Adjusted for longitude compression at latitude
    SQRT(
        POWER((t.latitude  - cp.home_latitude)  * 111.0, 2) +
        POWER((t.longitude - cp.home_longitude) * 111.0 *
              COS(RADIANS((t.latitude + cp.home_latitude) / 2.0)), 2)
    )                                                   AS geo_deviation_km,

    -- Spend ratio: how much larger is this transaction vs customer average?
    CASE
        WHEN cp.avg_txn_amount > 0 THEN t.amount / cp.avg_txn_amount
        ELSE 1.0
    END                                                 AS amount_ratio,

    -- Static category risk score
    COALESCE(mr.cat_risk, 0.35)                        AS merchant_cat_risk,

    COALESCE(cp.home_city,         'UNKNOWN')          AS home_city,
    COALESCE(cp.risk_tier,         'LOW')              AS customer_risk_tier,
    COALESCE(cp.account_age_days,  365)                AS account_age_days,

    -- Velocity flag: more than 5 transactions in last 5 minutes on same card
    CAST(COUNT(t.transaction_id) OVER w5 AS INT) > 5   AS velocity_flag,

    -- Geo-jump flag: current city ≠ home city AND distance > 500 km
    (
        t.city <> COALESCE(cp.home_city, t.city)
        AND
        SQRT(
            POWER((t.latitude  - cp.home_latitude)  * 111.0, 2) +
            POWER((t.longitude - cp.home_longitude) * 111.0 *
                  COS(RADIANS((t.latitude + cp.home_latitude) / 2.0)), 2)
        ) > 500.0
    )                                                   AS geo_jump_flag

FROM transactions AS t

-- Temporal join: look up the customer profile valid AT the transaction event time
-- This is the correct pattern for slowly-changing dimension enrichment in Flink
LEFT JOIN customer_profiles FOR SYSTEM_TIME AS OF t.`timestamp` AS cp
    ON t.user_id = cp.user_id

-- Static lookup for merchant category risk
LEFT JOIN merchant_risk AS mr
    ON t.merchant_category = mr.category

-- OVER window: rolling 5-minute window per card number
WINDOW
    w5 AS (
        PARTITION BY t.card_number
        ORDER BY t.`timestamp`
        RANGE BETWEEN INTERVAL '5' MINUTE PRECEDING AND CURRENT ROW
    ),
    w1h AS (
        PARTITION BY t.card_number
        ORDER BY t.`timestamp`
        RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW
    );
