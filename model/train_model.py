#!/usr/bin/env python3
"""
Train a fraud detection model on synthetic UPI/card transaction data.

Exports:
  - model/fraud_model.pkl  : scikit-learn Pipeline (scaler + GradientBoosting)
  - model/fraud_model.onnx : ONNX format for Confluent Model Registry (ML_PREDICT)
  - model/model_schema.json: Model metadata for registry upload

Features (must match Flink 03_risk_scoring.sql ML_PREDICT argument order):
  txn_count_5min, txn_sum_5min, geo_deviation_km, amount_ratio,
  merchant_cat_risk, velocity_flag, geo_jump_flag, account_age_days

Usage:
    python model/train_model.py
"""
import json
import pickle
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

MODEL_DIR = Path(__file__).parent

FEATURE_NAMES = [
    "txn_count_5min",
    "txn_sum_5min",
    "geo_deviation_km",
    "amount_ratio",
    "merchant_cat_risk",
    "velocity_flag",
    "geo_jump_flag",
    "account_age_days",
]


def generate_synthetic_data(
    n_normal: int = 9_500,
    n_fraud:  int = 500,
    seed:     int = 42,
) -> pd.DataFrame:
    rng = np.random.default_rng(seed)

    # ── Normal transactions ──────────────────────────────────────
    normal = pd.DataFrame({
        "txn_count_5min":    rng.integers(1, 4, n_normal),
        "txn_sum_5min":      rng.uniform(100, 3_000, n_normal),
        "geo_deviation_km":  rng.uniform(0, 50, n_normal),
        "amount_ratio":      rng.uniform(0.2, 2.5, n_normal),
        "merchant_cat_risk": rng.uniform(0.1, 0.5, n_normal),
        "velocity_flag":     np.zeros(n_normal),
        "geo_jump_flag":     np.zeros(n_normal),
        "account_age_days":  rng.integers(90, 3_650, n_normal),
        "is_fraud":          np.zeros(n_normal),
    })

    # ── Velocity fraud: rapid small UPI transactions ─────────────
    n_vel = n_fraud // 3
    velocity = pd.DataFrame({
        "txn_count_5min":    rng.integers(10, 25, n_vel),
        "txn_sum_5min":      rng.uniform(50, 500, n_vel),
        "geo_deviation_km":  rng.uniform(0, 30, n_vel),
        "amount_ratio":      rng.uniform(0.01, 0.5, n_vel),
        "merchant_cat_risk": rng.uniform(0.5, 0.8, n_vel),
        "velocity_flag":     np.ones(n_vel),
        "geo_jump_flag":     np.zeros(n_vel),
        "account_age_days":  rng.integers(1, 365, n_vel),
        "is_fraud":          np.ones(n_vel),
    })

    # ── Geo-jump fraud: ATM far from home ────────────────────────
    n_geo = n_fraud // 3
    geo = pd.DataFrame({
        "txn_count_5min":    rng.integers(1, 3, n_geo),
        "txn_sum_5min":      rng.uniform(5_000, 50_000, n_geo),
        "geo_deviation_km":  rng.uniform(500, 2_000, n_geo),
        "amount_ratio":      rng.uniform(5, 20, n_geo),
        "merchant_cat_risk": rng.uniform(0.6, 0.9, n_geo),
        "velocity_flag":     np.zeros(n_geo),
        "geo_jump_flag":     np.ones(n_geo),
        "account_age_days":  rng.integers(1, 730, n_geo),
        "is_fraud":          np.ones(n_geo),
    })

    # ── Amount anomaly: single large ECOMMERCE transaction ───────
    n_amt = n_fraud - n_vel - n_geo
    amount = pd.DataFrame({
        "txn_count_5min":    rng.integers(1, 2, n_amt),
        "txn_sum_5min":      rng.uniform(10_000, 100_000, n_amt),
        "geo_deviation_km":  rng.uniform(0, 100, n_amt),
        "amount_ratio":      rng.uniform(12, 25, n_amt),
        "merchant_cat_risk": rng.uniform(0.4, 0.7, n_amt),
        "velocity_flag":     np.zeros(n_amt),
        "geo_jump_flag":     np.zeros(n_amt),
        "account_age_days":  rng.integers(1, 180, n_amt),
        "is_fraud":          np.ones(n_amt),
    })

    df = pd.concat([normal, velocity, geo, amount]).sample(frac=1, random_state=seed)
    return df.reset_index(drop=True)


def train_and_export() -> None:
    print("Generating synthetic training data...")
    df      = generate_synthetic_data()
    X       = df[FEATURE_NAMES].astype(float)
    y       = df["is_fraud"]
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    print(f"Training on {len(X_train)} samples ({int(y_train.sum())} fraud)...")
    pipeline = Pipeline([
        ("scaler", StandardScaler()),
        ("model",  GradientBoostingClassifier(
            n_estimators=100,
            max_depth=4,
            learning_rate=0.1,
            subsample=0.8,
            random_state=42,
        )),
    ])
    pipeline.fit(X_train, y_train)

    y_proba = pipeline.predict_proba(X_test)[:, 1]
    auc     = roc_auc_score(y_test, y_proba)
    print(f"\nAUC-ROC: {auc:.4f}")
    print(classification_report(y_test, pipeline.predict(X_test)))

    if auc < 0.90:
        print("[WARN] AUC below target 0.90 — consider tuning hyperparameters")

    # ── Save pickle ──────────────────────────────────────────────
    pkl_path = MODEL_DIR / "fraud_model.pkl"
    with open(pkl_path, "wb") as f:
        pickle.dump(pipeline, f)
    print(f"Saved pickle: {pkl_path}")

    # ── Export to ONNX ───────────────────────────────────────────
    try:
        from skl2onnx import convert_sklearn
        from skl2onnx.common.data_types import FloatTensorType

        initial_type = [("float_input", FloatTensorType([None, len(FEATURE_NAMES)]))]
        onnx_model   = convert_sklearn(pipeline, initial_types=initial_type,
                                        target_opset=17)
        onnx_path = MODEL_DIR / "fraud_model.onnx"
        with open(onnx_path, "wb") as f:
            f.write(onnx_model.SerializeToString())
        print(f"Saved ONNX: {onnx_path}")
        print("→ Upload fraud_model.onnx to Confluent Cloud:")
        print("  Stream Processing → Models → Create Model → Upload ONNX")
        print("  Name: sentinel-fraud-model-v1")
    except ImportError:
        print("[WARN] skl2onnx not installed — skipping ONNX export")
        print("  Install with: pip install skl2onnx onnx")

    # ── Save model schema metadata ───────────────────────────────
    schema = {
        "model_name":      "sentinel-fraud-model",
        "version":         "1",
        "input_features":  FEATURE_NAMES,
        "output":          "fraud_probability",
        "output_index":    1,
        "framework":       "sklearn/GradientBoosting + ONNX",
        "auc_roc":         round(auc, 4),
        "training_samples": len(X_train),
        "fraud_class_weight": f"1:{int(len(y_train) / y_train.sum())}",
    }
    schema_path = MODEL_DIR / "model_schema.json"
    with open(schema_path, "w") as f:
        json.dump(schema, f, indent=2)
    print(f"Saved model schema: {schema_path}")


def score_example() -> None:
    """Quick sanity check on a known fraud pattern."""
    pkl_path = MODEL_DIR / "fraud_model.pkl"
    if not pkl_path.exists():
        print("Run train_and_export() first")
        return

    with open(pkl_path, "rb") as f:
        pipeline = pickle.load(f)

    # Known fraud: velocity pattern
    fraud_example = pd.DataFrame([{
        "txn_count_5min":    18,
        "txn_sum_5min":      320.0,
        "geo_deviation_km":  5.0,
        "amount_ratio":      0.05,
        "merchant_cat_risk": 0.7,
        "velocity_flag":     1.0,
        "geo_jump_flag":     0.0,
        "account_age_days":  45,
    }])

    score = pipeline.predict_proba(fraud_example)[0][1]
    print(f"\nSanity check — velocity fraud example risk score: {score:.4f}")
    assert score > 0.7, f"Expected score > 0.7 for velocity fraud, got {score:.4f}"
    print("✓ Sanity check passed")


if __name__ == "__main__":
    train_and_export()
    score_example()
