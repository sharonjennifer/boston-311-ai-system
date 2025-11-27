"""
bias_mitigation.py

Evaluate the bias-mitigated priority model by slicing performance across
neighborhood, department, and reason on the hold-out test set.

It recomputes the same train/val/test split (random_state=42),
gets predictions on the test set, and uses Fairlearn's MetricFrame
to compute metrics by slice.

Outputs:
  - ml_prioritization_dashboard/models/bias_report_mitigated.json
"""

import os
import json
from pathlib import Path
import math

import numpy as np
import pandas as pd
from google.cloud import bigquery
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    roc_auc_score,
    average_precision_score,
)
import joblib

from fairlearn.metrics import MetricFrame

# ROOT_DIR = repo root (.../boston-311-ai-system)
ROOT_DIR = Path(__file__).resolve().parent.parent
# APP_DIR  = ml_prioritization_dashboard folder
APP_DIR = Path(__file__).resolve().parent

SA_PATH = ROOT_DIR / "secrets" / "bq-dashboard-ro.json"

if SA_PATH.exists():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(SA_PATH)
    print(f"[INFO] Using service account key at {SA_PATH}")
else:
    print(
        f"[WARN] Service account file not found at {SA_PATH}. "
        "Falling back to default ADC. Make sure creds are set."
    )

PROJECT = "boston311-mlops"
DATASET = "boston311_service"
TRAIN_FEATURE_TABLE = os.getenv("TRAIN_FEATURE_TABLE", "tbl_train_features")

MODEL_DIR = APP_DIR / "models"
MODEL_DIR.mkdir(exist_ok=True)

BQ_LOCATION = os.getenv("BQ_LOCATION", "US")


def _clean_nans(obj):
    """
    Recursively replace NaN / +/-inf with None so that the output
    is valid JSON and editors/tools don't complain.
    """
    # Handle numpy scalar types as well
    if isinstance(obj, (float, np.floating)):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return float(obj)

    if isinstance(obj, dict):
        return {k: _clean_nans(v) for k, v in obj.items()}

    if isinstance(obj, list):
        return [_clean_nans(v) for v in obj]

    return obj


def compute_slice_report(feature_name, X_test, y_test, y_pred, y_score):
    """
    Compute overall + per-group metrics and disparities for a single
    categorical feature (e.g., 'neighborhood', 'department', 'reason').

    Returns:
        dict with keys:
          - overall: {accuracy, precision, recall, f1}
          - by_group: {metric_name -> {group -> value}}
          - roc_auc_by_group: {group -> value or None}
          - pr_auc_by_group: {group -> value or None}
          - disparities: {metric_name -> {min, max, range}}
          - warnings: [list of human-readable disparity warnings]
          - mitigation_suggestions: generic text about possible mitigation
    """
    print(f"[INFO] Computing slice metrics for '{feature_name}' ...")

    sensitive = X_test[feature_name].fillna("None")

    metrics_fns = {
        "accuracy": accuracy_score,
        "precision": lambda yt, yp: precision_score(yt, yp, zero_division=0),
        "recall": lambda yt, yp: recall_score(yt, yp, zero_division=0),
        "f1": lambda yt, yp: f1_score(yt, yp, zero_division=0),
    }

    # Fairlearn MetricFrame over groups
    mf = MetricFrame(
        metrics=metrics_fns,
        y_true=y_test,
        y_pred=y_pred,
        sensitive_features=sensitive,
    )

    # Overall metrics
    overall = {
        "accuracy": float(accuracy_score(y_test, y_pred)),
        "precision": float(precision_score(y_test, y_pred, zero_division=0)),
        "recall": float(recall_score(y_test, y_pred, zero_division=0)),
        "f1": float(f1_score(y_test, y_pred, zero_division=0)),
    }

    # Per-group metrics
    by_group = {"accuracy": {}, "precision": {}, "recall": {}, "f1": {}}
    for metric_name in by_group.keys():
        ser = mf.by_group[metric_name]
        for grp in ser.index:
            by_group[metric_name][str(grp)] = float(ser.loc[grp])

    # Per-group ROC-AUC & PR-AUC (need probabilities)
    groups = sensitive.unique()
    roc_auc_by_group = {}
    pr_auc_by_group = {}

    for g in groups:
        mask = (sensitive == g).to_numpy()
        n = int(mask.sum())
        g_str = str(g)

        if n < 2:
            # too few samples to compute AUC meaningfully
            roc_auc_by_group[g_str] = None
            pr_auc_by_group[g_str] = None
            continue

        try:
            roc_auc_by_group[g_str] = float(
                roc_auc_score(y_test[mask], y_score[mask])
            )
        except ValueError:
            roc_auc_by_group[g_str] = None

        try:
            pr_auc_by_group[g_str] = float(
                average_precision_score(y_test[mask], y_score[mask])
            )
        except ValueError:
            pr_auc_by_group[g_str] = None

    # Disparities (min, max, range) per metric
    disparities = {}
    warnings = []

    for metric_name, group_values in by_group.items():
        vals = [v for v in group_values.values() if v is not None]
        if not vals:
            continue
        min_v = float(min(vals))
        max_v = float(max(vals))
        range_v = max_v - min_v
        disparities[metric_name] = {
            "min": min_v,
            "max": max_v,
            "range": range_v,
        }

        # Flag disparities that might be concerning
        # (threshold can be tuned; 0.2 is a reasonable default)
        if range_v >= 0.2:
            warnings.append(
                f"Potential disparity on slice '{feature_name}' for metric "
                f"'{metric_name}': range={range_v:.3f} "
                f"(min={min_v:.3f}, max={max_v:.3f})"
            )

    mitigation_suggestions = (
        "If large disparities persist, consider strategies such as: "
        "re-sampling under-represented groups, applying group-aware "
        "re-weighting, or using fairness-constrained training "
        "(e.g., via Fairlearn's reduction algorithms)."
    )

    return {
        "overall": overall,
        "by_group": by_group,
        "roc_auc_by_group": roc_auc_by_group,
        "pr_auc_by_group": pr_auc_by_group,
        "disparities": disparities,
        "warnings": warnings,
        "mitigation_suggestions": mitigation_suggestions,
    }


# Main: load model, rebuild test set, compute slice metrics
if __name__ == "__main__":
    # 1. Load trained (bias-mitigated) model
    model_path = MODEL_DIR / "priority_model.pkl"
    if not model_path.exists():
        raise FileNotFoundError(
            f"Trained model not found at {model_path}. "
            "Run train_priority_xgb.py first."
        )

    print(f"[INFO] Loading trained model from {model_path} ...")
    pipe = joblib.load(model_path)

    # 2. Load data from BigQuery
    print("[INFO] Loading training data from BigQuery ...")
    bq = bigquery.Client(project=PROJECT, location=BQ_LOCATION)

    query = f"""
      SELECT
        y_priority,
        neighborhood, reason, department,
        hour_of_day, day_of_week, month,
        neigh_open_14d,
        prev_repeat_90d_30m,
        dept_pressure_30d
      FROM `{PROJECT}.{DATASET}.{TRAIN_FEATURE_TABLE}`
    """
    df = bq.query(query).to_dataframe()

    if df.empty:
        raise RuntimeError("Training table is empty; nothing to evaluate.")

    print(f"[INFO] Loaded {len(df)} rows from {TRAIN_FEATURE_TABLE}")

    # 3. Rebuild label + features and reproduce the test split (same as training)
    y = df["y_priority"].astype(int).to_numpy()
    cat = ["neighborhood", "reason", "department"]
    num = [
        "hour_of_day",
        "day_of_week",
        "month",
        "neigh_open_14d",
        "prev_repeat_90d_30m",
        "dept_pressure_30d",
    ]
    X = df[cat + num]

    # Split off 20% test, same random_state and stratify as training script
    X_train_full, X_test, y_train_full, y_test = train_test_split(
        X,
        y,
        test_size=0.2,
        random_state=42,
        stratify=y,
    )

    print(f"[INFO] Test set size: {len(X_test)}")

    # 4. Get predictions & probabilities on the test set
    clf = pipe.named_steps.get("clf", pipe)

    if hasattr(clf, "predict_proba"):
        y_score = pipe.predict_proba(X_test)[:, 1]
        y_pred = (y_score >= 0.5).astype(int)
    else:
        # models without predict_proba (should not happen for XGB here)
        y_score = pipe.predict(X_test)
        y_pred = (y_score >= 0.5).astype(int)

    # 5. Compute slice reports for neighborhood, department, reason
    report = {}
    for feature in ["neighborhood", "department", "reason"]:
        report[feature] = compute_slice_report(
            feature_name=feature,
            X_test=X_test,
            y_test=y_test,
            y_pred=y_pred,
            y_score=y_score,
        )

    # 6. Save mitigated bias report (cleaned to be strict JSON)
    out_name = "bias_report_mitigated.json"
    out_path = MODEL_DIR / out_name

    clean_report = _clean_nans(report)

    with open(out_path, "w") as f:
        json.dump(clean_report, f, indent=2)

    print(f"[OK] Saved mitigated bias report to {out_path.resolve()}")
