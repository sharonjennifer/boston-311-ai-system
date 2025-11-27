import os
import json
from pathlib import Path

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

# Paths & credentials
ROOT_DIR = Path(__file__).resolve().parent.parent   
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
BQ_LOCATION = os.getenv("BQ_LOCATION", "US")

MODEL_DIR = APP_DIR / "models"

# Load trained model + feature config
print("[INFO] Loading trained model and feature metadata...")
pipe = joblib.load(MODEL_DIR / "priority_model.pkl")

with open(MODEL_DIR / "feature_columns.json") as f:
    cols = json.load(f)
cat = cols["cat"]
num = cols["num"]

# Load data from BigQuery 
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
    raise RuntimeError("Training table is empty; nothing to run bias checks on.")

print(f"[INFO] Loaded {len(df)} rows from {TRAIN_FEATURE_TABLE} for bias analysis")

# Build features / label
y = df["y_priority"].astype(int)
X = df[cat + num]

# Hold-out test set for bias evaluation
X_train, X_test, y_train, y_test = train_test_split(
    X,
    y,
    test_size=0.2,
    random_state=42,
    stratify=y,
)

# Keep aligned slice columns for the test set
df_test = df.loc[X_test.index].copy()

# Get scores and hard predictions on test
if hasattr(pipe.named_steps["clf"], "predict_proba"):
    test_proba = pipe.predict_proba(X_test)[:, 1]
else:
    test_proba = pipe.predict(X_test)

test_pred = (test_proba >= 0.5).astype(int)


# Define metrics for Fairlearn
metric_fns = {
    "accuracy": accuracy_score,
    "precision": lambda yt, yp: precision_score(yt, yp, zero_division=0),
    "recall": lambda yt, yp: recall_score(yt, yp, zero_division=0),
    "f1": lambda yt, yp: f1_score(yt, yp, zero_division=0),
}

# We will also compute ROC-AUC and PR-AUC per group manually.
def auc_per_group(slice_series, y_true, y_score):
    """Compute ROC-AUC and PR-AUC per group (if both classes present)."""
    groups = slice_series.unique()
    roc_by_group = {}
    pr_by_group = {}
    for g in groups:
        mask = slice_series == g
        yt_g = y_true[mask]
        ys_g = y_score[mask]
        if yt_g.nunique() < 2:
            # AUC undefined if only one class in this group
            roc_by_group[g] = None
            pr_by_group[g] = None
            continue
        roc_by_group[g] = float(roc_auc_score(yt_g, ys_g))
        pr_by_group[g] = float(average_precision_score(yt_g, ys_g))
    return roc_by_group, pr_by_group

# Run bias checks for different slices
slice_columns = []
for col in ["neighborhood", "department", "reason"]:
    if col in df_test.columns:
        slice_columns.append(col)

if not slice_columns:
    raise RuntimeError("No slice columns found for bias analysis.")

bias_report = {}

print("[INFO] Running bias analysis by slices:", ", ".join(slice_columns))

for col in slice_columns:
    print(f"\n[INFO] Analyzing slice: {col}")
    sensitive = df_test[col].astype(str).fillna("UNKNOWN")

    # MetricFrame for classification metrics
    mf = MetricFrame(
        metrics=metric_fns,
        y_true=y_test,
        y_pred=test_pred,
        sensitive_features=sensitive,
    )

    # Overall metrics
    overall_metrics = {m: float(mf.overall[m]) for m in metric_fns.keys()}

    # Metrics by group
    by_group_metrics = {
        m: {str(g): float(v) for g, v in mf.by_group[m].to_dict().items()}
        for m in metric_fns.keys()
    }

    # Compute ROC-AUC / PR-AUC per group
    roc_by_group, pr_by_group = auc_per_group(sensitive, y_test, test_proba)

    # Compute disparities (range between best and worst group for each metric)
    disparities = {}
    warnings = []
    for m, groups_dict in by_group_metrics.items():
        vals = [v for v in groups_dict.values() if v is not None]
        if not vals:
            continue
        min_v, max_v = min(vals), max(vals)
        disparity_range = max_v - min_v
        disparities[m] = {
            "min": min_v,
            "max": max_v,
            "range": disparity_range,
        }
        # Flag large disparities (threshold can be tuned; use 0.1 here)
        if disparity_range > 0.1:
            warn_msg = (
                f"Potential disparity on slice '{col}' for metric '{m}': "
                f"range={disparity_range:.3f} (min={min_v:.3f}, max={max_v:.3f})"
            )
            print("[WARN]", warn_msg)
            warnings.append(warn_msg)

    # Store report for this slice
    bias_report[col] = {
        "overall": overall_metrics,
        "by_group": by_group_metrics,
        "roc_auc_by_group": roc_by_group,
        "pr_auc_by_group": pr_by_group,
        "disparities": disparities,
        "warnings": warnings,
        "mitigation_suggestions": (
            "If large disparities persist, consider strategies such as: "
            "re-sampling under-represented groups, applying group-aware "
            "re-weighting, or using fairness-constrained training "
            "(e.g., via Fairlearn's reduction algorithms)."
            if warnings
            else "No large disparities detected above the configured threshold."
        ),
    }

# Save bias report
out_path = MODEL_DIR / "bias_report.json"
with open(out_path, "w") as f:
    json.dump(bias_report, f, indent=2)

print(f"\n[OK] Bias analysis report written to {out_path}")
