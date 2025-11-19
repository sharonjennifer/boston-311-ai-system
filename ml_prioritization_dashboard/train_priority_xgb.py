import os
import json
from pathlib import Path

import numpy as np
import pandas as pd
from google.cloud import bigquery
from sklearn.model_selection import train_test_split
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from sklearn.pipeline import Pipeline
from sklearn.metrics import roc_auc_score, average_precision_score
import joblib

# BASE_DIR
BASE_DIR = Path(__file__).resolve().parent

# PROJECT_ROOT
PROJECT_ROOT = BASE_DIR.parent

SA_PATH = PROJECT_ROOT / "services" / "prioritization-api" / "secrets" / "bq-dashboard-ro.json"

if SA_PATH.exists():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(SA_PATH)
    print(f"[INFO] Using service account key at {SA_PATH}")
else:
    print(
        f"[WARN] Service account file not found at {SA_PATH}. "
        "Falling back to default ADC. Make sure creds are set."
    )

# Config
PROJECT   = "boston311-mlops"
DATASET   = "boston311_service"
TABLE     = "tbl_train_features"      
MODEL_DIR = BASE_DIR / "models"
MODEL_DIR.mkdir(exist_ok=True)

BQ_LOCATION = os.getenv("BQ_LOCATION", "US")

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
  FROM `{PROJECT}.{DATASET}.{TABLE}`
"""
df = bq.query(query).to_dataframe()

if df.empty:
    raise RuntimeError("Training table is empty; nothing to train on.")

# Split features / label
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

X_tr, X_te, y_tr, y_te = train_test_split(
    X,
    y,
    test_size=0.2,
    random_state=42,
    stratify=y,
)

# Preprocessing: OHE for categoricals, passthrough nums
pre = ColumnTransformer(
    transformers=[
        ("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=False), cat),
    ],
    remainder="passthrough",
)

# Model

try:
    from xgboost import XGBClassifier

    model = XGBClassifier(
        n_estimators=500,
        max_depth=6,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        reg_lambda=1.0,
        min_child_weight=1,
        objective="binary:logistic",
        eval_metric="aucpr",
        tree_method="hist",
        n_jobs=0,
        random_state=42,
    )
    algo = "xgboost"
except Exception:
    from sklearn.ensemble import HistGradientBoostingClassifier

    model = HistGradientBoostingClassifier(
        learning_rate=0.08,
        max_depth=8,
        max_bins=255,
        l2_regularization=0.0,
        early_stopping=True,
        random_state=42,
    )
    algo = "hist_gradient_boosting"

# Build pipeline & train
pipe = Pipeline([
    ("pre", pre),
    ("clf", model),
])

print("[INFO] Training model...")
pipe.fit(X_tr, y_tr)

# Evaluate
if hasattr(model, "predict_proba"):
    proba = pipe.predict_proba(X_te)[:, 1]
else:
    proba = pipe.predict(X_te)


def precision_at_k(y_true, p, k=0.05):
    """Precision in the top k fraction sorted by predicted probability."""
    n = max(1, int(len(p) * k))
    idx = np.argsort(-p)[:n]
    return float(y_true[idx].mean())


metrics = {
    "algo": algo,
    "roc_auc": float(roc_auc_score(y_te, proba)),
    "pr_auc": float(average_precision_score(y_te, proba)),
    "precision_at_1pct": precision_at_k(y_te, proba, 0.01),
    "precision_at_5pct": precision_at_k(y_te, proba, 0.05),
    "precision_at_10pct": precision_at_k(y_te, proba, 0.10),
    # score threshold corresponding to top 5% of the test set
    "critical_threshold_at_5pct": float(
        np.sort(proba)[::-1][max(1, int(0.05 * len(proba)) - 1)]
    ),
    # overall positive rate in full dataset
    "label_pos_rate": float(y.mean()),
}

print(json.dumps(metrics, indent=2))

# Save artifacts
joblib.dump(pipe, MODEL_DIR / "priority_model.pkl")

with open(MODEL_DIR / "model_report.json", "w") as f:
    json.dump(metrics, f, indent=2)

with open(MODEL_DIR / "feature_columns.json", "w") as f:
    json.dump({"cat": cat, "num": num}, f, indent=2)

print(f"[OK] Saved model + report in {MODEL_DIR.resolve()}")
