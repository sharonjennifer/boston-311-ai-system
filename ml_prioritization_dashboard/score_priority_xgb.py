"""
score_priority_xgb.py

Score all currently open 311 cases with the latest
bias-mitigated, hyperparameter-tuned priority model and
write results to:

  - CSV:   ml_priority_scores.csv  (for debugging)
  - BQ:    boston311_service.cases_ranking_ml  (for dashboard)
"""

import os
import json
from pathlib import Path

import numpy as np
import pandas as pd
from google.cloud import bigquery
import joblib

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

PROJECT      = "boston311-mlops"
DATASET      = "boston311_service"
FEATURE_VIEW = "v_open_case_features"

MODEL_DIR   = APP_DIR / "models"
BQ_LOCATION = os.getenv("BQ_LOCATION", "US")

# Load model & metadata 

model_path = MODEL_DIR / "priority_model.pkl"
if not model_path.exists():
    raise FileNotFoundError(
        f"priority_model.pkl not found at {model_path}. "
        "Run train_priority_xgb.py first to train and save the model."
    )

pipe = joblib.load(model_path)

# Feature columns used during training
with open(MODEL_DIR / "feature_columns.json") as f:
    cols = json.load(f)
cat = cols["cat"]
num = cols["num"]

# Prefer bias-mitigated report if present
report_path = MODEL_DIR / "model_report_bias_mitigated.json"
if not report_path.exists():
    report_path = MODEL_DIR / "model_report.json"

with open(report_path) as f:
    report = json.load(f)

critical_thr = report.get("critical_threshold_at_5pct")

print(f"[INFO] Loaded model from {model_path}")
print(f"[INFO] Feature columns: cat={cat}, num={num}")
print(f"[INFO] Critical threshold at 5% (from training) = {critical_thr}")


# Load open-case features from BigQuery

bq = bigquery.Client(project=PROJECT, location=BQ_LOCATION)

query = f"""
SELECT
  case_enquiry_id,
  neighborhood,
  reason,
  department,
  hour_of_day,
  day_of_week,
  month,
  neigh_open_14d,
  prev_repeat_90d_30m,
  dept_pressure_30d,
  latitude,
  longitude
FROM `{PROJECT}.{DATASET}.{FEATURE_VIEW}`
"""
df = bq.query(query).to_dataframe(create_bqstorage_client=False)

if df.empty:
    print("[INFO] No open cases to score. Exiting.")
    raise SystemExit(0)

print(f"[INFO] Retrieved {len(df)} open cases from {FEATURE_VIEW}")

# Build feature matrix & predict scores

X = df[cat + num]

# Get classifier from pipeline (or fall back to last step)
clf = getattr(pipe, "named_steps", {}).get("clf", None)
if clf is None:
    # Fallback: assume last step is the classifier
    clf = pipe.steps[-1][1]

if hasattr(clf, "predict_proba"):
    proba = pipe.predict_proba(X)[:, 1]
else:
    proba = pipe.predict(X)

# Build output frame (meta + score + segment + rank)

meta_cols = [
    "case_enquiry_id",
    "neighborhood",
    "reason",
    "department",
    "neigh_open_14d",
    "prev_repeat_90d_30m",
    "dept_pressure_30d",
    "latitude",
    "longitude",
]

df_out = df[meta_cols].copy()
df_out["priority_score"] = proba

# Rank by score (highest = most critical)
df_out = df_out.sort_values("priority_score", ascending=False).reset_index(drop=True)

n_total = len(df_out)
n_crit  = max(1, int(0.05 * n_total))  # top 5%

df_out["segment"] = "SECONDARY"
df_out.loc[: n_crit - 1, "segment"] = "CRITICAL"

df_out["rank_overall"] = np.arange(1, n_total + 1)

print(f"[INFO] Marked {n_crit} of {n_total} cases as CRITICAL (~5%)")
print("[INFO] Sample of scored cases:")
print(df_out.head())


# Write scores to CSV (debugging / offline analysis)

out_path = APP_DIR / "ml_priority_scores.csv"
df_out.to_csv(out_path, index=False)
print(f"[OK] Wrote scores to {out_path}")

# Write scores to BigQuery for dashboard

table_id = f"{PROJECT}.{DATASET}.cases_ranking_ml"

job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",  # overwrite previous ranking
    autodetect=True,
)

print(f"[INFO] Loading {len(df_out)} scored rows into {table_id} ...")
job = bq.load_table_from_dataframe(df_out, table_id, job_config=job_config)
job.result()

print("[OK] BigQuery table updated:", table_id)
