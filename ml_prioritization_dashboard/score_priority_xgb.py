import os
import json
from pathlib import Path

import numpy as np
import pandas as pd
from google.cloud import bigquery
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

PROJECT      = "boston311-mlops"
DATASET      = "boston311_service"
FEATURE_VIEW = "v_open_case_features"
MODEL_DIR    = BASE_DIR / "models"
BQ_LOCATION  = os.getenv("BQ_LOCATION", "US")

#  Load model & metadata
pipe = joblib.load(MODEL_DIR / "priority_model.pkl")

with open(MODEL_DIR / "feature_columns.json") as f:
    cols = json.load(f)
cat = cols["cat"]
num = cols["num"]

with open(MODEL_DIR / "model_report.json") as f:
    report = json.load(f)
critical_thr = report.get("critical_threshold_at_5pct")

print(f"[INFO] Loaded model from {MODEL_DIR}")
print(f"[INFO] Feature columns: cat={cat}, num={num}")
print(f"[INFO] Validation critical threshold (5%) = {critical_thr}")


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
df = bq.query(query).to_dataframe()

if df.empty:
    print("[INFO] No open cases to score. Exiting.")
    raise SystemExit(0)

print(f"[INFO] Retrieved {len(df)} open cases")

# Build feature matrix & predict scores
X = df[cat + num]

clf = pipe.named_steps.get("clf", pipe.steps[-1][1])
if hasattr(clf, "predict_proba"):
    proba = pipe.predict_proba(X)[:, 1]
else:
    proba = pipe.predict(X)

# Build output frame (meta + score)
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

# Segment & rank
df_out = df_out.sort_values("priority_score", ascending=False)

n_total = len(df_out)
n_crit  = max(1, int(0.05 * n_total))  # top 5%

df_out["segment"] = "SECONDARY"
df_out.iloc[:n_crit, df_out.columns.get_loc("segment")] = "CRITICAL"

df_out["rank_overall"] = np.arange(1, n_total + 1)

print(f"[INFO] Marked {n_crit} of {n_total} cases as CRITICAL (~5%)")
print("[INFO] Sample of scored cases:")
print(df_out.head())

out_path = BASE_DIR / "ml_priority_scores.csv"
df_out.to_csv(out_path, index=False)
print(f"[OK] Wrote scores to {out_path}")

# Write to BigQuery table for live dashboard
table_id = f"{PROJECT}.{DATASET}.cases_ranking_ml"

job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",  
    autodetect=True,                     
)

print(f"[INFO] Loading {len(df_out)} scored rows into {table_id} ...")
job = bq.load_table_from_dataframe(df_out, table_id, job_config=job_config)
job.result()

print("[OK] BigQuery table updated:", table_id)
