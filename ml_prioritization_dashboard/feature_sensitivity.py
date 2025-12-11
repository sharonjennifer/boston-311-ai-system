import os
import json
from pathlib import Path

import numpy as np
import pandas as pd
from google.cloud import bigquery
import joblib
import shap
import matplotlib.pyplot as plt

# Paths & credentials 
ROOT_DIR = Path(__file__).resolve().parent.parent      # repo root
APP_DIR = Path(__file__).resolve().parent              # ml_prioritization_dashboard
MODEL_DIR = APP_DIR / "models"

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

# Load model
pipe = joblib.load(MODEL_DIR / "priority_model.pkl")
with open(MODEL_DIR / "feature_columns.json") as f:
    cols = json.load(f)

cat = cols["cat"]
num = cols["num"]

pre = pipe.named_steps["pre"]
clf = pipe.named_steps["clf"]

metrics_path = MODEL_DIR / "model_report.json"
if metrics_path.exists():
    with open(metrics_path) as f:
        metrics = json.load(f)
    algo = metrics.get("algo", "unknown")
else:
    metrics = {}
    algo = "unknown"

if algo != "xgboost":
    print(
        f"[WARN] Final model algo in report is '{algo}', not 'xgboost'. "
        "SHAP TreeExplainer is designed for tree models; results may be less meaningful "
        "if this is not an XGBoost model."
    )

# Sample data from BigQuery 
bq = bigquery.Client(project=PROJECT, location=BQ_LOCATION)

query = f"""
  SELECT
    neighborhood, reason, department,
    hour_of_day, day_of_week, month,
    neigh_open_14d,
    prev_repeat_90d_30m,
    dept_pressure_30d
  FROM `{PROJECT}.{DATASET}.{TRAIN_FEATURE_TABLE}`
  LIMIT 2000
"""
df = bq.query(query).to_dataframe(create_bqstorage_client=False)

if df.empty:
    raise RuntimeError("No rows returned for SHAP analysis.")

X_sample = df[cat + num]

print(f"[INFO] Loaded {len(X_sample)} rows for SHAP feature sensitivity")

#  Transform through preprocessor 
X_trans = pre.transform(X_sample)
feature_names = pre.get_feature_names_out(cat + num)

# Compute SHAP values 
print("[INFO] Computing SHAP values (this may take a bit)...")
explainer = shap.TreeExplainer(clf)
shap_values = explainer.shap_values(X_trans)

# For binary classification, shap_values can be list [class0, class1]
if isinstance(shap_values, list):
    # take positive class (index 1)
    shap_values = shap_values[1]

# mean absolute SHAP per transformed feature
mean_abs_shap = np.abs(shap_values).mean(axis=0)

# Aggregate SHAP to original features
agg = {}

for fname, val in zip(feature_names, mean_abs_shap):
    fname = str(fname)

    if fname.startswith("cat__neighborhood"):
        key = "neighborhood"
    elif fname.startswith("cat__reason"):
        key = "reason"
    elif fname.startswith("cat__department"):
        key = "department"
    elif fname.startswith("remainder__"):
        # remainder__hour_of_day -> hour_of_day
        key = fname.split("__", 1)[1]
    else:
        key = fname

    agg[key] = agg.get(key, 0.0) + float(val)

# sort descending
sorted_items = sorted(agg.items(), key=lambda x: x[1], reverse=True)

# save JSON
fi_json = {k: float(v) for k, v in sorted_items}
with open(MODEL_DIR / "feature_importance_shap.json", "w") as f:
    json.dump(fi_json, f, indent=2)

print("[INFO] Saved SHAP importances to feature_importance_shap.json")

#  Plot top-k features 
top_k = min(10, len(sorted_items))
top_feats = [k for k, _ in sorted_items[:top_k]]
top_vals = [v for _, v in sorted_items[:top_k]]

plt.figure(figsize=(8, 5))
# horizontal bar chart, biggest at the top
y_pos = np.arange(top_k)
plt.barh(y_pos, top_vals[::-1])
plt.yticks(y_pos, top_feats[::-1])
plt.xlabel("Mean |SHAP value|")
plt.title("Top Feature Importances (SHAP)")
plt.tight_layout()

out_path = MODEL_DIR / "feature_importance_shap.png"
plt.savefig(out_path)
plt.close()

print(f"[OK] Saved SHAP feature importance plot to {out_path}")
