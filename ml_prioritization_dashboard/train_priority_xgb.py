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
from sklearn.metrics import ( roc_auc_score, average_precision_score, accuracy_score, precision_score, recall_score, f1_score, confusion_matrix, )
import joblib
import datetime
import tarfile
from google.cloud import storage
import matplotlib.pyplot as plt
import mlflow
import mlflow.sklearn

# Paths & credentials

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

# Config: training data from pipeline BigQuery dataset
PROJECT = "boston311-mlops"
DATASET = "boston311_service"
TRAIN_FEATURE_TABLE = os.getenv("TRAIN_FEATURE_TABLE", "tbl_train_features")

MODEL_DIR = Path(__file__).resolve().parent / "models"
MODEL_DIR.mkdir(exist_ok=True)

MODEL_NAME = "priority_xgb"
REGISTRY_BUCKET = os.getenv("MODEL_REGISTRY_BUCKET", "boston311-ml-model-registry")

# MLflow tracking
MLFLOW_TRACKING_URI = str(ROOT_DIR / "mlruns")
MLFLOW_EXPERIMENT_NAME = "priority_xgb_ci"

mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
mlflow.set_experiment(MLFLOW_EXPERIMENT_NAME)

BQ_LOCATION = os.getenv("BQ_LOCATION", "US")

# Helper: push model artifacts to GCS registry
def push_model_to_registry(model_dir: Path, metrics: dict) -> None:
    """
    Package model artifacts and push them to a GCS bucket acting as a model registry.

    Layout:
      gs://<REGISTRY_BUCKET>/<MODEL_NAME>/<version>/model_artifacts.tar.gz
      gs://<REGISTRY_BUCKET>/<MODEL_NAME>/<version>/metadata.json
    """
    if not REGISTRY_BUCKET:
        print("[WARN] MODEL_REGISTRY_BUCKET not set; skipping registry upload.")
        return

    ts = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    algo = metrics.get("algo", "unknown")
    version = f"{MODEL_NAME}_{algo}_{ts}"

    archive_name = f"{version}.tar.gz"
    archive_path = model_dir / archive_name

    artifact_files = [
        # core model + metadata
        "priority_model.pkl",
        "model_report.json",
        "model_report_bias_mitigated.json",
        "feature_columns.json",
        "priority_xgb_experiments.json",
        # bias analysis
        "bias_report.json",
        "bias_report_mitigated.json",
        # evaluation visuals
        "confusion_matrix_test.png",
        "model_comparison_val_roc_auc.png",
        "model_comparison_val_pr_auc.png",
        # feature sensitivity / SHAP
        "feature_importance_shap.json",
        "feature_importance_shap.png",
    ]

    print(f"[INFO] Creating model artifact archive {archive_path} ...")
    with tarfile.open(archive_path, "w:gz") as tar:
        for fname in artifact_files:
            fpath = model_dir / fname
            if fpath.exists():
                tar.add(fpath, arcname=fname)
            else:
                print(f"[WARN] Artifact {fname} not found; skipping in archive.")

    print(
        f"[INFO] Uploading model artifacts to "
        f"gs://{REGISTRY_BUCKET}/{MODEL_NAME}/{version}/ ..."
    )
    storage_client = storage.Client(project=PROJECT)
    bucket = storage_client.bucket(REGISTRY_BUCKET)

    # upload tar.gz
    blob = bucket.blob(f"{MODEL_NAME}/{version}/model_artifacts.tar.gz")
    blob.upload_from_filename(str(archive_path))

    # upload metadata.json with metrics + context
    metadata = {
        "model_name": MODEL_NAME,
        "version": version,
        "created_utc": ts,
        "project": PROJECT,
        "dataset": DATASET,
        "table": TRAIN_FEATURE_TABLE,
        "metrics": metrics,
    }
    metadata_blob = bucket.blob(f"{MODEL_NAME}/{version}/metadata.json")
    metadata_blob.upload_from_string(
        json.dumps(metadata, indent=2),
        content_type="application/json",
    )

    print(f"[OK] Model version {version} pushed to model registry bucket {REGISTRY_BUCKET}")


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
df = bq.query(query).to_dataframe(create_bqstorage_client=False)

if df.empty:
    raise RuntimeError("Training table is empty; nothing to train on.")

print(f"[INFO] Loaded {len(df)} training rows from {TRAIN_FEATURE_TABLE}")

# Features / label
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

# Bias mitigation: re-weighting by neighborhood, department, reason
MAX_DIM_WEIGHT = 3.0       # per-dimension cap
MAX_GLOBAL_WEIGHT = 5.0    # cap final combined weight

neigh_series = X["neighborhood"].fillna("None")
dept_series = X["department"].fillna("None")
reason_series = X["reason"].fillna("None")

# neighborhood weights
neigh_counts = neigh_series.value_counts()
neigh_raw = neigh_series.map(lambda n: neigh_counts.max() / neigh_counts[n])
neigh_w = neigh_raw.clip(upper=MAX_DIM_WEIGHT)

# department weights
dept_counts = dept_series.value_counts()
dept_raw = dept_series.map(lambda d: dept_counts.max() / dept_counts[d])
dept_w = dept_raw.clip(upper=MAX_DIM_WEIGHT)

# reason weights
reason_counts = reason_series.value_counts()
reason_raw = reason_series.map(lambda r: reason_counts.max() / reason_counts[r])
reason_w = reason_raw.clip(upper=MAX_DIM_WEIGHT)

# combine + normalise
combined_raw = neigh_w * dept_w * reason_w
combined_norm = combined_raw / combined_raw.mean()
sample_weight_full = combined_norm.clip(upper=MAX_GLOBAL_WEIGHT).to_numpy()

# Train / Val / Test split  (60 / 20 / 20)
X_train_full, X_test, y_train_full, y_test, w_train_full, w_test_unused = train_test_split(
    X,
    y,
    sample_weight_full,
    test_size=0.2,
    random_state=42,
    stratify=y,
)

X_train, X_val, y_train, y_val, w_train, w_val = train_test_split(
    X_train_full,
    y_train_full,
    w_train_full,
    test_size=0.25,   
    random_state=42,
    stratify=y_train_full,
)

print(
    f"[INFO] Split data: "
    f"train={len(X_train)}, val={len(X_val)}, test={len(X_test)}"
)

# Preprocessing and candidate models
pre = ColumnTransformer(
    transformers=[
        ("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=False), cat),
    ],
    remainder="passthrough",
)

candidates = []

try:
    from xgboost import XGBClassifier

    candidates = [
        (
            "xgb_baseline",
            XGBClassifier(
                n_estimators=300,
                max_depth=5,
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
            ),
            "xgboost",
        ),
        (
            "xgb_deeper",
            XGBClassifier(
                n_estimators=500,
                max_depth=7,
                learning_rate=0.03,
                subsample=0.9,
                colsample_bytree=0.9,
                reg_lambda=1.0,
                min_child_weight=2,
                objective="binary:logistic",
                eval_metric="aucpr",
                tree_method="hist",
                n_jobs=0,
                random_state=42,
            ),
            "xgboost",
        ),
    ]
except Exception:
    from sklearn.ensemble import HistGradientBoostingClassifier

    candidates = [
        (
            "hgb_baseline",
            HistGradientBoostingClassifier(
                learning_rate=0.08,
                max_depth=8,
                max_bins=255,
                l2_regularization=0.0,
                early_stopping=True,
                random_state=42,
            ),
            "hist_gradient_boosting",
        ),
        (
            "hgb_regularized",
            HistGradientBoostingClassifier(
                learning_rate=0.05,
                max_depth=8,
                max_bins=255,
                l2_regularization=0.5,
                early_stopping=True,
                random_state=42,
            ),
            "hist_gradient_boosting",
        ),
    ]


def precision_at_k(y_true, p, k=0.05):
    """Precision in the top k fraction sorted by predicted probability."""
    n = max(1, int(len(p) * k))
    idx = np.argsort(-p)[:n]
    return float(y_true[idx].mean())

# Model selection on validation set
best_name = None
best_algo = None
best_pipe = None
best_val_pr_auc = -1.0
best_val_metrics = {}
all_runs = []

print("[INFO] Starting model selection over candidates...")

for name, base_model, algo in candidates:
    pipe = Pipeline([
        ("pre", pre),
        ("clf", base_model),
    ])

    print(f"[INFO] Training candidate model: {name} ({algo})")

    with mlflow.start_run(run_name=name):
        mlflow.set_tag("model_name", MODEL_NAME)
        mlflow.set_tag("algo", algo)
        mlflow.log_param("candidate_name", name)

        params = getattr(base_model, "get_params", lambda: {})()
        for p_name, p_val in params.items():
            if isinstance(p_val, (str, int, float, bool)) or p_val is None:
                mlflow.log_param(p_name, p_val)

        pipe.fit(X_train, y_train, clf__sample_weight=w_train)

        clf = pipe.named_steps["clf"]
        if hasattr(clf, "predict_proba"):
            val_proba = pipe.predict_proba(X_val)[:, 1]
        else:
            val_proba = pipe.predict(X_val)

        val_roc_auc = float(roc_auc_score(y_val, val_proba))
        val_pr_auc = float(average_precision_score(y_val, val_proba))
        val_prec_5 = precision_at_k(y_val, val_proba, 0.05)
        val_pred = (val_proba >= 0.5).astype(int)

        val_accuracy = float(accuracy_score(y_val, val_pred))
        val_precision = float(precision_score(y_val, val_pred, zero_division=0))
        val_recall = float(recall_score(y_val, val_pred, zero_division=0))
        val_f1 = float(f1_score(y_val, val_pred, zero_division=0))

        mlflow.log_metric("val_roc_auc", val_roc_auc)
        mlflow.log_metric("val_pr_auc", val_pr_auc)
        mlflow.log_metric("val_precision_at_5pct", val_prec_5)
        mlflow.log_metric("val_accuracy", val_accuracy)
        mlflow.log_metric("val_precision", val_precision)
        mlflow.log_metric("val_recall", val_recall)
        mlflow.log_metric("val_f1", val_f1)

        run_ts = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        all_runs.append({
            "run_timestamp": run_ts,
            "name": name,
            "algo": algo,
            "params": params,
            "val_roc_auc": val_roc_auc,
            "val_pr_auc": val_pr_auc,
            "val_precision_at_5pct": val_prec_5,
            "val_accuracy": val_accuracy,
            "val_precision": val_precision,
            "val_recall": val_recall,
            "val_f1": val_f1,
        })

        print(
            f"[INFO] {name} - "
            f"val ROC-AUC={val_roc_auc:.3f}, "
            f"val PR-AUC={val_pr_auc:.3f}, "
            f"val precision@5%={val_prec_5:.3f}, "
            f"val acc={val_accuracy:.3f}, "
            f"val prec={val_precision:.3f}, "
            f"val rec={val_recall:.3f}, "
            f"val f1={val_f1:.3f}"
        )

        if val_pr_auc > best_val_pr_auc:
            best_val_pr_auc = val_pr_auc
            best_name = name
            best_algo = algo
            best_pipe = pipe
            best_val_metrics = {
                "val_roc_auc": val_roc_auc,
                "val_pr_auc": val_pr_auc,
                "val_precision_at_5pct": val_prec_5,
                "val_accuracy": val_accuracy,
                "val_precision": val_precision,
                "val_recall": val_recall,
                "val_f1": val_f1,
            }

if best_pipe is None:
    raise RuntimeError("No candidate model could be trained.")

print(
    f"[INFO] Best model on validation: {best_name} ({best_algo}) "
    f"with val PR-AUC={best_val_pr_auc:.3f}"
)

# Retrain best model on Train+Val, evaluate on Test
print("[INFO] Retraining best model on full train+val data with neighborhood re-weighting...")
best_pipe.fit(X_train_full, y_train_full, clf__sample_weight=w_train_full)

clf_best = best_pipe.named_steps["clf"]
if hasattr(clf_best, "predict_proba"):
    test_proba = best_pipe.predict_proba(X_test)[:, 1]
    test_pred = (test_proba >= 0.5).astype(int)
    test_accuracy = float(accuracy_score(y_test, test_pred))
    test_precision = float(precision_score(y_test, test_pred, zero_division=0))
    test_recall = float(recall_score(y_test, test_pred, zero_division=0))
    test_f1 = float(f1_score(y_test, test_pred, zero_division=0))
    test_cm = confusion_matrix(y_test, test_pred).tolist()
else:
    test_proba = best_pipe.predict(X_test)
    # For safety, still compute confusion matrix & derived metrics using hard preds
    test_pred = (test_proba >= 0.5).astype(int)
    test_accuracy = float(accuracy_score(y_test, test_pred))
    test_precision = float(precision_score(y_test, test_pred, zero_division=0))
    test_recall = float(recall_score(y_test, test_pred, zero_division=0))
    test_f1 = float(f1_score(y_test, test_pred, zero_division=0))
    test_cm = confusion_matrix(y_test, test_pred).tolist()

test_roc_auc = float(roc_auc_score(y_test, test_proba))
test_pr_auc = float(average_precision_score(y_test, test_proba))

test_precision_1 = precision_at_k(y_test, test_proba, 0.01)
test_precision_5 = precision_at_k(y_test, test_proba, 0.05)
test_precision_10 = precision_at_k(y_test, test_proba, 0.10)

critical_threshold = float(
    np.sort(test_proba)[::-1][max(1, int(0.05 * len(test_proba)) - 1)]
)

# Confusion matrix plot
fig, ax = plt.subplots()
im = ax.imshow(test_cm, interpolation="nearest")
ax.figure.colorbar(im, ax=ax)
ax.set(
    xticks=[0, 1],
    yticks=[0, 1],
    xticklabels=["Pred 0", "Pred 1"],
    yticklabels=["True 0", "True 1"],
    xlabel="Predicted label",
    ylabel="True label",
    title="Confusion Matrix (Test Set)",
)
for i in range(2):
    for j in range(2):
        ax.text(j, i, test_cm[i][j], ha="center", va="center", color="w")

cm_path = MODEL_DIR / "confusion_matrix_test.png"
plt.tight_layout()
plt.savefig(cm_path)
plt.close(fig)

label_pos_rate = float(y.mean())

# Metrics & experiment log
metrics = {
    "best_model_name": best_name,
    "algo": best_algo,
    # validation metrics (for model selection)
    "val_roc_auc": best_val_metrics["val_roc_auc"],
    "val_pr_auc": best_val_metrics["val_pr_auc"],
    "val_precision_at_5pct": best_val_metrics["val_precision_at_5pct"],
    "val_accuracy": best_val_metrics["val_accuracy"],
    "val_precision": best_val_metrics["val_precision"],
    "val_recall": best_val_metrics["val_recall"],
    "val_f1": best_val_metrics["val_f1"],
    # test metrics (hold-out)
    "test_roc_auc": test_roc_auc,
    "test_pr_auc": test_pr_auc,
    "test_accuracy": test_accuracy,
    "test_precision": test_precision,
    "test_recall": test_recall,
    "test_f1": test_f1,
    "test_confusion_matrix": test_cm,
    # ranking-style metrics
    "precision_at_1pct": test_precision_1,
    "precision_at_5pct": test_precision_5,
    "precision_at_10pct": test_precision_10,
    # threshold + label stats
    "critical_threshold_at_5pct": critical_threshold,
    "label_pos_rate": label_pos_rate,
}

metrics["bias_mitigation"] = {
    "enabled": True,
    "dimensions": ["neighborhood", "department", "reason"],
    "technique": "inverse_frequency_reweighting",
    "per_dimension_max_weight": MAX_DIM_WEIGHT,
    "global_max_weight": MAX_GLOBAL_WEIGHT,
    "notes": (
        "Training rows from under-represented neighborhoods, departments, "
        "and reasons receive higher sample_weight during model fit. "
        "Weights are inverse-frequency-based, capped per dimension and "
        "globally normalised to keep training stable."
    ),
}

# JSON experiment log (custom tracking)
experiments_path = MODEL_DIR / "priority_xgb_experiments.json"
with open(experiments_path, "w") as f:
    json.dump(all_runs, f, indent=2)

# This is the report object we will use for rollback + CI checks
report = metrics.copy()
print(json.dumps(report, indent=2))

# Rollback gate: compare against existing model_report.json

baseline_report_path = MODEL_DIR / "model_report.json"  # current prod report (if any)
baseline_metric_name = "test_pr_auc"

new_metric = report.get(baseline_metric_name)
baseline_metric = None

if baseline_report_path.exists():
    try:
        with open(baseline_report_path, "r") as f:
            baseline_report = json.load(f)
        baseline_metric = baseline_report.get(baseline_metric_name)
        if baseline_metric is not None:
            print(
                f"[INFO] Baseline {baseline_metric_name} from existing prod model: "
                f"{baseline_metric:.6f}"
            )
        else:
            print(
                f"[WARN] Baseline report missing {baseline_metric_name}; "
                "skipping rollback comparison."
            )
    except Exception as e:
        print(
            f"[WARN] Could not load baseline model_report.json for rollback: {e}"
        )

# Only gate if we have both metrics
if baseline_metric is not None and new_metric is not None:
    diff = new_metric - baseline_metric
    max_allowed_drop = 0.005  # allow tiny noise

    if diff < -max_allowed_drop:
        print(
            "[WARN] New model underperforms baseline "
            f"({baseline_metric_name}: new={new_metric:.6f}, "
            f"baseline={baseline_metric:.6f}, delta={diff:.6f})."
        )
        print(
            "[WARN] Rolling back: will NOT overwrite priority_model.pkl / "
            "model_report.json and will NOT push a new model artifact."
        )

        candidate_report_path = MODEL_DIR / "last_candidate_report.json"
        with open(candidate_report_path, "w") as f:
            json.dump(report, f, indent=2)
        print(
            f"[INFO] Wrote candidate report to {candidate_report_path} "
            "for offline analysis."
        )

        # Exit cleanly before saving/pushing model
        raise SystemExit(0)

print(
    f"[INFO] New model passes rollback gate on {baseline_metric_name} "
    "(no significant regression detected). Promoting to prod..."
)

# Save artifacts & push to registry
joblib.dump(best_pipe, MODEL_DIR / "priority_model.pkl")

with open(MODEL_DIR / "model_report.json", "w") as f:
    json.dump(metrics, f, indent=2)

with open(MODEL_DIR / "model_report_bias_mitigated.json", "w") as f:
    json.dump(metrics, f, indent=2)

with open(MODEL_DIR / "feature_columns.json", "w") as f:
    json.dump({"cat": cat, "num": num}, f, indent=2)

print(f"[OK] Saved best model + report in {MODEL_DIR.resolve()}")

try:
    push_model_to_registry(MODEL_DIR, metrics)
except Exception as e:
    print(f"[WARN] Failed to push model to registry: {e}")
