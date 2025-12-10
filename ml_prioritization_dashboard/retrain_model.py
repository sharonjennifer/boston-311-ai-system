"""
retrain_model.py

Retraining pipeline for the Boston 311 Priority Model.

This script is intended to be called when monitor_model.py detects
model performance decay (e.g., via Airflow / Cloud Scheduler).

What it does:
1. Calls the existing training script: train_priority_xgb.py
   - This script reads training data from BigQuery and writes
     a new models/priority_model.pkl file.
2. Optionally uploads the retrained model to GCS (if B311_MODEL_BUCKET is set).
3. (Optional placeholder) Triggers CI/CD to rebuild & redeploy the dashboard.

Exit codes:
- 0 → retraining pipeline completed successfully
- 1 → an error occurred
"""

import os
import sys
import logging
import subprocess
from pathlib import Path

# Make GCS upload optional
try:
    from google.cloud import storage
except ImportError:
    storage = None  # We handle this gracefully below

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

PROJECT_ID = os.getenv("B311_PROJECT_ID", "boston311-mlops")

# Directory containing this script (ml_prioritization_dashboard/)
# NOTE: must be __file__, not _file_
BASE_DIR = Path(__file__).resolve().parent

# Where the training script writes the model (relative to this folder)
LOCAL_MODEL_PATH = BASE_DIR / "models" / "priority_model.pkl"

# Optional: GCS bucket/object for latest model.
# Example:
#   export B311_MODEL_BUCKET="b311-models"
#   export B311_MODEL_BLOB="priority_model_latest.pkl"
GCS_MODEL_BUCKET = os.getenv("B311_MODEL_BUCKET", "")
GCS_MODEL_BLOB = os.getenv("B311_MODEL_BLOB", "priority_model_latest.pkl")


def run_training_script() -> None:
    """
    Call the existing training pipeline script.

    Adjust the command if train_priority_xgb.py expects CLI parameters.
    """
    training_script = BASE_DIR / "train_priority_xgb.py"
    if not training_script.exists():
        raise FileNotFoundError(
            f"Training script not found at {training_script}. "
            "Make sure you run this from the ml_prioritization_dashboard folder "
            "and that train_priority_xgb.py exists."
        )

    logging.info("Starting model retraining via train_priority_xgb.py ...")
    logging.info("Working directory for training: %s", BASE_DIR)

    # Change to the dashboard folder so relative paths in training script work
    os.chdir(BASE_DIR)

    cmd = [
        sys.executable,
        "train_priority_xgb.py",
        # If your training script supports it, you can wire the output path:
        # "--output-model-path", str(LOCAL_MODEL_PATH),
    ]

    logging.info("Running command: %s", " ".join(cmd))
    subprocess.run(cmd, check=True)
    logging.info("Training script finished successfully.")


def upload_model_to_gcs() -> None:
    """
    Upload the retrained model to GCS, if a bucket is configured.
    """
    if not GCS_MODEL_BUCKET:
        logging.info("No B311_MODEL_BUCKET set. Skipping GCS upload.")
        return

    if storage is None:
        logging.warning(
            "google-cloud-storage is not installed, cannot upload model to GCS. "
            "Install it with `pip install google-cloud-storage` if needed."
        )
        return

    if not LOCAL_MODEL_PATH.exists():
        logging.error("Local model file does not exist: %s", LOCAL_MODEL_PATH)
        raise FileNotFoundError(f"Model not found at {LOCAL_MODEL_PATH}")

    logging.info(
        "Uploading model to gs://%s/%s",
        GCS_MODEL_BUCKET,
        GCS_MODEL_BLOB,
    )

    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(GCS_MODEL_BUCKET)
    blob = bucket.blob(GCS_MODEL_BLOB)
    blob.upload_from_filename(str(LOCAL_MODEL_PATH))

    logging.info("Model uploaded to GCS successfully.")


def trigger_ci_cd() -> None:
    """
    Optional: Trigger Cloud Build / CI/CD after retraining.

    For this course project, we just log that this is where we'd call
    Cloud Build or rely on GitHub Actions to pick up the new model.

    Example ideas (not executed here):
      - Push a Git tag so a Cloud Build trigger runs
      - Or call gcloud builds submit with a Cloud Build config
    """
    # Example skeleton (COMMENTED OUT ON PURPOSE):
    #
    # cmd = [
    #     "gcloud", "builds", "submit",
    #     "--config=ml_prioritization_dashboard/cloudbuild.yaml",
    #     "--substitutions", f"_IMAGE_TAG=vretrain-$(date +%Y%m%d%H%M%S)"
    # ]
    # logging.info("Triggering Cloud Build: %s", " ".join(cmd))
    # subprocess.run(cmd, check=True)
    #
    logging.info("CI/CD trigger step is currently a no-op (documented in DEPLOYMENT.md).")


def main() -> int:
    try:
        run_training_script()
        upload_model_to_gcs()
        trigger_ci_cd()
    except Exception as e:
        logging.exception("Retraining failed: %s", e)
        return 1

    logging.info("Retraining pipeline completed successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
