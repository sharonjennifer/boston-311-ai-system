"""
retrain_model.py

Retraining pipeline for the Boston 311 Priority Model.

This script is meant to run AFTER monitor_model.py detects that the model
is decaying or drifting. In a real system, something like Airflow or
Cloud Scheduler would call this script.

1. Run the existing training script: train_priority_xgb.py
   - That script reads training data from BigQuery and produces a fresh
     models/priority_model.pkl file.
2. Optionally upload the new model to GCS if B311_MODEL_BUCKET is set.
3. Trigger CI/CD or Cloud Build once retraining is done.

Exit codes:
- 0 → retraining completed successfully
- 1 → some error happened during retraining
"""

import os
import sys
import logging
import subprocess
import argparse
from pathlib import Path

try:
    from google.cloud import storage
except ImportError:
    storage = None

# Simple logging so we can see how far the script gets and where it fails
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

# We keep the project id in an env var so it's easy to reuse this script
PROJECT_ID = os.getenv("B311_PROJECT_ID", "boston311-mlops")
BASE_DIR = Path(__file__).resolve().parent

# The training script will write the model here
LOCAL_MODEL_PATH = BASE_DIR / "models" / "priority_model.pkl"

# Optional: GCS location for copying the latest model
# Example:
#   export B311_MODEL_BUCKET="b311-models"
#   export B311_MODEL_BLOB="priority_model_latest.pkl"
GCS_MODEL_BUCKET = os.getenv("B311_MODEL_BUCKET", "")
GCS_MODEL_BLOB = os.getenv("B311_MODEL_BLOB", "priority_model_latest.pkl")


def run_training_script() -> None:
    """
    Run the existing training pipeline script using subprocess.

    This assumes train_priority_xgb.py lives in the same folder as this file
    and that it knows how to load data from BigQuery and save a model.
    """
    training_script = BASE_DIR / "train_priority_xgb.py"
    if not training_script.exists():
        raise FileNotFoundError(
            f"Training script not found at {training_script}. "
            "Make sure retrain_model.py and train_priority_xgb.py are in the same folder."
        )

    logging.info("Starting model retraining via train_priority_xgb.py ...")
    logging.info("Working directory for training: %s", BASE_DIR)

    # Switch into the dashboard folder so any relative paths in the training script work
    os.chdir(BASE_DIR)

    cmd = [
        sys.executable,
        "train_priority_xgb.py",
        # If the training script accepts an output path, we could pass it here, e.g.:
        # "--output-model-path", str(LOCAL_MODEL_PATH),
    ]

    logging.info("Running command: %s", " ".join(cmd))
    # check=True will raise CalledProcessError if training returns a non-zero exit code
    subprocess.run(cmd, check=True)
    logging.info("Training script finished successfully.")


def upload_model_to_gcs() -> None:
    """
    Upload the retrained model to a GCS bucket, if the bucket name is configured.

    This step is optional. If B311_MODEL_BUCKET is not set, we just log and skip.
    """
    # If no bucket is configured, we don't do anything for this step
    if not GCS_MODEL_BUCKET:
        logging.info("No B311_MODEL_BUCKET set. Skipping GCS upload.")
        return

    # If google-cloud-storage isn't installed, we can't upload the model
    if storage is None:
        logging.warning(
            "google-cloud-storage is not installed, cannot upload model to GCS. "
            "Install it with `pip install google-cloud-storage` if you want this step."
        )
        return

    # Make sure the model file actually exists before we try to upload it
    if not LOCAL_MODEL_PATH.exists():
        logging.error("Local model file does not exist: %s", LOCAL_MODEL_PATH)
        raise FileNotFoundError(f"Model not found at {LOCAL_MODEL_PATH}")

    logging.info(
        "Uploading model to gs://%s/%s",
        GCS_MODEL_BUCKET,
        GCS_MODEL_BLOB,
    )

    # Create a storage client and upload the file
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(GCS_MODEL_BUCKET)
    blob = bucket.blob(GCS_MODEL_BLOB)
    blob.upload_from_filename(str(LOCAL_MODEL_PATH))

    logging.info("Model uploaded to GCS successfully.")


def trigger_ci_cd() -> None:
    """
    Placeholder function where we would trigger CI/CD or Cloud Build.

    For the class project, we only log a message here and explain in the
    report that this is where automatic redeployment would usually happen.

    Example ideas (not implemented here):
      - Call `gcloud builds submit` with a Cloud Build config
      - Push a Git tag so a GitHub Actions / Cloud Build trigger fires
    """
    # Example skeleton (kept as comment for documentation):
    #
    # cmd = [
    #   "gcloud", "builds", "submit",
    #   "--config=ml_prioritization_dashboard/cloudbuild.yaml",
    #   "--substitutions", f"_IMAGE_TAG=vretrain-$(date +%Y%m%d%H%M%S)"
    # ]
    # logging.info("Triggering Cloud Build: %s", " ".join(cmd))
    # subprocess.run(cmd, check=True)
    #
    logging.info("CI/CD trigger step is currently a no-op (described in DEPLOYMENT.md).")


def main(argv=None) -> int:
    """
    Glue function that runs:
    - training
    - optional GCS upload
    - optional CI/CD trigger

    Use --dry-run to only log the steps without actually executing them.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what the pipeline would do, but do not retrain or upload.",
    )
    args = parser.parse_args(argv)

    if args.dry_run:
        logging.info("[DRY RUN] Would run train_priority_xgb.py")
        if GCS_MODEL_BUCKET:
            logging.info(
                "[DRY RUN] Would upload model to GCS bucket '%s' as '%s'",
                GCS_MODEL_BUCKET,
                GCS_MODEL_BLOB,
            )
        else:
            logging.info("[DRY RUN] No B311_MODEL_BUCKET set, would skip GCS upload.")
        logging.info("[DRY RUN] Would trigger CI/CD or Cloud Build step.")
        logging.info("[DRY RUN] Exiting without making any changes.")
        return 0

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
    # Return 0 for success, 1 for failure so other tools can react
    sys.exit(main())
