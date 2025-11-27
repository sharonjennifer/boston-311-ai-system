#!/usr/bin/env bash
set -e

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"

# Optional: build to ensure latest image
docker build -t boston311-priority-train -f "$ROOT_DIR/docker/priority_model/Dockerfile" "$ROOT_DIR"

# Run bias analysis
docker run --rm \
  -v "$ROOT_DIR/secrets/bq-dashboard-ro.json:/app/secrets/bq-dashboard-ro.json:ro" \
  -v "$ROOT_DIR/ml_prioritization_dashboard/models:/app/ml_prioritization_dashboard/models" \
  -e GCP_PROJECT=boston311-mlops \
  -e BQ_LOCATION=US \
  -e TRAIN_FEATURE_TABLE=tbl_train_features \
  boston311-priority-train \
  python ml_prioritization_dashboard/bias_check.py
