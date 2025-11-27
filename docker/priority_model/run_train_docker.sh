#!/usr/bin/env bash
set -e

# Resolve repo root (two levels up from this script: docker/priority_model -> docker -> root)
ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"

# Build the image using the Dockerfile in this folder, with repo root as context
docker build -t boston311-priority-train -f "$ROOT_DIR/docker/priority_model/Dockerfile" "$ROOT_DIR"

# Run training
docker run --rm \
  -v "$ROOT_DIR/secrets/bq-dashboard-ro.json:/app/secrets/bq-dashboard-ro.json:ro" \
  -v "$ROOT_DIR/ml_prioritization_dashboard/models:/app/ml_prioritization_dashboard/models" \
  -e GCP_PROJECT=boston311-mlops \
  -e BQ_LOCATION=US \
  -e MODEL_REGISTRY_BUCKET=boston311-ml-model-registry \
  -e TRAIN_FEATURE_TABLE=tbl_train_features \
  boston311-priority-train
