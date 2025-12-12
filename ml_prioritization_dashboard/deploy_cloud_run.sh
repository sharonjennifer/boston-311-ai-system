#!/usr/bin/env bash
# Deploy the Boston 311 Priority Dashboard to Cloud Run.
# This script:
#   1. Builds and pushes a Docker image to Artifact Registry
#   2. Deploys that image to a Cloud Run service
#   3. Fetches and prints the service URL
#   4. Warms up all dashboard tabs with curl
#
# Usage:
#   ./deploy_dashboard.sh              
#   ./deploy_dashboard.sh v202512101200 

set -euo pipefail

# Config 

# GCP project that owns the BigQuery datasets and Cloud Run service
PROJECT_ID="boston311-mlops"

# Region for Artifact Registry + Cloud Run
REGION="us-central1"

# Artifact Registry repo that stores dashboard images
REPO="b311-dashboard-repo"

# Base name for the dashboard image
IMAGE_NAME="priority-dashboard"

# Cloud Run service name
SERVICE="b311-priority-dashboard"

# Optional: tag passed as first argument; otherwise use timestamp (vYYYYMMDDHHMMSS)
IMAGE_TAG="${1:-v$(date +%Y%m%d%H%M%S)}"

# Fully-qualified Artifact Registry image path
IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/${IMAGE_NAME}:${IMAGE_TAG}"

echo "========================================"
echo " Deploying Boston 311 Priority Dashboard"
echo " Project : ${PROJECT_ID}"
echo " Region  : ${REGION}"
echo " Repo    : ${REPO}"
echo " Service : ${SERVICE}"
echo " Image   : ${IMAGE}"
echo " Tag     : ${IMAGE_TAG}"
echo "========================================"
echo

# 1. Build & push Docker image

echo "[1/4] Building and pushing Docker image to Artifact Registry..."

# SCRIPT_DIR = folder where this script lives (so we can run from anywhere)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Build a linux/amd64 image (Cloud Run) and push straight to Artifact Registry
docker buildx build \
  --platform=linux/amd64 \
  -t "${IMAGE}" \
  --push \
  .

echo "[1/4] Image build & push complete."
echo

# 2. Deploy to Cloud Run

echo "[2/4] Deploying to Cloud Run service: ${SERVICE} ..."

gcloud run deploy "${SERVICE}" \
  --image="${IMAGE}" \
  --region="${REGION}" \
  --platform=managed \
  --allow-unauthenticated \
  --service-account="b311-dashboard-sa@${PROJECT_ID}.iam.gserviceaccount.com" \
  --set-env-vars=\
B311_PROJECT_ID=${PROJECT_ID},\
B311_DATASET=boston311_service,\
B311_TABLE=cases_ranking_ml,\
B311_SLA_PROJECT=${PROJECT_ID},\
B311_SLA_DATASET=boston311,\
B311_SLA_TABLE=dashboard_daily_metrics,\
BQ_LOCATION=US,\
B311_USE_LOCAL_SA=false \
  --memory=1Gi \
  --cpu=1 \
  --timeout=120

echo "[2/4] Cloud Run deploy command finished."
echo

# 3. Fetch & print service URL

echo "[3/4] Fetching Cloud Run service URL..."

SERVICE_URL="$(gcloud run services describe "${SERVICE}" \
  --region "${REGION}" \
  --format 'value(status.url)')"

if [[ -z "${SERVICE_URL}" ]]; then
  echo "ERROR: Could not retrieve service URL. Check Cloud Run service status."
  exit 1
fi

echo "========================================"
echo " Deployment complete!"
echo " Cloud Run URL:"
echo "   ${SERVICE_URL}"
echo "========================================"
echo

# 4. Warm up all dashboard tabs

echo "[4/4] Warming up all dashboard tabs (this may take a few seconds each)..."
echo

# Command Center (home)
echo "Warm-up: / (Command Center)"
time curl -s -o /dev/null "${SERVICE_URL}/" || true
echo

# Work Queues
echo "Warm-up: /work-queues"
time curl -s -o /dev/null "${SERVICE_URL}/work-queues" || true
echo

# Neighborhood performance
echo "Warm-up: /neighborhoods"
time curl -s -o /dev/null "${SERVICE_URL}/neighborhoods" || true
echo

# SLA performance
echo "Warm-up: /sla-performance"
time curl -s -o /dev/null "${SERVICE_URL}/sla-performance" || true
echo

# Demand trends
echo "Warm-up: /demand-trends"
time curl -s -o /dev/null "${SERVICE_URL}/demand-trends" || true
echo

# Analytics
echo "Warm-up: /analytics"
time curl -s -o /dev/null "${SERVICE_URL}/analytics" || true
echo

echo "Warm-up complete. Service is live at:"
echo "  ${SERVICE_URL}"
echo
echo "You can manually check, for example:"
echo "  open \"${SERVICE_URL}/\"   # on macOS"
echo "or:"
echo "  curl \"${SERVICE_URL}/api/ping\""
