#!/usr/bin/env bash
set -euo pipefail

#############################################
# Config – change only if your project changes
#############################################

PROJECT_ID="boston311-mlops"
REGION="us-central1"
REPO="b311-dashboard-repo"
IMAGE_NAME="priority-dashboard"
SERVICE="b311-priority-dashboard"
IMAGE_TAG="${1:-v$(date +%Y%m%d%H%M%S)}"

# Derived variables

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

# Build & push Docker image

echo "[1/4] Building and pushing Docker image to Artifact Registry..."
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

docker buildx build \
  --platform=linux/amd64 \
  -t "${IMAGE}" \
  --push \
  .

echo "[1/4] Image build & push complete."
echo

# Deploy to Cloud Run

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

# Fetch & print service URL

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

# Warm up all dashboard tabs

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
