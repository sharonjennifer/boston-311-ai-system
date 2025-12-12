#!/usr/bin/env bash
# Deploy the Boston 311 Priority Dashboard to Cloud Run.
# 1) Build & push Docker image to Artifact Registry
# 2) Deploy to Cloud Run
# 3) Print service URL
# 4) Warm up tabs + /api/ping + /api/debug 

set -euo pipefail

# Config
PROJECT_ID="boston311-mlops"
REGION="us-central1"
REPO="b311-dashboard-repo"
IMAGE_NAME="priority-dashboard"
SERVICE="b311-priority-dashboard"

IMAGE_TAG="${1:-v$(date +%Y%m%d%H%M%S)}"
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

# 1) Build & push Docker image
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

# 2) Deploy to Cloud Run
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
B311_SLA_DATASET=boston311_service,\
B311_SLA_TABLE=dashboard_daily_metrics,\
B311_SLA_RAW_DATASET=boston311,\
B311_SLA_RAW_TABLE=service_requests_2025,\
BQ_LOCATION=US,\
B311_USE_LOCAL_SA=false,\
B311_DEPLOY_TAG=${IMAGE_TAG} \
  --memory=1Gi \
  --cpu=1 \
  --timeout=180

echo "[2/4] Cloud Run deploy command finished."
echo

# 3) Fetch & print service URL
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

# 4) Warm up routes
echo "[4/4] Warming up dashboard routes..."
echo

warm() {
  local path="$1"
  echo "Warm-up: ${path}"
  time curl -s -o /dev/null "${SERVICE_URL}${path}" || true
  echo
}

warm "/"
warm "/work-queues"
warm "/neighborhoods"
warm "/sla-performance"
warm "/demand-trends"
warm "/analytics"
warm "/api/ping"
warm "/api/debug"

echo "---- /api/debug (sanity check) ----"
curl -s "${SERVICE_URL}/api/debug" | python3 -m json.tool || true
echo "-----------------------------------"
echo

echo "Warm-up complete. Service is live at:"
echo "  ${SERVICE_URL}"
echo
echo "Quick checks:"
echo "  curl \"${SERVICE_URL}/api/ping\""
echo "  curl \"${SERVICE_URL}/api/debug\" | python3 -m json.tool"
