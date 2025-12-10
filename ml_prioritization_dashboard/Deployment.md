# Boston 311 Priority Dashboard – Deployment Guide

## 1. Deployment Overview

- **Deployment type:** Cloud deployment  
- **Cloud provider:** Google Cloud Platform (GCP)  
- **Service:** Cloud Run (fully managed, serverless containers)

The Boston 311 Priority Dashboard is a Flask-based web app that:

- Reads scored 311 cases and daily metrics from **BigQuery**
- Uses a local XGBoost model (`priority_model.pkl`) for priority scoring (if needed)
- Serves a multi-tab dashboard on **Cloud Run** via a single HTTP endpoint

High-level architecture:

> BigQuery (boston311-mlops) → Flask app (`priority_dashboard_app.py`) → Container (Docker) → Cloud Run service → Browser / curl

---

## 2. Prerequisites

Before deploying, you need:

1. **GCP project**
   - Project ID: `boston311-mlops`
   - Billing enabled

2. **APIs enabled** (once per project)
   - Cloud Run API  
   - Artifact Registry API  
   - Cloud Build API (optional, if you use it)  
   - BigQuery API  

3. **Artifact Registry repository**
   - Region: `us-central1`  
   - Repository name: `b311-dashboard-repo`  
   - Format: Docker  

   Example (one-time):

   ```bash
   gcloud artifacts repositories create b311-dashboard-repo      --repository-format=docker      --location=us-central1      --description="Boston 311 priority dashboard images"
   ```

4. **Service account for the dashboard**

   Example: `b311-dashboard-sa@boston311-mlops.iam.gserviceaccount.com`

   Recommended roles (or equivalent custom role):

   - `roles/run.admin`
   - `roles/artifactregistry.writer`
   - `roles/bigquery.dataViewer`
   - `roles/logging.logWriter`

5. **Local tools (for manual deploys)**

   On your machine or in Cloud Shell:

   - `gcloud` CLI installed and authenticated  
   - Docker / Docker Buildx  
   - Access to this GitHub repo  

   Configure gcloud:

   ```bash
   gcloud auth login
   gcloud config set project boston311-mlops
   ```

---

## 3. Project Layout (Dashboard Folder)

This guide assumes you are inside:

```bash
cd ml_prioritization_dashboard/
```

Key files:

- `priority_dashboard_app.py` – Flask app (routes + logic)  
- `Dockerfile` – container image definition  
- `requirements.txt` – Python dependencies  
- `deploy_cloud_run.sh` – **one-shot automated build & deploy script**  
- `templates/` – HTML templates for each dashboard tab  
- `static/` – CSS/JS files  

---

## 4. One-Command Deployment (Manual)

From a fresh environment (local machine or Cloud Shell):

```bash
# 1. Clone repo
git clone <YOUR_REPO_URL>
cd boston-311-ai-system/ml_prioritization_dashboard

# 2. Ensure deploy script is executable
chmod +x deploy_cloud_run.sh

# 3. Deploy (auto tag)
./deploy_cloud_run.sh

# or specify a custom image tag
./deploy_cloud_run.sh v1
```

The script will:

1. Set core config variables (project, region, repo, image name, service name).  
2. Build & push your Docker image to Artifact Registry.  
3. Deploy to Cloud Run with all required environment variables.  
4. Warm up all major dashboard routes:
   - `/` (Command Center)  
   - `/work-queues`  
   - `/neighborhoods`  
   - `/sla-performance`  
   - `/demand-trends`  
   - `/analytics`  

---

## 5. Verifying the Deployment

### 5.1 Health check endpoint

```bash
SERVICE_URL="https://b311-priority-dashboard-<HASH>-uc.a.run.app"
curl "${SERVICE_URL}/api/ping"
```

Expected output:

```json
{
  "status": "ok",
  "service": "b311-priority-dashboard",
  "timestamp": "2025-..."
}
```

### 5.2 Dashboard tabs (browser)

Open these in your browser to verify all views render correctly:

- `${SERVICE_URL}/`  
- `${SERVICE_URL}/work-queues`  
- `${SERVICE_URL}/neighborhoods`  
- `${SERVICE_URL}/sla-performance`  
- `${SERVICE_URL}/demand-trends`  
- `${SERVICE_URL}/analytics`  

These should feel responsive because they were pre-warmed during deployment.

---

## 6. CI/CD via GitHub Actions

A GitHub Actions workflow automatically deploys the dashboard when relevant code changes are pushed to `main`.

Workflow file:  
`.github/workflows/deploy-dashboard.yml`

Trigger conditions:

- Push to the `main` branch  
- Changes inside: `ml_prioritization_dashboard/**`

The workflow performs:

1. Checks out the repository  
2. Authenticates to Google Cloud using a GitHub Secret  
3. Configures Docker to authenticate with Artifact Registry  
4. Runs the one-shot deployment script:

```bash
cd ml_prioritization_dashboard
chmod +x deploy_cloud_run.sh
./deploy_cloud_run.sh
```

### Setting up CI/CD

1. Create or reuse a service account with these roles:
   - `roles/run.admin`
   - `roles/artifactregistry.writer`
   - `roles/bigquery.dataViewer`
   - `roles/logging.logWriter`

2. Create a **service account JSON key** for this account.

3. Add the JSON key to **GitHub Secrets** as `B311_DASHBOARD_SA_KEY`.

4. Ensure your GitHub Actions workflow refers to it:

```yaml
with:
  credentials_json: '${{ secrets.B311_DASHBOARD_SA_KEY }}'
```

Now, each push to `main` triggers a full **build + deploy** to Cloud Run.
