# Boston 311 AI System

AI-powered complaint management system for Boston with chatbots, analytics dashboards, and ML-driven insights.

---

## Project Overview

The **Boston 311 AI System** transforms how citizens and city staff interact with municipal service requests. It combines modern data engineering, machine learning, and natural language processing to deliver real-time insights, automated prioritization, and intelligent complaint management.

**Key Capabilities:**
- Public chatbot for case status and community insights
- Internal dashboards with KPIs, heat maps, and trend analysis
- ML-powered priority scoring and duplicate detection
- Automated city health reports for decision-makers

---

## Architecture

**Data Pipeline:**  
Boston 311 API → Cloud Composer (Airflow) → BigQuery (Silver/Gold layers)

**AI/ML:**  
Llama-3.1-8B / Gemma-2 LLM + PyTorch models + Vertex AI Vector Search

**Services:**  
FastAPI on Cloud Run

**Frontend:**  
Next.js React with Mapbox

**Infrastructure:**  
GCP (BigQuery, Cloud Run, Composer), Terraform IaC

---

## Repository Structure

boston-311-ai-system/
├── data_pipelines/ # Airflow DAGs, SQL transformations, data quality tests
├── services/ # FastAPI microservices (chat, prioritization, clustering)
├── models/ # PyTorch training scripts, notebooks, evaluation
├── webapp/ # Next.js React frontend with dashboards
├── infra/ # Terraform infrastructure as code
├── docs/ # Architecture, setup, and API documentation
├── tests/ # Unit and integration tests
└── .github/workflows/ # CI/CD automation

yaml
Copy code

---

## Installation

### Prerequisites
- Python 3.9+
- Node.js 18+
- Docker & Docker Compose
- GCP account with billing enabled
- Terraform 1.5+
- Git

---

### Step 1: Clone Repository
```bash
git clone https://github.com/sharonjennifer/boston-311-ai-system.git
cd boston-311-ai-system
Step 2: Python Environment Setup
bash
Copy code
# Create virtual environment
python -m venv venv

# Activate environment
venv\Scripts\activate          # Windows
source venv/bin/activate       # Mac/Linux

# Install dependencies
pip install --upgrade pip
pip install -r requirements.txt
Step 3: Configure Environment Variables
bash
Copy code
# Create environment file
copy .env.example .env         # Windows
cp .env.example .env           # Mac/Linux

# Edit .env with your credentials
# Required: GOOGLE_CLOUD_PROJECT, API keys, JWT secret
Step 4: GCP Authentication
bash
Copy code
# Authenticate
gcloud auth login
gcloud auth application-default login

# Set project
gcloud config set project YOUR_PROJECT_ID

# Enable required APIs
gcloud services enable bigquery.googleapis.com composer.googleapis.com run.googleapis.com aiplatform.googleapis.com
Step 5: Deploy Infrastructure
bash
Copy code
cd infra/terraform
terraform init
terraform plan -var-file=environments/dev.tfvars
terraform apply -var-file=environments/dev.tfvars
Step 6: Verify Installation
bash
Copy code
pytest tests/
flake8 services/ models/ data_pipelines/
black --check .
Usage Guidelines
Running Data Pipelines
bash
Copy code
# Upload Airflow DAGs
DAGS_BUCKET=$(gcloud composer environments describe YOUR_ENV --location us-central1 --format="value(config.dagGcsPrefix)")
gsutil -m rsync -r data_pipelines/dags $DAGS_BUCKET

# Trigger manual run
gcloud composer environments run YOUR_ENV --location us-central1 dags trigger ingest_311_data
Training ML Models
bash
Copy code
source venv/bin/activate
python models/training/train_priority.py
python models/training/train_clustering.py
python models/evaluation/evaluate_models.py
Deploying Services
bash
Copy code
# Deploy chatbot API
cd services/chat_api
gcloud builds submit --tag gcr.io/YOUR_PROJECT/chat-api
gcloud run deploy chat-api --image gcr.io/YOUR_PROJECT/chat-api --region us-central1

# Deploy prioritization service
cd services/prioritization
gcloud builds submit --tag gcr.io/YOUR_PROJECT/priority-service
gcloud run deploy priority-service --image gcr.io/YOUR_PROJECT/priority-service --region us-central1
Running Frontend Locally
bash
Copy code
cd webapp/frontend
npm install
npm run dev
# Visit http://localhost:3000
Using the Chatbot API
bash
Copy code
# Query case status
curl -X POST https://YOUR_API_URL/chat/query \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"query": "What is the status of case 101004123?"}'

# Get community insights
curl -X POST https://YOUR_API_URL/chat/query \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"query": "Top complaints in Dorchester this month?"}'
Development Workflow
Branching Strategy
main → Production-ready code

develop → Integration branch

feature/* → New features

bugfix/* → Bug fixes

Making Changes
bash
Copy code
git checkout -b feature/your-feature-name
pytest tests/
git commit -m "feat(chat): add case status lookup"
git push origin feature/your-feature-name
Running Tests
bash
Copy code
# All tests
pytest tests/

# With coverage
pytest tests/ --cov=services --cov=models --cov-report=html
Common Tasks
Adding New Data Sources

Create DAG in data_pipelines/dags/

Add SQL transformations in data_pipelines/sql/

Add data quality tests in data_pipelines/tests/

Deploy DAG to Cloud Composer

Updating ML Models

Train new model in models/training/

Evaluate performance in models/evaluation/

Update service with new model weights

Deploy updated service to Cloud Run

Monitor drift via Cloud Monitoring

Adding API Endpoints

Define endpoint in services/*/routes.py

Add business logic in separate modules

Write unit tests in tests/unit/

Update API documentation

Deploy service

Troubleshooting
Terraform fails with permission errors
Ensure your GCP user has Owner or Editor role.

Airflow DAGs not appearing
Check DAG syntax with python data_pipelines/dags/YOUR_DAG.py.

Cloud Run deployment fails
Check build logs with gcloud builds log BUILD_ID.

Frontend cannot connect to API
Verify API URL in .env.local and check CORS settings.

Model inference errors
Check model weights are properly loaded and input format matches training data.

Documentation
Architecture Overview

Setup Guide

API Reference

Monitoring Plan

Contributing
Pull requests require:

1 code review approval

Passing CI tests

Updated documentation for new features

Conventional commit messages

License
MIT License - see LICENSE file for details.

