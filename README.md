# Boston 311 AI System

AI-powered complaint management system for Boston with chatbots, analytics dashboards, and ML-driven insights.

## Project Information

The Boston 311 AI System provides intelligent analysis of municipal service requests through automated chatbots, ML-powered prioritization, and real-time analytics dashboards for both citizens and city staff.

**Key Features:**
- Case status lookup and community insights chatbot
- ML-based priority scoring and duplicate detection
- Operations dashboards with KPIs and heat maps
- Automated city health reports

**Architecture:**  
Data: Boston 311 API → Airflow → BigQuery (Silver/Gold)  
ML: Llama-3.1-8B/Gemma-2 + PyTorch + Vertex AI Vector Search  
Backend: FastAPI on Cloud Run  
Frontend: Next.js React with Mapbox  
Infrastructure: GCP, Terraform

**Repository Structure:**
boston-311-ai-system/
├── data_pipelines/    # Airflow DAGs, SQL transformations
├── services/          # FastAPI APIs (chat, prioritization, clustering)
├── models/            # PyTorch training scripts
├── webapp/            # Next.js frontend
├── infra/             # Terraform IaC
├── docs/              # Documentation
└── tests/             # Unit & integration tests

## Installation Instructions

**Prerequisites:** Python 3.9+, Node.js 18+, Docker, Terraform 1.5+, GCP account with billing
```bash
# Clone repository
git clone https://github.com/sharonjennifer/boston-311-ai-system.git
cd boston-311-ai-system

# Python setup
python -m venv venv
source venv/bin/activate  # or venv\Scripts\activate on Windows
pip install -r requirements.txt

# Configure environment
cp .env.example .env  # Edit with your credentials

# GCP authentication
gcloud auth application-default login
gcloud config set project YOUR_PROJECT_ID

# Deploy infrastructure
cd infra/terraform
terraform init
terraform apply -var-file=environments/dev.tfvars

# Run tests
pytest tests/
Usage Guidelines
Deploy Services:
bashgcloud builds submit --tag gcr.io/PROJECT/SERVICE_NAME
gcloud run deploy SERVICE_NAME --image gcr.io/PROJECT/SERVICE_NAME
Run Frontend Locally:
bashcd webapp/frontend
npm install
npm run dev
Train ML Models:
bashpython models/training/train_priority.py
python models/training/train_clustering.py
Development Workflow:

Branches: main (production), feature/* (new features), bugfix/* (fixes)
Commits: Use conventional commits (e.g., feat: add feature, fix: resolve bug)
PRs: Require 1 review and passing CI tests

Documentation

Architecture
Setup Guide
API Reference
Monitoring

License
MIT License - see LICENSE
Contact
Repository: https://github.com/sharonjennifer/boston-311-ai-system
Issues: https://github.com/sharonjennifer/boston-311-ai-system/issues
