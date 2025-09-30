# Boston 311 AI System

## Project Information

The Boston 311 system provides residents with a platform to report non-emergency issues such as potholes, graffiti, broken streetlights, trash pickup, and sidewalk repairs. While the system generates valuable data, both residents and city staff need more effective ways to interact with this information.

This project builds a solution that makes Boston 311 data more accessible, transparent, and actionable through:

**Citizen-Facing Chatbot:** Allows residents to ask questions in natural language about service request status, complaint patterns in their neighborhood, and expected resolution timelines.

**City Operations Dashboard:** Provides staff with real-time metrics, complaint clustering, prioritization models, and geospatial insights for faster, data-driven decision-making.

By combining structured request data with free-text descriptions, the project creates a modern platform that enhances transparency, reduces manual workloads, and strengthens trust between residents and city services.

**Tech Stack:** Boston 311 API → Airflow → BigQuery | Llama-3.1-8B/Gemma-2 + PyTorch | FastAPI on Cloud Run | Next.js + Mapbox | GCP + Terraform
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

**Prerequisites:** Python 3.9+, Node.js 18+, Docker, Terraform 1.5+
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

