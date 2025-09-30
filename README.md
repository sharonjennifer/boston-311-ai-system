Boston 311 AI System
AI-powered complaint management system for Boston with chatbots, analytics dashboards, and ML-driven insights.
Project Information
The Boston 311 system provides residents with a platform to report non-emergency issues such as potholes, graffiti, broken streetlights, trash pickup, and sidewalk repairs. While the system generates valuable data, both residents and city staff need more effective ways to interact with this information.
This project builds a solution that makes Boston 311 data more accessible, transparent, and actionable through:
Citizen-Facing Chatbot: Allows residents to ask questions in natural language about service request status, complaint patterns in their neighborhood, and expected resolution timelines.
City Operations Dashboard: Provides staff with real-time metrics, complaint clustering, prioritization models, and geospatial insights for faster, data-driven decision-making.
By combining structured request data with free-text descriptions, the project creates a modern platform that enhances transparency, reduces manual workloads, and strengthens trust between residents and city services.
Tech Stack: Boston 311 API → Airflow → BigQuery | Llama-3.1-8B/Gemma-2 + PyTorch | FastAPI on Cloud Run | Next.js + Mapbox | GCP + Terraform
Repository Structure:
boston-311-ai-system/
├── data_pipelines/  # Airflow DAGs, SQL transformations
├── services/        # FastAPI (chat, prioritization, clustering)
├── models/          # PyTorch training scripts
├── webapp/          # Next.js frontend
├── infra/           # Terraform infrastructure
├── docs/            # Documentation
└── tests/           # Unit & integration tests
Installation
Prerequisites: Python 3.9+, Node.js 18+, Docker, Terraform 1.5+, GCP account with billing
bash# Clone and setup
git clone https://github.com/sharonjennifer/boston-311-ai-system.git
cd boston-311-ai-system
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt

# Configure environment
cp .env.example .env  # Edit with your GCP credentials

# GCP authentication
gcloud auth application-default login
gcloud config set project YOUR_PROJECT_ID

# Deploy infrastructure
cd infra/terraform
terraform init
terraform apply -var-file=environments/dev.tfvars

# Verify installation
pytest tests/
Usage
Deploy Services:
bashgcloud builds submit --tag gcr.io/PROJECT/SERVICE_NAME
gcloud run deploy SERVICE_NAME --image gcr.io/PROJECT/SERVICE_NAME
Run Frontend Locally:
bashcd webapp/frontend
npm install
npm run dev
# Visit http://localhost:3000
Train ML Models:
bashpython models/training/train_priority.py
python models/training/train_clustering.py
Query Chatbot API:
bashcurl -X POST https://YOUR_API_URL/chat/query \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -d '{"query": "What is the status of case 101004123?"}'
Development Workflow:

Branches: main (production), feature/* (new features), bugfix/* (fixes)
Commits: Use conventional commits (feat: add feature, fix: resolve bug)
PRs: Require 1 review and passing CI tests

Documentation

Architecture Overview
Setup Guide
API Reference
Monitoring Plan

License
MIT License - see LICENSE
Contact
Repository: https://github.com/sharonjennifer/boston-311-ai-system
Issues: https://github.com/sharonjennifer/boston-311-ai-system/issues
