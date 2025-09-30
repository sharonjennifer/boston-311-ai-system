Boston 311 AI System
Project Information
The Boston 311 system provides residents with a platform to report non-emergency issues such as potholes, graffiti, broken streetlights, trash pickup, and sidewalk repairs. While the system generates valuable data, both residents and city staff need more effective ways to interact with this information.
This project builds a solution that makes Boston 311 data more accessible, transparent, and actionable through:
Citizen-Facing Chatbot: Allows residents to ask questions in natural language about service request status, complaint patterns in their neighborhood, and expected resolution timelines.
City Operations Dashboard: Provides staff with real-time metrics, complaint clustering, prioritization models, and geospatial insights for faster, data-driven decision-making.
By combining structured request data with free-text descriptions, the project creates a modern platform that enhances transparency, reduces manual workloads, and strengthens trust between residents and city services.
Tech Stack: Boston 311 API → Airflow → BigQuery | Llama-3.1-8B/Gemma-2 + PyTorch | FastAPI on Cloud Run | Next.js + Mapbox | GCP + Terraform
Structure:
boston-311-ai-system/
├── data_pipelines/  # Airflow DAGs, SQL
├── services/        # FastAPI (chat, prioritization, clustering)
├── models/          # PyTorch training
├── webapp/          # Next.js frontend
├── infra/           # Terraform
└── tests/           # Unit & integration tests
Installation
Prerequisites: Python 3.9+, Node.js 18+, Docker, Terraform 1.5+, GCP account
bash# Clone and setup
git clone https://github.com/sharonjennifer/boston-311-ai-system.git
cd boston-311-ai-system
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt

# Configure
cp .env.example .env  # Edit with credentials
gcloud auth application-default login
gcloud config set project YOUR_PROJECT_ID

# Deploy
cd infra/terraform
terraform init
terraform apply -var-file=environments/dev.tfvars
pytest tests/
Usage
Deploy Services:
bashgcloud builds submit --tag gcr.io/PROJECT/SERVICE
gcloud run deploy SERVICE --image gcr.io/PROJECT/SERVICE
Run Frontend:
bashcd webapp/frontend && npm install && npm run dev
Train Models:
bashpython models/training/train_priority.py
Development: Use main for production, feature/* for new work. Conventional commits required. PRs need 1 review + passing tests.
Documentation
Architecture | Setup Guide | API Reference | Monitoring
License & Contact
MIT License | Repository | Issues
