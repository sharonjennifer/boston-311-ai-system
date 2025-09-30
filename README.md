Boston 311 AI System
AI-powered complaint management system for Boston with chatbots, analytics dashboards, and ML-driven insights.

Features
Public Chatbots

Case status lookup by ticket ID
Community insights (top complaints by neighborhood)
Proactive alerts filtered by type, location, and timeframe

Internal Tools

Weekly city health reports with automated summarization
Department prioritization recommendations
Policy knowledge assistant (RAG-based)
Operations dashboards with KPIs, heat maps, trend detection

ML Capabilities

Priority scoring using PyTorch
Duplicate complaint clustering
Complaint lifecycle visualization (Sankey diagrams)
Model drift detection


Architecture
Data Pipeline: Boston 311 API → Cloud Composer (Airflow) → BigQuery (Silver/Gold layers)
AI/ML: Llama-3.1-8B/Gemma-2 LLM + PyTorch models + Vertex AI Vector Search
Services: FastAPI on Cloud Run
Frontend: Next.js React with Mapbox
Infrastructure: GCP (BigQuery, Cloud Run, Composer), Terraform IaC

Repository Structure
boston-311-ai-system/
├── data_pipelines/    # Airflow DAGs, SQL transformations
├── services/          # FastAPI microservices (chat, prioritization, clustering)
├── models/            # PyTorch training scripts, notebooks
├── webapp/            # Next.js React frontend
├── infra/             # Terraform infrastructure
├── docs/              # Documentation
├── tests/             # Unit & integration tests
└── .github/workflows/ # CI/CD pipelines

License
MIT License - see LICENSE file.
