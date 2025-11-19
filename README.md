# Boston 311 AI System

An end-to-end AI-powered platform for managing Boston's 311 non-emergency service requests. The system features automated data pipelines, ML-driven priority prediction, an intelligent chatbot, and interactive analytics dashboards.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## 🎯 Project Overview

The Boston 311 AI System processes and analyzes Boston's non-emergency service request data to:
- **Automate data ingestion and transformation** via Apache Airflow
- **Predict case priorities** using XGBoost machine learning models
- **Provide real-time insights** through an LLM-powered chatbot (Gemini/Vertex AI)
- **Visualize city service performance** with interactive Next.js dashboards
- **Monitor service equity** through automated bias detection

---

## 🏗️ System Architecture

```
┌─────────────────────────────────────────────────────────────┐
│         Google Cloud Composer (Apache Airflow)              │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  Daily Pipeline    │  Weekly Pipeline  │  Fairness   │   │
│  │  (Incremental)     │  (Full Refresh)   │  Monitoring │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
         ┌────────────────────────┐
         │   Google Cloud Storage │
         │  (Bronze/Silver/Gold)  │
         └────────────┬───────────┘
                      │
                      ▼
         ┌────────────────────────┐
         │    Google BigQuery     │
         │  • Staging Tables      │
         │  • Production Tables   │
         │  • Chatbot Views       │
         │  • Dashboard Views     │
         └────────┬───────────────┘
                  │
        ┌─────────┴──────────┐
        │                    │
        ▼                    ▼
┌───────────────┐   ┌────────────────┐
│  FastAPI      │   │  Next.js       │
│  Chatbot      │   │  Dashboard     │
│  (Gemini AI)  │   │  (Mapbox)      │
└───────────────┘   └────────────────┘
                           │
                           ▼
                  ┌─────────────────┐
                  │  Flask ML       │
                  │  Priority       │
                  │  Dashboard      │
                  └─────────────────┘
```

---

## 🛠️ Technology Stack

| Layer | Technology |
|-------|------------|
| **Data Source** | Boston 311 API |
| **Orchestration** | Apache Airflow (Google Cloud Composer) |
| **Data Warehouse** | Google BigQuery |
| **Storage** | Google Cloud Storage (GCS) |
| **ML Framework** | XGBoost, scikit-learn |
| **ML Platform** | Vertex AI (Gemini) |
| **Chatbot Backend** | FastAPI + Gemini AI |
| **Dashboard Frontend** | Next.js 14 + TypeScript + Tailwind CSS |
| **Visualization** | Mapbox GL JS, Recharts |
| **ML Dashboard** | Flask + Jinja2 |
| **Infrastructure** | Terraform (GCP) |
| **Version Control** | Git, DVC (Data Version Control) |

---

## 📊 Features Implemented

### 1. **Data Pipelines (Airflow DAGs)**

#### `boston311_daily.py`
- **Schedule**: `@daily`
- **Function**: Incremental ingestion of the last 28 days of service requests
- **Features**:
  - Fetches updated/new records from Boston 311 API
  - Stores raw data in GCS as JSONL
  - Loads into BigQuery staging tables
  - Merges into production using `MERGE` SQL
  - Error handling with retries and empty-file skip logic

#### `boston311_weekly.py`
- **Schedule**: `0 4 * * 1` (Mondays at 4 AM)
- **Function**: Full dataset refresh with deduplication
- **Features**:
  - Complete historical data fetch
  - ROW_NUMBER() deduplication by `case_enquiry_id`
  - Full table replacement using `CREATE OR REPLACE`
  - Long-term consistency and schema alignment

#### `boston311_build_filtered_tables.py`
- **Schedule**: `0 1 * * *` (Daily at 1 AM)
- **Function**: Creates optimized views for chatbot and dashboard
- **Output Tables**:
  - `boston311.chatbot` - Filtered fields for LLM queries
  - `boston311.dashboard` - Aggregated metrics by neighborhood/department

#### `boston311_chat_service.py`
- **Function**: Generates specialized tables for chatbot service
- **Features**: Optimized schema for fast text-based queries

#### `boston311_fairness_weekly.py`
- **Schedule**: Weekly
- **Function**: Automated bias detection and equity monitoring
- **Features**:
  - Analyzes 30-day on-time performance by neighborhood, department, source
  - Flags slices >10% below median
  - Writes alerts to `bos311_bias_alerts_weekly`
  - Tracks mitigation actions in `bos311_bias_actions_taken`
  - Sends email notifications via Composer

---

### 2. **ML Priority Prediction System**

Located in `ml_prioritization_dashboard/`:

#### Training Pipeline (`train_priority_xgb.py`)
- **Model**: XGBoost classifier
- **Features**: Case type, neighborhood, department, time features, historical metrics
- **Output**: Trained model saved in `models/` directory
- **Metrics**: Classification report, feature importance

#### Scoring Pipeline (`score_priority_xgb.py`)
- **Function**: Real-time priority prediction for new cases
- **Input**: BigQuery service request data
- **Output**: Priority scores (High/Medium/Low)

#### Flask Dashboard (`priority_dashboard_app.py`)
- **Routes**:
  - `/` - Overview with priority distribution
  - `/predict` - Real-time prediction interface
  - `/model-info` - Model performance metrics
- **UI**: HTML templates in `templates/`

---

### 3. **AI Chatbot (FastAPI + Gemini)**

Located in `chatbot/`:

#### Backend (`chatbot/app/`)
- **Framework**: FastAPI
- **LLM**: Google Gemini (Vertex AI)
- **Components**:
  - `main.py` - FastAPI application entry point
  - `router_gemini.py` - Gemini AI integration with routing logic
  - `bq.py` - BigQuery connector for data retrieval
  - `retrieval.py` - Document/context retrieval system
  - `sql_templates.py` - Parameterized SQL query templates
  - `config.py` - Environment configuration

#### Features
- Natural language queries about 311 cases
- Real-time data fetching from BigQuery
- Context-aware responses using RAG (Retrieval-Augmented Generation)
- SQL query generation from natural language

#### Corpus
- Pre-built knowledge base in `chatbot/corpus/`
- Domain-specific context for better LLM responses

---

### 4. **Analytics Dashboard (Next.js)**

Located in `dashboard/`:

#### Frontend Stack
- **Framework**: Next.js 14 with App Router
- **Language**: TypeScript
- **Styling**: Tailwind CSS
- **Maps**: Mapbox GL JS
- **Charts**: Recharts library

#### Pages
1. **Overview Dashboard** (`app/page.tsx`)
   - KPI cards (total cases, open cases, closed cases, on-time %)
   - Daily trend charts
   - Case type distribution
   - Department workload

2. **Map View** (`app/dashboard/`)
   - Interactive Mapbox visualization
   - Geospatial case distribution
   - Filterable by type/status

3. **Case Details** (API routes available)
   - Individual case lookup
   - Historical case data

4. **Priority View**
   - ML-predicted priority rankings
   - Real-time priority updates

#### Components (`dashboard/components/`)
- `KPICard.tsx` - Key performance indicator cards
- `DailyTrendChart.tsx` - Time series visualization
- `CaseTypeBarChart.tsx` - Case type breakdown
- `DepartmentWorkloadChart.tsx` - Department performance
- `NeighborhoodBarChart.tsx` - Neighborhood distribution
- `NeighborhoodTrendChart.tsx` - Neighborhood trends over time
- `MapWrapper.tsx` - Mapbox integration
- `PriorityTable.tsx` - ML priority display
- `Sidebar.tsx` - Navigation sidebar
- `Navbar.tsx` - Top navigation

#### API Routes (`dashboard/app/api/`)
- `/api/kpis` - KPI metrics
- `/api/charts/*` - Chart data endpoints
- `/api/map` - Map data with coordinates
- `/api/case/*` - Case detail retrieval
- `/api/prioritization` - ML priority data

---

## 📁 Project Structure

```
boston-311-ai-system/
├── chatbot/                          # FastAPI chatbot service
│   ├── app/
│   │   ├── main.py                   # FastAPI app
│   │   ├── router_gemini.py          # Gemini AI routing
│   │   ├── bq.py                     # BigQuery connector
│   │   ├── retrieval.py              # RAG retrieval
│   │   ├── sql_templates.py          # SQL templates
│   │   └── config.py                 # Configuration
│   ├── corpus/                       # Knowledge base
│   └── requirements.txt
│
├── dashboard/                        # Next.js analytics dashboard
│   ├── app/
│   │   ├── api/                      # API routes
│   │   │   ├── kpis/
│   │   │   ├── charts/
│   │   │   ├── map/
│   │   │   ├── case/
│   │   │   └── prioritization/
│   │   ├── dashboard/                # Dashboard pages
│   │   ├── layout.tsx
│   │   └── page.tsx                  # Home page
│   ├── components/                   # React components
│   │   ├── KPICard.tsx
│   │   ├── DailyTrendChart.tsx
│   │   ├── CaseTypeBarChart.tsx
│   │   ├── DepartmentWorkloadChart.tsx
│   │   ├── NeighborhoodBarChart.tsx
│   │   ├── NeighborhoodTrendChart.tsx
│   │   ├── MapWrapper.tsx
│   │   ├── PriorityTable.tsx
│   │   ├── Sidebar.tsx
│   │   └── Navbar.tsx
│   ├── package.json
│   └── next.config.ts
│
├── data_pipelines/                   # Airflow pipelines
│   ├── dags/
│   │   ├── boston311_daily.py        # Daily incremental
│   │   ├── boston311_weekly.py       # Weekly full refresh
│   │   ├── boston311_build_filtered_tables.py
│   │   ├── boston311_chat_service.py
│   │   └── boston311_fairness_weekly.py
│   ├── scripts/                      # Utility scripts
│   ├── data/                         # Sample/test data
│   └── tests/                        # Pipeline tests
│
├── ml_prioritization_dashboard/     # ML priority system
│   ├── models/                       # Trained models
│   ├── templates/                    # Flask HTML templates
│   ├── train_priority_xgb.py         # Model training
│   ├── score_priority_xgb.py         # Scoring script
│   └── priority_dashboard_app.py     # Flask dashboard
│
├── docs/                             # Documentation
│   ├── errors-failure.pdf
│   └── user-need.pdf
│
├── .dvc/                             # DVC configuration
├── dvc.yaml                          # DVC pipeline definition
├── .gitignore
├── pytest.ini                        # Test configuration
└── README.md
```

---

## 🚀 Getting Started

### Prerequisites

- **Python 3.9+** - Backend and data pipelines
- **Node.js 18+** - Frontend dashboard
- **Docker** - Containerization (optional)
- **Terraform 1.5+** - Infrastructure provisioning
- **Google Cloud SDK** - GCP authentication

### Installation

#### 1. Clone the Repository
```bash
git clone https://github.com/sharonjennifer/boston-311-ai-system.git
cd boston-311-ai-system
```

#### 2. Set Up Python Environment
```bash
python -m venv venv
source venv/bin/activate  # macOS/Linux
# venv\Scripts\activate   # Windows

pip install -r chatbot/requirements.txt
pip install -r ml_prioritization_dashboard/requirements.txt
```

#### 3. Configure GCP Authentication
```bash
gcloud auth application-default login
gcloud config set project <your-project-id>
```

#### 4. Set Up Environment Variables
```bash
cp .env.example .env
# Edit .env with your credentials:
# - GCP_PROJECT_ID
# - BIGQUERY_DATASET
# - GCS_BUCKET
# - VERTEX_AI_LOCATION
# - MAPBOX_ACCESS_TOKEN
```

#### 5. Install Dashboard Dependencies
```bash
cd dashboard
npm install
```

---

## 🏃 Running the System

### Data Pipelines (Cloud Composer)

Deploy DAGs to your Cloud Composer environment:
```bash
gsutil cp data_pipelines/dags/*.py gs://<composer-bucket>/dags/
```

Monitor in Airflow UI: `https://<composer-url>/home`

### Chatbot Service

```bash
cd chatbot
uvicorn app.main:app --reload --port 8000
```
Access: `http://localhost:8000/docs` (FastAPI Swagger UI)

### Analytics Dashboard

```bash
cd dashboard
npm run dev
```
Access: `http://localhost:3000`

### ML Priority Dashboard

```bash
cd ml_prioritization_dashboard
python priority_dashboard_app.py
```
Access: `http://localhost:5000`

---

## 🧪 Testing

```bash
# Run data pipeline tests
cd data_pipelines
pytest tests/

# Run dashboard tests (if implemented)
cd dashboard
npm test
```

---

## 📈 Pipeline Performance

| Pipeline | Avg Runtime | Frequency |
|----------|-------------|-----------|
| Daily Incremental | ~5 minutes | Daily |
| Weekly Full Refresh | ~15 minutes | Weekly |
| Filtered Tables | ~2 minutes | Daily |
| Fairness Monitor | ~8 minutes | Weekly |

---

## 🎯 Key Metrics & Monitoring

- **Data Freshness**: Daily updates ensure <24hr latency
- **Data Quality**: Weekly full refresh eliminates drift
- **Model Performance**: Priority model evaluated on hold-out test set
- **Bias Monitoring**: Automated weekly fairness audits
- **System Health**: Airflow DAG success rates tracked in Cloud Monitoring

---

## 📊 BigQuery Tables

### Production Tables
- `boston311.service_requests` - Main production table
- `boston311.chatbot` - Optimized for LLM queries
- `boston311.dashboard` - Aggregated metrics
- `boston311.bias_alerts_weekly` - Fairness monitoring
- `boston311.bias_actions_taken` - Mitigation tracking

---

## 🔒 Security & Privacy

- **Authentication**: GCP service account with least-privilege IAM roles
- **Data Storage**: Encrypted at rest in GCS and BigQuery
- **API Security**: Rate limiting and input validation
- **Secrets Management**: Environment variables, never committed to git

---

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'feat: add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Commit Conventions
- `feat:` - New features
- `fix:` - Bug fixes
- `docs:` - Documentation updates
- `refactor:` - Code refactoring
- `test:` - Test additions/updates

---

## 📝 Future Enhancements

- [ ] Real-time streaming pipeline (Pub/Sub + Dataflow)
- [ ] Advanced NLP for case classification
- [ ] Predictive maintenance for city infrastructure
- [ ] Mobile app integration
- [ ] Multi-language chatbot support
- [ ] Enhanced bias mitigation strategies

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 👥 Authors

- **Sharon Jennifer** - [GitHub](https://github.com/sharonjennifer)

---

## 🙏 Acknowledgments

- City of Boston for providing open 311 data
- Google Cloud Platform for infrastructure
- Northeastern University for academic support


---

**Built with ❤️ for the City of Boston**
