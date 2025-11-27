# Base image with Python 3.11
FROM python:3.11-slim

# Work inside /app
WORKDIR /app

# Install system deps (needed for some Python libs like xgboost / numpy)
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY ml_prioritization_dashboard/requirements.txt ./ml_prioritization_dashboard/requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r ml_prioritization_dashboard/requirements.txt

# Copy the whole repo (so we get all scripts + dashboard + models folder)
COPY . .

# Default env vars (same ones you use in CI)
ENV GCP_PROJECT=boston311-mlops \
    BQ_LOCATION=US \
    MODEL_REGISTRY_BUCKET=boston311-ml-model-registry \
    TRAIN_FEATURE_TABLE=tbl_train_features

# Where the training script expects the SA key:
# SA_PATH = ROOT_DIR / "secrets" / "bq-dashboard-ro.json"  (in Python)
ENV GOOGLE_APPLICATION_CREDENTIALS=/app/secrets/bq-dashboard-ro.json

# Make sure secrets dir exists in the image
RUN mkdir -p /app/secrets

# Default command: run the training script
CMD ["python", "ml_prioritization_dashboard/train_priority_xgb.py"]
