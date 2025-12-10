FROM python:3.11-slim

# Make Python logs unbuffered
ENV PYTHONUNBUFFERED=1

# Cloud Run will inject PORT; default only matters for local runs
ENV PORT=8080

# For Cloud Run we want to use its service account, not a local JSON key
ENV B311_USE_LOCAL_SA=false

WORKDIR /app

# System deps for numpy/pandas/scikit-learn/xgboost, etc.
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        gcc \
    && rm -rf /var/lib/apt/lists/*

# Install Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code + templates + static
COPY . .

# (Safety) Ensure model is present in the container where the app expects it
# If your model file is already under ./models/ this COPY is effectively a no-op,
# but it guarantees /app/models/priority_model.pkl exists.
COPY models/priority_model.pkl /app/models/priority_model.pkl

# Gunicorn entrypoint for Cloud Run
CMD ["gunicorn", "-b", "0.0.0.0:8080", "priority_dashboard_app:app"]
