# Use a lightweight Python image
FROM python:3.11-slim

# Set workdir
WORKDIR /app

# Install system deps (for faiss, numpy, etc.)
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt ./requirements.txt

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copy the chatbot code and data
COPY . .

# Expose port for uvicorn
EXPOSE 8080

# Default envs (can be overridden in Cloud Run)
ENV B311_DATA_DIR=data
ENV B311_LOG_LEVEL=INFO

# Run FastAPI with uvicorn
# IMPORTANT: 0.0.0.0 and port 8080 for Cloud Run
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
