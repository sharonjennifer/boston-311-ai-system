# Use an official lightweight Python image
FROM python:3.11-slim

WORKDIR /app

# Install system deps (for scientific stack)
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code
COPY . .

# Cloud Run will pass PORT; default to 8080 locally
ENV PORT=8080
ENV PYTHONUNBUFFERED=1

# Start via gunicorn (production-ready)
CMD exec gunicorn --bind 0.0.0.0:${PORT} priority_dashboard_app:app
