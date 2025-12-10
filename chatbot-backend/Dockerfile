# Production Dockerfile for Boston 311 Chatbot Backend
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first (for Docker layer caching)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create necessary directories
RUN mkdir -p /app/data /app/templates /app/faq

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PORT=8080

# Expose port (Cloud Run uses 8080 by default)
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=40s --retries=3 \
  CMD python -c "import requests; requests.get('http://localhost:8080/health')" || exit 1

# Run the application
CMD uvicorn app.main:app --host 0.0.0.0 --port ${PORT} --workers 1