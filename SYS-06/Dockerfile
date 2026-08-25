# SYS-06: Operational Alerting & Notification Engine Dockerfile
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Prevent Python from writing .pyc and buffer stdout/stderr
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source and configurations
COPY src/ ./src/
COPY phase-01-alert-contracts/ ./phase-01-alert-contracts/

# Run default entry point
CMD ["python", "-m", "src.alert_dispatcher.main"]
