# ==============================================================================
# 🧠 SYS-04: MLOps & Model Serving Center Dockerfile
# ==============================================================================
FROM python:3.11-slim

WORKDIR /app

# Install system runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

# Install PyTorch CPU directly (lightweight ~180MB CPU wheel)
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir torch --index-url https://download.pytorch.org/whl/cpu

# Copy dependencies list and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code and AI Models into the container
COPY . /app

# Ensure permanent fallback backup of all models inside the image layer
# This guarantees that even if Docker is quit, stopped, or run with fresh volume,
# the models are NEVER lost!
RUN mkdir -p /app/models_backup && \
    cp -r "/app/AI Models/models/"* /app/models_backup/ 2>/dev/null || true

# Expose standard serving port 8000
EXPOSE 8000

ENV PYTHONUNBUFFERED=1
ENV PORT=8000
ENV HOST=0.0.0.0
ENV MODELS_DIR=/app/models
ENV BACKUP_DIR=/app/models_backup

# Launch persistence guard and uvicorn server
CMD ["python", "entrypoint.py"]
