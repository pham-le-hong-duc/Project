FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first (this layer will be cached if requirements.txt doesn't change)
COPY docker/requirements.txt .

# Install Python dependencies with caching
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --upgrade pip && \
    pip install -r requirements.txt

# Copy application code (this layer rebuilds only when source code changes)
COPY src/ ./src/

# Default command (can be overridden in docker-compose)
CMD ["python", "-u"]
