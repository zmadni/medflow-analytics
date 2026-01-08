# RAG Backend - FastAPI + Java + PySpark
FROM python:3.11-bookworm

# Install system dependencies (Java + curl)
RUN apt-get update && \
apt-get install -y --no-install-recommends \
openjdk-17-jdk-headless \
curl \
&& rm -rf /var/lib/apt/lists/*

# Set Java environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Install uv package manager
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:${PATH}"

# Verify installations
RUN java -version && uv --version

WORKDIR /app

# Copy requirements first (Docker layer caching)
COPY backend/requirements.txt .

# Install Python packages with uv (fast!)
RUN uv pip install --system -r requirements.txt

# Copy application code
COPY backend/ .

# Copy knowledge base and schemas
COPY schemas/ /app/schemas/
COPY knowledge/ /app/knowledge/

# Expose FastAPI port
EXPOSE 8000

# Health check endpoint
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
CMD curl -f http://localhost:8000/health || exit 1

# Run FastAPI server
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]