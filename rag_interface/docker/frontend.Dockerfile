# RAG Frontend - Streamlit                                                                                                                                                14:48:21 [80/1885]
FROM python:3.11-slim

# Install curl for health checks
RUN apt-get update && \
apt-get install -y --no-install-recommends curl && \
rm -rf /var/lib/apt/lists/*

# Install uv
RUN curl -LsSf https://astral.sh/uv/install.sh | sh

# Add uv to PATH
ENV PATH="/root/.local/bin:${PATH}"

WORKDIR /app

# Copy frontend requirements
COPY frontend/requirements.txt .

# Install with uv
RUN uv pip install --system -r requirements.txt

# Copy frontend code
COPY frontend/ .

# Expose Streamlit port
EXPOSE 8501

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
CMD curl -f http://localhost:8501/_stcore/health || exit 1

# Run Streamlit
CMD ["streamlit", "run", "pages/app.py", "--server.port=8501", "--server.address=0.0.0.0"]