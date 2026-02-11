# Multi-stage build: Rust extension + Python application
# Usage:
#   docker build -t animetor-eval .
#   docker compose up -d

# ============================================================
# Stage 1: Build Rust extension
# ============================================================
FROM python:3.12-slim AS rust-builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl build-essential pkg-config libssl-dev \
    && curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y \
    && rm -rf /var/lib/apt/lists/*

ENV PATH="/root/.cargo/bin:${PATH}"

RUN pip install --no-cache-dir maturin

WORKDIR /build
COPY rust_ext/ ./rust_ext/

RUN cd rust_ext && maturin build --release --out /build/wheels

# ============================================================
# Stage 2: Application
# ============================================================
FROM python:3.12-slim AS app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies
COPY pixi.toml ./
RUN pip install --no-cache-dir \
    networkx pandas numpy httpx beautifulsoup4 lxml \
    matplotlib pydantic">=2.0" rich structlog typer "plotly>=6.5.2" \
    sparqlwrapper openskill "fastapi>=0.100" "uvicorn[standard]>=0.20" \
    "rapidfuzz>=3.0" "neo4j>=5.0" "python-dotenv>=1.0" "slowapi>=0.1.9"

# Install Rust extension from Stage 1
COPY --from=rust-builder /build/wheels/*.whl /tmp/wheels/
RUN pip install --no-cache-dir /tmp/wheels/*.whl && rm -rf /tmp/wheels

# Copy application code
COPY src/ ./src/
COPY static/ ./static/
COPY result/ ./result/

# Create data directories
RUN mkdir -p data/raw data/interim data/processed result/db result/json result/html result/notebooks

# Environment
ENV PYTHONPATH=/app \
    PYTHONUNBUFFERED=1 \
    ANIMETOR_LANG=ja

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD python -c "import httpx; httpx.get('http://localhost:8000/api/health')" || exit 1

# Default: run API server
CMD ["python", "-m", "uvicorn", "src.api:app", "--host", "0.0.0.0", "--port", "8000"]
