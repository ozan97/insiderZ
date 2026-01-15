# ==========================================
# Stage 1: The Builder (Heavy Lifting)
# ==========================================
FROM python:3.12-slim AS builder

WORKDIR /app

# Install git and compilers needed for python deps
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    libxml2-dev \
    libxslt-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Create the virtual environment
ENV UV_PROJECT_ENVIRONMENT="/app/.venv"
# Compile the environment
COPY pyproject.toml ./
RUN uv lock
RUN uv sync --frozen --no-install-project

# ==========================================
# Stage 2: The Runner (Slim & Clean)
# ==========================================
FROM python:3.12-slim

WORKDIR /app

# Copy uv (It's tiny, useful for running commands)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Copy the pre-built virtual environment from Stage 1
COPY --from=builder /app/.venv /app/.venv

# Add the venv to the PATH
ENV PATH="/app/.venv/bin:$PATH"

# Copy application code
COPY src/ ./src/

ENV DAGSTER_HOME=/opt/dagster/dagster_home
RUN mkdir -p $DAGSTER_HOME