FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Copy dependency files first (Caching layer)
COPY pyproject.toml uv.lock ./

# Install dependencies into system python
RUN uv sync --frozen --system

# Copy source code
COPY src/ ./src/
# Copy config files if you have them (but we will rely on Env Vars)
COPY .env .env 

# Create Dagster Home directory
RUN mkdir -p /opt/dagster/dagster_home
ENV DAGSTER_HOME=/opt/dagster/dagster_home

# Copy dagster.yaml if you created one (optional)
# COPY .dagster/dagster.yaml /opt/dagster/dagster_home/dagster.yaml

