# Use Python 3.12 Slim (Small & Fast)
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install system dependencies (curl needed for healthchecks/debugging)
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Install uv (The magic package manager)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# 1. Copy dependency files first (Caching layer)
# Docker will reuse this layer if pyproject.toml hasn't changed
COPY pyproject.toml uv.lock ./

# 2. Install dependencies
# --system: Install into the container's global python (no virtualenv needed inside Docker)
RUN uv sync --frozen --system

# 3. Copy the rest of the application code
COPY src/ ./src/

# 4. Setup Dagster Home
# We create a directory for Dagster to store its SQLite DB and logs
ENV DAGSTER_HOME=/opt/dagster/dagster_home
RUN mkdir -p $DAGSTER_HOME

# (Optional) We don't define a CMD here because docker-compose will handle it