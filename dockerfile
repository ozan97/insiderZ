FROM python:3.12-slim-trixie

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates build-essential libxml2-dev libxslt-dev && \
    rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
COPY pyproject.toml ./

RUN uv lock
RUN uv sync --frozen --no-cache

COPY src/ ./src/

ENV DAGSTER_HOME=/opt/dagster/dagster_home
RUN mkdir -p $DAGSTER_HOME