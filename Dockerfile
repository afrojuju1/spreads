FROM ghcr.io/astral-sh/uv:python3.12-trixie-slim

WORKDIR /app

ENV UV_PROJECT_ENVIRONMENT="/opt/venv"
ENV PATH="/opt/venv/bin:${PATH}"
ENV PYTHONPATH="/app/packages"

COPY pyproject.toml uv.lock alembic.ini ./
COPY packages ./packages
COPY alembic ./alembic
COPY data ./data
COPY docs ./docs

RUN apt-get update \
    && apt-get install -y --no-install-recommends postgresql-client \
    && rm -rf /var/lib/apt/lists/*

RUN uv sync --frozen
