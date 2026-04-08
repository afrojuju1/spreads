FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /app

ENV UV_PROJECT_ENVIRONMENT="/opt/venv"
ENV PATH="/opt/venv/bin:${PATH}"
ENV PYTHONPATH="/app/src"

COPY pyproject.toml uv.lock alembic.ini ./
COPY src ./src
COPY apps ./apps
COPY alembic ./alembic
COPY data ./data
COPY docs ./docs

RUN uv sync --frozen
