FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /app

COPY pyproject.toml uv.lock alembic.ini ./
COPY src ./src
COPY apps ./apps
COPY alembic ./alembic
COPY data ./data
COPY docs ./docs

RUN uv sync --frozen

ENV PATH="/app/.venv/bin:${PATH}"
ENV PYTHONPATH="/app/src"
