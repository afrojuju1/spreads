from __future__ import annotations

import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import create_engine, pool

from spreads.storage import alert_models as storage_alert_models  # noqa: F401
from spreads.storage import broker_models as storage_broker_models  # noqa: F401
from spreads.storage import calendar_models as storage_calendar_models  # noqa: F401
from spreads.storage import collector_models as storage_collector_models  # noqa: F401
from spreads.storage import control_models as storage_control_models  # noqa: F401
from spreads.storage import event_models as storage_event_models  # noqa: F401
from spreads.storage import execution_models as storage_execution_models  # noqa: F401
from spreads.storage import job_models as storage_job_models  # noqa: F401
from spreads.storage import post_market_models as storage_post_market_models  # noqa: F401
from spreads.storage import recovery_models as storage_recovery_models  # noqa: F401
from spreads.storage import risk_models as storage_risk_models  # noqa: F401
from spreads.storage import signal_models as storage_signal_models  # noqa: F401
from spreads.storage.db import Base
from spreads.storage import models as storage_models  # noqa: F401

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def normalize_database_url(url: str) -> str:
    if url.startswith("postgresql://"):
        return url.replace("postgresql://", "postgresql+psycopg://", 1)
    if url.startswith("postgres://"):
        return url.replace("postgres://", "postgresql+psycopg://", 1)
    return url


def get_url() -> str:
    env_url = os.environ.get("SPREADS_DATABASE_URL") or os.environ.get("DATABASE_URL")
    if env_url:
        return normalize_database_url(env_url)
    configured = config.get_main_option("sqlalchemy.url")
    return normalize_database_url(configured)


def run_migrations_offline() -> None:
    context.configure(
        url=get_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = create_engine(get_url(), poolclass=pool.NullPool)

    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
