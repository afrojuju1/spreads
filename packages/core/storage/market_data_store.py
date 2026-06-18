from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any
from urllib.parse import unquote, urlparse
from zoneinfo import ZoneInfo

import clickhouse_connect

from core.runtime.config import default_clickhouse_url
from core.storage.records import OptionQuoteTickRecord, OptionTradeTickRecord
from core.storage.serializers import parse_datetime, render_value

RAW_QUOTE_RETENTION_DAYS = 14
RAW_TRADE_RETENTION_DAYS = 90
QUOTE_SNAPSHOT_1S_RETENTION_DAYS = 90
QUOTE_SNAPSHOT_1M_RETENTION_DAYS = 730

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

QUOTE_COLUMNS = (
    "captured_at",
    "source_timestamp",
    "cycle_id",
    "label",
    "profile",
    "underlying_symbol",
    "strategy",
    "option_symbol",
    "leg_role",
    "bid",
    "ask",
    "midpoint",
    "bid_size",
    "ask_size",
    "source",
)

TRADE_COLUMNS = (
    "captured_at",
    "source_timestamp",
    "cycle_id",
    "label",
    "profile",
    "underlying_symbol",
    "strategy",
    "option_symbol",
    "leg_role",
    "price",
    "size",
    "premium",
    "exchange_code",
    "conditions_json",
    "included_in_score",
    "exclusion_reason",
    "raw_payload_json",
    "source",
)

QUOTE_SNAPSHOT_COLUMNS = (
    "bucket_at",
    "captured_at",
    "source_timestamp",
    "cycle_id",
    "label",
    "profile",
    "underlying_symbol",
    "strategy",
    "option_symbol",
    "leg_role",
    "bid",
    "ask",
    "midpoint",
    "bid_size",
    "ask_size",
    "source",
)

MARKET_DATA_TABLE_NAMES = (
    "option_quote_ticks",
    "option_trade_ticks",
    "option_quote_snapshots_1s",
    "option_quote_snapshots_1m",
)


@dataclass(frozen=True, slots=True)
class MarketDataTable:
    name: str
    data_class: str
    retention_days: int
    retention_column: str
    engine: str


MARKET_DATA_TABLES = (
    MarketDataTable(
        name="option_quote_ticks",
        data_class="option_quotes_raw",
        retention_days=RAW_QUOTE_RETENTION_DAYS,
        retention_column="captured_at",
        engine="MergeTree",
    ),
    MarketDataTable(
        name="option_trade_ticks",
        data_class="option_trades_raw",
        retention_days=RAW_TRADE_RETENTION_DAYS,
        retention_column="captured_at",
        engine="MergeTree",
    ),
    MarketDataTable(
        name="option_quote_snapshots_1s",
        data_class="option_quotes_snapshot_1s",
        retention_days=QUOTE_SNAPSHOT_1S_RETENTION_DAYS,
        retention_column="bucket_at",
        engine="ReplacingMergeTree",
    ),
    MarketDataTable(
        name="option_quote_snapshots_1m",
        data_class="option_quotes_snapshot_1m",
        retention_days=QUOTE_SNAPSHOT_1M_RETENTION_DAYS,
        retention_column="bucket_at",
        engine="ReplacingMergeTree",
    ),
)

_MARKET_DATA_TABLES_BY_NAME = {table.name: table for table in MARKET_DATA_TABLES}


class ClickHouseMarketDataStore:
    def __init__(self, clickhouse_url: str | None = None) -> None:
        self.clickhouse_url = str(clickhouse_url or default_clickhouse_url())
        self._connection = _parse_clickhouse_url(self.clickhouse_url)
        self.database = self._connection["database"]
        self._client: Any | None = None
        self._schema_ready = False

    def close(self) -> None:
        client = self._client
        if client is not None:
            close = getattr(client, "close", None)
            if callable(close):
                close()
        self._client = None

    def ensure_schema(self) -> None:
        if self._schema_ready:
            return
        database = _identifier(self.database)
        base_client = clickhouse_connect.get_client(**self._client_kwargs(database=None))
        base_client.command(f"CREATE DATABASE IF NOT EXISTS {database}")
        client = self._client_instance()
        for statement in self._schema_statements(database=database):
            client.command(statement)
        for statement in self._retention_statements(database=database):
            client.command(statement)
        self._schema_ready = True

    def schema_ready(self) -> bool:
        try:
            existing = self._existing_tables()
        except Exception:
            return False
        return set(MARKET_DATA_TABLE_NAMES).issubset(existing)

    def table_counts(self) -> dict[str, int]:
        self.ensure_schema()
        rows = self._query_dicts(
            """
            SELECT
              table,
              sum(rows) AS row_count
            FROM system.parts
            WHERE database = {database}
              AND table IN {tables}
              AND active = 1
            GROUP BY table
            """.format(
                database=_quote_string(self.database),
                tables=_string_tuple(MARKET_DATA_TABLE_NAMES),
            )
        )
        counts = {name: 0 for name in MARKET_DATA_TABLE_NAMES}
        counts.update({str(row["table"]): int(row["row_count"] or 0) for row in rows})
        return counts

    def truncate_all(self) -> None:
        self.ensure_schema()
        for table_name in MARKET_DATA_TABLE_NAMES:
            self._client_instance().command(f"TRUNCATE TABLE {_identifier(table_name)}")

    def table_storage_rows(self) -> list[dict[str, Any]]:
        self.ensure_schema()
        existing = self._existing_tables()
        stats_by_table = {
            str(row["table"]): row
            for row in self._query_dicts(
                """
                SELECT
                  table,
                  countIf(active = 1) AS active_part_count,
                  countIf(active = 0) AS inactive_part_count,
                  sumIf(rows, active = 1) AS estimated_live_rows,
                  sum(bytes_on_disk) AS total_size_bytes,
                  sumIf(bytes_on_disk, active = 1) AS active_size_bytes,
                  minIf(min_time, active = 1) AS oldest_value,
                  maxIf(max_time, active = 1) AS newest_value
                FROM system.parts
                WHERE database = {database}
                  AND table IN {tables}
                GROUP BY table
                """.format(
                    database=_quote_string(self.database),
                    tables=_string_tuple(MARKET_DATA_TABLE_NAMES),
                )
            )
        }
        rows: list[dict[str, Any]] = []
        for table in MARKET_DATA_TABLES:
            stats = stats_by_table.get(table.name, {})
            ttl_expression = self._ttl_expression(table)
            rows.append(
                {
                    "name": table.name,
                    "physical_table": f"{self.database}.{table.name}",
                    "database": self.database,
                    "engine": table.engine,
                    "data_class": table.data_class,
                    "schema_ready": table.name in existing,
                    "retention_owner": "clickhouse_ttl",
                    "retention_days": table.retention_days,
                    "retention_column": table.retention_column,
                    "ttl_policy": f"{ttl_expression} + INTERVAL {table.retention_days} DAY DELETE",
                    "active_part_count": int(stats.get("active_part_count") or 0),
                    "inactive_part_count": int(stats.get("inactive_part_count") or 0),
                    "estimated_live_rows": int(stats.get("estimated_live_rows") or 0),
                    "estimated_dead_rows": 0,
                    "total_size_bytes": int(stats.get("total_size_bytes") or 0),
                    "active_size_bytes": int(stats.get("active_size_bytes") or 0),
                    "oldest_value": render_value(stats.get("oldest_value")),
                    "newest_value": render_value(stats.get("newest_value")),
                }
            )
        return rows

    def storage_status(self) -> dict[str, Any]:
        tables = self.table_storage_rows()
        missing = [str(row["name"]) for row in tables if not row.get("schema_ready")]
        total_size_bytes = sum(int(row.get("total_size_bytes") or 0) for row in tables)
        estimated_live_rows = sum(int(row.get("estimated_live_rows") or 0) for row in tables)
        inactive_part_count = sum(int(row.get("inactive_part_count") or 0) for row in tables)
        return {
            "status": "blocked" if missing else "healthy",
            "summary": {
                "market_data_database": self.database,
                "market_data_url": _redact_clickhouse_url(self.clickhouse_url),
                "retention_owner": "clickhouse_ttl",
                "market_data_table_count": len(tables),
                "market_data_tables_ready": not missing,
                "missing_market_data_tables": missing,
                "total_size_bytes": total_size_bytes,
                "estimated_live_rows": estimated_live_rows,
                "estimated_dead_rows": 0,
                "inactive_part_count": inactive_part_count,
                "raw_quote_retention_days": RAW_QUOTE_RETENTION_DAYS,
                "raw_trade_retention_days": RAW_TRADE_RETENTION_DAYS,
                "quote_snapshot_1s_retention_days": QUOTE_SNAPSHOT_1S_RETENTION_DAYS,
                "quote_snapshot_1m_retention_days": QUOTE_SNAPSHOT_1M_RETENTION_DAYS,
            },
            "details": {
                "tables": tables,
                "maintenance": {
                    "retention_owner": "clickhouse_ttl",
                    "lock_profile": "ClickHouse table TTL owns market-data retention; Postgres partition maintenance is not part of raw tick storage.",
                    "manual_prune_command": None,
                    "default_state_uses_partition_catalog": False,
                },
            },
        }

    def save_option_quote_tick_rows(self, *, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        self.ensure_schema()
        normalized_rows = [_normalize_quote_row(row) for row in rows]
        self._client_instance().insert(
            "option_quote_ticks",
            [tuple(row[column] for column in QUOTE_COLUMNS) for row in normalized_rows],
            column_names=list(QUOTE_COLUMNS),
        )
        self._save_quote_snapshots(normalized_rows)
        return len(normalized_rows)

    def save_option_quote_ticks(
        self,
        *,
        cycle_id: str,
        label: str,
        profile: str,
        quotes: list[dict[str, Any]],
    ) -> int:
        rows = [
            {
                **dict(quote),
                "cycle_id": cycle_id,
                "label": label,
                "profile": profile,
            }
            for quote in quotes
        ]
        return self.save_option_quote_tick_rows(rows=rows)

    def save_option_trade_tick_rows(self, *, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        self.ensure_schema()
        normalized_rows = [_normalize_trade_row(row) for row in rows]
        self._client_instance().insert(
            "option_trade_ticks",
            [tuple(row[column] for column in TRADE_COLUMNS) for row in normalized_rows],
            column_names=list(TRADE_COLUMNS),
        )
        return len(normalized_rows)

    def save_option_trade_ticks(
        self,
        *,
        cycle_id: str,
        label: str,
        profile: str,
        trades: list[dict[str, Any]],
    ) -> int:
        rows = [
            {
                **dict(trade),
                "cycle_id": cycle_id,
                "label": label,
                "profile": profile,
            }
            for trade in trades
        ]
        return self.save_option_trade_tick_rows(rows=rows)

    def list_option_quote_ticks_window(
        self,
        *,
        option_symbols: list[str],
        captured_from: str | datetime,
        captured_to: str | datetime | None = None,
        label: str | None = None,
        profile: str | None = None,
        sources: str | list[str] | None = None,
    ) -> list[OptionQuoteTickRecord]:
        symbols = _normalized_texts(option_symbols)
        captured_from_dt = parse_datetime(captured_from)
        captured_to_dt = parse_datetime(captured_to)
        if not symbols or captured_from_dt is None or (captured_to_dt is not None and captured_from_dt >= captured_to_dt):
            return []
        clauses = [
            f"option_symbol IN {_string_tuple(symbols)}",
            f"captured_at >= {_quote_datetime(captured_from_dt)}",
        ]
        if captured_to_dt is not None:
            clauses.append(f"captured_at < {_quote_datetime(captured_to_dt)}")
        if label is not None:
            clauses.append(f"label = {_quote_string(label)}")
        if profile is not None:
            clauses.append(f"profile = {_quote_string(profile)}")
        source_values = _normalized_sources(sources)
        if source_values:
            clauses.append(f"source IN {_string_tuple(source_values)}")
        return self._query_dicts(
            f"""
            SELECT {", ".join(QUOTE_COLUMNS)}
            FROM option_quote_ticks
            WHERE {" AND ".join(clauses)}
            ORDER BY captured_at ASC
            """
        )

    def list_option_quote_snapshots_window(
        self,
        *,
        option_symbols: list[str],
        captured_from: str | datetime,
        captured_to: str | datetime | None = None,
        label: str | None = None,
        profile: str | None = None,
        resolution: str = "1m",
    ) -> list[OptionQuoteTickRecord]:
        symbols = _normalized_texts(option_symbols)
        captured_from_dt = parse_datetime(captured_from)
        captured_to_dt = parse_datetime(captured_to)
        if not symbols or captured_from_dt is None or (captured_to_dt is not None and captured_from_dt >= captured_to_dt):
            return []
        table_name = {
            "1s": "option_quote_snapshots_1s",
            "1m": "option_quote_snapshots_1m",
        }.get(str(resolution or "1m").strip().lower())
        if table_name is None:
            raise ValueError("option quote snapshot resolution must be '1s' or '1m'")
        clauses = [
            f"option_symbol IN {_string_tuple(symbols)}",
            f"captured_at >= {_quote_datetime(captured_from_dt)}",
        ]
        if captured_to_dt is not None:
            clauses.append(f"captured_at < {_quote_datetime(captured_to_dt)}")
        if label is not None:
            clauses.append(f"label = {_quote_string(label)}")
        if profile is not None:
            clauses.append(f"profile = {_quote_string(profile)}")
        return self._query_dicts(
            f"""
            SELECT {", ".join(QUOTE_SNAPSHOT_COLUMNS)}
            FROM {_identifier(table_name)}
            WHERE {" AND ".join(clauses)}
            ORDER BY captured_at ASC
            """
        )

    def list_option_trade_ticks_window(
        self,
        *,
        option_symbols: list[str],
        captured_from: str | datetime,
        captured_to: str | datetime | None = None,
        label: str | None = None,
        profile: str | None = None,
        sources: str | list[str] | None = None,
    ) -> list[OptionTradeTickRecord]:
        symbols = _normalized_texts(option_symbols)
        captured_from_dt = parse_datetime(captured_from)
        captured_to_dt = parse_datetime(captured_to)
        if not symbols or captured_from_dt is None or (captured_to_dt is not None and captured_from_dt >= captured_to_dt):
            return []
        clauses = [
            f"option_symbol IN {_string_tuple(symbols)}",
            f"captured_at >= {_quote_datetime(captured_from_dt)}",
        ]
        if captured_to_dt is not None:
            clauses.append(f"captured_at < {_quote_datetime(captured_to_dt)}")
        if label is not None:
            clauses.append(f"label = {_quote_string(label)}")
        if profile is not None:
            clauses.append(f"profile = {_quote_string(profile)}")
        source_values = _normalized_sources(sources)
        if source_values:
            clauses.append(f"source IN {_string_tuple(source_values)}")
        return self._query_dicts(
            f"""
            SELECT {", ".join(TRADE_COLUMNS)}
            FROM option_trade_ticks
            WHERE {" AND ".join(clauses)}
            ORDER BY captured_at ASC
            """
        )

    def list_latest_option_quotes_by_underlying(
        self,
        *,
        underlying_symbols: list[str],
        captured_from: str | datetime,
        captured_to: str | datetime | None = None,
        label: str | None = None,
        profile: str | None = None,
        limit: int = 1000,
    ) -> list[OptionQuoteTickRecord]:
        symbols = _normalized_texts(underlying_symbols)
        captured_from_dt = parse_datetime(captured_from)
        captured_to_dt = parse_datetime(captured_to)
        if not symbols or captured_from_dt is None or (captured_to_dt is not None and captured_from_dt >= captured_to_dt):
            return []
        clauses = [
            f"underlying_symbol IN {_string_tuple(symbols)}",
            f"captured_at >= {_quote_datetime(captured_from_dt)}",
        ]
        if captured_to_dt is not None:
            clauses.append(f"captured_at < {_quote_datetime(captured_to_dt)}")
        if label is not None:
            clauses.append(f"label = {_quote_string(label)}")
        if profile is not None:
            clauses.append(f"profile = {_quote_string(profile)}")
        return self._query_dicts(
            f"""
            SELECT
              latest_captured_at AS captured_at,
              latest_source_timestamp AS source_timestamp,
              latest_cycle_id AS cycle_id,
              latest_label AS label,
              latest_profile AS profile,
              underlying_symbol,
              latest_strategy AS strategy,
              option_symbol,
              latest_leg_role AS leg_role,
              latest_bid AS bid,
              latest_ask AS ask,
              latest_midpoint AS midpoint,
              latest_bid_size AS bid_size,
              latest_ask_size AS ask_size,
              latest_source AS source
            FROM
            (
              SELECT
                max(captured_at) AS latest_captured_at,
                argMax(source_timestamp, captured_at) AS latest_source_timestamp,
                argMax(cycle_id, captured_at) AS latest_cycle_id,
                argMax(label, captured_at) AS latest_label,
                argMax(profile, captured_at) AS latest_profile,
                underlying_symbol,
                argMax(strategy, captured_at) AS latest_strategy,
                option_symbol,
                argMax(leg_role, captured_at) AS latest_leg_role,
                argMax(bid, captured_at) AS latest_bid,
                argMax(ask, captured_at) AS latest_ask,
                argMax(midpoint, captured_at) AS latest_midpoint,
                argMax(bid_size, captured_at) AS latest_bid_size,
                argMax(ask_size, captured_at) AS latest_ask_size,
                argMax(source, captured_at) AS latest_source
              FROM option_quote_snapshots_1m
              WHERE {" AND ".join(clauses)}
              GROUP BY underlying_symbol, option_symbol
            )
            ORDER BY latest_captured_at DESC
            LIMIT {max(int(limit), 1)}
            """
        )

    def summarize_option_quote_window(
        self,
        *,
        option_symbols: list[str],
        captured_from: str | datetime,
        captured_to: str | datetime,
    ) -> dict[str, dict[str, Any]]:
        return self._summarize_option_window(
            table_name="option_quote_ticks",
            option_symbols=option_symbols,
            captured_from=captured_from,
            captured_to=captured_to,
        )

    def summarize_option_trade_window(
        self,
        *,
        option_symbols: list[str],
        captured_from: str | datetime,
        captured_to: str | datetime,
    ) -> dict[str, dict[str, Any]]:
        return self._summarize_option_window(
            table_name="option_trade_ticks",
            option_symbols=option_symbols,
            captured_from=captured_from,
            captured_to=captured_to,
        )

    def summarize_scoreable_trade_flow(
        self,
        *,
        label: str,
        underlyings: list[str],
        captured_from: str | datetime,
        captured_to: str | datetime,
    ) -> dict[str, dict[str, Any]]:
        symbols = _normalized_texts(underlyings)
        captured_from_dt = parse_datetime(captured_from)
        captured_to_dt = parse_datetime(captured_to)
        if not symbols or captured_from_dt is None or captured_to_dt is None or captured_from_dt >= captured_to_dt:
            return {}
        duration_minutes = max((captured_to_dt - captured_from_dt).total_seconds() / 60.0, 1.0 / 60.0)
        rows = self._query_dicts(
            f"""
            SELECT
              underlying_symbol,
              count() AS trade_count,
              uniqExact(option_symbol) AS contract_count,
              sum(premium) AS premium
            FROM option_trade_ticks
            WHERE label = {_quote_string(label)}
              AND included_in_score = 1
              AND underlying_symbol IN {_string_tuple(symbols)}
              AND captured_at >= {_quote_datetime(captured_from_dt)}
              AND captured_at < {_quote_datetime(captured_to_dt)}
            GROUP BY underlying_symbol
            """
        )
        payload: dict[str, dict[str, Any]] = {}
        for row in rows:
            symbol = str(row.get("underlying_symbol") or "").strip()
            if not symbol:
                continue
            trade_count = int(row.get("trade_count") or 0)
            contract_count = int(row.get("contract_count") or 0)
            premium = float(row.get("premium") or 0.0)
            payload[symbol] = {
                "duration_minutes": round(duration_minutes, 4),
                "scoreable_trade_count": trade_count,
                "scoreable_contract_count": contract_count,
                "scoreable_premium": round(premium, 4),
                "trade_rate_per_minute": round(trade_count / duration_minutes, 4),
                "contract_rate_per_minute": round(contract_count / duration_minutes, 4),
                "premium_rate_per_minute": round(premium / duration_minutes, 4),
            }
        return payload

    def latest_trade_session_date_before(self, *, label: str, before_session_date: str) -> str | None:
        current_session_start, _ = session_bounds(before_session_date)
        rows = self._query_dicts(
            f"""
            SELECT max(captured_at) AS latest_captured_at
            FROM option_trade_ticks
            WHERE label = {_quote_string(label)}
              AND captured_at < {_quote_datetime(current_session_start)}
            """
        )
        latest = rows[0].get("latest_captured_at") if rows else None
        if latest is None:
            return None
        latest_dt = latest if isinstance(latest, datetime) else parse_datetime(str(latest))
        if latest_dt is None:
            return None
        return latest_dt.astimezone(NEW_YORK).date().isoformat()

    def _summarize_option_window(
        self,
        *,
        table_name: str,
        option_symbols: list[str],
        captured_from: str | datetime,
        captured_to: str | datetime,
    ) -> dict[str, dict[str, Any]]:
        symbols = _normalized_texts(option_symbols)
        captured_from_dt = parse_datetime(captured_from)
        captured_to_dt = parse_datetime(captured_to)
        if not symbols or captured_from_dt is None or captured_to_dt is None or captured_from_dt >= captured_to_dt:
            return {}
        table = _identifier(table_name)
        rows = self._query_dicts(
            f"""
            SELECT
              option_symbol,
              count() AS tick_count,
              max(captured_at) AS last_captured_at
            FROM {table}
            WHERE option_symbol IN {_string_tuple(symbols)}
              AND captured_at >= {_quote_datetime(captured_from_dt)}
              AND captured_at < {_quote_datetime(captured_to_dt)}
            GROUP BY option_symbol
            """
        )
        return {
            str(row["option_symbol"]): {
                "tick_count": int(row.get("tick_count") or 0),
                "last_captured_at": render_value(row.get("last_captured_at")),
            }
            for row in rows
        }

    def _save_quote_snapshots(self, rows: list[dict[str, Any]]) -> None:
        self._insert_quote_snapshots(table_name="option_quote_snapshots_1s", rows=rows, bucket="second")
        self._insert_quote_snapshots(table_name="option_quote_snapshots_1m", rows=rows, bucket="minute")

    def _insert_quote_snapshots(self, *, table_name: str, rows: list[dict[str, Any]], bucket: str) -> None:
        latest_by_key: dict[tuple[Any, ...], dict[str, Any]] = {}
        for row in rows:
            captured_at = row["captured_at"]
            bucket_at = _bucket_datetime(captured_at, bucket=bucket)
            key = (bucket_at, row["label"], row["profile"], row["option_symbol"])
            existing = latest_by_key.get(key)
            if existing is None or row["captured_at"] >= existing["captured_at"]:
                latest_by_key[key] = {
                    **row,
                    "bucket_at": bucket_at,
                }
        if not latest_by_key:
            return
        self._client_instance().insert(
            table_name,
            [tuple(row[column] for column in QUOTE_SNAPSHOT_COLUMNS) for row in latest_by_key.values()],
            column_names=list(QUOTE_SNAPSHOT_COLUMNS),
        )

    def _client_instance(self):
        if self._client is None:
            self._client = clickhouse_connect.get_client(**self._client_kwargs(database=self.database))
        return self._client

    def _client_kwargs(self, *, database: str | None) -> dict[str, Any]:
        kwargs = {
            "host": self._connection["host"],
            "port": self._connection["port"],
            "username": self._connection["username"],
            "password": self._connection["password"],
            "secure": self._connection["secure"],
        }
        if database is not None:
            kwargs["database"] = database
        return kwargs

    def _existing_tables(self) -> set[str]:
        self.ensure_schema()
        rows = self._query_dicts(
            """
            SELECT name
            FROM system.tables
            WHERE database = {database}
              AND name IN {tables}
            """.format(
                database=_quote_string(self.database),
                tables=_string_tuple(MARKET_DATA_TABLE_NAMES),
            )
        )
        return {str(row["name"]) for row in rows}

    def _query_dicts(self, query: str) -> list[dict[str, Any]]:
        result = self._client_instance().query(query)
        columns = list(getattr(result, "column_names", []) or [])
        return [dict(zip(columns, row, strict=False)) for row in list(result.result_rows)]

    def _schema_statements(self, *, database: str) -> tuple[str, ...]:
        return (
            f"""
            CREATE TABLE IF NOT EXISTS {database}.option_quote_ticks
            (
              captured_at DateTime64(6, 'UTC'),
              source_timestamp Nullable(DateTime64(6, 'UTC')),
              cycle_id LowCardinality(String),
              label LowCardinality(String),
              profile LowCardinality(String),
              underlying_symbol LowCardinality(String),
              strategy LowCardinality(String),
              option_symbol String,
              leg_role LowCardinality(String),
              bid Float64,
              ask Float64,
              midpoint Float64,
              bid_size UInt32,
              ask_size UInt32,
              source LowCardinality(String)
            )
            ENGINE = MergeTree
            PARTITION BY toDate(captured_at)
            ORDER BY (label, profile, option_symbol, captured_at, cycle_id)
            TTL toDateTime(captured_at) + INTERVAL {RAW_QUOTE_RETENTION_DAYS} DAY DELETE
            SETTINGS ttl_only_drop_parts = 1
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {database}.option_trade_ticks
            (
              captured_at DateTime64(6, 'UTC'),
              source_timestamp Nullable(DateTime64(6, 'UTC')),
              cycle_id LowCardinality(String),
              label LowCardinality(String),
              profile LowCardinality(String),
              underlying_symbol LowCardinality(String),
              strategy LowCardinality(String),
              option_symbol String,
              leg_role LowCardinality(String),
              price Float64,
              size UInt32,
              premium Float64,
              exchange_code LowCardinality(String),
              conditions_json String,
              included_in_score UInt8,
              exclusion_reason String,
              raw_payload_json String,
              source LowCardinality(String)
            )
            ENGINE = MergeTree
            PARTITION BY toDate(captured_at)
            ORDER BY (label, underlying_symbol, option_symbol, captured_at, cycle_id)
            TTL toDateTime(captured_at) + INTERVAL {RAW_TRADE_RETENTION_DAYS} DAY DELETE
            SETTINGS ttl_only_drop_parts = 1
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {database}.option_quote_snapshots_1s
            (
              bucket_at DateTime('UTC'),
              captured_at DateTime64(6, 'UTC'),
              source_timestamp Nullable(DateTime64(6, 'UTC')),
              cycle_id LowCardinality(String),
              label LowCardinality(String),
              profile LowCardinality(String),
              underlying_symbol LowCardinality(String),
              strategy LowCardinality(String),
              option_symbol String,
              leg_role LowCardinality(String),
              bid Float64,
              ask Float64,
              midpoint Float64,
              bid_size UInt32,
              ask_size UInt32,
              source LowCardinality(String)
            )
            ENGINE = ReplacingMergeTree(captured_at)
            PARTITION BY toDate(bucket_at)
            ORDER BY (label, profile, option_symbol, bucket_at)
            TTL bucket_at + INTERVAL {QUOTE_SNAPSHOT_1S_RETENTION_DAYS} DAY DELETE
            SETTINGS ttl_only_drop_parts = 1
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {database}.option_quote_snapshots_1m
            (
              bucket_at DateTime('UTC'),
              captured_at DateTime64(6, 'UTC'),
              source_timestamp Nullable(DateTime64(6, 'UTC')),
              cycle_id LowCardinality(String),
              label LowCardinality(String),
              profile LowCardinality(String),
              underlying_symbol LowCardinality(String),
              strategy LowCardinality(String),
              option_symbol String,
              leg_role LowCardinality(String),
              bid Float64,
              ask Float64,
              midpoint Float64,
              bid_size UInt32,
              ask_size UInt32,
              source LowCardinality(String)
            )
            ENGINE = ReplacingMergeTree(captured_at)
            PARTITION BY toDate(bucket_at)
            ORDER BY (label, profile, option_symbol, bucket_at)
            TTL bucket_at + INTERVAL {QUOTE_SNAPSHOT_1M_RETENTION_DAYS} DAY DELETE
            SETTINGS ttl_only_drop_parts = 1
            """,
        )

    def _retention_statements(self, *, database: str) -> tuple[str, ...]:
        return tuple(
            f"""
            ALTER TABLE {database}.{_identifier(table.name)}
            MODIFY TTL {self._ttl_expression(table)} + INTERVAL {table.retention_days} DAY DELETE
            """
            for table in MARKET_DATA_TABLES
        )

    def _ttl_expression(self, table: MarketDataTable) -> str:
        if table.retention_column == "captured_at":
            return f"toDateTime({table.retention_column})"
        return table.retention_column


NEW_YORK = ZoneInfo("America/New_York")


def session_bounds(session_date: str) -> tuple[datetime, datetime]:
    ny_date = date.fromisoformat(session_date)
    session_start_local = datetime.combine(ny_date, time.min, tzinfo=NEW_YORK)
    session_end_local = session_start_local + timedelta(days=1)
    return session_start_local.astimezone(timezone.utc), session_end_local.astimezone(timezone.utc)


def _parse_clickhouse_url(url: str) -> dict[str, Any]:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        raise ValueError("ClickHouse URL must use http or https.")
    if not parsed.hostname:
        raise ValueError("ClickHouse URL must include a host.")
    database = parsed.path.strip("/") or "default"
    _identifier(database)
    return {
        "host": parsed.hostname,
        "port": parsed.port or (8443 if parsed.scheme == "https" else 8123),
        "username": unquote(parsed.username or "default"),
        "password": unquote(parsed.password or ""),
        "database": database,
        "secure": parsed.scheme == "https",
    }


def _identifier(value: str) -> str:
    text = str(value or "").strip()
    if not _IDENTIFIER_RE.fullmatch(text):
        raise ValueError(f"Unsafe ClickHouse identifier: {value!r}")
    return f"`{text}`"


def _quote_string(value: Any) -> str:
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def _string_tuple(values: tuple[str, ...] | list[str]) -> str:
    normalized = [str(value) for value in values]
    if not normalized:
        return "('')"
    return "(" + ", ".join(_quote_string(value) for value in normalized) + ")"


def _quote_datetime(value: datetime) -> str:
    parsed = value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return _quote_string(parsed.strftime("%Y-%m-%d %H:%M:%S.%f"))


def _redact_clickhouse_url(url: str) -> str:
    parsed = urlparse(url)
    if parsed.password is None:
        return url
    netloc = parsed.hostname or ""
    if parsed.port is not None:
        netloc = f"{netloc}:{parsed.port}"
    if parsed.username:
        netloc = f"{parsed.username}:***@{netloc}"
    return parsed._replace(netloc=netloc).geturl()


def _required_datetime(row: dict[str, Any], field_name: str) -> datetime:
    value = parse_datetime(row.get(field_name))
    if value is None:
        raise ValueError(f"Missing required datetime field {field_name!r}.")
    return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)


def _optional_datetime(row: dict[str, Any], field_name: str) -> datetime | None:
    value = parse_datetime(row.get(field_name))
    if value is None:
        return None
    return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)


def _text(value: Any, *, default: str = "") -> str:
    if value is None:
        return default
    rendered = str(value).strip()
    return rendered if rendered else default


def _float(value: Any) -> float:
    return float(value)


def _uint(value: Any) -> int:
    return max(int(value), 0)


def _json_text(value: Any, *, default: Any) -> str:
    resolved = default if value is None else value
    if isinstance(resolved, str):
        return resolved
    return json.dumps(resolved, sort_keys=True, separators=(",", ":"), default=str)


def _normalize_quote_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "captured_at": _required_datetime(row, "captured_at"),
        "source_timestamp": _optional_datetime(row, "source_timestamp"),
        "cycle_id": _text(row.get("cycle_id"), default="unknown"),
        "label": _text(row.get("label"), default="unknown"),
        "profile": _text(row.get("profile")),
        "underlying_symbol": _text(row.get("underlying_symbol")),
        "strategy": _text(row.get("strategy")),
        "option_symbol": _text(row.get("option_symbol"), default="UNKNOWN"),
        "leg_role": _text(row.get("leg_role"), default="unknown"),
        "bid": _float(row.get("bid")),
        "ask": _float(row.get("ask")),
        "midpoint": _float(row.get("midpoint")),
        "bid_size": _uint(row.get("bid_size")),
        "ask_size": _uint(row.get("ask_size")),
        "source": _text(row.get("source"), default="alpaca_websocket"),
    }


def _normalize_trade_row(row: dict[str, Any]) -> dict[str, Any]:
    conditions_value = row.get("conditions")
    if conditions_value is None:
        conditions_value = row.get("conditions_json")
    raw_payload_value = row.get("raw_payload")
    if raw_payload_value is None:
        raw_payload_value = row.get("raw_payload_json")
    return {
        "captured_at": _required_datetime(row, "captured_at"),
        "source_timestamp": _optional_datetime(row, "source_timestamp"),
        "cycle_id": _text(row.get("cycle_id"), default="unknown"),
        "label": _text(row.get("label"), default="unknown"),
        "profile": _text(row.get("profile")),
        "underlying_symbol": _text(row.get("underlying_symbol")),
        "strategy": _text(row.get("strategy")),
        "option_symbol": _text(row.get("option_symbol"), default="UNKNOWN"),
        "leg_role": _text(row.get("leg_role"), default="contract"),
        "price": _float(row.get("price")),
        "size": _uint(row.get("size")),
        "premium": _float(row.get("premium")),
        "exchange_code": _text(row.get("exchange_code")),
        "conditions_json": _json_text(conditions_value, default=[]),
        "included_in_score": 1 if bool(row.get("included_in_score")) else 0,
        "exclusion_reason": _text(row.get("exclusion_reason")),
        "raw_payload_json": _json_text(raw_payload_value, default={}),
        "source": _text(row.get("source"), default="alpaca_websocket"),
    }


def _bucket_datetime(value: datetime, *, bucket: str) -> datetime:
    normalized = value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if bucket == "minute":
        return normalized.replace(second=0, microsecond=0)
    if bucket == "second":
        return normalized.replace(microsecond=0)
    raise ValueError(f"Unsupported quote snapshot bucket: {bucket!r}")


def _normalized_texts(values: list[str]) -> list[str]:
    return sorted({text for value in values if (text := _text(value))})


def _normalized_sources(sources: str | list[str] | None) -> list[str]:
    if sources is None:
        return []
    if isinstance(sources, str):
        return [_text(sources)] if _text(sources) else []
    return _normalized_texts([str(source) for source in sources])


__all__ = [
    "ClickHouseMarketDataStore",
    "MARKET_DATA_TABLE_NAMES",
    "MARKET_DATA_TABLES",
    "QUOTE_SNAPSHOT_1M_RETENTION_DAYS",
    "QUOTE_SNAPSHOT_1S_RETENTION_DAYS",
    "RAW_QUOTE_RETENTION_DAYS",
    "RAW_TRADE_RETENTION_DAYS",
    "session_bounds",
]
