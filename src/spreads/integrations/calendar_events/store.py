from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

from .models import CalendarEventRecord


class CalendarEventStore:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.db_path)
        self.connection.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS calendar_events (
                event_id TEXT PRIMARY KEY,
                event_type TEXT NOT NULL,
                symbol TEXT,
                asset_scope TEXT,
                scheduled_at TEXT NOT NULL,
                window_start TEXT NOT NULL,
                window_end TEXT NOT NULL,
                source TEXT NOT NULL,
                source_confidence TEXT NOT NULL,
                status TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                ingested_at TEXT NOT NULL,
                source_updated_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_calendar_events_symbol
            ON calendar_events(symbol, scheduled_at);

            CREATE INDEX IF NOT EXISTS idx_calendar_events_asset_scope
            ON calendar_events(asset_scope, scheduled_at);

            CREATE TABLE IF NOT EXISTS calendar_event_refresh_state (
                source TEXT NOT NULL,
                scope_key TEXT NOT NULL,
                coverage_start TEXT NOT NULL,
                coverage_end TEXT NOT NULL,
                refreshed_at TEXT NOT NULL,
                PRIMARY KEY (source, scope_key)
            );
            """
        )
        self.connection.commit()

    def close(self) -> None:
        self.connection.close()

    def upsert_events(self, records: list[CalendarEventRecord]) -> None:
        if not records:
            return
        self.connection.executemany(
            """
            INSERT INTO calendar_events (
                event_id, event_type, symbol, asset_scope, scheduled_at,
                window_start, window_end, source, source_confidence, status,
                payload_json, ingested_at, source_updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(event_id) DO UPDATE SET
                event_type=excluded.event_type,
                symbol=excluded.symbol,
                asset_scope=excluded.asset_scope,
                scheduled_at=excluded.scheduled_at,
                window_start=excluded.window_start,
                window_end=excluded.window_end,
                source=excluded.source,
                source_confidence=excluded.source_confidence,
                status=excluded.status,
                payload_json=excluded.payload_json,
                ingested_at=excluded.ingested_at,
                source_updated_at=excluded.source_updated_at
            """,
            [
                (
                    record.event_id,
                    record.event_type,
                    record.symbol,
                    record.asset_scope,
                    record.scheduled_at,
                    record.window_start,
                    record.window_end,
                    record.source,
                    record.source_confidence,
                    record.status,
                    record.payload_json,
                    record.ingested_at,
                    record.source_updated_at,
                )
                for record in records
            ],
        )
        self.connection.commit()

    def set_refresh_state(
        self,
        *,
        source: str,
        scope_key: str,
        coverage_start: str,
        coverage_end: str,
        refreshed_at: str,
    ) -> None:
        self.connection.execute(
            """
            INSERT INTO calendar_event_refresh_state (
                source, scope_key, coverage_start, coverage_end, refreshed_at
            )
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(source, scope_key) DO UPDATE SET
                coverage_start=excluded.coverage_start,
                coverage_end=excluded.coverage_end,
                refreshed_at=excluded.refreshed_at
            """,
            (source, scope_key, coverage_start, coverage_end, refreshed_at),
        )
        self.connection.commit()

    def get_refresh_state(self, *, source: str, scope_key: str) -> sqlite3.Row | None:
        row = self.connection.execute(
            """
            SELECT source, scope_key, coverage_start, coverage_end, refreshed_at
            FROM calendar_event_refresh_state
            WHERE source = ? AND scope_key = ?
            """,
            (source, scope_key),
        ).fetchone()
        return row

    def has_fresh_coverage(
        self,
        *,
        source: str,
        scope_key: str,
        coverage_start: str,
        coverage_end: str,
        freshness_hours: int,
    ) -> bool:
        row = self.get_refresh_state(source=source, scope_key=scope_key)
        if row is None:
            return False
        if row["coverage_start"] > coverage_start or row["coverage_end"] < coverage_end:
            return False
        if freshness_hours <= 0:
            return True
        refreshed_at = datetime.fromisoformat(row["refreshed_at"])
        return refreshed_at >= datetime.now(UTC) - timedelta(hours=freshness_hours)

    def query_events(
        self,
        *,
        symbol: str,
        asset_scope: str | None,
        window_start: str,
        window_end: str,
    ) -> list[CalendarEventRecord]:
        params: list[str] = [window_start, window_end, symbol]
        query = """
            SELECT
                event_id, event_type, symbol, asset_scope, scheduled_at,
                window_start, window_end, source, source_confidence, status,
                payload_json, ingested_at, source_updated_at
            FROM calendar_events
            WHERE scheduled_at >= ?
              AND scheduled_at <= ?
              AND (
                    symbol = ?
        """
        if asset_scope:
            query += " OR asset_scope = ?"
            params.append(asset_scope)
        query += """
              )
            ORDER BY scheduled_at ASC
        """
        rows = self.connection.execute(query, params).fetchall()
        return [
            CalendarEventRecord(
                event_id=row["event_id"],
                event_type=row["event_type"],
                symbol=row["symbol"],
                asset_scope=row["asset_scope"],
                scheduled_at=row["scheduled_at"],
                window_start=row["window_start"],
                window_end=row["window_end"],
                source=row["source"],
                source_confidence=row["source_confidence"],
                status=row["status"],
                payload_json=row["payload_json"],
                ingested_at=row["ingested_at"],
                source_updated_at=row["source_updated_at"],
            )
            for row in rows
        ]
