from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any


DEFAULT_HISTORY_DB_PATH = Path("outputs") / "run_history" / "scanner_history.sqlite"


class RunHistoryStore:
    def __init__(self, path: str | Path = DEFAULT_HISTORY_DB_PATH) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.path)
        self.connection.row_factory = sqlite3.Row
        self._initialize_schema()

    def _initialize_schema(self) -> None:
        cursor = self.connection.cursor()
        cursor.executescript(
            """
            CREATE TABLE IF NOT EXISTS scan_runs (
                run_id TEXT PRIMARY KEY,
                generated_at TEXT NOT NULL,
                symbol TEXT NOT NULL,
                strategy TEXT NOT NULL DEFAULT 'call_credit',
                session_label TEXT,
                profile TEXT NOT NULL,
                spot_price REAL NOT NULL,
                candidate_count INTEGER NOT NULL,
                output_path TEXT,
                filters_json TEXT NOT NULL,
                setup_status TEXT,
                setup_score REAL,
                setup_json TEXT
            );

            CREATE TABLE IF NOT EXISTS scan_candidates (
                run_id TEXT NOT NULL,
                rank INTEGER NOT NULL,
                strategy TEXT NOT NULL DEFAULT 'call_credit',
                expiration_date TEXT NOT NULL,
                short_symbol TEXT NOT NULL,
                long_symbol TEXT NOT NULL,
                short_strike REAL NOT NULL,
                long_strike REAL NOT NULL,
                width REAL NOT NULL DEFAULT 0,
                midpoint_credit REAL NOT NULL DEFAULT 0,
                natural_credit REAL NOT NULL DEFAULT 0,
                breakeven REAL NOT NULL,
                max_profit REAL NOT NULL DEFAULT 0,
                max_loss REAL NOT NULL DEFAULT 0,
                quality_score REAL NOT NULL,
                return_on_risk REAL NOT NULL,
                short_otm_pct REAL NOT NULL,
                calendar_status TEXT,
                setup_status TEXT,
                expected_move REAL,
                short_vs_expected_move REAL,
                PRIMARY KEY (run_id, rank),
                FOREIGN KEY (run_id) REFERENCES scan_runs(run_id)
            );

            CREATE INDEX IF NOT EXISTS idx_scan_runs_symbol_generated_at
            ON scan_runs(symbol, generated_at DESC);

            CREATE INDEX IF NOT EXISTS idx_scan_candidates_run_id
            ON scan_candidates(run_id);

            CREATE TABLE IF NOT EXISTS option_quote_events (
                quote_id INTEGER PRIMARY KEY AUTOINCREMENT,
                cycle_id TEXT NOT NULL,
                captured_at TEXT NOT NULL,
                label TEXT NOT NULL,
                underlying_symbol TEXT,
                strategy TEXT,
                profile TEXT,
                option_symbol TEXT NOT NULL,
                leg_role TEXT NOT NULL,
                bid REAL NOT NULL,
                ask REAL NOT NULL,
                midpoint REAL NOT NULL,
                bid_size INTEGER NOT NULL,
                ask_size INTEGER NOT NULL,
                quote_timestamp TEXT,
                source TEXT NOT NULL DEFAULT 'alpaca_websocket'
            );

            CREATE INDEX IF NOT EXISTS idx_option_quote_events_cycle_id
            ON option_quote_events(cycle_id);

            CREATE INDEX IF NOT EXISTS idx_option_quote_events_symbol_captured_at
            ON option_quote_events(option_symbol, captured_at DESC);
            """
        )
        self._ensure_run_columns(
            {
                "strategy": "TEXT NOT NULL DEFAULT 'call_credit'",
                "session_label": "TEXT",
                "setup_json": "TEXT",
            }
        )
        self._ensure_candidate_columns(
            {
                "strategy": "TEXT NOT NULL DEFAULT 'call_credit'",
                "width": "REAL NOT NULL DEFAULT 0",
                "midpoint_credit": "REAL NOT NULL DEFAULT 0",
                "natural_credit": "REAL NOT NULL DEFAULT 0",
                "max_profit": "REAL NOT NULL DEFAULT 0",
                "max_loss": "REAL NOT NULL DEFAULT 0",
            }
        )
        self.connection.commit()

    def _ensure_run_columns(self, columns: dict[str, str]) -> None:
        existing = {
            row["name"]
            for row in self.connection.execute("PRAGMA table_info(scan_runs)").fetchall()
        }
        for column, definition in columns.items():
            if column in existing:
                continue
            try:
                self.connection.execute(
                    f"ALTER TABLE scan_runs ADD COLUMN {column} {definition}"
                )
            except sqlite3.OperationalError as exc:
                if "duplicate column name" not in str(exc).lower():
                    raise

    def _ensure_candidate_columns(self, columns: dict[str, str]) -> None:
        existing = {
            row["name"]
            for row in self.connection.execute("PRAGMA table_info(scan_candidates)").fetchall()
        }
        for column, definition in columns.items():
            if column in existing:
                continue
            try:
                self.connection.execute(
                    f"ALTER TABLE scan_candidates ADD COLUMN {column} {definition}"
                )
            except sqlite3.OperationalError as exc:
                if "duplicate column name" not in str(exc).lower():
                    raise

    def save_run(
        self,
        *,
        run_id: str,
        generated_at: str,
        symbol: str,
        strategy: str,
        session_label: str | None,
        profile: str,
        spot_price: float,
        output_path: str,
        filters: dict[str, Any],
        setup_status: str | None,
        setup_score: float | None,
        setup_payload: dict[str, Any] | None,
        candidates: list[Any],
    ) -> None:
        cursor = self.connection.cursor()
        cursor.execute(
            """
            INSERT OR REPLACE INTO scan_runs (
                run_id,
                generated_at,
                symbol,
                strategy,
                session_label,
                profile,
                spot_price,
                candidate_count,
                output_path,
                filters_json,
                setup_status,
                setup_score,
                setup_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                generated_at,
                symbol,
                strategy,
                session_label,
                profile,
                spot_price,
                len(candidates),
                output_path,
                json.dumps(filters, separators=(",", ":")),
                setup_status,
                setup_score,
                None if setup_payload is None else json.dumps(setup_payload, separators=(",", ":")),
            ),
        )
        cursor.execute("DELETE FROM scan_candidates WHERE run_id = ?", (run_id,))
        cursor.executemany(
            """
            INSERT INTO scan_candidates (
                run_id,
                rank,
                strategy,
                expiration_date,
                short_symbol,
                long_symbol,
                short_strike,
                long_strike,
                width,
                midpoint_credit,
                natural_credit,
                breakeven,
                max_profit,
                max_loss,
                quality_score,
                return_on_risk,
                short_otm_pct,
                calendar_status,
                setup_status,
                expected_move,
                short_vs_expected_move
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    run_id,
                    rank,
                    candidate.strategy,
                    candidate.expiration_date,
                    candidate.short_symbol,
                    candidate.long_symbol,
                    candidate.short_strike,
                    candidate.long_strike,
                    candidate.width,
                    candidate.midpoint_credit,
                    candidate.natural_credit,
                    candidate.breakeven,
                    candidate.max_profit,
                    candidate.max_loss,
                    candidate.quality_score,
                    candidate.return_on_risk,
                    candidate.short_otm_pct,
                    candidate.calendar_status,
                    getattr(candidate, "setup_status", None),
                    candidate.expected_move,
                    candidate.short_vs_expected_move,
                )
                for rank, candidate in enumerate(candidates, start=1)
            ],
        )
        self.connection.commit()

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        row = self.connection.execute(
            "SELECT * FROM scan_runs WHERE run_id = ?",
            (run_id,),
        ).fetchone()
        if row is None:
            return None
        payload = dict(row)
        payload["filters"] = json.loads(payload.pop("filters_json"))
        setup_json = payload.get("setup_json")
        payload["setup"] = None if not setup_json else json.loads(setup_json)
        return payload

    def get_latest_run(self, symbol: str, strategy: str | None = None) -> dict[str, Any] | None:
        if strategy is None:
            row = self.connection.execute(
                "SELECT * FROM scan_runs WHERE symbol = ? ORDER BY generated_at DESC LIMIT 1",
                (symbol,),
            ).fetchone()
        else:
            row = self.connection.execute(
                """
                SELECT *
                FROM scan_runs
                WHERE symbol = ? AND strategy = ?
                ORDER BY generated_at DESC
                LIMIT 1
                """,
                (symbol, strategy),
            ).fetchone()
        if row is None:
            return None
        payload = dict(row)
        payload["filters"] = json.loads(payload.pop("filters_json"))
        setup_json = payload.get("setup_json")
        payload["setup"] = None if not setup_json else json.loads(setup_json)
        return payload

    def list_candidates(self, run_id: str) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT * FROM scan_candidates WHERE run_id = ? ORDER BY rank ASC",
            (run_id,),
        ).fetchall()
        return [dict(row) for row in rows]

    def save_option_quote_events(
        self,
        *,
        cycle_id: str,
        label: str,
        profile: str,
        quotes: list[dict[str, Any]],
    ) -> int:
        if not quotes:
            return 0

        self.connection.executemany(
            """
            INSERT INTO option_quote_events (
                cycle_id,
                captured_at,
                label,
                underlying_symbol,
                strategy,
                profile,
                option_symbol,
                leg_role,
                bid,
                ask,
                midpoint,
                bid_size,
                ask_size,
                quote_timestamp,
                source
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    cycle_id,
                    quote["captured_at"],
                    label,
                    quote.get("underlying_symbol"),
                    quote.get("strategy"),
                    profile,
                    quote["option_symbol"],
                    quote["leg_role"],
                    quote["bid"],
                    quote["ask"],
                    quote["midpoint"],
                    quote["bid_size"],
                    quote["ask_size"],
                    quote.get("quote_timestamp"),
                    quote.get("source", "alpaca_websocket"),
                )
                for quote in quotes
            ],
        )
        self.connection.commit()
        return len(quotes)

    def close(self) -> None:
        self.connection.close()
