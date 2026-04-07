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
                profile TEXT NOT NULL,
                spot_price REAL NOT NULL,
                candidate_count INTEGER NOT NULL,
                output_path TEXT,
                filters_json TEXT NOT NULL,
                setup_status TEXT,
                setup_score REAL
            );

            CREATE TABLE IF NOT EXISTS scan_candidates (
                run_id TEXT NOT NULL,
                rank INTEGER NOT NULL,
                expiration_date TEXT NOT NULL,
                short_symbol TEXT NOT NULL,
                long_symbol TEXT NOT NULL,
                short_strike REAL NOT NULL,
                long_strike REAL NOT NULL,
                breakeven REAL NOT NULL,
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
            """
        )
        self.connection.commit()

    def save_run(
        self,
        *,
        run_id: str,
        generated_at: str,
        symbol: str,
        profile: str,
        spot_price: float,
        output_path: str,
        filters: dict[str, Any],
        setup_status: str | None,
        setup_score: float | None,
        candidates: list[Any],
    ) -> None:
        cursor = self.connection.cursor()
        cursor.execute(
            """
            INSERT OR REPLACE INTO scan_runs (
                run_id,
                generated_at,
                symbol,
                profile,
                spot_price,
                candidate_count,
                output_path,
                filters_json,
                setup_status,
                setup_score
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                generated_at,
                symbol,
                profile,
                spot_price,
                len(candidates),
                output_path,
                json.dumps(filters, separators=(",", ":")),
                setup_status,
                setup_score,
            ),
        )
        cursor.execute("DELETE FROM scan_candidates WHERE run_id = ?", (run_id,))
        cursor.executemany(
            """
            INSERT INTO scan_candidates (
                run_id,
                rank,
                expiration_date,
                short_symbol,
                long_symbol,
                short_strike,
                long_strike,
                breakeven,
                quality_score,
                return_on_risk,
                short_otm_pct,
                calendar_status,
                setup_status,
                expected_move,
                short_vs_expected_move
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    run_id,
                    rank,
                    candidate.expiration_date,
                    candidate.short_symbol,
                    candidate.long_symbol,
                    candidate.short_strike,
                    candidate.long_strike,
                    candidate.breakeven,
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
        return payload

    def get_latest_run(self, symbol: str) -> dict[str, Any] | None:
        row = self.connection.execute(
            "SELECT * FROM scan_runs WHERE symbol = ? ORDER BY generated_at DESC LIMIT 1",
            (symbol,),
        ).fetchone()
        if row is None:
            return None
        payload = dict(row)
        payload["filters"] = json.loads(payload.pop("filters_json"))
        return payload

    def list_candidates(self, run_id: str) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT * FROM scan_candidates WHERE run_id = ? ORDER BY rank ASC",
            (run_id,),
        ).fetchall()
        return [dict(row) for row in rows]

    def close(self) -> None:
        self.connection.close()
