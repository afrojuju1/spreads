from __future__ import annotations

import json
from typing import Any


class PostgresRunHistoryStore:
    def __init__(self, database_url: str) -> None:
        try:
            import psycopg
            from psycopg.rows import dict_row
        except ImportError as exc:
            raise RuntimeError(
                "psycopg is required for PostgreSQL storage. Install project dependencies first."
            ) from exc

        self.path = database_url
        self.connection = psycopg.connect(database_url, row_factory=dict_row)
        self._initialize_schema()

    def _initialize_schema(self) -> None:
        cursor = self.connection.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS scan_runs (
                run_id TEXT PRIMARY KEY,
                generated_at TIMESTAMPTZ NOT NULL,
                symbol TEXT NOT NULL,
                strategy TEXT NOT NULL DEFAULT 'call_credit',
                session_label TEXT,
                profile TEXT NOT NULL,
                spot_price DOUBLE PRECISION NOT NULL,
                candidate_count INTEGER NOT NULL,
                output_path TEXT,
                filters_json JSONB NOT NULL,
                setup_status TEXT,
                setup_score DOUBLE PRECISION,
                setup_json JSONB
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS scan_candidates (
                run_id TEXT NOT NULL REFERENCES scan_runs(run_id) ON DELETE CASCADE,
                rank INTEGER NOT NULL,
                strategy TEXT NOT NULL DEFAULT 'call_credit',
                expiration_date DATE NOT NULL,
                short_symbol TEXT NOT NULL,
                long_symbol TEXT NOT NULL,
                short_strike DOUBLE PRECISION NOT NULL,
                long_strike DOUBLE PRECISION NOT NULL,
                width DOUBLE PRECISION NOT NULL DEFAULT 0,
                midpoint_credit DOUBLE PRECISION NOT NULL DEFAULT 0,
                natural_credit DOUBLE PRECISION NOT NULL DEFAULT 0,
                breakeven DOUBLE PRECISION NOT NULL,
                max_profit DOUBLE PRECISION NOT NULL DEFAULT 0,
                max_loss DOUBLE PRECISION NOT NULL DEFAULT 0,
                quality_score DOUBLE PRECISION NOT NULL,
                return_on_risk DOUBLE PRECISION NOT NULL,
                short_otm_pct DOUBLE PRECISION NOT NULL,
                calendar_status TEXT,
                setup_status TEXT,
                expected_move DOUBLE PRECISION,
                short_vs_expected_move DOUBLE PRECISION,
                PRIMARY KEY (run_id, rank)
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS option_quote_events (
                quote_id BIGSERIAL PRIMARY KEY,
                cycle_id TEXT NOT NULL,
                captured_at TIMESTAMPTZ NOT NULL,
                label TEXT NOT NULL,
                underlying_symbol TEXT,
                strategy TEXT,
                profile TEXT,
                option_symbol TEXT NOT NULL,
                leg_role TEXT NOT NULL,
                bid DOUBLE PRECISION NOT NULL,
                ask DOUBLE PRECISION NOT NULL,
                midpoint DOUBLE PRECISION NOT NULL,
                bid_size INTEGER NOT NULL,
                ask_size INTEGER NOT NULL,
                quote_timestamp TIMESTAMPTZ,
                source TEXT NOT NULL DEFAULT 'alpaca_websocket'
            )
            """
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_scan_runs_symbol_generated_at ON scan_runs(symbol, generated_at DESC)"
        )
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_scan_candidates_run_id ON scan_candidates(run_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_option_quote_events_cycle_id ON option_quote_events(cycle_id)")
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_option_quote_events_symbol_captured_at ON option_quote_events(option_symbol, captured_at DESC)"
        )
        self.connection.commit()

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
            INSERT INTO scan_runs (
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
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s::jsonb)
            ON CONFLICT (run_id) DO UPDATE SET
                generated_at = EXCLUDED.generated_at,
                symbol = EXCLUDED.symbol,
                strategy = EXCLUDED.strategy,
                session_label = EXCLUDED.session_label,
                profile = EXCLUDED.profile,
                spot_price = EXCLUDED.spot_price,
                candidate_count = EXCLUDED.candidate_count,
                output_path = EXCLUDED.output_path,
                filters_json = EXCLUDED.filters_json,
                setup_status = EXCLUDED.setup_status,
                setup_score = EXCLUDED.setup_score,
                setup_json = EXCLUDED.setup_json
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
        cursor.execute("DELETE FROM scan_candidates WHERE run_id = %s", (run_id,))
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
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT * FROM scan_runs WHERE run_id = %s", (run_id,))
            row = cursor.fetchone()
        if row is None:
            return None
        payload = dict(row)
        payload["filters"] = payload.pop("filters_json")
        payload["setup"] = payload.get("setup_json")
        return payload

    def get_latest_run(self, symbol: str, strategy: str | None = None) -> dict[str, Any] | None:
        with self.connection.cursor() as cursor:
            if strategy is None:
                cursor.execute(
                    "SELECT * FROM scan_runs WHERE symbol = %s ORDER BY generated_at DESC LIMIT 1",
                    (symbol,),
                )
            else:
                cursor.execute(
                    """
                    SELECT * FROM scan_runs
                    WHERE symbol = %s AND strategy = %s
                    ORDER BY generated_at DESC
                    LIMIT 1
                    """,
                    (symbol, strategy),
                )
            row = cursor.fetchone()
        if row is None:
            return None
        payload = dict(row)
        payload["filters"] = payload.pop("filters_json")
        payload["setup"] = payload.get("setup_json")
        return payload

    def list_candidates(self, run_id: str) -> list[dict[str, Any]]:
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT * FROM scan_candidates WHERE run_id = %s ORDER BY rank ASC", (run_id,))
            rows = cursor.fetchall()
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
        with self.connection.cursor() as cursor:
            cursor.executemany(
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
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
