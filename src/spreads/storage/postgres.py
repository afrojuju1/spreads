from __future__ import annotations

import json
from datetime import date, datetime, timedelta
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
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT 1")

    @staticmethod
    def _normalize_value(value: Any) -> Any:
        if isinstance(value, datetime):
            rendered = value.isoformat()
            return rendered.replace("+00:00", "Z") if rendered.endswith("+00:00") else rendered
        if isinstance(value, date):
            return value.isoformat()
        return value

    @classmethod
    def _normalize_record(cls, row: dict[str, Any]) -> dict[str, Any]:
        return {key: cls._normalize_value(value) for key, value in row.items()}

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
        payload = self._normalize_record(dict(row))
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
        payload = self._normalize_record(dict(row))
        payload["filters"] = payload.pop("filters_json")
        payload["setup"] = payload.get("setup_json")
        return payload

    def list_candidates(self, run_id: str) -> list[dict[str, Any]]:
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT * FROM scan_candidates WHERE run_id = %s ORDER BY rank ASC", (run_id,))
            rows = cursor.fetchall()
        return [self._normalize_record(dict(row)) for row in rows]

    def list_runs(
        self,
        *,
        limit: int,
        symbol: str | None = None,
        strategy: str | None = None,
    ) -> list[dict[str, Any]]:
        query = "SELECT * FROM scan_runs"
        params: list[Any] = []
        clauses: list[str] = []
        if symbol:
            clauses.append("symbol = %s")
            params.append(symbol.upper())
        if strategy:
            clauses.append("strategy = %s")
            params.append(strategy)
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY generated_at DESC LIMIT %s"
        params.append(limit)
        with self.connection.cursor() as cursor:
            cursor.execute(query, params)
            rows = cursor.fetchall()
        return [self._normalize_record(dict(row)) for row in rows]

    def list_session_top_runs(
        self,
        *,
        session_date: str,
        session_label: str | None = None,
    ) -> list[dict[str, Any]]:
        session_start = date.fromisoformat(session_date)
        session_end = session_start + timedelta(days=1)
        query = """
            SELECT
                r.run_id,
                r.generated_at,
                r.symbol,
                r.strategy,
                r.profile,
                r.spot_price,
                r.candidate_count,
                r.setup_status,
                r.setup_score,
                r.setup_json,
                c.short_symbol,
                c.long_symbol,
                c.short_strike,
                c.long_strike,
                c.midpoint_credit,
                c.quality_score,
                c.calendar_status,
                c.expected_move,
                c.short_vs_expected_move
            FROM scan_runs r
            LEFT JOIN scan_candidates c
                ON c.run_id = r.run_id AND c.rank = 1
            WHERE r.generated_at >= %s
              AND r.generated_at < %s
        """
        params: list[Any] = [session_start.isoformat(), session_end.isoformat()]
        if session_label:
            query += " AND r.session_label = %s"
            params.append(session_label)
        query += " ORDER BY r.generated_at ASC"
        with self.connection.cursor() as cursor:
            cursor.execute(query, params)
            rows = cursor.fetchall()
        return [self._normalize_record(dict(row)) for row in rows]

    def list_session_quote_events(
        self,
        *,
        session_date: str,
        label: str,
    ) -> list[dict[str, Any]]:
        session_start = date.fromisoformat(session_date)
        session_end = session_start + timedelta(days=1)
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT *
                FROM option_quote_events
                WHERE captured_at >= %s
                  AND captured_at < %s
                  AND label = %s
                ORDER BY quote_id ASC
                """,
                (session_start.isoformat(), session_end.isoformat(), label),
            )
            rows = cursor.fetchall()
        return [self._normalize_record(dict(row)) for row in rows]

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
