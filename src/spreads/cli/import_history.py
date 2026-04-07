from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from spreads.storage import default_history_target
from spreads.storage.models import OptionQuoteEventModel, ScanCandidateModel, ScanRunModel
from spreads.storage.postgres import PostgresRunHistoryStore

DEFAULT_SQLITE_IMPORT_SOURCE = Path("outputs") / "run_history" / "scanner_history.sqlite"
RUN_BATCH_SIZE = 500
CANDIDATE_BATCH_SIZE = 1000
QUOTE_BATCH_SIZE = 5000


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import historical scanner data from SQLite into the Postgres backend."
    )
    parser.add_argument(
        "--source-sqlite",
        default=str(DEFAULT_SQLITE_IMPORT_SOURCE),
        help="Source SQLite database path. Default: outputs/run_history/scanner_history.sqlite",
    )
    parser.add_argument(
        "--target-db",
        default=default_history_target(),
        help=(
            "Target PostgreSQL URL. Defaults to "
            "SPREADS_DATABASE_URL / DATABASE_URL / the local Docker Postgres URL."
        ),
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="Delete existing target rows before importing.",
    )
    return parser.parse_args()


def chunked(rows: list[Any], size: int) -> list[list[Any]]:
    return [rows[index : index + size] for index in range(0, len(rows), size)]


def parse_datetime(value: str | None) -> datetime | None:
    if value is None:
        return None
    normalized = value.replace("Z", "+00:00") if value.endswith("Z") else value
    parsed = datetime.fromisoformat(normalized)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def parse_date(value: str | None) -> date | None:
    return None if value is None else date.fromisoformat(value)


def count_sqlite_rows(connection: sqlite3.Connection, table: str) -> int:
    return int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def fetch_sqlite_rows(connection: sqlite3.Connection, query: str) -> list[dict[str, Any]]:
    rows = connection.execute(query).fetchall()
    return [dict(row) for row in rows]


def ensure_target_is_postgres(target_db: str) -> None:
    if not (
        target_db.startswith("postgresql://")
        or target_db.startswith("postgres://")
        or target_db.startswith("postgresql+psycopg://")
    ):
        raise SystemExit("Target DB must be PostgreSQL. Set --target-db to a PostgreSQL URL.")


def ensure_target_schema_ready(store: PostgresRunHistoryStore) -> None:
    if not store.schema_ready():
        raise SystemExit(
            "Target schema is not ready. Run `uv run alembic upgrade head` first."
        )


def ensure_target_empty(store: PostgresRunHistoryStore) -> None:
    counts = store.table_counts()
    if any(counts.values()):
        detail = ", ".join(f"{table}={count}" for table, count in counts.items())
        raise SystemExit(
            f"Target Postgres database is not empty ({detail}). "
            "Re-run with --truncate if you want to replace its contents."
        )


def import_runs(store: PostgresRunHistoryStore, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0

    imported = 0
    for batch in chunked(rows, RUN_BATCH_SIZE):
        with store.session_scope() as session:
            session.add_all(
                [
                    ScanRunModel(
                        run_id=row["run_id"],
                        generated_at=parse_datetime(row["generated_at"]),
                        symbol=row["symbol"],
                        strategy=row["strategy"],
                        session_label=row.get("session_label"),
                        profile=row["profile"],
                        spot_price=row["spot_price"],
                        candidate_count=row["candidate_count"],
                        output_path=row.get("output_path"),
                        filters_json=json.loads(row["filters_json"]),
                        setup_status=row.get("setup_status"),
                        setup_score=row.get("setup_score"),
                        setup_json=(
                            None
                            if not row.get("setup_json")
                            else json.loads(row["setup_json"])
                        ),
                    )
                    for row in batch
                ]
            )
        imported += len(batch)
    return imported


def import_candidates(store: PostgresRunHistoryStore, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0

    imported = 0
    for batch in chunked(rows, CANDIDATE_BATCH_SIZE):
        with store.session_scope() as session:
            session.add_all(
                [
                    ScanCandidateModel(
                        run_id=row["run_id"],
                        rank=row["rank"],
                        strategy=row["strategy"],
                        expiration_date=parse_date(row["expiration_date"]),
                        short_symbol=row["short_symbol"],
                        long_symbol=row["long_symbol"],
                        short_strike=row["short_strike"],
                        long_strike=row["long_strike"],
                        width=row["width"],
                        midpoint_credit=row["midpoint_credit"],
                        natural_credit=row["natural_credit"],
                        breakeven=row["breakeven"],
                        max_profit=row["max_profit"],
                        max_loss=row["max_loss"],
                        quality_score=row["quality_score"],
                        return_on_risk=row["return_on_risk"],
                        short_otm_pct=row["short_otm_pct"],
                        calendar_status=row.get("calendar_status"),
                        setup_status=row.get("setup_status"),
                        expected_move=row.get("expected_move"),
                        short_vs_expected_move=row.get("short_vs_expected_move"),
                    )
                    for row in batch
                ]
            )
        imported += len(batch)
    return imported


def import_quotes(store: PostgresRunHistoryStore, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0

    imported = 0
    for batch in chunked(rows, QUOTE_BATCH_SIZE):
        with store.session_scope() as session:
            session.add_all(
                [
                    OptionQuoteEventModel(
                        cycle_id=row["cycle_id"],
                        captured_at=parse_datetime(row["captured_at"]),
                        label=row["label"],
                        underlying_symbol=row.get("underlying_symbol"),
                        strategy=row.get("strategy"),
                        profile=row.get("profile"),
                        option_symbol=row["option_symbol"],
                        leg_role=row["leg_role"],
                        bid=row["bid"],
                        ask=row["ask"],
                        midpoint=row["midpoint"],
                        bid_size=row["bid_size"],
                        ask_size=row["ask_size"],
                        quote_timestamp=parse_datetime(row.get("quote_timestamp")),
                        source=row.get("source", "alpaca_websocket"),
                    )
                    for row in batch
                ]
            )
        imported += len(batch)
    return imported


def main() -> None:
    args = parse_args()
    source_path = Path(args.source_sqlite)
    if not source_path.exists():
        raise SystemExit(f"SQLite source file does not exist: {source_path}")

    ensure_target_is_postgres(args.target_db)

    sqlite_conn = sqlite3.connect(source_path)
    sqlite_conn.row_factory = sqlite3.Row
    try:
        source_counts = {
            table: count_sqlite_rows(sqlite_conn, table)
            for table in ("scan_runs", "scan_candidates", "option_quote_events")
        }
        run_rows = fetch_sqlite_rows(
            sqlite_conn,
            "SELECT * FROM scan_runs ORDER BY generated_at ASC, run_id ASC",
        )
        candidate_rows = fetch_sqlite_rows(
            sqlite_conn,
            "SELECT * FROM scan_candidates ORDER BY run_id ASC, rank ASC",
        )
        quote_rows = fetch_sqlite_rows(
            sqlite_conn,
            "SELECT * FROM option_quote_events ORDER BY quote_id ASC",
        )
    finally:
        sqlite_conn.close()

    target_store = PostgresRunHistoryStore(args.target_db)
    try:
        ensure_target_schema_ready(target_store)
        if args.truncate:
            target_store.truncate_all()
        else:
            ensure_target_empty(target_store)

        imported_runs = import_runs(target_store, run_rows)
        imported_candidates = import_candidates(target_store, candidate_rows)
        imported_quotes = import_quotes(target_store, quote_rows)
        target_counts = target_store.table_counts()
    finally:
        target_store.close()

    print(f"Source SQLite: {source_path}")
    print(f"Target Postgres: {args.target_db}")
    print(
        "Imported rows: "
        f"runs={imported_runs}, candidates={imported_candidates}, quotes={imported_quotes}"
    )
    print(
        "Source counts: "
        f"scan_runs={source_counts['scan_runs']}, "
        f"scan_candidates={source_counts['scan_candidates']}, "
        f"option_quote_events={source_counts['option_quote_events']}"
    )
    print(
        "Target counts: "
        f"scan_runs={target_counts['scan_runs']}, "
        f"scan_candidates={target_counts['scan_candidates']}, "
        f"option_quote_events={target_counts['option_quote_events']}"
    )


if __name__ == "__main__":
    main()
