from __future__ import annotations

from typing import Any

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from core.cli.ops_render_helpers import (
    STATUS_STYLES,
    _render_attention,
    _render_bytes,
    _render_value,
    _status_text,
)


def render_storage_ops_state(console: Console, payload: dict[str, Any]) -> None:
    summary = dict(payload.get("summary") or {})
    details = dict(payload.get("details") or {})
    maintenance = dict(details.get("maintenance") or {})

    overview = Table.grid(padding=(0, 2))
    overview.add_row("Overall", _status_text(payload.get("status")))
    overview.add_row("Generated", _render_value(payload.get("generated_at")))
    overview.add_row("ClickHouse", "ready" if summary.get("market_data_tables_ready") else "review")
    overview.add_row("Database", _render_value(summary.get("market_data_database")))
    overview.add_row("Retention", _render_value(summary.get("retention_owner")))
    overview.add_row(
        "Latest Capture",
        (
            f"{_render_value(summary.get('latest_capture_status'))} @ "
            f"{_render_value(summary.get('latest_captured_at'))} | "
            f"quotes {_render_value(summary.get('latest_quote_rows_saved'))} | "
            f"trades {_render_value(summary.get('latest_trade_rows_saved'))}"
        ),
    )
    overview.add_row(
        "Storage",
        (
            f"{_render_bytes(summary.get('storage_total_size_bytes'))} | "
            f"live rows {_render_value(summary.get('storage_estimated_live_rows'))} | "
            f"dead rows {_render_value(summary.get('storage_estimated_dead_rows'))}"
        ),
    )
    overview.add_row("Schedule", _render_value(summary.get("schedule")))
    overview.add_row("Market Hours Safe", "yes" if summary.get("market_hours_safe") else "no")
    console.print(
        Panel(
            overview,
            title="Storage Ops State",
            border_style=STATUS_STYLES.get(str(payload.get("status")), "white"),
        )
    )

    _render_attention(console, payload)

    table_rows = list(details.get("tables") or [])
    if table_rows:
        table = Table(title="Market Data Storage", header_style="bold")
        table.add_column("Name")
        table.add_column("Class")
        table.add_column("Engine")
        table.add_column("Parts", justify="right")
        table.add_column("Rows Est.", justify="right")
        table.add_column("Size", justify="right")
        table.add_column("Retention", justify="right")
        table.add_column("Newest")
        for row in table_rows:
            retention_days = row.get("retention_days")
            active_parts = row.get("active_part_count")
            table.add_row(
                _render_value(row.get("name")),
                _render_value(row.get("data_class")),
                _render_value(row.get("engine")),
                _render_value(active_parts),
                _render_value(row.get("estimated_live_rows")),
                _render_bytes(row.get("total_size_bytes")),
                "-" if retention_days is None else f"{_render_value(retention_days)}d",
                _render_value(row.get("newest_value") or row.get("latest_captured_at")),
            )
        console.print(table)

    if maintenance:
        console.print(
            Panel(
                _render_value(maintenance.get("lock_profile")),
                title="Storage Maintenance",
            )
        )


__all__ = ["render_storage_ops_state"]
