from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol


class HistoryStore(Protocol):
    path: str | Path

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
    ) -> None: ...

    def get_run(self, run_id: str) -> dict[str, Any] | None: ...

    def get_latest_run(self, symbol: str, strategy: str | None = None) -> dict[str, Any] | None: ...

    def list_candidates(self, run_id: str) -> list[dict[str, Any]]: ...

    def save_option_quote_events(
        self,
        *,
        cycle_id: str,
        label: str,
        profile: str,
        quotes: list[dict[str, Any]],
    ) -> int: ...

    def close(self) -> None: ...
