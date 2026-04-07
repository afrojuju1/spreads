from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from fastapi import FastAPI, HTTPException, Query

from spreads.domain.profiles import UNIVERSE_PRESETS
from spreads.storage import build_history_store, default_database_url

app = FastAPI(title="Spreads API", version="0.1.0")


def live_snapshot_path(label: str) -> Path:
    return ROOT / "outputs" / "live_ideas" / f"latest_{label}.json"


def analysis_report_path(session_date: str, label: str) -> Path:
    return ROOT / "outputs" / "analysis" / f"post_close_{session_date}_{label}.md"


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/universes")
def list_universes() -> dict[str, list[str]]:
    return {name: list(symbols) for name, symbols in UNIVERSE_PRESETS.items()}


@app.get("/live/{label}")
def get_live_snapshot(label: str) -> dict[str, Any]:
    path = live_snapshot_path(label)
    if not path.exists():
        raise HTTPException(status_code=404, detail="Live snapshot not found")
    return json.loads(path.read_text())


@app.get("/analysis/{session_date}/{label}")
def get_analysis_report(session_date: str, label: str) -> dict[str, Any]:
    path = analysis_report_path(session_date, label)
    if not path.exists():
        raise HTTPException(status_code=404, detail="Analysis report not found")
    return {"path": str(path), "content": path.read_text()}


@app.get("/history/runs")
def list_history_runs(
    symbol: str | None = None,
    strategy: str | None = None,
    limit: int = Query(default=25, ge=1, le=500),
    db: str | None = None,
) -> dict[str, Any]:
    store = build_history_store(db or default_database_url())
    try:
        return {
            "runs": [
                run.to_dict()
                for run in store.list_runs(
                    limit=limit,
                    symbol=symbol,
                    strategy=strategy,
                )
            ]
        }
    finally:
        store.close()
