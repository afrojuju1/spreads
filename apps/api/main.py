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
from spreads.storage.factory import build_history_store
from spreads.storage.history import DEFAULT_HISTORY_DB_PATH

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
    db: str = str(DEFAULT_HISTORY_DB_PATH),
) -> dict[str, Any]:
    store = build_history_store(db)
    try:
        connection = getattr(store, "connection", None)
        if connection is None:
            raise HTTPException(status_code=500, detail="History backend does not expose query access")
        if str(db).startswith("postgres://") or str(db).startswith("postgresql://"):
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
            with connection.cursor() as cursor:
                cursor.execute(query, params)
                rows = cursor.fetchall()
            return {"runs": [dict(row) for row in rows]}
        query = "SELECT * FROM scan_runs"
        params = []
        clauses = []
        if symbol:
            clauses.append("symbol = ?")
            params.append(symbol.upper())
        if strategy:
            clauses.append("strategy = ?")
            params.append(strategy)
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY generated_at DESC LIMIT ?"
        params.append(limit)
        rows = connection.execute(query, params).fetchall()
        return {"runs": [dict(row) for row in rows]}
    finally:
        store.close()
