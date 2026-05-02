from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from core.services.market_intel.contracts import MarketIntelRun


class MarketIntelArtifactStore:
    def __init__(self, output_root: Path) -> None:
        self.output_root = output_root.expanduser()

    def run_dir(self, *, ticker: str, as_of: str, run_id: str) -> Path:
        safe_run_id = run_id.replace(":", "_").replace("/", "_")
        return self.output_root / ticker.upper() / as_of / f"run_{safe_run_id}"

    def create_run_tree(self, run: MarketIntelRun) -> None:
        run.run_dir.mkdir(parents=True, exist_ok=True)
        for source_type in ("sec", "ir", "market", "news", "calendar", "valuation_context"):
            (run.run_dir / "raw" / source_type).mkdir(parents=True, exist_ok=True)
            (run.run_dir / "normalized" / source_type).mkdir(parents=True, exist_ok=True)
        for trace_name in ("logs.jsonl", "agent_trace.jsonl", "hooks.jsonl", "model_calls.jsonl"):
            (run.run_dir / trace_name).touch()

    def write_run(self, run: MarketIntelRun) -> Path:
        return self.write_json(run.run_dir / "run.json", run.to_payload())

    def write_json(self, path: Path, payload: Any) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n")
        return path

    def write_text(self, path: Path, content: str) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
        return path

    def append_log(self, run: MarketIntelRun, event: str, payload: dict[str, Any]) -> None:
        self.append_jsonl(run.run_dir / "logs.jsonl", {"event": event, **payload})

    def append_agent_trace(
        self,
        run: MarketIntelRun,
        event: str,
        payload: dict[str, Any],
    ) -> None:
        self.append_jsonl(run.run_dir / "agent_trace.jsonl", {"event": event, **payload})

    def append_hook_trace(
        self,
        run: MarketIntelRun,
        event: str,
        payload: dict[str, Any],
    ) -> None:
        self.append_jsonl(run.run_dir / "hooks.jsonl", {"event": event, **payload})

    def append_model_call(self, run: MarketIntelRun, payload: dict[str, Any]) -> None:
        self.append_jsonl(run.run_dir / "model_calls.jsonl", payload)

    def append_jsonl(self, path: Path, payload: dict[str, Any]) -> None:
        record = {"logged_at": datetime.now(timezone.utc).isoformat(), **payload}
        log_path = path
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with log_path.open("a") as handle:
            handle.write(json.dumps(record, sort_keys=True, default=str) + "\n")
