from __future__ import annotations

import json
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
        for source_type in ("sec", "ir", "market", "news", "calendar"):
            (run.run_dir / "raw" / source_type).mkdir(parents=True, exist_ok=True)

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
        record = {"event": event, **payload}
        log_path = run.run_dir / "logs.jsonl"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with log_path.open("a") as handle:
            handle.write(json.dumps(record, sort_keys=True, default=str) + "\n")
