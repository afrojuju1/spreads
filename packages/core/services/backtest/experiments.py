from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from uuid import uuid4

from pydantic import Field

from core.model_contracts import DomainModel
from core.runtime.config import default_backtest_artifact_root
from core.services.backtest.models import BacktestArtifactKind, BacktestStorageKind
from core.storage.serializers import render_value


class BacktestArtifactWrite(DomainModel):
    backtest_artifact_id: str
    artifact_kind: str
    storage_kind: str
    uri: str
    content_type: str
    byte_count: int
    row_count: int | None = None
    payload_schema: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any]


def resolve_backtest_artifact_root(artifact_root: str | None = None) -> Path:
    return Path(artifact_root or default_backtest_artifact_root()).expanduser().resolve()


def write_json_artifact(
    *,
    artifact_root: str | None,
    backtest_run_id: str,
    artifact_kind: str | BacktestArtifactKind,
    payload: dict[str, Any],
    metadata: dict[str, Any] | None = None,
) -> BacktestArtifactWrite:
    root = resolve_backtest_artifact_root(artifact_root)
    run_dir = root / backtest_run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    artifact_id = f"bta_{uuid4().hex}"
    artifact_kind_value = artifact_kind.value if isinstance(artifact_kind, BacktestArtifactKind) else str(artifact_kind)
    path = run_dir / f"{artifact_kind_value}.json"
    rendered_payload = render_value(payload)
    path.write_text(json.dumps(rendered_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return BacktestArtifactWrite(
        backtest_artifact_id=artifact_id,
        artifact_kind=artifact_kind_value,
        storage_kind=BacktestStorageKind.FILE.value,
        uri=str(path),
        content_type="application/json",
        byte_count=path.stat().st_size,
        payload_schema={},
        metadata=dict(metadata or {}),
    )


__all__ = ["BacktestArtifactWrite", "resolve_backtest_artifact_root", "write_json_artifact"]
