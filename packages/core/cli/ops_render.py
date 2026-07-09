from __future__ import annotations

import json
from typing import Any

from rich.console import Console

from core.cli.ops_render_helpers import build_console
from core.cli.ops_render_jobs import render_jobs_view
from core.cli.ops_render_storage import render_storage_ops_state
from core.cli.ops_render_trading import render_trading_ops_state


def render_json_payload(console: Console, payload: dict[str, Any]) -> None:
    console.file.write(json.dumps(payload, indent=2, default=str) + "\n")


__all__ = [
    "build_console",
    "render_jobs_view",
    "render_json_payload",
    "render_storage_ops_state",
    "render_trading_ops_state",
]
