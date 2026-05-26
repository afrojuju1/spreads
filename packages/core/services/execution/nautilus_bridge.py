from __future__ import annotations

import json
import os
import shlex
import shutil
import subprocess
from collections.abc import Mapping
from pathlib import Path
from typing import Any

DEFAULT_NAUTILUS_BRIDGE_COMMAND = "alpaca-submit-order-list-bridge"
DEFAULT_NAUTILUS_BRIDGE_TIMEOUT_SECONDS = 45.0
DEFAULT_NAUTILUS_BRIDGE_CANDIDATES = (
    DEFAULT_NAUTILUS_BRIDGE_COMMAND,
    "/usr/local/bin/alpaca-submit-order-list-bridge",
    "/home/ade/.local/bin/alpaca-submit-order-list-bridge",
    "/home/ade/Projects/nautilus_trader/target/release/alpaca-submit-order-list-bridge",
    "/home/ade/Projects/nautilus_trader/target/debug/alpaca-submit-order-list-bridge",
)


class NautilusBridgeError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        reason: str,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.reason = reason
        self.details = dict(details or {})


def submit_nautilus_order_list(handoff: Mapping[str, Any]) -> dict[str, Any]:
    command = _bridge_command()
    timeout_seconds = _bridge_timeout_seconds()
    stdin_payload = json.dumps(dict(handoff), separators=(",", ":"), sort_keys=True)
    cwd = _clean_env_text("SPREADS_NAUTILUS_BRIDGE_CWD")
    try:
        completed = subprocess.run(
            command,
            input=stdin_payload,
            text=True,
            capture_output=True,
            timeout=timeout_seconds,
            check=False,
            cwd=cwd,
        )
    except FileNotFoundError as exc:
        raise NautilusBridgeError(
            f"Nautilus bridge command not found: {command[0]}",
            reason="nautilus_bridge_command_not_found",
            details={"command": _command_summary(command)},
        ) from exc
    except OSError as exc:
        raise NautilusBridgeError(
            f"Nautilus bridge command could not be started: {exc}",
            reason="nautilus_bridge_command_start_failed",
            details={"command": _command_summary(command)},
        ) from exc
    except subprocess.TimeoutExpired as exc:
        raise NautilusBridgeError(
            f"Nautilus bridge timed out after {timeout_seconds:g}s",
            reason="nautilus_bridge_timeout",
            details={
                "command": _command_summary(command),
                "timeout_seconds": timeout_seconds,
                "stdout": _tail(exc.stdout),
                "stderr": _tail(exc.stderr),
            },
        ) from exc

    if completed.returncode != 0:
        raise NautilusBridgeError(
            f"Nautilus bridge exited with status {completed.returncode}",
            reason="nautilus_bridge_process_failed",
            details={
                "command": _command_summary(command),
                "returncode": completed.returncode,
                "stdout": _tail(completed.stdout),
                "stderr": _tail(completed.stderr),
            },
        )

    return _validate_bridge_result(_parse_bridge_stdout(completed.stdout))


def describe_nautilus_bridge() -> dict[str, Any]:
    try:
        command = _bridge_command()
        timeout_seconds = _bridge_timeout_seconds()
    except NautilusBridgeError as exc:
        return {
            "status": "blocked",
            "ready": False,
            "reason": exc.reason,
            "message": str(exc),
            **({"details": exc.details} if exc.details else {}),
        }

    cwd = _clean_env_text("SPREADS_NAUTILUS_BRIDGE_CWD")
    resolved_program = _resolve_bridge_program(command[0]) if command else None
    ready = resolved_program is not None
    return {
        "status": "ready" if ready else "unavailable",
        "ready": ready,
        "reason": None if ready else "nautilus_bridge_command_not_found",
        "command": _command_summary(command),
        "resolved_program": resolved_program,
        "cwd": cwd,
        "timeout_seconds": timeout_seconds,
    }


def _bridge_command() -> list[str]:
    raw = _clean_env_text("SPREADS_NAUTILUS_BRIDGE_COMMAND")
    if raw is None:
        return [_default_bridge_program()]
    command = shlex.split(raw)
    if not command:
        raise NautilusBridgeError(
            "SPREADS_NAUTILUS_BRIDGE_COMMAND is empty",
            reason="nautilus_bridge_command_empty",
        )
    return command


def _default_bridge_program() -> str:
    for candidate in DEFAULT_NAUTILUS_BRIDGE_CANDIDATES:
        resolved = _resolve_bridge_program(candidate)
        if resolved is not None:
            return resolved
    return DEFAULT_NAUTILUS_BRIDGE_COMMAND


def _bridge_timeout_seconds() -> float:
    raw = _clean_env_text("SPREADS_NAUTILUS_BRIDGE_TIMEOUT_SECONDS")
    if raw is None:
        return DEFAULT_NAUTILUS_BRIDGE_TIMEOUT_SECONDS
    try:
        parsed = float(raw)
    except ValueError as exc:
        raise NautilusBridgeError(
            "SPREADS_NAUTILUS_BRIDGE_TIMEOUT_SECONDS must be numeric",
            reason="nautilus_bridge_timeout_invalid",
            details={"value": raw},
        ) from exc
    if parsed <= 0:
        raise NautilusBridgeError(
            "SPREADS_NAUTILUS_BRIDGE_TIMEOUT_SECONDS must be positive",
            reason="nautilus_bridge_timeout_invalid",
            details={"value": raw},
        )
    return parsed


def _parse_bridge_stdout(stdout: str) -> dict[str, Any]:
    for line in reversed((stdout or "").splitlines()):
        candidate = line.strip()
        if not candidate:
            continue
        if not candidate.startswith("{"):
            continue
        try:
            parsed = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            return parsed

    raise NautilusBridgeError(
        "Nautilus bridge did not emit a JSON object",
        reason="nautilus_bridge_invalid_output",
        details={"stdout": _tail(stdout)},
    )


def _validate_bridge_result(payload: dict[str, Any]) -> dict[str, Any]:
    status = str(payload.get("status") or "").strip().lower()
    if not status:
        raise NautilusBridgeError(
            "Nautilus bridge result is missing status",
            reason="nautilus_bridge_result_missing_status",
            details={"payload_keys": sorted(str(key) for key in payload.keys())},
        )
    events = payload.get("events")
    if events is not None and not isinstance(events, list):
        raise NautilusBridgeError(
            "Nautilus bridge result events must be a list",
            reason="nautilus_bridge_result_invalid_events",
            details={"events_type": type(events).__name__},
        )
    return payload


def _clean_env_text(name: str) -> str | None:
    value = os.environ.get(name)
    if value is None:
        return None
    cleaned = value.strip()
    return cleaned or None


def _command_summary(command: list[str]) -> dict[str, Any]:
    return {"program": command[0] if command else None, "argc": len(command)}


def _resolve_bridge_program(program: str | None) -> str | None:
    if program is None:
        return None
    if "/" not in program:
        return shutil.which(program)
    path = Path(program).expanduser()
    if path.is_file() and os.access(path, os.X_OK):
        return str(path)
    return None


def _tail(value: str | bytes | None, *, limit: int = 4000) -> str | None:
    if value is None:
        return None
    text = value.decode(errors="replace") if isinstance(value, bytes) else str(value)
    return text[-limit:]


__all__ = [
    "NautilusBridgeError",
    "describe_nautilus_bridge",
    "submit_nautilus_order_list",
]
