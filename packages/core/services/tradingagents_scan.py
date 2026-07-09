from __future__ import annotations

from collections.abc import Callable, Mapping
import json
import os
from pathlib import Path
import re
import subprocess
import time
from typing import Any
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, field_validator

from core.services.alert_delivery import plan_alert_delivery
from core.services.sources.dispatch import get_latest_ticker_source_snapshot
from core.value_coercion import (
    as_text as _as_text,
    coerce_bool,
    coerce_int,
    normalize_symbol,
    safe_component,
    utc_now as _utc_now,
    utc_now_iso as _utc_now_text,
)
from core.storage.serializers import parse_datetime

NEW_YORK = ZoneInfo("America/New_York")
DEFAULT_TRADINGAGENTS_DIR = "/home/ade/Projects/spreads/external/TradingAgents"
DEFAULT_ACTIONABLE_SIGNALS = ("Buy", "Overweight", "Sell", "Underweight")
RESEARCH_SOURCE = "research.tradingagents_scan"
QUALITY_MESSAGE_VALUE_RE = re.compile(
    r"\b(?:value|level)\s+(?P<value>-?\$-?\d+(?:,\d{3})*(?:\.\d+)?" r"(?:(?:\s*(?:trillion|billion|million|thousand))|(?:[TBMK]\+?))?)",
    re.I,
)


class TradingAgentsScanPayload(BaseModel):
    model_config = ConfigDict(extra="allow", frozen=True)

    source_id: str = "finviz_momentum"
    source_job_key: str | None = None
    max_source_age_seconds: int | None = None
    label: str = "finviz_tradingagents"
    session_id: str | None = None
    max_tickers: int = 5
    output_root: str | None = None
    tradingagents_dir: str | None = None
    timeout_seconds: int = 1800
    heartbeat_seconds: int = 30
    profile: str = "fast"
    llm_provider: str | None = None
    quick_model: str | None = None
    deep_model: str | None = None
    backend_url: str | None = None
    prefetch: bool | None = None
    require_sec: bool = False
    allow_quality_warn: bool = False
    actionable_signals: tuple[str, ...] = DEFAULT_ACTIONABLE_SIGNALS
    uv_project_environment: str | None = None

    @field_validator(
        "source_id",
        "source_job_key",
        "label",
        "session_id",
        "output_root",
        "tradingagents_dir",
        "profile",
        "llm_provider",
        "quick_model",
        "deep_model",
        "backend_url",
        "uv_project_environment",
        mode="before",
    )
    @classmethod
    def _normalize_optional_text_fields(cls, value: Any, info: Any) -> str | None:
        rendered = _as_text(value)
        if rendered is None and info.field_name in {"source_id", "label", "profile"}:
            return str(cls.model_fields[info.field_name].default)
        return rendered

    @field_validator("max_source_age_seconds", mode="before")
    @classmethod
    def _normalize_optional_count(cls, value: Any) -> int | None:
        normalized = coerce_int(value)
        return None if normalized is None else max(normalized, 0)

    @field_validator("max_tickers", "timeout_seconds", "heartbeat_seconds", mode="before")
    @classmethod
    def _normalize_positive_count(cls, value: Any, info: Any) -> int:
        default = int(cls.model_fields[info.field_name].default)
        return max(coerce_int(value) or default, 1)

    @field_validator("prefetch", mode="before")
    @classmethod
    def _normalize_optional_bool(cls, value: Any) -> bool | None:
        return coerce_bool(value, default=None)

    @field_validator("require_sec", "allow_quality_warn", mode="before")
    @classmethod
    def _normalize_bool(cls, value: Any) -> bool:
        return bool(coerce_bool(value, default=False))

    @field_validator("actionable_signals", mode="before")
    @classmethod
    def _normalize_actionable_signals(cls, value: Any) -> tuple[str, ...]:
        if value in (None, ""):
            return DEFAULT_ACTIONABLE_SIGNALS
        if isinstance(value, str):
            raw_items = value.split(",")
        elif isinstance(value, (list, tuple, set)):
            raw_items = list(value)
        else:
            return DEFAULT_ACTIONABLE_SIGNALS
        signals = tuple(str(item).strip() for item in raw_items if str(item).strip())
        return signals or DEFAULT_ACTIONABLE_SIGNALS


def _unique_symbols(values: Any) -> tuple[str, ...]:
    if not isinstance(values, list):
        return ()
    return tuple(dict.fromkeys(symbol for value in values if (symbol := normalize_symbol(value)) is not None))


def _session_date(payload: Mapping[str, Any]) -> str:
    explicit = _as_text(payload.get("session_date"))
    if explicit is not None:
        return explicit
    scheduled_for = parse_datetime(payload.get("scheduled_for"))
    if scheduled_for is not None:
        return scheduled_for.astimezone(NEW_YORK).date().isoformat()
    return _utc_now().astimezone(NEW_YORK).date().isoformat()


def _resolve_path(value: Any, *, base: Path | None = None) -> Path:
    path = Path(str(value)).expanduser()
    if path.is_absolute():
        return path
    root = base or Path.cwd()
    return (root / path).resolve()


def _tail_text(path: Path, *, max_chars: int = 4000) -> str:
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""
    return text[-max_chars:]


def _load_metadata_from_path(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _load_metadata_from_stdout(path: Path) -> dict[str, Any] | None:
    try:
        text = path.read_text(encoding="utf-8", errors="replace").strip()
    except OSError:
        return None
    if not text:
        return None
    start = text.find("{")
    if start < 0:
        return None
    try:
        payload = json.loads(text[start:])
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def _find_latest_metadata_path(
    *,
    output_root: Path,
    ticker: str,
    started_epoch: float,
) -> Path | None:
    safe_ticker = safe_component(ticker)
    candidates = list(output_root.glob(f"{safe_ticker}_*/run_metadata.json"))
    if not candidates:
        candidates = list(output_root.glob("*/run_metadata.json"))
    fresh: list[Path] = []
    for path in candidates:
        try:
            if path.stat().st_mtime >= started_epoch - 5:
                fresh.append(path)
        except OSError:
            continue
    for path in sorted(fresh, key=lambda item: item.stat().st_mtime, reverse=True):
        metadata = _load_metadata_from_path(path)
        if str((metadata or {}).get("ticker") or "").upper() == ticker.upper():
            return path
    return fresh[0] if fresh else None


def _actionable_result(result: Mapping[str, Any], payload: Mapping[str, Any]) -> bool:
    allowed = {
        item.lower()
        for item in tuple(payload.get("actionable_signals") or DEFAULT_ACTIONABLE_SIGNALS)
    }
    signal = str(result.get("validated_signal") or "").strip()
    quality_status = str(result.get("quality_status") or "").strip().lower()
    if signal.lower() not in allowed:
        return False
    if quality_status == "pass":
        return True
    return quality_status == "warn" and bool(payload.get("allow_quality_warn"))


def _quality_issue_key(issue: Mapping[str, Any]) -> tuple[str, str, str]:
    code = str(issue.get("code") or "").strip().lower()
    severity = str(issue.get("severity") or "").strip().lower()
    message = str(issue.get("message") or "").strip()
    match = QUALITY_MESSAGE_VALUE_RE.search(message)
    if match:
        normalized_value = re.sub(r"\s+", "", match.group("value").lower())
        return severity, code, normalized_value
    return severity, code, message.lower()


def _dedupe_quality_issues(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    deduped: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for item in value:
        if not isinstance(item, Mapping):
            continue
        issue = dict(item)
        key = _quality_issue_key(issue)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(issue)
    return deduped


def _metadata_result_fields(metadata: Mapping[str, Any]) -> dict[str, Any]:
    quality = metadata.get("quality") if isinstance(metadata.get("quality"), Mapping) else {}
    quality_issues = _dedupe_quality_issues(metadata.get("quality_issues"))
    if quality_issues:
        quality_error_count = sum(1 for issue in quality_issues if str(issue.get("severity") or "").strip().lower() == "error")
        quality_warning_count = sum(1 for issue in quality_issues if str(issue.get("severity") or "").strip().lower() == "warning")
    else:
        quality_error_count = quality.get("errors")
        quality_warning_count = quality.get("warnings")
    return {
        "validated_signal": metadata.get("validated_signal") or metadata.get("signal"),
        "raw_signal": metadata.get("raw_signal"),
        "quality_status": metadata.get("quality_status"),
        "blocked_reason": metadata.get("blocked_reason"),
        "report_path": metadata.get("report_path"),
        "wall_seconds": metadata.get("wall_seconds"),
        "run_profile": metadata.get("run_profile"),
        "quality_passed": quality.get("passed"),
        "quality_errors": quality_error_count,
        "quality_warnings": quality_warning_count,
        "quality_issues": quality_issues,
    }


def _build_command(
    *,
    ticker: str,
    session_date: str,
    output_root: Path,
    payload: Mapping[str, Any],
) -> list[str]:
    command = [
        "uv",
        "run",
        "python",
        "scripts/benchmark_run.py",
        "--ticker",
        ticker,
        "--date",
        session_date,
        "--profile",
        _as_text(payload.get("profile")) or "fast",
        "--output-root",
        str(output_root),
    ]
    for payload_key, flag in (
        ("llm_provider", "--llm-provider"),
        ("quick_model", "--quick-model"),
        ("deep_model", "--deep-model"),
        ("backend_url", "--backend-url"),
    ):
        value = _as_text(payload.get(payload_key))
        if value is not None:
            command.extend([flag, value])
    if payload.get("prefetch") is not None:
        command.append("--prefetch" if bool(payload.get("prefetch")) else "--no-prefetch")
    if bool(payload.get("require_sec")):
        command.append("--require-sec")
    return command


def _tradingagents_env(payload: Mapping[str, Any]) -> dict[str, str]:
    env = dict(os.environ)
    uv_environment = _as_text(payload.get("uv_project_environment")) or _as_text(os.environ.get("SPREADS_TRADINGAGENTS_UV_ENVIRONMENT"))
    if uv_environment is not None:
        env["UV_PROJECT_ENVIRONMENT"] = str(Path(uv_environment).expanduser())
    return env


def _run_tradingagents_ticker(
    *,
    ticker: str,
    session_date: str,
    tradingagents_dir: Path,
    output_root: Path,
    log_root: Path,
    payload: Mapping[str, Any],
    heartbeat: Callable[[], None],
) -> dict[str, Any]:
    started_at = _utc_now_text()
    started_epoch = time.time()
    started_monotonic = time.monotonic()
    timeout_seconds = max(coerce_int(payload.get("timeout_seconds")) or 1800, 1)
    heartbeat_seconds = max(coerce_int(payload.get("heartbeat_seconds")) or 30, 1)
    safe_ticker = safe_component(ticker)
    log_root.mkdir(parents=True, exist_ok=True)
    stdout_path = log_root / f"{safe_ticker}.stdout.log"
    stderr_path = log_root / f"{safe_ticker}.stderr.log"
    command = _build_command(
        ticker=ticker,
        session_date=session_date,
        output_root=output_root,
        payload=payload,
    )
    heartbeat()
    timed_out = False
    with (
        stdout_path.open("w", encoding="utf-8") as stdout_file,
        stderr_path.open(
            "w",
            encoding="utf-8",
        ) as stderr_file,
    ):
        process = subprocess.Popen(
            command,
            cwd=tradingagents_dir,
            env=_tradingagents_env(payload),
            stdout=stdout_file,
            stderr=stderr_file,
            text=True,
        )
        try:
            while process.poll() is None:
                elapsed = time.monotonic() - started_monotonic
                if elapsed >= timeout_seconds:
                    timed_out = True
                    process.terminate()
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=10)
                    break
                heartbeat()
                time.sleep(min(float(heartbeat_seconds), timeout_seconds - elapsed))
        except Exception:
            if process.poll() is None:
                process.terminate()
            raise
        returncode = process.returncode
    finished_at = _utc_now_text()
    elapsed_seconds = round(time.monotonic() - started_monotonic, 2)
    metadata_path = _find_latest_metadata_path(
        output_root=output_root,
        ticker=ticker,
        started_epoch=started_epoch,
    )
    metadata = _load_metadata_from_path(metadata_path) if metadata_path is not None else _load_metadata_from_stdout(stdout_path)
    status = "timed_out" if timed_out else "failed"
    if metadata is not None and returncode in {0, 1}:
        status = "completed"
    result = {
        "ticker": ticker,
        "status": status,
        "returncode": returncode,
        "started_at": started_at,
        "finished_at": finished_at,
        "elapsed_seconds": elapsed_seconds,
        "command": command,
        "stdout_path": str(stdout_path),
        "stderr_path": str(stderr_path),
        "metadata_path": None if metadata_path is None else str(metadata_path),
    }
    if metadata is not None:
        result.update(_metadata_result_fields(metadata))
    else:
        result["error_tail"] = _tail_text(stderr_path)
    result["actionable"] = _actionable_result(result, payload)
    return result


def _result_summary_line(result: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "ticker": result.get("ticker"),
        "status": result.get("status"),
        "validated_signal": result.get("validated_signal"),
        "quality_status": result.get("quality_status"),
        "blocked_reason": result.get("blocked_reason"),
        "actionable": result.get("actionable"),
        "report_path": result.get("report_path"),
        "elapsed_seconds": result.get("elapsed_seconds"),
    }


def _plan_actionable_alert(
    *,
    storage: Any,
    job_store: Any,
    session_date: str,
    session_id: str,
    label: str,
    job_run_id: str,
    source_id: str,
    source_entry: Mapping[str, Any],
    result: Mapping[str, Any],
) -> dict[str, Any]:
    ticker = str(result["ticker"])
    signal = str(result.get("validated_signal") or "n/a")
    quality_status = str(result.get("quality_status") or "n/a")
    payload = {
        "created_at": _utc_now_text(),
        "session_date": session_date,
        "label": label,
        "cycle_id": job_run_id,
        "symbol": ticker,
        "alert_type": "research_tradingagents_actionable",
        "strategy_mode": "research",
        "profile": str(result.get("run_profile") or "fast"),
        "description": (f"TradingAgents {signal} on {ticker}; " f"quality {quality_status}; source Finviz {source_id}."),
        "details": {
            "source": "finviz",
            "source_id": source_id,
            "source_entry": dict(source_entry),
            "tradingagents": dict(result),
        },
    }
    dedupe_key = "research_tradingagents_actionable|" f"{session_date}|{source_id}|{ticker}|{signal}"
    row, created = plan_alert_delivery(
        alert_store=storage.alerts,
        job_store=job_store,
        payload=payload,
        dedupe_key=dedupe_key,
        dedupe_state={
            "validated_signal": signal,
            "quality_status": quality_status,
            "report_path": result.get("report_path"),
            "metadata_path": result.get("metadata_path"),
        },
        session_id=session_id,
        planner_job_run_id=job_run_id,
        source=RESEARCH_SOURCE,
        correlation_id=job_run_id,
    )
    return {
        "alert_id": row.get("alert_id"),
        "status": row.get("status"),
        "created": created,
        "symbol": ticker,
    }


def _plan_batch_alert(
    *,
    storage: Any,
    job_store: Any,
    session_date: str,
    session_id: str,
    label: str,
    job_run_id: str,
    source_id: str,
    selected_tickers: tuple[str, ...],
    snapshot: Mapping[str, Any],
    ticker_results: list[dict[str, Any]],
) -> dict[str, Any]:
    actionable_count = sum(1 for result in ticker_results if result.get("actionable"))
    completed_count = sum(1 for result in ticker_results if result.get("status") == "completed")
    failed_count = sum(1 for result in ticker_results if result.get("status") == "failed")
    timed_out_count = sum(1 for result in ticker_results if result.get("status") == "timed_out")
    payload = {
        "created_at": _utc_now_text(),
        "session_date": session_date,
        "label": label,
        "cycle_id": job_run_id,
        "symbol": "FINVIZ",
        "alert_type": "research_tradingagents_batch_summary",
        "strategy_mode": "research",
        "profile": "batch",
        "description": (
            f"Finviz TradingAgents scan: {actionable_count} actionable, " f"{completed_count} completed, {failed_count + timed_out_count} incomplete."
        ),
        "details": {
            "source": "finviz",
            "source_id": source_id,
            "source_job_run_id": snapshot.get("job_run_id"),
            "source_generated_at": snapshot.get("generated_at"),
            "selected_tickers": list(selected_tickers),
            "candidate_count": len(selected_tickers),
            "completed_count": completed_count,
            "failed_count": failed_count,
            "timed_out_count": timed_out_count,
            "actionable_count": actionable_count,
            "ticker_results": [_result_summary_line(result) for result in ticker_results],
        },
    }
    ticker_scope = "_".join(safe_component(str(ticker).upper()) for ticker in selected_tickers) or "none"
    dedupe_key = "research_tradingagents_batch_summary|" f"{session_date}|{source_id}|{ticker_scope}"
    row, created = plan_alert_delivery(
        alert_store=storage.alerts,
        job_store=job_store,
        payload=payload,
        dedupe_key=dedupe_key,
        dedupe_state={
            "selected_tickers": list(selected_tickers),
            "actionable_count": actionable_count,
            "completed_count": completed_count,
            "failed_count": failed_count,
            "timed_out_count": timed_out_count,
        },
        session_id=session_id,
        planner_job_run_id=job_run_id,
        source=RESEARCH_SOURCE,
        correlation_id=job_run_id,
    )
    return {
        "alert_id": row.get("alert_id"),
        "status": row.get("status"),
        "created": created,
        "symbol": "FINVIZ",
    }


def run_tradingagents_scan(
    *,
    storage: Any,
    job_store: Any,
    job_run_id: str,
    payload: Mapping[str, Any],
    heartbeat: Callable[[], None],
) -> dict[str, Any]:
    payload = TradingAgentsScanPayload.model_validate(payload).model_dump()
    source_id = str(payload["source_id"])
    source_job_key = _as_text(payload.get("source_job_key")) or f"ticker_source:{source_id}"
    max_source_age_seconds = payload.get("max_source_age_seconds")
    snapshot = get_latest_ticker_source_snapshot(
        storage.engine_facts,
        source_id=source_id,
        job_key=source_job_key,
        max_age_seconds=max_source_age_seconds,
    )
    snapshot_status = str(snapshot.get("status") or "").strip().lower()
    session_date = _session_date(payload)
    label = str(payload["label"])
    session_id = _as_text(payload.get("session_id")) or f"research:{label}:{session_date}"
    if snapshot_status not in {"ready", "empty"}:
        return {
            "status": "skipped",
            "reason": f"source_snapshot_{snapshot_status or 'missing'}",
            "source_id": source_id,
            "source_job_key": source_job_key,
            "source_snapshot": snapshot,
        }

    max_tickers = int(payload["max_tickers"])
    selected_tickers = _unique_symbols(snapshot.get("symbols"))[:max_tickers]
    entry_by_symbol = {
        str(entry.get("symbol") or "").upper(): dict(entry)
        for entry in list(snapshot.get("entries") or [])
        if isinstance(entry, Mapping) and str(entry.get("symbol") or "").strip()
    }
    output_root = _resolve_path(
        _as_text(payload.get("output_root"))
        or _as_text(os.environ.get("SPREADS_TRADINGAGENTS_OUTPUT_ROOT"))
        or "outputs/tradingagents/finviz_momentum",
    )
    output_root.mkdir(parents=True, exist_ok=True)
    log_root = output_root / "_spreads_logs" / session_date / safe_component(job_run_id)

    ticker_results: list[dict[str, Any]] = []
    alert_results: list[dict[str, Any]] = []
    tradingagents_dir = _resolve_path(
        _as_text(payload.get("tradingagents_dir")) or _as_text(os.environ.get("SPREADS_TRADINGAGENTS_DIR")) or DEFAULT_TRADINGAGENTS_DIR,
    )
    benchmark_script = tradingagents_dir / "scripts" / "benchmark_run.py"
    if selected_tickers and not benchmark_script.exists():
        raise RuntimeError(f"TradingAgents benchmark entrypoint not found: {benchmark_script}")
    for ticker in selected_tickers:
        heartbeat()
        result = _run_tradingagents_ticker(
            ticker=ticker,
            session_date=session_date,
            tradingagents_dir=tradingagents_dir,
            output_root=output_root,
            log_root=log_root,
            payload=payload,
            heartbeat=heartbeat,
        )
        ticker_results.append(result)
        if result.get("actionable"):
            alert_results.append(
                _plan_actionable_alert(
                    storage=storage,
                    job_store=job_store,
                    session_date=session_date,
                    session_id=session_id,
                    label=label,
                    job_run_id=job_run_id,
                    source_id=source_id,
                    source_entry=entry_by_symbol.get(ticker, {}),
                    result=result,
                )
            )

    batch_alert = _plan_batch_alert(
        storage=storage,
        job_store=job_store,
        session_date=session_date,
        session_id=session_id,
        label=label,
        job_run_id=job_run_id,
        source_id=source_id,
        selected_tickers=selected_tickers,
        snapshot=snapshot,
        ticker_results=ticker_results,
    )
    completed_count = sum(1 for result in ticker_results if result.get("status") == "completed")
    failed_count = sum(1 for result in ticker_results if result.get("status") == "failed")
    timed_out_count = sum(1 for result in ticker_results if result.get("status") == "timed_out")
    actionable_count = sum(1 for result in ticker_results if result.get("actionable"))
    return {
        "status": "completed",
        "source_id": source_id,
        "source_job_key": source_job_key,
        "source_job_run_id": snapshot.get("job_run_id"),
        "source_generated_at": snapshot.get("generated_at"),
        "session_date": session_date,
        "label": label,
        "candidate_count": len(_unique_symbols(snapshot.get("symbols"))),
        "selected_tickers": list(selected_tickers),
        "completed_count": completed_count,
        "failed_count": failed_count,
        "timed_out_count": timed_out_count,
        "actionable_count": actionable_count,
        "suppressed_count": completed_count - actionable_count,
        "ticker_results": ticker_results,
        "alerts": [*alert_results, batch_alert],
        "output_root": str(output_root),
    }


__all__ = ["run_tradingagents_scan"]
