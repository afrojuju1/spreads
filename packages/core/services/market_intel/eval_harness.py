from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any
from uuid import uuid4

from core.services.market_intel.config import resolve_output_root
from core.services.market_intel.contracts import (
    MarketIntelDepth,
    MarketIntelRequest,
    SourceType,
    utc_now,
)
from core.services.market_intel.run_orchestrator import create_market_intel_run


DEFAULT_EVAL_OUTPUT_ROOT = Path("outputs/market_intel_eval")


def run_market_intel_eval(
    *,
    tickers: tuple[str, ...],
    as_of: date,
    sources: tuple[SourceType, ...],
    depth: MarketIntelDepth,
    no_llm: bool,
    output_root: Path = DEFAULT_EVAL_OUTPUT_ROOT,
) -> dict[str, Any]:
    started_at = utc_now()
    eval_id = f"market_intel_eval:{started_at.strftime('%Y%m%dT%H%M%SZ')}:{uuid4().hex[:8]}"
    eval_dir = resolve_output_root(output_root) / _safe_name(eval_id)
    runs_root = eval_dir / "runs"
    eval_dir.mkdir(parents=True, exist_ok=True)

    cases: list[dict[str, Any]] = []
    for ticker in tickers:
        request = MarketIntelRequest(
            ticker=ticker,
            as_of=as_of,
            output_root=runs_root,
            sources=sources,
            depth=depth,
            no_llm=no_llm,
        )
        try:
            run = create_market_intel_run(request)
            case = _evaluate_run(run_dir=Path(run.run_dir), require_llm=not no_llm)
            case.update(
                {
                    "ticker": run.ticker,
                    "as_of": run.as_of.isoformat(),
                    "run_id": run.run_id,
                    "run_dir": str(run.run_dir),
                    "status": run.status,
                    "warnings": list(run.warnings),
                }
            )
        except Exception as exc:
            case = {
                "ticker": ticker.upper(),
                "as_of": as_of.isoformat(),
                "passed": False,
                "failure_reasons": [f"run failed: {exc}"],
                "error": str(exc),
            }
        cases.append(case)

    passed_count = sum(1 for case in cases if case.get("passed") is True)
    payload = {
        "eval_id": eval_id,
        "started_at": started_at.isoformat(),
        "completed_at": utc_now().isoformat(),
        "eval_dir": str(eval_dir),
        "case_count": len(cases),
        "passed_count": passed_count,
        "failed_count": len(cases) - passed_count,
        "passed": passed_count == len(cases),
        "config": {
            "tickers": list(tickers),
            "as_of": as_of.isoformat(),
            "sources": list(sources),
            "depth": depth,
            "no_llm": no_llm,
        },
        "cases": cases,
    }
    _write_json(eval_dir / "eval.json", payload)
    _write_text(eval_dir / "eval.md", _render_eval_markdown(payload))
    return payload


def _evaluate_run(*, run_dir: Path, require_llm: bool) -> dict[str, Any]:
    evidence = _read_json(run_dir / "evidence.json")
    sources = _read_json(run_dir / "sources.json")
    review = _read_json(run_dir / "review.json")
    stages = _read_json(run_dir / "agent_stages.json")
    model_calls = _read_jsonl(run_dir / "model_calls.jsonl")
    evidence_count = len(evidence.get("items") if isinstance(evidence.get("items"), list) else [])
    artifact_count = len(sources.get("artifacts") if isinstance(sources.get("artifacts"), list) else [])
    findings = review.get("findings") if isinstance(review.get("findings"), list) else []
    blockers = [finding for finding in findings if finding.get("severity") == "blocker"]
    majors = [finding for finding in findings if finding.get("severity") == "major"]
    completed_model_calls = [
        call for call in model_calls if call.get("status") == "completed"
    ]
    failure_reasons: list[str] = []
    if artifact_count < 2:
        failure_reasons.append("expected at least two source artifacts")
    if evidence_count < 2:
        failure_reasons.append("expected at least two evidence items")
    if blockers:
        failure_reasons.append("review produced blocker findings")
    if majors:
        failure_reasons.append("review produced major findings")
    if require_llm:
        if stages.get("analyst_ran") is not True:
            failure_reasons.append("LLM analyst stage did not complete")
        if stages.get("skeptic_ran") is not True:
            failure_reasons.append("LLM skeptic stage did not complete")
        if not completed_model_calls:
            failure_reasons.append("no completed model calls recorded")
    return {
        "passed": not failure_reasons,
        "failure_reasons": failure_reasons,
        "artifact_count": artifact_count,
        "evidence_count": evidence_count,
        "finding_count": len(findings),
        "major_count": len(majors),
        "blocker_count": len(blockers),
        "completed_model_call_count": len(completed_model_calls),
    }


def _render_eval_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Market Intel Eval",
        "",
        f"- eval_id: `{payload['eval_id']}`",
        f"- passed: `{payload['passed']}`",
        f"- cases: `{payload['passed_count']}/{payload['case_count']}`",
        "",
        "## Cases",
        "",
    ]
    for case in payload["cases"]:
        lines.append(
            f"- {case.get('ticker')} {case.get('as_of')}: passed={case.get('passed')} "
            f"evidence={case.get('evidence_count', 0)} findings={case.get('finding_count', 0)}"
        )
        for reason in case.get("failure_reasons", []):
            lines.append(f"  - {reason}")
    lines.append("")
    return "\n".join(lines)


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        raw_lines = path.read_text().splitlines()
    except FileNotFoundError:
        return rows
    for line in raw_lines:
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n")


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)


def _safe_name(value: str) -> str:
    return value.replace(":", "_").replace("/", "_")
