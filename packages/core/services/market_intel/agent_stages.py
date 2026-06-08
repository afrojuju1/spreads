from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from core.value_coercion import as_list
from core.services.market_intel.artifact_store import MarketIntelArtifactStore
from core.services.market_intel.config import MarketIntelModelConfig
from core.services.market_intel.contracts import (
    EvidenceItem,
    MarketIntelRequest,
    MarketIntelRun,
    ModelProfile,
    SourceArtifact,
)
from core.services.market_intel.model_router import MarketIntelModelRouter


@dataclass(frozen=True)
class AgentStageResult:
    analyst_payload: dict[str, Any] | None = None
    skeptic_payload: dict[str, Any] | None = None
    warnings: tuple[str, ...] = ()


ANALYST_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["sections", "notes"],
    "properties": {
        "sections": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["name", "text", "evidence_refs"],
                "properties": {
                    "name": {
                        "type": "string",
                        "enum": [
                            "setup",
                            "why_now",
                            "variant_view",
                            "base_case",
                            "bull_case",
                            "bear_case",
                            "invalidation",
                            "portfolio_fit",
                            "expected_window",
                            "expected_return",
                        ],
                    },
                    "text": {"type": "string"},
                    "evidence_refs": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                },
            },
        },
        "notes": {
            "type": "array",
            "items": {"type": "string"},
        },
    },
}


SKEPTIC_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["findings", "approved_sections"],
    "properties": {
        "findings": {
            "type": "array",
            "maxItems": 3,
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["severity", "finding_type", "note", "evidence_refs"],
                "properties": {
                    "severity": {
                        "type": "string",
                        "enum": ["blocker", "major", "minor", "note"],
                    },
                    "finding_type": {"type": "string"},
                    "claim_ref": {"type": ["string", "null"]},
                    "note": {"type": "string"},
                    "required_action": {"type": ["string", "null"]},
                    "evidence_refs": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                },
            },
        },
        "approved_sections": {
            "type": "array",
            "maxItems": 10,
            "items": {"type": "string"},
        },
    },
}


def run_llm_agent_stages(
    *,
    request: MarketIntelRequest,
    run: MarketIntelRun,
    store: MarketIntelArtifactStore,
    model_config: MarketIntelModelConfig,
    artifacts: tuple[SourceArtifact, ...],
    evidence: tuple[EvidenceItem, ...],
) -> AgentStageResult:
    if not evidence:
        return AgentStageResult(warnings=("LLM stages skipped: no evidence collected",))

    router = MarketIntelModelRouter(
        config=model_config,
        artifact_store=store,
        run=run,
    )
    warnings: list[str] = []
    analyst_payload: dict[str, Any] | None = None
    skeptic_payload: dict[str, Any] | None = None
    reasoning_profile = _reasoning_profile(request.depth)

    try:
        store.append_agent_trace(run, "llm_analyst_started", {"agent_id": "AnalystDrafter"})
        analyst_response = router.invoke_structured(
            agent_id="AnalystDrafter",
            profile=reasoning_profile,
            depth=request.depth,
            schema=ANALYST_SCHEMA,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You draft concise market-intel sections from evidence. Use only the provided "
                        "evidence. Return two to four sections, not every possible section. Every "
                        "section must cite evidence_refs using evidence_id values from the evidence "
                        "pack. Do not cite source_artifact ids. Do not invent catalysts, valuation, "
                        "ownership, options, news, guidance, upside, downside, or investment merit. "
                        "If evidence only supports identity, filing, price, or volume context, write "
                        "that context plainly and put missing thesis ingredients in notes. "
                        "Return only one JSON object matching the schema. Do not write markdown. "
                        "Use only these section names: setup, why_now, variant_view, base_case, "
                        "bull_case, bear_case, invalidation, portfolio_fit, expected_window, "
                        "expected_return."
                    ),
                },
                {
                    "role": "user",
                    "content": json.dumps(
                        {
                            "ticker": run.ticker,
                            "as_of": run.as_of.isoformat(),
                            "source_artifacts": [artifact.to_payload() for artifact in artifacts],
                            "evidence": _evidence_pack(evidence),
                        },
                        sort_keys=True,
                    ),
                },
            ],
            options={
                "temperature": 0.1,
                "num_ctx": 4096,
                "num_predict": 900,
                "think": False,
                "keep_alive": "5m",
            },
        )
        store.write_text(run.run_dir / "llm" / "analyst_raw.txt", analyst_response.content)
        store.write_json(run.run_dir / "llm" / "analyst_raw_payload.json", analyst_response.raw_payload)
        analyst_payload = _parse_json_object(analyst_response.content)
        _validate_analyst_payload(analyst_payload)
        store.write_json(run.run_dir / "llm" / "analyst.json", analyst_payload)
        store.append_agent_trace(
            run,
            "llm_analyst_completed",
            {
                "model": analyst_response.model,
                "elapsed_seconds": round(analyst_response.elapsed_seconds, 6),
                "section_count": len(as_list(analyst_payload.get("sections"))),
            },
        )
    except Exception as exc:
        warnings.append(f"LLM analyst stage failed: {exc}")
        store.write_text(run.run_dir / "llm" / "analyst_error.txt", str(exc))
        store.append_agent_trace(run, "llm_analyst_failed", {"error": str(exc)})

    if analyst_payload is not None:
        try:
            store.append_agent_trace(run, "llm_skeptic_started", {"agent_id": "SkepticReviewer"})
            skeptic_response = router.invoke_structured(
                agent_id="SkepticReviewer",
                profile=reasoning_profile,
                depth=request.depth,
                schema=SKEPTIC_SCHEMA,
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "You are the skeptic gate. Review candidate thesis sections against "
                            "the evidence pack. Flag unsupported, overstated, stale, or weak claims."
                            " Return at most three findings. If there are no issues, return an empty"
                            " findings array. Do not penalize omitted sections when the analyst is "
                            "explicitly preserving a data gap. Return only one JSON object matching the schema. "
                            "Do not write markdown."
                        ),
                    },
                    {
                        "role": "user",
                        "content": json.dumps(
                            {
                                "ticker": run.ticker,
                                "as_of": run.as_of.isoformat(),
                                "evidence": _evidence_pack(evidence),
                                "candidate": analyst_payload,
                            },
                            sort_keys=True,
                        ),
                    },
                ],
                options={
                    "temperature": 0.0,
                    "num_ctx": 4096,
                    "num_predict": 1200,
                    "think": False,
                    "keep_alive": "5m",
                },
            )
            store.write_text(run.run_dir / "llm" / "skeptic_raw.txt", skeptic_response.content)
            store.write_json(run.run_dir / "llm" / "skeptic_raw_payload.json", skeptic_response.raw_payload)
            skeptic_payload = _parse_json_object(skeptic_response.content)
            _validate_skeptic_payload(skeptic_payload)
            store.write_json(run.run_dir / "llm" / "skeptic.json", skeptic_payload)
            store.append_agent_trace(
                run,
                "llm_skeptic_completed",
                {
                    "model": skeptic_response.model,
                    "elapsed_seconds": round(skeptic_response.elapsed_seconds, 6),
                    "finding_count": len(as_list(skeptic_payload.get("findings"))),
                },
            )
        except Exception as exc:
            warnings.append(f"LLM skeptic stage failed: {exc}")
            store.write_text(run.run_dir / "llm" / "skeptic_error.txt", str(exc))
            store.append_agent_trace(run, "llm_skeptic_failed", {"error": str(exc)})

    return AgentStageResult(
        analyst_payload=analyst_payload,
        skeptic_payload=skeptic_payload,
        warnings=tuple(warnings),
    )


def _evidence_pack(evidence: tuple[EvidenceItem, ...]) -> list[dict[str, Any]]:
    return [
        {
            "evidence_id": item.evidence_id,
            "claim_text": item.claim_text,
            "claim_type": item.claim_type,
            "tags": list(item.tags),
            "final_confidence": item.final_confidence,
            "source_rank": item.source_rank,
            "observed_at": None if item.observed_at is None else item.observed_at.isoformat(),
        }
        for item in evidence
    ]


def _reasoning_profile(depth: str) -> ModelProfile:
    if depth == "quick":
        return "fast_structured"
    if depth == "deep":
        return "deep_reasoning"
    return "standard_reasoning"


def _parse_json_object(content: str) -> dict[str, Any]:
    text = content.strip()
    if not text:
        raise ValueError("model returned an empty response")
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start < 0 or end <= start:
            raise
        parsed = json.loads(text[start : end + 1])
    if not isinstance(parsed, dict):
        raise ValueError("model response was not a JSON object")
    return parsed


def _validate_analyst_payload(payload: dict[str, Any]) -> None:
    sections = payload.get("sections")
    notes = payload.get("notes")
    if not isinstance(sections, list):
        raise ValueError("analyst payload missing sections array")
    if not isinstance(notes, list):
        raise ValueError("analyst payload missing notes array")
    allowed_names = set(ANALYST_SCHEMA["properties"]["sections"]["items"]["properties"]["name"]["enum"])
    for index, section in enumerate(sections):
        if not isinstance(section, dict):
            raise ValueError(f"analyst section {index} was not an object")
        name = section.get("name")
        text = section.get("text")
        refs = section.get("evidence_refs")
        if name not in allowed_names:
            raise ValueError(f"analyst section {index} used unsupported name {name!r}")
        if not isinstance(text, str) or not text.strip():
            raise ValueError(f"analyst section {name!r} missing text")
        if not isinstance(refs, list) or not all(isinstance(ref, str) and ref.strip() for ref in refs):
            raise ValueError(f"analyst section {name!r} missing evidence_refs")


def _validate_skeptic_payload(payload: dict[str, Any]) -> None:
    findings = payload.get("findings")
    approved_sections = payload.get("approved_sections")
    if not isinstance(findings, list):
        raise ValueError("skeptic payload missing findings array")
    if not isinstance(approved_sections, list):
        raise ValueError("skeptic payload missing approved_sections array")
    allowed_severities = set(SKEPTIC_SCHEMA["properties"]["findings"]["items"]["properties"]["severity"]["enum"])
    for index, finding in enumerate(findings):
        if not isinstance(finding, dict):
            raise ValueError(f"skeptic finding {index} was not an object")
        severity = finding.get("severity")
        finding_type = finding.get("finding_type")
        note = finding.get("note")
        refs = finding.get("evidence_refs")
        if severity not in allowed_severities:
            raise ValueError(f"skeptic finding {index} used unsupported severity {severity!r}")
        if not isinstance(finding_type, str) or not finding_type.strip():
            raise ValueError(f"skeptic finding {index} missing finding_type")
        if not isinstance(note, str) or not note.strip():
            raise ValueError(f"skeptic finding {index} missing note")
        if not isinstance(refs, list) or not all(isinstance(ref, str) and ref.strip() for ref in refs):
            raise ValueError(f"skeptic finding {index} missing evidence_refs")
