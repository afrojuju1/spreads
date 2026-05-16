from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from core.services.market_intel.contracts import (
    EvidenceItem,
    MarketIntelRequest,
    MarketIntelRun,
    ReviewFinding,
    SourceArtifact,
    ThesisArtifact,
)
from core.services.market_intel.ids import build_artifact_id


@dataclass(frozen=True)
class FinalizationResult:
    thesis: ThesisArtifact
    thesis_markdown: str
    review_markdown: str
    findings: tuple[ReviewFinding, ...]
    warnings: tuple[str, ...]


def finalize_market_intel_run(
    *,
    run: MarketIntelRun,
    request: MarketIntelRequest,
    artifacts: tuple[SourceArtifact, ...],
    evidence: tuple[EvidenceItem, ...],
    analyst_payload: dict[str, Any] | None = None,
    skeptic_payload: dict[str, Any] | None = None,
) -> FinalizationResult:
    del request
    evidence_ids = tuple(item.evidence_id for item in evidence)
    findings = list(_review_findings(run=run, artifacts=artifacts, evidence=evidence))
    llm_sections, llm_findings = _supported_llm_sections(
        run=run,
        analyst_payload=analyst_payload,
        evidence_ids=set(evidence_ids),
    )
    findings.extend(llm_findings)
    findings.extend(
        _skeptic_findings(
            run=run,
            skeptic_payload=skeptic_payload,
            evidence_ids=set(evidence_ids),
        )
    )
    if not findings:
        findings.append(
            ReviewFinding(
                finding_id=build_artifact_id(run.run_id, "finding", "passed_v0_guardrail"),
                run_id=run.run_id,
                severity="note",
                finding_type="guardrail_passed",
                claim_ref=None,
                evidence_refs=evidence_ids,
                note="All rendered v0 thesis claims map to collected evidence ids.",
            )
        )
    final_findings = tuple(findings)
    blocker_count = sum(1 for finding in findings if finding.severity == "blocker")
    warnings: list[str] = []
    if blocker_count:
        warnings.append("finalizer blocked unsupported thesis: missing required evidence")
    llm_section_count = len(llm_sections)

    thesis = ThesisArtifact(
        run_id=run.run_id,
        ticker=run.ticker,
        as_of=run.as_of,
        setup=llm_sections.get("setup") or _setup_summary(evidence),
        why_now=llm_sections.get("why_now") or _why_now_summary(evidence),
        variant_view=llm_sections.get("variant_view")
        or "Deterministic v0 finalizer only renders evidence-backed claims.",
        core_evidence=evidence_ids,
        base_case=llm_sections.get("base_case") or _base_case_summary(evidence),
        bull_case=llm_sections.get("bull_case"),
        bear_case=llm_sections.get("bear_case")
        or "Not generated yet. Needs supported downside evidence.",
        expected_window=llm_sections.get("expected_window"),
        expected_return=llm_sections.get("expected_return"),
        invalidation=llm_sections.get("invalidation")
        or "Not generated yet. Needs catalyst, valuation, and risk-stage evidence.",
        portfolio_fit=llm_sections.get("portfolio_fit"),
        thesis_quality=0.0 if blocker_count else min(0.5, 0.25 + 0.04 * llm_section_count),
        evidence_quality=_evidence_quality(evidence=evidence, artifacts=artifacts),
        confidence=0.0 if blocker_count else min(0.45, 0.08 * len(evidence) + 0.03 * llm_section_count),
        skeptic_notes=tuple(finding.note for finding in final_findings),
        source_pack=tuple(artifact.artifact_id for artifact in artifacts),
    )
    return FinalizationResult(
        thesis=thesis,
        thesis_markdown=_render_thesis_markdown(run=run, thesis=thesis, evidence=evidence, findings=final_findings),
        review_markdown=_render_review_markdown(run=run, findings=final_findings),
        findings=final_findings,
        warnings=tuple(warnings),
    )


def _review_findings(
    *,
    run: MarketIntelRun,
    artifacts: tuple[SourceArtifact, ...],
    evidence: tuple[EvidenceItem, ...],
) -> tuple[ReviewFinding, ...]:
    findings: list[ReviewFinding] = []
    artifact_ids = {artifact.artifact_id for artifact in artifacts}
    if not evidence:
        findings.append(
            ReviewFinding(
                finding_id=build_artifact_id(run.run_id, "finding", "no_evidence"),
                run_id=run.run_id,
                severity="blocker",
                finding_type="missing_evidence",
                claim_ref=None,
                note="No evidence items were collected; final thesis claims are blocked.",
                required_action="Collect at least one trusted source artifact and evidence item.",
            )
        )
    for item in evidence:
        if item.artifact_id is None:
            findings.append(
                ReviewFinding(
                    finding_id=build_artifact_id(run.run_id, "finding", item.evidence_id, "no_artifact"),
                    run_id=run.run_id,
                    severity="major",
                    finding_type="evidence_without_artifact",
                    claim_ref=item.evidence_id,
                    evidence_refs=(item.evidence_id,),
                    note=f"Evidence item {item.evidence_id} has no artifact reference.",
                    required_action="Attach the evidence item to a source artifact before using it in a thesis.",
                )
            )
        elif item.artifact_id not in artifact_ids:
            findings.append(
                ReviewFinding(
                    finding_id=build_artifact_id(run.run_id, "finding", item.evidence_id, "missing_artifact"),
                    run_id=run.run_id,
                    severity="major",
                    finding_type="missing_artifact",
                    claim_ref=item.evidence_id,
                    evidence_refs=(item.evidence_id,),
                    note=f"Evidence item {item.evidence_id} references an unknown artifact.",
                    required_action="Write the referenced source artifact or drop the evidence item.",
                )
            )
        if item.final_confidence < 0.5:
            findings.append(
                ReviewFinding(
                    finding_id=build_artifact_id(run.run_id, "finding", item.evidence_id, "low_confidence"),
                    run_id=run.run_id,
                    severity="minor",
                    finding_type="low_confidence_evidence",
                    claim_ref=item.evidence_id,
                    evidence_refs=(item.evidence_id,),
                    note=f"Evidence item {item.evidence_id} has final confidence {item.final_confidence}.",
                    required_action="Use only as a weak signal until corroborated.",
                )
            )
    return tuple(findings)


def _supported_llm_sections(
    *,
    run: MarketIntelRun,
    analyst_payload: dict[str, Any] | None,
    evidence_ids: set[str],
) -> tuple[dict[str, str], tuple[ReviewFinding, ...]]:
    if analyst_payload is None:
        return {}, ()
    sections: dict[str, str] = {}
    findings: list[ReviewFinding] = []
    raw_sections = analyst_payload.get("sections")
    if not isinstance(raw_sections, list):
        return {}, (
            ReviewFinding(
                finding_id=build_artifact_id(run.run_id, "finding", "llm_sections_missing"),
                run_id=run.run_id,
                severity="major",
                finding_type="missing_llm_sections",
                claim_ref=None,
                note="LLM analyst payload did not include a sections array.",
                required_action="Rerun the analyst stage or inspect llm/analyst.json.",
            ),
        )
    for index, section in enumerate(raw_sections):
        if not isinstance(section, dict):
            continue
        name = str(section.get("name") or "").strip()
        text = str(section.get("text") or "").strip()
        refs = tuple(
            str(ref).strip()
            for ref in section.get("evidence_refs", [])
            if str(ref).strip()
        )
        claim_ref = f"llm_section:{name or index}"
        if name not in _llm_section_fields() or not text or not refs:
            findings.append(
                ReviewFinding(
                    finding_id=build_artifact_id(run.run_id, "finding", claim_ref, "invalid"),
                    run_id=run.run_id,
                    severity="major",
                    finding_type="invalid_llm_section",
                    claim_ref=claim_ref,
                    evidence_refs=refs,
                    note=f"LLM section {name or index} was missing a supported name, text, or evidence refs.",
                    required_action="Regenerate the section with explicit evidence refs.",
                )
            )
            continue
        unsupported_refs = tuple(ref for ref in refs if ref not in evidence_ids)
        if unsupported_refs:
            findings.append(
                ReviewFinding(
                    finding_id=build_artifact_id(run.run_id, "finding", claim_ref, "unsupported_refs"),
                    run_id=run.run_id,
                    severity="major",
                    finding_type="unsupported_llm_section",
                    claim_ref=claim_ref,
                    evidence_refs=refs,
                    note=f"LLM section {name} referenced unknown evidence ids.",
                    required_action=f"Drop or repair refs: {', '.join(unsupported_refs)}.",
                )
            )
            continue
        sections[name] = _ensure_inline_refs(text, refs)
    return sections, tuple(findings)


def _skeptic_findings(
    *,
    run: MarketIntelRun,
    skeptic_payload: dict[str, Any] | None,
    evidence_ids: set[str],
) -> tuple[ReviewFinding, ...]:
    if skeptic_payload is None:
        return ()
    raw_findings = skeptic_payload.get("findings")
    if not isinstance(raw_findings, list):
        return (
            ReviewFinding(
                finding_id=build_artifact_id(run.run_id, "finding", "skeptic_findings_missing"),
                run_id=run.run_id,
                severity="major",
                finding_type="missing_skeptic_findings",
                claim_ref=None,
                note="LLM skeptic payload did not include a findings array.",
                required_action="Rerun the skeptic stage or inspect llm/skeptic.json.",
            ),
        )
    findings: list[ReviewFinding] = []
    for index, raw in enumerate(raw_findings):
        if not isinstance(raw, dict):
            continue
        refs = tuple(
            str(ref).strip()
            for ref in raw.get("evidence_refs", [])
            if str(ref).strip() and str(ref).strip() in evidence_ids
        )
        severity = str(raw.get("severity") or "note").strip()
        if severity not in {"blocker", "major", "minor", "note"}:
            severity = "note"
        note = str(raw.get("note") or "").strip()
        if not note:
            continue
        findings.append(
            ReviewFinding(
                finding_id=build_artifact_id(run.run_id, "finding", "skeptic", str(index)),
                run_id=run.run_id,
                severity=severity,  # type: ignore[arg-type]
                finding_type=str(raw.get("finding_type") or "skeptic_note").strip() or "skeptic_note",
                claim_ref=_optional_text(raw.get("claim_ref")),
                evidence_refs=refs,
                note=note,
                required_action=_optional_text(raw.get("required_action")),
            )
        )
    return tuple(findings)


def _llm_section_fields() -> set[str]:
    return {
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
    }


def _ensure_inline_refs(text: str, refs: tuple[str, ...]) -> str:
    missing_refs = [ref for ref in refs if f"[evidence:{ref}]" not in text]
    if not missing_refs:
        return text
    return f"{text} " + " ".join(f"[evidence:{ref}]" for ref in missing_refs)


def _optional_text(value: Any) -> str | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    return text or None


def _setup_summary(evidence: tuple[EvidenceItem, ...]) -> str | None:
    for item in evidence:
        if "identity" in item.tags:
            return _claim_with_ref(item)
    return _first_claim(evidence)


def _why_now_summary(evidence: tuple[EvidenceItem, ...]) -> str | None:
    for preferred_tag in ("filing", "price", "volume"):
        for item in evidence:
            if preferred_tag in item.tags:
                return _claim_with_ref(item)
    return _first_claim(evidence)


def _base_case_summary(evidence: tuple[EvidenceItem, ...]) -> str | None:
    market_items = [item for item in evidence if "market" in item.tags]
    if not market_items:
        return None
    return " ".join(_claim_with_ref(item) for item in market_items)


def _first_claim(evidence: tuple[EvidenceItem, ...]) -> str | None:
    if not evidence:
        return None
    return _claim_with_ref(evidence[0])


def _claim_with_ref(item: EvidenceItem) -> str:
    return f"{item.claim_text} [evidence:{item.evidence_id}]"


def _evidence_quality(
    *,
    evidence: tuple[EvidenceItem, ...],
    artifacts: tuple[SourceArtifact, ...],
) -> float:
    if not evidence or not artifacts:
        return 0.0
    trusted_artifacts = sum(1 for artifact in artifacts if artifact.trust_tier <= 2)
    confidence = sum(item.final_confidence for item in evidence) / len(evidence)
    source_score = trusted_artifacts / len(artifacts)
    return round(min(1.0, (confidence + source_score) / 2), 4)


def _render_thesis_markdown(
    *,
    run: MarketIntelRun,
    thesis: ThesisArtifact,
    evidence: tuple[EvidenceItem, ...],
    findings: tuple[ReviewFinding, ...],
) -> str:
    lines = [
        f"# Market Intel: {run.ticker}",
        "",
        f"- run_id: `{run.run_id}`",
        f"- as_of: `{run.as_of.isoformat()}`",
        f"- status: `{run.status}`",
        "",
        "Guardrail: every rendered claim below is sourced to an evidence id.",
        "",
    ]
    if thesis.setup:
        lines.extend(["## Setup", "", thesis.setup, ""])
    if thesis.why_now:
        lines.extend(["## Why Now", "", thesis.why_now, ""])
    if thesis.variant_view:
        lines.extend(["## Variant View", "", thesis.variant_view, ""])
    if thesis.base_case:
        lines.extend(["## Evidence-Backed Market Context", "", thesis.base_case, ""])
    if thesis.bull_case:
        lines.extend(["## Bull Case", "", thesis.bull_case, ""])
    if thesis.bear_case:
        lines.extend(["## Bear Case", "", thesis.bear_case, ""])
    if thesis.expected_window:
        lines.extend(["## Expected Window", "", thesis.expected_window, ""])
    if thesis.expected_return:
        lines.extend(["## Expected Return", "", thesis.expected_return, ""])
    if thesis.invalidation:
        lines.extend(["## Invalidation", "", thesis.invalidation, ""])
    if thesis.portfolio_fit:
        lines.extend(["## Portfolio Fit", "", thesis.portfolio_fit, ""])
    lines.extend(["## Evidence Claims", ""])
    if evidence:
        for item in evidence:
            lines.append(f"- `{item.evidence_id}`: {item.claim_text}")
    else:
        lines.append("- No evidence collected.")
    lines.extend(["", "## Skeptic Gate", ""])
    for finding in findings:
        lines.append(f"- {finding.severity}: {finding.note}")
    lines.append("")
    return "\n".join(lines)


def _render_review_markdown(
    *,
    run: MarketIntelRun,
    findings: tuple[ReviewFinding, ...],
) -> str:
    lines = [
        "# Skeptic Review",
        "",
        f"- run_id: `{run.run_id}`",
        f"- ticker: `{run.ticker}`",
        "",
        "## Findings",
        "",
    ]
    for finding in findings:
        lines.append(f"- `{finding.finding_id}` {finding.severity}/{finding.finding_type}: {finding.note}")
        if finding.required_action:
            lines.append(f"  Required action: {finding.required_action}")
    lines.append("")
    return "\n".join(lines)
