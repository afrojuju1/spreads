from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Any

from core.services.market_intel.artifact_store import MarketIntelArtifactStore
from core.services.market_intel.config import MarketIntelModelConfig
from core.services.market_intel.contracts import (
    EvidenceItem,
    MarketIntelRequest,
    MarketIntelRun,
    SourceArtifact,
    ThesisArtifact,
    utc_now,
)
from core.services.market_intel.ids import (
    build_config_hash,
    build_market_intel_run_id,
    normalize_ticker,
)
from core.services.market_intel.source_adapters import collect_sources


def create_market_intel_run(
    request: MarketIntelRequest,
    *,
    model_config: MarketIntelModelConfig | None = None,
) -> MarketIntelRun:
    model_config = model_config or MarketIntelModelConfig.from_env()
    ticker = normalize_ticker(request.ticker)
    started_at = utc_now()
    run_id = build_market_intel_run_id(
        ticker=ticker,
        as_of=request.as_of,
        started_at=started_at,
    )
    store = MarketIntelArtifactStore(request.output_root)
    run_dir = store.run_dir(
        ticker=ticker,
        as_of=request.as_of.isoformat(),
        run_id=run_id,
    )
    config_hash = build_config_hash(
        {
            "request": request.to_payload(),
            "models": model_config.to_payload(),
        }
    )
    run = MarketIntelRun(
        run_id=run_id,
        ticker=ticker,
        as_of=request.as_of,
        status="fetching_sources",
        config_hash=config_hash,
        started_at=started_at,
        output_root=request.output_root,
        run_dir=run_dir,
    )
    store.create_run_tree(run)
    store.write_run(run)
    store.append_agent_trace(
        run,
        "run_created",
        {
            "run_id": run.run_id,
            "ticker": run.ticker,
            "as_of": run.as_of.isoformat(),
            "sources": list(request.sources),
        },
    )
    store.append_hook_trace(
        run,
        "engine_run_started",
        {
            "run_id": run.run_id,
            "ticker": run.ticker,
            "sources": list(request.sources),
            "no_llm": request.no_llm,
        },
    )
    source_result = collect_sources(request, run=run, store=store)
    warnings = [
        *source_result.warnings,
        "agent stages are not implemented yet",
    ]
    if request.no_llm:
        warnings.append("LLM stages skipped by request")
    final_run = replace(
        run,
        status="completed_with_warnings",
        completed_at=utc_now(),
        warnings=tuple(dict.fromkeys(warnings)),
    )
    store.write_run(final_run)
    _write_initial_bundle(
        store=store,
        run=final_run,
        request=request,
        model_config=model_config,
        artifacts=source_result.artifacts,
        evidence=source_result.evidence,
    )
    store.append_log(
        final_run,
        "run_created",
        {
            "run_id": final_run.run_id,
            "ticker": final_run.ticker,
            "as_of": final_run.as_of.isoformat(),
            "status": final_run.status,
        },
    )
    store.append_log(
        final_run,
        "model_config_loaded",
        {
            "ollama_base_url": model_config.ollama_base_url,
            "max_llm_concurrency": model_config.max_llm_concurrency,
            "fast_structured_model": model_config.fast_structured_model,
            "standard_reasoning_model": model_config.standard_reasoning_model,
            "deep_reasoning_model": model_config.deep_reasoning_model,
            "long_context_model": model_config.long_context_model,
            "embedding_model": model_config.embedding_model,
        },
    )
    store.append_agent_trace(
        final_run,
        "run_completed",
        {
            "status": final_run.status,
            "artifact_count": len(source_result.artifacts),
            "evidence_count": len(source_result.evidence),
            "warning_count": len(final_run.warnings),
        },
    )
    store.append_hook_trace(
        final_run,
        "engine_run_finalized",
        {
            "run_id": final_run.run_id,
            "status": final_run.status,
            "artifact_count": len(source_result.artifacts),
            "evidence_count": len(source_result.evidence),
        },
    )
    return replace(final_run)


def _write_initial_bundle(
    *,
    store: MarketIntelArtifactStore,
    run: MarketIntelRun,
    request: MarketIntelRequest,
    model_config: MarketIntelModelConfig,
    artifacts: tuple[SourceArtifact, ...],
    evidence: tuple[EvidenceItem, ...],
) -> None:
    store.write_json(
        run.run_dir / "sources.json",
        {
            "run_id": run.run_id,
            "enabled_sources": list(request.sources),
            "artifacts": [artifact.to_payload() for artifact in artifacts],
            "warnings": list(run.warnings),
        },
    )
    store.write_json(
        run.run_dir / "evidence.json",
        {
            "run_id": run.run_id,
            "items": [item.to_payload() for item in evidence],
            "warnings": list(run.warnings),
        },
    )
    thesis = ThesisArtifact(
        run_id=run.run_id,
        ticker=run.ticker,
        as_of=run.as_of,
        skeptic_notes=("skeptic gate is not implemented yet",),
        core_evidence=tuple(item.evidence_id for item in evidence),
        evidence_quality=0.0 if not evidence else min(1.0, len(evidence) / 4),
        source_pack=tuple(artifact.artifact_id for artifact in artifacts),
    )
    store.write_json(run.run_dir / "thesis.json", thesis.to_payload())
    store.write_text(run.run_dir / "thesis.md", _render_initial_thesis(run))
    store.write_text(
        run.run_dir / "review.md",
        "# Skeptic Review\n\nNo skeptic review generated yet.\n",
    )
    store.write_json(
        run.run_dir / "model_config.json",
        {
            "run_id": run.run_id,
            "models": model_config.to_payload(),
        },
    )


def _render_initial_thesis(run: MarketIntelRun) -> str:
    return (
        f"# Market Intel: {run.ticker}\n\n"
        f"- run_id: `{run.run_id}`\n"
        f"- as_of: `{run.as_of.isoformat()}`\n"
        f"- status: `{run.status}`\n\n"
        "No thesis generated yet. The run now collects source artifacts and "
        "initial evidence, but the agent drafting and skeptic stages are still "
        "pending.\n"
    )


def run_summary_payload(run: MarketIntelRun) -> dict[str, Any]:
    return {
        "run_id": run.run_id,
        "ticker": run.ticker,
        "as_of": run.as_of.isoformat(),
        "status": run.status,
        "run_dir": str(Path(run.run_dir)),
        "warnings": list(run.warnings),
    }
