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
    utc_now,
)
from core.services.market_intel.agent_stages import (
    AgentStageResult,
    run_llm_agent_stages,
)
from core.services.market_intel.finalizer import (
    FinalizationResult,
    finalize_market_intel_run,
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
    warnings = [*source_result.warnings]
    agent_stage_result = AgentStageResult()
    if request.no_llm:
        warnings.append("LLM stages skipped by request")
    else:
        agent_stage_result = run_llm_agent_stages(
            request=request,
            run=replace(run, status="drafting_thesis"),
            store=store,
            model_config=model_config,
            artifacts=source_result.artifacts,
            evidence=source_result.evidence,
        )
        warnings.extend(agent_stage_result.warnings)
    candidate_run = replace(
        run,
        status="completed_with_warnings",
        completed_at=utc_now(),
        warnings=tuple(dict.fromkeys(warnings)),
    )
    finalization = finalize_market_intel_run(
        run=candidate_run,
        request=request,
        artifacts=source_result.artifacts,
        evidence=source_result.evidence,
        analyst_payload=agent_stage_result.analyst_payload,
        skeptic_payload=agent_stage_result.skeptic_payload,
    )
    final_run = replace(
        candidate_run,
        warnings=tuple(dict.fromkeys([*candidate_run.warnings, *finalization.warnings])),
    )
    store.write_run(final_run)
    _write_initial_bundle(
        store=store,
        run=final_run,
        request=request,
        model_config=model_config,
        artifacts=source_result.artifacts,
        evidence=source_result.evidence,
        agent_stage_result=agent_stage_result,
        finalization=finalization,
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
            "finding_count": len(finalization.findings),
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
    agent_stage_result: AgentStageResult,
    finalization: FinalizationResult,
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
    store.write_json(run.run_dir / "thesis.json", finalization.thesis.to_payload())
    store.write_text(run.run_dir / "thesis.md", finalization.thesis_markdown)
    store.write_json(
        run.run_dir / "agent_stages.json",
        {
            "run_id": run.run_id,
            "analyst_ran": agent_stage_result.analyst_payload is not None,
            "skeptic_ran": agent_stage_result.skeptic_payload is not None,
            "warnings": list(agent_stage_result.warnings),
        },
    )
    store.write_json(
        run.run_dir / "review.json",
        {
            "run_id": run.run_id,
            "findings": [finding.to_payload() for finding in finalization.findings],
        },
    )
    store.write_text(run.run_dir / "review.md", finalization.review_markdown)
    store.write_json(
        run.run_dir / "model_config.json",
        {
            "run_id": run.run_id,
            "models": model_config.to_payload(),
        },
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
