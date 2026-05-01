from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Any

from core.services.research_thesis.artifact_store import ResearchArtifactStore
from core.services.research_thesis.config import ResearchModelConfig
from core.services.research_thesis.contracts import (
    ResearchRequest,
    ResearchRun,
    ThesisArtifact,
    utc_now,
)
from core.services.research_thesis.ids import (
    build_config_hash,
    build_research_run_id,
    normalize_ticker,
)


def create_research_thesis_run(
    request: ResearchRequest,
    *,
    model_config: ResearchModelConfig | None = None,
) -> ResearchRun:
    model_config = model_config or ResearchModelConfig.from_env()
    ticker = normalize_ticker(request.ticker)
    started_at = utc_now()
    run_id = build_research_run_id(
        ticker=ticker,
        as_of=request.as_of,
        started_at=started_at,
    )
    store = ResearchArtifactStore(request.output_root)
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
    run = ResearchRun(
        run_id=run_id,
        ticker=ticker,
        as_of=request.as_of,
        status="created",
        config_hash=config_hash,
        started_at=started_at,
        output_root=request.output_root,
        run_dir=run_dir,
        warnings=(
            "source adapters are not implemented yet",
            "agent stages are not implemented yet",
        ),
    )
    store.create_run_tree(run)
    store.write_run(run)
    _write_initial_bundle(
        store=store,
        run=run,
        request=request,
        model_config=model_config,
    )
    store.append_log(
        run,
        "run_created",
        {
            "run_id": run.run_id,
            "ticker": run.ticker,
            "as_of": run.as_of.isoformat(),
            "status": run.status,
        },
    )
    store.append_log(
        run,
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
    return replace(run)


def _write_initial_bundle(
    *,
    store: ResearchArtifactStore,
    run: ResearchRun,
    request: ResearchRequest,
    model_config: ResearchModelConfig,
) -> None:
    store.write_json(
        run.run_dir / "sources.json",
        {
            "run_id": run.run_id,
            "enabled_sources": list(request.sources),
            "artifacts": [],
            "note": "source adapters are not implemented yet",
        },
    )
    store.write_json(
        run.run_dir / "evidence.json",
        {
            "run_id": run.run_id,
            "items": [],
            "note": "evidence extraction is not implemented yet",
        },
    )
    thesis = ThesisArtifact(
        run_id=run.run_id,
        ticker=run.ticker,
        as_of=run.as_of,
        skeptic_notes=("skeptic gate is not implemented yet",),
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


def _render_initial_thesis(run: ResearchRun) -> str:
    return (
        f"# Research Thesis: {run.ticker}\n\n"
        f"- run_id: `{run.run_id}`\n"
        f"- as_of: `{run.as_of.isoformat()}`\n"
        f"- status: `{run.status}`\n\n"
        "No thesis generated yet. This scaffold only creates the durable run "
        "bundle, model config snapshot, and log surface for later source and "
        "agent stages.\n"
    )


def run_summary_payload(run: ResearchRun) -> dict[str, Any]:
    return {
        "run_id": run.run_id,
        "ticker": run.ticker,
        "as_of": run.as_of.isoformat(),
        "status": run.status,
        "run_dir": str(Path(run.run_dir)),
        "warnings": list(run.warnings),
    }
