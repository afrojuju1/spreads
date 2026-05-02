from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Any

from core.services.market_intel.artifact_store import MarketIntelArtifactStore
from core.services.market_intel.config import MarketIntelModelConfig
from core.services.market_intel.contracts import (
    MarketIntelRequest,
    MarketIntelRun,
    ThesisArtifact,
    utc_now,
)
from core.services.market_intel.ids import (
    build_config_hash,
    build_market_intel_run_id,
    normalize_ticker,
)


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
    store: MarketIntelArtifactStore,
    run: MarketIntelRun,
    request: MarketIntelRequest,
    model_config: MarketIntelModelConfig,
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


def _render_initial_thesis(run: MarketIntelRun) -> str:
    return (
        f"# Market Intel: {run.ticker}\n\n"
        f"- run_id: `{run.run_id}`\n"
        f"- as_of: `{run.as_of.isoformat()}`\n"
        f"- status: `{run.status}`\n\n"
        "No thesis generated yet. This scaffold only creates the durable run "
        "bundle, model config snapshot, and log surface for later source and "
        "agent stages.\n"
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
