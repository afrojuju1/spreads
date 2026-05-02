from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from core.services.market_intel.artifact_store import MarketIntelArtifactStore
from core.services.market_intel.config import MarketIntelModelConfig
from core.services.market_intel.contracts import (
    ModelCallRecord,
    ModelProfile,
    MarketIntelDepth,
    MarketIntelRun,
    utc_now,
)


@dataclass(frozen=True)
class ModelResponse:
    content: str
    raw_payload: dict[str, Any]
    model: str
    elapsed_seconds: float


class OllamaModelClient:
    def __init__(self, config: MarketIntelModelConfig) -> None:
        self.config = config
        self.base_url = config.ollama_base_url.rstrip("/")

    def invoke(
        self,
        *,
        profile: ModelProfile,
        messages: list[dict[str, str]],
        schema: dict[str, Any] | None = None,
        options: dict[str, Any] | None = None,
    ) -> ModelResponse:
        model = self.config.model_for_profile(profile)
        if not model:
            raise ValueError(f"No Ollama model configured for profile {profile}")
        payload: dict[str, Any] = {
            "model": model,
            "messages": messages,
            "stream": False,
            "keep_alive": (options or {}).get("keep_alive", "0s"),
        }
        ollama_options = {
            key: value
            for key, value in (options or {}).items()
            if key not in {"keep_alive"} and value is not None
        }
        if ollama_options:
            payload["options"] = ollama_options
        if schema is not None:
            payload["format"] = schema

        request = urllib.request.Request(
            f"{self.base_url}/api/chat",
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        started = time.monotonic()
        try:
            with urllib.request.urlopen(request, timeout=120) as response:
                raw = json.loads(response.read().decode("utf-8"))
        except urllib.error.URLError as exc:
            raise RuntimeError(f"Ollama request failed: {exc}") from exc
        elapsed = time.monotonic() - started
        message = dict(raw.get("message") or {})
        return ModelResponse(
            content=str(message.get("content") or ""),
            raw_payload=raw,
            model=model,
            elapsed_seconds=elapsed,
        )


class MarketIntelModelRouter:
    def __init__(
        self,
        *,
        config: MarketIntelModelConfig | None = None,
        artifact_store: MarketIntelArtifactStore | None = None,
        run: MarketIntelRun | None = None,
    ) -> None:
        self.config = config or MarketIntelModelConfig.from_env()
        self.client = OllamaModelClient(self.config)
        self.artifact_store = artifact_store
        self.run = run

    def route(
        self,
        *,
        agent_id: str,
        depth: MarketIntelDepth,
        input_size: int = 0,
        latency_budget: float | None = None,
    ) -> ModelProfile:
        del latency_budget
        if agent_id in {"SourcePlanner", "SectorRouter"}:
            return "fast_structured"
        if input_size > 16000:
            return "long_context"
        if agent_id == "SkepticReviewer" or depth == "deep":
            return "deep_reasoning"
        return "standard_reasoning"

    def invoke_text(
        self,
        *,
        agent_id: str,
        messages: list[dict[str, str]],
        depth: MarketIntelDepth = "standard",
        profile: ModelProfile | None = None,
        options: dict[str, Any] | None = None,
    ) -> ModelResponse:
        routed_profile = profile or self.route(
            agent_id=agent_id,
            depth=depth,
            input_size=sum(len(row.get("content", "")) for row in messages),
        )
        return self._invoke(
            agent_id=agent_id,
            profile=routed_profile,
            messages=messages,
            schema=None,
            options=options,
        )

    def invoke_structured(
        self,
        *,
        agent_id: str,
        schema: dict[str, Any],
        messages: list[dict[str, str]],
        depth: MarketIntelDepth = "standard",
        profile: ModelProfile | None = None,
        options: dict[str, Any] | None = None,
    ) -> ModelResponse:
        routed_profile = profile or self.route(
            agent_id=agent_id,
            depth=depth,
            input_size=sum(len(row.get("content", "")) for row in messages),
        )
        return self._invoke(
            agent_id=agent_id,
            profile=routed_profile,
            messages=messages,
            schema=schema,
            options=options,
        )

    def _invoke(
        self,
        *,
        agent_id: str,
        profile: ModelProfile,
        messages: list[dict[str, str]],
        schema: dict[str, Any] | None,
        options: dict[str, Any] | None,
    ) -> ModelResponse:
        call_id = f"model_call:{uuid4().hex}"
        started_at = utc_now()
        started = time.monotonic()
        model_name = self.config.model_for_profile(profile)
        try:
            response = self.client.invoke(
                profile=profile,
                messages=messages,
                schema=schema,
                options=options,
            )
        except Exception as exc:
            self._log_call(
                ModelCallRecord(
                    call_id=call_id,
                    run_id=None if self.run is None else self.run.run_id,
                    agent_id=agent_id,
                    backend="ollama",
                    profile=profile,
                    model=model_name,
                    started_at=started_at,
                    completed_at=utc_now(),
                    elapsed_seconds=round(time.monotonic() - started, 6),
                    status="failed",
                    error=str(exc),
                )
            )
            raise
        self._log_call(
            ModelCallRecord(
                call_id=call_id,
                run_id=None if self.run is None else self.run.run_id,
                agent_id=agent_id,
                backend="ollama",
                profile=profile,
                model=response.model,
                started_at=started_at,
                completed_at=utc_now(),
                elapsed_seconds=round(response.elapsed_seconds, 6),
                status="completed",
                token_estimate=sum(len(row.get("content", "")) for row in messages)
                // 4,
            )
        )
        return response

    def _log_call(self, record: ModelCallRecord) -> None:
        if self.artifact_store is None or self.run is None:
            return
        self.artifact_store.append_log(
            self.run,
            "model_call",
            record.to_payload(),
        )
