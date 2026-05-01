from __future__ import annotations

import os
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from core.services.research_thesis.contracts import ModelProfile


DEFAULT_OLLAMA_BASE_URL = "http://localhost:11434"
DEFAULT_OUTPUT_ROOT = Path("outputs/research_thesis")


@dataclass(frozen=True)
class ResearchModelConfig:
    ollama_base_url: str = DEFAULT_OLLAMA_BASE_URL
    max_llm_concurrency: int = 1
    fast_structured_model: str = "qwen3:8b"
    standard_reasoning_model: str = "glm-4.7-flash:latest"
    deep_reasoning_model: str = "glm-4.7-flash:latest"
    long_context_model: str = "glm-4.7-flash:latest"
    embedding_model: str | None = None

    @classmethod
    def from_env(cls) -> "ResearchModelConfig":
        return cls(
            ollama_base_url=_env_text(
                "RESEARCH_THESIS_OLLAMA_BASE_URL", DEFAULT_OLLAMA_BASE_URL
            ),
            max_llm_concurrency=_env_int("RESEARCH_THESIS_LLM_MAX_CONCURRENCY", 1),
            fast_structured_model=_env_text(
                "RESEARCH_THESIS_MODEL_FAST_STRUCTURED", "qwen3:8b"
            ),
            standard_reasoning_model=_env_text(
                "RESEARCH_THESIS_MODEL_STANDARD_REASONING",
                "glm-4.7-flash:latest",
            ),
            deep_reasoning_model=_env_text(
                "RESEARCH_THESIS_MODEL_DEEP_REASONING",
                "glm-4.7-flash:latest",
            ),
            long_context_model=_env_text(
                "RESEARCH_THESIS_MODEL_LONG_CONTEXT",
                "glm-4.7-flash:latest",
            ),
            embedding_model=_env_optional_text("RESEARCH_THESIS_MODEL_EMBEDDING"),
        )

    def model_for_profile(self, profile: ModelProfile) -> str | None:
        if profile == "fast_structured":
            return self.fast_structured_model
        if profile == "standard_reasoning":
            return self.standard_reasoning_model
        if profile == "deep_reasoning":
            return self.deep_reasoning_model
        if profile == "long_context":
            return self.long_context_model
        if profile == "embedding":
            return self.embedding_model
        raise ValueError(f"Unsupported model profile: {profile}")

    def to_payload(self) -> dict[str, Any]:
        return asdict(self)


def resolve_output_root(value: str | Path | None = None) -> Path:
    raw = Path(value) if value not in (None, "") else DEFAULT_OUTPUT_ROOT
    return raw.expanduser()


def _env_text(name: str, default: str) -> str:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    return value.strip()


def _env_optional_text(name: str) -> str | None:
    value = os.getenv(name)
    if value is None or not value.strip():
        return None
    return value.strip()


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    try:
        parsed = int(value)
    except ValueError:
        return default
    return max(1, parsed)
