from __future__ import annotations

from typing import Any

from temporalio.converter import DataConverter

TEMPORAL_WIRE_PAYLOAD_LIMIT_BYTES = 64 * 1024
TEMPORAL_WORKFLOW_INPUT_LIMIT_BYTES = 60 * 1024


def temporal_payload_size(value: Any) -> int:
    payload = DataConverter.default.payload_converter.to_payload(value)
    return payload.ByteSize()


def require_temporal_payload_budget(
    value: Any,
    *,
    label: str,
    limit_bytes: int = TEMPORAL_WIRE_PAYLOAD_LIMIT_BYTES,
) -> int:
    payload_size = temporal_payload_size(value)
    if payload_size > limit_bytes:
        raise ValueError(
            f"{label} encodes to {payload_size} bytes, exceeding the {limit_bytes}-byte Temporal wire budget."
        )
    return payload_size


__all__ = [
    "TEMPORAL_WIRE_PAYLOAD_LIMIT_BYTES",
    "TEMPORAL_WORKFLOW_INPUT_LIMIT_BYTES",
    "require_temporal_payload_budget",
    "temporal_payload_size",
]
