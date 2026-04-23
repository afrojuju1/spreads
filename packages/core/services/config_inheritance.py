from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

EXTENDS_KEY = "extends"


def load_yaml_mapping(path: Path) -> dict[str, Any]:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"Expected mapping payload in {path}")
    return raw


def as_required_text(value: Any, *, field_name: str) -> str:
    rendered = str(value or "").strip()
    if not rendered:
        raise ValueError(f"{field_name} is required")
    return rendered


def as_mapping(value: Any, *, field_name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{field_name} must be a mapping")
    return dict(value)


def merge_mappings(
    base: dict[str, Any],
    override: dict[str, Any],
) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if key == EXTENDS_KEY:
            continue
        existing = merged.get(key)
        if (
            isinstance(existing, dict)
            and isinstance(value, dict)
            and EXTENDS_KEY not in value
        ):
            merged[key] = merge_mappings(existing, value)
        else:
            merged[key] = value
    return merged


def _policy_base_path(
    *,
    config_root: Path,
    policy_kind: str,
    ref: str,
    field_name: str,
    config_path: Path,
) -> Path:
    relative = Path(ref)
    if relative.is_absolute() or relative.suffix or ".." in relative.parts:
        raise ValueError(
            f"{field_name}.extends in {config_path} must name a policy under "
            f"policies/{policy_kind}"
        )
    if len(relative.parts) != 1:
        raise ValueError(
            f"{field_name}.extends in {config_path} must be a single policy name"
        )
    return config_root / "policies" / policy_kind / f"{ref}.yaml"


def resolve_policy_mapping(
    value: Any,
    *,
    field_name: str,
    policy_kind: str,
    config_root: Path,
    config_path: Path,
    seen: frozenset[Path] = frozenset(),
) -> dict[str, Any]:
    mapping = as_mapping(value, field_name=field_name)
    extends = mapping.get(EXTENDS_KEY)
    if extends in (None, ""):
        return {
            key: policy_value
            for key, policy_value in mapping.items()
            if key != EXTENDS_KEY
        }

    ref = as_required_text(extends, field_name=f"{field_name}.{EXTENDS_KEY}")
    base_path = _policy_base_path(
        config_root=config_root,
        policy_kind=policy_kind,
        ref=ref,
        field_name=field_name,
        config_path=config_path,
    )
    if base_path in seen:
        raise ValueError(f"Cycle detected while resolving {field_name}.extends")
    if not base_path.exists():
        raise FileNotFoundError(
            f"{field_name}.extends references missing policy {base_path}"
        )

    base_mapping = resolve_policy_mapping(
        load_yaml_mapping(base_path),
        field_name=f"{policy_kind}_policy:{ref}",
        policy_kind=policy_kind,
        config_root=config_root,
        config_path=base_path,
        seen=seen | frozenset({base_path}),
    )
    return merge_mappings(base_mapping, mapping)


__all__ = [
    "EXTENDS_KEY",
    "as_mapping",
    "as_required_text",
    "load_yaml_mapping",
    "merge_mappings",
    "resolve_policy_mapping",
]
