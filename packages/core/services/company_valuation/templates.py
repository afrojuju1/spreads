from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

from core.services.company_valuation.contracts import (
    CompanyValuationTemplate,
    CompanyValuationTemplateOverride,
)
from core.services.strategy_configs import _load_yaml_file, _yaml_directory_signature, default_config_root


def _yaml_file_signature(path: Path) -> tuple[str, int, int] | None:
    if not path.exists():
        return None
    stat = path.stat()
    return (path.name, stat.st_mtime_ns, stat.st_size)


def _as_mapping(value: Any, *, field_name: str) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError(f"{field_name} must be a mapping")
    return dict(value)


def _as_text(value: Any, *, field_name: str) -> str:
    rendered = str(value or "").strip()
    if not rendered:
        raise ValueError(f"{field_name} is required")
    return rendered


def _as_text_tuple(value: Any, *, field_name: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list):
        raise ValueError(f"{field_name} must be a list")
    return tuple(str(item).strip() for item in value if str(item or "").strip())


def _as_weight_map(
    value: Any,
    *,
    field_name: str,
    require_sum: int | None = None,
) -> dict[str, int]:
    mapping = _as_mapping(value, field_name=field_name)
    weights: dict[str, int] = {}
    for key, raw in mapping.items():
        weights[str(key)] = int(raw)
    if require_sum is not None and sum(weights.values()) != require_sum:
        raise ValueError(f"{field_name} must sum to {require_sum}")
    if any(weight < 0 for weight in weights.values()):
        raise ValueError(f"{field_name} values must be non-negative")
    return weights


def _as_status(value: Any) -> str:
    rendered = _as_text(value, field_name="status")
    if rendered not in {"active", "inactive"}:
        raise ValueError("status must be active or inactive")
    return rendered


def _normalized_text(value: str | None) -> str:
    return " ".join(str(value or "").lower().split())


@dataclass(frozen=True)
class CompanyValuationTemplateAssignment:
    template: CompanyValuationTemplate
    source: str
    reason: str
    limited_coverage_flag: bool = False


def default_company_valuation_config_root(
    config_root: str | Path | None = None,
) -> Path:
    return default_config_root(config_root) / "company_valuation"


def _template_root(config_root: str | Path | None = None) -> Path:
    return default_company_valuation_config_root(config_root) / "templates"


def _override_path(config_root: str | Path | None = None) -> Path:
    return default_company_valuation_config_root(config_root) / "issuer_overrides.yaml"


@lru_cache(maxsize=8)
def _load_company_valuation_templates_cached(
    root_key: str,
    signature: tuple[tuple[str, int, int], ...],
) -> tuple[CompanyValuationTemplate, ...]:
    root = Path(root_key)
    templates: dict[str, CompanyValuationTemplate] = {}
    for path in sorted(root.glob("*.yaml")):
        payload = _load_yaml_file(path)
        template = CompanyValuationTemplate(
            template_id=_as_text(payload.get("template_id"), field_name="template_id"),
            template_version=_as_text(
                payload.get("template_version"),
                field_name="template_version",
            ),
            status=_as_status(payload.get("status")),  # type: ignore[arg-type]
            assignment_rules=_as_mapping(
                payload.get("assignment_rules"),
                field_name="assignment_rules",
            ),
            required_features=_as_text_tuple(
                payload.get("required_features"),
                field_name="required_features",
            ),
            optional_features=_as_text_tuple(
                payload.get("optional_features"),
                field_name="optional_features",
            ),
            quality_weight_map=_as_weight_map(
                payload.get("quality_weight_map"),
                field_name="quality_weight_map",
                require_sum=100,
            ),
            ownership_weight_map=_as_weight_map(
                payload.get("ownership_weight_map"),
                field_name="ownership_weight_map",
            ),
            valuation_model_mix=_as_mapping(
                payload.get("valuation_model_mix"),
                field_name="valuation_model_mix",
            ),
            confidence_rules=_as_mapping(
                payload.get("confidence_rules"),
                field_name="confidence_rules",
            ),
            risk_rules=_as_mapping(payload.get("risk_rules"), field_name="risk_rules"),
            unsupported_conditions=_as_text_tuple(
                payload.get("unsupported_conditions"),
                field_name="unsupported_conditions",
            ),
        )
        if sum(template.ownership_weight_map.values()) > 15:
            raise ValueError(
                f"ownership_weight_map exceeds V1 cap for template {template.template_id}"
            )
        if template.template_id in templates:
            raise ValueError(f"Duplicate template_id {template.template_id}")
        templates[template.template_id] = template
    return tuple(templates.values())


def load_company_valuation_templates(
    config_root: str | Path | None = None,
) -> dict[str, CompanyValuationTemplate]:
    root = _template_root(config_root)
    if not root.exists():
        return {}
    return {
        template.template_id: template
        for template in _load_company_valuation_templates_cached(
            str(root),
            _yaml_directory_signature(root),
        )
    }


def resolve_company_valuation_template(
    template_id: str,
    config_root: str | Path | None = None,
) -> CompanyValuationTemplate:
    templates = load_company_valuation_templates(config_root)
    try:
        return templates[template_id]
    except KeyError as exc:
        raise ValueError(f"Unknown company valuation template: {template_id}") from exc


@lru_cache(maxsize=8)
def _load_company_valuation_issuer_overrides_cached(
    path_key: str,
    signature: tuple[str, int, int] | None,
) -> tuple[CompanyValuationTemplateOverride, ...]:
    path = Path(path_key)
    if not path.exists():
        return ()
    payload = _load_yaml_file(path)
    raw_overrides = payload.get("overrides")
    if raw_overrides is None:
        return ()
    if not isinstance(raw_overrides, list):
        raise ValueError("overrides must be a list")
    overrides: list[CompanyValuationTemplateOverride] = []
    for item in raw_overrides:
        if not isinstance(item, dict):
            raise ValueError("override entries must be mappings")
        overrides.append(
            CompanyValuationTemplateOverride(
                issuer_cik=_as_text(item.get("issuer_cik"), field_name="issuer_cik"),
                template_id=_as_text(item.get("template_id"), field_name="template_id"),
                reason=_as_text(item.get("reason"), field_name="reason"),
                active=bool(item.get("active", True)),
            )
        )
    return tuple(overrides)


def load_company_valuation_issuer_overrides(
    config_root: str | Path | None = None,
) -> dict[str, CompanyValuationTemplateOverride]:
    path = _override_path(config_root)
    return {
        override.issuer_cik: override
        for override in _load_company_valuation_issuer_overrides_cached(
            str(path),
            _yaml_file_signature(path),
        )
    }


def resolve_company_valuation_template_assignment(
    *,
    cik: str,
    company_name: str,
    sic: str | None = None,
    sic_description: str | None = None,
    naics: str | None = None,
    config_root: str | Path | None = None,
) -> CompanyValuationTemplateAssignment:
    templates = load_company_valuation_templates(config_root)
    overrides = load_company_valuation_issuer_overrides(config_root)
    override = overrides.get(str(cik).zfill(10))
    if override is not None and override.active:
        template = resolve_company_valuation_template(override.template_id, config_root)
        return CompanyValuationTemplateAssignment(
            template=template,
            source="issuer_override",
            reason=override.reason,
        )

    name_text = _normalized_text(company_name)
    sic_text = _normalized_text(sic_description)
    naics_text = str(naics or "").strip()
    sic_code = str(sic or "").strip()
    best_match: CompanyValuationTemplate | None = None
    best_score = 0
    best_reason = ""
    for template in templates.values():
        if template.status != "active":
            continue
        rules = template.assignment_rules
        score = 0
        reasons: list[str] = []
        keyword_rules = rules.get("keyword_rules")
        if isinstance(keyword_rules, list):
            for keyword in keyword_rules:
                normalized_keyword = _normalized_text(str(keyword))
                if normalized_keyword and (
                    normalized_keyword in name_text or normalized_keyword in sic_text
                ):
                    score += 1
                    reasons.append(f"keyword:{normalized_keyword}")
        sic_prefixes = rules.get("sic_prefixes")
        if isinstance(sic_prefixes, list):
            for prefix in sic_prefixes:
                if sic_code and sic_code.startswith(str(prefix).strip()):
                    score += 2
                    reasons.append(f"sic_prefix:{prefix}")
        naics_prefixes = rules.get("naics_prefixes")
        if isinstance(naics_prefixes, list):
            for prefix in naics_prefixes:
                if naics_text and naics_text.startswith(str(prefix).strip()):
                    score += 2
                    reasons.append(f"naics_prefix:{prefix}")
        if score > best_score:
            best_match = template
            best_score = score
            best_reason = ",".join(reasons)

    template = best_match or resolve_company_valuation_template("general_operating", config_root)
    source = "rule_match" if best_score > 0 else "default"
    reason = best_reason or "default:general_operating"
    limited_coverage_keywords = ("bank", "insurance", "reit", "real estate investment trust")
    limited_coverage_flag = any(
        keyword in name_text or keyword in sic_text for keyword in limited_coverage_keywords
    )
    if limited_coverage_flag:
        reason = f"{reason};limited_coverage"
    return CompanyValuationTemplateAssignment(
        template=template,
        source=source,
        reason=reason,
        limited_coverage_flag=limited_coverage_flag,
    )
