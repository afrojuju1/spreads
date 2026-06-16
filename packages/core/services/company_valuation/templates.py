from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

from core.services.company_valuation.contracts import (
    CompanyValuationTemplate,
    CompanyValuationTemplateOverride,
)
from core.services.trading_strategies import (
    _load_yaml_file,
    _yaml_directory_signature,
    default_config_root,
)


def _yaml_file_signature(path: Path) -> tuple[str, int, int] | None:
    if not path.exists():
        return None
    stat = path.stat()
    return (path.name, stat.st_mtime_ns, stat.st_size)


def _normalized_text(value: str | None) -> str:
    return " ".join(str(value or "").lower().split())


@dataclass(frozen=True)
class CompanyValuationTemplateAssignment:
    template: CompanyValuationTemplate
    source: str
    reason: str
    limited_coverage_flag: bool = False
    stressed_operator_flag: bool = False


def resolve_company_valuation_effective_template(
    *,
    issuer_row: dict[str, Any],
    config_root: str | Path | None = None,
) -> CompanyValuationTemplate:
    template_id = str(issuer_row["template_id"])
    if template_id == "energy_asset_heavy" and bool(issuer_row.get("stressed_operator_flag")):
        return resolve_company_valuation_template("stressed_operator", config_root)
    return resolve_company_valuation_template(template_id, config_root)


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
        template = CompanyValuationTemplate.model_validate(payload)
        if sum(template.quality_weight_map.values()) != 100:
            raise ValueError(f"quality_weight_map must sum to 100 for template {template.template_id}")
        if any(weight < 0 for weight in [*template.quality_weight_map.values(), *template.ownership_weight_map.values()]):
            raise ValueError(f"template weights must be non-negative for template {template.template_id}")
        if sum(template.ownership_weight_map.values()) > 15:
            raise ValueError(f"ownership_weight_map exceeds V1 cap for template {template.template_id}")
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
        overrides.append(CompanyValuationTemplateOverride.model_validate(item))
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
        stressed_operator_flag = bool(override.stressed_operator_flag or override.template_id == "stressed_operator")
        if stressed_operator_flag and override.template_id not in {
            "energy_asset_heavy",
            "stressed_operator",
        }:
            raise ValueError("stressed_operator_flag requires energy_asset_heavy as the base template")
        resolved_template_id = "energy_asset_heavy" if override.template_id == "stressed_operator" else override.template_id
        template = resolve_company_valuation_template(resolved_template_id, config_root)
        reason = override.reason
        if stressed_operator_flag:
            reason = f"{reason};stressed_operator_overlay"
        return CompanyValuationTemplateAssignment(
            template=template,
            source="issuer_override",
            reason=reason,
            stressed_operator_flag=stressed_operator_flag,
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
                if normalized_keyword and (normalized_keyword in name_text or normalized_keyword in sic_text):
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
    limited_coverage_flag = any(keyword in name_text or keyword in sic_text for keyword in limited_coverage_keywords)
    if limited_coverage_flag:
        reason = f"{reason};limited_coverage"
    return CompanyValuationTemplateAssignment(
        template=template,
        source=source,
        reason=reason,
        limited_coverage_flag=limited_coverage_flag,
        stressed_operator_flag=False,
    )
