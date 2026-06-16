from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from core.services.company_valuation.contracts import (
    CompanyValuationCanonicalTaxonomy,
    CompanyValuationDefaultTemplateResolution,
    CompanyValuationOverlayResolution,
    CompanyValuationOverlayRule,
    CompanyValuationRawClassification,
    CompanyValuationSupportPolicy,
    CompanyValuationSupportResolution,
    CompanyValuationSupportedIssuer,
    CompanyValuationTaxonomyMapping,
    CompanyValuationTaxonomyNode,
    CompanyValuationTaxonomyOverride,
    CompanyValuationTaxonomyResolution,
    CompanyValuationTemplateMapping,
)
from core.services.company_valuation.ids import normalize_cik, normalize_ticker
from core.services.company_valuation.templates import (
    default_company_valuation_config_root,
    resolve_company_valuation_template,
)
from core.services.trading_strategies import (
    _load_yaml_file,
    _yaml_file_signature,
)
from core.value_coercion import as_text, coerce_bool


def _normalized_text(value: str | None) -> str:
    return " ".join(str(value or "").lower().split())


def default_company_valuation_taxonomy_root(
    config_root: str | Path | None = None,
) -> Path:
    return default_company_valuation_config_root(config_root) / "taxonomy"


def _taxonomy_nodes_path(config_root: str | Path | None = None) -> Path:
    return default_company_valuation_taxonomy_root(config_root) / "nodes.yaml"


def _taxonomy_mappings_path(config_root: str | Path | None = None) -> Path:
    return default_company_valuation_taxonomy_root(config_root) / "mappings.yaml"


def _template_mappings_path(config_root: str | Path | None = None) -> Path:
    return default_company_valuation_taxonomy_root(config_root) / "template_mappings.yaml"


def _overlay_rules_path(config_root: str | Path | None = None) -> Path:
    return default_company_valuation_taxonomy_root(config_root) / "overlay_rules.yaml"


def _taxonomy_overrides_path(config_root: str | Path | None = None) -> Path:
    return default_company_valuation_taxonomy_root(config_root) / "issuer_taxonomy_overrides.yaml"


def _support_policy_path(config_root: str | Path | None = None) -> Path:
    return default_company_valuation_taxonomy_root(config_root) / "support_policy.yaml"


@lru_cache(maxsize=8)
def _load_company_valuation_taxonomy_nodes_cached(
    path_key: str,
    signature: tuple[str, int, int] | None,
) -> tuple[CompanyValuationTaxonomyNode, ...]:
    path = Path(path_key)
    if not path.exists():
        return ()
    payload = _load_yaml_file(path)
    taxonomy_version = as_text(payload.get("taxonomy_version"))
    if taxonomy_version is None:
        raise ValueError("taxonomy_version is required")
    raw_nodes = payload.get("nodes")
    if raw_nodes is None:
        return ()
    if not isinstance(raw_nodes, list):
        raise ValueError("nodes must be a list")
    nodes: list[CompanyValuationTaxonomyNode] = []
    for item in raw_nodes:
        if not isinstance(item, dict):
            raise ValueError("taxonomy node entries must be mappings")
        nodes.append(CompanyValuationTaxonomyNode.model_validate({**item, "taxonomy_version": item.get("taxonomy_version") or taxonomy_version}))
    by_id = {node.taxonomy_node_id: node for node in nodes}
    if len(by_id) != len(nodes):
        raise ValueError(f"Duplicate taxonomy_node_id in {path}")
    for node in nodes:
        if node.parent_taxonomy_node_id is not None and node.parent_taxonomy_node_id not in by_id:
            raise ValueError(f"Unknown parent_taxonomy_node_id {node.parent_taxonomy_node_id}")
    return tuple(nodes)


def load_company_valuation_taxonomy_nodes(
    config_root: str | Path | None = None,
) -> dict[str, CompanyValuationTaxonomyNode]:
    path = _taxonomy_nodes_path(config_root)
    return {
        node.taxonomy_node_id: node
        for node in _load_company_valuation_taxonomy_nodes_cached(
            str(path),
            _yaml_file_signature(path),
        )
    }


@lru_cache(maxsize=8)
def _load_company_valuation_taxonomy_mappings_cached(
    path_key: str,
    signature: tuple[str, int, int] | None,
) -> tuple[CompanyValuationTaxonomyMapping, ...]:
    path = Path(path_key)
    if not path.exists():
        return ()
    payload = _load_yaml_file(path)
    mapping_version = as_text(payload.get("mapping_version"))
    if mapping_version is None:
        raise ValueError("mapping_version is required")
    raw_mappings = payload.get("mappings")
    if raw_mappings is None:
        return ()
    if not isinstance(raw_mappings, list):
        raise ValueError("mappings must be a list")
    mappings: list[CompanyValuationTaxonomyMapping] = []
    for item in raw_mappings:
        if not isinstance(item, dict):
            raise ValueError("taxonomy mapping entries must be mappings")
        mappings.append(CompanyValuationTaxonomyMapping.model_validate({**item, "mapping_version": item.get("mapping_version") or mapping_version}))
    by_id = {mapping.mapping_id: mapping for mapping in mappings}
    if len(by_id) != len(mappings):
        raise ValueError(f"Duplicate mapping_id in {path}")
    return tuple(mappings)


def load_company_valuation_taxonomy_mappings(
    config_root: str | Path | None = None,
) -> tuple[CompanyValuationTaxonomyMapping, ...]:
    path = _taxonomy_mappings_path(config_root)
    mappings = _load_company_valuation_taxonomy_mappings_cached(
        str(path),
        _yaml_file_signature(path),
    )
    nodes = load_company_valuation_taxonomy_nodes(config_root)
    for mapping in mappings:
        for node_id in (
            mapping.canonical_sector_id,
            mapping.canonical_industry_group_id,
            mapping.canonical_industry_id,
            mapping.canonical_subindustry_id,
        ):
            if node_id is not None and node_id not in nodes:
                raise ValueError(f"Unknown taxonomy node reference {node_id} in {path}")
    return mappings


@lru_cache(maxsize=8)
def _load_company_valuation_template_mappings_cached(
    path_key: str,
    signature: tuple[str, int, int] | None,
) -> tuple[CompanyValuationTemplateMapping, ...]:
    path = Path(path_key)
    if not path.exists():
        return ()
    payload = _load_yaml_file(path)
    mapping_version = as_text(payload.get("mapping_version"))
    if mapping_version is None:
        raise ValueError("mapping_version is required")
    raw_mappings = payload.get("mappings")
    if raw_mappings is None:
        return ()
    if not isinstance(raw_mappings, list):
        raise ValueError("mappings must be a list")
    mappings: list[CompanyValuationTemplateMapping] = []
    for item in raw_mappings:
        if not isinstance(item, dict):
            raise ValueError("template mapping entries must be mappings")
        mappings.append(CompanyValuationTemplateMapping.model_validate({**item, "mapping_version": item.get("mapping_version") or mapping_version}))
    by_node_id = {mapping.taxonomy_node_id: mapping for mapping in mappings if mapping.active}
    if len(by_node_id) != len([mapping for mapping in mappings if mapping.active]):
        raise ValueError(f"Duplicate active taxonomy_node_id in {path}")
    return tuple(mappings)


def load_company_valuation_template_mappings(
    config_root: str | Path | None = None,
) -> tuple[CompanyValuationTemplateMapping, ...]:
    path = _template_mappings_path(config_root)
    mappings = _load_company_valuation_template_mappings_cached(
        str(path),
        _yaml_file_signature(path),
    )
    nodes = load_company_valuation_taxonomy_nodes(config_root)
    for mapping in mappings:
        if mapping.taxonomy_node_id not in nodes:
            raise ValueError(f"Unknown taxonomy node reference {mapping.taxonomy_node_id} in {path}")
    return mappings


@lru_cache(maxsize=8)
def _load_company_valuation_overlay_rules_cached(
    path_key: str,
    signature: tuple[str, int, int] | None,
) -> tuple[CompanyValuationOverlayRule, ...]:
    path = Path(path_key)
    if not path.exists():
        return ()
    payload = _load_yaml_file(path)
    rule_version = as_text(payload.get("rule_version"))
    if rule_version is None:
        raise ValueError("rule_version is required")
    raw_rules = payload.get("rules")
    if raw_rules is None:
        return ()
    if not isinstance(raw_rules, list):
        raise ValueError("rules must be a list")
    rules: list[CompanyValuationOverlayRule] = []
    for item in raw_rules:
        if not isinstance(item, dict):
            raise ValueError("overlay rule entries must be mappings")
        rules.append(CompanyValuationOverlayRule.model_validate({**item, "rule_version": item.get("rule_version") or rule_version}))
    by_id = {rule.rule_id: rule for rule in rules}
    if len(by_id) != len(rules):
        raise ValueError(f"Duplicate rule_id in {path}")
    return tuple(rules)


def load_company_valuation_overlay_rules(
    config_root: str | Path | None = None,
) -> tuple[CompanyValuationOverlayRule, ...]:
    path = _overlay_rules_path(config_root)
    return _load_company_valuation_overlay_rules_cached(
        str(path),
        _yaml_file_signature(path),
    )


@lru_cache(maxsize=8)
def _load_company_valuation_taxonomy_overrides_cached(
    path_key: str,
    signature: tuple[str, int, int] | None,
) -> tuple[CompanyValuationTaxonomyOverride, ...]:
    path = Path(path_key)
    if not path.exists():
        return ()
    payload = _load_yaml_file(path)
    raw_overrides = payload.get("overrides")
    if raw_overrides is None:
        return ()
    if not isinstance(raw_overrides, list):
        raise ValueError("overrides must be a list")
    overrides: list[CompanyValuationTaxonomyOverride] = []
    for item in raw_overrides:
        if not isinstance(item, dict):
            raise ValueError("taxonomy override entries must be mappings")
        overrides.append(CompanyValuationTaxonomyOverride.model_validate({**item, "issuer_cik": normalize_cik(item.get("issuer_cik"))}))
    by_cik = {override.issuer_cik: override for override in overrides}
    if len(by_cik) != len(overrides):
        raise ValueError(f"Duplicate issuer_cik in {path}")
    return tuple(overrides)


def load_company_valuation_taxonomy_overrides(
    config_root: str | Path | None = None,
) -> dict[str, CompanyValuationTaxonomyOverride]:
    path = _taxonomy_overrides_path(config_root)
    return {
        override.issuer_cik: override
        for override in _load_company_valuation_taxonomy_overrides_cached(
            str(path),
            _yaml_file_signature(path),
        )
    }


@lru_cache(maxsize=8)
def _load_company_valuation_support_policy_cached(
    path_key: str,
    signature: tuple[str, int, int] | None,
) -> CompanyValuationSupportPolicy:
    path = Path(path_key)
    if not path.exists():
        return CompanyValuationSupportPolicy(policy_version="v1")
    payload = _load_yaml_file(path)
    policy_version = as_text(payload.get("policy_version"))
    if policy_version is None:
        raise ValueError("policy_version is required")
    raw_supported_template_ids = payload.get("supported_template_ids")
    if raw_supported_template_ids is None:
        supported_template_ids: tuple[str, ...] = ()
    elif not isinstance(raw_supported_template_ids, list):
        raise ValueError("supported_template_ids must be a list")
    else:
        supported_template_ids = tuple(str(item).strip() for item in raw_supported_template_ids if str(item or "").strip())
    raw_supported_issuers = payload.get("supported_issuers")
    if raw_supported_issuers is None:
        supported_issuers: list[CompanyValuationSupportedIssuer] = []
    else:
        if not isinstance(raw_supported_issuers, list):
            raise ValueError("supported_issuers must be a list")
        supported_issuers = []
        for item in raw_supported_issuers:
            if not isinstance(item, dict):
                raise ValueError("supported_issuer entries must be mappings")
            supported_issuers.append(
                CompanyValuationSupportedIssuer.model_validate({**item, "ticker": normalize_ticker(item.get("ticker"))})
            )
    by_ticker = {issuer.ticker: issuer for issuer in supported_issuers if issuer.active}
    if len(by_ticker) != len([issuer for issuer in supported_issuers if issuer.active]):
        raise ValueError(f"Duplicate active supported issuer ticker in {path}")
    return CompanyValuationSupportPolicy(
        policy_version=policy_version,
        allowlist_required=bool(coerce_bool(payload.get("allowlist_required"), default=True)),
        supported_template_ids=supported_template_ids,
        supported_issuers=tuple(supported_issuers),
    )


def load_company_valuation_support_policy(
    config_root: str | Path | None = None,
) -> CompanyValuationSupportPolicy:
    path = _support_policy_path(config_root)
    return _load_company_valuation_support_policy_cached(
        str(path),
        _yaml_file_signature(path),
    )


def supported_company_valuation_tickers(
    config_root: str | Path | None = None,
) -> tuple[str, ...]:
    policy = load_company_valuation_support_policy(config_root)
    return tuple(issuer.ticker for issuer in policy.supported_issuers if issuer.active)


def resolve_company_valuation_raw_classification(
    *,
    sic: str | None = None,
    sic_title: str | None = None,
    naics: str | None = None,
    naics_title: str | None = None,
) -> CompanyValuationRawClassification:
    return CompanyValuationRawClassification(
        sic_code=as_text(sic),
        sic_title=as_text(sic_title),
        naics_code=as_text(naics),
        naics_title=as_text(naics_title),
    )


def _active_taxonomy_version(
    config_root: str | Path | None = None,
) -> str:
    nodes = load_company_valuation_taxonomy_nodes(config_root)
    if not nodes:
        return "v1"
    return next(iter(nodes.values())).taxonomy_version


def _mapping_matches(
    mapping: CompanyValuationTaxonomyMapping,
    raw: CompanyValuationRawClassification,
) -> bool:
    if not mapping.active:
        return False
    candidate_code = raw.sic_code if mapping.source_standard == "sic" else raw.naics_code
    if not candidate_code:
        return False
    if mapping.match_mode == "exact":
        return candidate_code == mapping.source_code
    return candidate_code.startswith(mapping.source_code)


def _mapping_confidence(mapping: CompanyValuationTaxonomyMapping) -> float:
    if mapping.source_standard == "naics":
        return 0.95 if mapping.match_mode == "exact" else 0.9
    return 0.85 if mapping.match_mode == "exact" else 0.75


def _mapping_sort_key(
    mapping: CompanyValuationTaxonomyMapping,
) -> tuple[int, int, int, int]:
    source_rank = 2 if mapping.source_standard == "naics" else 1
    match_rank = 2 if mapping.match_mode == "exact" else 1
    return (source_rank, match_rank, len(mapping.source_code), mapping.priority)


def resolve_company_valuation_canonical_taxonomy(
    *,
    cik: str,
    raw_classification: CompanyValuationRawClassification,
    config_root: str | Path | None = None,
) -> CompanyValuationCanonicalTaxonomy:
    taxonomy_version = _active_taxonomy_version(config_root)
    override = load_company_valuation_taxonomy_overrides(config_root).get(normalize_cik(cik))
    if override is not None and override.active:
        return CompanyValuationCanonicalTaxonomy(
            taxonomy_version=taxonomy_version,
            canonical_sector_id=override.canonical_sector_id,
            canonical_industry_group_id=override.canonical_industry_group_id,
            canonical_industry_id=override.canonical_industry_id,
            canonical_subindustry_id=override.canonical_subindustry_id,
            classification_source="issuer_override",
            classification_confidence=1.0,
            source_standard="issuer_override",
            reason=override.reason,
        )

    mappings = load_company_valuation_taxonomy_mappings(config_root)
    candidates = [mapping for mapping in mappings if _mapping_matches(mapping, raw_classification)]
    if not candidates:
        return CompanyValuationCanonicalTaxonomy(
            taxonomy_version=taxonomy_version,
            classification_source="unclassified",
            classification_confidence=0.0,
            reason="no taxonomy mapping matched raw SIC/NAICS",
        )

    best_mapping = max(candidates, key=_mapping_sort_key)
    return CompanyValuationCanonicalTaxonomy(
        taxonomy_version=taxonomy_version,
        canonical_sector_id=best_mapping.canonical_sector_id,
        canonical_industry_group_id=best_mapping.canonical_industry_group_id,
        canonical_industry_id=best_mapping.canonical_industry_id,
        canonical_subindustry_id=best_mapping.canonical_subindustry_id,
        classification_source=f"{best_mapping.source_standard}:{best_mapping.match_mode}",
        classification_confidence=_mapping_confidence(best_mapping),
        source_standard=best_mapping.source_standard,
        mapping_id=best_mapping.mapping_id,
        mapping_version=best_mapping.mapping_version,
        reason=best_mapping.notes or "",
    )


def resolve_company_valuation_default_template(
    canonical_taxonomy: CompanyValuationCanonicalTaxonomy,
    config_root: str | Path | None = None,
) -> CompanyValuationDefaultTemplateResolution:
    mappings = {mapping.taxonomy_node_id: mapping for mapping in load_company_valuation_template_mappings(config_root) if mapping.active}
    candidate_node_ids = (
        canonical_taxonomy.canonical_subindustry_id,
        canonical_taxonomy.canonical_industry_id,
        canonical_taxonomy.canonical_industry_group_id,
        canonical_taxonomy.canonical_sector_id,
    )
    for node_id in candidate_node_ids:
        if not node_id:
            continue
        mapping = mappings.get(node_id)
        if mapping is None:
            continue
        template = resolve_company_valuation_template(mapping.template_id, config_root)
        return CompanyValuationDefaultTemplateResolution(
            template_id=template.template_id,
            template_version=template.template_version,
            source="taxonomy_mapping",
            reason=mapping.notes or f"taxonomy:{mapping.taxonomy_level}",
            mapping_id=mapping.mapping_id,
            mapping_version=mapping.mapping_version,
        )

    template = resolve_company_valuation_template("general_operating", config_root)
    return CompanyValuationDefaultTemplateResolution(
        template_id=template.template_id,
        template_version=template.template_version,
        source="default",
        reason="default:general_operating",
    )


def resolve_company_valuation_support(
    *,
    ticker: str | None,
    canonical_taxonomy: CompanyValuationCanonicalTaxonomy,
    default_template: CompanyValuationDefaultTemplateResolution,
    config_root: str | Path | None = None,
) -> CompanyValuationSupportResolution:
    policy = load_company_valuation_support_policy(config_root)
    supported_template_ids = set(policy.supported_template_ids)
    normalized_ticker = normalize_ticker(ticker) if str(ticker or "").strip() else None
    supported_issuer_by_ticker = {issuer.ticker: issuer for issuer in policy.supported_issuers if issuer.active}
    supported_issuer = supported_issuer_by_ticker.get(normalized_ticker) if normalized_ticker is not None else None
    in_curated_universe = supported_issuer is not None
    expected_template_id = supported_issuer.expected_template_id if supported_issuer is not None else None
    support_tier = supported_issuer.support_tier if supported_issuer is not None else None

    if policy.allowlist_required and supported_issuer is None:
        return CompanyValuationSupportResolution(
            status="out_of_scope",
            reason="ticker is not in the curated supported issuer universe",
            in_curated_universe=False,
            support_tier=None,
        )

    if canonical_taxonomy.canonical_sector_id is None:
        return CompanyValuationSupportResolution(
            status="unsupported",
            reason="issuer is in scope but has no canonical taxonomy match yet",
            in_curated_universe=in_curated_universe,
            support_tier=support_tier,
            expected_template_id=expected_template_id,
        )

    if default_template.template_id not in supported_template_ids:
        return CompanyValuationSupportResolution(
            status="unsupported",
            reason=("issuer is in scope but taxonomy resolved to an unsupported valuation " f"template `{default_template.template_id}`"),
            in_curated_universe=in_curated_universe,
            support_tier=support_tier,
            expected_template_id=expected_template_id,
            expected_template_match=(None if expected_template_id is None else expected_template_id == default_template.template_id),
        )

    expected_template_match = None if expected_template_id is None else expected_template_id == default_template.template_id
    if expected_template_match is False:
        reason = (
            "issuer is in the curated universe and resolved to a supported template, "
            f"but the expected template is `{expected_template_id}` instead of "
            f"`{default_template.template_id}`"
        )
    else:
        reason = "issuer is in the curated universe and resolved to a supported template"
    return CompanyValuationSupportResolution(
        status="supported",
        reason=reason,
        in_curated_universe=in_curated_universe,
        support_tier=support_tier,
        expected_template_id=expected_template_id,
        expected_template_match=expected_template_match,
    )


def _overlay_rule_matches(
    rule: CompanyValuationOverlayRule,
    *,
    cik: str,
    company_name: str,
    raw_classification: CompanyValuationRawClassification,
) -> bool:
    normalized_cik = normalize_cik(cik)
    if normalized_cik in {normalize_cik(value) for value in rule.issuer_ciks}:
        return True
    company_name_text = _normalized_text(company_name)
    sic_title_text = _normalized_text(raw_classification.sic_title)
    if any(keyword in company_name_text for keyword in (_normalized_text(value) for value in rule.company_name_keywords) if keyword):
        return True
    if any(keyword in sic_title_text for keyword in (_normalized_text(value) for value in rule.sic_title_keywords) if keyword):
        return True
    if any(str(raw_classification.sic_code or "").startswith(prefix) for prefix in rule.sic_prefixes):
        return True
    return any(str(raw_classification.naics_code or "").startswith(prefix) for prefix in rule.naics_prefixes)


def resolve_company_valuation_overlays(
    *,
    cik: str,
    company_name: str,
    raw_classification: CompanyValuationRawClassification,
    config_root: str | Path | None = None,
) -> CompanyValuationOverlayResolution:
    rules = load_company_valuation_overlay_rules(config_root)
    flags = {rule.flag_key: False for rule in rules}
    reasons: dict[str, str] = {}
    for rule in rules:
        if not rule.active:
            continue
        if _overlay_rule_matches(
            rule,
            cik=cik,
            company_name=company_name,
            raw_classification=raw_classification,
        ):
            flags[rule.flag_key] = True
            reasons[rule.flag_key] = rule.reason
    return CompanyValuationOverlayResolution(flags=flags, reasons=reasons)


def resolve_company_valuation_taxonomy_context(
    *,
    cik: str,
    ticker: str | None = None,
    company_name: str,
    sic: str | None = None,
    sic_title: str | None = None,
    naics: str | None = None,
    naics_title: str | None = None,
    config_root: str | Path | None = None,
) -> CompanyValuationTaxonomyResolution:
    raw_classification = resolve_company_valuation_raw_classification(
        sic=sic,
        sic_title=sic_title,
        naics=naics,
        naics_title=naics_title,
    )
    canonical_taxonomy = resolve_company_valuation_canonical_taxonomy(
        cik=cik,
        raw_classification=raw_classification,
        config_root=config_root,
    )
    default_template = resolve_company_valuation_default_template(
        canonical_taxonomy,
        config_root=config_root,
    )
    support = resolve_company_valuation_support(
        ticker=ticker,
        canonical_taxonomy=canonical_taxonomy,
        default_template=default_template,
        config_root=config_root,
    )
    overlays = resolve_company_valuation_overlays(
        cik=cik,
        company_name=company_name,
        raw_classification=raw_classification,
        config_root=config_root,
    )
    return CompanyValuationTaxonomyResolution(
        raw_classification=raw_classification,
        canonical_taxonomy=canonical_taxonomy,
        default_template=default_template,
        support=support,
        overlays=overlays,
    )


__all__ = [
    "default_company_valuation_taxonomy_root",
    "load_company_valuation_overlay_rules",
    "load_company_valuation_support_policy",
    "supported_company_valuation_tickers",
    "load_company_valuation_taxonomy_mappings",
    "load_company_valuation_taxonomy_nodes",
    "load_company_valuation_taxonomy_overrides",
    "load_company_valuation_template_mappings",
    "resolve_company_valuation_canonical_taxonomy",
    "resolve_company_valuation_default_template",
    "resolve_company_valuation_overlays",
    "resolve_company_valuation_raw_classification",
    "resolve_company_valuation_support",
    "resolve_company_valuation_taxonomy_context",
]
