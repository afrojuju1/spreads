from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from core.services.company_valuation.contracts import (
    CompanyValuationOverlayRule,
    CompanyValuationTaxonomyResolution,
)
from core.services.company_valuation.ids import (
    build_issuer_overlay_flag_id,
    normalize_cik,
    normalize_ticker,
)
from core.services.company_valuation.taxonomy import (
    load_company_valuation_overlay_rules,
    load_company_valuation_taxonomy_mappings,
    load_company_valuation_taxonomy_nodes,
    load_company_valuation_template_mappings,
    resolve_company_valuation_taxonomy_context,
    supported_company_valuation_tickers,
)
from core.storage.company_valuation_repository import CompanyValuationRepository
from core.storage.serializers import render_value


TAXONOMY_LEVEL_ORDER = {
    "sector": 0,
    "industry_group": 1,
    "industry": 2,
    "subindustry": 3,
}


@dataclass(frozen=True)
class CompanyValuationTaxonomySyncRequest:
    tickers: tuple[str, ...] | None = None
    ciks: tuple[str, ...] | None = None
    issuer_ids: tuple[str, ...] | None = None
    issuer_limit: int | None = None
    supported_only: bool = False
    config_root: str | None = None
    sample_limit: int = 20
    output_root: str | None = None


@dataclass(frozen=True)
class CompanyValuationTaxonomySyncSample:
    issuer_id: str
    cik: str
    ticker: str | None
    company_name: str
    current_template_id: str
    taxonomy_default_template_id: str
    taxonomy_default_template_source: str
    classification_source: str
    canonical_sector_id: str | None
    canonical_subindustry_id: str | None
    support_status: str
    support_reason: str
    in_curated_universe: bool
    support_tier: str | None = None
    expected_template_id: str | None = None
    expected_template_match: bool | None = None
    overlay_flags: dict[str, bool] = field(default_factory=dict)

    def to_payload(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CompanyValuationTaxonomySyncResult:
    status: str
    started_at: datetime
    completed_at: datetime
    issuers_considered: int
    taxonomy_nodes_upserted: int
    taxonomy_mappings_upserted: int
    valuation_template_mappings_upserted: int
    issuer_classifications_upserted: int
    issuer_overlay_flags_replaced: int
    unclassified_count: int
    template_mismatch_count: int
    supported_unclassified_count: int
    supported_template_mismatch_count: int
    expected_template_mismatch_count: int
    taxonomy_override_count: int
    current_template_override_count: int
    classification_source_counts: dict[str, int] = field(default_factory=dict)
    support_status_counts: dict[str, int] = field(default_factory=dict)
    template_mismatch_pair_counts: dict[str, int] = field(default_factory=dict)
    overlay_true_counts: dict[str, int] = field(default_factory=dict)
    mismatch_samples: tuple[CompanyValuationTaxonomySyncSample, ...] = ()
    unclassified_samples: tuple[CompanyValuationTaxonomySyncSample, ...] = ()
    output_root: str | None = None
    manifest_path: str | None = None
    markdown_path: str | None = None
    mismatch_report_path: str | None = None
    unclassified_report_path: str | None = None
    notes: tuple[str, ...] = ()

    def to_payload(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["mismatch_samples"] = [row.to_payload() for row in self.mismatch_samples]
        payload["unclassified_samples"] = [
            row.to_payload() for row in self.unclassified_samples
        ]
        return payload


def _heartbeat(heartbeat: Callable[[], None] | None) -> None:
    if heartbeat is not None:
        heartbeat()


def _iso(value: Any) -> str | None:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    return None if value is None else str(value)


def _normalized_issuer_ids(values: tuple[str, ...] | None) -> tuple[str, ...] | None:
    if not values:
        return None
    normalized = tuple(
        dict.fromkeys(
            str(value).strip()
            for value in values
            if str(value or "").strip()
        )
    )
    return normalized or None


def _normalized_ciks(values: tuple[str, ...] | None) -> tuple[str, ...] | None:
    if not values:
        return None
    normalized = tuple(dict.fromkeys(normalize_cik(value) for value in values if str(value or "").strip()))
    return normalized or None


def _normalized_tickers(values: tuple[str, ...] | None) -> tuple[str, ...] | None:
    if not values:
        return None
    normalized = tuple(
        dict.fromkeys(
            normalize_ticker(value)
            for value in values
            if str(value or "").strip()
        )
    )
    return normalized or None


def _supported_scope_tickers(config_root: str | None) -> tuple[str, ...]:
    return supported_company_valuation_tickers(config_root)


def _resolved_ticker_scope(
    *,
    request_tickers: tuple[str, ...] | None,
    supported_only: bool,
    config_root: str | None,
) -> tuple[str, ...] | None:
    normalized_request = _normalized_tickers(request_tickers)
    if not supported_only:
        return normalized_request
    supported_tickers = _normalized_tickers(_supported_scope_tickers(config_root))
    if not supported_tickers:
        return ()
    if not normalized_request:
        return supported_tickers
    supported_set = set(supported_tickers)
    return tuple(ticker for ticker in normalized_request if ticker in supported_set)


def _taxonomy_node_payloads(
    config_root: str | None,
) -> list[dict[str, Any]]:
    nodes = load_company_valuation_taxonomy_nodes(config_root).values()
    ordered_nodes = sorted(
        nodes,
        key=lambda node: (
            TAXONOMY_LEVEL_ORDER.get(node.taxonomy_level, 99),
            node.taxonomy_code,
        ),
    )
    return [node.to_payload() for node in ordered_nodes]


def _taxonomy_mapping_payloads(
    config_root: str | None,
) -> list[dict[str, Any]]:
    return [
        mapping.to_payload()
        for mapping in load_company_valuation_taxonomy_mappings(config_root)
    ]


def _valuation_template_mapping_payloads(
    config_root: str | None,
) -> list[dict[str, Any]]:
    return [
        mapping.to_payload()
        for mapping in load_company_valuation_template_mappings(config_root)
    ]


def _classification_payload(
    *,
    issuer_row: dict[str, Any],
    resolution: CompanyValuationTaxonomyResolution,
    now: datetime,
) -> dict[str, Any]:
    return {
        "issuer_id": str(issuer_row["issuer_id"]),
        "taxonomy_version": resolution.canonical_taxonomy.taxonomy_version,
        "canonical_sector_id": resolution.canonical_taxonomy.canonical_sector_id,
        "canonical_industry_group_id": resolution.canonical_taxonomy.canonical_industry_group_id,
        "canonical_industry_id": resolution.canonical_taxonomy.canonical_industry_id,
        "canonical_subindustry_id": resolution.canonical_taxonomy.canonical_subindustry_id,
        "classification_source": resolution.canonical_taxonomy.classification_source,
        "classification_confidence": resolution.canonical_taxonomy.classification_confidence,
        "taxonomy_mapping_id": resolution.canonical_taxonomy.mapping_id,
        "valuation_template_mapping_id": resolution.default_template.mapping_id,
        "created_at": now,
        "updated_at": now,
    }


def _overlay_flag_payloads(
    *,
    issuer_row: dict[str, Any],
    resolution: CompanyValuationTaxonomyResolution,
    overlay_rules: tuple[CompanyValuationOverlayRule, ...],
    now: datetime,
) -> list[dict[str, Any]]:
    issuer_id = str(issuer_row["issuer_id"])
    cik = str(issuer_row["cik"])
    payloads: list[dict[str, Any]] = []
    for rule in overlay_rules:
        flag_value = bool(resolution.overlays.flags.get(rule.flag_key, False))
        payloads.append(
            {
                "issuer_overlay_flag_id": build_issuer_overlay_flag_id(cik, rule.flag_key),
                "issuer_id": issuer_id,
                "flag_key": rule.flag_key,
                "flag_value": flag_value,
                "source": "taxonomy_shadow_sync",
                "reason": resolution.overlays.reasons.get(rule.flag_key) or rule.reason,
                "active": rule.active,
                "created_at": now,
                "updated_at": now,
            }
        )
    return payloads


def _sample_from_resolution(
    *,
    issuer_row: dict[str, Any],
    resolution: CompanyValuationTaxonomyResolution,
) -> CompanyValuationTaxonomySyncSample:
    return CompanyValuationTaxonomySyncSample(
        issuer_id=str(issuer_row["issuer_id"]),
        cik=str(issuer_row["cik"]),
        ticker=str(issuer_row.get("ticker") or "") or None,
        company_name=str(issuer_row["company_name"]),
        current_template_id=str(issuer_row.get("template_id") or ""),
        taxonomy_default_template_id=resolution.default_template.template_id,
        taxonomy_default_template_source=resolution.default_template.source,
        classification_source=resolution.canonical_taxonomy.classification_source,
        canonical_sector_id=resolution.canonical_taxonomy.canonical_sector_id,
        canonical_subindustry_id=resolution.canonical_taxonomy.canonical_subindustry_id,
        support_status=resolution.support.status,
        support_reason=resolution.support.reason,
        in_curated_universe=resolution.support.in_curated_universe,
        support_tier=resolution.support.support_tier,
        expected_template_id=resolution.support.expected_template_id,
        expected_template_match=resolution.support.expected_template_match,
        overlay_flags=dict(resolution.overlays.flags),
    )


def _write_jsonl_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(render_value(row), sort_keys=True, default=_iso))
            handle.write("\n")


def _write_markdown_summary(
    *,
    request: CompanyValuationTaxonomySyncRequest,
    result: CompanyValuationTaxonomySyncResult,
    markdown_path: Path,
) -> None:
    lines = [
        "# Company Valuation Taxonomy Shadow Sync",
        "",
        f"- issuers_considered: `{result.issuers_considered}`",
        f"- issuer_classifications_upserted: `{result.issuer_classifications_upserted}`",
        f"- issuer_overlay_flags_replaced: `{result.issuer_overlay_flags_replaced}`",
        f"- unclassified_count: `{result.unclassified_count}`",
        f"- supported_unclassified_count: `{result.supported_unclassified_count}`",
        f"- template_mismatch_count: `{result.template_mismatch_count}`",
        f"- supported_template_mismatch_count: `{result.supported_template_mismatch_count}`",
        f"- expected_template_mismatch_count: `{result.expected_template_mismatch_count}`",
        f"- taxonomy_override_count: `{result.taxonomy_override_count}`",
        f"- current_template_override_count: `{result.current_template_override_count}`",
        "",
        "## Scope",
        "",
        f"- tickers: `{', '.join(request.tickers or ()) or 'all'}`",
        f"- ciks: `{', '.join(request.ciks or ()) or 'all'}`",
        f"- issuer_ids: `{', '.join(request.issuer_ids or ()) or 'all'}`",
        f"- issuer_limit: `{request.issuer_limit if request.issuer_limit is not None else 'none'}`",
        f"- supported_only: `{request.supported_only}`",
        "",
        "## Classification Sources",
        "",
    ]
    if result.classification_source_counts:
        for key, count in sorted(
            result.classification_source_counts.items(),
            key=lambda item: (-item[1], item[0]),
        ):
            lines.append(f"- `{key}`: `{count}`")
    else:
        lines.append("- none")
    lines.extend(["", "## Support Status", ""])
    if result.support_status_counts:
        for key, count in sorted(
            result.support_status_counts.items(),
            key=lambda item: (-item[1], item[0]),
        ):
            lines.append(f"- `{key}`: `{count}`")
    else:
        lines.append("- none")
    lines.extend(["", "## Template Mismatch Pairs", ""])
    if result.template_mismatch_pair_counts:
        for key, count in sorted(
            result.template_mismatch_pair_counts.items(),
            key=lambda item: (-item[1], item[0]),
        )[:20]:
            lines.append(f"- `{key}`: `{count}`")
    else:
        lines.append("- none")
    lines.extend(["", "## Overlay Counts", ""])
    if result.overlay_true_counts:
        for key, count in sorted(
            result.overlay_true_counts.items(),
            key=lambda item: (-item[1], item[0]),
        ):
            lines.append(f"- `{key}`: `{count}`")
    else:
        lines.append("- none")
    lines.extend(["", "## Mismatch Samples", ""])
    if result.mismatch_samples:
        for row in result.mismatch_samples:
            lines.append(
                f"- `{row.ticker or row.cik}` `{row.current_template_id}` -> `{row.taxonomy_default_template_id}` via `{row.classification_source}` support=`{row.support_status}`"
            )
    else:
        lines.append("- none")
    lines.extend(["", "## Unclassified Samples", ""])
    if result.unclassified_samples:
        for row in result.unclassified_samples:
            lines.append(
                f"- `{row.ticker or row.cik}` current template `{row.current_template_id}` with no canonical taxonomy match support=`{row.support_status}`"
            )
    else:
        lines.append("- none")
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")


def _write_report_artifacts(
    *,
    request: CompanyValuationTaxonomySyncRequest,
    result: CompanyValuationTaxonomySyncResult,
    mismatch_rows: list[dict[str, Any]],
    unclassified_rows: list[dict[str, Any]],
) -> CompanyValuationTaxonomySyncResult:
    output_root = Path(request.output_root or "outputs/company_valuation/taxonomy_sync")
    output_root.mkdir(parents=True, exist_ok=True)
    manifest_path = output_root / "manifest.json"
    markdown_path = output_root / "summary.md"
    mismatch_report_path = output_root / "mismatch_rows.jsonl"
    unclassified_report_path = output_root / "unclassified_rows.jsonl"
    enriched_result = CompanyValuationTaxonomySyncResult(
        **{
            **result.__dict__,
            "output_root": str(output_root),
            "manifest_path": str(manifest_path),
            "markdown_path": str(markdown_path),
            "mismatch_report_path": str(mismatch_report_path),
            "unclassified_report_path": str(unclassified_report_path),
        }
    )
    manifest_path.write_text(
        json.dumps(
            render_value(enriched_result.to_payload()),
            indent=2,
            sort_keys=True,
            default=_iso,
        )
        + "\n",
        encoding="utf-8",
    )
    _write_jsonl_rows(mismatch_report_path, mismatch_rows)
    _write_jsonl_rows(unclassified_report_path, unclassified_rows)
    _write_markdown_summary(
        request=request,
        result=enriched_result,
        markdown_path=markdown_path,
    )
    return enriched_result


def sync_company_valuation_taxonomy_state(
    request: CompanyValuationTaxonomySyncRequest,
    *,
    repository: CompanyValuationRepository | None = None,
    heartbeat: Callable[[], None] | None = None,
) -> CompanyValuationTaxonomySyncResult:
    started_at = datetime.now(UTC)
    repo = repository or CompanyValuationRepository()
    if not repo.taxonomy_schema_ready():
        raise RuntimeError(
            "Company valuation taxonomy tables are not present; run `uv run alembic upgrade head` first."
        )

    node_payloads = _taxonomy_node_payloads(request.config_root)
    mapping_payloads = _taxonomy_mapping_payloads(request.config_root)
    template_mapping_payloads = _valuation_template_mapping_payloads(request.config_root)
    overlay_rules = load_company_valuation_overlay_rules(request.config_root)

    _heartbeat(heartbeat)
    repo.upsert_taxonomy_nodes(node_payloads)
    _heartbeat(heartbeat)
    repo.upsert_taxonomy_mappings(mapping_payloads)
    _heartbeat(heartbeat)
    repo.upsert_valuation_template_mappings(template_mapping_payloads)

    resolved_tickers = _resolved_ticker_scope(
        request_tickers=request.tickers,
        supported_only=request.supported_only,
        config_root=request.config_root,
    )
    if request.supported_only and not resolved_tickers:
        issuer_rows = []
    else:
        issuer_rows = repo.list_issuers(
            issuer_ids=_normalized_issuer_ids(request.issuer_ids),
            ciks=_normalized_ciks(request.ciks),
            tickers=resolved_tickers,
            limit=request.issuer_limit,
        )

    overlay_true_counts: dict[str, int] = defaultdict(int)
    classification_source_counts: dict[str, int] = defaultdict(int)
    support_status_counts: dict[str, int] = defaultdict(int)
    template_mismatch_pair_counts: dict[str, int] = defaultdict(int)
    mismatch_samples: list[CompanyValuationTaxonomySyncSample] = []
    unclassified_samples: list[CompanyValuationTaxonomySyncSample] = []
    mismatch_rows: list[dict[str, Any]] = []
    unclassified_rows: list[dict[str, Any]] = []
    unclassified_count = 0
    supported_unclassified_count = 0
    template_mismatch_count = 0
    supported_template_mismatch_count = 0
    expected_template_mismatch_count = 0
    taxonomy_override_count = 0
    current_template_override_count = 0
    issuer_classifications_upserted = 0
    issuer_overlay_flags_replaced = 0

    for issuer_row in issuer_rows:
        _heartbeat(heartbeat)
        resolution = resolve_company_valuation_taxonomy_context(
            cik=str(issuer_row["cik"]),
            ticker=issuer_row.get("ticker"),
            company_name=str(issuer_row["company_name"]),
            sic=issuer_row.get("sic"),
            sic_title=issuer_row.get("sic_description"),
            naics=issuer_row.get("naics"),
            config_root=request.config_root,
        )
        now = datetime.now(UTC)
        repo.upsert_issuer_classification(
            _classification_payload(
                issuer_row=issuer_row,
                resolution=resolution,
                now=now,
            )
        )
        issuer_classifications_upserted += 1

        overlay_payloads = _overlay_flag_payloads(
            issuer_row=issuer_row,
            resolution=resolution,
            overlay_rules=overlay_rules,
            now=now,
        )
        issuer_overlay_flags_replaced += repo.replace_issuer_overlay_flags(
            issuer_id=str(issuer_row["issuer_id"]),
            payloads=overlay_payloads,
        )

        for rule in overlay_rules:
            if rule.active and resolution.overlays.flags.get(rule.flag_key):
                overlay_true_counts[rule.flag_key] += 1

        sample = _sample_from_resolution(issuer_row=issuer_row, resolution=resolution)
        classification_source_counts[resolution.canonical_taxonomy.classification_source] += 1
        support_status_counts[resolution.support.status] += 1
        if resolution.support.expected_template_match is False:
            expected_template_mismatch_count += 1
        if resolution.canonical_taxonomy.canonical_sector_id is None:
            unclassified_count += 1
            if resolution.support.in_curated_universe:
                supported_unclassified_count += 1
            unclassified_rows.append(sample.to_payload())
            if len(unclassified_samples) < request.sample_limit:
                unclassified_samples.append(sample)
        if str(issuer_row.get("template_id") or "") != resolution.default_template.template_id:
            template_mismatch_count += 1
            if resolution.support.status == "supported":
                supported_template_mismatch_count += 1
            pair_key = (
                f"{str(issuer_row.get('template_id') or '')}"
                f" -> {resolution.default_template.template_id}"
            )
            template_mismatch_pair_counts[pair_key] += 1
            mismatch_rows.append(sample.to_payload())
            if len(mismatch_samples) < request.sample_limit:
                mismatch_samples.append(sample)
        if resolution.canonical_taxonomy.source_standard == "issuer_override":
            taxonomy_override_count += 1
        if str(issuer_row.get("template_assignment_source") or "") == "issuer_override":
            current_template_override_count += 1

    completed_at = datetime.now(UTC)
    notes: list[str] = []
    if not issuer_rows:
        notes.append("No issuers matched the requested taxonomy sync scope.")
    if request.supported_only:
        notes.append(
            "Supported-only scope restricts the shadow sync to the curated supported issuer universe."
        )
    result = CompanyValuationTaxonomySyncResult(
        status="ok",
        started_at=started_at,
        completed_at=completed_at,
        issuers_considered=len(issuer_rows),
        taxonomy_nodes_upserted=len(node_payloads),
        taxonomy_mappings_upserted=len(mapping_payloads),
        valuation_template_mappings_upserted=len(template_mapping_payloads),
        issuer_classifications_upserted=issuer_classifications_upserted,
        issuer_overlay_flags_replaced=issuer_overlay_flags_replaced,
        unclassified_count=unclassified_count,
        template_mismatch_count=template_mismatch_count,
        supported_unclassified_count=supported_unclassified_count,
        supported_template_mismatch_count=supported_template_mismatch_count,
        expected_template_mismatch_count=expected_template_mismatch_count,
        taxonomy_override_count=taxonomy_override_count,
        current_template_override_count=current_template_override_count,
        classification_source_counts=dict(
            sorted(classification_source_counts.items(), key=lambda item: item[0])
        ),
        support_status_counts=dict(
            sorted(support_status_counts.items(), key=lambda item: item[0])
        ),
        template_mismatch_pair_counts=dict(
            sorted(
                template_mismatch_pair_counts.items(),
                key=lambda item: (-item[1], item[0]),
            )
        ),
        overlay_true_counts=dict(sorted(overlay_true_counts.items())),
        mismatch_samples=tuple(mismatch_samples),
        unclassified_samples=tuple(unclassified_samples),
        notes=tuple(notes),
    )
    if request.output_root:
        return _write_report_artifacts(
            request=request,
            result=result,
            mismatch_rows=mismatch_rows,
            unclassified_rows=unclassified_rows,
        )
    return result


__all__ = [
    "CompanyValuationTaxonomySyncRequest",
    "CompanyValuationTaxonomySyncResult",
    "CompanyValuationTaxonomySyncSample",
    "sync_company_valuation_taxonomy_state",
]
