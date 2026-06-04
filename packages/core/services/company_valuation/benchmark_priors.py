from __future__ import annotations

import json
import statistics
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from functools import lru_cache
from pathlib import Path
from typing import Any

from core.runtime.config import default_database_url
from core.services.company_valuation.contracts import (
    CompanyValuationBenchmarkPriorEntry,
    CompanyValuationBenchmarkPriorSet,
)
from core.services.company_valuation.evaluation import recompute_company_valuation
from core.services.company_valuation.taxonomy import (
    resolve_company_valuation_taxonomy_context,
)
from core.services.trading_strategies import (
    _load_yaml_file,
    _yaml_file_signature,
    default_config_root,
)
from core.storage.company_valuation_repository import CompanyValuationRepository
from core.storage.serializers import parse_datetime


def _benchmark_prior_path(config_root: str | Path | None = None) -> Path:
    return default_config_root(config_root) / "company_valuation" / "benchmark_priors.yaml"


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


def _as_int(value: Any, *, field_name: str) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be an integer") from exc


def _as_float(value: Any, *, field_name: str) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be numeric") from exc


def _as_target_field(value: Any) -> str:
    rendered = _as_text(value, field_name="target_field")
    if rendered not in {"average_target", "median_target"}:
        raise ValueError("target_field must be average_target or median_target")
    return rendered


def _normalized_as_of(value: str | datetime | None) -> datetime:
    parsed = parse_datetime(value) if isinstance(value, str) else value
    if parsed is None:
        return datetime.now(UTC)
    return parsed.astimezone(UTC)


def _safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
    return str(value)


@lru_cache(maxsize=8)
def _load_company_valuation_benchmark_priors_cached(
    path_key: str,
    signature: tuple[str, int, int] | None,
) -> tuple[CompanyValuationBenchmarkPriorSet, ...]:
    path = Path(path_key)
    if not path.exists():
        return ()
    payload = _load_yaml_file(path)
    raw_prior_sets = payload.get("prior_sets")
    if raw_prior_sets is None:
        return ()
    if not isinstance(raw_prior_sets, list):
        raise ValueError("prior_sets must be a list")
    prior_sets: list[CompanyValuationBenchmarkPriorSet] = []
    for item in raw_prior_sets:
        mapping = _as_mapping(item, field_name="prior_set")
        raw_entries = mapping.get("entries") or []
        if not isinstance(raw_entries, list):
            raise ValueError("entries must be a list")
        entries: list[CompanyValuationBenchmarkPriorEntry] = []
        for raw_entry in raw_entries:
            entry_mapping = _as_mapping(raw_entry, field_name="entry")
            entries.append(
                CompanyValuationBenchmarkPriorEntry(
                    ticker=_as_text(entry_mapping.get("ticker"), field_name="ticker").upper(),
                    analyst_count=_as_int(
                        entry_mapping.get("analyst_count"),
                        field_name="analyst_count",
                    ),
                    consensus_rating=(
                        str(entry_mapping.get("consensus_rating")).strip() if entry_mapping.get("consensus_rating") not in (None, "") else None
                    ),
                    average_target=_as_float(
                        entry_mapping.get("average_target"),
                        field_name="average_target",
                    ),
                    median_target=_as_float(
                        entry_mapping.get("median_target"),
                        field_name="median_target",
                    ),
                    low_target=_as_float(
                        entry_mapping.get("low_target"),
                        field_name="low_target",
                    ),
                    high_target=_as_float(
                        entry_mapping.get("high_target"),
                        field_name="high_target",
                    ),
                    source_url=(str(entry_mapping.get("source_url")).strip() if entry_mapping.get("source_url") not in (None, "") else None),
                    active=bool(entry_mapping.get("active", True)),
                )
            )
        prior_sets.append(
            CompanyValuationBenchmarkPriorSet(
                prior_set_id=_as_text(
                    mapping.get("prior_set_id"),
                    field_name="prior_set_id",
                ),
                basket_id=_as_text(mapping.get("basket_id"), field_name="basket_id"),
                template_id=_as_text(
                    mapping.get("template_id"),
                    field_name="template_id",
                ),
                as_of=_as_text(mapping.get("as_of"), field_name="as_of"),
                source_name=_as_text(
                    mapping.get("source_name"),
                    field_name="source_name",
                ),
                target_field=_as_target_field(mapping.get("target_field", "average_target")),
                supported_only_default=bool(mapping.get("supported_only_default", True)),
                minimum_coverage=max(int(mapping.get("minimum_coverage", 1)), 1),
                trigger_mean_abs_gap_delta=float(mapping.get("trigger_mean_abs_gap_delta", 0.2)),
                trigger_sign_mismatch_count=max(
                    int(mapping.get("trigger_sign_mismatch_count", 1)),
                    0,
                ),
                source_notes=str(mapping.get("source_notes") or "").strip(),
                entries=tuple(entries),
            )
        )
    return tuple(prior_sets)


def load_company_valuation_benchmark_priors(
    config_root: str | Path | None = None,
) -> dict[str, CompanyValuationBenchmarkPriorSet]:
    path = _benchmark_prior_path(config_root)
    return {
        prior_set.prior_set_id: prior_set
        for prior_set in _load_company_valuation_benchmark_priors_cached(
            str(path),
            _yaml_file_signature(path),
        )
    }


def resolve_company_valuation_benchmark_prior_set(
    prior_set_id: str,
    config_root: str | Path | None = None,
) -> CompanyValuationBenchmarkPriorSet:
    prior_sets = load_company_valuation_benchmark_priors(config_root)
    try:
        return prior_sets[prior_set_id]
    except KeyError as exc:
        raise ValueError(f"Unknown company valuation benchmark prior set: {prior_set_id}") from exc


def _benchmark_target_for_entry(
    *,
    entry: CompanyValuationBenchmarkPriorEntry,
    target_field: str,
) -> float | None:
    if target_field == "average_target":
        return entry.average_target if entry.average_target is not None else entry.median_target
    if target_field == "median_target":
        return entry.median_target if entry.median_target is not None else entry.average_target
    raise ValueError(f"Unsupported benchmark target field: {target_field}")


@dataclass(frozen=True)
class CompanyValuationBenchmarkPriorReportRequest:
    prior_set_id: str
    as_of: str | datetime | None = None
    supported_only: bool | None = None
    output_root: str | None = None
    config_root: str | None = None


@dataclass(frozen=True)
class CompanyValuationBenchmarkPriorReportRow:
    ticker: str
    support_status: str
    support_reason: str
    support_tier: str | None
    base_template_id: str
    effective_template_id: str
    current_price: float
    intrinsic_value_mid: float
    valuation_gap: float
    benchmark_target: float
    benchmark_gap: float
    gap_delta: float
    absolute_gap_delta: float
    valuation_confidence: float | None = None
    quality_score: float | None = None
    analyst_count: int | None = None
    consensus_rating: str | None = None
    source_url: str | None = None

    def to_payload(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CompanyValuationBenchmarkPriorReportResult:
    status: str
    prior_set_id: str
    basket_id: str
    template_id: str
    source_name: str
    target_field: str
    as_of: datetime
    supported_only: bool
    entry_count: int
    rows_compared: int
    skipped_entries: int
    mean_valuation_gap: float | None = None
    mean_benchmark_gap: float | None = None
    mean_gap_delta: float | None = None
    mean_abs_gap_delta: float | None = None
    median_abs_gap_delta: float | None = None
    sign_mismatch_count: int = 0
    under_benchmark_count: int = 0
    over_benchmark_count: int = 0
    calibration_gate_triggered: bool = False
    calibration_gate_reason: str = ""
    output_root: str | None = None
    manifest_path: str | None = None
    summary_path: str | None = None
    rows_path: str | None = None
    rows: tuple[CompanyValuationBenchmarkPriorReportRow, ...] = ()
    errors: tuple[str, ...] = ()

    def to_payload(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["as_of"] = _json_default(self.as_of)
        payload["rows"] = [row.to_payload() for row in self.rows]
        payload["errors"] = list(self.errors)
        return payload


def _summary_markdown(
    *,
    prior_set: CompanyValuationBenchmarkPriorSet,
    result: CompanyValuationBenchmarkPriorReportResult,
) -> str:
    lines = [
        f"# Benchmark Prior Report: {prior_set.prior_set_id}",
        "",
        f"- as_of: `{_json_default(result.as_of)}`",
        f"- basket_id: `{prior_set.basket_id}`",
        f"- template_id: `{prior_set.template_id}`",
        f"- source_name: `{prior_set.source_name}`",
        f"- target_field: `{prior_set.target_field}`",
        f"- supported_only: `{result.supported_only}`",
        f"- entry_count: `{result.entry_count}`",
        f"- rows_compared: `{result.rows_compared}`",
        f"- skipped_entries: `{result.skipped_entries}`",
        f"- mean_valuation_gap: `{result.mean_valuation_gap}`",
        f"- mean_benchmark_gap: `{result.mean_benchmark_gap}`",
        f"- mean_gap_delta: `{result.mean_gap_delta}`",
        f"- mean_abs_gap_delta: `{result.mean_abs_gap_delta}`",
        f"- median_abs_gap_delta: `{result.median_abs_gap_delta}`",
        f"- sign_mismatch_count: `{result.sign_mismatch_count}`",
        f"- under_benchmark_count: `{result.under_benchmark_count}`",
        f"- over_benchmark_count: `{result.over_benchmark_count}`",
        f"- calibration_gate_triggered: `{result.calibration_gate_triggered}`",
        f"- calibration_gate_reason: `{result.calibration_gate_reason}`",
        "",
        "## Largest Deviations",
    ]
    for row in sorted(result.rows, key=lambda item: item.absolute_gap_delta, reverse=True)[:10]:
        lines.append(
            f"- `{row.ticker}` support=`{row.support_status}` "
            f"valuation_gap=`{round(row.valuation_gap, 4)}` "
            f"benchmark_gap=`{round(row.benchmark_gap, 4)}` "
            f"delta=`{round(row.gap_delta, 4)}`"
        )
    if prior_set.source_notes:
        lines.extend(["", "## Notes", f"- {prior_set.source_notes}"])
    return "\n".join(lines) + "\n"


def report_company_valuation_benchmark_priors(
    request: CompanyValuationBenchmarkPriorReportRequest,
    *,
    repository: CompanyValuationRepository | None = None,
) -> CompanyValuationBenchmarkPriorReportResult:
    repo = repository or CompanyValuationRepository(default_database_url())
    prior_set = resolve_company_valuation_benchmark_prior_set(
        request.prior_set_id,
        request.config_root,
    )
    supported_only = prior_set.supported_only_default if request.supported_only is None else bool(request.supported_only)
    as_of_dt = _normalized_as_of(request.as_of)
    rows: list[CompanyValuationBenchmarkPriorReportRow] = []
    errors: list[str] = []
    skipped_entries = 0

    for entry in prior_set.entries:
        if not entry.active:
            skipped_entries += 1
            continue
        issuer_row = repo.get_issuer(ticker=entry.ticker)
        if issuer_row is None:
            errors.append(f"{entry.ticker}: issuer is not available")
            continue
        resolution = resolve_company_valuation_taxonomy_context(
            cik=str(issuer_row.get("cik") or ""),
            ticker=str(issuer_row.get("ticker") or entry.ticker),
            company_name=str(issuer_row.get("company_name") or ""),
            sic=issuer_row.get("sic"),
            sic_title=issuer_row.get("sic_description"),
            naics=issuer_row.get("naics"),
            config_root=request.config_root,
        )
        if supported_only and resolution.support.status != "supported":
            skipped_entries += 1
            continue
        benchmark_target = _benchmark_target_for_entry(
            entry=entry,
            target_field=prior_set.target_field,
        )
        if benchmark_target is None or benchmark_target <= 0.0:
            skipped_entries += 1
            continue
        try:
            recompute_result = recompute_company_valuation(
                ticker=entry.ticker,
                as_of=as_of_dt,
                repository=repo,
                config_root=request.config_root,
                persist=False,
            )
        except Exception as exc:
            errors.append(f"{entry.ticker}: {exc}")
            continue
        document = recompute_result.document
        valuation = dict(document.get("valuation") or {})
        quality = dict(document.get("quality") or {})
        current_price = _safe_float(valuation.get("current_price"))
        intrinsic_value_mid = _safe_float(valuation.get("intrinsic_value_mid"))
        valuation_gap = _safe_float(valuation.get("valuation_gap"))
        if current_price in (None, 0.0) or intrinsic_value_mid is None:
            skipped_entries += 1
            continue
        if valuation_gap is None:
            valuation_gap = (intrinsic_value_mid / current_price) - 1.0
        benchmark_gap = (benchmark_target / current_price) - 1.0
        gap_delta = valuation_gap - benchmark_gap
        rows.append(
            CompanyValuationBenchmarkPriorReportRow(
                ticker=entry.ticker,
                support_status=resolution.support.status,
                support_reason=resolution.support.reason,
                support_tier=resolution.support.support_tier,
                base_template_id=str(issuer_row.get("template_id") or ""),
                effective_template_id=str((document.get("source_summary") or {}).get("effective_template_id") or issuer_row.get("template_id") or ""),
                current_price=round(current_price, 4),
                intrinsic_value_mid=round(intrinsic_value_mid, 4),
                valuation_gap=round(valuation_gap, 6),
                benchmark_target=round(benchmark_target, 4),
                benchmark_gap=round(benchmark_gap, 6),
                gap_delta=round(gap_delta, 6),
                absolute_gap_delta=round(abs(gap_delta), 6),
                valuation_confidence=_safe_float(valuation.get("confidence")),
                quality_score=_safe_float(quality.get("total_score")),
                analyst_count=entry.analyst_count,
                consensus_rating=entry.consensus_rating,
                source_url=entry.source_url,
            )
        )

    mean_valuation_gap = None
    mean_benchmark_gap = None
    mean_gap_delta = None
    mean_abs_gap_delta = None
    median_abs_gap_delta = None
    sign_mismatch_count = 0
    under_benchmark_count = 0
    over_benchmark_count = 0
    calibration_gate_triggered = False
    calibration_gate_reason = "insufficient comparable rows"

    if rows:
        valuation_gaps = [row.valuation_gap for row in rows]
        benchmark_gaps = [row.benchmark_gap for row in rows]
        gap_deltas = [row.gap_delta for row in rows]
        abs_gap_deltas = [row.absolute_gap_delta for row in rows]
        sign_mismatch_count = sum((row.valuation_gap >= 0.0) != (row.benchmark_gap >= 0.0) for row in rows)
        under_benchmark_count = sum(row.gap_delta < 0.0 for row in rows)
        over_benchmark_count = sum(row.gap_delta > 0.0 for row in rows)
        mean_valuation_gap = round(statistics.mean(valuation_gaps), 6)
        mean_benchmark_gap = round(statistics.mean(benchmark_gaps), 6)
        mean_gap_delta = round(statistics.mean(gap_deltas), 6)
        mean_abs_gap_delta = round(statistics.mean(abs_gap_deltas), 6)
        median_abs_gap_delta = round(statistics.median(abs_gap_deltas), 6)
        calibration_gate_triggered = len(rows) >= prior_set.minimum_coverage and (
            mean_abs_gap_delta >= prior_set.trigger_mean_abs_gap_delta or sign_mismatch_count >= prior_set.trigger_sign_mismatch_count
        )
        if calibration_gate_triggered:
            calibration_gate_reason = f"rows={len(rows)} mean_abs_gap_delta={mean_abs_gap_delta} " f"sign_mismatch_count={sign_mismatch_count}"
        else:
            calibration_gate_reason = f"rows={len(rows)} below trigger thresholds"

    output_root = None if request.output_root is None else str(Path(request.output_root))
    manifest_path = None
    summary_path = None
    rows_path = None

    result = CompanyValuationBenchmarkPriorReportResult(
        status="ok",
        prior_set_id=prior_set.prior_set_id,
        basket_id=prior_set.basket_id,
        template_id=prior_set.template_id,
        source_name=prior_set.source_name,
        target_field=prior_set.target_field,
        as_of=as_of_dt,
        supported_only=supported_only,
        entry_count=len(prior_set.entries),
        rows_compared=len(rows),
        skipped_entries=skipped_entries,
        mean_valuation_gap=mean_valuation_gap,
        mean_benchmark_gap=mean_benchmark_gap,
        mean_gap_delta=mean_gap_delta,
        mean_abs_gap_delta=mean_abs_gap_delta,
        median_abs_gap_delta=median_abs_gap_delta,
        sign_mismatch_count=sign_mismatch_count,
        under_benchmark_count=under_benchmark_count,
        over_benchmark_count=over_benchmark_count,
        calibration_gate_triggered=calibration_gate_triggered,
        calibration_gate_reason=calibration_gate_reason,
        output_root=output_root,
        manifest_path=manifest_path,
        summary_path=summary_path,
        rows_path=rows_path,
        rows=tuple(sorted(rows, key=lambda item: item.absolute_gap_delta, reverse=True)),
        errors=tuple(errors),
    )

    if request.output_root is None:
        return result

    output_dir = Path(request.output_root)
    output_dir.mkdir(parents=True, exist_ok=True)
    rows_path = str(output_dir / "rows.jsonl")
    summary_path = str(output_dir / "summary.md")
    manifest_path = str(output_dir / "manifest.json")
    Path(rows_path).write_text(
        "".join(json.dumps(row.to_payload(), sort_keys=True, default=_json_default) + "\n" for row in result.rows),
        encoding="utf-8",
    )
    Path(summary_path).write_text(
        _summary_markdown(prior_set=prior_set, result=result),
        encoding="utf-8",
    )
    updated_result = CompanyValuationBenchmarkPriorReportResult(
        **{
            **result.__dict__,
            "manifest_path": manifest_path,
            "summary_path": summary_path,
            "rows_path": rows_path,
        }
    )
    Path(manifest_path).write_text(
        json.dumps(updated_result.to_payload(), sort_keys=True, default=_json_default, indent=2) + "\n",
        encoding="utf-8",
    )
    return updated_result


__all__ = [
    "CompanyValuationBenchmarkPriorReportRequest",
    "CompanyValuationBenchmarkPriorReportResult",
    "CompanyValuationBenchmarkPriorReportRow",
    "default_company_valuation_benchmark_prior_path",
    "load_company_valuation_benchmark_priors",
    "report_company_valuation_benchmark_priors",
    "resolve_company_valuation_benchmark_prior_set",
]


def default_company_valuation_benchmark_prior_path(
    config_root: str | Path | None = None,
) -> Path:
    return _benchmark_prior_path(config_root)
