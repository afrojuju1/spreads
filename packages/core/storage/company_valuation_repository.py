from __future__ import annotations

from typing import Any

from core.storage.base import RepositoryBase
from core.storage.company_valuation import (
    CompanyValuationLookupMixin,
    CompanyValuationMutationMixin,
    CompanyValuationOutputMixin,
    CompanyValuationPointInTimeMixin,
)


class CompanyValuationRepository(
    CompanyValuationMutationMixin,
    CompanyValuationLookupMixin,
    CompanyValuationPointInTimeMixin,
    CompanyValuationOutputMixin,
    RepositoryBase,
):
    def schema_ready(self) -> bool:
        return self.schema_has_tables(
            "issuers",
            "filings",
            "security_identifier_history",
            "institutional_filings",
            "unresolved_institutional_positions",
            "company_valuation_snapshots",
            "screening_rows",
        )

    def taxonomy_schema_ready(self) -> bool:
        return self.schema_has_tables(
            "taxonomy_nodes",
            "taxonomy_mappings",
            "valuation_template_mappings",
            "issuer_classifications",
            "issuer_overlay_flags",
        )

    @staticmethod
    def _assign_model(model: Any, payload: dict[str, Any]) -> None:
        for key, value in payload.items():
            setattr(model, key, value)

    @staticmethod
    def _preserve_created_at(model: Any, payload: dict[str, Any]) -> dict[str, Any]:
        if getattr(model, "created_at", None) is None or "created_at" not in payload:
            return payload
        next_payload = dict(payload)
        next_payload["created_at"] = model.created_at
        return next_payload

    @staticmethod
    def _statement_snapshot_sort_key(row: Any) -> tuple[Any, ...]:
        metrics = getattr(row, "metrics_json", None)
        metric_count = len(metrics) if isinstance(metrics, dict) else 0
        latest_snapshot_coverage = 0
        if isinstance(metrics, dict):
            latest_snapshot_coverage = sum(
                1
                for key in (
                    "current_assets",
                    "current_liabilities",
                    "inventory",
                    "total_assets",
                    "cash_and_equivalents",
                    "long_term_debt",
                    "total_liabilities",
                    "stockholders_equity",
                    "shares_outstanding",
                    "diluted_weighted_average_shares",
                    "deferred_revenue",
                )
                if metrics.get(key) is not None
            )
        return (
            row.period_end,
            latest_snapshot_coverage,
            1 if str(row.period_type or "") != "instant" else 0,
            metric_count,
            1 if metric_count >= 4 else 0,
            row.available_at,
        )
