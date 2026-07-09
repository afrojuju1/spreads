from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import func, or_, select

from core.storage.company_valuation_models import (
    CompanyValuationSnapshotModel,
    FeatureSnapshotModel,
    IssuerModel,
    ScreeningRowModel,
)
from core.storage.serializers import parse_date, parse_datetime


class CompanyValuationOutputMixin:
    def get_latest_company_valuation(
        self,
        *,
        issuer_id: str,
        as_of: str | datetime | None = None,
    ) -> dict[str, Any] | None:
        statement = select(CompanyValuationSnapshotModel).where(CompanyValuationSnapshotModel.issuer_id == issuer_id)
        as_of_dt = parse_datetime(as_of) if as_of is not None else None
        if as_of_dt is not None:
            statement = statement.where(CompanyValuationSnapshotModel.as_of <= as_of_dt)
        statement = statement.order_by(
            CompanyValuationSnapshotModel.as_of.desc(),
            CompanyValuationSnapshotModel.computed_at.desc(),
        ).limit(1)
        with self.session_factory() as session:
            row = session.scalar(statement)
        return None if row is None else self.row(row)

    def upsert_feature_snapshot(
        self,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        feature_snapshot_id = str(payload["feature_snapshot_id"])
        with self.session_scope() as session:
            row = session.get(FeatureSnapshotModel, feature_snapshot_id)
            if row is None:
                row = FeatureSnapshotModel(feature_snapshot_id=feature_snapshot_id)
                session.add(row)
            self._assign_model(row, payload)
        statement = select(FeatureSnapshotModel).where(FeatureSnapshotModel.feature_snapshot_id == feature_snapshot_id)
        with self.session_factory() as session:
            row = session.scalar(statement)
        return {} if row is None else self.row(row)

    def upsert_company_valuation_snapshot(
        self,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        snapshot_id = str(payload["company_valuation_snapshot_id"])
        with self.session_scope() as session:
            row = session.get(CompanyValuationSnapshotModel, snapshot_id)
            if row is None:
                row = CompanyValuationSnapshotModel(company_valuation_snapshot_id=snapshot_id)
                session.add(row)
            self._assign_model(row, payload)
        statement = select(CompanyValuationSnapshotModel).where(CompanyValuationSnapshotModel.company_valuation_snapshot_id == snapshot_id)
        with self.session_factory() as session:
            row = session.scalar(statement)
        return {} if row is None else self.row(row)

    def upsert_screening_row(
        self,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        screening_row_id = str(payload["screening_row_id"])
        with self.session_scope() as session:
            row = session.get(ScreeningRowModel, screening_row_id)
            if row is None:
                row = ScreeningRowModel(screening_row_id=screening_row_id)
                session.add(row)
            self._assign_model(row, payload)
        statement = select(ScreeningRowModel).where(ScreeningRowModel.screening_row_id == screening_row_id)
        with self.session_factory() as session:
            row = session.scalar(statement)
        return {} if row is None else self.row(row)

    def list_screening_rows(
        self,
        *,
        as_of: str,
        template_id: str | None = None,
        tickers: tuple[str, ...] | None = None,
        stressed_operator_only: bool = False,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        statement = (
            select(ScreeningRowModel)
            .join(IssuerModel, IssuerModel.issuer_id == ScreeningRowModel.issuer_id)
            .where(ScreeningRowModel.as_of == parse_date(as_of))
            .where(
                or_(
                    IssuerModel.template_assignment_source != "openfigi_seed",
                    IssuerModel.limited_coverage_flag.is_(False),
                )
            )
        )
        if template_id:
            statement = statement.where(ScreeningRowModel.template_id == template_id)
        if stressed_operator_only:
            statement = statement.where(ScreeningRowModel.stressed_operator_flag.is_(True))
        normalized_tickers = tuple(str(value).upper().strip() for value in (tickers or ()) if str(value or "").strip())
        if normalized_tickers:
            statement = statement.where(ScreeningRowModel.ticker.in_(normalized_tickers))
        statement = statement.order_by(
            ScreeningRowModel.overall_rank.asc().nullslast(),
            ScreeningRowModel.screen_rank_score.desc().nullslast(),
            ScreeningRowModel.quality_score.desc().nullslast(),
            ScreeningRowModel.valuation_gap.desc().nullslast(),
            ScreeningRowModel.ticker.asc(),
        ).limit(limit)
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)

    def latest_screening_as_of(self) -> str | None:
        statement = select(func.max(ScreeningRowModel.as_of))
        with self.session_factory() as session:
            value = session.scalar(statement)
        return None if value is None else str(value)
