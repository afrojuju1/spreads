from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import func, select

from core.storage.base import RepositoryBase
from core.storage.company_valuation_models import (
    CompanyValuationSnapshotModel,
    FilingModel,
    InstitutionalPositionModel,
    IssuerModel,
    MarketSnapshotModel,
    ScreeningRowModel,
    SecurityModel,
    TreasuryCurveSnapshotModel,
    BeneficialOwnerPositionModel,
    InsiderTransactionModel,
)
from core.storage.serializers import parse_date, parse_datetime


class CompanyValuationRepository(RepositoryBase):
    def schema_ready(self) -> bool:
        return self.schema_has_tables(
            "issuers",
            "filings",
            "company_valuation_snapshots",
            "screening_rows",
        )

    def get_issuer(
        self,
        *,
        issuer_id: str | None = None,
        ticker: str | None = None,
    ) -> dict[str, Any] | None:
        if not issuer_id and not ticker:
            raise ValueError("issuer_id or ticker is required")
        statement = select(IssuerModel, SecurityModel.ticker.label("ticker")).join(
            SecurityModel,
            SecurityModel.issuer_id == IssuerModel.issuer_id,
            isouter=True,
        )
        if issuer_id:
            statement = statement.where(IssuerModel.issuer_id == issuer_id)
        else:
            statement = statement.where(SecurityModel.ticker == str(ticker or "").upper())
        statement = statement.order_by(SecurityModel.is_primary.desc(), SecurityModel.created_at.desc()).limit(1)
        with self.session_factory() as session:
            row = session.execute(statement).first()
        if row is None:
            return None
        issuer_model, resolved_ticker = row
        return self.row(issuer_model, extra={"ticker": resolved_ticker})

    def get_latest_filing_before(
        self,
        *,
        issuer_id: str,
        as_of: str | datetime,
        form_types: tuple[str, ...] | None = None,
    ) -> dict[str, Any] | None:
        as_of_dt = parse_datetime(as_of)
        if as_of_dt is None:
            raise ValueError("as_of is required")
        statement = (
            select(FilingModel)
            .where(FilingModel.issuer_id == issuer_id)
            .where(FilingModel.available_at <= as_of_dt)
        )
        if form_types:
            statement = statement.where(FilingModel.form_type.in_(form_types))
        statement = statement.order_by(FilingModel.available_at.desc(), FilingModel.accepted_at.desc()).limit(1)
        with self.session_factory() as session:
            row = session.scalar(statement)
        return None if row is None else self.row(row)

    def get_latest_market_snapshot_before(
        self,
        *,
        issuer_id: str,
        as_of: str | datetime,
    ) -> dict[str, Any] | None:
        as_of_dt = parse_datetime(as_of)
        if as_of_dt is None:
            raise ValueError("as_of is required")
        statement = (
            select(MarketSnapshotModel)
            .where(MarketSnapshotModel.issuer_id == issuer_id)
            .where(MarketSnapshotModel.available_at <= as_of_dt)
            .order_by(MarketSnapshotModel.available_at.desc(), MarketSnapshotModel.captured_at.desc())
            .limit(1)
        )
        with self.session_factory() as session:
            row = session.scalar(statement)
        return None if row is None else self.row(row)

    def get_latest_treasury_curve_before(
        self,
        *,
        as_of: str | datetime,
    ) -> dict[str, Any] | None:
        as_of_dt = parse_datetime(as_of)
        if as_of_dt is None:
            raise ValueError("as_of is required")
        statement = (
            select(TreasuryCurveSnapshotModel)
            .where(TreasuryCurveSnapshotModel.available_at <= as_of_dt)
            .order_by(TreasuryCurveSnapshotModel.curve_date.desc())
            .limit(1)
        )
        with self.session_factory() as session:
            row = session.scalar(statement)
        return None if row is None else self.row(row)

    def latest_ownership_available_at_before(
        self,
        *,
        issuer_id: str,
        as_of: str | datetime,
    ) -> datetime | None:
        as_of_dt = parse_datetime(as_of)
        if as_of_dt is None:
            raise ValueError("as_of is required")
        statements = (
            select(func.max(BeneficialOwnerPositionModel.available_at)).where(
                BeneficialOwnerPositionModel.issuer_id == issuer_id,
                BeneficialOwnerPositionModel.available_at <= as_of_dt,
            ),
            select(func.max(InsiderTransactionModel.available_at)).where(
                InsiderTransactionModel.issuer_id == issuer_id,
                InsiderTransactionModel.available_at <= as_of_dt,
            ),
            select(func.max(InstitutionalPositionModel.available_at)).where(
                InstitutionalPositionModel.issuer_id == issuer_id,
                InstitutionalPositionModel.available_at <= as_of_dt,
            ),
        )
        timestamps: list[datetime] = []
        with self.session_factory() as session:
            for statement in statements:
                value = session.scalar(statement)
                if value is not None:
                    timestamps.append(value)
        if not timestamps:
            return None
        return max(timestamps)

    def get_latest_company_valuation(
        self,
        *,
        issuer_id: str,
        as_of: str | datetime | None = None,
    ) -> dict[str, Any] | None:
        statement = select(CompanyValuationSnapshotModel).where(
            CompanyValuationSnapshotModel.issuer_id == issuer_id
        )
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

    def list_screening_rows(
        self,
        *,
        as_of: str,
        template_id: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        statement = select(ScreeningRowModel).where(ScreeningRowModel.as_of == parse_date(as_of))
        if template_id:
            statement = statement.where(ScreeningRowModel.template_id == template_id)
        statement = statement.order_by(
            ScreeningRowModel.quality_score.desc().nullslast(),
            ScreeningRowModel.valuation_gap.desc().nullslast(),
            ScreeningRowModel.ticker.asc(),
        ).limit(limit)
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)
