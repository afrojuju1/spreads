from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import func, or_, select

from core.storage.company_valuation_models import (
    BeneficialOwnerPositionModel,
    FilingModel,
    InstitutionalPositionModel,
    MarketSnapshotModel,
    SecurityModel,
    StatementPeriodSnapshotModel,
    TreasuryCurveSnapshotModel,
    UnresolvedInstitutionalPositionModel,
    InsiderTransactionModel,
)
from core.storage.serializers import parse_date, parse_datetime


class CompanyValuationPointInTimeMixin:
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
        statement = select(FilingModel).where(FilingModel.issuer_id == issuer_id).where(FilingModel.available_at <= as_of_dt)
        if form_types:
            statement = statement.where(FilingModel.form_type.in_(form_types))
        statement = statement.order_by(FilingModel.available_at.desc(), FilingModel.accepted_at.desc()).limit(1)
        with self.session_factory() as session:
            row = session.scalar(statement)
        return None if row is None else self.row(row)

    def list_filings_before(
        self,
        *,
        issuer_id: str,
        as_of: str | datetime,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        as_of_dt = parse_datetime(as_of)
        if as_of_dt is None:
            raise ValueError("as_of is required")
        statement = (
            select(FilingModel)
            .where(FilingModel.issuer_id == issuer_id)
            .where(FilingModel.available_at <= as_of_dt)
            .order_by(FilingModel.available_at.desc(), FilingModel.accepted_at.desc())
            .limit(limit)
        )
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)

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

    def get_latest_statement_snapshot_before(
        self,
        *,
        issuer_id: str,
        as_of: str | datetime,
    ) -> dict[str, Any] | None:
        as_of_dt = parse_datetime(as_of)
        if as_of_dt is None:
            raise ValueError("as_of is required")
        statement = (
            select(StatementPeriodSnapshotModel)
            .where(StatementPeriodSnapshotModel.issuer_id == issuer_id)
            .where(StatementPeriodSnapshotModel.available_at <= as_of_dt)
            .order_by(
                StatementPeriodSnapshotModel.period_end.desc(),
                StatementPeriodSnapshotModel.available_at.desc(),
            )
            .limit(24)
        )
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        if not rows:
            return None
        row = sorted(rows, key=self._statement_snapshot_sort_key, reverse=True)[0]
        return None if row is None else self.row(row)

    def list_statement_snapshots_before(
        self,
        *,
        issuer_id: str,
        as_of: str | datetime,
        limit: int = 12,
    ) -> list[dict[str, Any]]:
        as_of_dt = parse_datetime(as_of)
        if as_of_dt is None:
            raise ValueError("as_of is required")
        fetch_limit = max(limit * 4, limit)
        statement = (
            select(StatementPeriodSnapshotModel)
            .where(StatementPeriodSnapshotModel.issuer_id == issuer_id)
            .where(StatementPeriodSnapshotModel.available_at <= as_of_dt)
            .order_by(
                StatementPeriodSnapshotModel.period_end.desc(),
                StatementPeriodSnapshotModel.available_at.desc(),
            )
            .limit(fetch_limit)
        )
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        ordered = sorted(rows, key=self._statement_snapshot_sort_key, reverse=True)[:limit]
        return self.rows(ordered)

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

    def list_insider_transactions_before(
        self,
        *,
        issuer_id: str,
        as_of: str | datetime,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        as_of_dt = parse_datetime(as_of)
        if as_of_dt is None:
            raise ValueError("as_of is required")
        statement = (
            select(InsiderTransactionModel)
            .where(InsiderTransactionModel.issuer_id == issuer_id)
            .where(InsiderTransactionModel.available_at <= as_of_dt)
            .order_by(
                InsiderTransactionModel.available_at.desc(),
                InsiderTransactionModel.transaction_date.desc(),
            )
            .limit(limit)
        )
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)

    def list_beneficial_owner_positions_before(
        self,
        *,
        issuer_id: str,
        as_of: str | datetime,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        as_of_dt = parse_datetime(as_of)
        if as_of_dt is None:
            raise ValueError("as_of is required")
        statement = (
            select(BeneficialOwnerPositionModel)
            .where(BeneficialOwnerPositionModel.issuer_id == issuer_id)
            .where(BeneficialOwnerPositionModel.available_at <= as_of_dt)
            .order_by(
                BeneficialOwnerPositionModel.available_at.desc(),
                BeneficialOwnerPositionModel.event_date.desc(),
            )
            .limit(limit)
        )
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)

    def list_institutional_positions_before(
        self,
        *,
        issuer_id: str,
        as_of: str | datetime,
        limit: int = 20000,
    ) -> list[dict[str, Any]]:
        as_of_dt = parse_datetime(as_of)
        if as_of_dt is None:
            raise ValueError("as_of is required")
        statement = (
            select(InstitutionalPositionModel)
            .where(InstitutionalPositionModel.issuer_id == issuer_id)
            .where(InstitutionalPositionModel.available_at <= as_of_dt)
            .order_by(
                InstitutionalPositionModel.available_at.desc(),
                InstitutionalPositionModel.report_period.desc(),
                InstitutionalPositionModel.market_value_reported.desc().nullslast(),
            )
            .limit(limit)
        )
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)

    def list_unresolved_institutional_positions(
        self,
        *,
        report_period: Any | None = None,
        statuses: tuple[str, ...] | None = None,
        due_before: str | datetime | None = None,
        limit: int = 10000,
    ) -> list[dict[str, Any]]:
        statement = select(UnresolvedInstitutionalPositionModel)
        if report_period is not None:
            statement = statement.where(UnresolvedInstitutionalPositionModel.report_period == parse_date(report_period))
        if statuses:
            statement = statement.where(UnresolvedInstitutionalPositionModel.resolution_status.in_(statuses))
        due_before_dt = parse_datetime(due_before) if due_before is not None else None
        if due_before_dt is not None:
            statement = statement.where(
                or_(
                    UnresolvedInstitutionalPositionModel.next_retry_at.is_(None),
                    UnresolvedInstitutionalPositionModel.next_retry_at <= due_before_dt,
                )
            )
        statement = statement.order_by(
            UnresolvedInstitutionalPositionModel.report_period.desc(),
            UnresolvedInstitutionalPositionModel.market_value_reported.desc().nullslast(),
            UnresolvedInstitutionalPositionModel.source_row_hash.asc(),
        ).limit(limit)
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)

    def update_unresolved_institutional_positions(
        self,
        *,
        source_row_hashes: list[str],
        updates: dict[str, Any],
    ) -> int:
        normalized = [str(value) for value in source_row_hashes if str(value).strip()]
        if not normalized:
            return 0
        with self.session_scope() as session:
            rows = session.scalars(
                select(UnresolvedInstitutionalPositionModel).where(UnresolvedInstitutionalPositionModel.source_row_hash.in_(normalized))
            ).all()
            for row in rows:
                self._assign_model(row, updates)
        return len(normalized)

    def get_primary_security(self, *, issuer_id: str) -> dict[str, Any] | None:
        statement = (
            select(SecurityModel)
            .where(SecurityModel.issuer_id == issuer_id)
            .order_by(SecurityModel.is_primary.desc(), SecurityModel.created_at.desc())
            .limit(1)
        )
        with self.session_factory() as session:
            row = session.scalar(statement)
        return None if row is None else self.row(row)
