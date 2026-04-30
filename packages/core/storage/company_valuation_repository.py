from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import and_, delete, func, or_, select

from core.storage.base import RepositoryBase
from core.storage.company_valuation_models import (
    BeneficialOwnerGroupMembershipModel,
    BeneficialOwnerGroupModel,
    BeneficialOwnerModel,
    BeneficialOwnershipFilingModel,
    BeneficialOwnerPositionModel,
    CompanyValuationSnapshotModel,
    FeatureSnapshotModel,
    FilingModel,
    InstitutionalFilingModel,
    InstitutionalHolderModel,
    InstitutionalPositionModel,
    IssuerModel,
    MarketSnapshotModel,
    ScreeningRowModel,
    SecurityIdentifierHistoryModel,
    SecurityModel,
    StatementPeriodSnapshotModel,
    TreasuryCurveSnapshotModel,
    UnresolvedInstitutionalPositionModel,
    InsiderTransactionModel,
    XbrlFactModel,
)
from core.storage.serializers import parse_date, parse_datetime


class CompanyValuationRepository(RepositoryBase):
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

    def upsert_issuer(
        self,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        issuer_id = str(payload["issuer_id"])
        with self.session_scope() as session:
            row = session.get(IssuerModel, issuer_id)
            if row is None:
                row = IssuerModel(issuer_id=issuer_id)
                session.add(row)
            self._assign_model(row, self._preserve_created_at(row, payload))
        return self.get_issuer(issuer_id=issuer_id) or {}

    def upsert_issuers(
        self,
        payloads: list[dict[str, Any]],
    ) -> int:
        if not payloads:
            return 0
        with self.session_scope() as session:
            for payload in payloads:
                issuer_id = str(payload["issuer_id"])
                row = session.get(IssuerModel, issuer_id)
                if row is None:
                    row = IssuerModel(issuer_id=issuer_id)
                    session.add(row)
                self._assign_model(row, self._preserve_created_at(row, payload))
        return len(payloads)

    def upsert_securities(
        self,
        payloads: list[dict[str, Any]],
    ) -> int:
        if not payloads:
            return 0
        with self.session_scope() as session:
            for payload in payloads:
                security_id = str(payload["security_id"])
                row = session.get(SecurityModel, security_id)
                if row is None:
                    row = SecurityModel(security_id=security_id)
                    session.add(row)
                self._assign_model(row, self._preserve_created_at(row, payload))
        return len(payloads)

    def upsert_security_identifier_history(
        self,
        payloads: list[dict[str, Any]],
    ) -> int:
        if not payloads:
            return 0
        with self.session_scope() as session:
            for payload in payloads:
                identifier_id = str(payload["security_identifier_id"])
                row = session.get(SecurityIdentifierHistoryModel, identifier_id)
                if row is None:
                    row = SecurityIdentifierHistoryModel(
                        security_identifier_id=identifier_id
                    )
                    session.add(row)
                self._assign_model(row, self._preserve_created_at(row, payload))
        return len(payloads)

    def upsert_filings(
        self,
        payloads: list[dict[str, Any]],
    ) -> int:
        if not payloads:
            return 0
        with self.session_scope() as session:
            for payload in payloads:
                filing_id = str(payload["filing_id"])
                row = session.get(FilingModel, filing_id)
                if row is None:
                    row = FilingModel(filing_id=filing_id)
                    session.add(row)
                self._assign_model(row, self._preserve_created_at(row, payload))
        return len(payloads)

    def upsert_institutional_holders(
        self,
        payloads: list[dict[str, Any]],
    ) -> int:
        if not payloads:
            return 0
        with self.session_scope() as session:
            for payload in payloads:
                holder_id = str(payload["institutional_holder_id"])
                row = session.get(InstitutionalHolderModel, holder_id)
                if row is None:
                    row = InstitutionalHolderModel(
                        institutional_holder_id=holder_id
                    )
                    session.add(row)
                self._assign_model(row, self._preserve_created_at(row, payload))
        return len(payloads)

    def upsert_institutional_filings(
        self,
        payloads: list[dict[str, Any]],
    ) -> int:
        if not payloads:
            return 0
        with self.session_scope() as session:
            for payload in payloads:
                filing_id = str(payload["filing_id"])
                row = session.get(InstitutionalFilingModel, filing_id)
                if row is None:
                    row = InstitutionalFilingModel(filing_id=filing_id)
                    session.add(row)
                self._assign_model(row, self._preserve_created_at(row, payload))
        return len(payloads)

    def upsert_beneficial_owners(
        self,
        payloads: list[dict[str, Any]],
    ) -> int:
        if not payloads:
            return 0
        with self.session_scope() as session:
            for payload in payloads:
                holder_id = str(payload["holder_id"])
                row = session.get(BeneficialOwnerModel, holder_id)
                if row is None:
                    row = BeneficialOwnerModel(holder_id=holder_id)
                    session.add(row)
                self._assign_model(row, self._preserve_created_at(row, payload))
        return len(payloads)

    def upsert_treasury_curve_snapshot(
        self,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        curve_snapshot_id = str(payload["curve_snapshot_id"])
        with self.session_scope() as session:
            row = session.get(TreasuryCurveSnapshotModel, curve_snapshot_id)
            if row is None:
                row = TreasuryCurveSnapshotModel(curve_snapshot_id=curve_snapshot_id)
                session.add(row)
            self._assign_model(row, payload)
        statement = select(TreasuryCurveSnapshotModel).where(
            TreasuryCurveSnapshotModel.curve_snapshot_id == curve_snapshot_id
        )
        with self.session_factory() as session:
            row = session.scalar(statement)
        return {} if row is None else self.row(row)

    def upsert_market_snapshot(
        self,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        market_snapshot_id = str(payload["market_snapshot_id"])
        with self.session_scope() as session:
            row = session.get(MarketSnapshotModel, market_snapshot_id)
            if row is None:
                row = MarketSnapshotModel(market_snapshot_id=market_snapshot_id)
                session.add(row)
            self._assign_model(row, payload)
        statement = select(MarketSnapshotModel).where(
            MarketSnapshotModel.market_snapshot_id == market_snapshot_id
        )
        with self.session_factory() as session:
            row = session.scalar(statement)
        return {} if row is None else self.row(row)

    def replace_insider_transactions_for_filings(
        self,
        *,
        filing_ids: list[str],
        transaction_payloads: list[dict[str, Any]],
    ) -> int:
        normalized_ids = [str(value) for value in filing_ids if str(value).strip()]
        if not normalized_ids:
            return 0
        with self.session_scope() as session:
            session.execute(
                delete(InsiderTransactionModel).where(
                    InsiderTransactionModel.filing_id.in_(normalized_ids)
                )
            )
            session.add_all(
                InsiderTransactionModel(**payload) for payload in transaction_payloads
            )
        return len(transaction_payloads)

    def replace_beneficial_ownership_for_filings(
        self,
        *,
        filing_ids: list[str],
        filing_payloads: list[dict[str, Any]],
        group_payloads: list[dict[str, Any]],
        membership_payloads: list[dict[str, Any]],
        position_payloads: list[dict[str, Any]],
    ) -> tuple[int, int, int, int]:
        normalized_ids = [str(value) for value in filing_ids if str(value).strip()]
        if not normalized_ids:
            return (0, 0, 0, 0)
        with self.session_scope() as session:
            session.execute(
                delete(BeneficialOwnerGroupMembershipModel).where(
                    BeneficialOwnerGroupMembershipModel.filing_id.in_(normalized_ids)
                )
            )
            session.execute(
                delete(BeneficialOwnerPositionModel).where(
                    BeneficialOwnerPositionModel.filing_id.in_(normalized_ids)
                )
            )
            session.execute(
                delete(BeneficialOwnerGroupModel).where(
                    BeneficialOwnerGroupModel.root_filing_id.in_(normalized_ids)
                )
            )
            session.execute(
                delete(BeneficialOwnershipFilingModel).where(
                    BeneficialOwnershipFilingModel.filing_id.in_(normalized_ids)
                )
            )
            session.add_all(
                BeneficialOwnershipFilingModel(**payload) for payload in filing_payloads
            )
            session.add_all(BeneficialOwnerGroupModel(**payload) for payload in group_payloads)
            session.add_all(
                BeneficialOwnerGroupMembershipModel(**payload)
                for payload in membership_payloads
            )
            session.add_all(
                BeneficialOwnerPositionModel(**payload) for payload in position_payloads
            )
        return (
            len(filing_payloads),
            len(group_payloads),
            len(membership_payloads),
            len(position_payloads),
        )

    def replace_institutional_positions_for_filings(
        self,
        *,
        filing_ids: list[str],
        position_payloads: list[dict[str, Any]],
    ) -> int:
        normalized_ids = [str(value) for value in filing_ids if str(value).strip()]
        if not normalized_ids:
            return 0
        with self.session_scope() as session:
            session.execute(
                delete(InstitutionalPositionModel).where(
                    InstitutionalPositionModel.filing_id.in_(normalized_ids)
                )
            )
            session.add_all(
                InstitutionalPositionModel(**payload) for payload in position_payloads
            )
        return len(position_payloads)

    def delete_institutional_positions_for_filings(
        self,
        *,
        filing_ids: list[str],
    ) -> int:
        normalized_ids = [str(value) for value in filing_ids if str(value).strip()]
        if not normalized_ids:
            return 0
        with self.session_scope() as session:
            session.execute(
                delete(InstitutionalPositionModel).where(
                    InstitutionalPositionModel.filing_id.in_(normalized_ids)
                )
            )
        return len(normalized_ids)

    def upsert_institutional_positions(
        self,
        payloads: list[dict[str, Any]],
    ) -> int:
        if not payloads:
            return 0
        with self.session_scope() as session:
            for payload in payloads:
                source_row_hash = str(payload["source_row_hash"])
                row = session.scalar(
                    select(InstitutionalPositionModel).where(
                        InstitutionalPositionModel.source_row_hash == source_row_hash
                    )
                )
                if row is None:
                    row = InstitutionalPositionModel()
                    session.add(row)
                self._assign_model(row, payload)
        return len(payloads)

    def upsert_unresolved_institutional_positions(
        self,
        payloads: list[dict[str, Any]],
    ) -> int:
        if not payloads:
            return 0
        with self.session_scope() as session:
            for payload in payloads:
                source_row_hash = str(payload["source_row_hash"])
                row = session.get(
                    UnresolvedInstitutionalPositionModel,
                    source_row_hash,
                )
                if row is None:
                    row = UnresolvedInstitutionalPositionModel(
                        source_row_hash=source_row_hash
                    )
                    session.add(row)
                self._assign_model(row, self._preserve_created_at(row, payload))
        return len(payloads)

    def delete_unresolved_institutional_positions_for_filings(
        self,
        *,
        filing_ids: list[str],
    ) -> int:
        normalized_ids = [str(value) for value in filing_ids if str(value).strip()]
        if not normalized_ids:
            return 0
        with self.session_scope() as session:
            session.execute(
                delete(UnresolvedInstitutionalPositionModel).where(
                    UnresolvedInstitutionalPositionModel.filing_id.in_(normalized_ids)
                )
            )
        return len(normalized_ids)

    def replace_filing_facts_and_snapshots(
        self,
        *,
        filing_ids: list[str],
        fact_payloads: list[dict[str, Any]],
        snapshot_payloads: list[dict[str, Any]],
    ) -> tuple[int, int]:
        normalized_ids = [str(value) for value in filing_ids if str(value).strip()]
        if not normalized_ids:
            return (0, 0)
        with self.session_scope() as session:
            session.execute(
                delete(StatementPeriodSnapshotModel).where(
                    StatementPeriodSnapshotModel.filing_id.in_(normalized_ids)
                )
            )
            session.execute(
                delete(XbrlFactModel).where(XbrlFactModel.filing_id.in_(normalized_ids))
            )
            session.add_all(XbrlFactModel(**payload) for payload in fact_payloads)
            session.add_all(
                StatementPeriodSnapshotModel(**payload) for payload in snapshot_payloads
            )
        return (len(fact_payloads), len(snapshot_payloads))

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

    def list_issuers(self) -> list[dict[str, Any]]:
        statement = (
            select(IssuerModel, SecurityModel.ticker.label("ticker"))
            .join(
                SecurityModel,
                and_(
                    SecurityModel.issuer_id == IssuerModel.issuer_id,
                    SecurityModel.is_primary.is_(True),
                ),
                isouter=True,
            )
            .order_by(
                IssuerModel.company_name.asc(),
                SecurityModel.created_at.desc(),
            )
        )
        with self.session_factory() as session:
            rows = session.execute(statement).all()
        return [
            self.row(issuer_model, extra={"ticker": resolved_ticker})
            for issuer_model, resolved_ticker in rows
        ]

    def list_issuers_for_screening(
        self,
        *,
        as_of: str | datetime,
        template_id: str | None = None,
        tickers: tuple[str, ...] | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        as_of_dt = parse_datetime(as_of)
        if as_of_dt is None:
            raise ValueError("as_of is required")
        statement = (
            select(IssuerModel, SecurityModel.ticker.label("ticker"))
            .join(
                StatementPeriodSnapshotModel,
                StatementPeriodSnapshotModel.issuer_id == IssuerModel.issuer_id,
            )
            .join(
                MarketSnapshotModel,
                MarketSnapshotModel.issuer_id == IssuerModel.issuer_id,
            )
            .join(
                SecurityModel,
                and_(
                    SecurityModel.issuer_id == IssuerModel.issuer_id,
                    SecurityModel.is_primary.is_(True),
                ),
                isouter=True,
            )
            .where(StatementPeriodSnapshotModel.available_at <= as_of_dt)
            .where(MarketSnapshotModel.available_at <= as_of_dt)
        )
        if template_id:
            statement = statement.where(IssuerModel.template_id == template_id)
        normalized_tickers = tuple(str(value).upper().strip() for value in (tickers or ()) if str(value or "").strip())
        if normalized_tickers:
            statement = statement.where(SecurityModel.ticker.in_(normalized_tickers))
        statement = statement.where(
            or_(
                IssuerModel.template_assignment_source != "openfigi_seed",
                IssuerModel.limited_coverage_flag.is_(False),
            )
        )
        statement = statement.distinct(IssuerModel.issuer_id, SecurityModel.ticker)
        statement = statement.order_by(IssuerModel.issuer_id.asc())
        if limit is not None:
            statement = statement.limit(limit)
        with self.session_factory() as session:
            rows = session.execute(statement).all()
        return [
            self.row(issuer_model, extra={"ticker": resolved_ticker})
            for issuer_model, resolved_ticker in rows
        ]

    def get_security(
        self,
        *,
        security_id: str | None = None,
        issuer_id: str | None = None,
        ticker: str | None = None,
        cusip: str | None = None,
    ) -> dict[str, Any] | None:
        statement = select(SecurityModel)
        if security_id:
            statement = statement.where(SecurityModel.security_id == security_id)
        if issuer_id:
            statement = statement.where(SecurityModel.issuer_id == issuer_id)
        if ticker:
            statement = statement.where(SecurityModel.ticker == str(ticker).upper())
        if cusip:
            statement = statement.where(SecurityModel.cusip == str(cusip).upper())
        statement = statement.order_by(
            SecurityModel.is_primary.desc(),
            SecurityModel.created_at.desc(),
        ).limit(1)
        with self.session_factory() as session:
            row = session.scalar(statement)
        return None if row is None else self.row(row)

    def find_security_identifier_mapping(
        self,
        *,
        identifier_type: str,
        identifier_value: str,
        effective_on: Any | None = None,
    ) -> dict[str, Any] | None:
        statement = select(SecurityIdentifierHistoryModel).where(
            SecurityIdentifierHistoryModel.identifier_type == str(identifier_type),
            SecurityIdentifierHistoryModel.identifier_value == str(identifier_value),
        )
        if effective_on is not None:
            effective_date = parse_date(effective_on)
            if effective_date is not None:
                statement = statement.where(
                    or_(
                        SecurityIdentifierHistoryModel.effective_from.is_(None),
                        SecurityIdentifierHistoryModel.effective_from <= effective_date,
                    ),
                    or_(
                        SecurityIdentifierHistoryModel.effective_to.is_(None),
                        SecurityIdentifierHistoryModel.effective_to >= effective_date,
                    ),
                )
        statement = statement.order_by(
            SecurityIdentifierHistoryModel.match_confidence.desc(),
            SecurityIdentifierHistoryModel.effective_from.desc().nullslast(),
            SecurityIdentifierHistoryModel.updated_at.desc(),
        ).limit(1)
        with self.session_factory() as session:
            row = session.scalar(statement)
        return None if row is None else self.row(row)

    def list_security_identifier_mappings(
        self,
        *,
        identifier_type: str,
        identifier_values: list[str],
        effective_on: Any | None = None,
    ) -> list[dict[str, Any]]:
        normalized_values = [str(value) for value in identifier_values if str(value).strip()]
        if not normalized_values:
            return []
        statement = select(SecurityIdentifierHistoryModel).where(
            SecurityIdentifierHistoryModel.identifier_type == str(identifier_type),
            SecurityIdentifierHistoryModel.identifier_value.in_(normalized_values),
        )
        if effective_on is not None:
            effective_date = parse_date(effective_on)
            if effective_date is not None:
                statement = statement.where(
                    or_(
                        SecurityIdentifierHistoryModel.effective_from.is_(None),
                        SecurityIdentifierHistoryModel.effective_from <= effective_date,
                    ),
                    or_(
                        SecurityIdentifierHistoryModel.effective_to.is_(None),
                        SecurityIdentifierHistoryModel.effective_to >= effective_date,
                    ),
                )
        statement = statement.order_by(
            SecurityIdentifierHistoryModel.identifier_value.asc(),
            SecurityIdentifierHistoryModel.match_confidence.desc(),
            SecurityIdentifierHistoryModel.effective_from.desc().nullslast(),
            SecurityIdentifierHistoryModel.updated_at.desc(),
        )
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)

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
            statement = statement.where(
                UnresolvedInstitutionalPositionModel.report_period == parse_date(report_period)
            )
        if statuses:
            statement = statement.where(
                UnresolvedInstitutionalPositionModel.resolution_status.in_(statuses)
            )
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
                select(UnresolvedInstitutionalPositionModel).where(
                    UnresolvedInstitutionalPositionModel.source_row_hash.in_(normalized)
                )
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
        statement = select(FeatureSnapshotModel).where(
            FeatureSnapshotModel.feature_snapshot_id == feature_snapshot_id
        )
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
                row = CompanyValuationSnapshotModel(
                    company_valuation_snapshot_id=snapshot_id
                )
                session.add(row)
            self._assign_model(row, payload)
        statement = select(CompanyValuationSnapshotModel).where(
            CompanyValuationSnapshotModel.company_valuation_snapshot_id == snapshot_id
        )
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
        statement = select(ScreeningRowModel).where(
            ScreeningRowModel.screening_row_id == screening_row_id
        )
        with self.session_factory() as session:
            row = session.scalar(statement)
        return {} if row is None else self.row(row)

    def list_screening_rows(
        self,
        *,
        as_of: str,
        template_id: str | None = None,
        tickers: tuple[str, ...] | None = None,
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
