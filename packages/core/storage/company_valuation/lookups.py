from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import and_, or_, select

from core.storage.company_valuation_models import (
    IssuerClassificationModel,
    IssuerModel,
    IssuerOverlayFlagModel,
    MarketSnapshotModel,
    SecurityIdentifierHistoryModel,
    SecurityModel,
    StatementPeriodSnapshotModel,
)
from core.storage.serializers import parse_date, parse_datetime


class CompanyValuationLookupMixin:
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

    def list_issuers(
        self,
        *,
        issuer_ids: tuple[str, ...] | None = None,
        ciks: tuple[str, ...] | None = None,
        tickers: tuple[str, ...] | None = None,
        has_raw_classification: bool | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
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
        normalized_issuer_ids = tuple(str(value).strip() for value in (issuer_ids or ()) if str(value or "").strip())
        if normalized_issuer_ids:
            statement = statement.where(IssuerModel.issuer_id.in_(normalized_issuer_ids))
        normalized_ciks = tuple(str(value).strip() for value in (ciks or ()) if str(value or "").strip())
        if normalized_ciks:
            statement = statement.where(IssuerModel.cik.in_(normalized_ciks))
        normalized_tickers = tuple(str(value).upper().strip() for value in (tickers or ()) if str(value or "").strip())
        if normalized_tickers:
            statement = statement.where(SecurityModel.ticker.in_(normalized_tickers))
        if has_raw_classification is True:
            statement = statement.where(
                IssuerModel.sic.is_not(None),
                IssuerModel.sic != "",
                IssuerModel.sic_description.is_not(None),
                IssuerModel.sic_description != "",
            )
        elif has_raw_classification is False:
            statement = statement.where(
                or_(
                    IssuerModel.sic.is_(None),
                    IssuerModel.sic == "",
                    IssuerModel.sic_description.is_(None),
                    IssuerModel.sic_description == "",
                )
            )
        if limit is not None:
            statement = statement.limit(limit)
        with self.session_factory() as session:
            rows = session.execute(statement).all()
        return [self.row(issuer_model, extra={"ticker": resolved_ticker}) for issuer_model, resolved_ticker in rows]

    def get_issuer_classification(
        self,
        *,
        issuer_id: str,
    ) -> dict[str, Any] | None:
        statement = (
            select(
                IssuerClassificationModel,
                IssuerModel.sic.label("raw_sic_code"),
                IssuerModel.sic_description.label("raw_sic_title"),
                IssuerModel.naics.label("raw_naics_code"),
            )
            .join(IssuerModel, IssuerModel.issuer_id == IssuerClassificationModel.issuer_id)
            .where(IssuerClassificationModel.issuer_id == str(issuer_id))
        )
        with self.session_factory() as session:
            row = session.execute(statement).first()
        if row is None:
            return None
        classification_model, raw_sic_code, raw_sic_title, raw_naics_code = row
        return self.row(
            classification_model,
            extra={
                "raw_sic_code": raw_sic_code,
                "raw_sic_title": raw_sic_title,
                "raw_naics_code": raw_naics_code,
                "raw_naics_title": None,
            },
        )

    def list_issuer_overlay_flags(
        self,
        *,
        issuer_id: str,
        active_only: bool = False,
    ) -> list[dict[str, Any]]:
        statement = select(IssuerOverlayFlagModel).where(IssuerOverlayFlagModel.issuer_id == str(issuer_id))
        if active_only:
            statement = statement.where(IssuerOverlayFlagModel.active.is_(True))
        statement = statement.order_by(IssuerOverlayFlagModel.flag_key.asc())
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)

    def list_issuers_for_screening(
        self,
        *,
        as_of: str | datetime,
        template_id: str | None = None,
        tickers: tuple[str, ...] | None = None,
        stressed_operator_only: bool = False,
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
        if stressed_operator_only:
            statement = statement.where(IssuerModel.stressed_operator_flag.is_(True))
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
        return [self.row(issuer_model, extra={"ticker": resolved_ticker}) for issuer_model, resolved_ticker in rows]

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
