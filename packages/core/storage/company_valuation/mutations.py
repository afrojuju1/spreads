from __future__ import annotations

from typing import Any

from sqlalchemy import delete, select

from core.storage.company_valuation_models import (
    BeneficialOwnerGroupMembershipModel,
    BeneficialOwnerGroupModel,
    BeneficialOwnerModel,
    BeneficialOwnershipFilingModel,
    BeneficialOwnerPositionModel,
    FilingModel,
    IssuerClassificationModel,
    InstitutionalFilingModel,
    InstitutionalHolderModel,
    InstitutionalPositionModel,
    IssuerModel,
    IssuerOverlayFlagModel,
    MarketSnapshotModel,
    SecurityIdentifierHistoryModel,
    SecurityModel,
    StatementPeriodSnapshotModel,
    TaxonomyMappingModel,
    TaxonomyNodeModel,
    TreasuryCurveSnapshotModel,
    UnresolvedInstitutionalPositionModel,
    ValuationTemplateMappingModel,
    InsiderTransactionModel,
    XbrlFactModel,
)


class CompanyValuationMutationMixin:
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

    def upsert_taxonomy_nodes(
        self,
        payloads: list[dict[str, Any]],
    ) -> int:
        if not payloads:
            return 0
        with self.session_scope() as session:
            for payload in payloads:
                taxonomy_node_id = str(payload["taxonomy_node_id"])
                row = session.get(TaxonomyNodeModel, taxonomy_node_id)
                if row is None:
                    row = TaxonomyNodeModel(taxonomy_node_id=taxonomy_node_id)
                    session.add(row)
                self._assign_model(row, payload)
        return len(payloads)

    def upsert_taxonomy_mappings(
        self,
        payloads: list[dict[str, Any]],
    ) -> int:
        if not payloads:
            return 0
        with self.session_scope() as session:
            for payload in payloads:
                mapping_id = str(payload["mapping_id"])
                row = session.get(TaxonomyMappingModel, mapping_id)
                if row is None:
                    row = TaxonomyMappingModel(mapping_id=mapping_id)
                    session.add(row)
                self._assign_model(row, payload)
        return len(payloads)

    def upsert_valuation_template_mappings(
        self,
        payloads: list[dict[str, Any]],
    ) -> int:
        if not payloads:
            return 0
        with self.session_scope() as session:
            for payload in payloads:
                mapping_id = str(payload["mapping_id"])
                row = session.get(ValuationTemplateMappingModel, mapping_id)
                if row is None:
                    row = ValuationTemplateMappingModel(mapping_id=mapping_id)
                    session.add(row)
                self._assign_model(row, payload)
        return len(payloads)

    def upsert_issuer_classification(
        self,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        issuer_id = str(payload["issuer_id"])
        with self.session_scope() as session:
            row = session.get(IssuerClassificationModel, issuer_id)
            if row is None:
                row = IssuerClassificationModel(issuer_id=issuer_id)
                session.add(row)
            self._assign_model(row, self._preserve_created_at(row, payload))
        return self.get_issuer_classification(issuer_id=issuer_id) or {}

    def upsert_issuer_overlay_flags(
        self,
        payloads: list[dict[str, Any]],
    ) -> int:
        if not payloads:
            return 0
        with self.session_scope() as session:
            for payload in payloads:
                flag_id = str(payload["issuer_overlay_flag_id"])
                row = session.get(IssuerOverlayFlagModel, flag_id)
                if row is None:
                    row = IssuerOverlayFlagModel(issuer_overlay_flag_id=flag_id)
                    session.add(row)
                self._assign_model(row, self._preserve_created_at(row, payload))
        return len(payloads)

    def replace_issuer_overlay_flags(
        self,
        *,
        issuer_id: str,
        payloads: list[dict[str, Any]],
    ) -> int:
        normalized_issuer_id = str(issuer_id)
        with self.session_scope() as session:
            session.execute(delete(IssuerOverlayFlagModel).where(IssuerOverlayFlagModel.issuer_id == normalized_issuer_id))
            session.add_all(IssuerOverlayFlagModel(**payload) for payload in payloads)
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
                    row = SecurityIdentifierHistoryModel(security_identifier_id=identifier_id)
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
                    row = InstitutionalHolderModel(institutional_holder_id=holder_id)
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
        statement = select(TreasuryCurveSnapshotModel).where(TreasuryCurveSnapshotModel.curve_snapshot_id == curve_snapshot_id)
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
        statement = select(MarketSnapshotModel).where(MarketSnapshotModel.market_snapshot_id == market_snapshot_id)
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
            session.execute(delete(InsiderTransactionModel).where(InsiderTransactionModel.filing_id.in_(normalized_ids)))
            session.add_all(InsiderTransactionModel(**payload) for payload in transaction_payloads)
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
            session.execute(delete(BeneficialOwnerGroupMembershipModel).where(BeneficialOwnerGroupMembershipModel.filing_id.in_(normalized_ids)))
            session.execute(delete(BeneficialOwnerPositionModel).where(BeneficialOwnerPositionModel.filing_id.in_(normalized_ids)))
            session.execute(delete(BeneficialOwnerGroupModel).where(BeneficialOwnerGroupModel.root_filing_id.in_(normalized_ids)))
            session.execute(delete(BeneficialOwnershipFilingModel).where(BeneficialOwnershipFilingModel.filing_id.in_(normalized_ids)))
            session.add_all(BeneficialOwnershipFilingModel(**payload) for payload in filing_payloads)
            session.add_all(BeneficialOwnerGroupModel(**payload) for payload in group_payloads)
            session.add_all(BeneficialOwnerGroupMembershipModel(**payload) for payload in membership_payloads)
            session.add_all(BeneficialOwnerPositionModel(**payload) for payload in position_payloads)
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
            session.execute(delete(InstitutionalPositionModel).where(InstitutionalPositionModel.filing_id.in_(normalized_ids)))
            session.add_all(InstitutionalPositionModel(**payload) for payload in position_payloads)
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
            session.execute(delete(InstitutionalPositionModel).where(InstitutionalPositionModel.filing_id.in_(normalized_ids)))
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
                row = session.scalar(select(InstitutionalPositionModel).where(InstitutionalPositionModel.source_row_hash == source_row_hash))
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
                    row = UnresolvedInstitutionalPositionModel(source_row_hash=source_row_hash)
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
            session.execute(delete(UnresolvedInstitutionalPositionModel).where(UnresolvedInstitutionalPositionModel.filing_id.in_(normalized_ids)))
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
            session.execute(delete(StatementPeriodSnapshotModel).where(StatementPeriodSnapshotModel.filing_id.in_(normalized_ids)))
            session.execute(delete(XbrlFactModel).where(XbrlFactModel.filing_id.in_(normalized_ids)))
            session.add_all(XbrlFactModel(**payload) for payload in fact_payloads)
            session.add_all(StatementPeriodSnapshotModel(**payload) for payload in snapshot_payloads)
        return (len(fact_payloads), len(snapshot_payloads))
