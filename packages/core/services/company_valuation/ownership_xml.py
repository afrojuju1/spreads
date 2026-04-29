from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any
from xml.etree import ElementTree as ET

from core.common import parse_float, parse_int
from core.services.company_valuation.ids import build_group_id, build_holder_id

FORM345_ROOT_TAG = "ownershipDocument"
SCHEDULE_13D_NAMESPACE = "http://www.sec.gov/edgar/schedule13D"
SCHEDULE_13G_NAMESPACE = "http://www.sec.gov/edgar/schedule13g"
COMMON_NAMESPACE = "http://www.sec.gov/edgar/common"


def _normalized_name(value: str | None) -> str:
    lowered = str(value or "").lower().strip()
    lowered = re.sub(r"\s+", " ", lowered)
    return re.sub(r"[^a-z0-9 ]", "", lowered)


def _truthy_flag(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y"}


def _parse_sec_date(value: str | None) -> date | None:
    rendered = str(value or "").strip()
    if not rendered:
        return None
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", rendered):
        return date.fromisoformat(rendered)
    if re.fullmatch(r"\d{2}/\d{2}/\d{4}", rendered):
        month, day, year = rendered.split("/")
        return date(int(year), int(month), int(day))
    return None


def _text(element: ET.Element | None, path: str) -> str | None:
    if element is None:
        return None
    node = element.find(path)
    if node is None or node.text is None:
        return None
    rendered = node.text.strip()
    return rendered or None


def _ns_text(element: ET.Element | None, path: str, namespaces: dict[str, str]) -> str | None:
    if element is None:
        return None
    node = element.find(path, namespaces)
    if node is None or node.text is None:
        return None
    rendered = node.text.strip()
    return rendered or None


def _root_namespace(tag: str) -> str | None:
    if not tag.startswith("{") or "}" not in tag:
        return None
    return tag[1:].split("}", 1)[0]


def _footnote_lookup_form345(root: ET.Element) -> dict[str, str]:
    lookup: dict[str, str] = {}
    for footnote in root.findall("footnotes/footnote"):
        footnote_id = footnote.get("id")
        if footnote_id and footnote.text:
            lookup[footnote_id] = footnote.text.strip()
    return lookup


def _owner_type_from_relationship(owner: ET.Element | None) -> str | None:
    relationship = owner.find("reportingOwnerRelationship") if owner is not None else None
    if relationship is None:
        return None
    if _truthy_flag(_text(relationship, "isOfficer")):
        officer_title = _text(relationship, "officerTitle")
        return "officer" if not officer_title else f"officer:{officer_title}"
    if _truthy_flag(_text(relationship, "isDirector")):
        return "director"
    if _truthy_flag(_text(relationship, "isTenPercentOwner")):
        return "ten_percent_owner"
    if _truthy_flag(_text(relationship, "isOther")):
        other_text = _text(relationship, "otherText")
        return "other" if not other_text else f"other:{other_text}"
    return None


def _transaction_footnotes(
    entry: ET.Element,
    footnote_lookup: dict[str, str],
) -> list[str]:
    notes: list[str] = []
    for footnote_id in entry.findall(".//footnoteId"):
        ref = footnote_id.get("id")
        if ref and ref in footnote_lookup:
            notes.append(footnote_lookup[ref])
    if not notes:
        notes.extend(footnote_lookup.values())
    return notes


@dataclass(frozen=True)
class InsiderOwnershipParseResult:
    owner_payloads: list[dict[str, Any]]
    transaction_payloads: list[dict[str, Any]]


def parse_form345_xml(
    *,
    xml_text: str,
    issuer_id: str,
    filing_id: str,
    filing_available_at: datetime,
    created_at: datetime,
) -> InsiderOwnershipParseResult:
    root = ET.fromstring(xml_text)
    if root.tag != FORM345_ROOT_TAG:
        raise ValueError("Expected SEC ownershipDocument XML")
    owners = root.findall("reportingOwner")
    owner_payloads: list[dict[str, Any]] = []
    holder_ids: list[str] = []
    for owner in owners:
        owner_id = owner.find("reportingOwnerId")
        cik = _text(owner_id, "rptOwnerCik")
        name = _text(owner_id, "rptOwnerName") or "UNKNOWN REPORTING OWNER"
        holder_id = build_holder_id(name, cik)
        holder_ids.append(holder_id)
        owner_payloads.append(
            {
                "holder_id": holder_id,
                "canonical_name": name,
                "normalized_name": _normalized_name(name),
                "holder_cik": cik,
                "holder_type": _owner_type_from_relationship(owner),
                "parent_holder_id": None,
                "created_at": created_at,
                "updated_at": created_at,
            }
        )

    primary_holder_id = holder_ids[0] if len(holder_ids) == 1 else None
    footnote_lookup = _footnote_lookup_form345(root)
    transaction_payloads: list[dict[str, Any]] = []
    tables = (
        ("nonDerivativeTable/nonDerivativeTransaction", "non_derivative"),
        ("derivativeTable/derivativeTransaction", "derivative"),
    )
    for path, security_type in tables:
        for entry in root.findall(path):
            transaction_payloads.append(
                {
                    "issuer_id": issuer_id,
                    "holder_id": primary_holder_id,
                    "filing_id": filing_id,
                    "transaction_date": _parse_sec_date(
                        _text(entry, "transactionDate/value")
                    ),
                    "transaction_code": _text(
                        entry,
                        "transactionCoding/transactionCode",
                    )
                    or "UNKNOWN",
                    "security_type": security_type,
                    "shares_delta": parse_float(
                        _text(entry, "transactionAmounts/transactionShares/value")
                    ),
                    "price": parse_float(
                        _text(
                            entry,
                            "transactionAmounts/transactionPricePerShare/value",
                        )
                    ),
                    "shares_owned_after": parse_float(
                        _text(
                            entry,
                            "postTransactionAmounts/sharesOwnedFollowingTransaction/value",
                        )
                    ),
                    "ownership_nature": _text(
                        entry,
                        "ownershipNature/directOrIndirectOwnership/value",
                    ),
                    "footnotes_json": _transaction_footnotes(entry, footnote_lookup),
                    "available_at": filing_available_at,
                }
            )
    return InsiderOwnershipParseResult(
        owner_payloads=owner_payloads,
        transaction_payloads=transaction_payloads,
    )


def _hash_position_row(*parts: str) -> str:
    payload = "|".join(part.strip() for part in parts)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class BeneficialOwnershipParseResult:
    owner_payloads: list[dict[str, Any]]
    filing_payload: dict[str, Any]
    group_payloads: list[dict[str, Any]]
    membership_payloads: list[dict[str, Any]]
    position_payloads: list[dict[str, Any]]


def parse_schedule_13d_g_xml(
    *,
    xml_text: str,
    issuer_id: str,
    issuer_cik: str,
    filing_id: str,
    filing_available_at: datetime,
    created_at: datetime,
    schedule_type: str,
) -> BeneficialOwnershipParseResult:
    root = ET.fromstring(xml_text)
    namespace = _root_namespace(root.tag) or SCHEDULE_13D_NAMESPACE
    namespaces = {"s": namespace, "c": COMMON_NAMESPACE, "com": COMMON_NAMESPACE}
    submission_type = _ns_text(root, "s:headerData/s:submissionType", namespaces) or schedule_type
    owner_payloads: list[dict[str, Any]] = []
    membership_payloads: list[dict[str, Any]] = []
    position_payloads: list[dict[str, Any]] = []
    group_payloads: list[dict[str, Any]] = []
    passive_flag = "13G" in submission_type.upper()
    control_intent_flag = "13D" in submission_type.upper()

    if namespace == SCHEDULE_13G_NAMESPACE:
        event_date = _parse_sec_date(
            _ns_text(
                root,
                "s:formData/s:coverPageHeader/s:eventDateRequiresFilingThisStatement",
                namespaces,
            )
        )
        reporting_people = root.findall(
            "s:formData/s:coverPageHeaderReportingPersonDetails",
            namespaces,
        )
        group_flag = len(reporting_people) > 1
        group_id = build_group_id(issuer_cik, filing_id) if group_flag else None
        if group_id is not None:
            names = [
                _ns_text(person, "s:reportingPersonName", namespaces) or "UNKNOWN"
                for person in reporting_people[:3]
            ]
            group_payloads.append(
                {
                    "group_id": group_id,
                    "issuer_id": issuer_id,
                    "group_name": "; ".join(names),
                    "group_kind": "joint_filing",
                    "root_filing_id": filing_id,
                    "effective_from": event_date,
                    "effective_to": None,
                    "created_at": created_at,
                }
            )
        for person in reporting_people:
            holder_name = _ns_text(person, "s:reportingPersonName", namespaces) or "UNKNOWN REPORTING PERSON"
            holder_id = build_holder_id(holder_name, None)
            holder_type = _ns_text(person, "s:typeOfReportingPerson", namespaces)
            owner_payloads.append(
                {
                    "holder_id": holder_id,
                    "canonical_name": holder_name,
                    "normalized_name": _normalized_name(holder_name),
                    "holder_cik": None,
                    "holder_type": holder_type,
                    "parent_holder_id": None,
                    "created_at": created_at,
                    "updated_at": created_at,
                }
            )
            if group_id is not None:
                membership_payloads.append(
                    {
                        "group_id": group_id,
                        "holder_id": holder_id,
                        "filing_id": filing_id,
                        "member_role": holder_type,
                        "effective_from": event_date,
                        "effective_to": None,
                    }
                )
            aggregate_amount_owned = _ns_text(
                person,
                "s:reportingPersonBeneficiallyOwnedAggregateNumberOfShares",
                namespaces,
            )
            percent_of_class = _ns_text(person, "s:classPercent", namespaces)
            sole_voting_power = _ns_text(
                person,
                "s:reportingPersonBeneficiallyOwnedNumberOfShares/s:soleVotingPower",
                namespaces,
            )
            shared_voting_power = _ns_text(
                person,
                "s:reportingPersonBeneficiallyOwnedNumberOfShares/s:sharedVotingPower",
                namespaces,
            )
            sole_dispositive_power = _ns_text(
                person,
                "s:reportingPersonBeneficiallyOwnedNumberOfShares/s:soleDispositivePower",
                namespaces,
            )
            shared_dispositive_power = _ns_text(
                person,
                "s:reportingPersonBeneficiallyOwnedNumberOfShares/s:sharedDispositivePower",
                namespaces,
            )
            source_row_hash = _hash_position_row(
                filing_id,
                holder_id,
                submission_type,
                aggregate_amount_owned or "",
                percent_of_class or "",
            )
            position_payloads.append(
                {
                    "issuer_id": issuer_id,
                    "holder_id": holder_id,
                    "group_id": group_id,
                    "filing_id": filing_id,
                    "schedule_type": schedule_type,
                    "event_date": event_date,
                    "available_at": filing_available_at,
                    "share_count_reported": parse_float(aggregate_amount_owned),
                    "ownership_pct": parse_float(percent_of_class),
                    "sole_voting_power": parse_float(sole_voting_power),
                    "shared_voting_power": parse_float(shared_voting_power),
                    "sole_dispositive_power": parse_float(sole_dispositive_power),
                    "shared_dispositive_power": parse_float(shared_dispositive_power),
                    "passive_flag": passive_flag,
                    "control_intent_flag": control_intent_flag,
                    "derivative_exposure_flag": False,
                    "source_row_hash": source_row_hash,
                }
            )
        filing_payload = {
            "filing_id": filing_id,
            "issuer_id": issuer_id,
            "schedule_type": schedule_type,
            "event_date": event_date,
            "passive_flag": passive_flag,
            "control_intent_flag": control_intent_flag,
            "group_flag": group_flag,
            "amendment_no": parse_int(
                _ns_text(root, "s:formData/s:coverPageHeader/s:amendmentNo", namespaces)
            ),
            "prior_schedule_type": None,
            "item4_purpose_text": None,
            "item5_interest_text": _ns_text(root, "s:formData/s:coverPageHeader/s:designateRulesPursuantThisScheduleFiled/s:designateRulePursuantThisScheduleFiled", namespaces),
            "item6_derivative_or_arrangement_text": _ns_text(
                root,
                "s:formData/s:coverPageHeaderReportingPersonDetails/s:comments",
                namespaces,
            ),
            "ownership_xml_version": _ns_text(root, "s:schemaVersion", namespaces),
        }
        return BeneficialOwnershipParseResult(
            owner_payloads=owner_payloads,
            filing_payload=filing_payload,
            group_payloads=group_payloads,
            membership_payloads=membership_payloads,
            position_payloads=position_payloads,
        )

    event_date = _parse_sec_date(
        _ns_text(root, "s:formData/s:coverPageHeader/s:dateOfEvent", namespaces)
    )
    purpose_text = _ns_text(
        root,
        "s:formData/s:items1To7/s:item4/s:transactionPurpose",
        namespaces,
    )
    contract_text = _ns_text(
        root,
        "s:formData/s:items1To7/s:item6/s:contractDescription",
        namespaces,
    )
    item5_interest_text = _ns_text(
        root,
        "s:formData/s:items1To7/s:item5/s:percentageOfClassSecurities",
        namespaces,
    )
    reporting_people = root.findall(
        "s:formData/s:reportingPersons/s:reportingPersonInfo",
        namespaces,
    )
    owner_refs: list[tuple[str, str | None]] = []
    group_flag = len(reporting_people) > 1
    group_id = build_group_id(issuer_cik, filing_id) if group_flag else None
    if group_id is not None:
        names = [
            _ns_text(person, "s:reportingPersonName", namespaces) or "UNKNOWN"
            for person in reporting_people[:3]
        ]
        group_payloads.append(
            {
                "group_id": group_id,
                "issuer_id": issuer_id,
                "group_name": "; ".join(names),
                "group_kind": "joint_filing",
                "root_filing_id": filing_id,
                "effective_from": event_date,
                "effective_to": None,
                "created_at": created_at,
            }
        )

    for person in reporting_people:
        holder_name = _ns_text(person, "s:reportingPersonName", namespaces)
        holder_cik = _ns_text(person, "s:reportingPersonCIK", namespaces)
        if not holder_name:
            holder_name = _ns_text(person, "s:reportingPersonNoCIK/s:personName", namespaces)
        if not holder_name:
            holder_name = "UNKNOWN REPORTING PERSON"
        holder_id = build_holder_id(holder_name, holder_cik)
        holder_type = _ns_text(person, "s:typeOfReportingPerson", namespaces)
        owner_refs.append((holder_id, holder_type))
        owner_payloads.append(
            {
                "holder_id": holder_id,
                "canonical_name": holder_name,
                "normalized_name": _normalized_name(holder_name),
                "holder_cik": holder_cik,
                "holder_type": holder_type,
                "parent_holder_id": None,
                "created_at": created_at,
                "updated_at": created_at,
            }
        )
        if group_id is not None:
            membership_payloads.append(
                {
                    "group_id": group_id,
                    "holder_id": holder_id,
                    "filing_id": filing_id,
                    "member_role": holder_type,
                    "effective_from": event_date,
                    "effective_to": None,
                }
            )
        aggregate_amount_owned = _ns_text(person, "s:aggregateAmountOwned", namespaces)
        percent_of_class = _ns_text(person, "s:percentOfClass", namespaces)
        sole_voting_power = _ns_text(person, "s:soleVotingPower", namespaces)
        shared_voting_power = _ns_text(person, "s:sharedVotingPower", namespaces)
        sole_dispositive_power = _ns_text(person, "s:soleDispositivePower", namespaces)
        shared_dispositive_power = _ns_text(person, "s:sharedDispositivePower", namespaces)
        source_row_hash = _hash_position_row(
            filing_id,
            holder_id,
            submission_type,
            aggregate_amount_owned or "",
            percent_of_class or "",
        )
        derivative_exposure_flag = any(
            token in str(contract_text or "").lower()
            for token in ("derivative", "swap", "option")
        )
        position_payloads.append(
            {
                "issuer_id": issuer_id,
                "holder_id": holder_id,
                "group_id": group_id,
                "filing_id": filing_id,
                "schedule_type": schedule_type,
                "event_date": event_date,
                "available_at": filing_available_at,
                "share_count_reported": parse_float(aggregate_amount_owned),
                "ownership_pct": parse_float(percent_of_class),
                "sole_voting_power": parse_float(sole_voting_power),
                "shared_voting_power": parse_float(shared_voting_power),
                "sole_dispositive_power": parse_float(sole_dispositive_power),
                "shared_dispositive_power": parse_float(shared_dispositive_power),
                "passive_flag": passive_flag,
                "control_intent_flag": control_intent_flag,
                "derivative_exposure_flag": derivative_exposure_flag,
                "source_row_hash": source_row_hash,
            }
        )

    filing_payload = {
        "filing_id": filing_id,
        "issuer_id": issuer_id,
        "schedule_type": schedule_type,
        "event_date": event_date,
        "passive_flag": passive_flag,
        "control_intent_flag": control_intent_flag,
        "group_flag": group_flag,
        "amendment_no": parse_int(
            _ns_text(root, "s:formData/s:coverPageHeader/s:amendmentNo", namespaces)
        ),
        "prior_schedule_type": "SC 13G"
        if _truthy_flag(
            _ns_text(
                root,
                "s:formData/s:coverPageHeader/s:previouslyFiledFlag",
                namespaces,
            )
        )
        and "13D" in submission_type.upper()
        else None,
        "item4_purpose_text": purpose_text,
        "item5_interest_text": item5_interest_text,
        "item6_derivative_or_arrangement_text": contract_text,
        "ownership_xml_version": None,
    }
    return BeneficialOwnershipParseResult(
        owner_payloads=owner_payloads,
        filing_payload=filing_payload,
        group_payloads=group_payloads,
        membership_payloads=membership_payloads,
        position_payloads=position_payloads,
    )


__all__ = [
    "BeneficialOwnershipParseResult",
    "InsiderOwnershipParseResult",
    "parse_form345_xml",
    "parse_schedule_13d_g_xml",
]
