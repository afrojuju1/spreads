from __future__ import annotations

import hashlib
import re
from datetime import date, datetime, timezone


def _as_text(value: str | None) -> str:
    rendered = str(value or "").strip()
    if not rendered:
        raise ValueError("value is required")
    return rendered


def _normalize_cik(cik: str | None) -> str:
    digits = re.sub(r"\D+", "", _as_text(cik))
    if not digits:
        raise ValueError("cik must include digits")
    return digits.zfill(10)


def _normalize_ticker(ticker: str | None) -> str:
    rendered = _as_text(ticker).upper()
    return re.sub(r"[^A-Z0-9.\-]", "", rendered)


def _normalize_cusip(value: str | None) -> str:
    rendered = _as_text(value).upper()
    normalized = re.sub(r"[^A-Z0-9]", "", rendered)
    if len(normalized) < 6:
        raise ValueError("cusip must include at least 6 alphanumeric characters")
    return normalized


def _normalize_name(value: str | None) -> str:
    rendered = re.sub(r"\s+", " ", _as_text(value).lower()).strip()
    return re.sub(r"[^a-z0-9 ]", "", rendered)


def _hash_parts(*parts: str) -> str:
    joined = "|".join(part.strip().lower() for part in parts if part.strip())
    return hashlib.sha1(joined.encode("utf-8")).hexdigest()[:16]


def _format_timestamp(value: date | datetime | str) -> str:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, datetime):
        normalized = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    else:
        normalized = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if normalized.tzinfo is None:
            normalized = normalized.replace(tzinfo=timezone.utc)
    return normalized.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def build_issuer_id(cik: str) -> str:
    return f"issuer:{_normalize_cik(cik)}"


def build_security_id(cik: str, ticker: str) -> str:
    return f"security:{_normalize_cik(cik)}:{_normalize_ticker(ticker)}"


def build_filing_id(accession_no: str) -> str:
    return f"filing:{_as_text(accession_no)}"


def build_holder_id(canonical_name: str, holder_cik: str | None = None) -> str:
    return f"holder:{_hash_parts(_normalize_name(canonical_name), _normalize_cik(holder_cik) if holder_cik else '')}"


def build_institutional_holder_id(
    manager_name: str,
    manager_cik: str | None = None,
) -> str:
    return (
        f"institutional_holder:{_hash_parts(_normalize_name(manager_name), _normalize_cik(manager_cik) if manager_cik else '')}"
    )


def build_institutional_filing_id(accession_no: str) -> str:
    return f"institutional_filing:{_as_text(accession_no)}"


def build_institutional_position_source_row_hash(
    *,
    filing_id: str,
    institutional_holder_id: str,
    issuer_name_reported: str | None,
    title_of_class: str | None,
    cusip: str | None,
    figi: str | None,
    put_call: str | None,
    share_count: float | None,
    market_value_reported: float | None,
    discretion_type: str | None,
) -> str:
    issuer_name_token = re.sub(r"[^a-z0-9 ]", "", re.sub(r"\s+", " ", str(issuer_name_reported or "").lower()).strip())
    title_token = re.sub(r"[^a-z0-9 ]", "", re.sub(r"\s+", " ", str(title_of_class or "").lower()).strip())
    return f"13frow:{_hash_parts(
        _as_text(filing_id),
        _as_text(institutional_holder_id),
        issuer_name_token,
        title_token,
        re.sub(r'[^A-Z0-9]', '', str(cusip or '').upper()),
        re.sub(r'[^A-Z0-9]', '', str(figi or '').upper()),
        str(put_call or '').upper().strip(),
        '' if share_count is None else f'{float(share_count):.12g}',
        '' if market_value_reported is None else f'{float(market_value_reported):.12g}',
        str(discretion_type or '').upper().strip(),
    )}"


def build_group_id(issuer_cik: str, seed: str) -> str:
    return f"group:{_normalize_cik(issuer_cik)}:{_hash_parts(seed)}"


def build_security_identifier_id(
    security_id: str,
    identifier_type: str,
    identifier_value: str,
    effective_from: date | datetime | str | None = None,
) -> str:
    effective_token = ""
    if effective_from is not None:
        effective_token = (
            effective_from.isoformat()
            if isinstance(effective_from, date)
            else str(effective_from)
        )
    return f"security_identifier:{_hash_parts(_as_text(security_id), _as_text(identifier_type), _as_text(identifier_value), effective_token)}"


def build_feature_snapshot_id(
    issuer_cik: str,
    as_of: date | datetime | str,
    feature_version: str,
) -> str:
    return f"feature_snapshot:{_normalize_cik(issuer_cik)}:{_format_timestamp(as_of)}:{_as_text(feature_version)}"


def build_company_valuation_snapshot_id(
    issuer_cik: str,
    as_of: date | datetime | str,
    valuation_version: str,
) -> str:
    return f"company_valuation:{_normalize_cik(issuer_cik)}:{_format_timestamp(as_of)}:{_as_text(valuation_version)}"


def build_market_snapshot_id(
    issuer_cik: str,
    captured_at: date | datetime | str,
) -> str:
    return f"market_snapshot:{_normalize_cik(issuer_cik)}:{_format_timestamp(captured_at)}"


def build_treasury_curve_snapshot_id(curve_date: date | datetime | str) -> str:
    if isinstance(curve_date, datetime):
        rendered = curve_date.date().isoformat()
    elif isinstance(curve_date, date):
        rendered = curve_date.isoformat()
    else:
        rendered = str(curve_date)
    return f"treasury_curve:{rendered}"


def build_screening_row_id(issuer_cik: str, as_of: date | datetime | str) -> str:
    if isinstance(as_of, datetime):
        rendered = as_of.astimezone(timezone.utc).date().isoformat()
    elif isinstance(as_of, date):
        rendered = as_of.isoformat()
    else:
        rendered = str(as_of)
    return f"screen:{_normalize_cik(issuer_cik)}:{rendered}"


def build_issuer_overlay_flag_id(issuer_cik: str, flag_key: str) -> str:
    return f"issuer_overlay_flag:{_normalize_cik(issuer_cik)}:{_as_text(flag_key).strip().lower()}"


def normalize_cik(value: str | None) -> str:
    return _normalize_cik(value)


def normalize_cusip(value: str | None) -> str:
    return _normalize_cusip(value)


def normalize_name(value: str | None) -> str:
    return _normalize_name(value)


def normalize_ticker(value: str | None) -> str:
    return _normalize_ticker(value)
