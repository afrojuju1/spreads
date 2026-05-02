from __future__ import annotations

import hashlib
import re
from datetime import date, datetime, timezone
from uuid import uuid4


def normalize_ticker(value: str | None) -> str:
    rendered = str(value or "").strip().upper()
    normalized = re.sub(r"[^A-Z0-9.\-]", "", rendered)
    if not normalized:
        raise ValueError("ticker is required")
    return normalized


def timestamp_token(value: datetime | None = None) -> str:
    current = value or datetime.now(timezone.utc)
    normalized = current if current.tzinfo else current.replace(tzinfo=timezone.utc)
    return normalized.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def build_market_intel_run_id(
    *,
    ticker: str,
    as_of: date,
    started_at: datetime,
) -> str:
    return f"market_intel:{normalize_ticker(ticker)}:{as_of.isoformat()}:{timestamp_token(started_at)}:{uuid4().hex[:8]}"


def build_artifact_id(*parts: str) -> str:
    joined = "|".join(str(part).strip().lower() for part in parts if str(part).strip())
    digest = hashlib.sha1(joined.encode("utf-8")).hexdigest()[:16]
    return f"market_intel_artifact:{digest}"


def build_config_hash(payload: dict[str, object]) -> str:
    normalized = repr(sorted(payload.items()))
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16]
