from __future__ import annotations

import json
import urllib.error
import urllib.request
from dataclasses import dataclass

from core.runtime.config import default_openfigi_api_key
from core.services.company_valuation.ids import normalize_cusip

DEFAULT_OPENFIGI_BASE_URL = "https://api.openfigi.com"
DEFAULT_OPENFIGI_TIMEOUT_SECONDS = 30.0


class OpenFigiRequestError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        response_body: str | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.response_body = response_body


@dataclass(frozen=True)
class OpenFigiMapping:
    ticker: str | None
    name: str | None
    exch_code: str | None
    market_sector: str | None
    security_type: str | None
    composite_figi: str | None
    share_class_figi: str | None
    security_description: str | None

    @classmethod
    def from_payload(cls, payload: dict[str, object]) -> "OpenFigiMapping":
        return cls(
            ticker=str(payload.get("ticker") or "").strip() or None,
            name=str(payload.get("name") or "").strip() or None,
            exch_code=str(payload.get("exchCode") or "").strip() or None,
            market_sector=str(payload.get("marketSector") or "").strip() or None,
            security_type=str(payload.get("securityType") or "").strip() or None,
            composite_figi=str(payload.get("compositeFIGI") or "").strip() or None,
            share_class_figi=str(payload.get("shareClassFIGI") or "").strip() or None,
            security_description=str(payload.get("securityDescription") or "").strip() or None,
        )


class OpenFigiClient:
    def __init__(
        self,
        *,
        base_url: str = DEFAULT_OPENFIGI_BASE_URL,
        api_key: str | None = None,
        timeout_seconds: float = DEFAULT_OPENFIGI_TIMEOUT_SECONDS,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key or default_openfigi_api_key()
        self.timeout_seconds = float(timeout_seconds)

    def map_cusips(
        self,
        cusips: list[str],
    ) -> dict[str, list[OpenFigiMapping]]:
        normalized = [normalize_cusip(value) for value in cusips if str(value or "").strip()]
        if not normalized:
            return {}
        payload = [{"idType": "ID_CUSIP", "idValue": value} for value in normalized]
        body = json.dumps(payload).encode("utf-8")
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if self.api_key:
            headers["X-OPENFIGI-APIKEY"] = self.api_key
        request = urllib.request.Request(
            f"{self.base_url}/v3/mapping",
            data=body,
            headers=headers,
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                response_body = response.read()
        except urllib.error.HTTPError as exc:
            rendered = exc.read().decode("utf-8", errors="replace")
            raise OpenFigiRequestError(
                f"OpenFIGI mapping failed: {exc.code} {exc.reason}",
                status_code=exc.code,
                response_body=rendered,
            ) from exc
        except urllib.error.URLError as exc:
            raise OpenFigiRequestError(f"Failed to reach OpenFIGI: {exc.reason}") from exc
        decoded = json.loads(response_body.decode("utf-8"))
        results: dict[str, list[OpenFigiMapping]] = {}
        for cusip, item in zip(normalized, decoded, strict=False):
            if not isinstance(item, dict):
                continue
            rows = item.get("data")
            if not isinstance(rows, list):
                continue
            results[cusip] = [
                OpenFigiMapping.from_payload(row)
                for row in rows
                if isinstance(row, dict)
            ]
        return results


def select_best_openfigi_mapping(
    mappings: list[OpenFigiMapping],
) -> OpenFigiMapping | None:
    if not mappings:
        return None
    ordered = sorted(
        mappings,
        key=lambda row: (
            1 if str(row.market_sector or "").lower() == "equity" else 0,
            1
            if str(row.security_type or "").lower()
            in {"common stock", "depositary receipt", "equity warrant", "preferred stock"}
            else 0,
            1 if str(row.exch_code or "").upper() == "US" else 0,
            1 if row.ticker else 0,
            1 if row.name else 0,
        ),
        reverse=True,
    )
    return ordered[0]


__all__ = [
    "DEFAULT_OPENFIGI_BASE_URL",
    "DEFAULT_OPENFIGI_TIMEOUT_SECONDS",
    "OpenFigiClient",
    "OpenFigiMapping",
    "OpenFigiRequestError",
    "select_best_openfigi_mapping",
]
