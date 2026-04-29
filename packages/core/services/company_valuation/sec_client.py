from __future__ import annotations

import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any

from core.runtime.config import (
    default_sec_request_interval_seconds,
    default_sec_user_agent,
)

DEFAULT_SEC_DATA_BASE_URL = "https://data.sec.gov"
DEFAULT_SEC_WWW_BASE_URL = "https://www.sec.gov"
DEFAULT_SEC_REQUEST_TIMEOUT_SECONDS = 30.0
DEFAULT_SEC_REQUEST_INTERVAL_SECONDS = default_sec_request_interval_seconds()


class SecRequestError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        url: str | None = None,
        response_body: str | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.url = url
        self.response_body = response_body


@dataclass(frozen=True)
class SecTickerLookup:
    ticker: str
    cik: str
    title: str


class SecEdgarClient:
    def __init__(
        self,
        *,
        data_base_url: str = DEFAULT_SEC_DATA_BASE_URL,
        www_base_url: str = DEFAULT_SEC_WWW_BASE_URL,
        user_agent: str | None = None,
        request_timeout_seconds: float = DEFAULT_SEC_REQUEST_TIMEOUT_SECONDS,
        request_interval_seconds: float = DEFAULT_SEC_REQUEST_INTERVAL_SECONDS,
    ) -> None:
        self.data_base_url = data_base_url.rstrip("/")
        self.www_base_url = www_base_url.rstrip("/")
        self.user_agent = user_agent or default_sec_user_agent()
        self.request_timeout_seconds = float(request_timeout_seconds)
        self.request_interval_seconds = float(request_interval_seconds)
        self._next_request_at = 0.0
        self.headers = {
            "User-Agent": self.user_agent,
        }

    def _throttle(self) -> None:
        now = time.monotonic()
        if now < self._next_request_at:
            time.sleep(self._next_request_at - now)
        self._next_request_at = time.monotonic() + self.request_interval_seconds

    def _read_url(
        self,
        url: str,
        *,
        headers: dict[str, str] | None = None,
    ) -> bytes:
        self._throttle()
        request_headers = dict(self.headers)
        if headers:
            request_headers.update(headers)
        request = urllib.request.Request(url, headers=request_headers)
        try:
            with urllib.request.urlopen(request, timeout=self.request_timeout_seconds) as response:
                return response.read()
        except urllib.error.HTTPError as exc:
            response_body = exc.read().decode("utf-8", errors="replace")
            raise SecRequestError(
                f"SEC request failed: {exc.code} {exc.reason} for {url}\n{response_body}",
                status_code=exc.code,
                url=url,
                response_body=response_body,
            ) from exc
        except urllib.error.URLError as exc:
            raise SecRequestError(
                f"Failed to reach SEC for {url}: {exc.reason}",
                url=url,
            ) from exc

    def get_bytes_url(
        self,
        url: str,
        *,
        headers: dict[str, str] | None = None,
    ) -> bytes:
        return self._read_url(url, headers=headers)

    def get_json_url(self, url: str) -> Any:
        body = self._read_url(url, headers={"Accept": "application/json"})
        if not body:
            return None
        return json.loads(body.decode("utf-8"))

    def get_text_url(self, url: str) -> str:
        body = self._read_url(url, headers={"Accept": "text/plain, text/html, application/xml;q=0.9, */*;q=0.8"})
        return body.decode("utf-8", errors="replace")

    def get_json_path(self, path: str) -> Any:
        return self.get_json_url(f"{self.data_base_url}{path}")

    def resolve_ticker(self, ticker: str) -> SecTickerLookup:
        payload = self.get_json_url(f"{self.www_base_url}/files/company_tickers.json")
        if not isinstance(payload, dict):
            raise SecRequestError("Unexpected SEC ticker mapping payload shape")
        target = str(ticker or "").upper().strip()
        for row in payload.values():
            if not isinstance(row, dict):
                continue
            candidate = str(row.get("ticker") or "").upper().strip()
            if candidate != target:
                continue
            cik = str(row.get("cik_str") or "").strip()
            title = str(row.get("title") or "").strip()
            if not cik:
                break
            return SecTickerLookup(ticker=target, cik=cik, title=title)
        raise ValueError(f"Unknown SEC ticker mapping for {ticker}")

    def get_submissions(self, cik: str) -> dict[str, Any]:
        return self.get_json_path(f"/submissions/CIK{str(cik).zfill(10)}.json")

    def get_company_tickers_exchange(self) -> dict[str, Any]:
        return self.get_json_url(f"{self.www_base_url}/files/company_tickers_exchange.json")

    def get_submissions_file(self, filename: str) -> dict[str, Any]:
        safe_name = urllib.parse.quote(str(filename or "").strip(), safe="/")
        if not safe_name:
            raise ValueError("filename is required")
        return self.get_json_path(f"/submissions/{safe_name}")

    def get_companyfacts(self, cik: str) -> dict[str, Any]:
        return self.get_json_path(f"/api/xbrl/companyfacts/CIK{str(cik).zfill(10)}.json")

    @staticmethod
    def archive_document_url(
        cik: str,
        accession_no: str,
        document_name: str,
    ) -> str:
        bare_cik = str(int(str(cik).zfill(10)))
        bare_accession = str(accession_no).replace("-", "")
        safe_name = urllib.parse.quote(str(document_name or "").strip(), safe="/")
        if not safe_name:
            raise ValueError("document_name is required")
        return f"{DEFAULT_SEC_WWW_BASE_URL}/Archives/edgar/data/{bare_cik}/{bare_accession}/{safe_name}"

    def filing_index_url(self, cik: str, accession_no: str) -> str:
        return self.archive_document_url(cik, accession_no, f"{accession_no}-index.htm")

    def get_filing_index_html(self, cik: str, accession_no: str) -> str:
        return self.get_text_url(self.filing_index_url(cik, accession_no))

    def resolve_filing_xml_url(
        self,
        *,
        cik: str,
        accession_no: str,
        primary_document_name: str | None = None,
        form_type: str | None = None,
    ) -> str | None:
        primary_name = str(primary_document_name or "").strip()
        if primary_name.lower().endswith(".xml"):
            return self.archive_document_url(cik, accession_no, primary_name)
        normalized_form = str(form_type or "").upper().strip()
        if normalized_form.removesuffix("/A") in {"3", "4", "5"}:
            return self.archive_document_url(cik, accession_no, "ownership.xml")
        if normalized_form in {
            "SC 13D",
            "SC 13D/A",
            "SC 13G",
            "SC 13G/A",
            "SCHEDULE 13D",
            "SCHEDULE 13D/A",
            "SCHEDULE 13G",
            "SCHEDULE 13G/A",
        }:
            return self.archive_document_url(cik, accession_no, "primary_doc.xml")
        index_html = self.get_filing_index_html(cik, accession_no)
        matches = re.findall(
            r'href="(/Archives/edgar/data/[^"]+?/(?!(?:xsl|XSL))[^"/]+?\.xml)"',
            index_html,
            flags=re.IGNORECASE,
        )
        if not matches:
            return None
        preferred_names = {"ownership.xml", "primary_doc.xml"}
        for path in matches:
            if path.rsplit("/", 1)[-1].lower() in preferred_names:
                return f"{self.www_base_url}{path}"
        return f"{self.www_base_url}{matches[0]}"


__all__ = [
    "DEFAULT_SEC_DATA_BASE_URL",
    "DEFAULT_SEC_REQUEST_INTERVAL_SECONDS",
    "DEFAULT_SEC_REQUEST_TIMEOUT_SECONDS",
    "DEFAULT_SEC_WWW_BASE_URL",
    "SecEdgarClient",
    "SecRequestError",
    "SecTickerLookup",
]
