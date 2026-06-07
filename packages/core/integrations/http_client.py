from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import httpx
from tenacity import Retrying, retry_if_exception_type, stop_after_attempt, wait_exponential

DEFAULT_RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})
SAFE_RETRY_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})
DEFAULT_USER_AGENT = "spreads/1.0"


class VendorHttpError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        method: str,
        url: str,
        status_code: int | None = None,
        response_body: str | None = None,
        reason: str | None = None,
    ) -> None:
        super().__init__(message)
        self.method = method
        self.url = url
        self.status_code = status_code
        self.response_body = response_body
        self.reason = reason


class _RetryableVendorHttpError(VendorHttpError):
    pass


@dataclass(frozen=True)
class VendorHttpResponse:
    method: str
    url: str
    status_code: int
    headers: Mapping[str, str]
    content: bytes

    @property
    def text(self) -> str:
        return self.content.decode("utf-8", errors="replace")


@dataclass(frozen=True)
class VendorHttpClient:
    default_headers: Mapping[str, str] | None = None
    timeout_seconds: float = 30.0
    retry_count: int = 2
    retry_backoff_seconds: float = 0.5
    retryable_status_codes: frozenset[int] = DEFAULT_RETRYABLE_STATUS_CODES
    user_agent: str = DEFAULT_USER_AGENT

    def request_json(
        self,
        method: str,
        base_url: str,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        body: Any | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> Any:
        response = self.request(method, base_url, path, params=params, body=body, headers=headers)
        if not response.content:
            return None
        try:
            return json.loads(response.text)
        except json.JSONDecodeError as exc:
            raise VendorHttpError(
                f"Vendor HTTP response was not valid JSON for {response.method} {response.url}: {exc}",
                method=response.method,
                url=response.url,
                status_code=response.status_code,
                response_body=response.text,
                reason=str(exc),
            ) from exc

    def request_text(
        self,
        method: str,
        base_url: str,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        body: Any | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> str:
        request_headers = {"Accept": "text/plain, text/html, text/csv, application/xml;q=0.9, */*;q=0.8"}
        if headers:
            request_headers.update(headers)
        return self.request(method, base_url, path, params=params, body=body, headers=request_headers).text

    def request_bytes(
        self,
        method: str,
        base_url: str,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        body: Any | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> bytes:
        request_headers = {"Accept": "*/*"}
        if headers:
            request_headers.update(headers)
        return self.request(method, base_url, path, params=params, body=body, headers=request_headers).content

    def request(
        self,
        method: str,
        base_url: str,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        body: Any | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> VendorHttpResponse:
        method_upper = str(method or "GET").upper()
        url = self._url(base_url, path)
        request_headers = self._headers(headers)
        request_params = self._params(params)

        max_attempts = self._max_attempts_for(method_upper)
        retrying = Retrying(
            reraise=True,
            retry=retry_if_exception_type(_RetryableVendorHttpError),
            stop=stop_after_attempt(max_attempts),
            wait=wait_exponential(multiplier=max(float(self.retry_backoff_seconds), 0.0)),
        )
        for attempt in retrying:
            with attempt:
                return self._request_once(
                    method_upper,
                    url,
                    params=request_params,
                    body=body,
                    headers=request_headers,
                )
        raise VendorHttpError(
            f"Failed to reach vendor for {method_upper} {url}",
            method=method_upper,
            url=url,
        )

    def _request_once(
        self,
        method: str,
        url: str,
        *,
        params: Mapping[str, Any] | None,
        body: Any | None,
        headers: Mapping[str, str],
    ) -> VendorHttpResponse:
        request_kwargs: dict[str, Any] = {
            "headers": headers,
            "params": params,
            "timeout": httpx.Timeout(float(self.timeout_seconds)),
        }
        if body is not None:
            request_kwargs["json"] = body

        try:
            response = httpx.request(
                method,
                url,
                **request_kwargs,
            )
            response.raise_for_status()
            return VendorHttpResponse(
                method=method,
                url=str(response.request.url),
                status_code=response.status_code,
                headers=dict(response.headers),
                content=response.content,
            )
        except httpx.HTTPStatusError as exc:
            response_body = exc.response.text
            error_cls = _RetryableVendorHttpError if self._is_retryable_status(method, exc.response.status_code) else VendorHttpError
            raise error_cls(
                f"Vendor HTTP request failed: {exc.response.status_code} {exc.response.reason_phrase} for {method} {exc.request.url}\n{response_body}",
                method=method,
                url=str(exc.request.url),
                status_code=exc.response.status_code,
                response_body=response_body,
                reason=exc.response.reason_phrase,
            ) from exc
        except httpx.RequestError as exc:
            error_cls = _RetryableVendorHttpError if method in SAFE_RETRY_METHODS else VendorHttpError
            raise error_cls(
                f"Failed to reach vendor for {method} {exc.request.url}: {exc}",
                method=method,
                url=str(exc.request.url),
                reason=str(exc),
            ) from exc

    @staticmethod
    def _url(
        base_url: str,
        path: str,
    ) -> str:
        return f"{base_url.rstrip('/')}{path}"

    @staticmethod
    def _params(params: Mapping[str, Any] | None) -> dict[str, Any] | None:
        if params:
            filtered = {str(key): value for key, value in params.items() if value not in (None, "")}
            if filtered:
                return filtered
        return None

    def _headers(self, headers: Mapping[str, str] | None) -> dict[str, str]:
        merged = {
            "Accept": "application/json",
            "User-Agent": self.user_agent,
        }
        if self.default_headers:
            merged.update({str(key): str(value) for key, value in self.default_headers.items()})
        if headers:
            merged.update({str(key): str(value) for key, value in headers.items()})
        return merged

    def _max_attempts_for(self, method: str) -> int:
        if method not in SAFE_RETRY_METHODS:
            return 1
        return max(int(self.retry_count), 0) + 1

    def _is_retryable_status(self, method: str, status_code: int) -> bool:
        return method in SAFE_RETRY_METHODS and status_code in self.retryable_status_codes


__all__ = [
    "DEFAULT_RETRYABLE_STATUS_CODES",
    "DEFAULT_USER_AGENT",
    "SAFE_RETRY_METHODS",
    "VendorHttpClient",
    "VendorHttpError",
    "VendorHttpResponse",
]
