from __future__ import annotations

import hashlib
import json
import secrets
from collections.abc import Mapping, Sequence
from datetime import timedelta
from typing import Any

import redis

from core.runtime.config import default_redis_url
from core.storage.serializers import render_value as _render_value
from core.value_coercion import safe_component, utc_now, utc_now_iso

PROVIDER_CACHE_PREFIX = "spreads:provider"
SECRET_FIELD_PARTS = (
    "api_key",
    "apikey",
    "authorization",
    "bearer",
    "cookie",
    "password",
    "secret",
    "token",
)


def provider_params_hash(params: Mapping[str, Any] | None) -> str:
    return hashlib.sha256(_stable_json(sanitize_provider_json(dict(params or {}))).encode("utf-8")).hexdigest()


def provider_payload_hash(payload: Any) -> str:
    return hashlib.sha256(_stable_json(sanitize_provider_json(payload)).encode("utf-8")).hexdigest()


def provider_response_key(provider: str, endpoint: str, params_hash: str, *, page_key: str | None = None) -> str:
    key = f"{PROVIDER_CACHE_PREFIX}:{safe_component(provider)}:{safe_component(endpoint)}:{safe_component(params_hash)}"
    if page_key:
        return f"{key}:page:{safe_component(page_key)}"
    return key


def provider_backoff_key(provider: str, endpoint: str) -> str:
    return f"{PROVIDER_CACHE_PREFIX}:{safe_component(provider)}:{safe_component(endpoint)}:backoff"


def provider_refresh_lock_key(scope: str) -> str:
    return f"{PROVIDER_CACHE_PREFIX}:refresh-lock:{safe_component(scope)}"


def sanitize_provider_json(value: Any) -> Any:
    if isinstance(value, Mapping):
        sanitized: dict[str, Any] = {}
        for key, item in value.items():
            rendered_key = str(key)
            sanitized[rendered_key] = "[redacted]" if _is_secret_field(rendered_key) else sanitize_provider_json(item)
        return sanitized
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [sanitize_provider_json(item) for item in value]
    return _render_value(value)


class ProviderHotCache:
    def __init__(self, *, redis_url: str | None = None, client: Any | None = None) -> None:
        self.client = client or redis.Redis.from_url(redis_url or default_redis_url(), decode_responses=True)
        self._owns_client = client is None

    def close(self) -> None:
        if self._owns_client:
            self.client.close()

    def get_payload(
        self,
        *,
        provider: str,
        endpoint: str,
        params_hash: str,
        page_key: str | None = None,
    ) -> Any | None:
        raw = self.client.get(provider_response_key(provider, endpoint, params_hash, page_key=page_key))
        if not raw:
            return None
        try:
            envelope = json.loads(raw)
        except json.JSONDecodeError:
            return None
        return envelope.get("payload") if isinstance(envelope, dict) else None

    def set_payload(
        self,
        *,
        provider: str,
        endpoint: str,
        params_hash: str,
        payload: Any,
        ttl_seconds: int,
        page_key: str | None = None,
    ) -> str:
        key = provider_response_key(provider, endpoint, params_hash, page_key=page_key)
        envelope = {
            "provider": provider,
            "endpoint": endpoint,
            "params_hash": params_hash,
            "page_key": page_key,
            "payload_hash": provider_payload_hash(payload),
            "fetched_at": utc_now_iso(),
            "payload": sanitize_provider_json(payload),
        }
        self.client.set(key, _stable_json(envelope), ex=max(1, int(ttl_seconds)))
        return key

    def get_backoff(self, *, provider: str, endpoint: str) -> dict[str, Any] | None:
        raw = self.client.get(provider_backoff_key(provider, endpoint))
        if not raw:
            return None
        try:
            envelope = json.loads(raw)
        except json.JSONDecodeError:
            return None
        return envelope if isinstance(envelope, dict) else None

    def set_backoff(
        self,
        *,
        provider: str,
        endpoint: str,
        ttl_seconds: int,
        reason: str | None = None,
    ) -> dict[str, Any]:
        ttl = max(1, int(ttl_seconds))
        envelope = {
            "provider": provider,
            "endpoint": endpoint,
            "reason": reason,
            "backoff_until": (utc_now() + timedelta(seconds=ttl)).isoformat(),
        }
        self.client.set(provider_backoff_key(provider, endpoint), _stable_json(envelope), ex=ttl)
        return envelope

    def acquire_refresh_lock(
        self,
        *,
        scope: str,
        ttl_seconds: int,
        token: str | None = None,
    ) -> str | None:
        lock_token = token or secrets.token_urlsafe(16)
        acquired = self.client.set(provider_refresh_lock_key(scope), lock_token, ex=max(1, int(ttl_seconds)), nx=True)
        return lock_token if acquired else None

    def release_refresh_lock(self, *, scope: str, token: str) -> bool:
        script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        end
        return 0
        """
        return bool(self.client.eval(script, 1, provider_refresh_lock_key(scope), token))


def _is_secret_field(key: str) -> bool:
    normalized = key.lower().replace("-", "_")
    return any(part in normalized for part in SECRET_FIELD_PARTS)


def _stable_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
