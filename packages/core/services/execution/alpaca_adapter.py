from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from core.integrations.alpaca.client import AlpacaClient
from core.services.alpaca import create_alpaca_client_from_env

from .runtimes import ALPACA_DIRECT_RUNTIME
from .shared import BROKER_NAME


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    rendered = str(value).strip()
    return rendered or None


@dataclass(frozen=True)
class AlpacaOrderSubmission:
    runtime: str
    broker: str
    submitted_order: dict[str, Any]
    order_snapshot: dict[str, Any]
    broker_order_id: str | None
    client_order_id: str | None


class AlpacaOrderAdapter:
    runtime = ALPACA_DIRECT_RUNTIME
    broker = BROKER_NAME

    def __init__(self, client: AlpacaClient | None = None) -> None:
        self.client = client or create_alpaca_client_from_env()

    def get_order_snapshot(
        self,
        broker_order_id: str,
        *,
        nested: bool = True,
        fallback: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        try:
            return self.client.get_order(broker_order_id, nested=nested)
        except Exception:
            if fallback is not None:
                return dict(fallback)
            raise

    def get_order_by_client_order_id(
        self,
        client_order_id: str,
        *,
        nested: bool = True,
    ) -> dict[str, Any]:
        return self.client.get_order_by_client_order_id(client_order_id, nested=nested)

    def submit_order(self, order_request: dict[str, Any]) -> AlpacaOrderSubmission:
        submitted_order = self.client.submit_order(dict(order_request))
        broker_order_id = _as_text(submitted_order.get("id"))
        order_snapshot = (
            dict(submitted_order)
            if broker_order_id is None
            else self.get_order_snapshot(
                broker_order_id,
                nested=True,
                fallback=submitted_order,
            )
        )
        return AlpacaOrderSubmission(
            runtime=self.runtime,
            broker=self.broker,
            submitted_order=dict(submitted_order),
            order_snapshot=order_snapshot,
            broker_order_id=broker_order_id,
            client_order_id=_as_text(submitted_order.get("client_order_id")),
        )

    def request_cancel(self, broker_order_id: str) -> dict[str, Any] | None:
        self.client.cancel_order(broker_order_id)
        try:
            return self.get_order_snapshot(broker_order_id, nested=True)
        except Exception:
            return None


def create_alpaca_order_adapter(
    client: AlpacaClient | None = None,
) -> AlpacaOrderAdapter:
    return AlpacaOrderAdapter(client=client)


__all__ = [
    "AlpacaOrderAdapter",
    "AlpacaOrderSubmission",
    "create_alpaca_order_adapter",
]
