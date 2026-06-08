from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from core.integrations.alpaca.client import AlpacaClient
from core.observability.logging import log_event
from core.services.alpaca import create_alpaca_client_from_env
from core.value_coercion import as_text

from .runtimes import ALPACA_DIRECT_RUNTIME
from .shared import BROKER_NAME

logger = logging.getLogger(__name__)


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
        except Exception as exc:
            if fallback is not None:
                log_event(
                    logger,
                    logging.WARNING,
                    "alpaca_order_snapshot_fallback_used",
                    exc_info=True,
                    broker_order_id=broker_order_id,
                    nested=nested,
                    error=str(exc),
                )
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
        broker_order_id = as_text(submitted_order.get("id"))
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
            client_order_id=as_text(submitted_order.get("client_order_id")),
        )

    def request_cancel(self, broker_order_id: str) -> dict[str, Any] | None:
        self.client.cancel_order(broker_order_id)
        try:
            return self.get_order_snapshot(broker_order_id, nested=True)
        except Exception as exc:
            log_event(
                logger,
                logging.WARNING,
                "alpaca_cancel_snapshot_refresh_failed",
                exc_info=True,
                broker_order_id=broker_order_id,
                error=str(exc),
            )
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
