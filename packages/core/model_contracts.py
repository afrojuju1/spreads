from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict


class DomainModel(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        populate_by_name=True,
        arbitrary_types_allowed=True,
    )

    def to_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json")
