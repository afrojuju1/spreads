from __future__ import annotations

from abc import ABC, abstractmethod

from calendar_events.models import CalendarEventQuery, CalendarEventRecord


class BaseCalendarEventAdapter(ABC):
    source_name = "base"
    source_confidence = "unknown"
    refresh_always = False

    @abstractmethod
    def applies_to(self, query: CalendarEventQuery) -> bool:
        raise NotImplementedError

    @abstractmethod
    def scope_key(self, query: CalendarEventQuery) -> str:
        raise NotImplementedError

    @abstractmethod
    def fetch(self, query: CalendarEventQuery) -> list[CalendarEventRecord]:
        raise NotImplementedError
