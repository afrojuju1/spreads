from __future__ import annotations

from core.storage.alert_repository import AlertRepository
from core.storage.job_repository import JobRepository


class OpsStore:
    def __init__(
        self,
        *,
        alerts: AlertRepository,
        jobs: JobRepository,
    ) -> None:
        self.alerts = alerts
        self.jobs = jobs

    def close(self) -> None:
        return None
