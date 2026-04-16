from __future__ import annotations

from core.storage.alert_repository import AlertRepository
from core.storage.job_repository import JobRepository
from core.storage.post_market_repository import PostMarketAnalysisRepository


class OpsStore:
    def __init__(
        self,
        *,
        alerts: AlertRepository,
        jobs: JobRepository,
        post_market: PostMarketAnalysisRepository,
    ) -> None:
        self.alerts = alerts
        self.jobs = jobs
        self.post_market = post_market

    def close(self) -> None:
        return None
