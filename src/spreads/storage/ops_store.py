from __future__ import annotations

from spreads.storage.alert_repository import AlertRepository
from spreads.storage.job_repository import JobRepository
from spreads.storage.post_market_repository import PostMarketAnalysisRepository


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
