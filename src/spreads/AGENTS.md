# Backend Service Map

- Prefer extending existing service entrypoints before adding new aggregators.

## Canonical Ownership

- session list/detail: `services/sessions.py`
- actual account and trading health: `services/account_state.py`
- closed-session verdicts and recommendations: `services/post_market_analysis.py` and `storage/post_market_repository.py`
- alert delivery state: `storage/alert_repository.py`
- job execution and scheduler behavior: `jobs/worker.py`, `jobs/registry.py`, and `storage/job_repository.py`

## Operator Visibility

- For operator visibility work, reuse these modules with thin adapters instead of introducing parallel API-only logic.
- For closed-session investigations, check post-market analysis before tuning strategy thresholds from raw session counts alone.
