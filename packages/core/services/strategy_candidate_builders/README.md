# Strategy Candidate Builders

This package is the target owner for option candidate construction used by the
DataEngine.

It owns typed contracts for:

- strategy candidate build settings
- market data requests and coverage summaries
- option structure construction
- candidate analytics and ranking
- candidate rows and diagnostics
- builder failures and run outcomes
- market data provider and candidate builder protocols

Keep this package focused on candidate construction and diagnostics. It should
not become a scheduler, product CLI, persistence owner, or execution path.
