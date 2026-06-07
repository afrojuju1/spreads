# Scanner Package Status

`packages/core/services/scanners` is stale implementation infrastructure.

It remains in place because the current `DataEngine` candidate-build path still reuses the scanner math, market-slice handling, diagnostics, and strategy builders. It is not the target architecture and should not grow into a separate product surface, CLI flow, orchestration model, or ownership boundary.

Future scanner/candidate work should redesign the flow around the engine spine:

```text
DataEngine -> candidate facts -> StrategyEngine -> RiskEngine -> ExecutionEngine
```

Keep acceptable changes narrow:

- bug fixes needed by the current DataEngine candidate build
- mechanical cleanup that does not deepen scanner ownership
- extraction of reusable math into `services/strategy_builders.py`, `services/option_structures.py`, or typed `services/trading_engine` contracts

Avoid:

- new orchestration inside this package
- new CLI/product workflows based on `services.scanners.service`
- new persistence ownership
- compatibility wrappers around stale scanner entrypoints
- broader feature work that should belong to DataEngine or StrategyEngine

When replacing this package, remove displaced scanner entrypoints and update callers directly. Do not add long-lived shims.
