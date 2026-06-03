# Test Instructions

- Do not add, update, or expand automated tests unless Ade explicitly asks for test work.
- Only write or modify e2e tests when Ade explicitly asks for e2e coverage.
- Default implementation validation belongs in the running stack, shipped CLIs, operator commands, and targeted runtime smoke checks.
- When Ade explicitly asks for tests, critical flow validations belong under `tests/e2e/`.
- Do not add new test files under `tests/` root. Critical validations belong in `tests/e2e/`; if a test does not merit that directory, it probably should not exist.
- Prefer one high-signal end-to-end validation through the canonical path over multiple narrow tests.
- Do not add fake unit tests, helper tests, render-only tests, coercion tests, or mock-heavy tests that only prove implementation details.
- Test behavior at the system boundary that matters in production: scheduler flow, discovery run flow, execution flow, backtest decisions, API/runtime read models, and critical policy gates.
- Mock only true external boundaries or hard-to-control infra surfaces. Do not mock the core service or flow you are claiming to validate.
- Assert on real outcomes and invariants, not private helper calls or incidental intermediate structure.
- If a change is not in a critical flow, prefer no new test over a low-value test.
- New critical flow tests should live in `tests/e2e/` and use clear flow-oriented names without `_e2e` suffixes.
- Keep verification targeted with `uv run python tests/e2e/...` or another single needed test file; do not turn normal work into broad repo-wide test sweeps.
