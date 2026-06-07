# Python Quality Commands

Use the default Ruff check as the required low-noise gate:

```bash
uv run ruff check packages/core packages/api
```

Use broader Ruff scans as advisory cleanup tools when auditing or refactoring Python. Keep these report-only unless the current bead explicitly accepts the cleanup scope:

```bash
uv run ruff check packages/core packages/api --select B,SIM,RET,DTZ --statistics --exit-zero
uv run ruff check packages/core packages/api --select C901,PLR0912,PLR0915 --statistics --exit-zero
```

The default config intentionally enforces only a small set of high-signal Bugbear, Datetime, Return, and Simplify rules that currently pass. Complexity findings are useful for planning beads, not a blanket requirement to refactor unrelated modules.
