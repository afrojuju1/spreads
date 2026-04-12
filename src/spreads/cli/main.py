from __future__ import annotations

import typer

from spreads.cli.ops import (
    audit_command,
    jobs_app,
    sessions_command,
    status_command,
    trading_command,
    uoa_app,
)
from spreads.cli.replay import replay_app

app = typer.Typer(
    add_completion=True,
    help="Spreads operator CLI.",
    no_args_is_help=True,
)

app.command("status", help="Show system and runtime health.")(status_command)
app.command("trading", help="Show live trading safety and readiness.")(trading_command)
app.command("sessions", help="List live sessions or inspect one session.")(
    sessions_command
)
app.command("audit", help="Replay one session for operator investigation.")(
    audit_command
)
app.add_typer(jobs_app, name="jobs")
app.add_typer(replay_app, name="replay")
app.add_typer(uoa_app, name="uoa")


def main() -> None:
    app()


if __name__ == "__main__":
    main()
