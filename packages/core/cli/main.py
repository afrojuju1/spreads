from __future__ import annotations

import typer

from core.cli.backtests import backtests_app
from core.cli.company_valuation import company_valuation_app
from core.cli.config import config_app
from core.cli.deploy import deploy_app
from core.cli.execution import execution_app
from core.cli.lifecycle import lifecycle_app
from core.cli.ops import jobs_app, ops_app
from core.cli.runtime import runtime_app

app = typer.Typer(
    add_completion=True,
    help="Spreads operator CLI.",
    no_args_is_help=True,
)


app.add_typer(ops_app, name="ops")
app.add_typer(jobs_app, name="jobs")
app.add_typer(execution_app, name="execution")
app.add_typer(lifecycle_app, name="lifecycle")
app.add_typer(deploy_app, name="deploy")
app.add_typer(config_app, name="config")
app.add_typer(company_valuation_app, name="company-valuation")
app.add_typer(runtime_app, name="runtime")
app.add_typer(backtests_app, name="backtests")


def main() -> None:
    app()


if __name__ == "__main__":
    main()
