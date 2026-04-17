from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import Any, Literal
from uuid import uuid4

from core.storage.serializers import parse_datetime


BacktestRunKind = Literal["bootstrap", "compare"]
BacktestRunStatus = Literal["running", "completed", "failed"]
BacktestArtifactType = Literal["summary_json", "sessions_csv", "comparison_json"]
BacktestArtifactRole = Literal["latest", "run", "export"]
BacktestArtifactFormat = Literal["json", "csv"]


def _parse_date(value: str | date | None) -> date | None:
    if value in (None, ""):
        return None
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _serialize_date(value: date | None) -> str | None:
    return None if value is None else value.isoformat()


def _serialize_datetime(value: datetime | None) -> str | None:
    return None if value is None else value.isoformat()


def new_backtest_run_id(kind: BacktestRunKind) -> str:
    return f"{kind}:{uuid4().hex}"


def new_backtest_artifact_id() -> str:
    return f"artifact:{uuid4().hex}"


@dataclass(frozen=True)
class BacktestTarget:
    bot_id: str | None = None
    automation_id: str | None = None
    strategy_config_id: str | None = None
    strategy_id: str | None = None
    config_hash: str | None = None
    start_date: date | None = None
    end_date: date | None = None
    session_limit: int | None = None

    def to_payload(self) -> dict[str, Any]:
        return {
            "bot_id": self.bot_id,
            "automation_id": self.automation_id,
            "strategy_config_id": self.strategy_config_id,
            "strategy_id": self.strategy_id,
            "config_hash": self.config_hash,
            "start_date": _serialize_date(self.start_date),
            "end_date": _serialize_date(self.end_date),
            "session_limit": self.session_limit,
        }

    @classmethod
    def from_payload(cls, payload: dict[str, Any] | None) -> BacktestTarget:
        payload = dict(payload or {})
        return cls(
            bot_id=payload.get("bot_id"),
            automation_id=payload.get("automation_id"),
            strategy_config_id=payload.get("strategy_config_id"),
            strategy_id=payload.get("strategy_id"),
            config_hash=payload.get("config_hash"),
            start_date=_parse_date(payload.get("start_date")),
            end_date=_parse_date(payload.get("end_date")),
            session_limit=(
                None
                if payload.get("session_limit") in (None, "")
                else int(payload["session_limit"])
            ),
        )


@dataclass(frozen=True)
class BacktestAggregate:
    session_count: int = 0
    modeled_selected_count: int = 0
    modeled_fill_count: int = 0
    modeled_position_count: int = 0
    modeled_closed_count: int = 0
    modeled_open_position_count: int = 0
    actual_selected_count: int = 0
    matched_selection_count: int = 0
    selection_match_rate: float | None = None
    modeled_fill_rate: float | None = None
    actual_fill_rate: float | None = None
    modeled_realized_pnl: float = 0.0
    modeled_unrealized_pnl: float = 0.0
    position_count: int = 0
    realized_pnl: float = 0.0
    unrealized_pnl: float = 0.0

    def to_payload(self) -> dict[str, Any]:
        return {
            "session_count": self.session_count,
            "modeled_selected_count": self.modeled_selected_count,
            "modeled_fill_count": self.modeled_fill_count,
            "modeled_position_count": self.modeled_position_count,
            "modeled_closed_count": self.modeled_closed_count,
            "modeled_open_position_count": self.modeled_open_position_count,
            "actual_selected_count": self.actual_selected_count,
            "matched_selection_count": self.matched_selection_count,
            "selection_match_rate": self.selection_match_rate,
            "modeled_fill_rate": self.modeled_fill_rate,
            "actual_fill_rate": self.actual_fill_rate,
            "modeled_realized_pnl": self.modeled_realized_pnl,
            "modeled_unrealized_pnl": self.modeled_unrealized_pnl,
            "position_count": self.position_count,
            "realized_pnl": self.realized_pnl,
            "unrealized_pnl": self.unrealized_pnl,
        }

    @classmethod
    def from_payload(cls, payload: dict[str, Any] | None) -> BacktestAggregate | None:
        if payload is None:
            return None
        payload = dict(payload)
        return cls(
            session_count=int(payload.get("session_count") or 0),
            modeled_selected_count=int(payload.get("modeled_selected_count") or 0),
            modeled_fill_count=int(payload.get("modeled_fill_count") or 0),
            modeled_position_count=int(payload.get("modeled_position_count") or 0),
            modeled_closed_count=int(payload.get("modeled_closed_count") or 0),
            modeled_open_position_count=int(
                payload.get("modeled_open_position_count") or 0
            ),
            actual_selected_count=int(payload.get("actual_selected_count") or 0),
            matched_selection_count=int(payload.get("matched_selection_count") or 0),
            selection_match_rate=(
                None
                if payload.get("selection_match_rate") in (None, "")
                else float(payload["selection_match_rate"])
            ),
            modeled_fill_rate=(
                None
                if payload.get("modeled_fill_rate") in (None, "")
                else float(payload["modeled_fill_rate"])
            ),
            actual_fill_rate=(
                None
                if payload.get("actual_fill_rate") in (None, "")
                else float(payload["actual_fill_rate"])
            ),
            modeled_realized_pnl=float(payload.get("modeled_realized_pnl") or 0.0),
            modeled_unrealized_pnl=float(payload.get("modeled_unrealized_pnl") or 0.0),
            position_count=int(payload.get("position_count") or 0),
            realized_pnl=float(payload.get("realized_pnl") or 0.0),
            unrealized_pnl=float(payload.get("unrealized_pnl") or 0.0),
        )


@dataclass(frozen=True)
class BacktestArtifact:
    id: str
    run_id: str
    artifact_type: BacktestArtifactType
    artifact_role: BacktestArtifactRole
    file_format: BacktestArtifactFormat
    path: str
    created_at: datetime
    size_bytes: int | None = None
    sha256: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_payload(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "run_id": self.run_id,
            "artifact_type": self.artifact_type,
            "artifact_role": self.artifact_role,
            "file_format": self.file_format,
            "path": self.path,
            "created_at": _serialize_datetime(self.created_at),
            "size_bytes": self.size_bytes,
            "sha256": self.sha256,
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> BacktestArtifact:
        payload = dict(payload)
        return cls(
            id=str(payload["id"]),
            run_id=str(payload["run_id"]),
            artifact_type=str(payload["artifact_type"]),
            artifact_role=str(payload["artifact_role"]),
            file_format=str(payload["file_format"]),
            path=str(payload["path"]),
            created_at=parse_datetime(payload.get("created_at")) or datetime.now(UTC),
            size_bytes=(
                None
                if payload.get("size_bytes") in (None, "")
                else int(payload["size_bytes"])
            ),
            sha256=payload.get("sha256"),
            metadata=dict(payload.get("metadata") or {}),
        )


@dataclass(frozen=True)
class BacktestSessionSummary:
    session_date: str
    automation_run_id: str | None = None
    opportunity_count: int = 0
    modeled_selected_opportunity_id: str | None = None
    actual_selected_opportunity_id: str | None = None
    selection_match: bool | None = None
    controls_allowed: bool | None = None
    controls_reason: str | None = None
    modeled_intent_state: str | None = None
    modeled_fill_state: str | None = None
    modeled_fill_price: float | None = None
    modeled_position: dict[str, Any] = field(default_factory=dict)
    modeled_exit_state: str | None = None
    modeled_exit_reason: str | None = None
    modeled_exit_recipe_ref: str | None = None
    modeled_exit_at: str | None = None
    modeled_exit_fill_price: float | None = None
    modeled_realized_pnl: float | None = None
    modeled_unrealized_pnl: float | None = None
    modeled_final_close_mark: float | None = None
    modeled_quote_event_count: int = 0
    modeled_snapshot_count: int = 0
    selected_intent_count: int = 0
    position_count: int = 0
    realized_pnl: float = 0.0
    unrealized_pnl: float = 0.0
    top_opportunities: list[dict[str, Any]] = field(default_factory=list)

    def to_payload(self) -> dict[str, Any]:
        return {
            "session_date": self.session_date,
            "automation_run_id": self.automation_run_id,
            "opportunity_count": self.opportunity_count,
            "modeled_selected_opportunity_id": self.modeled_selected_opportunity_id,
            "actual_selected_opportunity_id": self.actual_selected_opportunity_id,
            "selection_match": self.selection_match,
            "controls_allowed": self.controls_allowed,
            "controls_reason": self.controls_reason,
            "modeled_intent_state": self.modeled_intent_state,
            "modeled_fill_state": self.modeled_fill_state,
            "modeled_fill_price": self.modeled_fill_price,
            "modeled_position": dict(self.modeled_position),
            "modeled_exit_state": self.modeled_exit_state,
            "modeled_exit_reason": self.modeled_exit_reason,
            "modeled_exit_recipe_ref": self.modeled_exit_recipe_ref,
            "modeled_exit_at": self.modeled_exit_at,
            "modeled_exit_fill_price": self.modeled_exit_fill_price,
            "modeled_realized_pnl": self.modeled_realized_pnl,
            "modeled_unrealized_pnl": self.modeled_unrealized_pnl,
            "modeled_final_close_mark": self.modeled_final_close_mark,
            "modeled_quote_event_count": self.modeled_quote_event_count,
            "modeled_snapshot_count": self.modeled_snapshot_count,
            "selected_intent_count": self.selected_intent_count,
            "position_count": self.position_count,
            "realized_pnl": self.realized_pnl,
            "unrealized_pnl": self.unrealized_pnl,
            "top_opportunities": [dict(row) for row in self.top_opportunities],
        }

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> BacktestSessionSummary:
        payload = dict(payload)
        return cls(
            session_date=str(payload["session_date"]),
            automation_run_id=payload.get("automation_run_id"),
            opportunity_count=int(payload.get("opportunity_count") or 0),
            modeled_selected_opportunity_id=payload.get(
                "modeled_selected_opportunity_id"
            ),
            actual_selected_opportunity_id=payload.get(
                "actual_selected_opportunity_id"
            ),
            selection_match=payload.get("selection_match"),
            controls_allowed=payload.get("controls_allowed"),
            controls_reason=payload.get("controls_reason"),
            modeled_intent_state=payload.get("modeled_intent_state"),
            modeled_fill_state=payload.get("modeled_fill_state"),
            modeled_fill_price=(
                None
                if payload.get("modeled_fill_price") in (None, "")
                else float(payload["modeled_fill_price"])
            ),
            modeled_position=dict(payload.get("modeled_position") or {}),
            modeled_exit_state=payload.get("modeled_exit_state"),
            modeled_exit_reason=payload.get("modeled_exit_reason"),
            modeled_exit_recipe_ref=payload.get("modeled_exit_recipe_ref"),
            modeled_exit_at=payload.get("modeled_exit_at"),
            modeled_exit_fill_price=(
                None
                if payload.get("modeled_exit_fill_price") in (None, "")
                else float(payload["modeled_exit_fill_price"])
            ),
            modeled_realized_pnl=(
                None
                if payload.get("modeled_realized_pnl") in (None, "")
                else float(payload["modeled_realized_pnl"])
            ),
            modeled_unrealized_pnl=(
                None
                if payload.get("modeled_unrealized_pnl") in (None, "")
                else float(payload["modeled_unrealized_pnl"])
            ),
            modeled_final_close_mark=(
                None
                if payload.get("modeled_final_close_mark") in (None, "")
                else float(payload["modeled_final_close_mark"])
            ),
            modeled_quote_event_count=int(
                payload.get("modeled_quote_event_count") or 0
            ),
            modeled_snapshot_count=int(payload.get("modeled_snapshot_count") or 0),
            selected_intent_count=int(payload.get("selected_intent_count") or 0),
            position_count=int(payload.get("position_count") or 0),
            realized_pnl=float(payload.get("realized_pnl") or 0.0),
            unrealized_pnl=float(payload.get("unrealized_pnl") or 0.0),
            top_opportunities=[
                dict(row) for row in list(payload.get("top_opportunities") or [])
            ],
        )


@dataclass(frozen=True)
class BacktestRun:
    id: str
    kind: BacktestRunKind
    status: BacktestRunStatus
    engine_name: str
    engine_version: str
    created_at: datetime
    started_at: datetime
    completed_at: datetime | None = None
    output_root: str = ""
    target: BacktestTarget | None = None
    aggregate: BacktestAggregate | None = None
    sessions: list[BacktestSessionSummary] = field(default_factory=list)
    left_target: BacktestTarget | None = None
    right_target: BacktestTarget | None = None
    comparison_metrics: dict[str, dict[str, Any]] = field(default_factory=dict)
    params: dict[str, Any] = field(default_factory=dict)
    coverage: dict[str, Any] = field(default_factory=dict)
    artifacts: list[BacktestArtifact] = field(default_factory=list)
    left_run_id: str | None = None
    right_run_id: str | None = None
    error_text: str | None = None

    @property
    def artifact_paths(self) -> dict[str, Any]:
        paths: dict[str, Any] = {
            "run_id": self.id,
            "output_root": self.output_root,
        }
        run_dir: str | None = None
        for artifact in self.artifacts:
            if artifact.artifact_role == "latest":
                key = artifact.artifact_type
            elif (
                artifact.artifact_role == "run"
                and artifact.artifact_type == "comparison_json"
            ):
                key = "comparison_run_json"
            else:
                key = f"{artifact.artifact_role}_{artifact.artifact_type}"
            paths[key] = artifact.path
            if artifact.artifact_role == "run":
                run_dir = str(artifact.path.rsplit("/", 1)[0])
        if run_dir is not None:
            paths["run_dir"] = run_dir
        return paths

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "id": self.id,
            "kind": self.kind,
            "status": self.status,
            "engine_name": self.engine_name,
            "engine_version": self.engine_version,
            "created_at": _serialize_datetime(self.created_at),
            "started_at": _serialize_datetime(self.started_at),
            "completed_at": _serialize_datetime(self.completed_at),
            "output_root": self.output_root,
            "params": dict(self.params),
            "coverage": dict(self.coverage),
            "artifacts": [artifact.to_payload() for artifact in self.artifacts],
            "artifact_paths": self.artifact_paths,
            "left_run_id": self.left_run_id,
            "right_run_id": self.right_run_id,
            "error_text": self.error_text,
        }
        if self.kind == "bootstrap":
            payload.update(
                {
                    "target": None if self.target is None else self.target.to_payload(),
                    "aggregate": (
                        None if self.aggregate is None else self.aggregate.to_payload()
                    ),
                    "sessions": [session.to_payload() for session in self.sessions],
                }
            )
        else:
            payload.update(
                {
                    "left": (
                        None
                        if self.left_target is None
                        else self.left_target.to_payload()
                    ),
                    "right": (
                        None
                        if self.right_target is None
                        else self.right_target.to_payload()
                    ),
                    "metrics": {
                        key: dict(value)
                        for key, value in self.comparison_metrics.items()
                    },
                }
            )
        return payload

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> BacktestRun:
        payload = dict(payload)
        kind = str(payload.get("kind") or "bootstrap")
        return cls(
            id=str(payload.get("id") or new_backtest_run_id(kind)),
            kind=kind,
            status=str(payload.get("status") or "completed"),
            engine_name=str(payload.get("engine_name") or "bootstrap_backtest"),
            engine_version=str(payload.get("engine_version") or "v1"),
            created_at=parse_datetime(payload.get("created_at")) or datetime.now(UTC),
            started_at=parse_datetime(payload.get("started_at")) or datetime.now(UTC),
            completed_at=parse_datetime(payload.get("completed_at")),
            output_root=str(payload.get("output_root") or ""),
            target=(
                None
                if kind != "bootstrap"
                else BacktestTarget.from_payload(payload.get("target"))
            ),
            aggregate=(
                None
                if kind != "bootstrap"
                else BacktestAggregate.from_payload(payload.get("aggregate"))
            ),
            sessions=(
                []
                if kind != "bootstrap"
                else [
                    BacktestSessionSummary.from_payload(dict(row))
                    for row in list(payload.get("sessions") or [])
                ]
            ),
            left_target=(
                None
                if kind != "compare"
                else BacktestTarget.from_payload(payload.get("left"))
            ),
            right_target=(
                None
                if kind != "compare"
                else BacktestTarget.from_payload(payload.get("right"))
            ),
            comparison_metrics=(
                {}
                if kind != "compare"
                else {
                    str(key): dict(value)
                    for key, value in dict(payload.get("metrics") or {}).items()
                }
            ),
            params=dict(payload.get("params") or {}),
            coverage=dict(payload.get("coverage") or {}),
            artifacts=[
                BacktestArtifact.from_payload(dict(row))
                for row in list(payload.get("artifacts") or [])
            ],
            left_run_id=payload.get("left_run_id"),
            right_run_id=payload.get("right_run_id"),
            error_text=payload.get("error_text"),
        )


__all__ = [
    "BacktestAggregate",
    "BacktestArtifact",
    "BacktestArtifactFormat",
    "BacktestArtifactRole",
    "BacktestArtifactType",
    "BacktestRun",
    "BacktestRunKind",
    "BacktestRunStatus",
    "BacktestSessionSummary",
    "BacktestTarget",
    "new_backtest_artifact_id",
    "new_backtest_run_id",
]
