from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timedelta
import os
import re
from typing import Any

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, ValidationInfo, field_validator, model_validator

from core.common import parse_float, parse_int, pick
from core.services.market_dates import NEW_YORK
from core.services.strategy_candidate_builders.market_data import build_expected_move_estimates, group_contracts_by_expiration
from core.services.trading_strategies import build_entry_strategy_symbols, load_universe_symbols
from core.value_coercion import as_text, coerce_bool, utc_now_iso as _iso_now

VALID_TICKER_SOURCE_RECIPES = frozenset({"strategy_union", "finviz_screener", "stock_prefilter", "earnings_event_window"})


def _normalized_recipe_args(value: Any) -> dict[str, Any]:
    return {str(key): item for key, item in value.items()} if isinstance(value, Mapping) else {}


class TickerSourceRecipeArgsModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="allow", populate_by_name=True)

    @field_validator("*", mode="before")
    @classmethod
    def _blank_to_none(cls, value: Any) -> Any:
        return None if value == "" else value


class TargetDteOptionFilterConfig(TickerSourceRecipeArgsModel):
    enabled: bool = False
    min_dte: int = Field(default=7, ge=0)
    max_dte: int = Field(default=21, ge=0)
    feed: str = "opra"
    stock_feed: str = "sip"
    require_expected_move: bool = True
    min_expected_move_count: int = Field(default=1, ge=1)

    @field_validator("feed", "stock_feed", mode="before")
    @classmethod
    def _normalize_feed(cls, value: Any, info: ValidationInfo) -> str:
        return as_text(value) or ("sip" if info.field_name == "stock_feed" else "opra")

    @field_validator("min_dte", "max_dte", "min_expected_move_count", mode="before")
    @classmethod
    def _normalize_count(cls, value: Any, info: ValidationInfo) -> Any:
        return cls.model_fields[info.field_name].default if value in (None, "") else value

    @model_validator(mode="after")
    def _validate_range(self) -> TargetDteOptionFilterConfig:
        if self.max_dte < self.min_dte:
            return self.model_copy(update={"max_dte": self.min_dte})
        return self


class TargetDteRecipeArgs(TickerSourceRecipeArgsModel):
    min_dte: int = Field(default=7, ge=0)
    max_dte: int = Field(default=21, ge=0)
    feed: str = "opra"
    stock_feed: str = "sip"
    target_option_filter: TargetDteOptionFilterConfig = Field(default_factory=TargetDteOptionFilterConfig)

    @model_validator(mode="before")
    @classmethod
    def _normalize_target_option_filter(cls, value: Any) -> dict[str, Any]:
        mapping = _normalized_recipe_args(value)
        raw = mapping.get("target_dte_options", mapping.get("target_dte_option_filter"))
        nested = dict(raw) if isinstance(raw, Mapping) else {}
        if not isinstance(raw, Mapping):
            nested["enabled"] = bool(coerce_bool(raw, default=False))
        nested.setdefault("enabled", False)
        nested.setdefault("min_dte", mapping.get("min_dte", 7))
        nested.setdefault("max_dte", mapping.get("max_dte", 21))
        nested.setdefault("feed", mapping.get("feed", "opra"))
        nested.setdefault("stock_feed", mapping.get("stock_feed", "sip"))
        mapping["target_option_filter"] = nested
        return mapping

    @field_validator("feed", "stock_feed", mode="before")
    @classmethod
    def _normalize_feed(cls, value: Any, info: ValidationInfo) -> str:
        return as_text(value) or ("sip" if info.field_name == "stock_feed" else "opra")

    @model_validator(mode="after")
    def _validate_range(self) -> TargetDteRecipeArgs:
        if self.max_dte < self.min_dte:
            return self.model_copy(update={"max_dte": self.min_dte})
        return self


class StrategyUnionRecipeArgs(TickerSourceRecipeArgsModel):
    candidate_builder: str | None = None
    build_profile: str | None = None

    @field_validator("candidate_builder", "build_profile", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: Any) -> str | None:
        return as_text(value)


class StockPrefilterRecipeArgs(TickerSourceRecipeArgsModel):
    top: int = Field(default=15, ge=1)
    most_actives_top: int | None = Field(default=None, ge=1)
    movers_top: int | None = Field(default=None, ge=1)
    min_price: float = Field(default=10.0, ge=0)
    min_daily_volume: int = Field(default=0, ge=0)
    news_limit: int | None = Field(default=None, ge=1)
    most_actives_by: str = "volume"
    stock_feed: str = "sip"
    exclude_leveraged_and_inverse_etfs: bool = False

    @field_validator("most_actives_by", "stock_feed", mode="before")
    @classmethod
    def _normalize_text(cls, value: Any, info: ValidationInfo) -> str:
        return as_text(value) or ("sip" if info.field_name == "stock_feed" else "volume")


class EarningsEventWindowRecipeArgs(TargetDteRecipeArgs):
    lookahead_days: int = Field(default=30, ge=1, validation_alias=AliasChoices("lookahead_days", "window_days"))
    front_window_days: int = Field(default=10, ge=1)
    min_source_confidence: str = "medium"
    include_conflicts: bool = Field(default=False, validation_alias=AliasChoices("include_conflicts", "allow_conflicts"))
    min_price: float = Field(default=10.0, ge=0)
    min_daily_volume: int = Field(default=1_000_000, ge=0, validation_alias=AliasChoices("min_daily_volume", "min_volume"))
    max_symbols: int = Field(default=25, ge=1)
    actionability_candidate_limit: int | None = Field(default=None, ge=1)
    stock_feed: str = "sip"

    @field_validator("min_source_confidence", "stock_feed", mode="before")
    @classmethod
    def _normalize_text(cls, value: Any, info: ValidationInfo) -> str:
        return as_text(value) or ("sip" if info.field_name == "stock_feed" else "medium")


class FinvizScreenerRecipeArgs(TargetDteRecipeArgs):
    source: str = "auto"
    source_url: str | None = None
    csv_url: str | None = None
    url: str | None = None
    source_url_env: str | None = None
    csv_url_env: str | None = None
    url_env: str | None = None
    csv_path: str | None = None
    path: str | None = None
    csv_path_env: str | None = None
    path_env: str | None = None
    source_symbol_limit: int | None = Field(default=None, ge=1)
    min_price: float = Field(default=0.0, ge=0)
    min_market_cap: float = Field(default=0.0, ge=0)
    min_volume: int = Field(default=0, ge=0, validation_alias=AliasChoices("min_volume", "min_daily_volume"))
    exclude_industries: tuple[str, ...] = ()
    exclude_company_keywords: tuple[str, ...] = ()
    timeout_seconds: int = Field(default=20, ge=1)
    cookie_env: str = "FINVIZ_COOKIE"

    @field_validator("source", "cookie_env", mode="before")
    @classmethod
    def _normalize_default_text(cls, value: Any, info: ValidationInfo) -> str:
        default = cls.model_fields[info.field_name].default
        return as_text(value) or str(default)

    @field_validator(
        "source_url",
        "csv_url",
        "url",
        "source_url_env",
        "csv_url_env",
        "url_env",
        "csv_path",
        "path",
        "csv_path_env",
        "path_env",
        mode="before",
    )
    @classmethod
    def _normalize_optional_text(cls, value: Any) -> str | None:
        return as_text(value)

    @field_validator("exclude_industries", "exclude_company_keywords", mode="before")
    @classmethod
    def _normalize_text_tuple(cls, value: Any) -> tuple[str, ...]:
        if value in (None, "", ()):
            return ()
        raw_items = value.split(",") if isinstance(value, str) else list(value) if isinstance(value, list | tuple | set) else [value]
        return tuple(str(item).strip() for item in raw_items if str(item or "").strip())


def _target_dte_option_filter_result(
    *,
    client: Any,
    symbol: str,
    config: TargetDteOptionFilterConfig,
) -> dict[str, Any]:
    reference_date = datetime.now(NEW_YORK).date()
    min_expiration = (reference_date + timedelta(days=config.min_dte)).isoformat()
    max_expiration = (reference_date + timedelta(days=config.max_dte)).isoformat()
    result: dict[str, Any] = {
        "status": "passed",
        "reason": None,
        "min_dte": config.min_dte,
        "max_dte": config.max_dte,
        "min_expiration": min_expiration,
        "max_expiration": max_expiration,
        "feed": config.feed,
        "require_expected_move": config.require_expected_move,
        "min_expected_move_count": config.min_expected_move_count,
    }

    call_contracts = client.list_option_contracts(
        symbol,
        min_expiration,
        max_expiration,
        option_type="call",
    )
    put_contracts = client.list_option_contracts(
        symbol,
        min_expiration,
        max_expiration,
        option_type="put",
    )
    call_contracts_by_expiration = group_contracts_by_expiration(call_contracts)
    put_contracts_by_expiration = group_contracts_by_expiration(put_contracts)
    expirations = sorted(set(call_contracts_by_expiration) | set(put_contracts_by_expiration))
    common_expirations = sorted(set(call_contracts_by_expiration) & set(put_contracts_by_expiration))
    result.update(
        {
            "call_contract_count": len(call_contracts),
            "put_contract_count": len(put_contracts),
            "contract_count": len(call_contracts) + len(put_contracts),
            "expiration_count": len(expirations),
            "common_expiration_count": len(common_expirations),
            "expirations": expirations,
        }
    )
    if not call_contracts or not put_contracts:
        result.update({"status": "filtered_out", "reason": "target_dte_contracts_missing"})
        return result
    if not common_expirations:
        result.update({"status": "filtered_out", "reason": "target_dte_common_expiration_missing"})
        return result
    if not config.require_expected_move:
        return result

    spot_price = client.get_underlying_price(symbol, config.stock_feed)
    call_snapshots_by_expiration = {}
    put_snapshots_by_expiration = {}
    for expiration_date in common_expirations:
        call_snapshots_by_expiration[expiration_date] = client.get_option_chain_snapshots(
            symbol,
            expiration_date,
            "call",
            config.feed,
        )
        put_snapshots_by_expiration[expiration_date] = client.get_option_chain_snapshots(
            symbol,
            expiration_date,
            "put",
            config.feed,
        )
    expected_moves = build_expected_move_estimates(
        spot_price=spot_price,
        call_contracts_by_expiration=call_contracts_by_expiration,
        put_contracts_by_expiration=put_contracts_by_expiration,
        call_snapshots_by_expiration=call_snapshots_by_expiration,
        put_snapshots_by_expiration=put_snapshots_by_expiration,
    )
    result.update(
        {
            "spot_price": round(float(spot_price), 4),
            "expected_move_count": len(expected_moves),
            "expected_move_expirations": sorted(expected_moves),
        }
    )
    if len(expected_moves) < config.min_expected_move_count:
        result.update({"status": "filtered_out", "reason": "target_dte_expected_move_missing"})
    return result


_ENV_TOKEN_REGEX = re.compile(r"\$\{([A-Z0-9_]+)\}")


def _expand_env_tokens(value: Any) -> str | None:
    text = as_text(value)
    if text is None:
        return None
    expanded = _ENV_TOKEN_REGEX.sub(
        lambda match: os.environ.get(match.group(1), ""),
        text,
    )
    return as_text(expanded)


def _recipe_text_arg(
    recipe_args: Mapping[str, Any] | BaseModel,
    field_name: str,
    *,
    env_field_name: str | None = None,
) -> str | None:
    value = recipe_args.get(field_name) if isinstance(recipe_args, Mapping) else getattr(recipe_args, field_name, None)
    direct = _expand_env_tokens(value)
    if direct is not None:
        return direct
    if env_field_name is None:
        return None
    env_value = recipe_args.get(env_field_name) if isinstance(recipe_args, Mapping) else getattr(recipe_args, env_field_name, None)
    env_name = as_text(env_value)
    if env_name is None:
        return None
    return as_text(os.environ.get(env_name))


_LEVERAGE_REGEX = re.compile(r"\b(?:[2-9](?:\.\d+)?x|ultra|ultrapro|leveraged|leverage)\b")
_INVERSE_REGEX = re.compile(r"\b(?:inverse|short|bear|ultrashort)\b")
_ETF_NAME_REGEX = re.compile(r"\b(?:etf|trust|fund|shares|direxion|proshares|graniteshares|yieldmax)\b")


def _looks_like_leveraged_or_inverse_etf(asset: Mapping[str, Any] | None) -> bool:
    if not isinstance(asset, Mapping):
        return False
    name = str(asset.get("name") or "").strip().lower()
    if not name:
        return False
    if not _ETF_NAME_REGEX.search(name):
        return False
    return bool(_LEVERAGE_REGEX.search(name) or _INVERSE_REGEX.search(name))


def _stock_snapshot_price(snapshot: Mapping[str, Any]) -> float | None:
    latest_trade = snapshot.get("latestTrade") if isinstance(snapshot.get("latestTrade"), Mapping) else {}
    latest_quote = snapshot.get("latestQuote") if isinstance(snapshot.get("latestQuote"), Mapping) else {}
    minute_bar = snapshot.get("minuteBar") if isinstance(snapshot.get("minuteBar"), Mapping) else {}
    daily_bar = snapshot.get("dailyBar") if isinstance(snapshot.get("dailyBar"), Mapping) else {}

    trade_price = parse_float(pick(latest_trade, "p", "price"))
    if trade_price is not None and trade_price > 0:
        return trade_price

    bid = parse_float(pick(latest_quote, "bp", "bid_price"))
    ask = parse_float(pick(latest_quote, "ap", "ask_price"))
    if bid is not None and ask is not None and bid > 0 and ask > 0 and ask >= bid:
        return (bid + ask) / 2.0

    minute_close = parse_float(pick(minute_bar, "c", "close"))
    if minute_close is not None and minute_close > 0:
        return minute_close

    daily_close = parse_float(pick(daily_bar, "c", "close"))
    if daily_close is not None and daily_close > 0:
        return daily_close
    return None


def _stock_snapshot_daily_volume(snapshot: Mapping[str, Any]) -> int:
    daily_bar = snapshot.get("dailyBar") if isinstance(snapshot.get("dailyBar"), Mapping) else {}
    return parse_int(pick(daily_bar, "v", "volume")) or 0


def _stock_snapshot_daily_percent_change(snapshot: Mapping[str, Any]) -> float | None:
    daily_bar = snapshot.get("dailyBar") if isinstance(snapshot.get("dailyBar"), Mapping) else {}
    prev_daily_bar = snapshot.get("prevDailyBar") if isinstance(snapshot.get("prevDailyBar"), Mapping) else {}
    current_close = parse_float(pick(daily_bar, "c", "close"))
    previous_close = parse_float(pick(prev_daily_bar, "c", "close"))
    if current_close is None or previous_close is None or previous_close <= 0:
        return None
    return ((current_close - previous_close) / previous_close) * 100.0


def _rank_score(rank: int | None, *, total: int, weight: float) -> float:
    if rank is None or total <= 0:
        return 0.0
    return max(weight * float(total - rank) / float(total), 0.0)


def _build_strategy_union_result(
    *,
    source_id: str,
    recipe: str,
    recipe_args: Mapping[str, Any],
    config_root: str | None,
) -> dict[str, Any]:
    args = StrategyUnionRecipeArgs.model_validate(recipe_args)
    symbols = build_entry_strategy_symbols(
        config_root=config_root,
        candidate_builder_key=args.candidate_builder,
        build_profile=args.build_profile,
    )
    source_tags = [f"recipe:{str(recipe or '').strip().lower()}"]
    if args.build_profile is not None:
        build_profile = args.build_profile
        source_tags.append(f"build_profile:{build_profile}")
    if args.candidate_builder is not None:
        candidate_builder = args.candidate_builder
        source_tags.append(f"candidate_builder:{candidate_builder}")
    generated_at = _iso_now()
    return {
        "status": "completed",
        "source_id": str(source_id),
        "recipe": str(recipe),
        "generated_at": generated_at,
        "symbols": list(symbols),
        "entries": [
            {
                "symbol": symbol,
                "observation_state": "selected",
                "score": None,
                "reason_codes": [str(recipe)],
                "source_tags": list(source_tags),
            }
            for symbol in symbols
        ],
        "observations": [
            {
                "symbol": symbol,
                "observation_state": "selected",
                "score": None,
                "reason_codes": [str(recipe)],
                "source_tags": list(source_tags),
            }
            for symbol in symbols
        ],
        "summary": {
            "symbol_count": len(symbols),
            "recipe": str(recipe),
        },
        "degradation": {
            "status": "ok" if symbols else "empty",
            "reason": None if symbols else "no_symbols",
        },
    }



def build_ticker_source_symbols(
    *,
    recipe: str,
    recipe_args: Mapping[str, Any] | None = None,
    config_root: str | None = None,
) -> tuple[str, ...]:
    normalized_recipe = str(recipe or "").strip().lower()
    normalized_args = _normalized_recipe_args(recipe_args)
    if normalized_recipe == "strategy_union":
        return tuple(
            _build_strategy_union_result(
                source_id="ticker_source",
                recipe=normalized_recipe,
                recipe_args=normalized_args,
                config_root=config_root,
            ).get("symbols")
            or []
        )
    if normalized_recipe == "finviz_screener":
        from core.services.sources.finviz import _run_finviz_screener_feed

        return tuple(
            _run_finviz_screener_feed(
                source_id="ticker_source",
                recipe=normalized_recipe,
                recipe_args=normalized_args,
            ).get("symbols")
            or []
        )
    if normalized_recipe == "stock_prefilter":
        from core.services.sources.stock_prefilter import _run_stock_prefilter_feed

        return tuple(
            _run_stock_prefilter_feed(
                source_id="ticker_source",
                recipe=normalized_recipe,
                recipe_args=normalized_args,
            ).get("symbols")
            or []
        )
    if normalized_recipe == "earnings_event_window":
        from core.services.sources.earnings import _run_earnings_event_window_feed

        return tuple(
            _run_earnings_event_window_feed(
                source_id="ticker_source",
                recipe=normalized_recipe,
                recipe_args=normalized_args,
            ).get("symbols")
            or []
        )
    raise ValueError(f"Unsupported ticker source recipe: {recipe}")


def run_ticker_source(
    *,
    source_id: str,
    recipe: str,
    recipe_args: Mapping[str, Any] | None = None,
    config_root: str | None = None,
) -> dict[str, Any]:
    normalized_args = _normalized_recipe_args(recipe_args)
    normalized_recipe = str(recipe or "").strip().lower()
    if normalized_recipe == "strategy_union":
        return _build_strategy_union_result(
            source_id=source_id,
            recipe=normalized_recipe,
            recipe_args=normalized_args,
            config_root=config_root,
        )
    if normalized_recipe == "finviz_screener":
        from core.services.sources.finviz import _run_finviz_screener_feed

        return _run_finviz_screener_feed(
            source_id=source_id,
            recipe=normalized_recipe,
            recipe_args=normalized_args,
        )
    if normalized_recipe == "stock_prefilter":
        from core.services.sources.stock_prefilter import _run_stock_prefilter_feed

        return _run_stock_prefilter_feed(
            source_id=source_id,
            recipe=normalized_recipe,
            recipe_args=normalized_args,
        )
    if normalized_recipe == "earnings_event_window":
        from core.services.sources.earnings import _run_earnings_event_window_feed

        return _run_earnings_event_window_feed(
            source_id=source_id,
            recipe=normalized_recipe,
            recipe_args=normalized_args,
        )
    raise ValueError(f"Unsupported ticker source recipe: {recipe}")


def _ticker_source_run_id(*, source_id: str, job_run_id: str | None, generated_at: str | None) -> str:
    if job_run_id not in (None, ""):
        return f"ticker_source_run:{job_run_id}"
    generated_key = str(generated_at or _iso_now()).replace(":", "").replace("-", "")
    return f"ticker_source_run:{source_id}:{generated_key}"


def persist_ticker_source_result(
    engine_facts: Any,
    *,
    source_id: str,
    recipe: str,
    job_run_id: str | None,
    result: Mapping[str, Any],
    config_hash: str | None = None,
) -> dict[str, Any]:
    if engine_facts is None or not engine_facts.schema_ready():
        return {
            "status": "skipped",
            "reason": "engine_fact_schema_unavailable",
        }
    generated_at = as_text(result.get("generated_at")) or _iso_now()
    ticker_source_run_id = _ticker_source_run_id(
        source_id=str(source_id),
        job_run_id=job_run_id,
        generated_at=generated_at,
    )
    row = engine_facts.upsert_ticker_source_run(
        ticker_source_run_id=ticker_source_run_id,
        ticker_source_type=str(recipe),
        ticker_source_id=str(source_id),
        job_run_id=job_run_id,
        status=str(result.get("status") or "completed"),
        config_hash=config_hash,
        generated_at=generated_at,
        completed_at=_iso_now(),
        symbols=[str(symbol).upper() for symbol in list(result.get("symbols") or []) if str(symbol or "").strip()],
        entries=[dict(item) for item in list(result.get("entries") or []) if isinstance(item, Mapping)],
        observations=[dict(item) for item in list(result.get("observations") or []) if isinstance(item, Mapping)],
        summary=dict(result.get("summary") or {}),
        evidence={
            "source_id": str(source_id),
            "recipe": str(recipe),
            "degradation": dict(result.get("degradation") or {}),
        },
        updated_at=_iso_now(),
    )
    return {
        "status": "ok",
        "ticker_source_run_id": row.get("ticker_source_run_id"),
        "observed_count": row.get("observed_count"),
        "selected_count": row.get("selected_count"),
        "excluded_count": row.get("excluded_count"),
    }


def get_latest_ticker_source_snapshot(
    engine_facts: Any,
    *,
    source_id: str,
    job_key: str | None = None,
    max_age_seconds: int | None = None,
) -> dict[str, Any]:
    if engine_facts is None or not engine_facts.schema_ready():
        return {
            "status": "missing",
            "source_id": str(source_id),
            "job_key": None if job_key is None else str(job_key),
            "symbols": [],
            "entries": [],
            "summary": {},
            "degradation": {
                "status": "missing",
                "reason": "engine_fact_schema_unavailable",
            },
            "ticker_source_run_id": None,
            "generated_at": None,
            "age_seconds": None,
        }
    snapshot = engine_facts.get_latest_ticker_source_snapshot(
        ticker_source_id=str(source_id),
        max_age_seconds=max_age_seconds,
    )
    return {
        "status": str(snapshot.get("status") or "missing"),
        "source_id": str(source_id),
        "job_key": None if job_key is None else str(job_key),
        "ticker_source_run_id": snapshot.get("ticker_source_run_id"),
        "ticker_source_type": snapshot.get("ticker_source_type"),
        "job_run_id": snapshot.get("job_run_id"),
        "generated_at": snapshot.get("generated_at"),
        "age_seconds": snapshot.get("age_seconds"),
        "symbols": list(snapshot.get("symbols") or []),
        "entries": [dict(item) for item in list(snapshot.get("entries") or []) if isinstance(item, Mapping)],
        "summary": dict(snapshot.get("summary") or {}),
        "degradation": dict(snapshot.get("degradation") or {}),
    }


def resolve_ticker_source_symbols(
    engine_facts: Any,
    *,
    source_id: str,
    job_key: str,
    max_age_seconds: int | None = None,
    fallback_universe_ref: str | None = None,
    config_root: str | None = None,
) -> dict[str, Any]:
    snapshot = get_latest_ticker_source_snapshot(
        engine_facts,
        source_id=source_id,
        job_key=job_key,
        max_age_seconds=max_age_seconds,
    )
    snapshot_status = str(snapshot.get("status") or "").strip().lower()
    if snapshot_status in {"ready", "empty"}:
        return {
            "kind": "ticker_source",
            "status": snapshot_status,
            "source_id": str(source_id),
            "job_key": str(job_key),
            "ticker_source_run_id": snapshot.get("ticker_source_run_id"),
            "job_run_id": snapshot.get("job_run_id"),
            "generated_at": snapshot.get("generated_at"),
            "age_seconds": snapshot.get("age_seconds"),
            "symbols": list(snapshot.get("symbols") or []),
            "entries": [dict(item) for item in list(snapshot.get("entries") or []) if isinstance(item, Mapping)],
            "summary": dict(snapshot.get("summary") or {}),
            "degradation": dict(snapshot.get("degradation") or {}),
        }
    if fallback_universe_ref:
        fallback_symbols = load_universe_symbols(
            fallback_universe_ref,
            config_root=config_root,
        )
        return {
            "kind": "fallback_universe",
            "status": "fallback",
            "source_id": str(source_id),
            "job_key": str(job_key),
            "fallback_universe_ref": str(fallback_universe_ref),
            "symbols": list(fallback_symbols),
            "summary": {
                "symbol_count": len(fallback_symbols),
                "fallback_universe_ref": str(fallback_universe_ref),
            },
            "degradation": {
                "status": "fallback",
                "reason": snapshot_status or "missing",
            },
            "source_snapshot": snapshot,
        }
    return {
        "kind": "ticker_source",
        "status": snapshot_status or "missing",
        "source_id": str(source_id),
        "job_key": str(job_key),
        "symbols": [],
        "ticker_source_run_id": snapshot.get("ticker_source_run_id"),
        "entries": [dict(item) for item in list(snapshot.get("entries") or []) if isinstance(item, Mapping)],
        "summary": dict(snapshot.get("summary") or {}),
        "degradation": dict(snapshot.get("degradation") or {}),
        "source_snapshot": snapshot,
    }


__all__ = [
    "VALID_TICKER_SOURCE_RECIPES",
    "build_ticker_source_symbols",
    "get_latest_ticker_source_snapshot",
    "persist_ticker_source_result",
    "resolve_ticker_source_symbols",
    "run_ticker_source",
]
