from __future__ import annotations

import json
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
from sklearn.cluster import HDBSCAN, MiniBatchKMeans
from sklearn.impute import SimpleImputer
from sklearn.metrics import adjusted_rand_score, silhouette_score
from sklearn.preprocessing import StandardScaler
from core.value_coercion import coerce_float

DEFAULT_CLUSTER_FEATURE_COLUMNS = (
    "ff__revenue_ttm_growth",
    "ff__gross_margin_ttm",
    "ff__operating_margin_ttm",
    "ff__net_margin_ttm",
    "ff__free_cash_flow_margin_ttm",
    "ff__capex_intensity",
    "ff__net_leverage",
    "ff__roic_ttm",
    "ff__asset_turnover",
    "ff__inventory_turns",
    "ff__diluted_share_growth_ttm",
    "ff__sbc_as_pct_revenue",
    "ff__deferred_revenue_growth",
    "of__beneficial_owner_total_pct",
    "of__beneficial_owner_max_pct",
    "of__institutional_holder_count",
    "of__institutional_top_holder_pct_of_tracked",
)

DEFAULT_MULTIPLE_COLUMNS = (
    "ev_ebit_at_as_of",
    "ev_fcf_at_as_of",
    "pb_at_as_of",
    "ps_at_as_of",
)

DEFAULT_SUMMARY_COLUMNS = (
    "quality_score",
    "market_price_close",
    "valuation_gap_at_as_of",
    "ff__revenue_ttm_growth",
    "ff__gross_margin_ttm",
    "ff__operating_margin_ttm",
    "ff__free_cash_flow_margin_ttm",
    "ff__capex_intensity",
    "ff__net_leverage",
    "ff__roic_ttm",
    "ff__asset_turnover",
    "ff__inventory_turns",
    "of__beneficial_owner_total_pct",
    "of__institutional_holder_count",
    "market_cap_at_as_of",
    "ev_ebit_at_as_of",
    "ev_fcf_at_as_of",
    "pb_at_as_of",
    "ps_at_as_of",
)


@dataclass(frozen=True)
class CompanyValuationClusteringRequest:
    dataset_root: str
    output_root: str | None = None
    template_ids: tuple[str, ...] | None = None
    min_k: int = 2
    max_k: int = 6
    random_state: int = 42
    min_rows_per_template: int = 30
    min_rows_per_cluster: int = 12


@dataclass(frozen=True)
class CompanyValuationClusteringResult:
    status: str
    started_at: datetime
    completed_at: datetime
    dataset_root: str
    output_root: str
    template_summaries: dict[str, Any] = field(default_factory=dict)
    assignment_count: int = 0
    summary_path: str | None = None
    assignments_path: str | None = None
    markdown_path: str | None = None
    errors: tuple[str, ...] = ()

    def to_payload(self) -> dict[str, Any]:
        return asdict(self)


def _normalized_template_ids(values: tuple[str, ...] | None) -> tuple[str, ...]:
    return tuple(dict.fromkeys(str(value).strip() for value in (values or ()) if str(value or "").strip()))


def _as_float_series(frame: pd.DataFrame, column: str) -> pd.Series:
    return pd.to_numeric(frame[column], errors="coerce")


def _winsorize_series(series: pd.Series, *, lower: float = 0.02, upper: float = 0.98) -> pd.Series:
    valid = series.dropna()
    if valid.empty:
        return series
    low = valid.quantile(lower)
    high = valid.quantile(upper)
    return series.clip(lower=low, upper=high)


def _load_dataset(dataset_root: Path) -> pd.DataFrame:
    if not dataset_root.exists():
        raise FileNotFoundError(f"Dataset root does not exist: {dataset_root}")
    if dataset_root.is_file():
        suffix = dataset_root.suffix.lower()
        if suffix == ".parquet":
            return pd.read_parquet(dataset_root)
        if suffix in {".jsonl", ".ndjson"}:
            return pd.read_json(dataset_root, lines=True)
        raise ValueError(f"Unsupported dataset file format: {dataset_root}")
    parquet_files = list(dataset_root.rglob("*.parquet"))
    if parquet_files:
        return pd.read_parquet(dataset_root)
    jsonl_files = list(dataset_root.rglob("*.jsonl"))
    if jsonl_files:
        frames = [pd.read_json(path, lines=True) for path in sorted(jsonl_files)]
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    raise ValueError(f"No supported dataset files found under {dataset_root}")


def _prepare_feature_matrix(frame: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    available_columns = [column for column in DEFAULT_CLUSTER_FEATURE_COLUMNS if column in frame.columns]
    if not available_columns:
        raise ValueError("No configured clustering feature columns are available in the dataset")
    working = frame.copy()
    transformed_columns: list[str] = []
    for column in available_columns:
        numeric = _as_float_series(working, column)
        if numeric.notna().sum() < max(8, int(len(working) * 0.10)):
            continue
        working[column] = _winsorize_series(numeric)
        working[f"{column}__missing"] = numeric.isna().astype(int)
        transformed_columns.append(column)
        transformed_columns.append(f"{column}__missing")
    if not transformed_columns:
        raise ValueError("All clustering feature columns were too sparse after preprocessing")
    matrix_frame = working[transformed_columns].copy()
    for column in matrix_frame.columns:
        matrix_frame[column] = _as_float_series(matrix_frame, column)
    numeric_columns = [column for column in transformed_columns if not column.endswith("__missing")]
    imputer = SimpleImputer(strategy="median")
    matrix_frame.loc[:, numeric_columns] = imputer.fit_transform(matrix_frame[numeric_columns])
    scaler = StandardScaler()
    matrix_frame.loc[:, numeric_columns] = scaler.fit_transform(matrix_frame[numeric_columns])
    return matrix_frame, transformed_columns


def _rolling_kmeans_stability(
    matrix_frame: pd.DataFrame,
    *,
    years: pd.Series,
    k: int,
    random_state: int,
) -> float | None:
    unique_years = sorted({int(value) for value in years.dropna().tolist()})
    if len(unique_years) < 4:
        return None
    scores: list[float] = []
    for index in range(2, len(unique_years)):
        prior_cutoff = unique_years[index - 1]
        next_cutoff = unique_years[index]
        prior_mask = years.astype(int) <= prior_cutoff
        next_mask = years.astype(int) <= next_cutoff
        if prior_mask.sum() < max(k * 4, 20) or next_mask.sum() < max(k * 4, 20):
            continue
        model_left = MiniBatchKMeans(
            n_clusters=k,
            random_state=random_state,
            batch_size=min(max(len(matrix_frame), 256), 2048),
            n_init="auto",
        )
        model_right = MiniBatchKMeans(
            n_clusters=k,
            random_state=random_state,
            batch_size=min(max(len(matrix_frame), 256), 2048),
            n_init="auto",
        )
        model_left.fit(matrix_frame.loc[prior_mask].to_numpy())
        model_right.fit(matrix_frame.loc[next_mask].to_numpy())
        common_matrix = matrix_frame.loc[prior_mask].to_numpy()
        left_labels = model_left.predict(common_matrix)
        right_labels = model_right.predict(common_matrix)
        scores.append(adjusted_rand_score(left_labels, right_labels))
    if not scores:
        return None
    return round(sum(scores) / len(scores), 6)


def _evaluate_kmeans_grid(
    matrix_frame: pd.DataFrame,
    *,
    years: pd.Series,
    min_k: int,
    max_k: int,
    random_state: int,
) -> tuple[list[dict[str, Any]], MiniBatchKMeans, pd.Series]:
    evaluations: list[dict[str, Any]] = []
    best_payload: dict[str, Any] | None = None
    best_model: MiniBatchKMeans | None = None
    best_labels: pd.Series | None = None
    matrix = matrix_frame.to_numpy()
    max_clusters = min(max_k, max(2, len(matrix_frame) - 1))
    for k in range(max(min_k, 2), max_clusters + 1):
        model = MiniBatchKMeans(
            n_clusters=k,
            random_state=random_state,
            batch_size=min(max(len(matrix_frame), 256), 2048),
            n_init="auto",
        )
        labels = pd.Series(model.fit_predict(matrix), index=matrix_frame.index)
        cluster_count = labels.nunique()
        silhouette = None
        if cluster_count >= 2 and len(matrix_frame) > cluster_count:
            silhouette = float(silhouette_score(matrix, labels.to_numpy()))
        stability = _rolling_kmeans_stability(
            matrix_frame,
            years=years,
            k=k,
            random_state=random_state,
        )
        complexity_penalty = max(k - 2, 0) * 0.03
        score = (silhouette or -1.0) + ((stability or 0.0) * 0.25) - complexity_penalty
        payload = {
            "k": k,
            "cluster_count": int(cluster_count),
            "silhouette_score": None if silhouette is None else round(silhouette, 6),
            "rolling_stability_ari": stability,
            "complexity_penalty": round(complexity_penalty, 6),
            "selection_score": round(score, 6),
        }
        evaluations.append(payload)
        if best_payload is None or score > float(best_payload["selection_score"]):
            best_payload = payload
            best_model = model
            best_labels = labels
    if best_model is None or best_labels is None:
        raise ValueError("Unable to fit MiniBatchKMeans on the dataset")
    return evaluations, best_model, best_labels


def _evaluate_hdbscan(
    matrix_frame: pd.DataFrame,
    *,
    min_rows_per_cluster: int,
) -> tuple[HDBSCAN, pd.Series, dict[str, Any]]:
    min_cluster_size = max(min_rows_per_cluster, max(8, len(matrix_frame) // 20))
    min_samples = max(5, min_cluster_size // 2)
    model = HDBSCAN(
        min_cluster_size=min_cluster_size,
        min_samples=min_samples,
        allow_single_cluster=False,
        copy=True,
    )
    labels = pd.Series(model.fit_predict(matrix_frame.to_numpy()), index=matrix_frame.index)
    non_noise = labels[labels >= 0]
    silhouette = None
    if non_noise.nunique() >= 2 and len(non_noise) > non_noise.nunique():
        silhouette = float(
            silhouette_score(
                matrix_frame.loc[non_noise.index].to_numpy(),
                non_noise.to_numpy(),
            )
        )
    payload = {
        "cluster_count": int(non_noise.nunique()),
        "noise_count": int((labels < 0).sum()),
        "noise_fraction": round(float((labels < 0).mean()), 6),
        "silhouette_score": None if silhouette is None else round(silhouette, 6),
        "min_cluster_size": int(min_cluster_size),
        "min_samples": int(min_samples),
    }
    return model, labels, payload


def _cluster_feature_medians(frame: pd.DataFrame) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for column in DEFAULT_SUMMARY_COLUMNS:
        if column not in frame.columns:
            continue
        numeric = _as_float_series(frame, column)
        if numeric.notna().any():
            payload[column] = round(float(numeric.median()), 6)
    return payload


def _proposed_cluster_label(
    *,
    template_id: str,
    medians: dict[str, Any],
) -> str:
    gross_margin = coerce_float(medians.get("ff__gross_margin_ttm")) or 0.0
    roic = coerce_float(medians.get("ff__roic_ttm")) or 0.0
    capex_intensity = coerce_float(medians.get("ff__capex_intensity")) or 0.0
    asset_turnover = coerce_float(medians.get("ff__asset_turnover")) or 0.0
    market_cap = coerce_float(medians.get("market_cap_at_as_of")) or 0.0
    net_leverage = coerce_float(medians.get("ff__net_leverage")) or 0.0
    quality_score = coerce_float(medians.get("quality_score")) or 0.0
    free_cash_flow_margin = coerce_float(medians.get("ff__free_cash_flow_margin_ttm")) or 0.0
    operating_margin = coerce_float(medians.get("ff__operating_margin_ttm")) or 0.0
    revenue_growth = coerce_float(medians.get("ff__revenue_ttm_growth")) or 0.0
    ps_multiple = coerce_float(medians.get("ps_at_as_of")) or 0.0
    if template_id == "industrial_manufacturing":
        if gross_margin >= 0.40 and ps_multiple >= 3.0:
            return "electrification_automation_compounder"
        if net_leverage >= 4.0 or revenue_growth < 0.0:
            return "cyclical_capital_goods"
        if roic >= 0.18 and asset_turnover >= 0.60:
            return "industrial_core_compounder"
        if capex_intensity >= 0.035 and asset_turnover >= 0.45:
            return "cyclical_heavy_equipment"
        return "diversified_industrial_platform"
    if template_id == "energy_asset_heavy":
        if quality_score < 50.0 or net_leverage >= 3.0 or free_cash_flow_margin <= 0.08 or operating_margin <= 0.0:
            return "stressed_operator"
        if market_cap >= 100_000_000_000:
            return "healthy_energy_core"
        return "upstream_ep_compounder"
    return "candidate_subtemplate"


def _multiple_regime_rank(
    cluster_summaries: list[dict[str, Any]],
    *,
    column: str,
) -> dict[int, str]:
    scored: list[tuple[int, float]] = []
    for summary in cluster_summaries:
        cluster_id = int(summary["cluster_id"])
        value = coerce_float(summary.get("median_features", {}).get(column))
        if value is None:
            continue
        scored.append((cluster_id, value))
    if not scored:
        return {}
    scored.sort(key=lambda item: item[1])
    if len(scored) == 1:
        return {scored[0][0]: "mid"}
    if len(scored) == 2:
        return {scored[0][0]: "low", scored[1][0]: "high"}
    mapping: dict[int, str] = {}
    for index, (cluster_id, _value) in enumerate(scored):
        if index == 0:
            mapping[cluster_id] = "low"
        elif index == len(scored) - 1:
            mapping[cluster_id] = "high"
        else:
            mapping[cluster_id] = "mid"
    return mapping


def _summarize_clusters(
    frame: pd.DataFrame,
    *,
    label_column: str,
    template_id: str,
) -> list[dict[str, Any]]:
    summaries: list[dict[str, Any]] = []
    for cluster_id, cluster_frame in frame.groupby(label_column):
        if int(cluster_id) < 0:
            continue
        issuer_counts = Counter(cluster_frame["ticker"].astype(str).tolist())
        medians = _cluster_feature_medians(cluster_frame)
        summaries.append(
            {
                "cluster_id": int(cluster_id),
                "row_count": int(len(cluster_frame)),
                "issuer_count": int(cluster_frame["ticker"].nunique()),
                "tickers": sorted(issuer_counts.keys()),
                "ticker_row_counts": dict(sorted(issuer_counts.items())),
                "median_features": medians,
                "proposed_subtemplate_label": _proposed_cluster_label(
                    template_id=template_id,
                    medians=medians,
                ),
            }
        )
    regime_ev_ebit = _multiple_regime_rank(summaries, column="ev_ebit_at_as_of")
    regime_ev_fcf = _multiple_regime_rank(summaries, column="ev_fcf_at_as_of")
    regime_pb = _multiple_regime_rank(summaries, column="pb_at_as_of")
    for summary in summaries:
        cluster_id = int(summary["cluster_id"])
        summary["multiple_regime_labels"] = {
            "ev_ebit": regime_ev_ebit.get(cluster_id),
            "ev_fcf": regime_ev_fcf.get(cluster_id),
            "pb": regime_pb.get(cluster_id),
        }
    return sorted(summaries, key=lambda item: int(item["cluster_id"]))


def _issuer_cluster_mapping(
    frame: pd.DataFrame,
    *,
    label_column: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for ticker, ticker_frame in frame.groupby("ticker"):
        counts = Counter(int(value) for value in ticker_frame[label_column].tolist())
        dominant_cluster, dominant_count = counts.most_common(1)[0]
        rows.append(
            {
                "ticker": str(ticker),
                "dominant_cluster": int(dominant_cluster),
                "dominant_fraction": round(dominant_count / len(ticker_frame), 6),
                "cluster_row_counts": dict(sorted(counts.items())),
            }
        )
    return sorted(rows, key=lambda item: item["ticker"])


def _template_markdown_block(template_id: str, summary: dict[str, Any]) -> str:
    lines = [f"## `{template_id}`", ""]
    lines.append(f"- rows: `{summary['row_count']}` | issuers: `{summary['issuer_count']}` | feature columns: `{len(summary['feature_columns'])}`")
    best = summary["kmeans"]["best"]
    lines.append(
        f"- best `MiniBatchKMeans`: `k={best['k']}` with silhouette `{best['silhouette_score']}` and rolling ARI `{best['rolling_stability_ari']}`"
    )
    hdbscan = summary["hdbscan"]
    lines.append(
        f"- `HDBSCAN`: clusters `{hdbscan['cluster_count']}`, noise fraction `{hdbscan['noise_fraction']}`, silhouette `{hdbscan['silhouette_score']}`"
    )
    lines.append("")
    lines.append("Issuer mapping:")
    for row in summary["issuer_cluster_map"]:
        lines.append(f"- `{row['ticker']}` -> cluster `{row['dominant_cluster']}` (`{row['dominant_fraction']:.2%}` dominant)")
    lines.append("")
    lines.append("Candidate sub-templates:")
    for cluster in summary["cluster_summaries"]:
        labels = cluster["multiple_regime_labels"]
        lines.append(
            f"- cluster `{cluster['cluster_id']}`: `{cluster['proposed_subtemplate_label']}` | tickers `{', '.join(cluster['tickers'])}` | regimes `ev_ebit={labels.get('ev_ebit')}`, `ev_fcf={labels.get('ev_fcf')}`, `pb={labels.get('pb')}`"
        )
    lines.append("")
    return "\n".join(lines)


def _write_markdown_summary(
    *,
    request: CompanyValuationClusteringRequest,
    template_summaries: dict[str, Any],
    markdown_path: Path,
) -> None:
    lines = [
        "# Company Valuation Template Discovery Run",
        "",
        f"- dataset_root: `{request.dataset_root}`",
        f"- templates: `{', '.join(sorted(template_summaries.keys()))}`",
        "",
    ]
    for template_id in sorted(template_summaries):
        lines.append(_template_markdown_block(template_id, template_summaries[template_id]))
    markdown_path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")


def analyze_company_valuation_research_dataset(
    request: CompanyValuationClusteringRequest,
) -> CompanyValuationClusteringResult:
    started_at = datetime.now(UTC)
    dataset_root = Path(request.dataset_root)
    output_root = Path(request.output_root) if request.output_root else dataset_root.parent / "analysis"
    output_root.mkdir(parents=True, exist_ok=True)

    frame = _load_dataset(dataset_root)
    if frame.empty:
        raise ValueError("Research dataset is empty")
    if "template_id" not in frame.columns or "ticker" not in frame.columns:
        raise ValueError("Research dataset is missing required columns")
    if "as_of" in frame.columns:
        frame["as_of"] = pd.to_datetime(frame["as_of"], utc=True, errors="coerce")
    if "as_of_year" not in frame.columns and "as_of" in frame.columns:
        frame["as_of_year"] = frame["as_of"].dt.year

    template_ids = _normalized_template_ids(request.template_ids) or tuple(sorted(str(value) for value in frame["template_id"].dropna().unique()))
    template_summaries: dict[str, Any] = {}
    assignment_frames: list[pd.DataFrame] = []
    errors: list[str] = []

    for template_id in template_ids:
        template_frame = frame.loc[frame["template_id"] == template_id].copy()
        if len(template_frame) < request.min_rows_per_template:
            errors.append(f"{template_id}: not enough rows for clustering ({len(template_frame)} < {request.min_rows_per_template})")
            continue
        try:
            matrix_frame, feature_columns = _prepare_feature_matrix(template_frame)
            years = pd.to_numeric(template_frame["as_of_year"], errors="coerce")
            kmeans_evaluations, _best_model, kmeans_labels = _evaluate_kmeans_grid(
                matrix_frame,
                years=years,
                min_k=request.min_k,
                max_k=request.max_k,
                random_state=request.random_state,
            )
            hdbscan_model, hdbscan_labels, hdbscan_summary = _evaluate_hdbscan(
                matrix_frame,
                min_rows_per_cluster=request.min_rows_per_cluster,
            )
            template_frame["kmeans_cluster"] = kmeans_labels.astype(int)
            template_frame["hdbscan_cluster"] = hdbscan_labels.astype(int)
            template_frame["cluster_method"] = "kmeans"
            assignment_frames.append(template_frame.copy())

            best_kmeans = max(kmeans_evaluations, key=lambda row: float(row["selection_score"]))
            cluster_summaries = _summarize_clusters(
                template_frame,
                label_column="kmeans_cluster",
                template_id=template_id,
            )
            template_summaries[template_id] = {
                "row_count": int(len(template_frame)),
                "issuer_count": int(template_frame["ticker"].nunique()),
                "feature_columns": feature_columns,
                "kmeans": {
                    "evaluations": kmeans_evaluations,
                    "best": best_kmeans,
                },
                "hdbscan": hdbscan_summary,
                "issuer_cluster_map": _issuer_cluster_mapping(
                    template_frame,
                    label_column="kmeans_cluster",
                ),
                "cluster_summaries": cluster_summaries,
                "hdbscan_cluster_counts": dict(sorted(Counter(int(value) for value in hdbscan_model.labels_).items())),
            }
        except Exception as exc:
            errors.append(f"{template_id}: {exc}")

    if not template_summaries:
        raise ValueError("No template cohorts were successfully analyzed")

    assignments = pd.concat(assignment_frames, ignore_index=True) if assignment_frames else pd.DataFrame()
    assignments_path = output_root / "cluster_assignments.parquet"
    if not assignments.empty:
        assignments.to_parquet(assignments_path, engine="pyarrow", index=False)
    summary_path = output_root / "cluster_summary.json"
    summary_path.write_text(
        json.dumps(template_summaries, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    markdown_path = output_root / "cluster_summary.md"
    _write_markdown_summary(
        request=request,
        template_summaries=template_summaries,
        markdown_path=markdown_path,
    )

    completed_at = datetime.now(UTC)
    return CompanyValuationClusteringResult(
        status="ok" if not errors else "partial",
        started_at=started_at,
        completed_at=completed_at,
        dataset_root=str(dataset_root),
        output_root=str(output_root),
        template_summaries=template_summaries,
        assignment_count=0 if assignments.empty else int(len(assignments)),
        summary_path=str(summary_path),
        assignments_path=None if assignments.empty else str(assignments_path),
        markdown_path=str(markdown_path),
        errors=tuple(errors),
    )


__all__ = [
    "CompanyValuationClusteringRequest",
    "CompanyValuationClusteringResult",
    "analyze_company_valuation_research_dataset",
]
