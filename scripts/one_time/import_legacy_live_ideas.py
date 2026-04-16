#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
PACKAGES = ROOT / "packages"
if str(PACKAGES) not in sys.path:
    sys.path.insert(0, str(PACKAGES))

from core.storage import build_collector_repository, default_database_url


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="One-time backfill of legacy live idea snapshot/event files into Postgres collector tables."
    )
    parser.add_argument(
        "--db",
        default=default_database_url(),
        help="Target Postgres database URL. Default: SPREADS_DATABASE_URL / DATABASE_URL / local Docker Postgres.",
    )
    parser.add_argument(
        "--live-ideas-dir",
        default=str(ROOT / "outputs" / "live_ideas"),
        help="Directory containing legacy snapshots/ and events_*.jsonl files.",
    )
    parser.add_argument(
        "--labels",
        help="Optional comma-separated label allowlist.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Inspect and report what would be imported without writing to Postgres.",
    )
    parser.add_argument(
        "--replace-existing",
        action="store_true",
        help="Replace existing collector cycles instead of skipping them.",
    )
    return parser.parse_args()


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def load_event_logs(live_ideas_dir: Path) -> dict[str, list[dict[str, Any]]]:
    events_by_cycle: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for path in sorted(live_ideas_dir.glob("events_*.jsonl")):
        with path.open(encoding="utf-8") as handle:
            for raw_line in handle:
                raw_line = raw_line.strip()
                if not raw_line:
                    continue
                payload = json.loads(raw_line)
                cycle_id = payload.get("cycle_id")
                if not cycle_id:
                    continue
                events_by_cycle[str(cycle_id)].append(payload)
    return events_by_cycle


def infer_universe_label(payload: dict[str, Any]) -> str:
    explicit = payload.get("universe_label")
    if explicit:
        return str(explicit)
    label = str(payload["label"])
    strategy = str(payload["strategy"])
    profile = str(payload["profile"])
    greeks_source = str(payload["greeks_source"])
    suffix = f"_{strategy}_{profile}_{greeks_source}"
    if label.endswith(suffix):
        inferred = label[: -len(suffix)]
        if inferred:
            return inferred
    return label


def normalize_candidate(candidate: dict[str, Any]) -> dict[str, Any] | None:
    run_id = candidate.get("run_id")
    if not run_id:
        return None
    return candidate


def main() -> int:
    args = parse_args()
    live_ideas_dir = Path(args.live_ideas_dir)
    snapshots_dir = live_ideas_dir / "snapshots"
    allowed_labels = None
    if args.labels:
        allowed_labels = {
            item.strip() for item in args.labels.split(",") if item.strip()
        }

    if not snapshots_dir.exists():
        raise SystemExit(f"Snapshots directory not found: {snapshots_dir}")

    events_by_cycle = load_event_logs(live_ideas_dir)
    collector_store = build_collector_repository(args.db)

    imported_cycles = 0
    skipped_existing = 0
    skipped_invalid = 0
    imported_candidates = 0
    imported_events = 0

    try:
        for snapshot_path in sorted(snapshots_dir.glob("*.json")):
            payload = load_json(snapshot_path)
            label = str(payload.get("label", ""))
            if allowed_labels and label not in allowed_labels:
                continue

            cycle_id = payload.get("cycle_id")
            generated_at = payload.get("generated_at")
            strategy = payload.get("strategy")
            profile = payload.get("profile")
            greeks_source = payload.get("greeks_source")
            symbols = payload.get("symbols")
            if (
                not cycle_id
                or not generated_at
                or not label
                or not strategy
                or not profile
                or not greeks_source
                or not isinstance(symbols, list)
            ):
                skipped_invalid += 1
                continue

            if (
                collector_store.get_cycle(str(cycle_id)) is not None
                and not args.replace_existing
            ):
                skipped_existing += 1
                continue

            board_candidates = [
                normalized
                for candidate in payload.get("board_candidates", [])
                if isinstance(candidate, dict)
                for normalized in [normalize_candidate(candidate)]
                if normalized is not None
            ]
            watchlist_candidates = [
                normalized
                for candidate in payload.get("watchlist_candidates", [])
                if isinstance(candidate, dict)
                for normalized in [normalize_candidate(candidate)]
                if normalized is not None
            ]
            failures = [
                failure
                for failure in payload.get("failures", [])
                if isinstance(failure, dict)
            ]
            selection_state = payload.get("selection_state")
            if not isinstance(selection_state, dict):
                selection_state = {}

            cycle_events = events_by_cycle.get(str(cycle_id), [])
            normalized_events = [
                event
                for event in cycle_events
                if isinstance(event, dict)
                and event.get("symbol")
                and event.get("event_type")
                and event.get("message")
            ]

            if not args.dry_run:
                collector_store.save_cycle(
                    cycle_id=str(cycle_id),
                    label=label,
                    generated_at=str(generated_at),
                    universe_label=infer_universe_label(payload),
                    strategy=str(strategy),
                    profile=str(profile),
                    greeks_source=str(greeks_source),
                    symbols=[str(symbol) for symbol in symbols],
                    failures=failures,
                    selection_state=selection_state,
                    board_candidates=board_candidates,
                    watchlist_candidates=watchlist_candidates,
                    events=normalized_events,
                )

            imported_cycles += 1
            imported_candidates += len(board_candidates) + len(watchlist_candidates)
            imported_events += len(normalized_events)

        print(
            json.dumps(
                {
                    "dry_run": args.dry_run,
                    "imported_cycles": imported_cycles,
                    "imported_candidates": imported_candidates,
                    "imported_events": imported_events,
                    "skipped_existing": skipped_existing,
                    "skipped_invalid": skipped_invalid,
                },
                indent=2,
            )
        )
    finally:
        collector_store.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
