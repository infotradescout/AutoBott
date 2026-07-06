from __future__ import annotations

import argparse
import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from .decision_lab import build_decision_lab_report
from .options_universe import resolve_symbol_universe
from .phase1_campaign_runner import run_phase1_campaign
from .phase1_historical_backfill import run_historical_backfill
from .runtime_paths import gate_path, phase1_replay_campaign_root


def run_historical_decision_lab(
    *,
    symbols: list[str],
    start_date: date,
    end_date: date,
    interval_minutes: int | None = 30,
    run_id: str | None = None,
    corpus_root: str | Path | None = None,
    artifacts_root: str | Path | None = None,
    active_gate_path: str | Path | None = None,
) -> dict[str, Any]:
    resolved_symbols = resolve_symbol_universe(symbols)
    if not resolved_symbols:
        raise ValueError("symbols_required")

    resolved_run_id = run_id or datetime.now(UTC).strftime("decision-lab-%Y%m%d-%H%M%S")
    resolved_artifacts_root = Path(artifacts_root) if artifacts_root is not None else phase1_replay_campaign_root()
    resolved_corpus_root = Path(corpus_root) if corpus_root is not None else resolved_artifacts_root / "decision_lab_historical_corpus" / resolved_run_id

    backfill = run_historical_backfill(
        symbols=resolved_symbols,
        start_date=start_date,
        end_date=end_date,
        corpus_root=resolved_corpus_root,
        interval_minutes=interval_minutes,
    )
    campaign = run_phase1_campaign(
        resolved_corpus_root,
        artifacts_root=resolved_artifacts_root,
        campaign_run_id=resolved_run_id,
        active_gate_path=active_gate_path or gate_path(),
    )
    report = build_decision_lab_report(resolved_artifacts_root / resolved_run_id)
    return {
        "ok": True,
        "run_id": resolved_run_id,
        "symbols": resolved_symbols,
        "backfill": backfill,
        "campaign": campaign,
        "decision_lab": report,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run historical backfill, replay campaign, and Decision Lab baseline report.")
    parser.add_argument("--symbols", nargs="+", required=True, help="Ticker list, or TOP_OPTIONS_100 for the full options universe.")
    parser.add_argument("--start", required=True, help="Inclusive YYYY-MM-DD start date.")
    parser.add_argument("--end", required=True, help="Inclusive YYYY-MM-DD end date.")
    parser.add_argument("--interval-minutes", type=int, default=30, help="Intraday bar interval. Use 30 or 60 for larger universes.")
    parser.add_argument("--run-id", help="Stable campaign id. Defaults to decision-lab timestamp.")
    parser.add_argument("--corpus-root", help="Optional output root for synthesized snapshots.")
    parser.add_argument("--artifacts-root", help=f"Optional campaign artifacts root. Defaults to {phase1_replay_campaign_root()}.")
    args = parser.parse_args(argv)

    result = run_historical_decision_lab(
        symbols=args.symbols,
        start_date=date.fromisoformat(args.start),
        end_date=date.fromisoformat(args.end),
        interval_minutes=args.interval_minutes,
        run_id=args.run_id,
        corpus_root=args.corpus_root,
        artifacts_root=args.artifacts_root,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
