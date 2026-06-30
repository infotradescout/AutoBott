from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from .phase1_replay_campaign import run_replay_campaign
from .phase1_snapshot_corpus import SnapshotCorpusQualityRules, load_snapshot_corpus


def run_phase1_campaign(
    snapshot_corpus: str | Path,
    *,
    symbols: list[str] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    exit_policy: str = "fixed_v1",
    artifacts_root: str | Path | None = None,
    campaign_run_id: str = "default",
    active_gate_path: str | Path | None = None,
    quality_rules: SnapshotCorpusQualityRules | None = None,
) -> dict[str, Any]:
    if exit_policy != "fixed_v1":
        raise ValueError(f"unsupported_exit_policy:{exit_policy}")

    corpus_summary = load_snapshot_corpus(
        snapshot_corpus,
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        quality_rules=quality_rules,
    )
    if not corpus_summary["quality"]["campaign_ready"]:
        reasons = ",".join(corpus_summary["quality"]["blocking_reasons"])
        raise ValueError(f"snapshot_corpus_not_campaign_ready:{reasons}")

    return run_replay_campaign(
        corpus_summary["snapshot_paths"],
        artifacts_root=artifacts_root,
        campaign_run_id=campaign_run_id,
        active_gate_path=active_gate_path,
        snapshot_source_label=str(snapshot_corpus),
        campaign_context={"corpus_summary": corpus_summary},
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run a Phase 1 replay campaign from a manifest-backed snapshot corpus.")
    parser.add_argument("--snapshot-corpus", required=True, help="Root directory containing YYYY-MM-DD/SYMBOL/manifest.json entries.")
    parser.add_argument("--symbols", nargs="*", help="Optional symbol filter, for example: SPY QQQ")
    parser.add_argument("--start-date", help="Inclusive YYYY-MM-DD start date filter.")
    parser.add_argument("--end-date", help="Inclusive YYYY-MM-DD end date filter.")
    parser.add_argument("--exit-policy", default="fixed_v1", help="Replay exit policy version.")
    parser.add_argument("--out", help="Artifacts root directory. Defaults to artifacts/phase1_replay_campaign.")
    parser.add_argument("--campaign-run-id", default="default", help="Stable campaign identifier for artifact output.")
    args = parser.parse_args(argv)

    result = run_phase1_campaign(
        args.snapshot_corpus,
        symbols=args.symbols,
        start_date=args.start_date,
        end_date=args.end_date,
        exit_policy=args.exit_policy,
        artifacts_root=args.out,
        campaign_run_id=args.campaign_run_id,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
