from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .phase1_replay import run_replay
from .runtime_paths import artifacts_root as default_artifacts_root


FILL_MODELS = ("optimistic_mid", "realistic_mid_penalty", "conservative", "stress")


def run_slippage_sweep(
    snapshots: str | Path,
    *,
    artifacts_root: str | Path | None = None,
    run_id: str = "default",
) -> dict[str, Any]:
    artifact_dir = (Path(artifacts_root) if artifacts_root is not None else default_artifacts_root() / "phase1_replay") / run_id
    artifact_dir.mkdir(parents=True, exist_ok=True)
    results: dict[str, Any] = {}

    for fill_model in FILL_MODELS:
        replay_result = run_replay(
            snapshots,
            artifacts_root=artifact_dir,
            run_id=fill_model,
            fill_model=fill_model,
        )
        gate_path = artifact_dir / fill_model / "gate.json"
        gate = json.loads(gate_path.read_text(encoding="utf-8"))
        results[fill_model] = {
            "run": replay_result,
            "profit_factor": gate.get("profit_factor", 0.0),
            "expectancy": gate.get("expectancy_per_trade", 0.0),
            "fill_rate": gate.get("fill_rate", 0.0),
            "eligible_for_paper": gate.get("eligible_for_paper", False),
        }

    report = {
        "run_id": run_id,
        "results": results,
    }
    (artifact_dir / "slippage_sweep.json").write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    return report
