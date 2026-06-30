from __future__ import annotations

import os
from pathlib import Path


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def data_root() -> Path:
    configured = os.getenv("AUTOBOTT_DATA_ROOT")
    return Path(configured) if configured else repo_root() / "data"


def artifacts_root() -> Path:
    configured = os.getenv("AUTOBOTT_ARTIFACTS_ROOT")
    return Path(configured) if configured else repo_root() / "artifacts"


def gate_path() -> Path:
    configured = os.getenv("AUTOBOTT_GATE_PATH")
    return Path(configured) if configured else repo_root() / "data" / "PHASE1_CYCLE_GATE.json"


def phase1_snapshots_root() -> Path:
    return data_root() / "phase1_snapshots"


def phase1_replay_campaign_root() -> Path:
    return artifacts_root() / "phase1_replay_campaign"
