from __future__ import annotations

from autobott_v2 import runtime_paths


def test_runtime_paths_default_to_repo_relative_roots(monkeypatch) -> None:
    monkeypatch.delenv("AUTOBOTT_DATA_ROOT", raising=False)
    monkeypatch.delenv("AUTOBOTT_ARTIFACTS_ROOT", raising=False)
    monkeypatch.delenv("AUTOBOTT_GATE_PATH", raising=False)

    repo_root = runtime_paths.repo_root()
    assert runtime_paths.data_root() == repo_root / "data"
    assert runtime_paths.artifacts_root() == repo_root / "artifacts"
    assert runtime_paths.gate_path() == repo_root / "data" / "PHASE1_CYCLE_GATE.json"
    assert runtime_paths.phase1_snapshots_root() == repo_root / "data" / "phase1_snapshots"
    assert runtime_paths.phase1_replay_campaign_root() == repo_root / "artifacts" / "phase1_replay_campaign"


def test_runtime_paths_honor_env_overrides(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("AUTOBOTT_DATA_ROOT", str(tmp_path / "data-root"))
    monkeypatch.setenv("AUTOBOTT_ARTIFACTS_ROOT", str(tmp_path / "artifacts-root"))
    monkeypatch.setenv("AUTOBOTT_GATE_PATH", str(tmp_path / "gate-root" / "PHASE1_CYCLE_GATE.json"))

    assert runtime_paths.data_root() == tmp_path / "data-root"
    assert runtime_paths.artifacts_root() == tmp_path / "artifacts-root"
    assert runtime_paths.gate_path() == tmp_path / "gate-root" / "PHASE1_CYCLE_GATE.json"
    assert runtime_paths.phase1_snapshots_root() == tmp_path / "data-root" / "phase1_snapshots"
    assert runtime_paths.phase1_replay_campaign_root() == tmp_path / "artifacts-root" / "phase1_replay_campaign"
