from __future__ import annotations

import ast
import os
from pathlib import Path


def _load_render_helper():
    path = Path(__file__).resolve().parent.parent / "render_service_dashboard_v2.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    helper = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "_is_render_starter_runtime"
    )
    module = ast.Module(body=[helper], type_ignores=[])
    ast.fix_missing_locations(module)
    namespace = {"os": os}
    exec(compile(module, str(path), "exec"), namespace)
    return namespace["_is_render_starter_runtime"]


def test_render_starter_runtime_detects_render_web(monkeypatch):
    monkeypatch.setenv("RENDER", "true")
    monkeypatch.setenv("RENDER_SERVICE_TYPE", "web")

    assert _load_render_helper()() is True


def test_render_starter_runtime_ignores_local(monkeypatch):
    monkeypatch.delenv("RENDER", raising=False)
    monkeypatch.delenv("RENDER_SERVICE_TYPE", raising=False)

    assert _load_render_helper()() is False


def test_render_starter_defaults_disable_nonessential_sidecars():
    path = Path(__file__).resolve().parent.parent / "render_service_dashboard_v2.py"
    source = path.read_text(encoding="utf-8")

    for expected in (
        'os.environ.setdefault("VIXW_HEAVY_MODE", "false")',
        'os.environ.setdefault("ENABLE_REPLAY_AUTO_PROMOTE", "false")',
        'os.environ.setdefault("REPLAY_AUTO_PROMOTE_ENABLED", "false")',
        'os.environ.setdefault("ENABLE_HISTORICAL_REPLAY_LEARNING", "false")',
        'os.environ.setdefault("ENABLE_DECISION_MEMORY_WORKER", "false")',
        'os.environ.setdefault("ENABLE_MARKET_CONTEXT_WORKER", "false")',
    ):
        assert expected in source


def test_vixw_smoke_profile_enables_exact_runtime_flag():
    path = Path(__file__).resolve().parent.parent / "render_service_dashboard_v2.py"
    source = path.read_text(encoding="utf-8")

    assert '"VIXW_HEAVY_MODE": "true"' in source
    assert '"VIXW_ONLY_PAPER_MODE": "true"' in source
    assert 'profile == "vixw_paper_smoke"' in source


def test_vixw_enablement_snapshot_logs_runtime_flag_and_reachability():
    path = Path(__file__).resolve().parent.parent / "render_service_dashboard_v2.py"
    source = path.read_text(encoding="utf-8")

    assert 'env_var = "VIXW_HEAVY_MODE"' in source
    assert "VIXW runtime enabled:" in source
    assert "VIXW_ONLY_PAPER_MODE=" in source
    assert "run_vixw_regime_forever reachable:" in source
    assert 'runtime_telemetry.set_worker("vixw_regime_sidecar", bool(vixw_snapshot["runtime_enabled"]))' in source
