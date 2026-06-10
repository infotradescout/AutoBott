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


def test_render_service_applies_memory_controls_after_config_import():
    path = Path(__file__).resolve().parent.parent / "render_service.py"
    source = path.read_text(encoding="utf-8")

    for expected in (
        'os.environ.setdefault("ENABLE_YFINANCE_FALLBACK", "false")',
        "config.LOOP_INTERVAL_SECONDS = max(60,",
        "config.CONTINUOUS_ENTRY_SEARCH_SLEEP_SECONDS = max(",
        "config.SCAN_INTRADAY_BARS = min(25,",
        "config.ENABLE_YFINANCE_FALLBACK = _env_bool(",
        "config.ENABLE_LOOP_GC = True",
        "config.ENABLE_SIGNAL_PATTERN_MEMORY = False",
    ):
        assert expected in source
