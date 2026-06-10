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
        'os.environ.setdefault("RENDER_STARTER_SAFE_MODE", "true")',
        'os.environ.setdefault("DASHBOARD_TRUTH_CACHE_SECONDS", "30")',
        'os.environ.setdefault("ENABLE_YFINANCE_FALLBACK", "false")',
        'os.environ.setdefault("VIXW_HEAVY_MODE", "false")',
        'os.environ.setdefault("ENABLE_REPLAY_AUTO_PROMOTE", "false")',
        'os.environ.setdefault("REPLAY_AUTO_PROMOTE_ENABLED", "false")',
        'os.environ.setdefault("ENABLE_HISTORICAL_REPLAY_LEARNING", "false")',
        'os.environ.setdefault("ENABLE_DECISION_MEMORY_WORKER", "false")',
        'os.environ.setdefault("ENABLE_MARKET_CONTEXT_WORKER", "false")',
    ):
        assert expected in source


def test_render_blueprint_stays_on_starter_with_no_cost_memory_controls():
    path = Path(__file__).resolve().parents[2] / "render.yaml"
    source = path.read_text(encoding="utf-8")

    for expected in (
        "plan: starter",
        "key: PYTHONMALLOC",
        "value: malloc",
        "key: MALLOC_ARENA_MAX",
        'value: "2"',
        "key: ENABLE_YFINANCE_FALLBACK",
        'value: "false"',
        "key: RENDER_STARTER_SAFE_MODE",
        'value: "true"',
        "key: DASHBOARD_TRUTH_CACHE_SECONDS",
        'value: "30"',
    ):
        assert expected in source


def test_render_service_applies_starter_safe_mode_clamps():
    path = Path(__file__).resolve().parent.parent / "render_service.py"
    source = path.read_text(encoding="utf-8")

    for expected in (
        'os.environ.setdefault("RENDER_STARTER_SAFE_MODE", "true")',
        "def _apply_render_starter_safe_mode()",
        'config.UNIVERSE_MODE = "core"',
        "config.AUTO_EXPAND_UNIVERSE_WITH_MOVERS = False",
        "config.ENABLE_YFINANCE_FALLBACK = False",
        "config.OPTION_ENRICHMENT_MAX_ATTEMPTS_PER_CYCLE = min(",
        "config.MAX_CONTRACTS_PER_TICKER_PER_HOUR = min(",
        "config.DASHBOARD_TRUTH_CACHE_SECONDS = max(",
        "config.DISABLE_VERBOSE_MARKET_DIAGNOSTICS = True",
        "_apply_render_starter_safe_mode()",
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
