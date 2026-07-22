from __future__ import annotations

import ast
from pathlib import Path

from autobott_v2.vix_evidence import VixEvidenceRules, resolve_vix_strategy_config, vix_parameter_candidates
from autobott_v2.vix_ibkr_broker import (
    DisabledVixBrokerAdapter,
    VixBrokerExecutionDisabled,
    describe_vix_broker,
    load_vix_broker_adapter,
    vix_execution_enabled,
)
from autobott_v2.vix_sim_runner import DEFAULT_SCENARIOS, run_vix_simulation_campaign, simulate_one_closed_cycle, vix_sim_enabled
from autobott_v2.vix_trader import load_vix_cycles


def test_paper_modules_do_not_import_vix_trader() -> None:
    root = Path(__file__).resolve().parents[1] / "src" / "autobott_v2"
    for name in ("trading_cycle.py", "session_runner.py", "session_supervisor.py", "execution_orchestrator.py", "execution_broker.py"):
        tree = ast.parse((root / name).read_text(encoding="utf-8"))
        imported = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.add(node.module)
                imported.update(f"{node.module}.{alias.name}" for alias in node.names)
        joined = " ".join(sorted(imported))
        assert "vix_trader" not in joined
        assert "vix_evidence" not in joined
        assert "vix_sim_runner" not in joined
        assert "vix_ibkr_broker" not in joined


def test_dashboard_does_not_eagerly_import_vix_at_module_level() -> None:
    path = Path(__file__).resolve().parents[1] / "src" / "autobott_v2" / "dashboard_app.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    top_level_modules: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.ImportFrom) and node.module:
            top_level_modules.add(node.module)
        elif isinstance(node, ast.Import):
            top_level_modules.update(alias.name for alias in node.names)
    assert "vix_trader" not in top_level_modules
    assert "vix_evidence" not in top_level_modules
    assert "vix_sim_runner" not in top_level_modules
    assert "vix_ibkr_broker" not in top_level_modules


def test_vix_sim_and_ibkr_default_off() -> None:
    assert vix_sim_enabled(environ={}) is False
    assert vix_execution_enabled(environ={}) is False
    selection = describe_vix_broker(environ={})
    assert selection.broker_id == "disabled"
    assert selection.to_json_dict()["affects_alpaca_paper"] is False
    adapter = load_vix_broker_adapter(environ={})
    assert isinstance(adapter, DisabledVixBrokerAdapter)


def test_ibkr_requires_dual_arm_and_credentials() -> None:
    selection = describe_vix_broker(environ={"AUTOBOTT_VIX_BROKER": "ibkr", "AUTOBOTT_VIX_EXECUTION_ENABLED": "false"})
    assert selection.execution_enabled is False
    try:
        load_vix_broker_adapter(
            environ={
                "AUTOBOTT_VIX_BROKER": "ibkr",
                "AUTOBOTT_VIX_EXECUTION_ENABLED": "true",
            }
        )
        raised = False
    except VixBrokerExecutionDisabled:
        raised = True
    assert raised is True


def test_simulation_campaign_accumulates_closed_cycles_and_promotes(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("AUTOBOTT_DATA_ROOT", str(tmp_path))
    candidate = vix_parameter_candidates()[0]
    store = tmp_path / "vix_trader" / "cycles.jsonl"
    result = run_vix_simulation_campaign(
        cycles_per_candidate=10,
        candidates=(candidate,),
        scenarios=DEFAULT_SCENARIOS,
        store_path=store,
        evidence_rules=VixEvidenceRules(min_closed_cycles=10, min_profit_factor=1.0, min_expectancy=0.0, max_drawdown_pct_of_capital=100.0),
        write_evidence=True,
    )
    assert result.paper_trading_affected is False
    assert result.cycles_written == 10
    rows = load_vix_cycles(path=store, limit=100)
    assert len(rows) == 10
    assert all(row["lifecycle_state"] == "CLOSED" for row in rows)
    assert all((row.get("strategy_payload") or {}).get("configuration_fingerprint") for row in rows)
    assert all((row.get("strategy_payload") or {}).get("does_not_affect_alpaca_paper") is True for row in rows)
    resolution = resolve_vix_strategy_config(
        cycles=rows,
        candidates=(candidate,),
        rules=VixEvidenceRules(min_closed_cycles=10, min_profit_factor=1.0, min_expectancy=0.0, max_drawdown_pct_of_capital=100.0),
    )
    assert resolution.config is not None
    assert resolution.profitability_status == "evidence_selected"
    assert resolution.selected_metrics["expectancy"] > 0


def test_simulate_one_closed_cycle_is_isolated(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("AUTOBOTT_DATA_ROOT", str(tmp_path))
    candidate = vix_parameter_candidates()[0]
    row = simulate_one_closed_cycle(candidate=candidate, scenario=DEFAULT_SCENARIOS[0], store_path=tmp_path / "one.jsonl")
    assert row["lifecycle_state"] == "CLOSED"
    assert row["strategy_payload"]["simulation_only"] is True
