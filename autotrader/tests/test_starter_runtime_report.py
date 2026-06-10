from __future__ import annotations

from autotrader import starter_runtime_report


def test_starter_runtime_report_contains_contract_fields():
    report = starter_runtime_report.build_report()

    assert "current_commit" in report
    assert "runtime_mode" in report
    assert "ticker_universe" in report
    assert "providers" in report
    assert "runtime_caps" in report
    assert "diagnostics" in report
    assert "memory_env" in report
    assert "enable_yfinance_fallback" in report["providers"]
    assert "option_enrichment_max_attempts_per_cycle" in report["runtime_caps"]
    assert "max_contracts_per_ticker_per_hour" in report["runtime_caps"]


def test_starter_runtime_report_applies_effective_starter_safe_values(monkeypatch):
    monkeypatch.setenv("RENDER_STARTER_SAFE_MODE", "true")
    monkeypatch.setenv("ENABLE_YFINANCE_FALLBACK", "false")
    monkeypatch.setenv("DASHBOARD_TRUTH_CACHE_SECONDS", "30")

    report = starter_runtime_report.build_report()

    assert report["runtime_mode"]["render_starter_safe_mode"] is True
    assert report["ticker_universe"]["universe_mode"] == "core"
    assert report["ticker_universe"]["auto_expand_universe_with_movers"] is False
    assert report["providers"]["enable_yfinance_fallback"] is False
    assert report["runtime_caps"]["option_enrichment_max_attempts_per_cycle"] <= 2
    assert report["runtime_caps"]["max_contracts_per_ticker_per_hour"] <= 1
    assert report["runtime_caps"]["loop_interval_seconds"] >= 60
    assert report["diagnostics"]["dashboard_truth_cache_seconds"] >= 30
    assert report["diagnostics"]["disable_verbose_market_diagnostics"] is True
