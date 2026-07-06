from __future__ import annotations

from datetime import date
from pathlib import Path

from autobott_v2 import decision_lab_runner


def test_run_historical_decision_lab_expands_top_options_universe(monkeypatch, tmp_path) -> None:
    calls: dict[str, object] = {}

    def _fake_backfill(**kwargs):
        calls["backfill"] = kwargs
        return {"corpus_root": str(kwargs["corpus_root"]), "symbols": kwargs["symbols"]}

    def _fake_campaign(corpus_root, **kwargs):
        calls["campaign"] = {"corpus_root": corpus_root, **kwargs}
        return {"artifact_dir": str(Path(kwargs["artifacts_root"]) / kwargs["campaign_run_id"])}

    def _fake_report(campaign_dir):
        calls["report"] = {"campaign_dir": campaign_dir}
        return {"ok": True, "summary": {"closed_trades": 12}, "baselines": {"actual_vs_no_trade": 42.0}}

    monkeypatch.setattr(decision_lab_runner, "run_historical_backfill", _fake_backfill)
    monkeypatch.setattr(decision_lab_runner, "run_phase1_campaign", _fake_campaign)
    monkeypatch.setattr(decision_lab_runner, "build_decision_lab_report", _fake_report)
    monkeypatch.setattr(decision_lab_runner, "gate_path", lambda: tmp_path / "gate.json")

    result = decision_lab_runner.run_historical_decision_lab(
        symbols=["TOP_OPTIONS_100"],
        start_date=date(2024, 6, 1),
        end_date=date(2024, 6, 10),
        interval_minutes=30,
        run_id="lab1",
        artifacts_root=tmp_path / "artifacts",
    )

    backfill = calls["backfill"]
    campaign = calls["campaign"]
    assert isinstance(backfill, dict)
    assert isinstance(campaign, dict)
    assert len(backfill["symbols"]) == 100
    assert backfill["symbols"][:5] == ["SPY", "QQQ", "IWM", "DIA", "TLT"]
    assert backfill["interval_minutes"] == 30
    assert str(backfill["corpus_root"]).endswith("decision_lab_historical_corpus\\lab1") or str(backfill["corpus_root"]).endswith("decision_lab_historical_corpus/lab1")
    assert campaign["campaign_run_id"] == "lab1"
    assert result["decision_lab"]["summary"]["closed_trades"] == 12
