from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import autobott_v2.paper_readiness as paper_readiness
from autobott_v2.paper_readiness import run_paper_readiness_probe
from autobott_v2.runtime_control import default_runtime_state, save_runtime_state


class FakePaperClient:
    def get_account(self):
        return {"status": "ACTIVE"}

    def get_latest_stock_quotes(self, symbols):
        return {
            symbol.upper(): {
                "t": "2026-07-01T15:35:00Z",
                "bp": 599.9 if symbol.upper() == "SPY" else 519.9,
                "ap": 600.1 if symbol.upper() == "SPY" else 520.1,
            }
            for symbol in symbols
        }

    def get_stock_bars(self, symbols, *, start, end, timeframe="1Min", limit=35):
        rows = [
            {
                "t": (end).isoformat().replace("+00:00", "Z"),
                "o": 600.0,
                "h": 600.2,
                "l": 599.8,
                "c": 600.1,
                "v": 1000,
            }
            for _ in range(35)
        ]
        return {symbol.upper(): list(rows) for symbol in symbols}

    def get_option_chain_snapshots(self, symbol):
        return {
            f"{symbol.upper()}260703C00600000": {
                "latestQuote": {"bp": 4.9, "ap": 5.1, "t": "2026-07-01T15:35:00Z"},
                "greeks": {"delta": 0.55, "theta": -0.04, "vega": 0.08, "iv": 0.22},
                "details": {"expiration_date": "2026-07-03", "strike_price": 600, "type": "call"},
                "dailyBar": {"v": 500},
                "open_interest": 1000,
            },
            f"{symbol.upper()}260703P00600000": {
                "latestQuote": {"bp": 4.7, "ap": 4.9, "t": "2026-07-01T15:35:00Z"},
                "greeks": {"delta": -0.45, "theta": -0.04, "vega": 0.08, "iv": 0.24},
                "details": {"expiration_date": "2026-07-03", "strike_price": 600, "type": "put"},
                "dailyBar": {"v": 500},
                "open_interest": 1000,
            },
        }


def test_paper_readiness_probe_returns_paper_ready(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("ALPACA_ENV", "paper")
    monkeypatch.setenv("ALPACA_API_KEY_ID", "paper-key")
    monkeypatch.setenv("ALPACA_API_SECRET_KEY", "paper-secret")
    monkeypatch.setenv("ALPACA_TRADING_BASE_URL", "https://paper-api.alpaca.markets")
    monkeypatch.setenv("ALPACA_DATA_BASE_URL", "https://data.alpaca.markets")
    monkeypatch.setenv("AUTOBOTT_PAPER_ONLY", "true")
    monkeypatch.setenv("AUTOBOTT_ALLOW_ORDER_PLACEMENT", "true")
    monkeypatch.setenv("AUTOBOTT_DATA_ROOT", str(tmp_path / "data"))
    save_runtime_state(default_runtime_state(), state_path=tmp_path / "data" / "execution" / "runtime_state.json")

    result = run_paper_readiness_probe(
        symbol="SPY",
        client=FakePaperClient(),
        corpus_root=tmp_path / "corpus",
        scheduled_market_time=datetime(2026, 7, 1, 15, 35, tzinfo=UTC),
        captured_at_utc=datetime(2026, 7, 1, 15, 35, tzinfo=UTC),
    )

    assert result["ok"] is True
    assert result["status"] == "paper_trading_ready"
    assert result["paper_execution_ready"] is True
    assert result["option_chain_count"] > 0
    assert Path(result["snapshot_path"]).exists()


def test_paper_readiness_probe_reports_connectivity_failure(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("ALPACA_ENV", "paper")
    monkeypatch.setenv("ALPACA_API_KEY_ID", "paper-key")
    monkeypatch.setenv("ALPACA_API_SECRET_KEY", "paper-secret")
    monkeypatch.setenv("ALPACA_TRADING_BASE_URL", "https://paper-api.alpaca.markets")
    monkeypatch.setenv("ALPACA_DATA_BASE_URL", "https://data.alpaca.markets")
    monkeypatch.setenv("AUTOBOTT_PAPER_ONLY", "true")
    monkeypatch.setenv("AUTOBOTT_ALLOW_ORDER_PLACEMENT", "false")
    monkeypatch.setenv("AUTOBOTT_DATA_ROOT", str(tmp_path / "data"))
    save_runtime_state(default_runtime_state(), state_path=tmp_path / "data" / "execution" / "runtime_state.json")

    class FailingClient:
        def get_account(self):
            raise ValueError("network_down")

    result = run_paper_readiness_probe(symbol="SPY", client=FailingClient())

    assert result["ok"] is False
    assert result["status"] == "paper_connectivity_failed"


def test_paper_readiness_probe_reports_execution_blockers(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("ALPACA_ENV", "paper")
    monkeypatch.setenv("ALPACA_API_KEY_ID", "paper-key")
    monkeypatch.setenv("ALPACA_API_SECRET_KEY", "paper-secret")
    monkeypatch.setenv("ALPACA_TRADING_BASE_URL", "https://paper-api.alpaca.markets")
    monkeypatch.setenv("ALPACA_DATA_BASE_URL", "https://data.alpaca.markets")
    monkeypatch.setenv("AUTOBOTT_PAPER_ONLY", "true")
    monkeypatch.setenv("AUTOBOTT_ALLOW_ORDER_PLACEMENT", "false")
    monkeypatch.setenv("AUTOBOTT_DATA_ROOT", str(tmp_path / "data"))
    save_runtime_state(default_runtime_state(), state_path=tmp_path / "data" / "execution" / "runtime_state.json")

    result = run_paper_readiness_probe(
        symbol="SPY",
        client=FakePaperClient(),
        corpus_root=tmp_path / "corpus",
        scheduled_market_time=datetime(2026, 7, 1, 15, 35, tzinfo=UTC),
        captured_at_utc=datetime(2026, 7, 1, 15, 35, tzinfo=UTC),
    )

    assert result["ok"] is True
    assert result["status"] == "paper_data_ready_execution_blocked"
    assert result["paper_execution_ready"] is False
    assert "order_placement_disabled" in result["execution_blockers"]


def test_paper_readiness_main_arms_runtime_and_requires_trading_ready(monkeypatch, capsys) -> None:
    armed = {}

    monkeypatch.setattr(
        paper_readiness,
        "arm_paper_execution",
        lambda reason: armed.setdefault("reason", reason),
    )
    monkeypatch.setattr(
        paper_readiness,
        "run_paper_readiness_probe",
        lambda symbol, corpus_root=None: {"ok": True, "status": "paper_trading_ready", "symbol": symbol},
    )

    exit_code = paper_readiness.main(["--symbol", "QQQ", "--arm-runtime", "--arm-reason", "tomorrow_cutover", "--require-trading-ready"])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert armed["reason"] == "tomorrow_cutover"
    assert payload["status"] == "paper_trading_ready"
    assert payload["symbol"] == "QQQ"


def test_paper_readiness_main_returns_nonzero_when_trading_not_ready(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        paper_readiness,
        "run_paper_readiness_probe",
        lambda symbol, corpus_root=None: {"ok": True, "status": "paper_data_ready_execution_blocked", "symbol": symbol},
    )

    exit_code = paper_readiness.main(["--require-trading-ready"])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 2
    assert payload["status"] == "paper_data_ready_execution_blocked"
