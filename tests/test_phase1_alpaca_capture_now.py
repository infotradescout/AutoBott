import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from autobott_v2.phase1_alpaca_capture_now import capture_now
from autobott_v2.phase1_alpaca_client import AlpacaPaperClient
from autobott_v2.phase1_alpaca_config import AlpacaPaperConfig


class FakeAlpacaClient:
    def get_latest_stock_quotes(self, symbols):
        return {
            symbol.upper(): {
                "t": datetime(2026, 6, 30, 14, 30, tzinfo=UTC).isoformat(),
                "bp": 599.9 if symbol.upper() == "SPY" else 519.9,
                "ap": 600.1 if symbol.upper() == "SPY" else 520.1,
            }
            for symbol in symbols
        }

    def get_stock_bars(self, symbols, *, start, end, timeframe="1Min", limit=35):
        return {
            symbol.upper(): [
                {
                    "t": (end - timedelta(minutes=index)).isoformat(),
                    "o": 600.0,
                    "h": 600.2,
                    "l": 599.8,
                    "c": 600.1,
                    "v": 1000 + index,
                }
                for index in range(min(limit, 3))
            ]
            for symbol in symbols
        }

    def get_option_chain_snapshots(self, symbol):
        return {
            f"{symbol.upper()}260701C00600000": {
                "latestQuote": {
                    "bp": 4.9,
                    "ap": 5.1,
                }
            }
        }


def _paper_config() -> AlpacaPaperConfig:
    return AlpacaPaperConfig(
        env="paper",
        api_key="paper-key",
        secret_key="paper-secret",
        trading_base_url="https://paper-api.alpaca.markets",
        data_base_url="https://data.alpaca.markets",
        live_trading_enabled=False,
        paper_only=True,
        allow_order_placement=False,
    )


def _now_sequence(*timestamps: datetime):
    iterator = iter(timestamps)

    def _next():
        return next(iterator)

    return _next


def test_alpaca_capture_has_no_order_methods() -> None:
    for method_name in ("submit_order", "replace_order", "cancel_order"):
        assert not hasattr(AlpacaPaperClient, method_name)


def test_capture_manifest_marks_paper_capture(tmp_path) -> None:
    result = capture_now(
        symbols=["SPY"],
        minutes=1,
        interval_seconds=60,
        corpus_root=tmp_path,
        client=FakeAlpacaClient(),
        config=_paper_config(),
        now_fn=_now_sequence(
            datetime(2026, 6, 30, 14, 30, tzinfo=UTC),
            datetime(2026, 6, 30, 14, 30, tzinfo=UTC),
            datetime(2026, 6, 30, 14, 31, tzinfo=UTC),
        ),
        sleep_fn=lambda _: None,
    )

    manifest_path = tmp_path / "2026-06-30" / "SPY" / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert result["corpus_type"] == "paper_capture"
    assert manifest["source"] == "alpaca"
    assert manifest["corpus_type"] == "paper_capture"
    assert manifest["order_placement_enabled"] is False
    assert manifest["snapshots_captured"] == 1
    assert manifest["option_quotes_captured"] == 1


def test_capture_does_not_mutate_active_gate(tmp_path) -> None:
    active_gate = tmp_path / "PHASE1_CYCLE_GATE.json"
    active_gate.write_text(json.dumps({"sentinel": True}, sort_keys=True), encoding="utf-8")

    capture_now(
        symbols=["QQQ"],
        minutes=1,
        interval_seconds=60,
        corpus_root=tmp_path / "captures",
        client=FakeAlpacaClient(),
        config=_paper_config(),
        active_gate_path=active_gate,
        now_fn=_now_sequence(
            datetime(2026, 6, 30, 14, 30, tzinfo=UTC),
            datetime(2026, 6, 30, 14, 30, tzinfo=UTC),
            datetime(2026, 6, 30, 14, 31, tzinfo=UTC),
        ),
        sleep_fn=lambda _: None,
    )

    assert json.loads(active_gate.read_text(encoding="utf-8")) == {"sentinel": True}


def test_capture_uses_env_roots_when_paths_are_omitted(monkeypatch, tmp_path) -> None:
    data_root = tmp_path / "durable-data"
    gate_path = data_root / "PHASE1_CYCLE_GATE.json"
    gate_path.parent.mkdir(parents=True, exist_ok=True)
    gate_path.write_text(json.dumps({"sentinel": True}, sort_keys=True), encoding="utf-8")
    monkeypatch.setenv("AUTOBOTT_DATA_ROOT", str(data_root))
    monkeypatch.setenv("AUTOBOTT_GATE_PATH", str(gate_path))

    result = capture_now(
        symbols=["SPY"],
        minutes=1,
        interval_seconds=60,
        client=FakeAlpacaClient(),
        config=_paper_config(),
        now_fn=_now_sequence(
            datetime(2026, 6, 30, 14, 30, tzinfo=UTC),
            datetime(2026, 6, 30, 14, 30, tzinfo=UTC),
            datetime(2026, 6, 30, 14, 31, tzinfo=UTC),
        ),
        sleep_fn=lambda _: None,
    )

    manifest_path = data_root / "phase1_snapshots" / "2026-06-30" / "SPY" / "manifest.json"
    assert manifest_path.exists()
    assert Path(result["corpus_root"]) == data_root / "phase1_snapshots"
    assert json.loads(gate_path.read_text(encoding="utf-8")) == {"sentinel": True}
