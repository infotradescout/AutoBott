from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from autobott_v2.core_runner import CoreRunnerRules, select_core_runner_pair
from autobott_v2.phase1_engine import build_decision_card
from autobott_v2.phase1_models import DecisionStatus, Phase1Rules
from autobott_v2.phase1_snapshot_capture import (
    AlpacaMarketDataClient,
    CaptureRules,
    _select_manual_mirror_candidates,
    _select_chain_subset,
    capture_snapshot_session,
    capture_symbol_snapshot,
    write_snapshot_day_manifest,
)
from autobott_v2.phase1_config import AlpacaReadOnlyConfig
from autobott_v2.phase1_validate import _decision_input_from_snapshot


class FakeCaptureClient:
    def get_stock_bars(self, symbols, *, start, end, timeframe="1Min", limit=35):
        return {
            symbol.upper(): [
                {
                    "t": (end - timedelta(minutes=34 - index)).isoformat(),
                    "o": base + index * 0.1,
                    "h": base + index * 0.1 + 0.2,
                    "l": base + index * 0.1 - 0.2,
                    "c": base + index * 0.1 + 0.05,
                    "v": 1000 + index * 10,
                }
                for index in range(35)
            ]
            for symbol, base in ((symbol, _base_price(symbol)) for symbol in symbols)
        }

    def get_latest_stock_quotes(self, symbols):
        return {
            symbol.upper(): {
                "t": datetime(2026, 6, 30, 13, 30, tzinfo=UTC).isoformat(),
                "bp": _base_price(symbol) - 0.1,
                "ap": _base_price(symbol) + 0.1,
            }
            for symbol in symbols
        }

    def get_option_chain_snapshots(self, symbol):
        upper = symbol.upper()
        return {
            f"{upper}260701C00600000": _option_snapshot("2026-07-01", "call", 600.0, 0.56, 4.9, 5.1),
            f"{upper}260701P00600000": _option_snapshot("2026-07-01", "put", 600.0, -0.56, 4.9, 5.1),
            f"{upper}260718C00600000": _option_snapshot("2026-07-18", "call", 600.0, 0.48, 5.4, 5.6),
            f"{upper}260718P00600000": _option_snapshot("2026-07-18", "put", 600.0, -0.48, 5.4, 5.6),
        }


class ShortFirstContextClient(FakeCaptureClient):
    def __init__(self) -> None:
        self.calls_by_symbol = {}

    def get_stock_bars(self, symbols, *, start, end, timeframe="1Min", limit=35):
        symbol = symbols[0].upper()
        self.calls_by_symbol[symbol] = self.calls_by_symbol.get(symbol, 0) + 1
        payload = super().get_stock_bars(symbols, start=start, end=end, timeframe=timeframe, limit=limit)
        if symbol == "UVXY" and self.calls_by_symbol[symbol] == 1:
            payload[symbol] = payload[symbol][:2]
        return payload


class VixCaptureClient:
    def __init__(self, *, include_greeks: bool = True) -> None:
        self.include_greeks = include_greeks
        self.bar_symbols: list[str] = []
        self.option_symbols: list[str] = []

    def get_stock_bars(self, symbols, *, start, end, timeframe="1Min", limit=35):
        symbol = symbols[0].upper()
        self.bar_symbols.append(symbol)
        base = {"VIXY": 16.0, "SPY": 600.0, "QQQ": 520.0}.get(symbol, 18.0)
        return {
            symbol: [
                {
                    "t": (end - timedelta(minutes=34 - index)).isoformat(),
                    "o": base + index * 0.03,
                    "h": base + index * 0.03 + 0.05,
                    "l": base + index * 0.03 - 0.05,
                    "c": base + index * 0.03,
                    "v": 2000 + index,
                }
                for index in range(35)
            ]
        }

    def get_latest_stock_quotes(self, symbols):
        return {
            symbol.upper(): {
                "t": "2026-06-30T14:35:00Z",
                "bp": _base_price(symbol) - 0.05,
                "ap": _base_price(symbol) + 0.05,
            }
            for symbol in symbols
        }

    def get_option_chain_snapshots(self, symbol):
        self.option_symbols.append(symbol.upper())
        primary = _option_snapshot("2026-07-07", "call", 24.0, 0.52, 1.90, 2.00)
        runner = _option_snapshot("2026-07-07", "call", 28.0, 0.18, 0.30, 0.36)
        for row in (primary, runner):
            row["underlying_price"] = 24.0
            if not self.include_greeks:
                row["greeks"] = {}
        return {
            "VIXW260707C00024000": primary,
            "VIXW260707C00028000": runner,
        }


class ParityOnlyVixCaptureClient(VixCaptureClient):
    """Alpaca-shaped VIXW snapshots without spot, IV, or provider Greeks."""

    def get_option_chain_snapshots(self, symbol):
        self.option_symbols.append(symbol.upper())
        return {
            "VIXW260707C00020000": _parity_option_snapshot("call", 20.0, 4.00, 4.20),
            "VIXW260707P00020000": _parity_option_snapshot("put", 20.0, 0.09, 0.11),
            "VIXW260707C00024000": _parity_option_snapshot("call", 24.0, 0.97, 1.03),
            "VIXW260707P00024000": _parity_option_snapshot("put", 24.0, 0.97, 1.03),
            "VIXW260707C00028000": _parity_option_snapshot("call", 28.0, 0.14, 0.16),
            "VIXW260707P00028000": _parity_option_snapshot("put", 28.0, 4.05, 4.25),
            # A stale/mismatched pair must not move the robust expiry forward
            # away from the three coherent estimates around 24.
            "VIXW260707C00040000": _parity_option_snapshot("call", 40.0, 0.09, 0.11),
            "VIXW260707P00040000": _parity_option_snapshot("put", 40.0, 4.90, 5.10),
        }


def _base_price(symbol: str) -> float:
    return {"SPY": 600.0, "QQQ": 520.0, "VIXY": 16.0, "UVXY": 18.0}.get(symbol.upper(), 600.0)


def _option_snapshot(expiration: str, option_type: str, strike: float, delta: float, bid: float, ask: float):
    return {
        "latestQuote": {
            "t": datetime(2026, 6, 30, 13, 30, tzinfo=UTC).isoformat(),
            "bp": bid,
            "ap": ask,
        },
        "latestTrade": {
            "t": datetime(2026, 6, 30, 13, 30, tzinfo=UTC).isoformat(),
            "p": round((bid + ask) / 2, 4),
        },
        "greeks": {
            "delta": delta,
            "theta": -0.04,
            "vega": 0.08,
            "iv": 0.24,
        },
        "details": {
            "expiration_date": expiration,
            "type": option_type,
            "strike_price": strike,
        },
        "dailyBar": {
            "v": 250,
        },
        "open_interest": 900,
    }


def _parity_option_snapshot(option_type: str, strike: float, bid: float, ask: float):
    return {
        "latestQuote": {
            "t": "2026-06-30T14:35:00Z",
            "bp": bid,
            "ap": ask,
        },
        "details": {
            "expiration_date": "2026-07-07",
            "type": option_type,
            "strike_price": str(strike),
        },
        "dailyBar": {"v": 250},
        "open_interest": 900,
    }


def _now_sequence(*timestamps: datetime):
    iterator = iter(timestamps)

    def _next():
        return next(iterator)

    return _next


def test_snapshot_capture_writes_day_manifest(tmp_path) -> None:
    result = capture_snapshot_session(
        symbols=["SPY"],
        corpus_root=tmp_path,
        interval_seconds=60,
        start_time="09:30",
        end_time="09:31",
        trading_date="2026-06-30",
        corpus_type="paper_capture",
        data_client=FakeCaptureClient(),
        sleep_fn=lambda _: None,
        now_fn=_now_sequence(
            datetime(2026, 6, 30, 13, 30, tzinfo=UTC),
            datetime(2026, 6, 30, 13, 30, tzinfo=UTC),
            datetime(2026, 6, 30, 13, 31, tzinfo=UTC),
        ),
    )

    manifest_path = tmp_path / "2026-06-30" / "SPY" / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert result["snapshots_written"] == 2
    assert manifest["schema_version"] == "phase1_snapshot_day_manifest.v1"
    assert manifest["snapshots_captured"] == 2


def test_snapshot_capture_records_corpus_type(tmp_path) -> None:
    capture_snapshot_session(
        symbols=["SPY"],
        corpus_root=tmp_path,
        interval_seconds=60,
        start_time="09:30",
        end_time="09:30",
        trading_date="2026-06-30",
        corpus_type="paper_capture",
        data_client=FakeCaptureClient(),
        sleep_fn=lambda _: None,
        now_fn=_now_sequence(
            datetime(2026, 6, 30, 13, 30, tzinfo=UTC),
            datetime(2026, 6, 30, 13, 30, tzinfo=UTC),
        ),
    )

    manifest = json.loads((tmp_path / "2026-06-30" / "SPY" / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["corpus_type"] == "paper_capture"


def test_snapshot_capture_records_market_and_utc_timestamps(tmp_path) -> None:
    capture_snapshot_session(
        symbols=["SPY"],
        corpus_root=tmp_path,
        interval_seconds=60,
        start_time="09:30",
        end_time="09:30",
        trading_date="2026-06-30",
        corpus_type="paper_capture",
        data_client=FakeCaptureClient(),
        sleep_fn=lambda _: None,
        now_fn=_now_sequence(
            datetime(2026, 6, 30, 13, 30, tzinfo=UTC),
            datetime(2026, 6, 30, 13, 30, tzinfo=UTC),
        ),
    )

    snapshot = json.loads((tmp_path / "2026-06-30" / "SPY" / "snapshots" / "093000.json").read_text(encoding="utf-8"))

    assert snapshot["market_timezone"] == "America/New_York"
    assert snapshot["timestamp_utc"] == "2026-06-30T13:30:00Z"
    assert snapshot["timestamp_market"] == "2026-06-30T09:30:00-04:00"


def test_snapshot_capture_detects_missing_intervals_after_finalize(tmp_path) -> None:
    client = FakeCaptureClient()
    market_tz = timezone(timedelta(hours=-4))
    capture_symbol_snapshot(
        symbol="SPY",
        corpus_root=tmp_path,
        scheduled_market_time=datetime(2026, 6, 30, 9, 30, tzinfo=market_tz),
        captured_at_utc=datetime(2026, 6, 30, 13, 30, tzinfo=UTC),
        corpus_type="paper_capture",
        market_timezone="America/New_York",
        volatility_proxy_symbol="VIXY",
        data_client=client,
        rules=CaptureRules(),
    )
    capture_symbol_snapshot(
        symbol="SPY",
        corpus_root=tmp_path,
        scheduled_market_time=datetime(2026, 6, 30, 9, 32, tzinfo=market_tz),
        captured_at_utc=datetime(2026, 6, 30, 13, 32, tzinfo=UTC),
        corpus_type="paper_capture",
        market_timezone="America/New_York",
        volatility_proxy_symbol="VIXY",
        data_client=client,
        rules=CaptureRules(),
    )

    manifest = write_snapshot_day_manifest(tmp_path / "2026-06-30" / "SPY", trading_date="2026-06-30", symbol="SPY", capture_interval_seconds=60)

    assert manifest["missing_intervals"] == ["09:31:00"]


def test_snapshot_capture_retries_short_context_bar_response(tmp_path) -> None:
    client = ShortFirstContextClient()
    snapshot_path = capture_symbol_snapshot(
        symbol="SPY",
        corpus_root=tmp_path,
        scheduled_market_time=datetime(2026, 6, 30, 10, 35, tzinfo=timezone(timedelta(hours=-4))),
        captured_at_utc=datetime(2026, 6, 30, 14, 35, tzinfo=UTC),
        corpus_type="paper_capture",
        market_timezone="America/New_York",
        volatility_proxy_symbol="UVXY",
        data_client=client,
        rules=CaptureRules(),
    )

    snapshot = json.loads(Path(snapshot_path).read_text(encoding="utf-8"))

    assert client.calls_by_symbol["UVXY"] == 2
    assert len(snapshot["context"]["vix_bars"]) == 35


def test_vix_capture_uses_vixy_signal_and_real_vix_contracts(tmp_path) -> None:
    client = VixCaptureClient()
    snapshot_path = capture_symbol_snapshot(
        symbol="VIX",
        corpus_root=tmp_path,
        scheduled_market_time=datetime(2026, 6, 30, 10, 35, tzinfo=timezone(timedelta(hours=-4))),
        captured_at_utc=datetime(2026, 6, 30, 14, 35, tzinfo=UTC),
        corpus_type="paper_capture",
        market_timezone="America/New_York",
        volatility_proxy_symbol="VIXY",
        data_client=client,
        rules=CaptureRules(),
    )

    snapshot = json.loads(Path(snapshot_path).read_text(encoding="utf-8"))

    assert "VIXY" in client.bar_symbols
    assert "VIX" not in client.bar_symbols
    assert client.option_symbols == ["VIX"]
    assert snapshot["ticker"] == "VIX"
    assert snapshot["underlying_quote"]["symbol"] == "VIX"
    assert snapshot["underlying_quote"]["last"] == 24.0
    assert {row["option_symbol"] for row in snapshot["option_chain"]} == {
        "VIXW260707C00024000",
        "VIXW260707C00028000",
    }
    assert {row["underlying"] for row in snapshot["option_chain"]} == {"VIX"}


def test_api_shaped_vix_chain_preserves_returned_symbols_through_decision_and_pair(tmp_path) -> None:
    snapshot_path = capture_symbol_snapshot(
        symbol="VIX",
        corpus_root=tmp_path,
        scheduled_market_time=datetime(2026, 6, 30, 10, 35, tzinfo=timezone(timedelta(hours=-4))),
        captured_at_utc=datetime(2026, 6, 30, 14, 35, tzinfo=UTC),
        corpus_type="paper_capture",
        market_timezone="America/New_York",
        volatility_proxy_symbol="VIXY",
        data_client=VixCaptureClient(),
        rules=CaptureRules(),
    )
    snapshot = json.loads(Path(snapshot_path).read_text(encoding="utf-8"))
    decision_input = _decision_input_from_snapshot(snapshot)

    decision = build_decision_card(
        decision_input,
        Phase1Rules(risk_off_bullish_exempt_symbols=("VIX",)),
    )
    assert decision.decision is DecisionStatus.TRADE_CANDIDATE
    assert decision.selected_contract is not None

    pair = select_core_runner_pair(
        decision.selected_contract,
        decision_input.option_chain,
        rules=CoreRunnerRules(),
    )

    assert decision.selected_contract.option_symbol == "VIXW260707C00024000"
    assert pair is not None
    assert pair.primary.option_symbol == "VIXW260707C00024000"
    assert pair.runner.option_symbol == "VIXW260707C00028000"


def test_api_shaped_vix_chain_derives_forward_and_greeks_from_put_call_parity(tmp_path) -> None:
    client = ParityOnlyVixCaptureClient()
    snapshot_path = capture_symbol_snapshot(
        symbol="VIX",
        corpus_root=tmp_path,
        scheduled_market_time=datetime(2026, 6, 30, 10, 35, tzinfo=timezone(timedelta(hours=-4))),
        captured_at_utc=datetime(2026, 6, 30, 14, 35, tzinfo=UTC),
        corpus_type="paper_capture",
        market_timezone="America/New_York",
        volatility_proxy_symbol="VIXY",
        data_client=client,
        rules=CaptureRules(),
    )
    snapshot = json.loads(Path(snapshot_path).read_text(encoding="utf-8"))

    assert client.option_symbols == ["VIX"]
    assert snapshot["underlying_quote"]["last"] == pytest.approx(24.0, abs=0.01)
    assert snapshot["underlying_quote"]["last"] != _base_price("VIXY")
    chain = {row["option_symbol"]: row for row in snapshot["option_chain"]}
    assert {
        "VIXW260707C00024000",
        "VIXW260707C00028000",
        "VIXW260707P00020000",
        "VIXW260707P00024000",
    }.issubset(chain)
    assert chain["VIXW260707C00024000"]["delta"] == pytest.approx(0.52, abs=0.03)
    assert chain["VIXW260707P00024000"]["delta"] == pytest.approx(-0.48, abs=0.03)
    assert abs(chain["VIXW260707C00028000"]["delta"]) < abs(chain["VIXW260707C00024000"]["delta"])
    assert abs(chain["VIXW260707P00020000"]["delta"]) < abs(chain["VIXW260707P00024000"]["delta"])
    assert all(row["implied_volatility"] > 0 for row in chain.values())

    decision_input = _decision_input_from_snapshot(snapshot)
    decision = build_decision_card(
        decision_input,
        Phase1Rules(risk_off_bullish_exempt_symbols=("VIX",)),
    )
    assert decision.decision is DecisionStatus.TRADE_CANDIDATE
    assert decision.selected_contract is not None
    pair = select_core_runner_pair(
        decision.selected_contract,
        decision_input.option_chain,
        rules=CoreRunnerRules(),
    )
    assert pair is not None
    assert pair.primary.option_symbol == "VIXW260707C00024000"
    assert pair.runner.option_symbol == "VIXW260707C00028000"


def test_vixw_market_data_request_uses_vix_underlying_and_vixw_root(monkeypatch) -> None:
    client = AlpacaMarketDataClient(
        AlpacaReadOnlyConfig(
            api_key="paper-key",
            secret_key="paper-secret",
            base_url="https://paper-api.alpaca.markets",
            data_url="https://data.alpaca.markets",
            paper=True,
        )
    )
    calls: list[tuple[str, dict[str, str], str | None]] = []
    option_symbol = "VIXW260814C00024000"

    def fake_get_json_with_retry(path, params, *, base_url=None):
        calls.append((path, dict(params), base_url))
        if path == "/v2/options/contracts":
            return {
                "option_contracts": [
                    {
                        "symbol": option_symbol,
                        "underlying_symbol": "VIX",
                        "root_symbol": "VIXW",
                        "style": "european",
                        "expiration_date": "2026-08-14",
                        "strike_price": "24",
                        "type": "call",
                        "tradable": True,
                    }
                ]
            }
        return {
            "snapshots": {
                option_symbol: {
                    "latestQuote": {"bp": 1.9, "ap": 2.0},
                    "greeks": {"delta": 0.52, "theta": -0.03, "vega": 0.08, "iv": 0.7},
                }
            }
        }

    monkeypatch.setattr(client, "_get_json_with_retry", fake_get_json_with_retry)

    chain = client.get_option_chain_snapshots("VIXW")

    assert set(chain) == {option_symbol}
    contract_call = next(call for call in calls if call[0] == "/v2/options/contracts")
    snapshot_call = next(call for call in calls if call[0].startswith("/v1beta1/options/snapshots/"))
    assert contract_call[1]["underlying_symbols"] == "VIX"
    assert contract_call[1]["root_symbol"] == "VIXW"
    assert snapshot_call[0] == "/v1beta1/options/snapshots/VIX"
    assert snapshot_call[1]["root_symbol"] == "VIXW"


def test_vix_capture_requires_provider_greeks(tmp_path) -> None:
    with pytest.raises(ValueError, match="empty_option_chain"):
        capture_symbol_snapshot(
            symbol="VIX",
            corpus_root=tmp_path,
            scheduled_market_time=datetime(2026, 6, 30, 10, 35, tzinfo=timezone(timedelta(hours=-4))),
            captured_at_utc=datetime(2026, 6, 30, 14, 35, tzinfo=UTC),
            corpus_type="paper_capture",
            market_timezone="America/New_York",
            volatility_proxy_symbol="VIXY",
            data_client=VixCaptureClient(include_greeks=False),
            rules=CaptureRules(),
        )


def test_chain_subset_preserves_farther_otm_runner_from_dense_chain() -> None:
    deltas = [0.58, 0.56, 0.54, 0.52, 0.50]
    contracts = [
        {
            "option_symbol": f"VXX260701C{int(strike * 1000):08d}",
            "expiration": "2026-07-01",
            "option_type": "call",
            "strike": strike,
            "delta": delta,
            "bid": 4.8 - index * 0.3,
            "ask": 5.0 - index * 0.3,
            "spread_pct": 0.04,
            "open_interest": 1000,
            "volume": 500,
        }
        for index, (strike, delta) in enumerate(zip([45, 46, 47, 48, 49], deltas))
    ]
    runner_symbol = "VXX260701C00052000"
    contracts.append(
        {
            "option_symbol": runner_symbol,
            "expiration": "2026-07-01",
            "option_type": "call",
            "strike": 52.0,
            "delta": 0.14,
            "bid": 0.20,
            "ask": 0.24,
            "spread_pct": 0.1818,
            "open_interest": 700,
            "volume": 220,
        }
    )

    selected = _select_chain_subset(
        contracts,
        underlying_price=47.0,
        as_of_date=date(2026, 6, 30),
        rules=CaptureRules(),
    )

    assert runner_symbol in {contract["option_symbol"] for contract in selected}
    assert len([contract for contract in selected if contract["option_type"] == "call"]) <= 8


def test_chain_subset_prunes_wide_spread_runner_in_favor_of_executable_runner() -> None:
    contracts = [
        {
            "option_symbol": f"C{index}",
            "expiration": "2026-07-01",
            "option_type": "call",
            "strike": 45.0 + index,
            "delta": 0.58 - index * 0.02,
            "bid": 4.8 - index * 0.3,
            "ask": 5.0 - index * 0.3,
            "spread_pct": 0.05,
            "open_interest": 1000,
            "volume": 500,
        }
        for index in range(4)
    ]
    contracts.extend(
        [
            {
                "option_symbol": "WIDE",
                "expiration": "2026-07-01",
                "option_type": "call",
                "strike": 52.0,
                "delta": 0.15,
                "bid": 0.50,
                "ask": 1.00,
                "spread_pct": 0.6667,
                "open_interest": 900,
                "volume": 300,
            },
            {
                "option_symbol": "VALID",
                "expiration": "2026-07-01",
                "option_type": "call",
                "strike": 53.0,
                "delta": 0.10,
                "bid": 0.90,
                "ask": 1.00,
                "spread_pct": 0.1053,
                "open_interest": 800,
                "volume": 250,
            },
        ]
    )

    selected = _select_chain_subset(
        contracts,
        underlying_price=47.0,
        as_of_date=date(2026, 6, 30),
        rules=CaptureRules(),
    )
    symbols = {contract["option_symbol"] for contract in selected}

    assert "VALID" in symbols
    assert "WIDE" not in symbols


def test_manual_mirror_candidates_preserve_affordable_contract_pruned_from_execution_subset() -> None:
    contracts = [
        {
            "option_symbol": f"CORE{index}",
            "expiration": "2026-07-01",
            "option_type": "call",
            "strike": 45.0 + index,
            "delta": 0.58 - index * 0.04,
            "bid": 4.8 - index * 0.3,
            "ask": 5.0 - index * 0.3,
            "spread_pct": 0.05,
            "open_interest": 1000,
            "volume": 500,
        }
        for index in range(4)
    ]
    contracts.extend(
        [
            {
                "option_symbol": "PAIR_RUNNER",
                "expiration": "2026-07-01",
                "option_type": "call",
                "strike": 52.0,
                "delta": 0.30,
                "bid": 1.40,
                "ask": 1.50,
                "spread_pct": 0.069,
                "open_interest": 900,
                "volume": 300,
            },
            {
                "option_symbol": "MANUAL_UNDER_100",
                "expiration": "2026-07-01",
                "option_type": "call",
                "strike": 53.0,
                "delta": 0.10,
                "bid": 0.74,
                "ask": 0.80,
                "spread_pct": 0.0779,
                "open_interest": 800,
                "volume": 250,
            },
        ]
    )

    execution_subset = _select_chain_subset(
        contracts,
        underlying_price=47.0,
        as_of_date=date(2026, 6, 30),
        rules=CaptureRules(),
    )
    manual_subset = _select_manual_mirror_candidates(contracts, max_contract_cost=100.0)

    assert "MANUAL_UNDER_100" not in {contract["option_symbol"] for contract in execution_subset}
    assert {contract["option_symbol"] for contract in manual_subset} == {"MANUAL_UNDER_100"}
