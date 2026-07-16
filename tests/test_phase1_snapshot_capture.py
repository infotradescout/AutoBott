from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta, timezone
from pathlib import Path

from autobott_v2.phase1_snapshot_capture import (
    CaptureRules,
    _select_manual_mirror_candidates,
    _select_chain_subset,
    capture_snapshot_session,
    capture_symbol_snapshot,
    write_snapshot_day_manifest,
)


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
