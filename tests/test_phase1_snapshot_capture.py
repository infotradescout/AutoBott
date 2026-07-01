from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta, timezone

from autobott_v2.phase1_snapshot_capture import CaptureRules, capture_snapshot_session, capture_symbol_snapshot, write_snapshot_day_manifest


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


def _base_price(symbol: str) -> float:
    return {"SPY": 600.0, "QQQ": 520.0, "VIXY": 16.0}.get(symbol.upper(), 600.0)


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
