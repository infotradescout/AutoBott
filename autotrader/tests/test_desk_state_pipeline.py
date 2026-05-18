"""Regression tests for shared desk-state market context and candidate queue."""

from __future__ import annotations

import sys
import unittest
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytz

_PKG_DIR = Path(__file__).resolve().parent.parent
if str(_PKG_DIR) not in sys.path:
    sys.path.insert(0, str(_PKG_DIR))

import candidate_queue  # noqa: E402
import config  # noqa: E402
import market_context  # noqa: E402


EASTERN = pytz.timezone(config.EASTERN_TZ)


class FakeDataClient:
    def __init__(self, trends: dict[str, str]):
        self.trends = trends

    def get_stock_bars(self, symbol: str, timeframe: str, limit: int):
        trend = self.trends.get(symbol, "up")
        base = 100.0
        if trend == "down":
            closes = [base - (idx * 0.2) for idx in range(limit)]
        elif trend == "flat":
            closes = [base + ((idx % 2) * 0.01) for idx in range(limit)]
        else:
            closes = [base + (idx * 0.2) for idx in range(limit)]
        return pd.DataFrame(
            {
                "open": closes,
                "high": [value + 0.1 for value in closes],
                "low": [value - 0.1 for value in closes],
                "close": closes,
                "volume": [1000 for _ in closes],
            }
        )


class DeskStatePipelineTests(unittest.TestCase):
    def test_market_context_prefers_calls_on_broad_uptrend(self):
        now = EASTERN.localize(datetime(2026, 5, 18, 10, 0, 0))
        context = market_context.build_market_context(
            FakeDataClient({"SPY": "up", "QQQ": "up", "IWM": "up"}),
            now,
            vix_value=18.0,
        )

        self.assertEqual(context["regime"], "trend_up")
        self.assertEqual(context["preferred_direction"], "call")
        self.assertEqual(context["volatility"], "normal")

    def test_candidate_queue_ranks_regime_aligned_signal_first(self):
        now = EASTERN.localize(datetime(2026, 5, 18, 10, 0, 0))
        context = {
            "timestamp_et": now.isoformat(),
            "regime": "trend_up",
            "preferred_direction": "call",
            "allowed_profiles": [],
            "blocked_profiles": [],
        }
        queue = candidate_queue.build_candidate_queue(
            [
                {"symbol": "QQQ", "direction": "put", "signal_score": 9.0, "direction_score": 0.9, "rvol": 2.0},
                {"symbol": "SPY", "direction": "call", "signal_score": 8.0, "direction_score": 0.9, "rvol": 2.0},
            ],
            market_context=context,
            now_et=now,
        )

        self.assertEqual(queue["candidates"][0]["symbol"], "SPY")
        self.assertGreater(queue["candidates"][0]["edge_score"], queue["candidates"][1]["edge_score"])

    def test_candidate_queue_hard_blocks_regime_profile(self):
        now = EASTERN.localize(datetime(2026, 5, 18, 10, 0, 0))
        context = {
            "timestamp_et": now.isoformat(),
            "regime": "trend_up",
            "preferred_direction": "call",
            "allowed_profiles": ["vwap_continuation"],
            "blocked_profiles": ["reversal_snapback"],
            "source": "market_context_worker",
        }
        queue = candidate_queue.build_candidate_queue(
            [
                {"symbol": "QQQ", "direction": "call", "strategy_profile": "reversal_snapback", "signal_score": 9.5, "direction_score": 0.95, "rvol": 2.0},
                {"symbol": "SPY", "direction": "call", "strategy_profile": "vwap_continuation", "signal_score": 9.0, "direction_score": 0.9, "rvol": 2.0},
            ],
            market_context=context,
            now_et=now,
        )

        self.assertEqual(len(queue["candidates"]), 1)
        self.assertEqual(queue["candidates"][0]["symbol"], "SPY")


if __name__ == "__main__":
    unittest.main()
