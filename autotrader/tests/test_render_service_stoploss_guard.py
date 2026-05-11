"""Regression tests for render_service independent stop-loss guard helpers."""

from __future__ import annotations

import sys
import unittest
from datetime import datetime, timedelta
from pathlib import Path

import pytz

_PKG_DIR = Path(__file__).resolve().parent.parent
if str(_PKG_DIR) not in sys.path:
    sys.path.insert(0, str(_PKG_DIR))

import config  # noqa: E402
import render_service  # noqa: E402


EASTERN = pytz.timezone(config.EASTERN_TZ)


class FakeOrder:
    def __init__(self, symbol: str, side: str, status: str, filled_at: datetime):
        self.symbol = symbol
        self.side = side
        self.status = status
        self.filled_at = filled_at
        self.submitted_at = filled_at


class FakeBroker:
    def __init__(self, orders):
        self._orders = list(orders)

    def get_recent_orders(self, limit: int = 100):
        return self._orders[:limit]


class RenderServiceStoplossGuardTests(unittest.TestCase):
    def setUp(self):
        self._config_values = {
            "ANTI_CHURN_HOLD_MINUTES": config.ANTI_CHURN_HOLD_MINUTES,
        }
        config.ANTI_CHURN_HOLD_MINUTES = 10
        self.now = EASTERN.localize(datetime(2026, 5, 11, 13, 16, 7))

    def tearDown(self):
        for key, value in self._config_values.items():
            setattr(config, key, value)

    def test_recent_filled_buy_creates_minimum_hold_remaining(self):
        symbol = "ORCL260515C00195000"
        broker = FakeBroker(
            [
                FakeOrder(
                    symbol,
                    "buy",
                    "filled",
                    self.now - timedelta(minutes=4, seconds=30),
                )
            ]
        )

        remaining = render_service._minimum_hold_remaining_seconds(
            runtime_state={},
            broker=broker,
            symbol=symbol,
            now_et=self.now,
        )

        self.assertGreaterEqual(remaining, 320)
        self.assertLessEqual(remaining, 340)

    def test_old_filled_buy_has_no_minimum_hold_remaining(self):
        symbol = "ORCL260515C00195000"
        broker = FakeBroker(
            [
                FakeOrder(
                    symbol,
                    "buy",
                    "filled",
                    self.now - timedelta(minutes=12),
                )
            ]
        )

        remaining = render_service._minimum_hold_remaining_seconds(
            runtime_state={},
            broker=broker,
            symbol=symbol,
            now_et=self.now,
        )

        self.assertEqual(remaining, 0)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
