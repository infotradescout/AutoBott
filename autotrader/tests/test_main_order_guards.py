"""Regression tests for main entry order guardrails."""

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
import main  # noqa: E402


EASTERN = pytz.timezone(config.EASTERN_TZ)


class FakeOrder:
    def __init__(self, symbol: str, side: str, status: str, submitted_at: datetime):
        self.symbol = symbol
        self.side = side
        self.status = status
        self.submitted_at = submitted_at


class FakeBroker:
    def __init__(self, orders):
        self._orders = list(orders)

    def get_recent_orders(self, limit: int = 500):
        return self._orders[:limit]


class MainOrderGuardTests(unittest.TestCase):
    def setUp(self):
        self._config_values = {
            "ALPACA_BUY_ORDER_CAP_COUNTS_CANCELED": config.ALPACA_BUY_ORDER_CAP_COUNTS_CANCELED,
        }
        config.ALPACA_BUY_ORDER_CAP_COUNTS_CANCELED = True
        self.now = EASTERN.localize(datetime(2026, 5, 11, 9, 55, 0))

    def tearDown(self):
        for key, value in self._config_values.items():
            setattr(config, key, value)

    def test_same_day_canceled_buy_counts_toward_ticker_cap(self):
        broker = FakeBroker(
            [
                FakeOrder(
                    "JPM260515P00290000",
                    "buy",
                    "canceled",
                    self.now - timedelta(minutes=5),
                )
            ]
        )

        counts = main._alpaca_option_buy_order_counts_by_ticker_today(broker, self.now)

        self.assertEqual(counts.get("JPM"), 1)

    def test_same_day_sell_does_not_count_as_buy_attempt(self):
        broker = FakeBroker(
            [
                FakeOrder(
                    "JPM260515P00290000",
                    "sell",
                    "filled",
                    self.now - timedelta(minutes=1),
                )
            ]
        )

        counts = main._alpaca_option_buy_order_counts_by_ticker_today(broker, self.now)

        self.assertNotIn("JPM", counts)

    def test_prior_day_buy_does_not_count_today(self):
        broker = FakeBroker(
            [
                FakeOrder(
                    "JPM260515P00290000",
                    "buy",
                    "filled",
                    self.now - timedelta(days=1),
                )
            ]
        )

        counts = main._alpaca_option_buy_order_counts_by_ticker_today(broker, self.now)

        self.assertNotIn("JPM", counts)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
