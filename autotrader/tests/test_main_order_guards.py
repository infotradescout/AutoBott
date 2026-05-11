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
            "ALPACA_CANCELED_BUY_ORDER_COOLDOWN_MINUTES": config.ALPACA_CANCELED_BUY_ORDER_COOLDOWN_MINUTES,
        }
        config.ALPACA_BUY_ORDER_CAP_COUNTS_CANCELED = False
        config.ALPACA_CANCELED_BUY_ORDER_COOLDOWN_MINUTES = 10
        self.now = EASTERN.localize(datetime(2026, 5, 11, 9, 55, 0))

    def tearDown(self):
        for key, value in self._config_values.items():
            setattr(config, key, value)

    def test_recent_same_day_canceled_buy_counts_during_cooldown(self):
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

    def test_old_same_day_canceled_buy_does_not_block_all_day(self):
        broker = FakeBroker(
            [
                FakeOrder(
                    "JPM260515P00290000",
                    "buy",
                    "canceled",
                    self.now - timedelta(minutes=30),
                )
            ]
        )

        counts = main._alpaca_option_buy_order_counts_by_ticker_today(broker, self.now)

        self.assertNotIn("JPM", counts)

    def test_same_day_filled_buy_counts_toward_ticker_cap(self):
        broker = FakeBroker(
            [
                FakeOrder(
                    "JPM260515P00290000",
                    "buy",
                    "filled",
                    self.now - timedelta(hours=2),
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
