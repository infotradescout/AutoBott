"""Regression tests for VIX proxy sidecar order guards."""

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
import vixw_regime  # noqa: E402


EASTERN = pytz.timezone(config.EASTERN_TZ)


class FakePosition:
    def __init__(self, symbol: str, qty: int):
        self.symbol = symbol
        self.qty = qty


class FakeOrder:
    def __init__(self, symbol: str, side: str, status: str, submitted_at: datetime):
        self.symbol = symbol
        self.side = side
        self.status = status
        self.submitted_at = submitted_at


class FakeBroker:
    def __init__(self, positions=None, orders=None):
        self._positions = list(positions or [])
        self._orders = list(orders or [])

    def get_open_option_positions(self):
        return self._positions

    def get_recent_orders(self, limit: int = 500):
        return self._orders[:limit]


class VixwRegimeGuardTests(unittest.TestCase):
    def setUp(self):
        self._config_values = {
            "VIXW_REQUIRE_TRADING_CONTROL_CLEAR": config.VIXW_REQUIRE_TRADING_CONTROL_CLEAR,
            "VIXW_INCLUDE_OPEN_ORDERS_IN_EXPOSURE": config.VIXW_INCLUDE_OPEN_ORDERS_IN_EXPOSURE,
            "VIXW_MAX_BUY_ORDERS_PER_DAY": config.VIXW_MAX_BUY_ORDERS_PER_DAY,
            "VIXW_COUNT_CANCELED_ORDERS_IN_DAILY_CAP": config.VIXW_COUNT_CANCELED_ORDERS_IN_DAILY_CAP,
        }
        config.VIXW_REQUIRE_TRADING_CONTROL_CLEAR = False
        config.VIXW_INCLUDE_OPEN_ORDERS_IN_EXPOSURE = True
        config.VIXW_MAX_BUY_ORDERS_PER_DAY = 1
        config.VIXW_COUNT_CANCELED_ORDERS_IN_DAILY_CAP = True
        self.now = EASTERN.localize(datetime(2026, 5, 6, 13, 42, 0))

    def tearDown(self):
        for key, value in self._config_values.items():
            setattr(config, key, value)

    def test_open_proxy_buy_order_blocks_new_entry(self):
        broker = FakeBroker(
            orders=[
                FakeOrder(
                    "VIXY260515C00025000",
                    "buy",
                    "new",
                    self.now - timedelta(hours=3),
                )
            ]
        )

        reason = vixw_regime._proxy_entry_block_reason(broker, self.now)

        self.assertIn("existing proxy buy order open", reason)
        self.assertIn("VIXY260515C00025000:new", reason)

    def test_same_day_filled_proxy_buy_hits_daily_cap(self):
        broker = FakeBroker(
            orders=[
                FakeOrder(
                    "VIXY260515C00025000",
                    "buy",
                    "filled",
                    self.now - timedelta(hours=4),
                )
            ]
        )

        reason = vixw_regime._proxy_entry_block_reason(broker, self.now)

        self.assertIn("daily proxy buy order cap reached", reason)

    def test_canceled_proxy_buy_counts_toward_daily_cap_by_default(self):
        broker = FakeBroker(
            orders=[
                FakeOrder(
                    "VIXY260515C00025000",
                    "buy",
                    "canceled",
                    self.now - timedelta(hours=1),
                )
            ]
        )

        reason = vixw_regime._proxy_entry_block_reason(broker, self.now)

        self.assertIn("daily proxy buy order cap reached", reason)

    def test_non_proxy_order_does_not_block(self):
        broker = FakeBroker(
            orders=[
                FakeOrder(
                    "AAPL260515C00200000",
                    "buy",
                    "filled",
                    self.now - timedelta(hours=1),
                )
            ]
        )

        reason = vixw_regime._proxy_entry_block_reason(broker, self.now)

        self.assertIsNone(reason)

    def test_trading_control_does_not_block_sidecar_by_default(self):
        old_loader = vixw_regime.load_trading_control
        config.VIXW_REQUIRE_TRADING_CONTROL_CLEAR = False
        vixw_regime.load_trading_control = lambda: {"manual_stop": True, "dry_run": False}
        try:
            reason = vixw_regime._proxy_entry_block_reason(FakeBroker(), self.now)
        finally:
            vixw_regime.load_trading_control = old_loader

        self.assertIsNone(reason)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
