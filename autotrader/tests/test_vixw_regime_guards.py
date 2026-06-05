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
    def __init__(self, symbol: str, qty: int, avg_entry_price: float = 0.0, opened_at=None):
        self.symbol = symbol
        self.qty = qty
        self.avg_entry_price = avg_entry_price
        self.opened_at = opened_at


class FakeOrder:
    def __init__(self, symbol: str, side: str, status: str, submitted_at: datetime, order_id: str = ""):
        self.symbol = symbol
        self.side = side
        self.status = status
        self.submitted_at = submitted_at
        self.id = order_id


class FakeBroker:
    def __init__(self, positions=None, orders=None):
        self._positions = list(positions or [])
        self._orders = list(orders or [])
        self.limit_sells = []
        self.canceled_orders = []
        self.market_closes = []

    def get_open_option_positions(self):
        return self._positions

    def get_recent_orders(self, limit: int = 500):
        return self._orders[:limit]

    def place_option_limit_sell(self, option_symbol: str, qty: int, limit_price: float):
        self.limit_sells.append((option_symbol, qty, limit_price))
        return FakeOrder(option_symbol, "sell", "new", EASTERN.localize(datetime(2026, 5, 6, 13, 43, 0)), "sell-1")

    def get_open_orders_for_symbol(self, symbol: str, side: str | None = None):
        side_lc = str(side or "").lower()
        return [
            order for order in self._orders
            if order.symbol == symbol and (not side_lc or order.side == side_lc) and order.status == "new"
        ]

    def cancel_order(self, order_id: str):
        self.canceled_orders.append(order_id)

    def close_option_market(self, option_symbol: str, qty: int):
        self.market_closes.append((option_symbol, qty))
        return FakeOrder(option_symbol, "sell", "new", EASTERN.localize(datetime(2026, 5, 6, 13, 44, 0)), "close-1")


class FakeDataClient:
    def __init__(self, quotes=None):
        self.quotes = dict(quotes or {})

    def get_latest_option_quote(self, symbol: str):
        return self.quotes.get(symbol, {"bid": 0.0, "ask": 0.0})


class VixwRegimeGuardTests(unittest.TestCase):
    def setUp(self):
        self._config_values = {
            "VIXW_REQUIRE_TRADING_CONTROL_CLEAR": config.VIXW_REQUIRE_TRADING_CONTROL_CLEAR,
            "VIXW_INCLUDE_OPEN_ORDERS_IN_EXPOSURE": config.VIXW_INCLUDE_OPEN_ORDERS_IN_EXPOSURE,
            "VIXW_MAX_BUY_ORDERS_PER_DAY": config.VIXW_MAX_BUY_ORDERS_PER_DAY,
            "VIXW_COUNT_CANCELED_ORDERS_IN_DAILY_CAP": config.VIXW_COUNT_CANCELED_ORDERS_IN_DAILY_CAP,
            "VIXW_PLACE_PROFIT_TRAP_AFTER_FILL": config.VIXW_PLACE_PROFIT_TRAP_AFTER_FILL,
            "VIXW_PROFIT_TARGET_MULTIPLIER": config.VIXW_PROFIT_TARGET_MULTIPLIER,
            "VIXW_MIN_PROFIT_TARGET_INCREMENT": config.VIXW_MIN_PROFIT_TARGET_INCREMENT,
            "VIXW_MANAGE_OPEN_POSITIONS": config.VIXW_MANAGE_OPEN_POSITIONS,
            "VIXW_STOP_LOSS_PCT": config.VIXW_STOP_LOSS_PCT,
            "VIXW_MIN_DTE_TRADING_DAYS": config.VIXW_MIN_DTE_TRADING_DAYS,
            "VIXW_MAX_DTE_TRADING_DAYS": config.VIXW_MAX_DTE_TRADING_DAYS,
            "VIXW_MAX_OPTION_SPREAD_PCT": config.VIXW_MAX_OPTION_SPREAD_PCT,
            "VIXW_ENTRY_BLOCK_AFTER_OPEN_MINUTES": config.VIXW_ENTRY_BLOCK_AFTER_OPEN_MINUTES,
            "VIXW_NO_NEW_ENTRIES_AFTER": config.VIXW_NO_NEW_ENTRIES_AFTER,
            "VIXW_MAX_QUOTE_AGE_SECONDS": config.VIXW_MAX_QUOTE_AGE_SECONDS,
            "VIXW_MAX_HOLD_MINUTES": config.VIXW_MAX_HOLD_MINUTES,
            "VIXW_TIME_STOP_MIN_PROGRESS_PCT": config.VIXW_TIME_STOP_MIN_PROGRESS_PCT,
        }
        config.VIXW_REQUIRE_TRADING_CONTROL_CLEAR = False
        config.VIXW_INCLUDE_OPEN_ORDERS_IN_EXPOSURE = True
        config.VIXW_MAX_BUY_ORDERS_PER_DAY = 1
        config.VIXW_COUNT_CANCELED_ORDERS_IN_DAILY_CAP = True
        config.VIXW_PLACE_PROFIT_TRAP_AFTER_FILL = True
        config.VIXW_PROFIT_TARGET_MULTIPLIER = 1.25
        config.VIXW_MIN_PROFIT_TARGET_INCREMENT = 0.01
        config.VIXW_MANAGE_OPEN_POSITIONS = True
        config.VIXW_STOP_LOSS_PCT = 0.15
        config.VIXW_MIN_DTE_TRADING_DAYS = 3
        config.VIXW_MAX_DTE_TRADING_DAYS = 7
        config.VIXW_MAX_OPTION_SPREAD_PCT = 3.0
        config.VIXW_ENTRY_BLOCK_AFTER_OPEN_MINUTES = 30
        config.VIXW_NO_NEW_ENTRIES_AFTER = "15:45"
        config.VIXW_MAX_QUOTE_AGE_SECONDS = 60
        config.VIXW_MAX_HOLD_MINUTES = 15
        config.VIXW_TIME_STOP_MIN_PROGRESS_PCT = 0.05
        self.now = EASTERN.localize(datetime(2026, 5, 6, 13, 42, 0))

    def _vix_bars(self, closes=None, *, last_high=19.45, last_close=19.10):
        closes = list(closes or [
            18.80, 18.82, 18.84, 18.86, 18.88,
            18.90, 18.94, 18.98, 19.02, 19.06,
            19.12, 19.18, 19.25, 19.35, 19.28,
            last_close,
        ])
        bars = []
        for idx, close in enumerate(closes):
            if idx == len(closes) - 2:
                bars.append({"open": close - 0.07, "high": close + 0.08, "low": close - 0.14, "close": close})
            elif idx == len(closes) - 1:
                bars.append({"open": close + 0.08, "high": max(last_high, close + 0.12), "low": close - 0.10, "close": close})
            else:
                bars.append({"open": close - 0.02, "high": close + 0.04, "low": close - 0.04, "close": close})
        return bars

    def _vxx_bars(self):
        return [{"close": value} for value in [14.0, 14.02, 14.03, 14.05, 14.07, 14.10]]

    def _contract(self, days: int = 5):
        expiration = (self.now.date() + timedelta(days=days)).isoformat()
        return {
            "symbol": "VIXY260511C00025000",
            "expiration_date": expiration,
            "volume": 10,
            "open_interest": 50,
        }

    def _quote(self, bid: float = 0.29, ask: float = 0.298, mark=None, age_seconds: int = 10):
        quote = {
            "bid": bid,
            "ask": ask,
            "updated_at": (self.now - timedelta(seconds=age_seconds)).isoformat(),
        }
        if mark is not None:
            quote["mark"] = mark
        return quote

    def _entry_decision(self, **overrides):
        payload = {
            "ticker": "VIXY",
            "underlying_price": 14.10,
            "vix_bars": self._vix_bars(),
            "vxx_bars": self._vxx_bars(),
            "option_contract": self._contract(),
            "option_quote": self._quote(),
            "now_et": self.now,
            "macro_blocked": False,
        }
        payload.update(overrides)
        return vixw_regime._build_vixw_entry_telemetry(**payload)

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

    def test_profit_target_uses_fill_cost_basis(self):
        self.assertEqual(vixw_regime._profit_target_price(0.27), 0.34)
        self.assertEqual(vixw_regime._profit_target_price(0.20), 0.25)

    def test_profit_trap_places_limit_sell_after_fill(self):
        broker = FakeBroker()

        result = vixw_regime._submit_proxy_profit_trap(
            broker,
            symbol="VIXY260515C00025000",
            qty=1,
            fill_price=0.27,
        )

        self.assertTrue(result["submitted"])
        self.assertEqual(result["target_price"], 0.34)
        self.assertEqual(result["order_id"], "sell-1")
        self.assertEqual(broker.limit_sells, [("VIXY260515C00025000", 1, 0.34)])

    def test_stop_loss_cancels_profit_trap_then_closes_position(self):
        symbol = "VIXY260515C00025000"
        broker = FakeBroker(
            positions=[FakePosition(symbol, 1, avg_entry_price=0.27)],
            orders=[FakeOrder(symbol, "sell", "new", self.now, "profit-1")],
        )
        data_client = FakeDataClient({symbol: {"bid": 0.21, "ask": 0.23, "mark": 0.22}})

        actions = vixw_regime._manage_proxy_stop_losses(broker, data_client, self.now)

        self.assertEqual(len(actions), 1)
        self.assertEqual(actions[0]["decision"], "stop_loss_exit_submitted")
        self.assertIn("MARK_STOP_LOSS", actions[0]["reason"])
        self.assertEqual(broker.canceled_orders, ["profit-1"])
        self.assertEqual(broker.market_closes, [(symbol, 1)])

    def test_stop_loss_holds_when_bid_above_stop(self):
        symbol = "VIXY260515C00025000"
        broker = FakeBroker(positions=[FakePosition(symbol, 1, avg_entry_price=0.27)])
        data_client = FakeDataClient({symbol: {"bid": 0.24, "ask": 0.25}})

        actions = vixw_regime._manage_proxy_stop_losses(broker, data_client, self.now)

        self.assertEqual(actions, [])
        self.assertEqual(broker.market_closes, [])

    def test_absolute_high_vix_alone_blocks_entry(self):
        decision = self._entry_decision(vix_bars=self._vix_bars([27.8] * 15 + [28.5]))

        self.assertFalse(decision["entry_allowed"])
        self.assertEqual(decision["vix_level_regime"], "stressed")
        self.assertEqual(decision["skip_reason"], "BLOCKED_STRESSED_VIX")

    def test_vix_spike_with_accelerating_momentum_blocks_entry(self):
        decision = self._entry_decision(vix_bars=self._vix_bars([18.0] * 12 + [18.1, 18.2, 18.5, 19.2]))

        self.assertFalse(decision["entry_allowed"])
        self.assertEqual(decision["vix_momentum_state"], "accelerating")
        self.assertEqual(decision["skip_reason"], "BLOCKED_ACCELERATING_VIX_SPIKE")

    def test_vix_deceleration_with_wick_failure_allows_entry(self):
        decision = self._entry_decision()

        self.assertTrue(decision["entry_allowed"])
        self.assertEqual(decision["entry_reason"], "VIX_DECELERATION_WICK_FAILURE")
        self.assertTrue(decision["failed_breakout"])
        self.assertTrue(decision["close_back_inside_range"])

    def test_wide_option_spread_blocks_entry(self):
        decision = self._entry_decision(option_quote=self._quote(0.20, 0.35))

        self.assertFalse(decision["entry_allowed"])
        self.assertEqual(decision["skip_reason"], "BLOCKED_OPTION_SPREAD_WIDE")

    def test_zero_and_one_dte_contracts_block_entry(self):
        zero_dte = self._entry_decision(option_contract=self._contract(days=0))
        one_dte = self._entry_decision(option_contract=self._contract(days=1))

        self.assertEqual(zero_dte["skip_reason"], "BLOCKED_DTE_OUT_OF_RANGE")
        self.assertEqual(one_dte["skip_reason"], "BLOCKED_DTE_OUT_OF_RANGE")

    def test_first_30_minutes_after_open_blocks_entry(self):
        now = EASTERN.localize(datetime(2026, 5, 6, 9, 45, 0))
        decision = self._entry_decision(now_et=now)

        self.assertFalse(decision["entry_allowed"])
        self.assertEqual(decision["skip_reason"], "BLOCKED_OPENING_WINDOW")

    def test_new_entries_after_345_pm_block(self):
        now = EASTERN.localize(datetime(2026, 5, 6, 15, 45, 0))
        decision = self._entry_decision(now_et=now)

        self.assertFalse(decision["entry_allowed"])
        self.assertEqual(decision["skip_reason"], "BLOCKED_LATE_SESSION")

    def test_missing_mark_blocks_unless_safe_fallback_mark_is_valid(self):
        bad = self._entry_decision(option_quote={"bid": 0.0, "ask": 0.30})
        fallback = self._entry_decision(option_quote=self._quote(0.29, 0.298))

        self.assertEqual(bad["skip_reason"], "BLOCKED_OPTION_QUOTE_INVALID")
        self.assertTrue(fallback["entry_allowed"])
        self.assertEqual(fallback["option_mark_source"], "midpoint_fallback")

    def test_mark_based_stop_decision_triggers_at_15_percent(self):
        decision = vixw_regime._position_exit_decision(
            entry_price=0.27,
            mark=0.22,
            bid=0.21,
            held_minutes=2,
        )

        self.assertEqual(decision["action"], "close")
        self.assertEqual(decision["reason"], "MARK_STOP_LOSS")

    def test_profit_trap_decision_triggers_at_25_percent(self):
        decision = vixw_regime._position_exit_decision(
            entry_price=0.27,
            mark=0.34,
            bid=0.33,
            held_minutes=2,
        )

        self.assertEqual(decision["action"], "profit")
        self.assertEqual(decision["reason"], "PROFIT_TRAP_MARK_REACHED")

    def test_hard_time_stop_triggers_after_max_hold_window(self):
        decision = vixw_regime._position_exit_decision(
            entry_price=0.27,
            mark=0.28,
            bid=0.27,
            held_minutes=16,
        )

        self.assertEqual(decision["action"], "close")
        self.assertEqual(decision["reason"], "HARD_TIME_STOP")

    def test_macro_blocked_flag_blocks_entry(self):
        decision = self._entry_decision(macro_blocked=True)

        self.assertFalse(decision["entry_allowed"])
        self.assertEqual(decision["skip_reason"], "BLOCKED_MACRO_NEWS_EVENT")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
