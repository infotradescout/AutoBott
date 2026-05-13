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
    def __init__(
        self,
        symbol: str,
        side: str,
        status: str,
        submitted_at: datetime,
        *,
        qty: int = 1,
        limit_price: float | None = None,
    ):
        self.id = f"fake-{symbol}-{submitted_at.timestamp()}"
        self.symbol = symbol
        self.side = side
        self.status = status
        self.submitted_at = submitted_at
        self.qty = qty
        self.limit_price = limit_price
        self.filled_qty = 0
        self.filled_avg_price = None


class FakeBroker:
    def __init__(self, orders):
        self._orders = list(orders)
        self.canceled_order_ids = []

    def get_recent_orders(self, limit: int = 500):
        return self._orders[:limit]

    def cancel_order(self, order_id: str):
        self.canceled_order_ids.append(order_id)


class FakePosition:
    def __init__(self, symbol: str, qty: int = 1, underlying_symbol: str | None = None):
        self.symbol = symbol
        self.qty = str(qty)
        self.underlying_symbol = underlying_symbol


class FakeEntryBroker:
    def __init__(self, status: str = "new"):
        self.order = FakeOrder("ORCL260515C00195000", "buy", status, EASTERN.localize(datetime(2026, 5, 11, 13, 10, 0)))
        self.canceled = 0
        self.submitted = 0

    def place_option_limit_buy(self, option_symbol: str, qty: int, ask_price: float):
        self.submitted += 1
        self.order.symbol = option_symbol
        return self.order

    def get_order_status(self, order_id: str):
        return self.order

    def cancel_order(self, order_id: str):
        self.canceled += 1


class FakeDataClient:
    def get_latest_option_quote(self, option_symbol: str):
        return {"bid": 5.0, "ask": 5.04}


class FakeUniverseDataClient:
    def __init__(self):
        self.assets = {
            "MSFT": {"tradable": True, "status": "active"},
            "NVDA": {"tradable": True, "options_enabled": True, "status": "active"},
        }
        self.prices = {"MSFT": 420.0, "NVDA": 900.0}

    def get_top_movers(self, top: int = 20):
        return ["MSFT"], ["NVDA"]

    def get_asset(self, symbol: str):
        return self.assets[symbol]

    def get_latest_stock_price(self, symbol: str):
        return self.prices.get(symbol)

    def get_all_optionable_tickers(self, *, max_count: int | None = None):
        return ["MSFT", "NVDA", "TSLA", "META", "AMZN"]


class MainOrderGuardTests(unittest.TestCase):
    def setUp(self):
        self._config_values = {
            "ALPACA_BUY_ORDER_CAP_COUNTS_CANCELED": config.ALPACA_BUY_ORDER_CAP_COUNTS_CANCELED,
            "ALPACA_CANCELED_BUY_ORDER_COOLDOWN_MINUTES": config.ALPACA_CANCELED_BUY_ORDER_COOLDOWN_MINUTES,
            "ANTI_CHURN_HOLD_MINUTES": config.ANTI_CHURN_HOLD_MINUTES,
            "MIN_HOLD_EXIT_BYPASS_REASONS": config.MIN_HOLD_EXIT_BYPASS_REASONS,
            "ENTRY_ORDER_STATUS_WAIT_SECONDS": config.ENTRY_ORDER_STATUS_WAIT_SECONDS,
            "ENTRY_LIMIT_ATTEMPTS": config.ENTRY_LIMIT_ATTEMPTS,
            "CANCEL_UNFILLED_ENTRY_BEFORE_RETRY": config.CANCEL_UNFILLED_ENTRY_BEFORE_RETRY,
            "ENABLE_ENTRY_MARKET_FALLBACK": config.ENABLE_ENTRY_MARKET_FALLBACK,
            "ENTRY_RESTING_ORDER_MAX_MINUTES": config.ENTRY_RESTING_ORDER_MAX_MINUTES,
            "MIN_HOLD_EXIT_BYPASS_REASONS": config.MIN_HOLD_EXIT_BYPASS_REASONS,
            "UNIVERSE_MODE": config.UNIVERSE_MODE,
            "AUTO_EXPAND_UNIVERSE_WITH_MOVERS": config.AUTO_EXPAND_UNIVERSE_WITH_MOVERS,
            "UNIVERSE_MAX_TICKERS": config.UNIVERSE_MAX_TICKERS,
            "UNIVERSE_MOVER_TOP": config.UNIVERSE_MOVER_TOP,
            "POSITION_SIZE_USD": config.POSITION_SIZE_USD,
            "RISK_PER_TRADE_PCT": config.RISK_PER_TRADE_PCT,
            "MAX_POSITION_SIZE_USD": config.MAX_POSITION_SIZE_USD,
            "MAX_CONTRACTS_PER_ENTRY": config.MAX_CONTRACTS_PER_ENTRY,
            "MAX_CONTRACTS_PER_TICKER": config.MAX_CONTRACTS_PER_TICKER,
            "DRAWDOWN_REDUCE_AFTER_CONSEC_LOSSES": config.DRAWDOWN_REDUCE_AFTER_CONSEC_LOSSES,
            "DRAWDOWN_SIZE_MULTIPLIER": config.DRAWDOWN_SIZE_MULTIPLIER,
            "EXECUTION_MIN_RVOL_AFTER_IGNORE": config.EXECUTION_MIN_RVOL_AFTER_IGNORE,
            "FAST_START_MIN_SIGNAL_SCORE": config.FAST_START_MIN_SIGNAL_SCORE,
            "FAST_START_MIN_DIRECTION_SCORE": config.FAST_START_MIN_DIRECTION_SCORE,
            "FAST_START_MIN_ABS_ROC_PCT": config.FAST_START_MIN_ABS_ROC_PCT,
            "FAST_START_MIN_VWAP_DISTANCE_PCT": config.FAST_START_MIN_VWAP_DISTANCE_PCT,
        }
        config.ALPACA_BUY_ORDER_CAP_COUNTS_CANCELED = False
        config.ALPACA_CANCELED_BUY_ORDER_COOLDOWN_MINUTES = 10
        config.ANTI_CHURN_HOLD_MINUTES = 30
        config.ENTRY_ORDER_STATUS_WAIT_SECONDS = 1
        config.ENTRY_LIMIT_ATTEMPTS = 1
        config.CANCEL_UNFILLED_ENTRY_BEFORE_RETRY = True
        config.ENABLE_ENTRY_MARKET_FALLBACK = False
        config.ENTRY_RESTING_ORDER_MAX_MINUTES = 10
        config.MIN_HOLD_EXIT_BYPASS_REASONS = ("eod_close",)
        config.UNIVERSE_MODE = "movers"
        config.AUTO_EXPAND_UNIVERSE_WITH_MOVERS = True
        config.UNIVERSE_MAX_TICKERS = 15
        config.UNIVERSE_MOVER_TOP = 50
        config.POSITION_SIZE_USD = 1200
        config.RISK_PER_TRADE_PCT = 0.01
        config.MAX_POSITION_SIZE_USD = 1500
        config.MAX_CONTRACTS_PER_ENTRY = 8
        config.MAX_CONTRACTS_PER_TICKER = 8
        config.DRAWDOWN_REDUCE_AFTER_CONSEC_LOSSES = 1
        config.DRAWDOWN_SIZE_MULTIPLIER = 0.5
        config.EXECUTION_MIN_RVOL_AFTER_IGNORE = 0.20
        config.FAST_START_MIN_SIGNAL_SCORE = 8.4
        config.FAST_START_MIN_DIRECTION_SCORE = 0.75
        config.FAST_START_MIN_ABS_ROC_PCT = 0.12
        config.FAST_START_MIN_VWAP_DISTANCE_PCT = 0.08
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

    def test_minimum_hold_defers_strategy_before_thirty_minutes(self):
        entry_time = self.now - timedelta(minutes=4)

        deferred = main._is_in_anti_churn_window(entry_time, self.now)

        self.assertTrue(deferred)

    def test_minimum_hold_still_defers_after_ten_minutes(self):
        entry_time = self.now - timedelta(minutes=10, seconds=1)

        deferred = main._is_in_anti_churn_window(entry_time, self.now)

        self.assertTrue(deferred)

    def test_minimum_hold_releases_strategy_after_thirty_minutes(self):
        entry_time = self.now - timedelta(minutes=30, seconds=1)

        deferred = main._is_in_anti_churn_window(entry_time, self.now)

        self.assertFalse(deferred)

    def test_minimum_hold_allows_eod_bypass(self):
        entry_time = self.now - timedelta(minutes=4)

        blocked = main._minimum_hold_blocks_exit("eod_close", entry_time, self.now)

        self.assertFalse(blocked)

    def test_minimum_hold_blocks_exposure_normalize(self):
        entry_time = self.now - timedelta(minutes=4)

        blocked = main._minimum_hold_blocks_exit("exposure_normalize", entry_time, self.now)

        self.assertTrue(blocked)

    def test_recent_filled_buy_entry_time_recovers_missing_runtime_meta(self):
        order = FakeOrder(
            "QQQ260512C00710000",
            "buy",
            "filled",
            self.now - timedelta(minutes=2),
        )
        order.filled_qty = 1
        broker = FakeBroker([order])

        entry_time = main._recent_filled_buy_entry_time(broker, "QQQ260512C00710000", self.now)

        self.assertIsNotNone(entry_time)
        self.assertTrue(main._minimum_hold_blocks_exit("profit_target", entry_time, self.now))

    def test_unfilled_active_limit_entry_is_left_resting(self):
        broker = FakeEntryBroker(status="new")

        result = main._execute_limit_entry(
            broker=broker,
            data_client=FakeDataClient(),
            option_symbol="ORCL260515C00195000",
            qty=1,
            now_et=self.now,
            label="ENTRY ORCL",
            initial_quote={"bid": 5.0, "ask": 5.04, "midpoint": 5.02, "spread_pct": 0.8},
        )

        self.assertTrue(result.get("pending_open"))
        self.assertEqual(result.get("status"), "new")
        self.assertEqual(broker.submitted, 1)
        self.assertEqual(broker.canceled, 0)

    def test_unfilled_limit_entry_can_cancel_and_retry(self):
        config.ENTRY_LIMIT_ATTEMPTS = 2
        config.CANCEL_UNFILLED_ENTRY_BEFORE_RETRY = True
        broker = FakeEntryBroker(status="new")

        result = main._execute_limit_entry(
            broker=broker,
            data_client=FakeDataClient(),
            option_symbol="ORCL260515C00195000",
            qty=1,
            now_et=self.now,
            label="ENTRY ORCL",
            initial_quote={"bid": 5.0, "ask": 5.04, "midpoint": 5.02, "spread_pct": 0.8},
        )

        self.assertTrue(result.get("pending_open"))
        self.assertEqual(result.get("status"), "new")
        self.assertEqual(broker.submitted, 2)
        self.assertEqual(broker.canceled, 1)

    def test_entry_qty_uses_budget_and_contract_cap(self):
        qty = main._entry_qty_for_budget(
            ask_price=0.52,
            equity=150000.0,
            consecutive_losses=0,
            max_trade_premium=1500.0,
        )

        self.assertEqual(qty, 8)

    def test_entry_qty_reduces_after_loss(self):
        qty = main._entry_qty_for_budget(
            ask_price=1.0,
            equity=150000.0,
            consecutive_losses=1,
            max_trade_premium=1500.0,
        )

        self.assertEqual(qty, 7)

    def test_fast_start_midday_still_rejects_dead_rvol(self):
        now = EASTERN.localize(datetime(2026, 5, 13, 11, 31, 0))

        ok, reason = main._fast_start_entry_quality_ok(
            {
                "signal_score": 18.0,
                "direction_score": 1.0,
                "rvol": 0.02,
                "roc": -0.30,
                "price": 99.0,
                "vwap": 100.0,
            },
            now,
        )

        self.assertFalse(ok)
        self.assertIn("RVOL too weak", reason)

    def test_execution_sort_prefers_rvol_before_raw_score(self):
        weak_high_score = {"symbol": "CRM", "signal_score": 17.26, "direction_score": 1.0, "rvol": 0.02, "roc": -0.30}
        active_lower_score = {"symbol": "IWM", "signal_score": 22.29, "direction_score": 0.50, "rvol": 7.13, "roc": 0.01}
        signals = [weak_high_score, active_lower_score]

        signals.sort(key=main._execution_signal_sort_key, reverse=True)

        self.assertEqual(signals[0]["symbol"], "IWM")

    def test_ticker_open_qty_counts_live_positions_and_meta(self):
        positions = [FakePosition("IWM260515C00282000", qty=3)]
        meta = {
            "IWM260515C00282000": {"ticker": "IWM", "qty": 3},
            "IWM260515C00283000": {"ticker": "IWM", "qty": 2},
            "QQQ260515P00450000": {"ticker": "QQQ", "qty": 4},
        }

        self.assertEqual(main._ticker_open_qty(positions, meta, "IWM"), 5)

    def test_ticker_open_qty_uses_meta_when_live_position_not_seen_yet(self):
        meta = {"IWM260515C00282000": {"ticker": "IWM", "qty": 4}}

        self.assertEqual(main._ticker_open_qty([], meta, "IWM"), 4)

    def test_active_buy_order_counts_as_resting_entry(self):
        broker = FakeBroker(
            [
                FakeOrder(
                    "ORCL260515C00195000",
                    "buy",
                    "new",
                    self.now - timedelta(minutes=1),
                )
            ]
        )

        counts = main._alpaca_active_option_buy_order_counts_by_ticker_today(broker, self.now)

        self.assertEqual(counts.get("ORCL"), 1)

    def test_active_buy_order_premium_counts_pending_exposure(self):
        broker = FakeBroker(
            [
                FakeOrder(
                    "ORCL260515C00195000",
                    "buy",
                    "new",
                    self.now - timedelta(minutes=1),
                    qty=2,
                    limit_price=5.12,
                ),
                FakeOrder(
                    "JPM260515P00290000",
                    "buy",
                    "canceled",
                    self.now - timedelta(minutes=1),
                    qty=1,
                    limit_price=0.73,
                ),
            ]
        )

        premium = main._alpaca_active_option_buy_order_premium_usd_today(broker, self.now)

        self.assertEqual(premium, 1024.0)

    def test_stale_active_entry_order_is_canceled_after_max_rest(self):
        order = FakeOrder(
            "ORCL260515C00195000",
            "buy",
            "new",
            self.now - timedelta(minutes=11),
        )
        broker = FakeBroker([order])

        canceled = main._cancel_stale_active_entry_buy_orders(broker, self.now)

        self.assertEqual(canceled, 1)
        self.assertEqual(broker.canceled_order_ids, [order.id])

    def test_recent_active_entry_order_is_left_resting(self):
        order = FakeOrder(
            "ORCL260515C00195000",
            "buy",
            "new",
            self.now - timedelta(minutes=5),
        )
        broker = FakeBroker([order])

        canceled = main._cancel_stale_active_entry_buy_orders(broker, self.now)

        self.assertEqual(canceled, 0)

    def test_active_entry_order_for_open_ticker_is_canceled(self):
        config.MAX_CONTRACTS_PER_TICKER = 1
        order = FakeOrder(
            "ORCL260515C00195000",
            "buy",
            "new",
            self.now - timedelta(minutes=1),
        )
        broker = FakeBroker([order])
        positions = [FakePosition("ORCL260515C00195000", qty=1)]

        canceled = main._cancel_active_entry_buys_for_open_tickers(broker, positions, self.now)

        self.assertEqual(canceled, 1)
        self.assertEqual(broker.canceled_order_ids, [order.id])

    def test_active_scale_in_order_for_open_ticker_is_kept_below_cap(self):
        config.MAX_CONTRACTS_PER_TICKER = 8
        order = FakeOrder(
            "ORCL260515C00195000",
            "buy",
            "new",
            self.now - timedelta(minutes=1),
        )
        broker = FakeBroker([order])
        positions = [FakePosition("ORCL260515C00195000", qty=1)]

        canceled = main._cancel_active_entry_buys_for_open_tickers(broker, positions, self.now)

        self.assertEqual(canceled, 0)
        self.assertEqual(broker.canceled_order_ids, [])

    def test_excess_active_entry_orders_for_same_ticker_are_canceled(self):
        older = FakeOrder(
            "ORCL260515C00195000",
            "buy",
            "new",
            self.now - timedelta(minutes=3),
        )
        newer = FakeOrder(
            "ORCL260515C00195000",
            "buy",
            "new",
            self.now - timedelta(minutes=1),
        )
        other = FakeOrder(
            "JPM260515P00290000",
            "buy",
            "new",
            self.now - timedelta(minutes=1),
        )
        broker = FakeBroker([older, newer, other])

        canceled = main._cancel_excess_active_entry_buys_per_ticker(broker, self.now)

        self.assertEqual(canceled, 1)
        self.assertEqual(broker.canceled_order_ids, [older.id])

    def test_mover_filter_keeps_assets_when_options_enabled_field_is_missing(self):
        data_client = FakeUniverseDataClient()

        kept = main._filter_mover_candidates(data_client, ["MSFT"], protected=set(config.CORE_TICKERS))

        self.assertEqual(kept, ["MSFT"])

    def test_scan_universe_supplements_when_movers_are_too_small(self):
        data_client = FakeUniverseDataClient()

        universe = main._build_scan_universe(data_client)

        self.assertIn("MSFT", universe)
        self.assertIn("NVDA", universe)
        self.assertIn("TSLA", universe)
        self.assertGreater(len(universe), len(config.TICKERS))


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
